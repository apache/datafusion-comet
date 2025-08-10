// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

/// Utils for array vector, etc.
use crate::errors::ExpressionError;
use crate::execution::operators::ExecutionError;
use arrow::datatypes::{DataType, Field, SchemaRef};
use arrow::{
    array::ArrayData,
    error::ArrowError,
    ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema},
};
use datafusion::execution::FunctionRegistry;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_comet_proto::spark_operator::HashAggregate;
use datafusion_comet_spark_expr::create_comet_physical_fun;
use std::collections::HashMap;
use std::sync::Arc;

impl From<ArrowError> for ExecutionError {
    fn from(error: ArrowError) -> ExecutionError {
        ExecutionError::ArrowError(error.to_string())
    }
}

impl From<ArrowError> for ExpressionError {
    fn from(error: ArrowError) -> ExpressionError {
        ExpressionError::ArrowError(error.to_string())
    }
}

impl From<ExpressionError> for ArrowError {
    fn from(error: ExpressionError) -> ArrowError {
        ArrowError::ComputeError(error.to_string())
    }
}

pub trait SparkArrowConvert {
    /// Build Arrow Arrays from C data interface passed from Spark.
    /// It accepts a tuple (ArrowArray address, ArrowSchema address).
    fn from_spark(addresses: (i64, i64)) -> Result<Self, ExecutionError>
    where
        Self: Sized;

    /// Move Arrow Arrays to C data interface.
    fn move_to_spark(&self, array: i64, schema: i64) -> Result<(), ExecutionError>;
}

impl SparkArrowConvert for ArrayData {
    fn from_spark(addresses: (i64, i64)) -> Result<Self, ExecutionError> {
        let (array_ptr, schema_ptr) = addresses;

        let array_ptr = array_ptr as *mut FFI_ArrowArray;
        let schema_ptr = schema_ptr as *mut FFI_ArrowSchema;

        if array_ptr.is_null() || schema_ptr.is_null() {
            return Err(ExecutionError::ArrowError(
                "At least one of passed pointers is null".to_string(),
            ));
        };

        // `ArrowArray` will convert raw pointers back to `Arc`. No worries
        // about memory leak.
        let mut ffi_array = unsafe {
            let array_data = std::ptr::replace(array_ptr, FFI_ArrowArray::empty());
            let schema_data = std::ptr::replace(schema_ptr, FFI_ArrowSchema::empty());

            from_ffi(array_data, &schema_data)?
        };

        // Align imported buffers from Java.
        ffi_array.align_buffers();

        Ok(ffi_array)
    }

    /// Move this ArrowData to pointers of Arrow C data interface.
    fn move_to_spark(&self, array: i64, schema: i64) -> Result<(), ExecutionError> {
        let array_ptr = array as *mut FFI_ArrowArray;
        let schema_ptr = schema as *mut FFI_ArrowSchema;

        let array_align = std::mem::align_of::<FFI_ArrowArray>();
        let schema_align = std::mem::align_of::<FFI_ArrowSchema>();

        // Check if the pointer alignment is correct.
        if array_ptr.align_offset(array_align) != 0 || schema_ptr.align_offset(schema_align) != 0 {
            unsafe {
                std::ptr::write_unaligned(array_ptr, FFI_ArrowArray::new(self));
                std::ptr::write_unaligned(schema_ptr, FFI_ArrowSchema::try_from(self.data_type())?);
            }
        } else {
            // SAFETY: `array_ptr` and `schema_ptr` are aligned correctly.
            unsafe {
                std::ptr::write(array_ptr, FFI_ArrowArray::new(self));
                std::ptr::write(schema_ptr, FFI_ArrowSchema::try_from(self.data_type())?);
            }
        }

        Ok(())
    }
}

/// Converts a slice of bytes to i128. The bytes are serialized in big-endian order by
/// `BigInteger.toByteArray()` in Java.
pub fn bytes_to_i128(slice: &[u8]) -> i128 {
    let mut bytes = [0; 16];
    let mut i = 0;
    while i != 16 && i != slice.len() {
        bytes[i] = slice[slice.len() - 1 - i];
        i += 1;
    }

    // if the decimal is negative, we need to flip all the bits
    if (slice[0] as i8) < 0 {
        while i < 16 {
            bytes[i] = !bytes[i];
            i += 1;
        }
    }

    i128::from_le_bytes(bytes)
}

type GroupingExprs = Vec<(Arc<dyn PhysicalExpr>, String)>;
type GroupingExprResult = Result<GroupingExprs, ExecutionError>;

/// Provides utilities to support grouping on Map type in HashAggregate.
pub struct HashAggregateMapConverter {
    // Maps index of a grouping expression to its original Map type. This is used to convert a
    // grouping expression return type back to Map type after aggregation.
    expr_index_to_map_type: HashMap<usize, DataType>,
}

impl HashAggregateMapConverter {
    pub fn default() -> Self {
        Self {
            expr_index_to_map_type: HashMap::new(),
        }
    }

    /// Iterates through grouping expressions, and wraps those with Map type with `map_to_list`
    /// scalar function.
    pub fn maybe_wrap_map_type_in_grouping_exprs(
        &mut self,
        fn_registry: &dyn FunctionRegistry,
        grouping_exprs: GroupingExprs,
        child_schema: SchemaRef,
    ) -> GroupingExprResult {
        grouping_exprs
            .into_iter()
            .enumerate()
            .map(|(idx, (physical_expr, expr_name))| {
                let expr_data_type = physical_expr.data_type(&child_schema)?;

                if let DataType::Map(field_ref, _) = &expr_data_type {
                    let list_type = DataType::List(Arc::clone(field_ref));

                    // Update the map with the grouping expression index and its original Map type.
                    self.expr_index_to_map_type
                        .insert(idx, expr_data_type.clone());

                    // Create `map_to_list` expression to wrap the original grouping expression.
                    let map_to_list_func = create_comet_physical_fun(
                        "map_to_list",
                        list_type.clone(),
                        fn_registry,
                        None,
                    )?;
                    let map_to_list_expr = ScalarFunctionExpr::new(
                        "map_to_list",
                        map_to_list_func,
                        vec![physical_expr],
                        Arc::new(Field::new("map_to_list", list_type, true)),
                    );

                    // Return the scalar function expression.
                    Ok((
                        Arc::new(map_to_list_expr) as Arc<dyn PhysicalExpr>,
                        expr_name,
                    ))
                } else {
                    Ok((physical_expr, expr_name))
                }
            })
            .collect()
    }

    /// Iterates over the aggregate schema, find the grouping expressions with Map type, and
    /// wraps them with `map_from_list` scalar function to convert them back to Map type. It returns
    /// a new ProjectionExec stacked on top of the original aggregate execution plan. If there was
    /// no grouping expression with Map type, it returns the original aggregate execution plan.
    pub fn maybe_project_map_type_with_aggregation(
        &self,
        fn_registry: &dyn FunctionRegistry,
        hash_agg: &HashAggregate,
        aggregate: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>, ExecutionError> {
        // If there was no grouping expression with Map type, return the original aggregate plan.
        if self.expr_index_to_map_type.is_empty() {
            return Ok(aggregate);
        }

        // Insert the projection expressions in this.
        let mut projection_exprs = Vec::new();

        let num_grouping_cols = hash_agg.grouping_exprs.len();
        let agg_schema = aggregate.schema();

        // Iterate through the aggregate schema. The aggregate schema contains both grouping
        // expressions and aggregate expressions. The grouping expressions are at the beginning of
        // the schema.
        for (field_idx, field) in agg_schema.fields().iter().enumerate() {
            let opt_map_type = self.expr_index_to_map_type.get(&field_idx);

            // If the current field is not a grouping expression or the grouping expression does not
            // have Map type, then project the current field as it is.
            if field_idx >= num_grouping_cols || opt_map_type.is_none() {
                let col_expr =
                    Arc::new(Column::new(field.name(), field_idx)) as Arc<dyn PhysicalExpr>;
                projection_exprs.push((col_expr, field.name().to_string()));
                continue;
            }

            let map_type = opt_map_type.unwrap();

            // Create `map_from_list` expression to convert the List type back to Map type. This
            // expression was previously wrapped with `map_to_list` during grouping.
            let map_from_list_func =
                create_comet_physical_fun("map_from_list", map_type.clone(), fn_registry, None)?;
            let col_expr = Arc::new(Column::new(field.name(), field_idx));
            let map_to_list_expr = Arc::new(ScalarFunctionExpr::new(
                "map_from_list",
                map_from_list_func,
                vec![col_expr],
                Arc::new(Field::new(
                    field.name(),
                    map_type.clone(),
                    field.is_nullable(),
                )),
            )) as Arc<dyn PhysicalExpr>;

            // Add the `map_from_list` expression to the projection expressions.
            projection_exprs.push((map_to_list_expr, field.name().to_string()));
        }

        // Return a new ProjectionExec on top of the original aggregate plan.
        Ok(Arc::new(ProjectionExec::try_new(
            projection_exprs,
            Arc::clone(&aggregate),
        )?) as Arc<dyn ExecutionPlan>)
    }
}
