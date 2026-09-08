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

use arrow::array::{make_array, Array, ArrayRef, StructArray};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result as DataFusionResult, ScalarValue};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{
    ColumnarValue, ExpressionPlacement, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, StructFieldAccess, Volatility,
};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::ScalarFunctionExpr;
use std::{
    fmt::{Display, Formatter},
    hash::Hash,
    sync::Arc,
};

#[derive(Debug, Eq)]
pub struct GetStructField {
    child: Arc<dyn PhysicalExpr>,
    ordinal: usize,
}

impl Hash for GetStructField {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.child.hash(state);
        self.ordinal.hash(state);
    }
}
impl PartialEq for GetStructField {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child) && self.ordinal.eq(&other.ordinal)
    }
}

impl GetStructField {
    pub fn new(child: Arc<dyn PhysicalExpr>, ordinal: usize) -> Self {
        Self { child, ordinal }
    }

    /// Expose an unambiguous field name to DataFusion's nested projection and pruning.
    pub fn with_field_access(
        child: Arc<dyn PhysicalExpr>,
        ordinal: usize,
        schema: &Schema,
    ) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        let expr = Self::new(child, ordinal);
        let field = expr.child_field(schema)?;
        let DataType::Struct(fields) = expr.child.data_type(schema)? else {
            unreachable!()
        };
        // Spark accesses by ordinal, including structs with duplicate field names.
        if fields.iter().filter(|f| f.name() == field.name()).count() != 1 {
            return Ok(Arc::new(expr));
        }
        Ok(Arc::new(ScalarFunctionExpr::try_new(
            Arc::new(ScalarUDF::new_from_impl(StructFieldUdf {
                name: field.name().clone(),
                signature: Signature::any(1, Volatility::Immutable),
            })),
            vec![expr.child],
            schema,
            Arc::new(ConfigOptions::default()),
        )?))
    }

    fn child_field(&self, input_schema: &Schema) -> DataFusionResult<Arc<Field>> {
        match self.child.data_type(input_schema)? {
            DataType::Struct(fields) => Ok(Arc::clone(&fields[self.ordinal])),
            data_type => Err(DataFusionError::Plan(format!(
                "Expect struct field, got {data_type:?}"
            ))),
        }
    }

    /// Extract field `ordinal` from a struct array, propagating the parent struct's null mask.
    ///
    /// Spark semantics: a field of a NULL struct is NULL. Arrow stores a StructArray's child
    /// arrays with their own validity, INDEPENDENT of the parent struct's null buffer -- so the
    /// raw child value at a row where the struct itself is null can be non-null (e.g. parquet
    /// files where a logically-null struct column still has a populated child buffer). Returning
    /// the child column verbatim then makes `isnotnull(struct.field)` wrongly true for a null
    /// struct. Union the struct's null mask into the child's (null where the struct is null OR
    /// the child is null).
    fn project_field(struct_array: &StructArray, ordinal: usize) -> DataFusionResult<ArrayRef> {
        let child = struct_array.column(ordinal);
        match struct_array.nulls() {
            Some(_) => {
                let combined = NullBuffer::union(struct_array.nulls(), child.nulls());
                let data = child.to_data().into_builder().nulls(combined).build()?;
                Ok(make_array(data))
            }
            None => Ok(Arc::clone(child)),
        }
    }
}

impl PhysicalExpr for GetStructField {
    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }

    fn data_type(&self, input_schema: &Schema) -> DataFusionResult<DataType> {
        Ok(self.child_field(input_schema)?.data_type().clone())
    }

    fn nullable(&self, input_schema: &Schema) -> DataFusionResult<bool> {
        // A field extracted from a struct is nullable if EITHER the field itself is declared
        // nullable OR the parent struct can be null -- a field of a null struct is null (Spark
        // semantics, enforced by `project_field` unioning the parent null mask). Reporting only
        // the field's own nullability under-declares: a non-nullable field of a nullable struct
        // then carries the parent's nulls while claiming non-nullable, which fails Arrow's
        // RecordBatch validation downstream with "declared as non-nullable but contains null
        // values" (e.g. once the projected column reaches a shuffle/sort). Mirrors Spark's
        // `GetStructField.nullable = child.nullable || field.nullable`.
        Ok(self.child.nullable(input_schema)? || self.child_field(input_schema)?.is_nullable())
    }

    fn evaluate(&self, batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        let child_value = self.child.evaluate(batch)?;

        match child_value {
            ColumnarValue::Array(array) => {
                let struct_array = array
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .expect("A struct is expected");

                Ok(ColumnarValue::Array(Self::project_field(
                    struct_array,
                    self.ordinal,
                )?))
            }
            ColumnarValue::Scalar(ScalarValue::Struct(struct_array)) => Ok(ColumnarValue::Array(
                Self::project_field(&struct_array, self.ordinal)?,
            )),
            value => Err(DataFusionError::Execution(format!(
                "Expected a struct array, got {value:?}"
            ))),
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::common::Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(GetStructField::new(
            Arc::clone(&children[0]),
            self.ordinal,
        )))
    }
}

impl Display for GetStructField {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "GetStructField [child: {:?}, ordinal: {:?}]",
            self.child, self.ordinal
        )
    }
}

/// Name-based evaluation survives the schema adapter reordering or narrowing the struct.
#[derive(Debug, PartialEq, Eq, Hash)]
struct StructFieldUdf {
    name: String,
    signature: Signature,
}

impl ScalarUDFImpl for StructFieldUdf {
    fn name(&self) -> &str {
        "spark_get_struct_field"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, types: &[DataType]) -> DataFusionResult<DataType> {
        match &types[0] {
            DataType::Struct(fields) => fields
                .iter()
                .find(|f| f.name() == &self.name)
                .map(|f| f.data_type().clone())
                .ok_or_else(|| {
                    DataFusionError::Plan(format!("Missing struct field {}", self.name))
                }),
            _ => datafusion::common::exec_err!("Expected a struct"),
        }
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> DataFusionResult<Arc<Field>> {
        let parent = &args.arg_fields[0];
        let DataType::Struct(fields) = parent.data_type() else {
            return datafusion::common::exec_err!("Expected a struct");
        };
        let field = fields
            .iter()
            .find(|f| f.name() == &self.name)
            .ok_or_else(|| DataFusionError::Plan(format!("Missing struct field {}", self.name)))?;
        Ok(Arc::new(field.as_ref().clone().with_nullable(
            parent.is_nullable() || field.is_nullable(),
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let scalar = matches!(args.args[0], ColumnarValue::Scalar(_));
        let array = args.args[0]
            .clone()
            .into_array(if scalar { 1 } else { args.number_rows })?;
        let array = array
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| DataFusionError::Execution("Expected a struct".into()))?;
        let ordinal = array
            .fields()
            .iter()
            .position(|f| f.name() == &self.name)
            .ok_or_else(|| {
                DataFusionError::Execution(format!("Missing struct field {}", self.name))
            })?;
        let result = GetStructField::project_field(array, ordinal)?;
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &result, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(result))
        }
    }

    fn struct_field_access(&self, _args: &[Option<ScalarValue>]) -> Option<StructFieldAccess> {
        Some(StructFieldAccess {
            source_arg: 0,
            field_path: vec![self.name.clone()],
        })
    }

    fn placement(&self, args: &[ExpressionPlacement]) -> ExpressionPlacement {
        match args.first() {
            Some(ExpressionPlacement::Column | ExpressionPlacement::MoveTowardsLeafNodes) => {
                ExpressionPlacement::MoveTowardsLeafNodes
            }
            _ => ExpressionPlacement::KeepInPlace,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::Fields;
    use datafusion::physical_expr::expressions::Column;

    #[test]
    fn field_access_handles_reordered_fields_scalars_and_duplicate_names() {
        use datafusion::physical_expr::expressions::Literal;
        let fields: Fields = vec![
            Field::new("other", DataType::Int64, false),
            Field::new("k.dot", DataType::Int64, false),
        ]
        .into();
        let schema = Schema::new(vec![Field::new(
            "s",
            DataType::Struct(fields.clone()),
            false,
        )]);
        let expr =
            GetStructField::with_field_access(Arc::new(Column::new("s", 0)), 1, &schema).unwrap();
        let reordered = StructArray::new(
            vec![fields[1].clone(), fields[0].clone()].into(),
            vec![
                Arc::new(Int64Array::from(vec![42])),
                Arc::new(Int64Array::from(vec![99])),
            ],
            None,
        );
        let literal: Arc<dyn PhysicalExpr> =
            Arc::new(Literal::new(ScalarValue::Struct(Arc::new(reordered))));
        let expr = expr.with_new_children(vec![literal]).unwrap();
        let batch = RecordBatch::new_empty(Arc::new(schema));
        assert!(matches!(
            expr.evaluate(&batch).unwrap(),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(42)))
        ));

        let duplicate = StructArray::new(
            vec![fields[1].clone(), fields[1].clone()].into(),
            vec![
                Arc::new(Int64Array::from(vec![42])),
                Arc::new(Int64Array::from(vec![99])),
            ],
            None,
        );
        let literal: Arc<dyn PhysicalExpr> =
            Arc::new(Literal::new(ScalarValue::Struct(Arc::new(duplicate))));
        let expr = GetStructField::with_field_access(literal, 1, &Schema::empty()).unwrap();
        assert!(expr.downcast_ref::<GetStructField>().is_some());
        let output = expr.evaluate(&batch).unwrap().into_array(1).unwrap();
        assert_eq!(
            ScalarValue::try_from_array(&output, 0).unwrap(),
            ScalarValue::Int64(Some(99))
        );
    }

    // A field of a NULL struct must be NULL (Spark semantics) even when the child buffer holds a
    // non-null value at that row -- Arrow stores child validity independently of the parent
    // struct's null mask, so a logically-null struct column read from parquet can still carry a
    // populated child buffer. Without propagating the parent null mask, `isnotnull(struct.field)`
    // wrongly evaluates TRUE for a null struct.
    #[test]
    fn field_of_null_struct_is_null() {
        // Child is non-null at every row; the struct itself is null at rows 1 and 3.
        let child = Arc::new(Int64Array::from(vec![10_i64, 20, 30, 40])) as ArrayRef;
        let fields: Fields = Fields::from(vec![Field::new("version", DataType::Int64, true)]);
        let nulls = NullBuffer::from(vec![true, false, true, false]);
        let struct_array = StructArray::new(fields.clone(), vec![child], Some(nulls));
        let schema = Schema::new(vec![Field::new("cm", DataType::Struct(fields), true)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(struct_array)]).unwrap();

        let expr =
            GetStructField::with_field_access(Arc::new(Column::new("cm", 0)), 0, &batch.schema())
                .unwrap();
        let out = expr
            .evaluate(&batch)
            .unwrap()
            .into_array(batch.num_rows())
            .unwrap();
        let out = out.as_any().downcast_ref::<Int64Array>().unwrap();

        assert!(!out.is_null(0) && out.value(0) == 10);
        assert!(out.is_null(1), "field of a null struct must be null");
        assert!(!out.is_null(2) && out.value(2) == 30);
        assert!(out.is_null(3), "field of a null struct must be null");
    }

    // A NON-nullable field of a NULLABLE struct must report `nullable() == true`: `project_field`
    // unions the parent struct's null mask, so the projected column carries nulls wherever the
    // struct is null. Reporting the field's own (non-nullable) flag would make the output schema
    // lie, failing Arrow RecordBatch validation downstream with "declared as non-nullable but
    // contains null values" once the column reaches a shuffle/sort.
    #[test]
    fn non_nullable_field_of_nullable_struct_is_nullable() {
        // `size` is declared non-nullable, but the enclosing struct is nullable.
        let inner: Fields = Fields::from(vec![Field::new("size", DataType::Int64, false)]);
        let schema = Schema::new(vec![Field::new(
            "add",
            DataType::Struct(inner),
            /* struct nullable */ true,
        )]);

        let expr =
            GetStructField::with_field_access(Arc::new(Column::new("add", 0)), 0, &schema).unwrap();
        assert!(
            expr.nullable(&schema).unwrap(),
            "a field of a nullable struct must be nullable even if the field itself is non-nullable"
        );
    }

    // A non-nullable field of a NON-nullable struct stays non-nullable (no over-declaring).
    #[test]
    fn non_nullable_field_of_non_nullable_struct_stays_non_nullable() {
        let inner: Fields = Fields::from(vec![Field::new("size", DataType::Int64, false)]);
        let schema = Schema::new(vec![Field::new(
            "add",
            DataType::Struct(inner),
            /* struct nullable */ false,
        )]);

        let expr =
            GetStructField::with_field_access(Arc::new(Column::new("add", 0)), 0, &schema).unwrap();
        assert!(
            !expr.nullable(&schema).unwrap(),
            "a non-nullable field of a non-nullable struct must remain non-nullable"
        );
    }
}
