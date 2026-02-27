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

use arrow::array::{Array, ArrayRef, BooleanArray, ListArray};
use arrow::compute::kernels::take::take;
use arrow::datatypes::{DataType, Field, Schema, UInt32Type};
use arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use std::any::Any;
use std::fmt::{Debug, Display, Formatter};
use std::hash::Hash;
use std::sync::Arc;

const LAMBDA_VAR_COLUMN: &str = "__comet_lambda_var";

/// Spark-compatible `array_exists(array, x -> predicate(x))`.
///
/// Evaluates the lambda body vectorized over all elements in a single pass rather
/// than per-element to avoid repeated batch construction overhead.
#[derive(Debug, Eq)]
pub struct ArrayExistsExpr {
    array_expr: Arc<dyn PhysicalExpr>,
    lambda_body: Arc<dyn PhysicalExpr>,
    follow_three_valued_logic: bool,
}

impl Hash for ArrayExistsExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.array_expr.hash(state);
        self.lambda_body.hash(state);
        self.follow_three_valued_logic.hash(state);
    }
}

impl PartialEq for ArrayExistsExpr {
    fn eq(&self, other: &Self) -> bool {
        self.array_expr.eq(&other.array_expr)
            && self.lambda_body.eq(&other.lambda_body)
            && self
                .follow_three_valued_logic
                .eq(&other.follow_three_valued_logic)
    }
}

impl ArrayExistsExpr {
    pub fn new(
        array_expr: Arc<dyn PhysicalExpr>,
        lambda_body: Arc<dyn PhysicalExpr>,
        follow_three_valued_logic: bool,
    ) -> Self {
        Self {
            array_expr,
            lambda_body,
            follow_three_valued_logic,
        }
    }
}

impl PhysicalExpr for ArrayExistsExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }

    fn data_type(&self, _input_schema: &Schema) -> DataFusionResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &Schema) -> DataFusionResult<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        let num_rows = batch.num_rows();

        // Evaluate the array expression
        let array_value = self.array_expr.evaluate(batch)?.into_array(num_rows)?;

        let list_array = array_value
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("ArrayExists expects a ListArray input".to_string())
            })?;

        let offsets = list_array.offsets();
        let values = list_array.values();
        let total_elements = values.len();

        if total_elements == 0 {
            let mut result_builder = BooleanArray::builder(num_rows);
            for row in 0..num_rows {
                if list_array.is_null(row) {
                    result_builder.append_null();
                } else {
                    result_builder.append_value(false);
                }
            }
            return Ok(ColumnarValue::Array(Arc::new(result_builder.finish())));
        }

        let mut repeat_indices = Vec::with_capacity(total_elements);
        for row in 0..num_rows {
            let start = offsets[row] as usize;
            let end = offsets[row + 1] as usize;
            for _ in start..end {
                repeat_indices.push(row as u32);
            }
        }

        let repeat_indices_array = arrow::array::PrimitiveArray::<UInt32Type>::from(repeat_indices);

        let mut expanded_columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns() + 1);
        let mut expanded_fields: Vec<Arc<Field>> = Vec::with_capacity(batch.num_columns() + 1);

        for (i, col) in batch.columns().iter().enumerate() {
            let expanded = take(col.as_ref(), &repeat_indices_array, None)?;
            expanded_columns.push(expanded);
            expanded_fields.push(Arc::new(batch.schema().field(i).clone()));
        }

        let element_field = Arc::new(Field::new(
            LAMBDA_VAR_COLUMN,
            values.data_type().clone(),
            true,
        ));
        expanded_columns.push(Arc::clone(values));
        expanded_fields.push(element_field);

        let expanded_schema = Arc::new(Schema::new(expanded_fields));
        let expanded_batch = RecordBatch::try_new(expanded_schema, expanded_columns)?;

        let body_result = self
            .lambda_body
            .evaluate(&expanded_batch)?
            .into_array(total_elements)?;

        let body_booleans = body_result
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "ArrayExists lambda body must return BooleanArray".to_string(),
                )
            })?;

        let mut result_builder = BooleanArray::builder(num_rows);
        for row in 0..num_rows {
            if list_array.is_null(row) {
                result_builder.append_null();
                continue;
            }

            let start = offsets[row] as usize;
            let end = offsets[row + 1] as usize;

            if start == end {
                result_builder.append_value(false);
                continue;
            }

            let mut found_true = false;
            let mut found_null = false;

            for idx in start..end {
                if body_booleans.is_null(idx) {
                    found_null = true;
                } else if body_booleans.value(idx) {
                    found_true = true;
                    break;
                }
            }

            if found_true {
                result_builder.append_value(true);
            } else if found_null && self.follow_three_valued_logic {
                result_builder.append_null();
            } else {
                result_builder.append_value(false);
            }
        }

        Ok(ColumnarValue::Array(Arc::new(result_builder.finish())))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.array_expr, &self.lambda_body]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        match children.len() {
            2 => Ok(Arc::new(ArrayExistsExpr::new(
                Arc::clone(&children[0]),
                Arc::clone(&children[1]),
                self.follow_three_valued_logic,
            ))),
            _ => Err(DataFusionError::Internal(
                "ArrayExistsExpr should have exactly two children".to_string(),
            )),
        }
    }
}

impl Display for ArrayExistsExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ArrayExists [array: {:?}, lambda_body: {:?}]",
            self.array_expr, self.lambda_body
        )
    }
}

#[derive(Debug, Eq)]
pub struct LambdaVariableExpr {
    data_type: DataType,
}

impl Hash for LambdaVariableExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.data_type.hash(state);
    }
}

impl PartialEq for LambdaVariableExpr {
    fn eq(&self, other: &Self) -> bool {
        self.data_type == other.data_type
    }
}

impl LambdaVariableExpr {
    pub fn new(data_type: DataType) -> Self {
        Self { data_type }
    }
}

impl PhysicalExpr for LambdaVariableExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }

    fn data_type(&self, _input_schema: &Schema) -> DataFusionResult<DataType> {
        Ok(self.data_type.clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> DataFusionResult<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        let schema = batch.schema();
        let idx = schema.index_of(LAMBDA_VAR_COLUMN).map_err(|_| {
            DataFusionError::Internal(format!(
                "Lambda variable column '{}' not found in batch schema",
                LAMBDA_VAR_COLUMN
            ))
        })?;
        Ok(ColumnarValue::Array(Arc::clone(batch.column(idx))))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        if children.is_empty() {
            Ok(Arc::new(LambdaVariableExpr::new(self.data_type.clone())))
        } else {
            Err(DataFusionError::Internal(
                "LambdaVariableExpr should have no children".to_string(),
            ))
        }
    }
}

impl Display for LambdaVariableExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "LambdaVariable({})", self.data_type)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow::array::ListArray;
    use arrow::datatypes::Int32Type;
    use datafusion::physical_expr::expressions::{Column, Literal};
    use datafusion::{
        common::ScalarValue, logical_expr::Operator, physical_expr::expressions::BinaryExpr,
    };

    fn make_lambda_var_expr() -> Arc<dyn PhysicalExpr> {
        Arc::new(LambdaVariableExpr::new(DataType::Int32))
    }

    fn make_gt_predicate(threshold: i32) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(
            make_lambda_var_expr(),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(threshold)))),
        ))
    }

    #[test]
    fn test_basic_exists() -> DataFusionResult<()> {
        // exists(array(1, 2, 3), x -> x > 2) = true
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
            Some(1),
            Some(2),
            Some(3),
        ])]);

        let schema = Arc::new(Schema::new(vec![Field::new(
            "arr",
            list.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(list)])?;

        let array_expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("arr", 0));
        let lambda_body = make_gt_predicate(2);
        let expr = ArrayExistsExpr::new(array_expr, lambda_body, true);

        let result = expr.evaluate(&batch)?.into_array(1)?;
        let bools = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(bools.value(0));
        assert!(!bools.is_null(0));
        Ok(())
    }

    #[test]
    fn test_empty_array() -> DataFusionResult<()> {
        // exists(array(), x -> x > 0) = false
        let list =
            ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                Some(Vec::<Option<i32>>::new()),
            ]);

        let schema = Arc::new(Schema::new(vec![Field::new(
            "arr",
            list.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(list)])?;

        let array_expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("arr", 0));
        let lambda_body = make_gt_predicate(0);
        let expr = ArrayExistsExpr::new(array_expr, lambda_body, true);

        let result = expr.evaluate(&batch)?.into_array(1)?;
        let bools = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(!bools.value(0));
        assert!(!bools.is_null(0));
        Ok(())
    }

    #[test]
    fn test_null_array() -> DataFusionResult<()> {
        // exists(null, x -> x > 0) = null
        let list =
            ListArray::from_iter_primitive::<Int32Type, _, _>(vec![None::<Vec<Option<i32>>>]);

        let schema = Arc::new(Schema::new(vec![Field::new(
            "arr",
            list.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(list)])?;

        let array_expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("arr", 0));
        let lambda_body = make_gt_predicate(0);
        let expr = ArrayExistsExpr::new(array_expr, lambda_body, true);

        let result = expr.evaluate(&batch)?.into_array(1)?;
        let bools = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(bools.is_null(0));
        Ok(())
    }

    #[test]
    fn test_three_valued_logic() -> DataFusionResult<()> {
        // exists(array(1, null, 3), x -> x > 5) = null (three-valued logic)
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
            Some(1),
            None,
            Some(3),
        ])]);

        let schema = Arc::new(Schema::new(vec![Field::new(
            "arr",
            list.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(list)])?;

        let array_expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("arr", 0));
        let lambda_body = make_gt_predicate(5);

        // With three-valued logic: result should be null
        let expr = ArrayExistsExpr::new(Arc::clone(&array_expr), Arc::clone(&lambda_body), true);
        let result = expr.evaluate(&batch)?.into_array(1)?;
        let bools = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(bools.is_null(0));

        // Without three-valued logic: result should be false
        let expr2 = ArrayExistsExpr::new(array_expr, lambda_body, false);
        let result2 = expr2.evaluate(&batch)?.into_array(1)?;
        let bools2 = result2.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(!bools2.is_null(0));
        assert!(!bools2.value(0));
        Ok(())
    }

    #[test]
    fn test_null_elements_with_match() -> DataFusionResult<()> {
        // exists(array(1, null, 3), x -> x > 2) = true (because 3 > 2)
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
            Some(1),
            None,
            Some(3),
        ])]);

        let schema = Arc::new(Schema::new(vec![Field::new(
            "arr",
            list.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(list)])?;

        let array_expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("arr", 0));
        let lambda_body = make_gt_predicate(2);
        let expr = ArrayExistsExpr::new(array_expr, lambda_body, true);

        let result = expr.evaluate(&batch)?.into_array(1)?;
        let bools = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(!bools.is_null(0));
        assert!(bools.value(0));
        Ok(())
    }

    #[test]
    fn test_multiple_rows() -> DataFusionResult<()> {
        // Row 0: [1, 2, 3] -> x > 2 -> true
        // Row 1: [1, 2]    -> x > 2 -> false
        // Row 2: null       -> null
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2), Some(3)]),
            Some(vec![Some(1), Some(2)]),
            None,
        ]);

        let schema = Arc::new(Schema::new(vec![Field::new(
            "arr",
            list.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(list)])?;

        let array_expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("arr", 0));
        let lambda_body = make_gt_predicate(2);
        let expr = ArrayExistsExpr::new(array_expr, lambda_body, true);

        let result = expr.evaluate(&batch)?.into_array(3)?;
        let bools = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(bools.value(0));
        assert!(!bools.value(1));
        assert!(bools.is_null(2));
        Ok(())
    }
}
