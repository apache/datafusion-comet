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

use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::sync::Arc;

use arrow::array::new_empty_array;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_comet_common::SparkError;

/// Spark's runtime nullability check. Only the value itself is checked, not its nested fields.
#[derive(Debug, Eq)]
pub struct AssertNotNull {
    child: Arc<dyn PhysicalExpr>,
    walked_type_path: String,
}

impl AssertNotNull {
    pub fn new(child: Arc<dyn PhysicalExpr>, walked_type_path: String) -> Self {
        Self {
            child,
            walked_type_path,
        }
    }
}

impl PartialEq for AssertNotNull {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child) && self.walked_type_path == other.walked_type_path
    }
}

impl Hash for AssertNotNull {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.child.hash(state);
        self.walked_type_path.hash(state);
    }
}

impl Display for AssertNotNull {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "AssertNotNull({}, {:?})",
            self.child, self.walked_type_path
        )
    }
}

impl PhysicalExpr for AssertNotNull {
    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.child.data_type(input_schema)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        // There is no row to check, including when a conditional filters out every row.
        if batch.num_rows() == 0 {
            return Ok(ColumnarValue::Array(new_empty_array(
                &self.data_type(batch.schema().as_ref())?,
            )));
        }
        let value = self.child.evaluate(batch)?;
        let has_null = match &value {
            // A dictionary key can be non-null but refer to a null value. Conversely, an
            // unused null dictionary entry does not make any row null.
            ColumnarValue::Array(array) => array.logical_null_count() > 0,
            ColumnarValue::Scalar(scalar) => scalar.is_null(),
        };
        if has_null {
            return Err(SparkError::NotNullAssertViolation {
                walked_type_path: self.walked_type_path.clone(),
            }
            .into());
        }
        Ok(value)
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            self.walked_type_path.clone(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, DictionaryArray, Int32Array, StringArray, StructArray};
    use arrow::buffer::NullBuffer;
    use arrow::datatypes::{Field, Int32Type};
    use datafusion::common::ScalarValue;
    use datafusion::physical_expr::expressions::{Column, Literal};

    fn evaluate(array: ArrayRef) -> Result<ColumnarValue> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            array.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![array])?;
        AssertNotNull::new(Arc::new(Column::new("a", 0)), "\ns\na\n".into()).evaluate(&batch)
    }

    #[test]
    fn array_values_and_slices() -> Result<()> {
        let array = Arc::new(Int32Array::from(vec![None, Some(1), Some(2)])) as ArrayRef;
        assert!(evaluate(Arc::clone(&array)).is_err());
        let slice = array.slice(1, 2);
        let ColumnarValue::Array(result) = evaluate(Arc::clone(&slice))? else {
            panic!("expected array")
        };
        assert!(Arc::ptr_eq(&slice, &result));
        assert_eq!(evaluate(array.slice(0, 0))?.into_array(0)?.len(), 0);
        Ok(())
    }

    #[test]
    fn dictionary_logical_nulls() -> Result<()> {
        let values = Arc::new(StringArray::from(vec![Some("value"), None])) as ArrayRef;
        for (keys, should_fail) in [
            (vec![Some(0), Some(1)], true),
            (vec![Some(0), None], true),
            (vec![Some(0), Some(0)], false),
        ] {
            let array =
                DictionaryArray::<Int32Type>::try_new(Int32Array::from(keys), Arc::clone(&values))?;
            assert_eq!(evaluate(Arc::new(array)).is_err(), should_fail);
        }
        Ok(())
    }

    #[test]
    fn checks_struct_validity_not_nullable_fields() -> Result<()> {
        let fields = vec![Arc::new(Field::new("a", DataType::Int32, true))];
        let values = Arc::new(Int32Array::from(vec![None, Some(1)])) as ArrayRef;
        evaluate(Arc::new(StructArray::new(
            fields.clone().into(),
            vec![Arc::clone(&values)],
            None,
        )))?;
        assert!(evaluate(Arc::new(StructArray::new(
            fields.into(),
            vec![values],
            Some(NullBuffer::from(vec![true, false]))
        )))
        .is_err());
        Ok(())
    }

    #[test]
    fn scalars_empty_batches_and_error_path() -> Result<()> {
        let batch = RecordBatch::try_from_iter(vec![(
            "a",
            Arc::new(Int32Array::from(vec![1])) as ArrayRef,
        )])?;
        let expr = AssertNotNull::new(
            Arc::new(Literal::new(ScalarValue::Int32(None))),
            "\ns\na\n".into(),
        );
        assert!(!expr.nullable(batch.schema().as_ref())?);
        assert_eq!(expr.data_type(batch.schema().as_ref())?, DataType::Int32);
        let error = expr.evaluate(&batch).unwrap_err();
        let datafusion::common::DataFusionError::External(error) = error else {
            panic!("expected Spark error")
        };
        assert!(
            matches!(error.downcast_ref::<SparkError>(), Some(SparkError::NotNullAssertViolation { walked_type_path }) if walked_type_path == "\ns\na\n")
        );
        assert_eq!(expr.evaluate(&batch.slice(0, 0))?.into_array(0)?.len(), 0);
        let expr = Arc::new(expr)
            .with_new_children(vec![Arc::new(Literal::new(ScalarValue::Int32(Some(7))))])?;
        assert_eq!(
            expr.evaluate(&batch)?.into_array(1)?.as_ref(),
            &Int32Array::from(vec![7])
        );
        Ok(())
    }
}
