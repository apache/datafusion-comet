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

use arrow::array::StructArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::{internal_err, Result as DataFusionResult, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use std::{
    fmt::{Display, Formatter},
    hash::Hash,
    sync::Arc,
};

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct CreateNamedStruct {
    values: Vec<Arc<dyn PhysicalExpr>>,
    names: Vec<String>,
    // Catalyst's declared field nullability is part of the nested Arrow type. Conservative
    // native inference can make equivalent constructors disagree, e.g. in array_union.
    field_nullable: Vec<bool>,
}

impl CreateNamedStruct {
    /// Constructs a struct with one Catalyst name and nullability flag per child.
    /// Rejects mismatched metadata rather than silently dropping fields during evaluation.
    pub fn try_new(
        values: Vec<Arc<dyn PhysicalExpr>>,
        names: Vec<String>,
        field_nullable: Vec<bool>,
    ) -> DataFusionResult<Self> {
        if values.len() != names.len() || values.len() != field_nullable.len() {
            return internal_err!(
                "CreateNamedStruct requires one name and nullability flag per value"
            );
        }
        Ok(Self {
            values,
            names,
            field_nullable,
        })
    }

    fn fields(&self, schema: &Schema) -> DataFusionResult<Vec<Field>> {
        self.values
            .iter()
            .zip(&self.names)
            .zip(&self.field_nullable)
            .map(|((expr, name), nullable)| {
                // Keep physical representations such as dictionary-encoded children intact.
                let data_type = expr.data_type(schema)?;
                Ok(Field::new(name, data_type, *nullable))
            })
            .collect()
    }
}

impl PhysicalExpr for CreateNamedStruct {
    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }

    fn data_type(&self, input_schema: &Schema) -> DataFusionResult<DataType> {
        let fields = self.fields(input_schema)?;
        Ok(DataType::Struct(fields.into()))
    }

    fn nullable(&self, _input_schema: &Schema) -> DataFusionResult<bool> {
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        let values = self
            .values
            .iter()
            .map(|expr| expr.evaluate(batch))
            .collect::<datafusion::common::Result<Vec<_>>>()?;
        // When every field value is a scalar (e.g. an all-literal `named_struct` that reaches native
        // as a `CreateNamedStruct` because constant folding is disabled), return a scalar struct
        // rather than a length-1 `StructArray`. A downstream consumer such as `make_array` then
        // broadcasts the constant struct to the batch row count instead of failing with a
        // mixed-length error when a sibling child is a full-length column. This matches what a
        // constant-folded struct literal produces, and `GetStructField::evaluate` already handles a
        // scalar struct input.
        let all_scalar =
            !values.is_empty() && values.iter().all(|v| matches!(v, ColumnarValue::Scalar(_)));
        let arrays = ColumnarValue::values_to_arrays(&values)?;
        let fields = self.fields(&batch.schema())?;
        let struct_array = StructArray::try_new(fields.into(), arrays, None)?;
        if all_scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::Struct(Arc::new(
                struct_array,
            ))))
        } else {
            Ok(ColumnarValue::Array(Arc::new(struct_array)))
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.values.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::common::Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(CreateNamedStruct::try_new(
            children,
            self.names.clone(),
            self.field_nullable.clone(),
        )?))
    }
}

impl Display for CreateNamedStruct {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CreateNamedStruct [values: {:?}, names: {:?}]",
            self.values, self.names
        )
    }
}

#[cfg(test)]
mod test {
    use super::CreateNamedStruct;
    use crate::{Cast, EvalMode, SparkCastOptions};
    use arrow::array::{
        Array, Decimal128Array, DictionaryArray, Float64Array, Int32Array, Int64Array, RecordBatch,
        StringArray, StructArray,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::{Result, ScalarValue};
    use datafusion::physical_expr::expressions::{Column, Literal};
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_plan::ColumnarValue;
    use std::sync::Arc;

    #[test]
    fn test_create_struct_preserves_catalyst_nullability() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from_iter_values(0..8))],
        )?;
        let values: Vec<Arc<dyn PhysicalExpr>> = [DataType::Float64, DataType::Decimal128(18, 2)]
            .into_iter()
            .map(|data_type| {
                Arc::new(Cast::new(
                    Arc::new(Column::new("id", 0)),
                    data_type,
                    SparkCastOptions::new_without_timezone(EvalMode::Legacy, false),
                    None,
                    None,
                )) as Arc<dyn PhysicalExpr>
            })
            .collect();
        // Comet's native cast is conservative, but Catalyst proves BIGINT -> DOUBLE nonnullable.
        assert!(values[0].nullable(&schema)?);
        let expr = Arc::new(CreateNamedStruct::try_new(
            values.clone(),
            vec!["score".to_string(), "amount".to_string()],
            vec![false, true],
        )?);
        let expected = DataType::Struct(
            vec![
                Field::new("score", DataType::Float64, false),
                Field::new("amount", DataType::Decimal128(18, 2), true),
            ]
            .into(),
        );
        let rewritten = Arc::clone(&expr).with_new_children(values)?;
        for expr in [expr as Arc<dyn PhysicalExpr>, rewritten] {
            assert_eq!(expr.data_type(&schema)?, expected);
            let value = expr.evaluate(&batch)?.into_array(batch.num_rows())?;
            assert_eq!(value.data_type(), &expected);
            let value = value.as_any().downcast_ref::<StructArray>().unwrap();
            assert_eq!(value.len(), 8);
            assert_eq!(value.null_count(), 0);
            let score = value
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            let amount = value
                .column(1)
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap();
            for row in 0..8 {
                assert_eq!(score.value(row), row as f64);
                assert_eq!(amount.value(row), row as i128 * 100);
            }
        }
        Ok(())
    }

    #[test]
    fn test_create_struct_rejects_inconsistent_field_metadata() {
        let value: Arc<dyn PhysicalExpr> = Arc::new(Literal::new(ScalarValue::Int32(Some(1))));
        assert!(CreateNamedStruct::try_new(vec![Arc::clone(&value)], vec![], vec![false]).is_err());
        assert!(CreateNamedStruct::try_new(vec![value], vec!["a".to_string()], vec![]).is_err());
    }

    #[test]
    fn test_create_struct_from_dict_encoded_i32() -> Result<()> {
        let keys = Int32Array::from(vec![0, 1, 2]);
        let values = Int32Array::from(vec![0, 111, 233]);
        let dict = DictionaryArray::try_new(keys, Arc::new(values))?;
        let data_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int32));
        let schema = Schema::new(vec![Field::new("a", data_type, false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(dict)])?;
        let field_names = vec!["a".to_string()];
        let x = CreateNamedStruct::try_new(
            vec![Arc::new(Column::new("a", 0))],
            field_names,
            vec![false],
        )?;
        let ColumnarValue::Array(x) = x.evaluate(&batch)? else {
            unreachable!()
        };
        assert_eq!(3, x.len());
        Ok(())
    }

    #[test]
    fn test_create_struct_from_dict_encoded_string() -> Result<()> {
        let keys = Int32Array::from(vec![0, 1, 2]);
        let values = StringArray::from(vec!["a".to_string(), "b".to_string(), "c".to_string()]);
        let dict = DictionaryArray::try_new(keys, Arc::new(values))?;
        let data_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let schema = Schema::new(vec![Field::new("a", data_type, false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(dict)])?;
        let field_names = vec!["a".to_string()];
        let x = CreateNamedStruct::try_new(
            vec![Arc::new(Column::new("a", 0))],
            field_names,
            vec![false],
        )?;
        let ColumnarValue::Array(x) = x.evaluate(&batch)? else {
            unreachable!()
        };
        assert_eq!(3, x.len());
        Ok(())
    }
}
