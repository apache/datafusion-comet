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

use arrow::array::RecordBatch;
use arrow::array::{Array, ArrayRef, StringArray};
use arrow::datatypes::DataType::{FixedSizeList, LargeList, List, Null};
use arrow::datatypes::Schema;
use arrow::datatypes::{DataType, Field, Fields};
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::functions_nested::arrays_zip::arrays_zip_inner;
use std::any::Any;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Debug, Eq, Hash, PartialEq)]
pub struct SparkArraysZipFunc {
    values: Vec<Arc<dyn PhysicalExpr>>,
    names: Vec<String>,
}

impl SparkArraysZipFunc {
    pub fn new(values: Vec<Arc<dyn PhysicalExpr>>, names: Vec<String>) -> Self {
        Self { values, names }
    }
    fn fields(&self, schema: &Schema) -> Result<Vec<Field>> {
        let mut fields: Vec<Field> = Vec::with_capacity(self.values.len());
        for (i, v) in self.values.iter().enumerate() {
            let element_type = match (*v).as_ref().data_type(schema)? {
                List(field) | LargeList(field) | FixedSizeList(field, _) => {
                    field.data_type().clone()
                }
                Null => Null,
                dt => {
                    return exec_err!("arrays_zip expects array arguments, got {dt}");
                }
            };
            fields.push(Field::new(format!("{}", self.names[i]), element_type, true));
        }

        Ok(fields)
    }
}

impl Display for SparkArraysZipFunc {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ArraysZip [values: {:?}, names: {:?}]",
            self.values, self.names
        )
    }
}

impl PhysicalExpr for SparkArraysZipFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        let fields = self.fields(input_schema)?;
        Ok(List(Arc::new(Field::new_list_field(
            DataType::Struct(Fields::from(fields)),
            true,
        ))))
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let values = self
            .values
            .iter()
            .map(|e| e.evaluate(batch))
            .collect::<datafusion::common::Result<Vec<_>>>()?;

        let len = values
            .iter()
            .fold(Option::<usize>::None, |acc, arg| match arg {
                ColumnarValue::Scalar(_) => acc,
                ColumnarValue::Array(a) => Some(a.len()),
            });

        let is_scalar = len.is_none();

        let arrays = ColumnarValue::values_to_arrays(&values)?;
        let names = vec![Arc::new(StringArray::from(self.names.clone())) as ArrayRef];

        // TODO: replace this with DF's function
        let result = arrays_zip_inner(&arrays, &names);

        if is_scalar {
            // If all inputs are scalar, keeps output as scalar
            let result = result.and_then(|arr| ScalarValue::try_from_array(&arr, 0));
            result.map(ColumnarValue::Scalar)
        } else {
            result.map(ColumnarValue::Array)
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.values.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(SparkArraysZipFunc::new(
            children.clone(),
            self.names.clone(),
        )))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}
