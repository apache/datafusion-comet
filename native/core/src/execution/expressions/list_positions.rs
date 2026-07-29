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
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int32Array, ListArray, RecordBatch};
use arrow::datatypes::{DataType, Field, FieldRef, Schema};
use datafusion::common::{exec_err, Result as DataFusionResult};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ColumnarValue;

/// A `PhysicalExpr` that takes a `List<T>` input and produces a `List<Int32>` where each row's
/// values are `[0, 1, ..., len - 1]`. Offsets and the null bitmap are inherited from the input,
/// so when the resulting list is unnested in parallel with the original list it produces the
/// `pos` column expected by Spark's `posexplode`.
#[derive(Debug, Clone)]
pub struct ListPositionsExpr {
    child: Arc<dyn PhysicalExpr>,
    field: FieldRef,
}

impl ListPositionsExpr {
    pub fn new(child: Arc<dyn PhysicalExpr>) -> Self {
        let field = Arc::new(Field::new(
            "item",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        ));
        Self { child, field }
    }
}

impl Display for ListPositionsExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "list_positions({})", self.child)
    }
}

impl PartialEq for ListPositionsExpr {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child)
    }
}

impl Eq for ListPositionsExpr {}

impl Hash for ListPositionsExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.child.hash(state);
    }
}

impl PhysicalExpr for ListPositionsExpr {
    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }

    fn data_type(&self, _input_schema: &Schema) -> DataFusionResult<DataType> {
        Ok(self.field.data_type().clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> DataFusionResult<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        let value = self.child.evaluate(batch)?;
        let array = value.into_array(batch.num_rows())?;

        let list = match array.as_any().downcast_ref::<ListArray>() {
            Some(list) => list,
            None => {
                return exec_err!(
                    "ListPositionsExpr expected List input, got {}",
                    array.data_type()
                );
            }
        };

        let offsets = list.offsets();
        let total_len = *offsets.last().unwrap() as usize;

        let mut values: Vec<i32> = Vec::with_capacity(total_len);
        for window in offsets.windows(2) {
            let start = window[0];
            let end = window[1];
            for i in 0..(end - start) {
                values.push(i);
            }
        }

        let element_field = Arc::new(Field::new("item", DataType::Int32, true));
        let result = ListArray::new(
            element_field,
            offsets.clone(),
            Arc::new(Int32Array::from(values)),
            list.nulls().cloned(),
        );

        Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        if children.len() != 1 {
            return exec_err!(
                "ListPositionsExpr expects exactly 1 child, got {}",
                children.len()
            );
        }
        Ok(Arc::new(ListPositionsExpr::new(Arc::clone(&children[0]))))
    }
}
