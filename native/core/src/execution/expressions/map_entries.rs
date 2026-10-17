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

use arrow::array::{Array, ListArray, MapArray, RecordBatch};
use arrow::datatypes::{DataType, FieldRef, Schema};
use datafusion::common::{exec_err, Result as DataFusionResult};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ColumnarValue;

/// Exposes a map's entries as a list for `ExplodeExec`, sharing the input buffers.
/// Preserve the original entry fields, including nullability and metadata; the
/// SQL `map_entries` function rebuilds those fields and loses their metadata.
#[derive(Debug, Clone)]
pub struct MapEntriesExpr {
    child: Arc<dyn PhysicalExpr>,
}

impl MapEntriesExpr {
    pub fn new(child: Arc<dyn PhysicalExpr>) -> Self {
        Self { child }
    }
}

impl Display for MapEntriesExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "map_entries({})", self.child)
    }
}

impl PartialEq for MapEntriesExpr {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child)
    }
}

impl Eq for MapEntriesExpr {}

impl Hash for MapEntriesExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.child.hash(state);
    }
}

impl PhysicalExpr for MapEntriesExpr {
    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }

    fn return_field(&self, input_schema: &Schema) -> DataFusionResult<FieldRef> {
        let field = self.child.return_field(input_schema)?;
        let DataType::Map(entries, _) = field.data_type() else {
            return exec_err!(
                "MapEntriesExpr expected Map input, got {}",
                field.data_type()
            );
        };
        Ok(Arc::new(
            field
                .as_ref()
                .clone()
                .with_data_type(DataType::List(Arc::clone(entries))),
        ))
    }

    fn evaluate(&self, batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        let array = self.child.evaluate(batch)?.into_array(batch.num_rows())?;
        let Some(map) = array.as_any().downcast_ref::<MapArray>() else {
            return exec_err!(
                "MapEntriesExpr expected Map input, got {}",
                array.data_type()
            );
        };
        let DataType::Map(entries, _) = map.data_type() else {
            unreachable!("MapArray downcast guarantees DataType::Map");
        };
        let list = ListArray::try_new(
            Arc::clone(entries),
            map.offsets().clone(),
            Arc::new(map.entries().clone()),
            map.nulls().cloned(),
        )?;
        Ok(ColumnarValue::Array(Arc::new(list)))
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
                "MapEntriesExpr expects exactly 1 child, got {}",
                children.len()
            );
        }
        Ok(Arc::new(Self::new(Arc::clone(&children[0]))))
    }
}
