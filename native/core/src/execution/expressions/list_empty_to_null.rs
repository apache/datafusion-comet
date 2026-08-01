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

use arrow::array::{Array, ArrayRef, ListArray, RecordBatch};
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::datatypes::{DataType, Field, FieldRef, Schema};
use datafusion::common::{exec_err, Result as DataFusionResult};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ColumnarValue;

/// A `PhysicalExpr` that marks every empty row of a `List<T>` input as null.
/// Bridges DataFusion's `UnnestExec` (which drops empty rows under
/// `preserve_nulls=true`) to Spark's `explode_outer`/`posexplode_outer`
/// semantics. See <https://github.com/apache/datafusion/issues/19053>.
#[derive(Debug, Clone)]
pub struct ListEmptyToNullExpr {
    child: Arc<dyn PhysicalExpr>,
}

impl ListEmptyToNullExpr {
    pub fn new(child: Arc<dyn PhysicalExpr>) -> Self {
        Self { child }
    }
}

impl Display for ListEmptyToNullExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "list_empty_to_null({})", self.child)
    }
}

impl PartialEq for ListEmptyToNullExpr {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child)
    }
}

impl Eq for ListEmptyToNullExpr {}

impl Hash for ListEmptyToNullExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.child.hash(state);
    }
}

impl PhysicalExpr for ListEmptyToNullExpr {
    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }

    fn return_field(&self, input_schema: &Schema) -> DataFusionResult<FieldRef> {
        // Preserve the child field's name and element type; force the outer
        // list to nullable because we mark empty rows as null.
        let child_field = self.child.return_field(input_schema)?;
        Ok(Arc::new(Field::new(
            child_field.name(),
            child_field.data_type().clone(),
            true,
        )))
    }

    fn evaluate(&self, batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        let value = self.child.evaluate(batch)?;
        let array = value.into_array(batch.num_rows())?;

        let Some(list) = array.as_any().downcast_ref::<ListArray>() else {
            return exec_err!(
                "ListEmptyToNullExpr expected List input, got {}",
                array.data_type()
            );
        };

        let offsets = list.offsets();
        let len = list.len();
        let existing_nulls = list.nulls();

        // Fast path: no currently-valid row is empty, so the input already
        // satisfies outer semantics. `is_valid` returns true when `nulls` is
        // `None`, so this single scan short-circuits on the first empty
        // valid row without allocating.
        let has_valid_empty = (0..len)
            .any(|i| offsets[i + 1] == offsets[i] && existing_nulls.is_none_or(|n| n.is_valid(i)));
        if !has_valid_empty {
            return Ok(ColumnarValue::Array(Arc::clone(&array)));
        }

        let non_empty = BooleanBuffer::collect_bool(len, |i| offsets[i + 1] > offsets[i]);
        let new_nulls = match existing_nulls {
            None => NullBuffer::new(non_empty),
            Some(existing) => {
                let combined = existing.inner() & &non_empty;
                let null_count = len - combined.count_set_bits();
                // SAFETY: null_count was just derived as len - popcount(combined).
                unsafe { NullBuffer::new_unchecked(combined, null_count) }
            }
        };

        let DataType::List(element_field) = list.data_type() else {
            unreachable!("ListArray downcast guarantees DataType::List");
        };

        let result = ListArray::try_new(
            Arc::clone(element_field),
            offsets.clone(),
            Arc::clone(list.values()),
            Some(new_nulls),
        )?;

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
                "ListEmptyToNullExpr expects exactly 1 child, got {}",
                children.len()
            );
        }
        Ok(Arc::new(ListEmptyToNullExpr::new(Arc::clone(&children[0]))))
    }
}
