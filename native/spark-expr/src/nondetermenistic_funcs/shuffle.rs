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

use crate::nondetermenistic_funcs::internal::mersenne::SparkMersenneTwister;
use arrow::array::{
    Array, ArrayRef, FixedSizeListArray, GenericListArray, OffsetSizeTrait, RecordBatch,
    UInt64Array,
};
use arrow::buffer::OffsetBuffer;
use arrow::compute::take;
use arrow::datatypes::{DataType, FieldRef, Schema};
use datafusion::common::{exec_err, Result};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

/// Port of `RandomIndicesGenerator.getNextIndices`. Fills `out` with, for each
/// output position, the source position it should draw from. `out` is reused
/// across rows to avoid a per-row allocation. Advances the PRNG state (a Commons
/// Math3 `MersenneTwister`), which is shared across every row in the partition.
fn next_indices_into(rng: &mut SparkMersenneTwister, length: usize, out: &mut Vec<usize>) {
    out.clear();
    out.resize(length, 0);
    let mut i = 0usize;
    while i < length {
        let j = rng.next_int((i + 1) as i32) as usize;
        if j != i {
            out[i] = out[j];
        }
        out[j] = i;
        i += 1;
    }
}

/// Physical expression for Spark's `shuffle`. Like `RandExpr`, the generator
/// state is kept in a `Mutex` so that it advances continuously across every
/// batch in a partition, matching Spark's stateful per-partition evaluation.
#[derive(Debug)]
pub struct ShuffleExpr {
    child: Arc<dyn PhysicalExpr>,
    /// Random seed already combined with the partition index by the planner.
    seed: i64,
    state_holder: Arc<Mutex<Option<SparkMersenneTwister>>>,
}

impl ShuffleExpr {
    pub fn new(child: Arc<dyn PhysicalExpr>, seed: i64) -> Self {
        Self {
            child,
            seed,
            state_holder: Arc::new(Mutex::new(None)),
        }
    }
}

impl Display for ShuffleExpr {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Shuffle({}, {})", self.child, self.seed)
    }
}

impl PartialEq for ShuffleExpr {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child) && self.seed.eq(&other.seed)
    }
}

impl Eq for ShuffleExpr {}

impl Hash for ShuffleExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.child.hash(state);
        self.seed.hash(state);
    }
}

impl PhysicalExpr for ShuffleExpr {
    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.child.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.child.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let input = self.child.evaluate(batch)?.into_array(batch.num_rows())?;

        let mut state = self.state_holder.lock().unwrap();
        let rng = state.get_or_insert_with(|| SparkMersenneTwister::new(self.seed));

        let result = match input.data_type() {
            DataType::List(field) => shuffle_generic_list::<i32>(input.as_ref(), field, rng)?,
            DataType::LargeList(field) => shuffle_generic_list::<i64>(input.as_ref(), field, rng)?,
            DataType::FixedSizeList(field, value_length) => {
                shuffle_fixed_size_list(input.as_ref(), field, *value_length, rng)?
            }
            other => {
                return exec_err!("shuffle does not support type '{other}'");
            }
        };

        Ok(ColumnarValue::Array(result))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(ShuffleExpr::new(
            Arc::clone(&children[0]),
            self.seed,
        )))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

/// Gather, for every row in order, the absolute source indices of that row's
/// elements in shuffled order. `span(row)` returns the row's `(start, length)`
/// within the values array. Null rows are copied through as-is without drawing
/// from the PRNG, matching Spark. The scratch buffer is reused across rows.
fn gather_shuffled_indices(
    rng: &mut SparkMersenneTwister,
    num_rows: usize,
    total_values: usize,
    is_null: impl Fn(usize) -> bool,
    span: impl Fn(usize) -> (usize, usize),
) -> Vec<u64> {
    let mut gathered: Vec<u64> = Vec::with_capacity(total_values);
    let mut scratch: Vec<usize> = Vec::new();
    for row in 0..num_rows {
        let (start, length) = span(row);
        if is_null(row) {
            gathered.extend((start..start + length).map(|idx| idx as u64));
        } else {
            next_indices_into(rng, length, &mut scratch);
            gathered.extend(scratch.iter().map(|&local| (start + local) as u64));
        }
    }
    gathered
}

fn shuffle_generic_list<O: OffsetSizeTrait>(
    array: &dyn Array,
    field: &FieldRef,
    rng: &mut SparkMersenneTwister,
) -> Result<ArrayRef> {
    let list = array
        .as_any()
        .downcast_ref::<GenericListArray<O>>()
        .expect("expected a list array");
    let values = list.values();
    let offsets = list.offsets();

    let gathered = gather_shuffled_indices(
        rng,
        list.len(),
        values.len(),
        |row| list.is_null(row),
        |row| {
            let start = offsets[row].as_usize();
            (start, offsets[row + 1].as_usize() - start)
        },
    );

    let indices = UInt64Array::from(gathered);
    let new_values = take(values.as_ref(), &indices, None)?;

    // Shuffling preserves row lengths, so the new offsets are the input offsets
    // rebased to start at zero. This also normalizes any slice offset on the input.
    let base = offsets[0].as_usize();
    let new_offsets: Vec<O> = offsets
        .iter()
        .map(|o| O::usize_as(o.as_usize() - base))
        .collect();

    Ok(Arc::new(GenericListArray::<O>::try_new(
        Arc::clone(field),
        OffsetBuffer::new(new_offsets.into()),
        new_values,
        list.nulls().cloned(),
    )?))
}

fn shuffle_fixed_size_list(
    array: &dyn Array,
    field: &FieldRef,
    value_length: i32,
    rng: &mut SparkMersenneTwister,
) -> Result<ArrayRef> {
    let list = array
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .expect("expected a fixed size list array");
    let values = list.values();
    let length = value_length as usize;

    let gathered = gather_shuffled_indices(
        rng,
        list.len(),
        values.len(),
        |row| list.is_null(row),
        |row| (row * length, length),
    );

    let indices = UInt64Array::from(gathered);
    let new_values = take(values.as_ref(), &indices, None)?;

    Ok(Arc::new(FixedSizeListArray::try_new(
        Arc::clone(field),
        value_length,
        new_values,
        list.nulls().cloned(),
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Golden vectors generated from Apache Commons Math3 `MersenneTwister`
    /// driving Spark's `RandomIndicesGenerator.getNextIndices`.
    fn indices(seed: i64, lengths: &[usize]) -> Vec<Vec<usize>> {
        let mut rng = SparkMersenneTwister::new(seed);
        lengths
            .iter()
            .map(|&len| {
                let mut out = Vec::new();
                next_indices_into(&mut rng, len, &mut out);
                out
            })
            .collect()
    }

    #[test]
    fn test_single_row_permutations() {
        assert_eq!(indices(42, &[5]), vec![vec![4, 2, 1, 0, 3]]);
        assert_eq!(indices(42, &[10]), vec![vec![5, 7, 1, 0, 3, 6, 8, 2, 4, 9]]);
        assert_eq!(indices(0, &[5]), vec![vec![3, 0, 1, 2, 4]]);
        assert_eq!(indices(123456789, &[8]), vec![vec![2, 6, 1, 7, 4, 0, 5, 3]]);
    }

    #[test]
    fn test_state_advances_across_rows() {
        // One generator shared by consecutive rows, matching Spark's per-partition state.
        assert_eq!(
            indices(42, &[3, 3, 3, 4]),
            vec![
                vec![0, 2, 1],
                vec![2, 1, 0],
                vec![2, 0, 1],
                vec![3, 2, 1, 0]
            ]
        );
    }

    #[test]
    fn test_empty_and_single_element() {
        // A length-0 row draws nothing; a length-1 row still consumes one draw.
        assert_eq!(indices(7, &[0, 1, 2]), vec![vec![], vec![0], vec![1, 0]]);
    }
}
