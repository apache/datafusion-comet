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

/// A bit-for-bit port of Apache Commons Math3's `MersenneTwister`, the RNG Spark
/// uses to shuffle arrays. Spark seeds a fresh generator per partition with
/// `randomSeed + partitionIndex` and drives the "inside-out" Fisher-Yates
/// algorithm from `org.apache.spark.sql.catalyst.util.RandomIndicesGenerator`.
///
/// See:
/// - `org/apache/commons/math3/random/MersenneTwister.java`
/// - `org/apache/commons/math3/random/BitsStreamGenerator.java` (`nextInt(int)`)
#[derive(Debug, Clone)]
pub(crate) struct SparkMersenneTwister {
    /// Bytes pool.
    mt: [i32; Self::N],
    /// Current index in the bytes pool.
    mti: usize,
}

impl SparkMersenneTwister {
    /// Size of the bytes pool.
    const N: usize = 624;
    /// Period second parameter.
    const M: usize = 397;
    /// X * MATRIX_A for X = {0, 1}.
    const MAG01: [i32; 2] = [0x0, 0x9908b0dfu32 as i32];

    pub(crate) fn new(seed: i64) -> Self {
        // `mti` is set by seeding before it is ever read; 0 is just a placeholder.
        let mut twister = SparkMersenneTwister {
            mt: [0i32; Self::N],
            mti: 0,
        };
        twister.set_seed_long(seed);
        twister
    }

    fn set_seed_int(&mut self, seed: i32) {
        // We use a long masked by 0xffffffff as a poor man's unsigned int.
        let mut long_mt = seed as i64;
        self.mt[0] = long_mt as i32;
        let mut mti = 1usize;
        while mti < Self::N {
            long_mt = (1812433253i64.wrapping_mul(long_mt ^ (long_mt >> 30)) + mti as i64)
                & 0xffffffffi64;
            self.mt[mti] = long_mt as i32;
            mti += 1;
        }
        self.mti = mti;
    }

    fn set_seed_int_array(&mut self, seed: &[i32]) {
        self.set_seed_int(19650218);
        let mut i = 1usize;
        let mut j = 0usize;

        for _ in 0..Self::N.max(seed.len()) {
            let mt_i = self.mt[i] as i64 & 0xffffffffi64;
            let mt_im1 = self.mt[i - 1] as i64 & 0xffffffffi64;
            let l = (mt_i ^ ((mt_im1 ^ (mt_im1 >> 30)).wrapping_mul(1664525)))
                .wrapping_add(seed[j] as i64)
                .wrapping_add(j as i64);
            self.mt[i] = (l & 0xffffffffi64) as i32;
            i += 1;
            j += 1;
            if i >= Self::N {
                self.mt[0] = self.mt[Self::N - 1];
                i = 1;
            }
            if j >= seed.len() {
                j = 0;
            }
        }

        for _ in 0..(Self::N - 1) {
            let mt_i = self.mt[i] as i64 & 0xffffffffi64;
            let mt_im1 = self.mt[i - 1] as i64 & 0xffffffffi64;
            let l = (mt_i ^ ((mt_im1 ^ (mt_im1 >> 30)).wrapping_mul(1566083941)))
                .wrapping_sub(i as i64);
            self.mt[i] = (l & 0xffffffffi64) as i32;
            i += 1;
            if i >= Self::N {
                self.mt[0] = self.mt[Self::N - 1];
                i = 1;
            }
        }

        // MSB is 1, assuring a non-zero initial array.
        self.mt[0] = 0x80000000u32 as i32;
    }

    fn set_seed_long(&mut self, seed: i64) {
        self.set_seed_int_array(&[(seed >> 32) as i32, seed as i32]);
    }

    fn next(&mut self, bits: u32) -> i32 {
        let mut y: i32;
        if self.mti >= Self::N {
            // Generate N words at one time.
            let mut mt_next = self.mt[0];
            for k in 0..(Self::N - Self::M) {
                let mt_curr = mt_next;
                mt_next = self.mt[k + 1];
                y = (mt_curr & (0x80000000u32 as i32)) | (mt_next & 0x7fffffff);
                self.mt[k] = self.mt[k + Self::M]
                    ^ (((y as u32) >> 1) as i32)
                    ^ Self::MAG01[(y & 0x1) as usize];
            }
            for k in (Self::N - Self::M)..(Self::N - 1) {
                let mt_curr = mt_next;
                mt_next = self.mt[k + 1];
                y = (mt_curr & (0x80000000u32 as i32)) | (mt_next & 0x7fffffff);
                self.mt[k] = self.mt[k + Self::M - Self::N]
                    ^ (((y as u32) >> 1) as i32)
                    ^ Self::MAG01[(y & 0x1) as usize];
            }
            y = (mt_next & (0x80000000u32 as i32)) | (self.mt[0] & 0x7fffffff);
            self.mt[Self::N - 1] =
                self.mt[Self::M - 1] ^ (((y as u32) >> 1) as i32) ^ Self::MAG01[(y & 0x1) as usize];
            self.mti = 0;
        }

        y = self.mt[self.mti];
        self.mti += 1;

        // Tempering.
        y ^= ((y as u32) >> 11) as i32;
        y ^= (y << 7) & (0x9d2c5680u32 as i32);
        y ^= (y << 15) & (0xefc60000u32 as i32);
        y ^= ((y as u32) >> 18) as i32;

        ((y as u32) >> (32 - bits)) as i32
    }

    /// Port of `BitsStreamGenerator.nextInt(int n)`. The caller always passes a
    /// strictly positive `n`, matching Spark's `random.nextInt(i + 1)`.
    fn next_int(&mut self, n: i32) -> i32 {
        if (n & n.wrapping_neg()) == n {
            // n is a power of two.
            return ((n as i64 * self.next(31) as i64) >> 31) as i32;
        }
        loop {
            let bits = self.next(31);
            let val = bits % n;
            if bits.wrapping_sub(val).wrapping_add(n.wrapping_sub(1)) >= 0 {
                return val;
            }
        }
    }

    /// Port of `RandomIndicesGenerator.getNextIndices`. Fills `out` with, for each
    /// output position, the source position it should draw from. `out` is reused
    /// across rows to avoid a per-row allocation. Advances the RNG state, which is
    /// shared across every row in the partition.
    fn next_indices_into(&mut self, length: usize, out: &mut Vec<usize>) {
        out.clear();
        out.resize(length, 0);
        let mut i = 0usize;
        while i < length {
            let j = self.next_int((i + 1) as i32) as usize;
            if j != i {
                out[i] = out[j];
            }
            out[j] = i;
            i += 1;
        }
    }

    #[cfg(test)]
    fn next_indices(&mut self, length: usize) -> Vec<usize> {
        let mut out = Vec::new();
        self.next_indices_into(length, &mut out);
        out
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
            DataType::Null => Arc::clone(&input),
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
/// from the RNG, matching Spark. The scratch buffer is reused across rows.
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
            rng.next_indices_into(length, &mut scratch);
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
        lengths.iter().map(|&len| rng.next_indices(len)).collect()
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
