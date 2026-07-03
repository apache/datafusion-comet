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

//! Spark-compatible `approx_count_distinct`, a faithful port of Spark's
//! `HyperLogLogPlusPlus` / `HyperLogLogPlusPlusHelper`. Values are hashed with Spark's
//! `XxHash64` (seed 42, floats normalized first) and the registers are stored using the exact
//! same packed-`Long` buffer layout Spark uses (10 six-bit registers per 64-bit word). Keeping
//! the wire format identical means the partial-aggregation state matches Spark's
//! `aggBufferSchema`, and the cardinality estimate uses the same bias-correction tables, so
//! results are bit-identical to Spark.

use crate::agg_funcs::hll_plus_plus_const::{BIAS_DATA, RAW_ESTIMATE_DATA, THRESHOLDS};
use crate::hash_funcs::create_xxhash64_hashes;
use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Float32Array, Float64Array, Int64Array,
};
use arrow::datatypes::{DataType, Field, FieldRef, Float32Type, Float64Type};
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, EmitTo, GroupsAccumulator, Signature, Volatility,
};
use std::sync::Arc;

/// Number of interpolation points used when estimating the bias (Spark's `K`).
const K: usize = 6;
/// The seed Spark's `HyperLogLogPlusPlus` uses for `XxHash64`.
const HASH_SEED: u64 = 42;
/// Bits per register (Spark's `REGISTER_SIZE`). A 64-bit hash yields at most 64 leading zeros.
const REGISTER_SIZE: u64 = 6;
/// Registers packed per 64-bit word (Spark's `REGISTERS_PER_WORD`); only 60 of 64 bits are used.
const REGISTERS_PER_WORD: usize = 10;
/// Mask for a single register within a word (Spark's `REGISTER_WORD_MASK`).
const REGISTER_WORD_MASK: u64 = (1 << REGISTER_SIZE) - 1;

/// Number of 64-bit words needed to store all `m = 1 << p` registers, matching Spark's `numWords`.
fn num_words(p: usize) -> usize {
    (1usize << p) / REGISTERS_PER_WORD + 1
}

/// Compute the precision `p` for a given `relativeSD`, matching Spark's
/// `HyperLogLogPlusPlusHelper`. Kept here so the native tests can exercise it, but the
/// serde computes the same value and passes `p` through the protobuf.
pub fn hllpp_precision(relative_sd: f64) -> i32 {
    (2.0 * (1.106 / relative_sd).ln() / 2.0_f64.ln()).ceil() as i32
}

/// Spark-compatible `approx_count_distinct` aggregate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HllPlusPlus {
    name: String,
    signature: Signature,
    /// Number of addressing bits (precision). Determines the register count `m = 1 << p`.
    p: usize,
}

impl std::hash::Hash for HllPlusPlus {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.signature.hash(state);
        self.p.hash(state);
    }
}

impl HllPlusPlus {
    pub fn new(p: i32) -> Self {
        assert!(p >= 4, "HLL++ requires at least 4 bits of precision");
        Self {
            name: "approx_count_distinct".to_string(),
            signature: Signature::any(1, Volatility::Immutable),
            p: p as usize,
        }
    }
}

impl AggregateUDFImpl for HllPlusPlus {
    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(HllPlusPlusAccumulator::new(self.p)))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        // The partial state is the packed register buffer: one `Long` per word, exactly matching
        // the `MS[i]` buffer attributes of Spark's `HyperLogLogPlusPlus`.
        Ok((0..num_words(self.p))
            .map(|i| Arc::new(Field::new(format!("MS[{i}]"), DataType::Int64, false)) as FieldRef)
            .collect())
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(HllPlusPlusGroupsAccumulator::new(self.p)))
    }
}

/// Normalize a float/double column the way Spark's `NormalizeNaNAndZero` does before hashing:
/// every NaN becomes the canonical NaN and `-0.0` becomes `0.0`. Returns the input unchanged for
/// non-floating-point types.
fn normalize_floats(array: &ArrayRef) -> ArrayRef {
    match array.data_type() {
        DataType::Float32 => {
            let normalized: Float32Array = array.as_primitive::<Float32Type>().unary(|v| {
                if v.is_nan() {
                    f32::NAN
                } else {
                    v + 0.0
                }
            });
            Arc::new(normalized)
        }
        DataType::Float64 => {
            let normalized: Float64Array = array.as_primitive::<Float64Type>().unary(|v| {
                if v.is_nan() {
                    f64::NAN
                } else {
                    v + 0.0
                }
            });
            Arc::new(normalized)
        }
        _ => Arc::clone(array),
    }
}

/// Hash a value column with Spark's `XxHash64` (seed 42), normalizing floats first.
fn hash_values(array: &ArrayRef) -> Result<Vec<u64>> {
    let normalized = normalize_floats(array);
    let mut hashes = vec![HASH_SEED; normalized.len()];
    create_xxhash64_hashes(&[normalized], &mut hashes)?;
    Ok(hashes)
}

/// Update the packed register buffer from a hashed value. Mirrors `HyperLogLogPlusPlusHelper.update`.
#[inline]
fn update_word(words: &mut [i64], p: usize, hash: u64) {
    let idx = (hash >> (64 - p)) as usize;
    let w_padding = 1u64 << (p - 1);
    let pw = (((hash << p) | w_padding).leading_zeros() + 1) as u64;

    let word_offset = idx / REGISTERS_PER_WORD;
    let shift = REGISTER_SIZE * (idx - word_offset * REGISTERS_PER_WORD) as u64;
    let word = words[word_offset] as u64;
    let mask = REGISTER_WORD_MASK << shift;
    let m_idx = (word & mask) >> shift;
    if pw > m_idx {
        words[word_offset] = ((word & !mask) | (pw << shift)) as i64;
    }
}

/// Merge `src` packed registers into `dst` by taking the per-register maximum. Mirrors
/// `HyperLogLogPlusPlusHelper.merge`.
#[inline]
fn merge_words(dst: &mut [i64], src: &[i64], m: usize) {
    let mut idx = 0;
    for (d, s) in dst.iter_mut().zip(src.iter()) {
        let word1 = *d as u64;
        let word2 = *s as u64;
        let mut word = 0u64;
        let mut i = 0;
        let mut mask = REGISTER_WORD_MASK;
        while idx < m && i < REGISTERS_PER_WORD {
            word |= (word1 & mask).max(word2 & mask);
            mask <<= REGISTER_SIZE;
            i += 1;
            idx += 1;
        }
        *d = word as i64;
    }
}

/// The precomputed `alpha * m * m` constant, matching `HyperLogLogPlusPlusHelper.alphaM2`.
fn alpha_m2(p: usize, m: f64) -> f64 {
    match p {
        4 => 0.673 * m * m,
        5 => 0.697 * m * m,
        6 => 0.709 * m * m,
        _ => (0.7213 / (1.0 + 1.079 / m)) * m * m,
    }
}

/// Estimate the bias using KNN interpolation over Spark's appendix tables.
fn estimate_bias(p: usize, e: f64) -> f64 {
    let estimates = RAW_ESTIMATE_DATA[p - 4];
    let biases = BIAS_DATA[p - 4];
    let num_estimates = estimates.len();

    // Index of the interpolation estimate closest to `e` (matches Java's Arrays.binarySearch).
    let nearest = match estimates.binary_search_by(|v| v.partial_cmp(&e).unwrap()) {
        Ok(ix) => ix,
        Err(ix) => ix,
    };

    let distance = |i: usize| {
        let diff = e - estimates[i];
        diff * diff
    };

    let mut low = nearest.saturating_sub(K - 1);
    let mut high = (low + K).min(num_estimates);
    while high < num_estimates && distance(high) < distance(low) {
        low += 1;
        high += 1;
    }

    let bias_sum: f64 = biases[low..high].iter().sum();
    bias_sum / (high - low) as f64
}

/// Compute the HyperLogLog++ cardinality estimate from a packed register buffer. Faithful port of
/// `HyperLogLogPlusPlusHelper.query`.
fn query(p: usize, words: &[i64]) -> i64 {
    let m_usize = 1usize << p;
    let m = m_usize as f64;

    let mut z_inverse = 0.0;
    let mut v = 0.0;
    let mut idx = 0;
    for &word in words {
        let word = word as u64;
        let mut i = 0;
        let mut shift = 0;
        while idx < m_usize && i < REGISTERS_PER_WORD {
            let m_idx = (word >> shift) & REGISTER_WORD_MASK;
            z_inverse += 1.0 / ((1u64 << m_idx) as f64);
            if m_idx == 0 {
                v += 1.0;
            }
            shift += REGISTER_SIZE;
            i += 1;
            idx += 1;
        }
    }

    let e = alpha_m2(p, m) / z_inverse;
    let e_bias_corrected = if p < 19 && e < 5.0 * m {
        e - estimate_bias(p, e)
    } else {
        e
    };

    let estimate = if v > 0.0 {
        // Linear counting for small cardinalities.
        let h = m * (m / v).ln();
        if (p < 19 && h <= THRESHOLDS[p - 4]) || e <= 2.5 * m {
            h
        } else {
            e_bias_corrected
        }
    } else {
        e_bias_corrected
    };

    // Match Java's Math.round: floor(x + 0.5).
    (estimate + 0.5).floor() as i64
}

/// Per-group accumulator (also used for the ungrouped / merge path).
#[derive(Debug)]
struct HllPlusPlusAccumulator {
    p: usize,
    words: Vec<i64>,
}

impl HllPlusPlusAccumulator {
    fn new(p: usize) -> Self {
        Self {
            p,
            words: vec![0i64; num_words(p)],
        }
    }
}

impl Accumulator for HllPlusPlusAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = &values[0];
        let hashes = hash_values(array)?;
        for (i, &hash) in hashes.iter().enumerate() {
            if array.is_null(i) {
                continue;
            }
            update_word(&mut self.words, self.p, hash);
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let m = 1usize << self.p;
        let num_rows = states[0].len();
        let cols: Vec<&Int64Array> = states
            .iter()
            .map(|s| s.as_primitive::<arrow::datatypes::Int64Type>())
            .collect();
        let mut partial = vec![0i64; self.words.len()];
        for row in 0..num_rows {
            for (w, col) in cols.iter().enumerate() {
                partial[w] = col.value(row);
            }
            merge_words(&mut self.words, &partial, m);
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(self
            .words
            .iter()
            .map(|&w| ScalarValue::Int64(Some(w)))
            .collect())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(query(self.p, &self.words))))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.words.capacity() * std::mem::size_of::<i64>()
    }
}

/// Vectorized grouped accumulator. The `numWords` words for all groups live in a single flat
/// buffer, with group `g` occupying `words[g * num_words .. (g + 1) * num_words]`.
#[derive(Debug)]
struct HllPlusPlusGroupsAccumulator {
    p: usize,
    m: usize,
    num_words: usize,
    words: Vec<i64>,
}

impl HllPlusPlusGroupsAccumulator {
    fn new(p: usize) -> Self {
        Self {
            p,
            m: 1usize << p,
            num_words: num_words(p),
            words: Vec::new(),
        }
    }

    fn resize(&mut self, total_num_groups: usize) {
        self.words.resize(total_num_groups * self.num_words, 0i64);
    }

    fn group_slice(&mut self, group_index: usize) -> &mut [i64] {
        let start = group_index * self.num_words;
        &mut self.words[start..start + self.num_words]
    }

    /// Drain the words for the emitted groups, leaving the rest for a subsequent emit.
    fn emit_words(&mut self, emit_to: EmitTo) -> Vec<i64> {
        match emit_to {
            EmitTo::All => std::mem::take(&mut self.words),
            EmitTo::First(n) => {
                let split = n * self.num_words;
                let remainder = self.words.split_off(split);
                std::mem::replace(&mut self.words, remainder)
            }
        }
    }
}

impl GroupsAccumulator for HllPlusPlusGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.resize(total_num_groups);
        let p = self.p;
        let array = &values[0];
        let hashes = hash_values(array)?;
        for (i, &group_index) in group_indices.iter().enumerate() {
            if let Some(filter) = opt_filter {
                if !filter.is_valid(i) || !filter.value(i) {
                    continue;
                }
            }
            if array.is_null(i) {
                continue;
            }
            update_word(self.group_slice(group_index), p, hashes[i]);
        }
        Ok(())
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        _opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.resize(total_num_groups);
        let m = self.m;
        let nw = self.num_words;
        let cols: Vec<&Int64Array> = values
            .iter()
            .map(|s| s.as_primitive::<arrow::datatypes::Int64Type>())
            .collect();
        let mut partial = vec![0i64; nw];
        for (row, &group_index) in group_indices.iter().enumerate() {
            for (w, col) in cols.iter().enumerate() {
                partial[w] = col.value(row);
            }
            let start = group_index * nw;
            merge_words(&mut self.words[start..start + nw], &partial, m);
        }
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let p = self.p;
        let nw = self.num_words;
        let words = self.emit_words(emit_to);
        let estimates: Vec<i64> = words.chunks_exact(nw).map(|w| query(p, w)).collect();
        Ok(Arc::new(Int64Array::from(estimates)))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let nw = self.num_words;
        let words = self.emit_words(emit_to);
        let num_groups = words.len() / nw;
        // Emit one Int64 column per word, transposing the flat per-group layout.
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(nw);
        for w in 0..nw {
            let col: Vec<i64> = (0..num_groups).map(|g| words[g * nw + w]).collect();
            columns.push(Arc::new(Int64Array::from(col)));
        }
        Ok(columns)
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.words.capacity() * std::mem::size_of::<i64>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};

    fn acc(p: usize) -> HllPlusPlusAccumulator {
        HllPlusPlusAccumulator::new(p)
    }

    fn eval(a: &mut HllPlusPlusAccumulator) -> i64 {
        match a.evaluate().unwrap() {
            ScalarValue::Int64(Some(v)) => v,
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn precision_matches_spark_default() {
        assert_eq!(hllpp_precision(0.05), 9);
    }

    #[test]
    fn exact_for_small_cardinality() {
        let mut a = acc(9);
        let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 1, 2, 3]));
        a.update_batch(&[values]).unwrap();
        assert_eq!(eval(&mut a), 5);
    }

    #[test]
    fn empty_is_zero() {
        let mut a = acc(9);
        assert_eq!(eval(&mut a), 0);
    }

    #[test]
    fn nulls_are_ignored() {
        let mut a = acc(9);
        let values: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None, Some(2), None]));
        a.update_batch(&[values]).unwrap();
        assert_eq!(eval(&mut a), 2);
    }

    #[test]
    fn strings_small_cardinality() {
        let mut a = acc(9);
        let values: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "a", "c", "b"]));
        a.update_batch(&[values]).unwrap();
        assert_eq!(eval(&mut a), 3);
    }

    #[test]
    fn merge_matches_single_shot() {
        let single = {
            let mut a = acc(9);
            let v: ArrayRef = Arc::new(Int32Array::from((0..1000).collect::<Vec<i32>>()));
            a.update_batch(&[v]).unwrap();
            eval(&mut a)
        };

        let mut left = acc(9);
        left.update_batch(
            &[Arc::new(Int32Array::from((0..600).collect::<Vec<i32>>())) as ArrayRef],
        )
        .unwrap();
        let mut right = acc(9);
        right
            .update_batch(&[
                Arc::new(Int32Array::from((400..1000).collect::<Vec<i32>>())) as ArrayRef,
            ])
            .unwrap();

        let mut merged = acc(9);
        for a in [&mut left, &mut right] {
            let state = a.state().unwrap();
            let arrays: Vec<ArrayRef> = state
                .into_iter()
                .map(|s| ScalarValue::iter_to_array(vec![s]).unwrap())
                .collect();
            merged.merge_batch(&arrays).unwrap();
        }
        assert_eq!(eval(&mut merged), single);
    }

    #[test]
    fn large_cardinality_is_approximate() {
        let mut a = acc(9);
        let v: ArrayRef = Arc::new(Int32Array::from((0..100_000).collect::<Vec<i32>>()));
        a.update_batch(&[v]).unwrap();
        let est = eval(&mut a);
        let err = (est - 100_000).abs() as f64 / 100_000.0;
        assert!(err < 0.1, "estimate {est} too far from 100000 (err {err})");
    }

    // ---- GroupsAccumulator ----

    fn eval_groups(acc: &mut HllPlusPlusGroupsAccumulator) -> Vec<i64> {
        acc.evaluate(EmitTo::All)
            .unwrap()
            .as_primitive::<arrow::datatypes::Int64Type>()
            .values()
            .to_vec()
    }

    #[test]
    fn groups_multi_group_and_nulls() {
        let mut acc = HllPlusPlusGroupsAccumulator::new(9);
        // group 0: distinct {1,2,3}; group 1: {10}, plus a null (ignored)
        let values: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(10),
            Some(2),
            Some(3),
            None,
        ]));
        acc.update_batch(&[values], &[0, 1, 0, 0, 1], None, 2)
            .unwrap();
        assert_eq!(eval_groups(&mut acc), vec![3, 1]);
    }

    #[test]
    fn groups_matches_scalar_accumulator() {
        // The grouped path (single group) must agree with the scalar accumulator exactly.
        let data: Vec<i32> = (0..20_000).map(|i| i % 7000).collect();
        let scalar = {
            let mut a = acc(12);
            a.update_batch(&[Arc::new(Int32Array::from(data.clone())) as ArrayRef])
                .unwrap();
            eval(&mut a)
        };
        let mut g = HllPlusPlusGroupsAccumulator::new(12);
        g.update_batch(
            &[Arc::new(Int32Array::from(data.clone())) as ArrayRef],
            &vec![0usize; data.len()],
            None,
            1,
        )
        .unwrap();
        assert_eq!(eval_groups(&mut g)[0], scalar);
    }

    #[test]
    fn groups_merge_roundtrip() {
        let mut left = HllPlusPlusGroupsAccumulator::new(9);
        left.update_batch(
            &[Arc::new(Int32Array::from((0..600).collect::<Vec<i32>>())) as ArrayRef],
            &vec![0usize; 600],
            None,
            1,
        )
        .unwrap();
        let mut right = HllPlusPlusGroupsAccumulator::new(9);
        right
            .update_batch(
                &[Arc::new(Int32Array::from((400..1000).collect::<Vec<i32>>())) as ArrayRef],
                &vec![0usize; 600],
                None,
                1,
            )
            .unwrap();

        let mut merged = HllPlusPlusGroupsAccumulator::new(9);
        for part in [&mut left, &mut right] {
            let state = part.state(EmitTo::All).unwrap();
            let n = state[0].len();
            merged
                .merge_batch(&state, &vec![0usize; n], None, 1)
                .unwrap();
        }
        let single = {
            let mut a = acc(9);
            a.update_batch(&[
                Arc::new(Int32Array::from((0..1000).collect::<Vec<i32>>())) as ArrayRef
            ])
            .unwrap();
            eval(&mut a)
        };
        assert_eq!(eval_groups(&mut merged)[0], single);
    }
}
