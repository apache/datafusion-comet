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

//! A faithful port of Spark's `QuantileSummaries` (Greenwald-Khanna), the
//! sketch behind `approx_percentile` / `percentile_approx`. Kept free of any
//! DataFusion dependency so it can be unit-tested in isolation.
//!
//! Reference: `org.apache.spark.sql.catalyst.util.QuantileSummaries`.
//!
//! Results are bit-identical to Spark for deterministic plans. What is
//! load-bearing for that identity, and must not change:
//!   - the 50000-value head-buffer flush boundary (`DEFAULT_HEAD_SIZE`),
//!   - the integer arithmetic: `floor(2 * relative_error * count)` for `delta`,
//!     `2 * relative_error * count` for the compress/merge threshold, `/ 2` for
//!     `target_error`, and `ceil(percentile * count)` for the query rank,
//!   - the backward compress traversal with the `< merge_threshold` test that
//!     always preserves the minimum,
//!   - the non-commutative merge interleave and its asymmetric delta adjustment,
//!   - the ascending multi-percentile query sweep with its ceil-rank search.
//!
//! The data-structure choices (Vec vs deque, copy vs clone, rebuild vs in-place)
//! do not affect results and are free to be optimized.

/// A single sampled statistic: the value, its minimum rank jump `g`, and the
/// maximum span of the rank `delta`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Stats {
    pub value: f64,
    pub g: i64,
    pub delta: i64,
}

/// Reusable workspace owned by an accumulator rather than by each summary.
#[derive(Debug, Default)]
pub struct QuantileSummariesScratch {
    sampled: Vec<Stats>,
}

impl QuantileSummariesScratch {
    /// Heap bytes of the workspace buffer. Flush/compress/merge swap this
    /// buffer with a summary's `sampled`, so a given allocation is owned by
    /// exactly one of the two at any time; summing this with each summary's
    /// [`QuantileSummaries::heap_size`] never double-counts.
    pub fn heap_size(&self) -> usize {
        self.sampled.capacity() * std::mem::size_of::<Stats>()
    }
}

#[derive(Debug)]
pub struct QuantileSummaries {
    compress_threshold: usize,
    relative_error: f64,
    sampled: Vec<Stats>,
    count: i64,
    compressed: bool,
    head_sampled: Vec<f64>,
}

/// Hand-written (not derived) so that `clone_from` reuses the destination's
/// `Vec` allocations: the derived impl keeps the default `clone_from`, which
/// is `*self = source.clone()` and reallocates both vectors.
impl Clone for QuantileSummaries {
    fn clone(&self) -> Self {
        Self {
            compress_threshold: self.compress_threshold,
            relative_error: self.relative_error,
            sampled: self.sampled.clone(),
            count: self.count,
            compressed: self.compressed,
            head_sampled: self.head_sampled.clone(),
        }
    }

    fn clone_from(&mut self, source: &Self) {
        // Destructuring makes adding a field a compile error here instead of
        // a silently missing copy.
        let Self {
            compress_threshold,
            relative_error,
            sampled,
            count,
            compressed,
            head_sampled,
        } = source;
        self.compress_threshold = *compress_threshold;
        self.relative_error = *relative_error;
        self.sampled.clone_from(sampled);
        self.count = *count;
        self.compressed = *compressed;
        self.head_sampled.clone_from(head_sampled);
    }
}

impl QuantileSummaries {
    pub const DEFAULT_COMPRESS_THRESHOLD: usize = 10000;
    pub const DEFAULT_HEAD_SIZE: usize = 50000;

    /// Mirrors Spark's `PercentileDigest` ctor which builds a summary with
    /// `compressed = true`.
    pub fn new(compress_threshold: usize, relative_error: f64) -> Self {
        Self {
            compress_threshold,
            relative_error,
            sampled: Vec::new(),
            count: 0,
            compressed: true,
            head_sampled: Vec::new(),
        }
    }

    pub fn count(&self) -> i64 {
        self.count
    }

    /// Heap bytes currently owned by this summary (excluding the struct
    /// itself): the `sampled` and `head_sampled` capacities. The shared
    /// scratch buffer is accounted separately by
    /// [`QuantileSummariesScratch::heap_size`].
    pub fn heap_size(&self) -> usize {
        self.sampled.capacity() * std::mem::size_of::<Stats>()
            + self.head_sampled.capacity() * std::mem::size_of::<f64>()
    }

    /// Reserve space in the head buffer for `additional` incoming values, so a
    /// batch of inserts does not repeatedly grow it.
    pub fn reserve(&mut self, additional: usize) {
        self.head_sampled.reserve(additional);
    }

    pub fn insert(&mut self, x: f64, scratch: &mut QuantileSummariesScratch) {
        self.head_sampled.push(x);
        self.compressed = false;
        if self.head_sampled.len() >= Self::DEFAULT_HEAD_SIZE {
            self.with_head_buffer_inserted(scratch);
            if self.sampled.len() >= self.compress_threshold {
                self.compress(scratch);
            }
        }
    }

    fn with_head_buffer_inserted(&mut self, scratch: &mut QuantileSummariesScratch) {
        if self.head_sampled.is_empty() {
            return;
        }
        let mut current_count = self.count;
        let mut sorted = std::mem::take(&mut self.head_sampled);
        // Spark relies on `Array[Double].sorted`. The sort key is the value
        // itself, so equal keys are bit-equal elements and stability is
        // irrelevant; `total_cmp` gives a deterministic total order.
        sorted.sort_unstable_by(|a, b| a.total_cmp(b));

        scratch.sampled.clear();
        scratch.sampled.reserve(self.sampled.len() + sorted.len());
        let mut sample_idx = 0usize;
        let mut ops_idx = 0usize;
        while ops_idx < sorted.len() {
            let current_sample = sorted[ops_idx];
            while sample_idx < self.sampled.len()
                && self.sampled[sample_idx].value <= current_sample
            {
                scratch.sampled.push(self.sampled[sample_idx]);
                sample_idx += 1;
            }
            current_count += 1;
            let delta = if scratch.sampled.is_empty()
                || (sample_idx == self.sampled.len() && ops_idx == sorted.len() - 1)
            {
                0
            } else {
                // Spark: `math.floor(2 * relativeError * currentCount).toLong`
                // (verified `.toLong` in 3.4/3.5/4.0/4.1), matching our i64.
                (2.0 * self.relative_error * current_count as f64).floor() as i64
            };
            scratch.sampled.push(Stats {
                value: current_sample,
                g: 1,
                delta,
            });
            ops_idx += 1;
        }
        while sample_idx < self.sampled.len() {
            scratch.sampled.push(self.sampled[sample_idx]);
            sample_idx += 1;
        }
        std::mem::swap(&mut self.sampled, &mut scratch.sampled);
        scratch.sampled.clear();
        self.count = current_count;
    }

    pub fn compress(&mut self, scratch: &mut QuantileSummariesScratch) {
        // Already compressed and the head buffer is empty (insert clears the
        // flag whenever it stages a value), so there is nothing to do. This
        // mirrors Spark's `PercentileDigest.isCompressed` guard, which also
        // compresses at most once.
        if self.compressed {
            return;
        }
        self.with_head_buffer_inserted(scratch);
        let merge_threshold = 2.0 * self.relative_error * self.count as f64;
        Self::compress_immut(&self.sampled, merge_threshold, &mut scratch.sampled);
        std::mem::swap(&mut self.sampled, &mut scratch.sampled);
        scratch.sampled.clear();
        self.compressed = true;
    }

    fn compress_immut(current_samples: &[Stats], merge_threshold: f64, res: &mut Vec<Stats>) {
        res.clear();
        if current_samples.is_empty() {
            return;
        }
        // Spark prepends into a `ListBuffer`; we push in the same order and
        // reverse once, which yields an identical sequence.
        let mut head = current_samples[current_samples.len() - 1];
        // Traverse backward from size-2 down to index 1 (index 0 is preserved
        // separately so the minimum is always kept).
        let mut i = current_samples.len() as isize - 2;
        while i >= 1 {
            let sample1 = current_samples[i as usize];
            if ((sample1.g + head.g + head.delta) as f64) < merge_threshold {
                head.g += sample1.g;
            } else {
                res.push(head);
                head = sample1;
            }
            i -= 1;
        }
        res.push(head);
        let curr_head = current_samples[0];
        if curr_head.value <= head.value && current_samples.len() > 1 {
            res.push(curr_head);
        }
        res.reverse();
    }

    pub fn merge(&mut self, other: &QuantileSummaries, scratch: &mut QuantileSummariesScratch) {
        debug_assert!(self.head_sampled.is_empty(), "compress before merge");
        debug_assert!(other.head_sampled.is_empty(), "compress before merge");
        if other.count == 0 {
            return;
        }
        if self.count == 0 {
            // `other.head_sampled` is empty (asserted above), so this only
            // copies `sampled`, reusing our existing buffer.
            self.clone_from(other);
            return;
        }
        let merged_relative_error = self.relative_error.max(other.relative_error);
        let merged_count = self.count + other.count;
        let additional_self_delta =
            (2.0 * other.relative_error * other.count as f64).floor() as i64;
        let additional_other_delta = (2.0 * self.relative_error * self.count as f64).floor() as i64;

        scratch.sampled.clear();
        scratch
            .sampled
            .reserve(self.sampled.len() + other.sampled.len());
        let mut self_idx = 0usize;
        let mut other_idx = 0usize;
        while self_idx < self.sampled.len() && other_idx < other.sampled.len() {
            let self_sample = &self.sampled[self_idx];
            let other_sample = &other.sampled[other_idx];
            let (mut next_sample, additional_delta) = if self_sample.value < other_sample.value {
                self_idx += 1;
                (
                    *self_sample,
                    if other_idx > 0 {
                        additional_self_delta
                    } else {
                        0
                    },
                )
            } else {
                other_idx += 1;
                (
                    *other_sample,
                    if self_idx > 0 {
                        additional_other_delta
                    } else {
                        0
                    },
                )
            };
            next_sample.delta += additional_delta;
            scratch.sampled.push(next_sample);
        }
        while self_idx < self.sampled.len() {
            scratch.sampled.push(self.sampled[self_idx]);
            self_idx += 1;
        }
        while other_idx < other.sampled.len() {
            scratch.sampled.push(other.sampled[other_idx]);
            other_idx += 1;
        }
        Self::compress_immut(
            &scratch.sampled,
            2.0 * merged_relative_error * merged_count as f64,
            &mut self.sampled,
        );
        scratch.sampled.clear();
        self.compress_threshold = other.compress_threshold;
        self.relative_error = merged_relative_error;
        self.count = merged_count;
        self.compressed = true;
    }

    pub fn query(&self, percentiles: &[f64]) -> Option<Vec<f64>> {
        debug_assert!(self.head_sampled.is_empty(), "compress before query");
        if self.sampled.is_empty() {
            return None;
        }
        let target_error = self
            .sampled
            .iter()
            .fold(i64::MIN, |m, s| m.max(s.delta + s.g))
            / 2;

        let mut indexed: Vec<(f64, usize)> = percentiles
            .iter()
            .enumerate()
            .map(|(i, p)| (*p, i))
            .collect();
        indexed.sort_by(|a, b| a.0.total_cmp(&b.0));

        let mut result = vec![0.0f64; percentiles.len()];
        let mut index = 0usize;
        let mut min_rank = self.sampled[0].g;
        for (percentile, pos) in indexed {
            if percentile <= self.relative_error {
                result[pos] = self.sampled[0].value;
            } else if percentile >= 1.0 - self.relative_error {
                result[pos] = self.sampled[self.sampled.len() - 1].value;
            } else {
                let (new_index, new_min_rank, approx) =
                    self.find_approx_quantile(index, min_rank, target_error, percentile);
                index = new_index;
                min_rank = new_min_rank;
                result[pos] = approx;
            }
        }
        Some(result)
    }

    fn find_approx_quantile(
        &self,
        index: usize,
        min_rank_at_index: i64,
        target_error: i64,
        percentile: f64,
    ) -> (usize, i64, f64) {
        let mut cur_sample = &self.sampled[index];
        let rank = (percentile * self.count as f64).ceil() as i64;
        let mut i = index;
        let mut min_rank = min_rank_at_index;
        while i < self.sampled.len() - 1 {
            let max_rank = min_rank + cur_sample.delta;
            if max_rank - target_error <= rank && rank <= min_rank + target_error {
                return (i, min_rank, cur_sample.value);
            } else {
                i += 1;
                cur_sample = &self.sampled[i];
                min_rank += cur_sample.g;
            }
        }
        (
            self.sampled.len() - 1,
            0,
            self.sampled[self.sampled.len() - 1].value,
        )
    }

    /// Comet-internal little-endian layout (NOT Spark's big-endian serializer):
    /// count(i64) | relative_error(f64) | n(u32) | n * [value(f64) g(i64) delta(i64)].
    /// Callers must `compress()` first.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(8 + 8 + 4 + self.sampled.len() * 24);
        buf.extend_from_slice(&self.count.to_le_bytes());
        buf.extend_from_slice(&self.relative_error.to_le_bytes());
        buf.extend_from_slice(&(self.sampled.len() as u32).to_le_bytes());
        for s in &self.sampled {
            buf.extend_from_slice(&s.value.to_le_bytes());
            buf.extend_from_slice(&s.g.to_le_bytes());
            buf.extend_from_slice(&s.delta.to_le_bytes());
        }
        buf
    }

    pub fn from_bytes(compress_threshold: usize, bytes: &[u8]) -> Self {
        let mut off = 0usize;
        let take = |off: &mut usize, n: usize| {
            let s = &bytes[*off..*off + n];
            *off += n;
            s
        };
        let count = i64::from_le_bytes(take(&mut off, 8).try_into().unwrap());
        let relative_error = f64::from_le_bytes(take(&mut off, 8).try_into().unwrap());
        let n = u32::from_le_bytes(take(&mut off, 4).try_into().unwrap()) as usize;
        let mut sampled = Vec::with_capacity(n);
        for _ in 0..n {
            let value = f64::from_le_bytes(take(&mut off, 8).try_into().unwrap());
            let g = i64::from_le_bytes(take(&mut off, 8).try_into().unwrap());
            let delta = i64::from_le_bytes(take(&mut off, 8).try_into().unwrap());
            sampled.push(Stats { value, g, delta });
        }
        QuantileSummaries {
            compress_threshold,
            relative_error,
            sampled,
            count,
            compressed: true,
            head_sampled: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EPS: f64 = 1.0 / 10000.0;

    fn summary_of(values: &[f64]) -> QuantileSummaries {
        summary_of_with_error(values, EPS)
    }

    fn summary_of_with_error(values: &[f64], relative_error: f64) -> QuantileSummaries {
        let mut qs = QuantileSummaries::new(
            QuantileSummaries::DEFAULT_COMPRESS_THRESHOLD,
            relative_error,
        );
        let mut scratch = QuantileSummariesScratch::default();
        for &v in values {
            qs.insert(v, &mut scratch);
        }
        qs.compress(&mut scratch);
        qs
    }

    /// Brute-force Spark-equivalent exact rank used to bound the approximation.
    fn exact_percentile(sorted: &[f64], p: f64) -> f64 {
        let rank = (p * sorted.len() as f64).ceil() as usize;
        let idx = rank.saturating_sub(1).min(sorted.len() - 1);
        sorted[idx]
    }

    #[test]
    fn empty_summary_queries_none() {
        let qs = QuantileSummaries::new(QuantileSummaries::DEFAULT_COMPRESS_THRESHOLD, EPS);
        assert_eq!(qs.query(&[0.5]), None);
    }

    #[test]
    fn query_returns_actual_inserted_values() {
        let values: Vec<f64> = (1..=1000).map(|i| i as f64).collect();
        let qs = summary_of(&values);
        // GK returns an actual inserted value, never an interpolation.
        for p in [0.1, 0.25, 0.5, 0.75, 0.9] {
            let got = qs.query(&[p]).unwrap()[0];
            assert!(values.contains(&got), "p={p} produced non-member {got}");
        }
    }

    #[test]
    fn query_within_relative_error_bound() {
        let values: Vec<f64> = (1..=10000).map(|i| i as f64).collect();
        let mut sorted = values.clone();
        sorted.sort_by(|a, b| a.total_cmp(b));
        let qs = summary_of(&values);
        for p in [0.01, 0.1, 0.5, 0.9, 0.99] {
            let got = qs.query(&[p]).unwrap()[0];
            let exact = exact_percentile(&sorted, p);
            // rank error bounded by relativeError * count.
            let rank_err = (got - exact).abs();
            assert!(
                rank_err <= EPS * values.len() as f64 + 1.0,
                "p={p} got={got} exact={exact}"
            );
        }
    }

    #[test]
    fn multi_percentile_matches_single() {
        let values: Vec<f64> = (1..=5000).map(|i| i as f64).collect();
        let qs = summary_of(&values);
        let ps = [0.9, 0.1, 0.5, 0.99, 0.01];
        let batch = qs.query(&ps).unwrap();
        for (i, &p) in ps.iter().enumerate() {
            assert_eq!(batch[i], qs.query(&[p]).unwrap()[0]);
        }
    }

    #[test]
    fn repeated_merges_are_within_bound() {
        let left: Vec<f64> = (1..=5000).map(|i| i as f64).collect();
        let middle: Vec<f64> = (5001..=10000).map(|i| i as f64).collect();
        let right: Vec<f64> = (10001..=15000).map(|i| i as f64).collect();
        let mut merged = summary_of(&left);
        let mut scratch = QuantileSummariesScratch::default();
        merged.merge(&summary_of(&middle), &mut scratch);
        merged.merge(&summary_of(&right), &mut scratch);
        let mut all: Vec<f64> = left
            .iter()
            .chain(middle.iter())
            .chain(right.iter())
            .cloned()
            .collect();
        all.sort_by(|x, y| x.total_cmp(y));
        let got = merged.query(&[0.5]).unwrap()[0];
        let exact = exact_percentile(&all, 0.5);
        assert!((got - exact).abs() <= EPS * all.len() as f64 + 1.0);
    }

    #[test]
    fn flush_reuses_shared_sample_buffer_and_drops_head() {
        let mut scratch = QuantileSummariesScratch::default();
        let mut summary = summary_of(&(1..=100).map(|i| i as f64).collect::<Vec<_>>());
        summary.insert(50.5, &mut scratch);
        scratch
            .sampled
            .reserve(summary.sampled.len() + summary.head_sampled.len());
        let sampled_ptr = summary.sampled.as_ptr();
        let buffer_ptr = scratch.sampled.as_ptr();

        summary.with_head_buffer_inserted(&mut scratch);

        assert_eq!(summary.sampled.as_ptr(), buffer_ptr);
        assert_eq!(scratch.sampled.as_ptr(), sampled_ptr);
        assert_eq!(summary.head_sampled.capacity(), 0);
    }

    #[test]
    fn merge_keeps_uncompressed_capacity_in_shared_scratch() {
        let error = 0.01;
        let mut summary =
            summary_of_with_error(&(1..=10_000).map(|i| i as f64).collect::<Vec<_>>(), error);
        let other = summary_of_with_error(
            &(10_001..=20_000).map(|i| i as f64).collect::<Vec<_>>(),
            error,
        );
        let merged_len = summary.sampled.len() + other.sampled.len();
        let mut scratch = QuantileSummariesScratch::default();
        scratch.sampled.reserve(merged_len);
        let buffer_ptr = scratch.sampled.as_ptr();

        summary.merge(&other, &mut scratch);

        assert_eq!(scratch.sampled.as_ptr(), buffer_ptr);
        assert!(scratch.heap_size() >= merged_len * std::mem::size_of::<Stats>());
        assert!(summary.sampled.capacity() < merged_len);
    }

    #[test]
    fn extreme_percentiles_hit_short_circuits() {
        let values: Vec<f64> = (1..=1000).map(|i| i as f64).collect();
        let qs = summary_of(&values);
        // 0.0 <= relative_error short-circuits to the minimum, 1.0 >= 1 -
        // relative_error to the maximum.
        assert_eq!(qs.query(&[0.0]).unwrap()[0], 1.0);
        assert_eq!(qs.query(&[1.0]).unwrap()[0], 1000.0);
    }

    #[test]
    fn negative_values_are_ordered() {
        let values: Vec<f64> = (-500..500).map(|i| i as f64).collect();
        let mut sorted = values.clone();
        sorted.sort_by(|a, b| a.total_cmp(b));
        let qs = summary_of(&values);
        let got = qs.query(&[0.5]).unwrap()[0];
        let exact = exact_percentile(&sorted, 0.5);
        assert!((got - exact).abs() <= EPS * values.len() as f64 + 1.0);
    }

    #[test]
    fn duplicate_heavy_column_accumulates_g() {
        // Five distinct values repeated 200 times each; forces `g` accumulation
        // in `compress_immut`.
        let values: Vec<f64> = (0..1000).map(|i| (i % 5) as f64).collect();
        let qs = summary_of(&values);
        for &p in &[0.1, 0.5, 0.9] {
            let got = qs.query(&[p]).unwrap()[0];
            assert!((0.0..=4.0).contains(&got), "p={p} produced {got}");
        }
    }

    #[test]
    fn signed_zero_ordering_is_deterministic() {
        // `total_cmp` orders -0.0 before 0.0; inserting both must not panic and
        // must produce a value drawn from the input.
        let qs = summary_of(&[-0.0, 0.0, -0.0, 0.0, 1.0]);
        let got = qs.query(&[0.5]).unwrap()[0];
        assert!(
            got == 0.0 || got == 1.0,
            "median of signed zeros produced {got}"
        );
    }

    #[test]
    fn serde_round_trips() {
        let qs = summary_of(&(1..=2000).map(|i| i as f64).collect::<Vec<_>>());
        let bytes = qs.to_bytes();
        let back =
            QuantileSummaries::from_bytes(QuantileSummaries::DEFAULT_COMPRESS_THRESHOLD, &bytes);
        assert_eq!(qs.count(), back.count());
        assert_eq!(qs.query(&[0.5]), back.query(&[0.5]));
    }
}
