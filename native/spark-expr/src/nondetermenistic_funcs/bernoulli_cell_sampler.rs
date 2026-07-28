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

use crate::nondetermenistic_funcs::internal::StatefulSeedValueGenerator;
use crate::nondetermenistic_funcs::rand::XorShiftRandom;

/// Port of Spark's `BernoulliCellSampler`, used by `SampleExec` when sampling without
/// replacement. One `XORShiftRandom.nextDouble()` is drawn per row, and the row is kept when the
/// value falls in `[lower_bound, upper_bound)`.
///
/// See: <https://github.com/apache/spark/blob/v4.1.1/core/src/main/scala/org/apache/spark/util/random/RandomSampler.scala>
#[derive(Debug)]
pub struct BernoulliCellSampler {
    lower_bound: f64,
    upper_bound: f64,
    rng: XorShiftRandom,
}

impl BernoulliCellSampler {
    /// `seed` must already include the partition index, matching Spark, which seeds a fresh
    /// sampler per partition with `seed + partitionIndex`.
    pub fn new(lower_bound: f64, upper_bound: f64, seed: i64) -> Self {
        Self {
            lower_bound,
            upper_bound,
            rng: XorShiftRandom::from_init_seed(seed),
        }
    }

    /// Whether the next row should be included in the sample.
    pub fn sample(&mut self) -> bool {
        if self.upper_bound - self.lower_bound <= 0.0 {
            // Spark returns early here without consuming a value from the generator.
            return false;
        }
        let x = self.rng.next_f64();
        x >= self.lower_bound && x < self.upper_bound
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Exactly one value is drawn per row, in order, so that the sampler stays in lockstep with
    /// the generator Spark uses. `XorShiftRandom` itself is verified against Spark separately.
    #[test]
    fn test_consumes_one_draw_per_row() {
        let mut sampler = BernoulliCellSampler::new(0.0, 0.3, 42);
        let mut rng = XorShiftRandom::from_init_seed(42);
        for _ in 0..100 {
            let x = rng.next_f64();
            assert_eq!(sampler.sample(), x < 0.3);
        }
    }

    /// The complement range of the test above must select exactly the rows that it rejected,
    /// which is the property `randomSplit` relies on.
    #[test]
    fn test_ranges_partition_the_rows() {
        let mut lower = BernoulliCellSampler::new(0.0, 0.3, 7);
        let mut upper = BernoulliCellSampler::new(0.3, 1.0, 7);
        for _ in 0..1000 {
            assert_ne!(lower.sample(), upper.sample());
        }
    }

    #[test]
    fn test_empty_range_selects_nothing() {
        let mut sampler = BernoulliCellSampler::new(0.5, 0.5, 42);
        for _ in 0..100 {
            assert!(!sampler.sample());
        }
    }
}
