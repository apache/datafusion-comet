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
use arrow::array::{RecordBatch, StringBuilder};
use arrow::datatypes::{DataType, Schema};
use datafusion::common::Result;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

/// Write one random alphanumeric character into `out`, matching a single iteration of
/// `org.apache.spark.sql.catalyst.expressions.ExpressionImplUtils.randStr`: draw
/// `abs(rng.nextInt() % 62)` and map it onto `0-9`, `a-z`, or `A-Z`.
fn next_rand_char(rng: &mut XorShiftRandom) -> u8 {
    let v = (rng.next_i32() % 62).abs();
    if v < 10 {
        b'0' + v as u8
    } else if v < 36 {
        b'a' + (v - 10) as u8
    } else {
        b'A' + (v - 36) as u8
    }
}

/// Physical expression for Spark's `randstr(length, seed)` (Spark 4.0+). Like `RandExpr`, the
/// generator state is kept in a `Mutex` so that it advances continuously across every batch in a
/// partition, matching Spark's stateful per-partition evaluation. Spark seeds a fresh
/// `XORShiftRandom` per partition with `seed + partitionIndex`. `length` is a required literal, so
/// every row produces a string of the same length.
#[derive(Debug)]
pub struct RandStrExpr {
    length: usize,
    /// Random seed already combined with the partition index by the planner.
    seed: i64,
    state_holder: Arc<Mutex<Option<XorShiftRandom>>>,
}

impl RandStrExpr {
    pub fn new(length: usize, seed: i64) -> Self {
        Self {
            length,
            seed,
            state_holder: Arc::new(Mutex::new(None)),
        }
    }
}

impl Display for RandStrExpr {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "RandStr({}, {})", self.length, self.seed)
    }
}

impl PartialEq for RandStrExpr {
    fn eq(&self, other: &Self) -> bool {
        self.length == other.length && self.seed == other.seed
    }
}

impl Eq for RandStrExpr {}

impl Hash for RandStrExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.length.hash(state);
        self.seed.hash(state);
    }
}

impl PhysicalExpr for RandStrExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let num_rows = batch.num_rows();

        let mut state = self.state_holder.lock().unwrap();
        let rng = state.get_or_insert_with(|| XorShiftRandom::from_init_seed(self.seed));

        // Every row produces a `length`-character string; pre-size both builder buffers and build
        // each string in a reused buffer. Every character is ASCII, so pushing keeps this a single
        // pass with no UTF-8 validation scan and no per-row allocation.
        // `saturating_mul` keeps the pre-size hint from overflowing on adversarially huge length
        // literals; it only affects the initial capacity, not correctness.
        let mut builder =
            StringBuilder::with_capacity(num_rows, num_rows.saturating_mul(self.length));
        let mut buf = String::with_capacity(self.length);
        for _ in 0..num_rows {
            buf.clear();
            for _ in 0..self.length {
                buf.push(next_rand_char(rng) as char);
            }
            builder.append_value(&buf);
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(RandStrExpr::new(self.length, self.seed)))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, RecordBatchOptions, StringArray};

    // Golden values captured from Spark 4.1.1 `randstr(length, seed)` (via
    // `org.apache.spark.sql.catalyst.expressions.ExpressionImplUtils.randStr`, a single partition so
    // the generator is seeded with `seed + 0`). These assert byte-level Spark compatibility
    // in-process, independent of the (slow, Spark-profile-only) SQL comparison tests. The
    // consecutive strings correspond to consecutive rows in one partition, matching how Comet
    // advances a single `XorShiftRandom` per row.
    const SPARK_SEED_42_LEN10_FIRST_5: [&str; 5] = [
        "pll6YOIJNn",
        "I2NS5bEWFX",
        "kbQpBdnHSp",
        "RQpCUGa76m",
        "NEAQ35Q71s",
    ];
    const SPARK_SEED_0_LEN12_FIRST_3: [&str; 3] = ["ceV0PXaR2IlB", "hHi56d0uCtuw", "SOTLiL13mRb3"];
    // Negative seed also locks in the serde's `Int -> Long` sign extension: Spark produces the same
    // strings for `randstr(8, -1)` and `randstr(8, -1L)`, and the wire seed is -1i64.
    const SPARK_SEED_NEG1_LEN8_FIRST_3: [&str; 3] = ["S4MAXZER", "uFNlolIG", "ofPTaM5d"];

    fn empty_batch(num_rows: usize) -> RecordBatch {
        RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(num_rows)),
        )
        .unwrap()
    }

    fn collect_strs(expr: &RandStrExpr, batch: &RecordBatch) -> Vec<String> {
        let arr = expr
            .evaluate(batch)
            .unwrap()
            .into_array(batch.num_rows())
            .unwrap();
        let arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
        (0..arr.len()).map(|i| arr.value(i).to_string()).collect()
    }

    fn eval_strs(length: usize, seed: i64, num_rows: usize) -> Vec<String> {
        collect_strs(&RandStrExpr::new(length, seed), &empty_batch(num_rows))
    }

    #[test]
    fn test_randstr_length_and_charset() {
        for s in eval_strs(12, 42, 20) {
            assert_eq!(s.len(), 12);
            assert!(s.bytes().all(|b| b.is_ascii_alphanumeric()));
        }
    }

    #[test]
    fn test_randstr_matches_spark_golden() {
        // Bit-for-bit equality with Spark 4.1.1 across positive, zero, and negative seeds.
        assert_eq!(eval_strs(10, 42, 5), SPARK_SEED_42_LEN10_FIRST_5);
        assert_eq!(eval_strs(12, 0, 3), SPARK_SEED_0_LEN12_FIRST_3);
        assert_eq!(eval_strs(8, -1, 3), SPARK_SEED_NEG1_LEN8_FIRST_3);
    }

    #[test]
    fn test_randstr_partition_index_seed() {
        // The planner seeds each partition with `base_seed + partition_index` before constructing
        // `RandStrExpr`. A partition-2 evaluation with base seed 40 must therefore reproduce Spark's
        // output for seed 42, exercising the whole seed-arithmetic path.
        let base_seed: i64 = 40;
        let partition: i64 = 2;
        assert_eq!(
            eval_strs(10, base_seed + partition, 5),
            SPARK_SEED_42_LEN10_FIRST_5
        );
        // Two partitions sharing a resolved seed produce identical streams (seed fully determines
        // the output); different partition indices diverge.
        assert_eq!(eval_strs(8, 100 + 5, 4), eval_strs(8, 90 + 15, 4));
        assert_ne!(eval_strs(8, 100 + 5, 4), eval_strs(8, 100 + 6, 4));
    }

    #[test]
    fn test_randstr_zero_length() {
        // Zero length yields empty strings regardless of seed (the seed dimension is irrelevant).
        for seed in [0, 42, -1, i64::MIN, i64::MAX] {
            assert_eq!(eval_strs(0, seed, 3), vec!["", "", ""]);
        }
    }

    #[test]
    fn test_randstr_large_length() {
        // Smoke test for the pre-sized builder capacity math (`num_rows * length`): a large length
        // over several rows must still produce well-formed alphanumeric strings of the exact length.
        let length = 65_536;
        for s in eval_strs(length, 7, 3) {
            assert_eq!(s.len(), length);
            assert!(s.bytes().all(|b| b.is_ascii_alphanumeric()));
        }
    }

    #[test]
    fn test_randstr_deterministic_for_seed() {
        // Same length and seed -> identical sequence.
        assert_eq!(eval_strs(8, 42, 5), eval_strs(8, 42, 5));
        // Different seed -> different sequence.
        assert_ne!(eval_strs(8, 42, 5), eval_strs(8, 0, 5));
    }

    #[test]
    fn test_randstr_state_advances_across_batches() {
        // A single expression evaluated over two batches yields the same strings as
        // one batch of the combined size (state persists across batches).
        let expr = RandStrExpr::new(6, 7);
        let mut streamed = Vec::new();
        for n in [3usize, 4usize] {
            streamed.extend(collect_strs(&expr, &empty_batch(n)));
        }
        assert_eq!(streamed, eval_strs(6, 7, 7));
    }
}
