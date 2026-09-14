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
use arrow::array::{RecordBatch, StringBuilder};
use arrow::datatypes::{DataType, Schema};
use datafusion::common::Result;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

/// Draw one RFC 4122 version 4 UUID from the generator, matching
/// `org.apache.spark.sql.catalyst.util.RandomUUIDGenerator.getNextUUID`: two
/// `nextLong()` draws with the version (4) and variant (10) bits masked in.
/// `Uuid`'s canonical lowercase hyphenated form matches `java.util.UUID.toString()`.
fn next_uuid(rng: &mut SparkMersenneTwister) -> Uuid {
    let most = (rng.next_long() as u64 & 0xFFFF_FFFF_FFFF_0FFF) | 0x0000_0000_0000_4000;
    let least = (rng.next_long() as u64 | 0x8000_0000_0000_0000) & 0xBFFF_FFFF_FFFF_FFFF;
    Uuid::from_u64_pair(most, least)
}

/// Physical expression for Spark's `uuid()`. Like `ShuffleExpr`, the generator
/// state is kept in a `Mutex` so that it advances continuously across every
/// batch in a partition, matching Spark's stateful per-partition evaluation.
/// Spark seeds a fresh `RandomUUIDGenerator` (a Commons Math3 `MersenneTwister`)
/// per partition with `randomSeed + partitionIndex`.
#[derive(Debug)]
pub struct UuidExpr {
    /// Random seed already combined with the partition index by the planner.
    seed: i64,
    state_holder: Arc<Mutex<Option<SparkMersenneTwister>>>,
}

impl UuidExpr {
    pub fn new(seed: i64) -> Self {
        Self {
            seed,
            state_holder: Arc::new(Mutex::new(None)),
        }
    }
}

impl Display for UuidExpr {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Uuid({})", self.seed)
    }
}

impl PartialEq for UuidExpr {
    fn eq(&self, other: &Self) -> bool {
        self.seed.eq(&other.seed)
    }
}

impl Eq for UuidExpr {}

impl Hash for UuidExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.seed.hash(state);
    }
}

impl PhysicalExpr for UuidExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let num_rows = batch.num_rows();

        let mut state = self.state_holder.lock().unwrap();
        let rng = state.get_or_insert_with(|| SparkMersenneTwister::new(self.seed));

        // Each canonical UUID is exactly 36 bytes, so pre-size both builder buffers and encode
        // into a reused stack buffer to avoid a per-row heap allocation.
        const LEN: usize = uuid::fmt::Hyphenated::LENGTH;
        let mut builder = StringBuilder::with_capacity(num_rows, num_rows * LEN);
        let mut buf = [0u8; LEN];
        for _ in 0..num_rows {
            builder.append_value(next_uuid(rng).hyphenated().encode_lower(&mut buf));
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
        Ok(Arc::new(UuidExpr::new(self.seed)))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, RecordBatchOptions, StringArray};

    fn empty_batch(num_rows: usize) -> RecordBatch {
        RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(num_rows)),
        )
        .unwrap()
    }

    fn collect_uuids(expr: &UuidExpr, batch: &RecordBatch) -> Vec<String> {
        let arr = expr
            .evaluate(batch)
            .unwrap()
            .into_array(batch.num_rows())
            .unwrap();
        let arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
        (0..arr.len()).map(|i| arr.value(i).to_string()).collect()
    }

    fn eval_uuids(seed: i64, num_rows: usize) -> Vec<String> {
        collect_uuids(&UuidExpr::new(seed), &empty_batch(num_rows))
    }

    #[test]
    fn test_uuid_version_and_variant_bits() {
        // The RNG and the version/variant masking are ours; the canonical string layout is
        // guaranteed by the `uuid` crate. Assert the RFC 4122 v4 bits our masking sets, at their
        // fixed positions in `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`.
        for uuid in eval_uuids(42, 20) {
            let bytes = uuid.as_bytes();
            assert_eq!(bytes[14], b'4'); // version nibble
            assert!(matches!(bytes[19], b'8' | b'9' | b'a' | b'b')); // variant nibble
        }
    }

    #[test]
    fn test_uuid_deterministic_for_seed() {
        // Same seed -> identical sequence.
        assert_eq!(eval_uuids(42, 5), eval_uuids(42, 5));
        // Different seed -> different sequence.
        assert_ne!(eval_uuids(42, 5), eval_uuids(0, 5));
    }

    #[test]
    fn test_uuid_state_advances_across_batches() {
        // A single expression evaluated over two batches yields the same UUIDs as
        // one batch of the combined size (state persists across batches).
        let expr = UuidExpr::new(7);
        let mut streamed = Vec::new();
        for n in [3usize, 4usize] {
            streamed.extend(collect_uuids(&expr, &empty_batch(n)));
        }
        assert_eq!(streamed, eval_uuids(7, 7));
        // All distinct.
        let mut sorted = streamed.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), streamed.len());
    }

    #[test]
    fn test_uuid_empty_batch_does_not_advance_state() {
        // A zero-row batch (e.g. `LIMIT 0` over a `uuid` projection, or a fully filtered
        // partition) must not consume any RNG draws. If the code ever grew a `next_uuid()`
        // call outside the `0..num_rows` loop, the state would drift and downstream
        // bit-for-bit tests would fail intermittently.
        let expr = UuidExpr::new(7);
        let _ = collect_uuids(&expr, &empty_batch(0));
        let after_empty = collect_uuids(&expr, &empty_batch(3));
        assert_eq!(after_empty, eval_uuids(7, 3));
    }

    /// Golden fixtures captured from Commons Math3's `MersenneTwister` (the exact RNG that
    /// backs Spark's `org.apache.spark.sql.catalyst.util.RandomUUIDGenerator`). Regenerate
    /// with a tiny Java program:
    ///
    /// ```java
    /// import org.apache.commons.math3.random.MersenneTwister;
    /// import java.util.UUID;
    /// static UUID next(MersenneTwister r) {
    ///     long m = (r.nextLong() & 0xFFFFFFFFFFFF0FFFL) | 0x0000000000004000L;
    ///     long l = (r.nextLong() | 0x8000000000000000L) & 0xBFFFFFFFFFFFFFFFL;
    ///     return new UUID(m, l);
    /// }
    /// ```
    ///
    /// `next_long()` is used *only* by `uuid`, so the shuffle tests (which exercise
    /// `next_int`) do not guard against a `next_long` regression such as swapping the
    /// high/low words or masking with the wrong constant. This test locks all 128 bits
    /// per row, catches sign-extension bugs in `set_seed_long` via the negative seeds,
    /// and runs entirely in Rust so it fires without a JVM roundtrip.
    ///
    /// These constants are not the only line of defense, and must not become it. They were
    /// hand-captured, so their provenance cannot be checked from the repo, and regenerating them
    /// from this Rust code instead of from Commons Math3 would make the test circular and silently
    /// stop it testing anything. The backstop is `CometUuidExpressionSuite`, which builds
    /// `Uuid(Some(seed))` directly and compares Comet against Spark bit for bit on *every*
    /// supported profile -- including 3.4 and 3.5, where the SQL `uuid(seed)` form does not exist
    /// and `uuid_with_seed.sql` is skipped. If these values ever disagree with that suite, trust
    /// the suite.
    #[test]
    fn test_uuid_matches_commons_math3_random_uuid_generator() {
        let cases: &[(i64, &[&str])] = &[
            (
                0,
                &[
                    "269567e9-5d09-4af5-b20f-16851fc4a81a",
                    "2a52c3b8-890f-4aae-9607-3331ab0d4f01",
                    "46beb52b-622b-4226-bb12-4f40c6cdba04",
                    "24fdcf77-ec9f-4ac6-a096-b36d3b9fe378",
                    "cd614b40-85b9-4227-be28-64d85fcfbb24",
                ],
            ),
            (
                -1,
                &[
                    "05965e7e-3faf-4328-968d-6a409e667b13",
                    "36438051-74c0-4a52-9d78-e5c31853117e",
                    "eb6900f4-e69b-4bc6-9efc-c0ebc49b2c2e",
                    "bb6bb8b3-b20f-49e8-be16-41ab0dea1a19",
                    "bcb43f50-314a-4fdc-badf-4da2884aa53c",
                ],
            ),
            (
                i64::MIN,
                &[
                    "444b2d95-8a54-40b5-950e-9ef88de5a4fd",
                    "e3ff5c8a-242c-467f-b1d3-ca6b6acb483d",
                    "4d07fa9a-6df6-43c2-9523-a5fc295411cd",
                    "4f9549c4-92ed-4e93-9fa4-412e5e6740ed",
                    "88776d39-8d3b-4942-9eb2-25821ac806a7",
                ],
            ),
            (
                i64::MAX,
                &[
                    "596c0db4-d77a-4abb-9c91-5e7f323f02b0",
                    "98e2f79d-4b33-4ca4-b094-4ff3c2ee7d7b",
                    "39bae786-bdb1-4092-88eb-3aa52b52c0e8",
                    "d00c376b-87e7-4c8d-85d8-f7053996d6e6",
                    "ae65bca1-3143-4dc4-9a54-56d786f99699",
                ],
            ),
            (
                42,
                &[
                    "6f155395-c8b9-436b-a39c-d247226bc2b2",
                    "92da2253-2186-4525-a440-17ad3083a275",
                    "24ba1728-e648-487f-8c20-446d4411d15c",
                    "1188645d-be7b-4681-9936-8b24d7c51835",
                    "b4b1bfea-867b-4c0a-9f8f-bfe01cc571c0",
                ],
            ),
        ];
        for (seed, expected) in cases {
            let actual = eval_uuids(*seed, expected.len());
            assert_eq!(
                actual,
                expected.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
                "mismatch for seed {seed}"
            );
        }
    }
}
