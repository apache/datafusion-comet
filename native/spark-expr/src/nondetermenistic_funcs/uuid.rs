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
}
