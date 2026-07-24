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

use crate::nondetermenistic_funcs::shuffle::SparkMersenneTwister;
use arrow::array::{RecordBatch, StringArray};
use arrow::datatypes::{DataType, Schema};
use datafusion::common::Result;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

/// Draw one RFC 4122 version 4 UUID string from the generator, matching
/// `org.apache.spark.sql.catalyst.util.RandomUUIDGenerator.getNextUUID`: two
/// `nextLong()` draws with the version (4) and variant (10) bits masked in.
fn next_uuid_string(rng: &mut SparkMersenneTwister) -> String {
    let most = (rng.next_long() as u64 & 0xFFFF_FFFF_FFFF_0FFF) | 0x0000_0000_0000_4000;
    let least = (rng.next_long() as u64 | 0x8000_0000_0000_0000) & 0xBFFF_FFFF_FFFF_FFFF;
    // Matches `java.util.UUID.toString()`: lowercase, zero-padded, hyphenated.
    format!(
        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        (most >> 32) & 0xFFFF_FFFF,
        (most >> 16) & 0xFFFF,
        most & 0xFFFF,
        (least >> 48) & 0xFFFF,
        least & 0xFFFF_FFFF_FFFF,
    )
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

        let result: StringArray = (0..num_rows).map(|_| Some(next_uuid_string(rng))).collect();
        Ok(ColumnarValue::Array(Arc::new(result)))
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
    use arrow::array::{Array, RecordBatchOptions};

    fn empty_batch(num_rows: usize) -> RecordBatch {
        RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(num_rows)),
        )
        .unwrap()
    }

    fn eval_uuids(seed: i64, num_rows: usize) -> Vec<String> {
        let batch = empty_batch(num_rows);
        let expr = UuidExpr::new(seed);
        let result = expr.evaluate(&batch).unwrap().into_array(num_rows).unwrap();
        let arr = result.as_any().downcast_ref::<StringArray>().unwrap();
        (0..arr.len()).map(|i| arr.value(i).to_string()).collect()
    }

    #[test]
    fn test_uuid_format_and_version() {
        for uuid in eval_uuids(42, 20) {
            // Canonical 8-4-4-4-12 lowercase hex.
            assert_eq!(uuid.len(), 36);
            let parts: Vec<&str> = uuid.split('-').collect();
            assert_eq!(
                parts.iter().map(|p| p.len()).collect::<Vec<_>>(),
                vec![8, 4, 4, 4, 12]
            );
            assert!(uuid
                .chars()
                .all(|c| c == '-' || c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
            // Version 4: first nibble of the third group.
            assert_eq!(parts[2].as_bytes()[0], b'4');
            // Variant 10xx: first nibble of the fourth group is 8, 9, a, or b.
            assert!(matches!(parts[3].as_bytes()[0], b'8' | b'9' | b'a' | b'b'));
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
            let batch = empty_batch(n);
            let arr = expr.evaluate(&batch).unwrap().into_array(n).unwrap();
            let arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
            streamed.extend((0..arr.len()).map(|i| arr.value(i).to_string()));
        }
        assert_eq!(streamed, eval_uuids(7, 7));
        // All distinct.
        let mut sorted = streamed.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), streamed.len());
    }
}
