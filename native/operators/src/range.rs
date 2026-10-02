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

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::Result;
use datafusion::physical_plan::memory::{LazyBatchGenerator, LazyMemoryExec};
use datafusion::physical_plan::Partitioning;
use parking_lot::RwLock;
use std::any::Any;
use std::fmt;
use std::sync::Arc;

/// The number of values per batch in Spark's generated code for `RangeExec`.
const SPARK_BATCH_SIZE: i64 = 1000;

/// Builds the plan for one Spark partition of `spark.range` or SQL `range()`: a single non-null
/// `BIGINT` column, produced `batch_size` rows at a time.
///
/// Partition bounds and values follow Spark's generated code for `RangeExec` (`initRange` and
/// `doProduce`). It computes the partition's bounds in `BigInteger` arithmetic, then walks the
/// partition in batches of [`SPARK_BATCH_SIZE`] values with wrapping `long` arithmetic, which
/// gives different values from `start + i * step` where that arithmetic overflows. Spark's
/// interpreted `RangeExec` disagrees with its generated code in those cases; this operator matches
/// the generated code, which Spark runs by default.
///
/// `num_elements` is Spark's element count truncated to a long, which is how Spark's generated
/// code reads it, and `partition` is the Spark partition index.
pub fn range_exec(
    start: i64,
    step: i64,
    num_elements: i64,
    num_slices: i32,
    partition: i32,
    batch_size: usize,
) -> Result<LazyMemoryExec> {
    let (partition_start, partition_elements) =
        partition_bounds(start, step, num_elements, num_slices, partition);
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let generator = RangeGenerator::new(
        Arc::clone(&schema),
        step,
        partition_start,
        partition_elements,
        batch_size,
    );
    let mut exec = LazyMemoryExec::try_new(schema, vec![Arc::new(RwLock::new(generator))])?;
    exec.try_set_partitioning(Partitioning::UnknownPartitioning(1))?;
    Ok(exec)
}

/// The first value of partition `partition` and the number of values in it, computed the way
/// Spark's generated code for `RangeExec` does (`initRange`): in `BigInteger` arithmetic, with the
/// partition's start and end clamped to the `Long` range.
///
/// `i128` holds every intermediate value. The product `partition * num_elements` is below 2^94,
/// and dividing by `num_slices` before multiplying by `step` keeps the next product below 2^126.
/// Division truncates toward zero and the remainder takes the dividend's sign, as with
/// `BigInteger`.
fn partition_bounds(
    start: i64,
    step: i64,
    num_elements: i64,
    num_slices: i32,
    partition: i32,
) -> (i64, i64) {
    fn clamp(value: i128) -> i64 {
        value.clamp(i64::MIN as i128, i64::MAX as i128) as i64
    }

    let index = partition as i128;
    let start = start as i128;
    let step = step as i128;
    let num_slices = num_slices as i128;
    let num_elements = num_elements as i128;
    let partition_start = clamp(index * num_elements / num_slices * step + start);
    let partition_end = clamp((index + 1) * num_elements / num_slices * step + start);
    let start_to_end = partition_end as i128 - partition_start as i128;
    // `BigInteger.longValue` keeps the low 64 bits, as `as i64` does.
    let count = (start_to_end / step) as i64;
    if count < 0 {
        (partition_start, 0)
    } else if start_to_end % step != 0 {
        (partition_start, count + 1)
    } else {
        (partition_start, count)
    }
}

/// Generates one partition's values, keeping the state of the loop in Spark's generated code for
/// `RangeExec` across output batches.
#[derive(Debug)]
struct RangeGenerator {
    schema: SchemaRef,
    step: i64,
    partition_start: i64,
    partition_elements: i64,
    batch_size: usize,
    /// The first value of the current Spark batch.
    next_index: i64,
    /// The end of the current Spark batch.
    batch_end: i64,
    /// The number of values not yet assigned to a Spark batch.
    num_elements_todo: i64,
    /// How many values the current Spark batch holds, and how many of them have been produced.
    local_end: i32,
    local_idx: i32,
}

impl RangeGenerator {
    fn new(
        schema: SchemaRef,
        step: i64,
        partition_start: i64,
        partition_elements: i64,
        batch_size: usize,
    ) -> Self {
        Self {
            schema,
            step,
            partition_start,
            partition_elements,
            batch_size,
            next_index: partition_start,
            batch_end: partition_start,
            num_elements_todo: partition_elements,
            local_end: 0,
            local_idx: 0,
        }
    }

    /// Produces up to `batch_size` values, or none once the partition is exhausted.
    fn next_values(&mut self) -> Vec<i64> {
        // `local_end` never exceeds the size of its Spark batch, so this bounds what is left.
        let remaining = (self.local_end - self.local_idx).max(0) as i64 + self.num_elements_todo;
        let limit = remaining.min(self.batch_size as i64) as usize;
        let mut values = Vec::with_capacity(limit);
        while values.len() < limit {
            if self.local_idx >= self.local_end {
                // The current Spark batch is done, so start the next one where it ended.
                let todo = self.num_elements_todo.min(SPARK_BATCH_SIZE);
                if todo == 0 {
                    break;
                }
                self.num_elements_todo -= todo;
                self.next_index = self.batch_end;
                self.batch_end = self.batch_end.wrapping_add(todo.wrapping_mul(self.step));
                // Java's `long` division wraps where Rust's `/` panics (`i64::MIN / -1`), and
                // Spark casts the quotient to `int`.
                self.local_end = self
                    .batch_end
                    .wrapping_sub(self.next_index)
                    .wrapping_div(self.step) as i32;
                self.local_idx = 0;
            } else {
                let count = (self.local_end - self.local_idx).min((limit - values.len()) as i32);
                values.extend((self.local_idx..self.local_idx + count).map(|local_idx| {
                    (local_idx as i64)
                        .wrapping_mul(self.step)
                        .wrapping_add(self.next_index)
                }));
                self.local_idx += count;
            }
        }
        values
    }
}

impl fmt::Display for RangeGenerator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CometRange: partition_start={}, step={}, partition_elements={}",
            self.partition_start, self.step, self.partition_elements
        )
    }
}

impl LazyBatchGenerator for RangeGenerator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn generate_next_batch(&mut self) -> Result<Option<RecordBatch>> {
        let values = self.next_values();
        if values.is_empty() {
            return Ok(None);
        }
        let column = Arc::new(Int64Array::from(values));
        Ok(Some(RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![column],
        )?))
    }

    fn reset_state(&self) -> Arc<RwLock<dyn LazyBatchGenerator>> {
        Arc::new(RwLock::new(RangeGenerator::new(
            Arc::clone(&self.schema),
            self.step,
            self.partition_start,
            self.partition_elements,
            self.batch_size,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::SessionContext;
    use futures::StreamExt;

    async fn collect(
        start: i64,
        step: i64,
        num_elements: i64,
        num_slices: i32,
        batch_size: usize,
    ) -> Vec<i64> {
        let ctx = SessionContext::new();
        let mut values = Vec::new();
        for partition in 0..num_slices {
            let range =
                range_exec(start, step, num_elements, num_slices, partition, batch_size).unwrap();
            let mut stream = range.execute(0, ctx.task_ctx()).unwrap();
            while let Some(batch) = stream.next().await {
                let batch = batch.unwrap();
                assert!(batch.num_rows() <= batch_size);
                let column = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                values.extend(column.values().iter().copied());
            }
        }
        values
    }

    /// Spark's element count for `range(start, end, step)`, which these tests only call where it
    /// fits in a long.
    fn num_elements(start: i64, end: i64, step: i64) -> i64 {
        let (start, end, step) = (start as i128, end as i128, step as i128);
        let n = if (end - start) % step == 0 || (end > start) != (step > 0) {
            (end - start) / step
        } else {
            (end - start) / step + 1
        };
        n as i64
    }

    #[tokio::test]
    async fn test_values_across_partitions_and_batches() {
        for (start, end, step, slices) in [
            (0i64, 10i64, 1i64, 3),
            (0, 10, 1, 15),
            (3, 15, 3, 2),
            (100, -100, -2, 3),
            (-5000, 5000, 7, 4),
        ] {
            let expected: Vec<i64> = if step > 0 {
                (start..end).step_by(step as usize).collect()
            } else {
                (end + 1..=start).rev().step_by((-step) as usize).collect()
            };
            for batch_size in [1, 7, 1000, 8192] {
                let n = num_elements(start, end, step);
                assert_eq!(
                    collect(start, step, n, slices, batch_size).await,
                    expected,
                    "range({start}, {end}, {step}, {slices}) with {batch_size}-row batches"
                );
            }
        }
    }

    /// The cases from Spark's `DataFrameRangeSuite` whose bounds are at the edges of the `Long`
    /// range.
    #[tokio::test]
    async fn test_long_range_edges() {
        let n = num_elements(i64::MIN, i64::MAX, i64::MAX);
        assert_eq!(
            collect(i64::MIN, i64::MAX, n, 100, 8192).await,
            vec![i64::MIN, -1, i64::MAX - 1]
        );
        let n = num_elements(i64::MAX, i64::MIN, i64::MIN);
        assert_eq!(
            collect(i64::MAX, i64::MIN, n, 100, 8192).await,
            vec![i64::MAX, -1]
        );
    }

    /// Spark's generated code returns no rows here because the end of its batch wraps around,
    /// while its interpreted `RangeExec` returns four.
    #[tokio::test]
    async fn test_generated_code_overflow() {
        let step = 1i64 << 62;
        let n = num_elements(i64::MIN, i64::MAX, step);
        assert_eq!(n, 4);
        assert!(collect(i64::MIN, step, n, 1, 8192).await.is_empty());
    }

    #[test]
    fn test_partition_bounds() {
        // range(0, 10, 1, 3): Spark puts 3, 3 and 4 values in the three partitions.
        assert_eq!(partition_bounds(0, 1, 10, 3, 0), (0, 3));
        assert_eq!(partition_bounds(0, 1, 10, 3, 1), (3, 3));
        assert_eq!(partition_bounds(0, 1, 10, 3, 2), (6, 4));
        // More slices than values leaves some partitions empty.
        assert_eq!(partition_bounds(0, 1, 10, 15, 0), (0, 0));
        assert_eq!(partition_bounds(0, 1, 10, 15, 14), (9, 1));
    }
}
