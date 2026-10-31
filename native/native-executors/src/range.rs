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

//! Range exchanges: the split points of a range shuffle, found by sampling the exchange's input
//! as Spark's `RangePartitioner` does.
//!
//! The scheduler runs a sampling job over the stage that feeds a range exchange: a
//! [`SampleExec`] over each of its partitions, gathered to one partition. It then sorts the
//! sample by the exchange's sort keys and takes evenly spaced rows as the split points of a
//! [`RangePartitioning`](datafusion::physical_expr::Partitioning::Range), which Ballista plans as a
//! range shuffle. Like Spark's, the sampling job runs the producing stage once more.

use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::{
    concat_batches, interleave_record_batch, lexsort_to_indices, take_record_batch, SortColumn,
    SortOptions,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{internal_datafusion_err, Result, ScalarValue, SplitPoint};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;

/// How many rows to sample from each of `input_partitions` partitions to range-partition their
/// rows into `output_partitions`, as Spark's `RangePartitioner` decides.
pub fn sample_size_per_partition(output_partitions: usize, input_partitions: usize) -> usize {
    let sample_size = (20 * output_partitions).min(1_000_000);
    (3 * sample_size).div_ceil(input_partitions.max(1))
}

/// A uniform sample of up to `per_partition` rows of each partition of its input, keeping only
/// the columns `keys`, by reservoir sampling.
#[derive(Debug)]
pub struct SampleExec {
    input: Arc<dyn ExecutionPlan>,
    keys: Vec<usize>,
    per_partition: usize,
    schema: SchemaRef,
    props: Arc<PlanProperties>,
}

impl SampleExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        keys: Vec<usize>,
        per_partition: usize,
    ) -> Result<Self> {
        let schema = Arc::new(input.schema().project(&keys)?);
        let props = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(input.output_partitioning().partition_count()),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            keys,
            per_partition,
            schema,
            props,
        })
    }

    pub fn keys(&self) -> &[usize] {
        &self.keys
    }

    pub fn per_partition(&self) -> usize {
        self.per_partition
    }
}

impl DisplayAs for SampleExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SampleExec: keys={:?}, per_partition={}",
            self.keys, self.per_partition
        )
    }
}

impl ExecutionPlan for SampleExec {
    fn name(&self) -> &str {
        "SampleExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    /// The keys are column indexes, so there are no expressions to visit.
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input] = <[_; 1]>::try_from(children)
            .map_err(|_| internal_datafusion_err!("SampleExec takes one input"))?;
        Ok(Arc::new(Self::try_new(
            input,
            self.keys.clone(),
            self.per_partition,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mut input = self.input.execute(partition, context)?;
        let keys = self.keys.clone();
        let capacity = self.per_partition;
        let schema = Arc::clone(&self.schema);
        let stream = futures::stream::once(async move {
            let mut reservoir = RecordBatch::new_empty(Arc::clone(&schema));
            let mut rng = XorShift::new(partition as u64);
            // Rows of the partition seen so far.
            let mut seen: u64 = 0;
            while let Some(batch) = input.next().await {
                let batch = batch?.project(&keys)?;
                // Where each row of the new reservoir comes from: (0, row) of the reservoir or
                // (1, row) of the batch.
                let mut indices: Vec<(usize, usize)> =
                    (0..reservoir.num_rows()).map(|row| (0, row)).collect();
                let mut changed = false;
                for row in 0..batch.num_rows() {
                    if indices.len() < capacity {
                        indices.push((1, row));
                        changed = true;
                    } else {
                        let slot = rng.below(seen + 1) as usize;
                        if slot < capacity {
                            indices[slot] = (1, row);
                            changed = true;
                        }
                    }
                    seen += 1;
                }
                if changed {
                    reservoir = interleave_record_batch(&[&reservoir, &batch], &indices)?;
                }
            }
            Ok(reservoir)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        )))
    }
}

/// A small deterministic random number generator, so that a query samples the same rows each
/// time it runs.
struct XorShift(u64);

impl XorShift {
    fn new(seed: u64) -> Self {
        // Any nonzero state works; mix the seed so that neighbouring partitions differ.
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    /// A number in `0..bound`.
    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }
}

/// The split points that cut `samples`, rows of the exchange's sort keys, into `partitions`
/// ranges of about the same number of rows, in the order `options` gives each key. Equal
/// candidates collapse into one, so there can be fewer than `partitions - 1`, and none when the
/// sample is empty.
pub fn split_points(
    schema: &SchemaRef,
    samples: &[RecordBatch],
    options: &[SortOptions],
    partitions: usize,
) -> Result<Vec<SplitPoint>> {
    let batch = concat_batches(schema, samples)?;
    let rows = batch.num_rows();
    if rows == 0 || partitions <= 1 {
        return Ok(vec![]);
    }
    let columns: Vec<SortColumn> = batch
        .columns()
        .iter()
        .zip(options)
        .map(|(values, options)| SortColumn {
            values: Arc::clone(values),
            options: Some(*options),
        })
        .collect();
    let sorted = take_record_batch(&batch, &lexsort_to_indices(&columns, None)?)?;
    let converter = RowConverter::new(
        schema
            .fields()
            .iter()
            .zip(options)
            .map(|(field, options)| {
                SortField::new_with_options(field.data_type().clone(), *options)
            })
            .collect(),
    )?;
    let keys = converter.convert_columns(sorted.columns())?;
    let mut points = vec![];
    let mut last = None;
    for i in 1..partitions {
        let index = i * rows / partitions;
        let key = keys.row(index);
        // Split points must be strictly ordered.
        if last.is_some_and(|last| key <= keys.row(last)) {
            continue;
        }
        last = Some(index);
        let values = sorted
            .columns()
            .iter()
            .map(|column| ScalarValue::try_from_array(column, index))
            .collect::<Result<Vec<_>>>()?;
        points.push(SplitPoint::new(values));
    }
    Ok(points)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;

    fn longs(values: Vec<Option<i64>>) -> (SchemaRef, RecordBatch) {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(values))],
        )
        .unwrap();
        (schema, batch)
    }

    fn values(points: &[SplitPoint]) -> Vec<Vec<ScalarValue>> {
        points.iter().map(|point| point.values().to_vec()).collect()
    }

    #[test]
    fn split_points_cut_a_sample_into_equal_ranges() {
        let (schema, batch) = longs((0..100).rev().map(Some).collect());
        let points = split_points(&schema, &[batch], &[SortOptions::default()], 4).unwrap();
        assert_eq!(
            values(&points),
            [25, 50, 75].map(|v| vec![ScalarValue::Int64(Some(v))])
        );
    }

    #[test]
    fn split_points_follow_the_sort_direction_and_null_order() {
        let mut keys: Vec<Option<i64>> = (0..6).map(Some).collect();
        keys.extend([None, None]);
        let (schema, batch) = longs(keys);
        let descending = SortOptions {
            descending: true,
            nulls_first: true,
        };
        let points = split_points(&schema, &[batch], &[descending], 4).unwrap();
        // Sorted: null, null, 5, 4, 3, 2, 1, 0.
        assert_eq!(
            values(&points),
            [Some(5), Some(3), Some(1)].map(|v| vec![ScalarValue::Int64(v)])
        );
    }

    #[test]
    fn equal_candidates_collapse_into_one_split_point() {
        let (schema, batch) = longs([1, 1, 1, 1, 1, 1, 2, 2].map(Some).to_vec());
        let points = split_points(&schema, &[batch], &[SortOptions::default()], 4).unwrap();
        assert_eq!(
            values(&points),
            [1, 2].map(|v| vec![ScalarValue::Int64(Some(v))])
        );
    }

    #[test]
    fn an_empty_sample_has_no_split_points() {
        let (schema, _) = longs(vec![]);
        let points = split_points(&schema, &[], &[SortOptions::default()], 4).unwrap();
        assert!(points.is_empty());
    }

    #[test]
    fn split_points_order_compound_keys_lexicographically() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["x", "x", "y", "y"])),
                Arc::new(Int64Array::from(vec![2, 1, 2, 1])),
            ],
        )
        .unwrap();
        let options = [
            SortOptions::default(),
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        ];
        let points = split_points(&schema, &[batch], &options, 2).unwrap();
        // Sorted: (x, 2), (x, 1), (y, 2), (y, 1).
        assert_eq!(
            values(&points),
            [vec![
                ScalarValue::Utf8(Some("y".to_string())),
                ScalarValue::Int64(Some(2))
            ]]
        );
    }

    #[tokio::test]
    async fn sample_keeps_up_to_its_size_from_each_partition() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Utf8, false),
        ]));
        let batch = |start: i64| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from_iter_values(start..start + 50)),
                    Arc::new(StringArray::from_iter_values(
                        (start..start + 50).map(|v| v.to_string()),
                    )),
                ],
            )
            .unwrap()
        };
        // Two partitions: one of 100 rows in two batches, and one of 3 rows.
        let partitions = vec![vec![batch(0), batch(50)], vec![batch(1000).slice(0, 3)]];
        let input =
            MemorySourceConfig::try_new_exec(&partitions, Arc::clone(&schema), None).unwrap();
        let sample: Arc<dyn ExecutionPlan> =
            Arc::new(SampleExec::try_new(input, vec![0], 10).unwrap());
        let ctx = SessionContext::new();

        let mut sizes = vec![];
        for partition in 0..2 {
            let batches = datafusion::physical_plan::common::collect(
                sample.execute(partition, ctx.task_ctx()).unwrap(),
            )
            .await
            .unwrap();
            let rows: Vec<i64> = batches
                .iter()
                .flat_map(|batch| {
                    assert_eq!(batch.num_columns(), 1);
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .values()
                        .to_vec()
                })
                .collect();
            let mut distinct = rows.clone();
            distinct.sort();
            distinct.dedup();
            assert_eq!(distinct.len(), rows.len(), "{rows:?} repeats a row");
            sizes.push(rows.len());
        }
        assert_eq!(sizes, [10, 3]);
        // The whole plan collects the same rows.
        assert_eq!(
            collect(sample, ctx.task_ctx())
                .await
                .unwrap()
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            13
        );
    }
}
