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

use arrow::array::{ArrayRef, UInt64Array};
use arrow::compute::{take_arrays, TakeOptions};
use arrow::row::{Row, RowConverter, Rows, SortField};
use datafusion::physical_expr::LexOrdering;
use rand::{rngs::SmallRng, Rng, SeedableRng};

pub struct RangePartitioner;

impl RangePartitioner {
    /// Given a number of rows, sample size, and a random seed, generates unique indices to take
    /// from an input batch to act as a random sample.
    /// Adapted from https://en.wikipedia.org/wiki/Reservoir_sampling#Optimal:_Algorithm_L
    /// We use sample_size instead of k and num_rows instead of n.
    /// We use indices instead of actual values in the reservoir  since we'll do one take() on the
    /// input arrays at the end.
    pub fn reservoir_sample_indices(num_rows: usize, sample_size: usize, seed: u64) -> Vec<u64> {
        assert!(sample_size > 0);
        assert!(
            num_rows > sample_size,
            "Sample size > num_rows yields original batch."
        );

        // Initialize our reservoir with indices of the first |sample_size| elements.
        let mut reservoir: Vec<u64> = (0..sample_size as u64).collect();

        let mut rng = SmallRng::seed_from_u64(seed);
        let mut w = (rng.random::<f64>().ln() / sample_size as f64).exp();
        let mut i = sample_size - 1;

        while i < num_rows {
            i += (rng.random::<f64>().ln() / (1.0 - w).ln()).floor() as usize + 1;

            if i < num_rows {
                // Replace a random item in the reservoir with i
                let random_index = rng.random_range(0..sample_size);
                reservoir[random_index] = i as u64;
                w *= (rng.random::<f64>().ln() / sample_size as f64).exp();
            }
        }

        reservoir
    }

    /// Given a batch of Rows, an ordered vector of Rows that represent partition boundaries, and
    /// a slice with enough space for the input batch, determines a partition id for every input
    /// Row using binary search.
    pub fn partition_indices_for_batch(
        row_batch: &Rows,
        partition_bounds_vec: &Vec<Row>,
        partition_ids: &mut [u32],
    ) {
        row_batch.iter().enumerate().for_each(|(row_idx, row)| {
            partition_ids[row_idx] =
                partition_bounds_vec.partition_point(|bound| *bound <= row) as u32
        });
    }

    /// Given input arrays and range partitioning metadata: samples the input arrays, generates
    /// partition bounds, and returns Rows (for comparison against) and a RowConverter (for
    /// adapting future incoming batches).
    pub fn generate_bounds(
        partition_arrays: &Vec<ArrayRef>,
        lex_ordering: &LexOrdering,
        num_output_partitions: usize,
        num_rows: usize,
        sample_size: usize,
        seed: u64,
    ) -> (Rows, RowConverter) {
        let sampled_columns = if sample_size < num_rows {
            // Construct our sample indices.
            let sample_indices = UInt64Array::from(RangePartitioner::reservoir_sample_indices(
                num_rows,
                sample_size,
                seed,
            ));

            // Extract our sampled data from the input data.
            take_arrays(
                partition_arrays,
                &sample_indices,
                Some(TakeOptions {
                    check_bounds: false,
                }),
            )
            .unwrap()
        } else {
            // Requested sample_size is larger than the batch, so just use the batch.
            partition_arrays.clone()
        };

        // Generate our bounds indices.
        let sort_fields: Vec<SortField> = partition_arrays
            .iter()
            .zip(lex_ordering)
            .map(|(array, sort_expr)| {
                SortField::new_with_options(array.data_type().clone(), sort_expr.options)
            })
            .collect();

        let (bounds_indices, row_converter) = RangePartitioner::determine_bounds_for_rows(
            sort_fields,
            sampled_columns.as_slice(),
            num_output_partitions,
        );

        // Extract our bounds data from the sampled data.
        let bounds_indices_array = UInt64Array::from(bounds_indices);
        let bounds_arrays = take_arrays(
            sampled_columns.as_slice(),
            &bounds_indices_array,
            Some(TakeOptions {
                check_bounds: false,
            }),
        )
        .unwrap();

        // Convert the bounds data to Rows and return with RowConverter.
        (
            row_converter
                .convert_columns(bounds_arrays.as_slice())
                .unwrap(),
            row_converter,
        )
    }

    /// Given a sort ordering, sampled data, and a number of target partitions, finds the partition
    /// bounds and returns them as indices into the sampled data.
    /// Adapted from org.apache.spark.RangePartitioner.determineBounds but without weighted
    /// values since we don't have cross-partition samples to merge.
    pub fn determine_bounds_for_rows(
        sort_fields: Vec<SortField>,
        sampled_columns: &[ArrayRef],
        partitions: usize,
    ) -> (Vec<u64>, RowConverter) {
        assert!(partitions > 1);

        let converter = RowConverter::new(sort_fields).unwrap();
        let sampled_rows = converter.convert_columns(sampled_columns).unwrap();
        let mut sorted_sampled_rows: Vec<(usize, Row)> = sampled_rows.iter().enumerate().collect();
        sorted_sampled_rows.sort_unstable_by(|(_, a), (_, b)| a.cmp(b));

        let num_candidates = sampled_rows.num_rows();
        let step = 1.0 / partitions as f64;
        let mut cumulative_weights = 0.0;
        let mut target = step;
        let mut bounds_indices: Vec<u64> = Vec::with_capacity(partitions - 1);
        let mut i = 0;
        let mut j = 0;
        let mut previous_bound = None;
        let sample_weight = 1.0 / num_candidates as f64;
        while (i < num_candidates) && (j < partitions - 1) {
            let key = sorted_sampled_rows[i];
            cumulative_weights += sample_weight;
            if cumulative_weights >= target {
                // Skip duplicate values.
                if previous_bound.is_none() || key.1 > previous_bound.unwrap() {
                    bounds_indices.push(key.0 as u64);
                    target += step;
                    j += 1;
                    previous_bound = Some(key.1)
                }
            }
            i += 1
        }

        (bounds_indices, converter)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow::array::{Array, AsArray, Int64Array, RecordBatch, UInt64Array};
    use arrow::compute::take_record_batch;
    use arrow::datatypes::DataType::{Float64, Int64};
    use arrow::datatypes::{Field, Float64Type, Int32Type, Int64Type, Schema};
    use datafusion::common::record_batch;
    use itertools::Itertools;
    use std::sync::Arc;

    fn sample_batch(input: RecordBatch, indices: Vec<u64>) -> RecordBatch {
        let indices = UInt64Array::from(indices);
        take_record_batch(&input, &indices).unwrap()
    }

    fn check_sample_indices(indices: &[u64], batch_size: usize, sample_size: usize) {
        // sample indices size should never exceed the batch size
        assert!(indices.len() <= batch_size);
        // number of samples should be the smaller of batch size and sample size
        assert_eq!(indices.len(), batch_size.min(sample_size));
        // Check that indices are not out of bounds
        indices
            .iter()
            .for_each(|&idx| assert!(idx < batch_size as u64));
        // Check that values are distinct
        let sorted_indices = indices.iter().sorted().collect_vec();
        assert_eq!(
            sorted_indices.len(),
            sorted_indices.iter().dedup().collect_vec().len()
        );
    }

    fn check_bounds_indices(indices: &[u64], sample_size: usize) {
        // bounds indices size should never exceed the sample size
        assert!(indices.len() <= sample_size);
        // Check that indices are not out of bounds
        indices
            .iter()
            .for_each(|&idx| assert!(idx < sample_size as u64));
        // Check that values are distinct
        let sorted_indices = indices.iter().sorted().collect_vec();
        assert_eq!(
            sorted_indices.len(),
            sorted_indices.iter().dedup().collect_vec().len()
        );
    }

    #[test]
    // We want to verify that with hand-written bounds for a distribution of data that we get
    // reasonable partition indices for a random batch. For this scenario, we create a full
    // RecordBatch with one partition column. The values in the column are uniform randomly
    // distributed between [0,10). We request 10 partitions with bounds of [1,2,3,4,5,6,7,8,9],
    // and the result should be 10 bins with reasonably close counts.
    fn partition_indices_for_batch() {
        let sort_fields = vec![SortField::new(Int64)];
        let row_converter = RowConverter::new(sort_fields).unwrap();
        let mut partition_ids = vec![0u32; 8192];
        let mut partition_counts = [0u32; 10];

        let input_batch = create_random_batch(8192, false, Some((0, 10)));
        let bounds = record_batch!(("a", Int64, (1..=9).collect_vec())).unwrap();

        let input_rows = row_converter
            .convert_columns(input_batch.columns())
            .unwrap();

        let bounds_rows = row_converter.convert_columns(bounds.columns()).unwrap();

        let bounds_rows_vec = bounds_rows.iter().collect_vec();

        RangePartitioner::partition_indices_for_batch(
            &input_rows,
            &bounds_rows_vec,
            &mut partition_ids,
        );

        partition_ids
            .iter()
            .for_each(|&partition_id| partition_counts[partition_id as usize] += 1);

        // The RecordBatch won't be perfectly distributed, so for 8192 / 10 possible values we
        // check that each partition just has >700 values in them.
        partition_counts
            .iter()
            .for_each(|&partition_count| assert!(partition_count > 700));
    }

    #[test]
    // We want to verify that reservoir sampling yields valid indices for different size input
    // batches. We randomly generate batch sizes and sample sizes, and then construct reservoir
    // samples for each scenario. Finally, we validate the indices.
    fn reservoir_sample_random() {
        let mut rng = SmallRng::seed_from_u64(42);

        for _ in 0..8192 {
            let batch_size: usize = rng.random_range(1..=8192);
            // We don't test sample size > batch_size since in that case you would just take the
            // entire batch as the sample.
            let sample_size: usize = rng.random_range(1..batch_size);
            let indices = RangePartitioner::reservoir_sample_indices(batch_size, sample_size, 42);

            check_sample_indices(&indices, batch_size, sample_size);
        }
    }

    #[test]
    // org.apache.spark.util.random.SamplingUtilsSuite
    // "SPARK-18678 reservoirSampleAndCount with tiny input"
    fn reservoir_sample_and_count_with_tiny_input() {
        let batch = record_batch!(("a", Int32, vec![0, 1])).unwrap();
        let mut counts: Vec<i32> = vec![0; 2];
        for i in 0..500 {
            let indices = RangePartitioner::reservoir_sample_indices(batch.num_rows(), 1, i);
            let result = sample_batch(batch.clone(), indices);
            assert_eq!(result.num_rows(), 1);
            counts[result.column(0).as_primitive::<Int32Type>().value(0) as usize] += 1;
        }
        // If correct, should be true with prob ~ 0.99999707 according to original Spark test.
        assert!((counts[0] - counts[1]).abs() <= 100)
    }

    #[test]
    // org.apache.spark.PartitioningSuite
    // "RangePartitioner.determineBounds"
    fn determine_bounds_for_rows() {
        // The original test had weights on the values. We just duplicate them because our
        // determine_bounds function is unweighted.
        let batch = record_batch!((
            "a",
            Float64,
            vec![
                Some(0.7),
                Some(0.7),
                Some(0.1),
                Some(0.4),
                Some(0.3),
                Some(0.2),
                Some(0.5),
                Some(1.0),
                Some(1.0),
                Some(1.0),
            ]
        ))
        .unwrap();

        let sort_fields = vec![SortField::new(Float64)];

        let (rows, _) =
            RangePartitioner::determine_bounds_for_rows(sort_fields, batch.columns(), 3);

        check_bounds_indices(rows.as_slice(), batch.num_rows());

        assert_eq!(rows.len(), 2);

        let indices = UInt64Array::from(rows);

        let bounds = take_record_batch(&batch, &indices).unwrap();
        let bounds_array = bounds.column(0).as_primitive::<Float64Type>();
        assert_eq!(bounds_array.values(), &[0.4, 0.7]);
    }

    #[test]
    // We want to verify that determining bounds yields valid indices for different size sample
    // batches. We randomly generate batches and number of partitions, and then construct
    // bounds for each scenario. Finally, we validate the indices.
    fn determine_bounds_random() {
        let mut rng = SmallRng::seed_from_u64(42);

        let sort_fields = vec![SortField::new(Int64)];

        for _ in 0..2048 {
            let batch_size = rng.random_range(0..=8192);
            // We don't test fewer than 2 partitions since this is used by the
            // MultiPartitionShuffleRepartitioner which is for >1 partitions.
            let num_partitions = rng.random_range(2..=10000);

            let batch = create_random_batch(batch_size, false, None);

            let (rows, _) = RangePartitioner::determine_bounds_for_rows(
                sort_fields.clone(),
                batch.columns(),
                num_partitions,
            );

            check_bounds_indices(rows.as_slice(), batch_size as usize);

            let rows_array = UInt64Array::from(rows);

            let bounds = take_record_batch(&batch, &rows_array).unwrap();

            let bounds_vec: Vec<i64> = bounds
                .column(0)
                .as_primitive::<Int64Type>()
                .values()
                .to_vec();

            // Bounds should be sorted.
            assert!(bounds_vec.is_sorted());
            // Bounds should be unique.
            assert_eq!(
                bounds_vec.len(),
                bounds_vec.iter().dedup().collect_vec().len()
            );
        }
    }

    #[test]
    // We want to make sure that finding bounds works with nulls. DF has more exhaustive tests for
    // sorting with nulls, so we defer to those for more coverage. This is just a small
    // deterministic test to verify that nulls can be partition boundaries.
    fn determine_bounds_with_nulls() {
        let batch = record_batch!(("a", Float64, vec![None, None, Some(0.1),])).unwrap();

        let sort_fields = vec![SortField::new(Float64)];

        let (rows, _) =
            RangePartitioner::determine_bounds_for_rows(sort_fields, batch.columns(), 2);

        assert_eq!(rows.len(), 1);

        let indices = UInt64Array::from(rows);

        let bounds = take_record_batch(&batch, &indices).unwrap();
        let bounds_array = bounds.column(0).as_primitive::<Float64Type>();
        assert!(bounds_array.is_null(0));
    }

    fn create_random_batch(batch_size: u32, sort: bool, range: Option<(i64, i64)>) -> RecordBatch {
        let mut rng = SmallRng::seed_from_u64(42);
        let mut column: Vec<i64> = if let Some((min, max)) = range {
            assert!(min <= max);
            (0..batch_size)
                .map(|_| rng.random_range(min..max))
                .collect()
        } else {
            (0..batch_size).map(|_| rng.random::<i64>()).collect()
        };
        if sort {
            column.sort();
        }
        let array = Int64Array::from(column);
        let schema = Arc::new(Schema::new(vec![Field::new("a", Int64, true)]));
        RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(array)]).unwrap()
    }
}
