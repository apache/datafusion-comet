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
use rand::Rng;

pub struct RangePartitioner;

impl RangePartitioner {
    // Adapted from https://en.wikipedia.org/wiki/Reservoir_sampling#Optimal:_Algorithm_L
    // We use sample_size instead of k and input_length instead of n.
    // We use indices in the reservoir instead of actual values since we'll do one take() on the
    // input array at the end.
    pub fn reservoir_sample_indices(num_rows: usize, sample_size: usize) -> Vec<u64> {
        assert!(sample_size > 0);

        if num_rows <= sample_size {
            // Just return the original input since we can't create a bigger sample.
            return (0..num_rows as u64).collect();
        }

        // Initialize our reservoir with indices of the first |sample_size| elements.
        let mut reservoir: Vec<u64> = (0..sample_size as u64).collect();

        let mut rng = rand::rng();
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

    pub fn generate_bounds(
        partition_arrays: &Vec<ArrayRef>,
        lex_ordering: &LexOrdering,
        num_output_partitions: usize,
        num_rows: usize,    // TODO: u16
        sample_size: usize, // TODO: u16
    ) -> (Rows, RowConverter) {
        let sample_indices = UInt64Array::from(RangePartitioner::reservoir_sample_indices(
            num_rows,
            sample_size,
        ));

        let sampled_columns = take_arrays(
            partition_arrays,
            &sample_indices,
            Some(TakeOptions {
                check_bounds: false,
            }),
        )
        .unwrap();

        let sort_fields: Vec<SortField> = partition_arrays
            .iter()
            .zip(lex_ordering)
            .map(|(array, sort_expr)| {
                SortField::new_with_options(array.data_type().clone(), sort_expr.options)
            })
            .collect();

        let (bounds_indices, row_converter) = RangePartitioner::determine_bounds_for_rows(
            sort_fields,
            sampled_columns,
            num_output_partitions,
        );

        let bounds_indices_array = UInt64Array::from(bounds_indices);

        let bounds_arrays = take_arrays(
            partition_arrays,
            &bounds_indices_array,
            Some(TakeOptions {
                check_bounds: false,
            }),
        )
        .unwrap();

        (
            row_converter
                .convert_columns(bounds_arrays.as_slice())
                .unwrap(),
            row_converter,
        )
    }

    // Adapted from org.apache.spark.RangePartitioner.determineBounds
    pub fn determine_bounds_for_rows(
        sort_fields: Vec<SortField>,
        sampled_columns: Vec<ArrayRef>,
        partitions: usize,
    ) -> (Vec<u64>, RowConverter) {
        assert!(partitions > 1);

        let converter = RowConverter::new(sort_fields).unwrap();
        let sampled_rows = converter
            .convert_columns(sampled_columns.as_slice())
            .unwrap();
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
                    // bounds.push(key.1);
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
    use datafusion::common::{record_batch, HashSet};
    use itertools::Itertools;
    use std::sync::Arc;

    fn sample_batch(input: RecordBatch, indices: Vec<u64>) -> RecordBatch {
        let indices = UInt64Array::from(indices);
        take_record_batch(&input, &indices).unwrap()
    }

    fn check_indices(indices: &Vec<u64>, batch_size: usize, sample_size: usize) {
        // sample indices size should never exceed the batch size
        assert!(indices.len() <= batch_size);
        assert_eq!(indices.len(), batch_size.min(sample_size));
        // Check that values are distinct and not out of bounds
        indices
            .iter()
            .for_each(|&idx| assert!(idx < batch_size as u64));
        // Check that values are distinct and not out of bounds
        let sorted_indices = indices.iter().sorted().collect_vec();
        assert_eq!(
            sorted_indices.len(),
            sorted_indices.iter().dedup().collect_vec().len()
        );
    }

    #[test]
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

        partition_counts
            .iter()
            .for_each(|&partition_count| assert!(partition_count > 700));
    }

    #[test]
    fn reservoir_sample_fuzz() {
        let mut rng = rand::rng();

        for _ in 0..1000 {
            let batch_size: usize = rng.random_range(0..=8192);
            let sample_size: usize = rng.random_range(1..=8192);
            let reservoir = RangePartitioner::reservoir_sample_indices(batch_size, sample_size);

            assert_eq!(reservoir.len(), sample_size.min(batch_size));

            let mut set: HashSet<u64> = HashSet::with_capacity(sample_size);
            reservoir.iter().for_each(|&idx| {
                assert!(idx < batch_size as u64);
                assert!(set.insert(idx));
            });
        }
    }

    #[test]
    // org.apache.spark.util.random.SamplingUtilsSuite
    // "reservoirSampleAndCount"
    fn reservoir_sample() {
        let batch = create_random_batch(100, false, None);
        // sample_size > batch.num_rows returns entire batch after sampling
        let sample1_indices = RangePartitioner::reservoir_sample_indices(batch.num_rows(), 150);
        check_indices(&sample1_indices, batch.num_rows(), 150);
        assert_eq!(batch, sample_batch(batch.clone(), sample1_indices));
        // sample_size == batch.num_rows returns entire batch after sampling
        let sample2_indices = RangePartitioner::reservoir_sample_indices(batch.num_rows(), 100);
        check_indices(&sample2_indices, batch.num_rows(), 100);
        assert_eq!(batch, sample_batch(batch.clone(), sample2_indices));
        // sample_size < batch.num_rows returns a random subset, so can't compare to original batch
        let sample3_indices = RangePartitioner::reservoir_sample_indices(batch.num_rows(), 10);
        check_indices(&sample3_indices, batch.num_rows(), 10);
        assert_eq!(sample3_indices.len(), 10);
    }

    #[test]
    // org.apache.spark.util.random.SamplingUtilsSuite
    // "SPARK-18678 reservoirSampleAndCount with tiny input"
    fn reservoir_sample_and_count_with_tiny_input() {
        let batch = record_batch!(("a", Int32, vec![0, 1])).unwrap();
        let mut counts: Vec<i32> = vec![0; 2];
        for _i in 0..500 {
            let indices = RangePartitioner::reservoir_sample_indices(batch.num_rows(), 1);
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
            RangePartitioner::determine_bounds_for_rows(sort_fields, Vec::from(batch.columns()), 3);

        assert_eq!(rows.len(), 2);

        let indices = UInt64Array::from(rows);

        let bounds = take_record_batch(&batch, &indices).unwrap();
        let bounds_array = bounds.column(0).as_primitive::<Float64Type>();
        assert_eq!(bounds_array.values(), &[0.4, 0.7]);
    }

    #[test]
    fn determine_bounds_sizes() {
        let batch = record_batch!(("a", Float64, vec![Some(0.1), Some(0.2), Some(0.3),])).unwrap();

        let sort_fields = vec![SortField::new(Float64)];

        // num_partitions < sample size
        let mut num_partitions = batch.num_rows() - 1;
        let (rows, _) = RangePartitioner::determine_bounds_for_rows(
            sort_fields.clone(),
            Vec::from(batch.columns()),
            num_partitions,
        );
        assert_eq!(rows.len(), num_partitions - 1);

        // num_partitions == sample size
        num_partitions = batch.num_rows();
        let (rows, _) = RangePartitioner::determine_bounds_for_rows(
            sort_fields.clone(),
            Vec::from(batch.columns()),
            num_partitions,
        );
        assert_eq!(rows.len(), num_partitions - 1);

        // num_partitions > sample size
        num_partitions = batch.num_rows() + 1;
        let (rows, _) = RangePartitioner::determine_bounds_for_rows(
            sort_fields.clone(),
            Vec::from(batch.columns()),
            num_partitions,
        );
        assert_eq!(rows.len(), batch.num_rows());
    }

    #[test]
    fn determine_bounds_fuzz() {
        let mut rng = rand::rng();

        let sort_fields = vec![SortField::new(Int64)];

        for _ in 0..1000 {
            let batch_size = rng.random_range(0..=8192);
            let num_partitions = rng.random_range(2..1048576);

            let batch = create_random_batch(batch_size, false, None);

            let (rows, _) = RangePartitioner::determine_bounds_for_rows(
                sort_fields.clone(),
                Vec::from(batch.columns()),
                num_partitions,
            );

            if batch_size < num_partitions as u32 {
                assert_eq!(rows.len(), batch_size as usize);
            } else {
                assert_eq!(rows.len(), (num_partitions - 1) as usize);
            }

            let mut set: HashSet<u64> = HashSet::with_capacity(rows.len());
            rows.iter().for_each(|&idx| {
                assert!(idx < batch_size as u64);
                assert!(set.insert(idx));
            });

            let rows_array = UInt64Array::from(rows);

            let bounds = take_record_batch(&batch, &rows_array).unwrap();

            let bounds_vec: Vec<i64> = bounds
                .column(0)
                .as_primitive::<Int64Type>()
                .values()
                .to_vec();

            assert!(bounds_vec.is_sorted());
        }
    }

    #[test]
    fn determine_bounds_with_nulls() {
        let batch = record_batch!(("a", Float64, vec![None, None, Some(0.1),])).unwrap();

        let sort_fields = vec![SortField::new(Float64)];

        let (rows, _) =
            RangePartitioner::determine_bounds_for_rows(sort_fields, Vec::from(batch.columns()), 2);

        assert_eq!(rows.len(), 1);

        let indices = UInt64Array::from(rows);

        let bounds = take_record_batch(&batch, &indices).unwrap();
        let bounds_array = bounds.column(0).as_primitive::<Float64Type>();
        assert!(bounds_array.is_null(0));
    }

    fn create_random_batch(batch_size: u32, sort: bool, range: Option<(i64, i64)>) -> RecordBatch {
        let mut rng = rand::rng();
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
