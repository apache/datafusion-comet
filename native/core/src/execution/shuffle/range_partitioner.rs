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

use arrow::array::ArrayRef;
use arrow::row::{Row, RowConverter, SortField};
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

    // Adapted from org.apache.spark.RangePartitioner.determineBounds
    pub fn determine_bounds_for_rows(
        sort_fields: Vec<SortField>,
        sampled_columns: Vec<ArrayRef>,
        partitions: i32,
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
        let mut bounds_indices: Vec<u64> = Vec::with_capacity((partitions - 1) as usize);
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
    use arrow::array::{Array, AsArray, Int32Array, Int64Array, RecordBatch, UInt64Array};
    use arrow::compute::take_record_batch;
    use arrow::datatypes::DataType::{Float64, Int64};
    use arrow::datatypes::{DataType, Field, Float64Type, Int32Type, Int64Type, Schema};
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
        let sorted_indices = indices.into_iter().sorted().collect_vec();
        assert_eq!(
            sorted_indices.len(),
            sorted_indices.iter().dedup().collect_vec().len()
        );
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
        let batch = create_random_batch(100);
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
        let column = vec![0, 1];
        let array = Arc::new(Int32Array::from(column));
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![array.clone()]).unwrap();
        let mut counts: Vec<i32> = vec![0; array.len()];
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
        let mut num_partitions = (batch.num_rows() - 1) as i32;
        let (rows, _) = RangePartitioner::determine_bounds_for_rows(
            sort_fields.clone(),
            Vec::from(batch.columns()),
            num_partitions,
        );
        assert_eq!(rows.len() as i32, num_partitions - 1);

        // num_partitions == sample size
        num_partitions = batch.num_rows() as i32;
        let (rows, _) = RangePartitioner::determine_bounds_for_rows(
            sort_fields.clone(),
            Vec::from(batch.columns()),
            num_partitions,
        );
        assert_eq!(rows.len() as i32, num_partitions - 1);

        // num_partitions > sample size
        num_partitions = (batch.num_rows() + 1) as i32;
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
            let batch_size: i32 = rng.random_range(0..=8192);
            let num_partitions: i32 = rng.random_range(2..1048576);

            let batch = create_random_batch(batch_size);

            let (rows, _) = RangePartitioner::determine_bounds_for_rows(
                sort_fields.clone(),
                Vec::from(batch.columns()),
                num_partitions,
            );

            if batch_size < num_partitions {
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

    fn create_random_batch(batch_size: i32) -> RecordBatch {
        let mut rng = rand::rng();
        let column: Vec<i64> = (0..batch_size).map(|_| rng.random::<i64>()).collect();
        let array = Int64Array::from(column);
        let schema = Arc::new(Schema::new(vec![Field::new("a", Int64, true)]));
        RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(array)]).unwrap()
    }
}
