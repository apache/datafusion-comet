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

use arrow::array::{ArrayRef, RecordBatch, UInt64Array};
use arrow::compute::take_record_batch;
use arrow::row::{OwnedRow, Row, RowConverter, SortField};
use datafusion::common::HashSet;
use num::ToPrimitive;
use rand::Rng;

pub struct RangePartitioner;

impl RangePartitioner {
    // Adapted from https://en.wikipedia.org/wiki/Reservoir_sampling#Optimal:_Algorithm_L
    // We use sample_size instead of k and input_length instead of n.
    // We use indices in the reservoir instead of actual values since we'll do one take() on the
    // input array at the end.
    pub fn reservoir_sample(input: RecordBatch, sample_size: usize) -> RecordBatch {
        // Build our indices array and then take the values from the input.
        let indices = UInt64Array::from(Self::reservoir_sample_indices(
            input.num_rows(),
            sample_size,
        ));

        // TODO: This bounds checks, probably not necessary.
        take_record_batch(&input, &indices).unwrap()
    }

    pub fn reservoir_sample_indices(num_rows: usize, sample_size: usize) -> Vec<u64> {
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

        let set: HashSet<u64> = reservoir.iter().map(|e| *e).collect();
        assert_eq!(set.len(), reservoir.len());

        reservoir
    }

    // Adapted from org.apache.spark.RangePartitioner.determineBounds
    fn determine_bounds<T>(mut candidates: Vec<(T, f64)>, partitions: i32) -> Vec<T>
    where
        T: PartialOrd + Copy,
    {
        candidates.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        let num_candidates = candidates.len();
        let sum_weights = candidates
            .iter()
            .map(|x| x.1.to_f64().unwrap())
            .sum::<f64>();
        let step = sum_weights / partitions as f64;
        let mut cumulative_weights = 0.0;
        let mut target = step;
        let mut bounds = Vec::with_capacity((partitions - 1) as usize);
        let mut i = 0;
        let mut j = 0;
        let mut previous_bound: Option<T> = None;
        while (i < num_candidates) && (j < partitions - 1) {
            let (key, weight) = &candidates[i];
            cumulative_weights += weight;
            if cumulative_weights >= target {
                // Skip duplicate values.
                if previous_bound.is_none() || *key > previous_bound.unwrap() {
                    bounds.push(*key);
                    target += step;
                    j += 1;
                    previous_bound = Some(*key)
                }
            }
            i += 1
        }
        bounds
    }

    // Adapted from org.apache.spark.RangePartitioner.determineBounds
    pub fn determine_bounds_for_rows(
        sort_fields: Vec<SortField>,
        sampled_columns: Vec<ArrayRef>,
        partitions: i32,
    ) -> (Vec<OwnedRow>, RowConverter) {
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
        let mut bounds_indices = Vec::with_capacity((partitions - 1) as usize);
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
                    bounds_indices.push(key.0);
                    target += step;
                    j += 1;
                    previous_bound = Some(key.1)
                }
            }
            i += 1
        }

        let mut result: Vec<OwnedRow> = Vec::with_capacity(bounds_indices.len());

        bounds_indices
            .iter()
            .for_each(|idx| result.push(sampled_rows.row(*idx).owned()));

        (result, converter)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow::array::{AsArray, Float64Array, Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use datafusion::physical_expr::expressions::col;
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion::physical_plan::sorts::sort::sort_batch;
    use std::sync::Arc;

    #[test]
    fn test_sort_record_batch() {
        let batch = create_random_batch(10000);
        let sample_size = 20;
        let lex_ordering = LexOrdering::new(vec![PhysicalSortExpr::new_default(
            col("a", batch.schema().as_ref()).unwrap(),
        )]);
        let batch = sort_batch(&batch, &lex_ordering, None).unwrap();
        let sample = RangePartitioner::reservoir_sample(batch, sample_size);
        let sorted_sample = sort_batch(&sample, &lex_ordering, None).unwrap();
    }

    #[test]
    // org.apache.spark.util.random.SamplingUtilsSuite
    // "reservoirSampleAndCount"
    fn reservoir_sample() {
        let batch = create_random_batch(100);
        let sample1 = RangePartitioner::reservoir_sample(batch.clone(), 150);
        assert_eq!(batch, sample1.into());
        let sample2 = RangePartitioner::reservoir_sample(batch.clone(), 100);
        assert_eq!(batch, sample2.into());
        let sample3 = RangePartitioner::reservoir_sample(batch.clone(), 10);
        assert_eq!(sample3.num_rows(), 10);
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
            let result = RangePartitioner::reservoir_sample(batch.clone(), 1);
            assert_eq!(result.num_rows(), 1);
            counts[result.column(0).as_primitive::<Int32Type>().value(0) as usize] += 1;
        }
        // If correct, should be true with prob ~ 0.99999707 according to original Spark test.
        assert!((counts[0] - counts[1]).abs() <= 100)
    }

    #[test]
    // org.apache.spark.PartitioningSuite
    // "RangePartitioner.determineBounds"
    fn determine_bounds() {
        let candidates = vec![
            (0.7, 2.0),
            (0.1, 1.0),
            (0.4, 1.0),
            (0.3, 1.0),
            (0.2, 1.0),
            (0.5, 1.0),
            (1.0, 3.0),
        ];
        let result = RangePartitioner::determine_bounds(candidates, 3);
        assert_eq!(vec![0.4, 0.7], result);
    }

    fn create_random_batch(batch_size: i32) -> RecordBatch {
        let mut rng = rand::rng();
        let column: Vec<f64> = (0..batch_size).map(|_| rng.random::<f64>()).collect();
        let array = Float64Array::from(column);
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Float64, true)]));
        RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(array)]).unwrap()
    }
}
