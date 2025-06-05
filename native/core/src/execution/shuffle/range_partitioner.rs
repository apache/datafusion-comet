use arrow::array::RecordBatch;
use num::ToPrimitive;

struct RangePartitioner {}

impl RangePartitioner {
    // Adapted from org.apache.spark.RangePartitioner.sketch
    fn sketch(input: RecordBatch, sample_size_per_partition: i32) {}

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
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    // org.apache.spark.PartitioningSuite
    // "RangPartitioner.sketch"
    fn sketch_test() {
        let batch = create_batch(30);
        println!("{:?}", batch);
        RangePartitioner::sketch(batch, 30);
    }

    #[test]
    // org.apache.spark.PartitioningSuite
    // "RangePartitioner.determineBounds"
    fn determine_bounds_test() {
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

    fn create_batch(batch_size: i32) -> RecordBatch {
        let column: Vec<i32> = (0..batch_size).collect();
        let array = Int32Array::from(column);
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(array)]).unwrap()
    }
}
