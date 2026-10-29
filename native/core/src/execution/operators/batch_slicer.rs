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

use arrow::array::RecordBatch;

/// Splits the batches a JVM-fed leaf receives so that none is wider than the session batch
/// size. The JVM sizes those batches itself, so this is the one place a smaller native batch
/// size takes effect on them; see `scalar_batch_rows` in `jni_api` for why it can be smaller.
/// Slices share the input's buffers, so splitting copies nothing.
pub(crate) struct BatchSlicer {
    max_rows: usize,
    rest: Option<RecordBatch>,
}

impl BatchSlicer {
    pub fn new(max_rows: usize) -> Self {
        Self {
            max_rows: max_rows.max(1),
            rest: None,
        }
    }

    /// Takes a batch in and returns its first slice, holding the remainder for `pending`.
    pub fn slice(&mut self, batch: RecordBatch) -> RecordBatch {
        if batch.num_rows() <= self.max_rows {
            return batch;
        }
        let head = batch.slice(0, self.max_rows);
        self.rest = Some(batch.slice(self.max_rows, batch.num_rows() - self.max_rows));
        head
    }

    /// The next slice of the batch last passed to `slice`, if any of it is left.
    pub fn pending(&mut self) -> Option<RecordBatch> {
        self.rest.take().map(|rest| self.slice(rest))
    }
}

#[cfg(test)]
mod tests {
    use super::BatchSlicer;
    use arrow::array::{Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn batch(n: i32) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from_iter_values(0..n))]).unwrap()
    }

    fn values(batch: &RecordBatch) -> Vec<i32> {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    #[test]
    fn passes_a_batch_within_the_limit_through() {
        let mut slicer = BatchSlicer::new(4);
        assert_eq!(values(&slicer.slice(batch(4))), vec![0, 1, 2, 3]);
        assert!(slicer.pending().is_none());
    }

    #[test]
    fn splits_a_wider_batch_into_slices_of_at_most_the_limit() {
        let mut slicer = BatchSlicer::new(4);
        assert_eq!(values(&slicer.slice(batch(10))), vec![0, 1, 2, 3]);
        assert_eq!(values(&slicer.pending().unwrap()), vec![4, 5, 6, 7]);
        assert_eq!(values(&slicer.pending().unwrap()), vec![8, 9]);
        assert!(slicer.pending().is_none());
    }
}
