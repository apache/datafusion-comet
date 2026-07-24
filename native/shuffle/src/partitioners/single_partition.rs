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

use crate::metrics::ShufflePartitionerMetrics;
use crate::partitioners::ShufflePartitioner;
use crate::writers::PartitionWriter;
use arrow::array::RecordBatch;
use std::iter;
use tokio::time::Instant;

/// A partitioner that writes all shuffle data to a single file and a single index file.
///
/// Batches are streamed straight to the long-lived `BufBatchWriter` inside the
/// [`PartitionWriter`], whose internal `BatchCoalescer` combines sub-`batch_size` batches
/// into `batch_size`-row IPC blocks across calls. A batch that is already `>= batch_size`
/// and lands on an empty coalescer buffer is passed through and written verbatim as a
/// single block, which may exceed `batch_size` (see the `BatchCoalescer` bypass in
/// `BufBatchWriter`). Block boundaries therefore depend on how the input is chunked, but
/// every row is written exactly once in order. The partitioner does no buffering or
/// concatenation of its own; doing so would copy every row an extra time before the
/// coalescer handled it.
pub(crate) struct SinglePartitionShufflePartitioner<T: PartitionWriter> {
    partition_writer: T,
    /// Metrics for the repartitioner
    metrics: ShufflePartitionerMetrics,
}

impl<T: PartitionWriter> SinglePartitionShufflePartitioner<T> {
    pub(crate) fn new(partition_writer: T, metrics: ShufflePartitionerMetrics) -> Self {
        Self {
            partition_writer,
            metrics,
        }
    }
}

#[async_trait::async_trait]
impl<T: PartitionWriter> ShufflePartitioner for SinglePartitionShufflePartitioner<T> {
    async fn insert_batch(&mut self, batch: RecordBatch) -> datafusion::common::Result<()> {
        let start_time = Instant::now();
        let num_rows = batch.num_rows();

        if num_rows > 0 {
            self.metrics.data_size.add(batch.get_array_memory_size());
            self.metrics.baseline.record_output(num_rows);

            // Stream directly to the writer; its BatchCoalescer handles batching to batch_size.
            self.partition_writer
                .write(0, &mut iter::once(Ok(batch)), &self.metrics)?;
        }

        self.metrics.input_batches.add(1);
        self.metrics
            .baseline
            .elapsed_compute()
            .add_duration(start_time.elapsed());
        Ok(())
    }

    fn shuffle_write(&mut self) -> datafusion::common::Result<()> {
        let start_time = Instant::now();

        self.partition_writer
            .finish_partition(0, &mut iter::empty(), &self.metrics)?;

        self.partition_writer.finish_all(&self.metrics)?;

        self.metrics
            .baseline
            .elapsed_compute()
            .add_duration(start_time.elapsed());
        Ok(())
    }
}
