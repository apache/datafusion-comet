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
use crate::writers::partition_writer::PartitionWriter;
use arrow::array::RecordBatch;
use datafusion::common::DataFusionError;
use std::iter;
use tokio::time::Instant;

/// A partitioner that writes all shuffle data to a single file and a single index file
pub(crate) struct SinglePartitionShufflePartitioner<T: PartitionWriter> {
    partition_writer: T,
    /// Batches that are smaller than the batch size and to be concatenated
    buffered_batches: Vec<RecordBatch>,
    /// Number of rows in the concatenating batches
    num_buffered_rows: usize,
    /// Metrics for the repartitioner
    metrics: ShufflePartitionerMetrics,
    /// The configured batch size
    batch_size: usize,
}

impl<T: PartitionWriter> SinglePartitionShufflePartitioner<T> {
    pub(crate) fn try_new(
        partition_writer: T,
        metrics: ShufflePartitionerMetrics,
        batch_size: usize,
    ) -> datafusion::common::Result<Self> {
        Ok(Self {
            partition_writer,
            buffered_batches: vec![],
            num_buffered_rows: 0,
            metrics,
            batch_size,
        })
    }

    /// Add a batch to the buffer of the partitioner, these buffered batches will be concatenated
    /// and written to the output data file when the number of rows in the buffer reaches the batch size.
    fn add_buffered_batch(&mut self, batch: RecordBatch) {
        self.num_buffered_rows += batch.num_rows();
        self.buffered_batches.push(batch);
    }

    /// Consumes buffered batches and return a concatenated batch if successful
    fn concat_buffered_batches(&mut self) -> datafusion::common::Result<Option<RecordBatch>> {
        if self.buffered_batches.is_empty() {
            Ok(None)
        } else if self.buffered_batches.len() == 1 {
            let batch = self.buffered_batches.remove(0);
            self.num_buffered_rows = 0;
            Ok(Some(batch))
        } else {
            let schema = &self.buffered_batches[0].schema();
            match arrow::compute::concat_batches(schema, self.buffered_batches.iter()) {
                Ok(concatenated) => {
                    self.buffered_batches.clear();
                    self.num_buffered_rows = 0;
                    Ok(Some(concatenated))
                }
                Err(e) => Err(DataFusionError::ArrowError(
                    Box::from(e),
                    Some(DataFusionError::get_back_trace()),
                )),
            }
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

            if num_rows >= self.batch_size || num_rows + self.num_buffered_rows > self.batch_size {
                let concatenated_batch = self.concat_buffered_batches()?;

                // Write the concatenated buffered batch
                if let Some(batch) = concatenated_batch {
                    self.partition_writer
                        .write(0, &mut iter::once(Ok(batch)), &self.metrics)?;
                }

                if num_rows >= self.batch_size {
                    // Write the new batch
                    self.partition_writer
                        .write(0, &mut iter::once(Ok(batch)), &self.metrics)?;
                } else {
                    // Add the new batch to the buffer
                    self.add_buffered_batch(batch);
                }
            } else {
                self.add_buffered_batch(batch);
            }
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
        let concatenated_batch = self.concat_buffered_batches()?;

        // Write the concatenated buffered batch
        if let Some(batch) = concatenated_batch {
            self.partition_writer
                .write(0, &mut iter::once(Ok(batch)), &self.metrics)?;
        }
        self.partition_writer.finish_all(&self.metrics)?;

        self.metrics
            .baseline
            .elapsed_compute()
            .add_duration(start_time.elapsed());
        Ok(())
    }
}
