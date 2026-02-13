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

use crate::execution::shuffle::metrics::ShufflePartitionerMetrics;
use crate::execution::shuffle::partitioners::ShufflePartitioner;
use crate::execution::shuffle::writers::BufBatchWriter;
use crate::execution::shuffle::{CompressionCodec, ShuffleBlockWriter};
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::common::DataFusionError;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use tokio::time::Instant;

/// A partitioner that writes all shuffle data to a single file and a single index file
pub(crate) struct SinglePartitionShufflePartitioner {
    // output_data_file: File,
    output_data_writer: BufBatchWriter<ShuffleBlockWriter, File>,
    output_index_path: String,
    /// Batches that are smaller than the batch size and to be concatenated
    buffered_batches: Vec<RecordBatch>,
    /// Number of rows in the concatenating batches
    num_buffered_rows: usize,
    /// Metrics for the repartitioner
    metrics: ShufflePartitionerMetrics,
    /// The configured batch size
    batch_size: usize,
}

impl SinglePartitionShufflePartitioner {
    pub(crate) fn try_new(
        output_data_path: String,
        output_index_path: String,
        schema: SchemaRef,
        metrics: ShufflePartitionerMetrics,
        batch_size: usize,
        codec: CompressionCodec,
        write_buffer_size: usize,
    ) -> datafusion::common::Result<Self> {
        let shuffle_block_writer = ShuffleBlockWriter::try_new(schema.as_ref(), codec.clone())?;

        let output_data_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(output_data_path)?;

        let output_data_writer = BufBatchWriter::new(
            shuffle_block_writer,
            output_data_file,
            write_buffer_size,
            batch_size,
        );

        Ok(Self {
            output_data_writer,
            output_index_path,
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
impl ShufflePartitioner for SinglePartitionShufflePartitioner {
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
                    self.output_data_writer.write(
                        &batch,
                        &self.metrics.encode_time,
                        &self.metrics.write_time,
                    )?;
                }

                if num_rows >= self.batch_size {
                    // Write the new batch
                    self.output_data_writer.write(
                        &batch,
                        &self.metrics.encode_time,
                        &self.metrics.write_time,
                    )?;
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
            self.output_data_writer.write(
                &batch,
                &self.metrics.encode_time,
                &self.metrics.write_time,
            )?;
        }
        self.output_data_writer
            .flush(&self.metrics.encode_time, &self.metrics.write_time)?;

        // Write index file. It should only contain 2 entries: 0 and the total number of bytes written
        let index_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(self.output_index_path.clone())
            .map_err(|e| DataFusionError::Execution(format!("shuffle write error: {e:?}")))?;
        let mut index_buf_writer = BufWriter::new(index_file);
        let data_file_length = self.output_data_writer.writer_stream_position()?;
        for offset in [0, data_file_length] {
            index_buf_writer.write_all(&(offset as i64).to_le_bytes()[..])?;
        }
        index_buf_writer.flush()?;

        self.metrics
            .baseline
            .elapsed_compute()
            .add_duration(start_time.elapsed());
        Ok(())
    }
}
