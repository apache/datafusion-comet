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
use crate::writers::BufBatchWriter;
use crate::{CompressionCodec, IpcStreamWriter, ShuffleBlockWriter, ShuffleFormat};
use arrow::array::RecordBatch;
use arrow::compute::kernels::coalesce::BatchCoalescer;
use arrow::datatypes::SchemaRef;
use datafusion::common::DataFusionError;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use tokio::time::Instant;

/// Output strategy for writing shuffle data in either block or IPC stream format.
enum OutputWriter {
    Block(BufBatchWriter<ShuffleBlockWriter, File>),
    /// The writer is wrapped in Option so it can be taken for finish().
    IpcStream {
        writer: Option<IpcStreamWriter<BufWriter<File>>>,
        coalescer: Option<BatchCoalescer>,
        batch_size: usize,
    },
}

/// A partitioner that writes all shuffle data to a single file and a single index file
pub(crate) struct SinglePartitionShufflePartitioner {
    output: OutputWriter,
    output_data_path: PathBuf,
    output_index_path: String,
    buffered_batches: Vec<RecordBatch>,
    num_buffered_rows: usize,
    metrics: ShufflePartitionerMetrics,
    batch_size: usize,
}

impl SinglePartitionShufflePartitioner {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_new(
        output_data_path: String,
        output_index_path: String,
        schema: SchemaRef,
        metrics: ShufflePartitionerMetrics,
        batch_size: usize,
        codec: CompressionCodec,
        format: ShuffleFormat,
        write_buffer_size: usize,
    ) -> datafusion::common::Result<Self> {
        let output_data_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&output_data_path)?;

        let output = match format {
            ShuffleFormat::Block => {
                let shuffle_block_writer =
                    ShuffleBlockWriter::try_new(schema.as_ref(), codec.clone())?;
                OutputWriter::Block(BufBatchWriter::new(
                    shuffle_block_writer,
                    output_data_file,
                    write_buffer_size,
                    batch_size,
                ))
            }
            ShuffleFormat::IpcStream => {
                let buf_writer =
                    BufWriter::with_capacity(write_buffer_size, output_data_file);
                let writer = IpcStreamWriter::try_new(buf_writer, schema.as_ref(), codec)?;
                OutputWriter::IpcStream {
                    writer: Some(writer),
                    coalescer: None,
                    batch_size,
                }
            }
        };

        Ok(Self {
            output,
            output_data_path: PathBuf::from(output_data_path),
            output_index_path,
            buffered_batches: vec![],
            num_buffered_rows: 0,
            metrics,
            batch_size,
        })
    }

    fn add_buffered_batch(&mut self, batch: RecordBatch) {
        self.num_buffered_rows += batch.num_rows();
        self.buffered_batches.push(batch);
    }

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

    fn write_batch(&mut self, batch: &RecordBatch) -> datafusion::common::Result<()> {
        match &mut self.output {
            OutputWriter::Block(writer) => {
                writer.write(batch, &self.metrics.encode_time, &self.metrics.write_time)?;
                Ok(())
            }
            OutputWriter::IpcStream {
                writer,
                coalescer,
                batch_size,
            } => {
                let w = writer.as_mut().ok_or_else(|| {
                    DataFusionError::Internal("IPC stream writer already finished".to_string())
                })?;
                let coal = coalescer
                    .get_or_insert_with(|| BatchCoalescer::new(batch.schema(), *batch_size));
                coal.push_batch(batch.clone())?;

                while let Some(b) = coal.next_completed_batch() {
                    w.write_batch(&b, &self.metrics.encode_time)?;
                }
                Ok(())
            }
        }
    }

    fn flush_output(&mut self) -> datafusion::common::Result<()> {
        match &mut self.output {
            OutputWriter::Block(writer) => {
                writer.flush(&self.metrics.encode_time, &self.metrics.write_time)
            }
            OutputWriter::IpcStream {
                writer, coalescer, ..
            } => {
                let w = writer.as_mut().ok_or_else(|| {
                    DataFusionError::Internal("IPC stream writer already finished".to_string())
                })?;
                if let Some(coal) = coalescer {
                    coal.finish_buffered_batch()?;
                    while let Some(b) = coal.next_completed_batch() {
                        w.write_batch(&b, &self.metrics.encode_time)?;
                    }
                }
                Ok(())
            }
        }
    }

    fn finish_ipc_stream(&mut self) -> datafusion::common::Result<()> {
        if let OutputWriter::IpcStream { writer, .. } = &mut self.output {
            if let Some(w) = writer.take() {
                let buf_writer = w.finish()?;
                buf_writer
                    .into_inner()
                    .map_err(|e| DataFusionError::Execution(format!("flush error: {e}")))?;
            }
        }
        Ok(())
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

                if let Some(batch) = concatenated_batch {
                    self.write_batch(&batch)?;
                }

                if num_rows >= self.batch_size {
                    self.write_batch(&batch)?;
                } else {
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

        if let Some(batch) = concatenated_batch {
            self.write_batch(&batch)?;
        }
        self.flush_output()?;
        self.finish_ipc_stream()?;

        let data_file_length = std::fs::metadata(&self.output_data_path)?.len();
        let index_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(self.output_index_path.clone())
            .map_err(|e| DataFusionError::Execution(format!("shuffle write error: {e:?}")))?;
        let mut index_buf_writer = BufWriter::new(index_file);
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
