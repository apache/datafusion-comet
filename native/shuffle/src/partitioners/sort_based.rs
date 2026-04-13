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
use crate::{comet_partitioning, CometPartitioning, CompressionCodec, ShuffleBlockWriter};
use arrow::array::{ArrayRef, RecordBatch, UInt32Array};
use arrow::compute::take;
use arrow::datatypes::SchemaRef;
use datafusion::common::DataFusionError;
use datafusion::execution::disk_manager::RefCountedTempFile;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion_comet_spark_expr::murmur3::create_murmur3_hashes;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Seek, Write};
use std::sync::Arc;
use tokio::time::Instant;

/// Per-partition writer that owns a persistent BufBatchWriter with BatchCoalescer,
/// so small batches are accumulated to batch_size before encoding.
struct PartitionSpillWriter {
    /// The BufBatchWriter that coalesces and encodes batches.
    /// None until the first batch is written to this partition.
    writer: Option<BufBatchWriter<ShuffleBlockWriter, File>>,
    /// Temp file handle — kept alive so the file isn't deleted until we're done
    _temp_file: Option<RefCountedTempFile>,
    /// Path to the spill file for copying in shuffle_write
    spill_path: Option<std::path::PathBuf>,
}

impl PartitionSpillWriter {
    fn new() -> Self {
        Self {
            writer: None,
            _temp_file: None,
            spill_path: None,
        }
    }

    fn ensure_writer(
        &mut self,
        runtime: &RuntimeEnv,
        shuffle_block_writer: &ShuffleBlockWriter,
        write_buffer_size: usize,
        batch_size: usize,
    ) -> datafusion::common::Result<()> {
        if self.writer.is_none() {
            let temp_file = runtime
                .disk_manager
                .create_tmp_file("sort shuffle spill")?;
            let path = temp_file.path().to_path_buf();
            let file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&path)
                .map_err(|e| {
                    DataFusionError::Execution(format!("Error creating spill file: {e}"))
                })?;
            self.writer = Some(BufBatchWriter::new(
                shuffle_block_writer.clone(),
                file,
                write_buffer_size,
                batch_size,
            ));
            self.spill_path = Some(path);
            self._temp_file = Some(temp_file);
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn write_batch(
        &mut self,
        batch: &RecordBatch,
        runtime: &RuntimeEnv,
        shuffle_block_writer: &ShuffleBlockWriter,
        write_buffer_size: usize,
        batch_size: usize,
        encode_time: &datafusion::physical_plan::metrics::Time,
        write_time: &datafusion::physical_plan::metrics::Time,
    ) -> datafusion::common::Result<()> {
        self.ensure_writer(runtime, shuffle_block_writer, write_buffer_size, batch_size)?;
        self.writer
            .as_mut()
            .unwrap()
            .write(batch, encode_time, write_time)?;
        Ok(())
    }

    fn flush(
        &mut self,
        encode_time: &datafusion::physical_plan::metrics::Time,
        write_time: &datafusion::physical_plan::metrics::Time,
    ) -> datafusion::common::Result<()> {
        if let Some(writer) = &mut self.writer {
            writer.flush(encode_time, write_time)?;
        }
        Ok(())
    }

    fn path(&self) -> Option<&std::path::Path> {
        self.spill_path.as_deref()
    }
}

/// A shuffle repartitioner that sorts each batch by partition ID using counting sort,
/// then slices and writes per-partition sub-batches immediately. This avoids
/// per-partition Arrow builders, so memory usage is O(batch_size) regardless of
/// partition count.
///
/// Each partition has a persistent BufBatchWriter with a BatchCoalescer that accumulates
/// small slices to batch_size before encoding, avoiding per-slice IPC schema overhead.
pub(crate) struct SortBasedPartitioner {
    output_data_file: String,
    output_index_file: String,
    partition_writers: Vec<PartitionSpillWriter>,
    shuffle_block_writer: ShuffleBlockWriter,
    partitioning: CometPartitioning,
    runtime: Arc<RuntimeEnv>,
    metrics: ShufflePartitionerMetrics,
    batch_size: usize,
    /// Held to keep the memory reservation alive for the lifetime of this partitioner
    _reservation: MemoryReservation,
    write_buffer_size: usize,
    hashes_buf: Vec<u32>,
    partition_ids: Vec<u32>,
    sorted_indices: Vec<u32>,
    partition_starts: Vec<usize>,
}

impl SortBasedPartitioner {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_new(
        partition: usize,
        output_data_file: String,
        output_index_file: String,
        schema: SchemaRef,
        partitioning: CometPartitioning,
        metrics: ShufflePartitionerMetrics,
        runtime: Arc<RuntimeEnv>,
        batch_size: usize,
        codec: CompressionCodec,
        write_buffer_size: usize,
    ) -> datafusion::common::Result<Self> {
        let num_output_partitions = partitioning.partition_count();
        let shuffle_block_writer = ShuffleBlockWriter::try_new(schema.as_ref(), codec.clone())?;
        let partition_writers = (0..num_output_partitions)
            .map(|_| PartitionSpillWriter::new())
            .collect();
        let reservation = MemoryConsumer::new(format!("SortBasedPartitioner[{partition}]"))
            .register(&runtime.memory_pool);
        let hashes_buf = match partitioning {
            CometPartitioning::Hash(_, _) | CometPartitioning::RoundRobin(_, _) => {
                vec![0u32; batch_size]
            }
            _ => vec![],
        };
        Ok(Self {
            output_data_file,
            output_index_file,
            partition_writers,
            shuffle_block_writer,
            partitioning,
            runtime,
            metrics,
            batch_size,
            _reservation: reservation,
            write_buffer_size,
            hashes_buf,
            partition_ids: vec![0u32; batch_size],
            sorted_indices: vec![0u32; batch_size],
            partition_starts: vec![0usize; num_output_partitions + 1],
        })
    }

    fn compute_partition_ids_and_sort(
        &mut self,
        input: &RecordBatch,
    ) -> datafusion::common::Result<()> {
        let num_rows = input.num_rows();
        let num_partitions = self.partitioning.partition_count();

        match &self.partitioning {
            CometPartitioning::Hash(exprs, num_output_partitions) => {
                let arrays = exprs
                    .iter()
                    .map(|expr| expr.evaluate(input)?.into_array(num_rows))
                    .collect::<datafusion::common::Result<Vec<_>>>()?;
                let hashes_buf = &mut self.hashes_buf[..num_rows];
                hashes_buf.fill(42_u32);
                create_murmur3_hashes(&arrays, hashes_buf)?;
                let partition_ids = &mut self.partition_ids[..num_rows];
                for (idx, hash) in hashes_buf.iter().enumerate() {
                    partition_ids[idx] =
                        comet_partitioning::pmod(*hash, *num_output_partitions) as u32;
                }
            }
            CometPartitioning::RangePartitioning(
                lex_ordering,
                _num_output_partitions,
                row_converter,
                bounds,
            ) => {
                let arrays = lex_ordering
                    .iter()
                    .map(|expr| expr.expr.evaluate(input)?.into_array(num_rows))
                    .collect::<datafusion::common::Result<Vec<_>>>()?;
                let row_batch = row_converter.convert_columns(arrays.as_slice())?;
                let partition_ids = &mut self.partition_ids[..num_rows];
                row_batch.iter().enumerate().for_each(|(row_idx, row)| {
                    partition_ids[row_idx] = bounds
                        .as_slice()
                        .partition_point(|bound| bound.row() <= row)
                        as u32;
                });
            }
            CometPartitioning::RoundRobin(num_output_partitions, max_hash_columns) => {
                let num_columns_to_hash = if *max_hash_columns == 0 {
                    input.num_columns()
                } else {
                    (*max_hash_columns).min(input.num_columns())
                };
                let columns_to_hash: Vec<ArrayRef> = (0..num_columns_to_hash)
                    .map(|i| Arc::clone(input.column(i)))
                    .collect();
                let hashes_buf = &mut self.hashes_buf[..num_rows];
                hashes_buf.fill(42_u32);
                create_murmur3_hashes(&columns_to_hash, hashes_buf)?;
                let partition_ids = &mut self.partition_ids[..num_rows];
                for (idx, hash) in hashes_buf.iter().enumerate() {
                    partition_ids[idx] =
                        comet_partitioning::pmod(*hash, *num_output_partitions) as u32;
                }
            }
            other => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Unsupported shuffle partitioning scheme {other:?}"
                )));
            }
        }

        // Counting sort
        let partition_starts = &mut self.partition_starts[..num_partitions + 1];
        partition_starts.fill(0);
        let partition_ids = &self.partition_ids[..num_rows];
        for &pid in partition_ids.iter() {
            partition_starts[pid as usize + 1] += 1;
        }
        for i in 1..=num_partitions {
            partition_starts[i] += partition_starts[i - 1];
        }
        let sorted_indices = &mut self.sorted_indices[..num_rows];
        let mut cursors = partition_starts.to_vec();
        for (row_idx, &pid) in partition_ids.iter().enumerate() {
            let pos = cursors[pid as usize];
            sorted_indices[pos] = row_idx as u32;
            cursors[pid as usize] += 1;
        }
        Ok(())
    }

    fn process_batch(&mut self, input: RecordBatch) -> datafusion::common::Result<()> {
        if input.num_rows() == 0 {
            return Ok(());
        }
        let num_rows = input.num_rows();
        let num_partitions = self.partitioning.partition_count();

        self.metrics.data_size.add(input.get_array_memory_size());
        self.metrics.baseline.record_output(num_rows);

        {
            let repart_start = Instant::now();
            self.compute_partition_ids_and_sort(&input)?;
            self.metrics
                .repart_time
                .add_duration(repart_start.elapsed());
        }

        let sorted_indices = &self.sorted_indices[..num_rows];
        let partition_starts = &self.partition_starts[..num_partitions + 1];
        let indices_array = UInt32Array::from_iter_values(sorted_indices.iter().copied());
        let sorted_batch = RecordBatch::try_new(
            input.schema(),
            input
                .columns()
                .iter()
                .map(|col| take(col, &indices_array, None))
                .collect::<Result<Vec<_>, _>>()?,
        )?;

        for partition_id in 0..num_partitions {
            let start = partition_starts[partition_id];
            let end = partition_starts[partition_id + 1];
            let len = end - start;
            if len == 0 {
                continue;
            }
            let partition_batch = sorted_batch.slice(start, len);
            self.partition_writers[partition_id].write_batch(
                &partition_batch,
                &self.runtime,
                &self.shuffle_block_writer,
                self.write_buffer_size,
                self.batch_size,
                &self.metrics.encode_time,
                &self.metrics.write_time,
            )?;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl ShufflePartitioner for SortBasedPartitioner {
    async fn insert_batch(&mut self, batch: RecordBatch) -> datafusion::common::Result<()> {
        let start_time = Instant::now();
        let mut start = 0;
        while start < batch.num_rows() {
            let end = (start + self.batch_size).min(batch.num_rows());
            let slice = batch.slice(start, end - start);
            self.process_batch(slice)?;
            start = end;
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
        let num_output_partitions = self.partition_writers.len();
        let mut offsets = vec![0i64; num_output_partitions + 1];
        let data_file = self.output_data_file.clone();
        let index_file = self.output_index_file.clone();

        // Flush all partition writers to ensure all data is written to spill files
        for writer in &mut self.partition_writers {
            writer.flush(&self.metrics.encode_time, &self.metrics.write_time)?;
        }

        let output_data = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(data_file)
            .map_err(|e| DataFusionError::Execution(format!("shuffle write error: {e:?}")))?;
        let mut output_data = BufWriter::new(output_data);

        for (i, partition_writer) in self
            .partition_writers
            .iter()
            .enumerate()
            .take(num_output_partitions)
        {
            offsets[i] = output_data.stream_position()? as i64;
            if let Some(spill_path) = partition_writer.path() {
                let mut spill_file = File::open(spill_path)?;
                let mut write_timer = self.metrics.write_time.timer();
                std::io::copy(&mut spill_file, &mut output_data)?;
                write_timer.stop();
            }
        }

        let mut write_timer = self.metrics.write_time.timer();
        output_data.flush()?;
        write_timer.stop();

        offsets[num_output_partitions] = output_data.stream_position()? as i64;

        let mut write_timer = self.metrics.write_time.timer();
        let mut output_index = BufWriter::new(
            File::create(index_file)
                .map_err(|e| DataFusionError::Execution(format!("shuffle write error: {e:?}")))?,
        );
        for offset in offsets {
            output_index.write_all(&offset.to_le_bytes()[..])?;
        }
        output_index.flush()?;
        write_timer.stop();

        self.metrics
            .baseline
            .elapsed_compute()
            .add_duration(start_time.elapsed());
        Ok(())
    }
}

impl Debug for SortBasedPartitioner {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("SortBasedPartitioner")
            .field("partitions", &self.partition_writers.len())
            .finish()
    }
}
