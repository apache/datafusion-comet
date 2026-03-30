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
use crate::{comet_partitioning, CometPartitioning, CompressionCodec};
use arrow::array::{ArrayRef, RecordBatch};
use arrow::compute::kernels::coalesce::BatchCoalescer;
use arrow::compute::interleave_record_batch;
use arrow::datatypes::SchemaRef;
use arrow::ipc::writer::StreamWriter;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion_comet_spark_expr::murmur3::create_murmur3_hashes;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Seek, Write};
use std::sync::Arc;
use tokio::time::Instant;

/// Per-partition output stream that serializes Arrow IPC batches into an
/// in-memory buffer with compression. The block format matches
/// `ShuffleBlockWriter::write_batch` exactly:
///
/// - 8 bytes: payload length (u64 LE) — total bytes after this prefix
/// - 8 bytes: field_count (usize LE)
/// - 4 bytes: codec tag (b"SNAP", b"LZ4_", b"ZSTD", or b"NONE")
/// - N bytes: compressed Arrow IPC stream data
pub(crate) struct PartitionOutputStream {
    schema: SchemaRef,
    codec: CompressionCodec,
    buffer: Vec<u8>,
    /// Coalesces small batches into target_batch_size before serialization,
    /// reducing per-block IPC schema overhead. Lazily initialized on first write.
    coalescer: Option<BatchCoalescer>,
    batch_size: usize,
}

impl PartitionOutputStream {
    pub(crate) fn try_new(
        schema: SchemaRef,
        codec: CompressionCodec,
        batch_size: usize,
    ) -> Result<Self> {
        Ok(Self {
            schema,
            codec,
            buffer: Vec::with_capacity(1024 * 1024),
            coalescer: None,
            batch_size,
        })
    }

    /// Push a batch into the coalescer, serializing any completed batches as
    /// length-prefixed compressed IPC blocks. Returns total bytes written.
    fn write_batch(&mut self, batch: &RecordBatch) -> Result<usize> {
        if batch.num_rows() == 0 {
            return Ok(0);
        }

        let coalescer = self
            .coalescer
            .get_or_insert_with(|| BatchCoalescer::new(batch.schema(), self.batch_size));
        coalescer.push_batch(batch.clone())?;

        let mut completed = Vec::new();
        while let Some(batch) = coalescer.next_completed_batch() {
            completed.push(batch);
        }

        let mut total_bytes = 0;
        for batch in &completed {
            total_bytes += self.write_ipc_block(batch)?;
        }
        Ok(total_bytes)
    }

    /// Flush any remaining rows in the coalescer as a final IPC block.
    /// Must be called before `drain_buffer` or `finish` to avoid losing data.
    fn flush(&mut self) -> Result<usize> {
        let mut total_bytes = 0;
        if let Some(coalescer) = &mut self.coalescer {
            coalescer.finish_buffered_batch()?;
            let mut remaining = Vec::new();
            while let Some(batch) = coalescer.next_completed_batch() {
                remaining.push(batch);
            }
            for batch in &remaining {
                total_bytes += self.write_ipc_block(batch)?;
            }
        }
        Ok(total_bytes)
    }

    /// Serialize a single record batch as a length-prefixed compressed IPC block.
    fn write_ipc_block(&mut self, batch: &RecordBatch) -> Result<usize> {
        let start_pos = self.buffer.len();

        // Write 8-byte placeholder for length prefix
        self.buffer.extend_from_slice(&0u64.to_le_bytes());

        // Write field count (8 bytes, usize LE)
        let field_count = self.schema.fields().len();
        self.buffer
            .extend_from_slice(&(field_count as u64).to_le_bytes());

        // Write codec tag (4 bytes)
        let codec_tag: &[u8; 4] = match &self.codec {
            CompressionCodec::Snappy => b"SNAP",
            CompressionCodec::Lz4Frame => b"LZ4_",
            CompressionCodec::Zstd(_) => b"ZSTD",
            CompressionCodec::None => b"NONE",
        };
        self.buffer.extend_from_slice(codec_tag);

        // Write compressed IPC data
        match &self.codec {
            CompressionCodec::None => {
                let mut arrow_writer = StreamWriter::try_new(&mut self.buffer, &batch.schema())?;
                arrow_writer.write(batch)?;
                arrow_writer.finish()?;
                // StreamWriter::into_inner returns the inner writer; we don't need it
                // since we're writing directly to self.buffer
                arrow_writer.into_inner()?;
            }
            CompressionCodec::Lz4Frame => {
                let mut wtr = lz4_flex::frame::FrameEncoder::new(&mut self.buffer);
                let mut arrow_writer = StreamWriter::try_new(&mut wtr, &batch.schema())?;
                arrow_writer.write(batch)?;
                arrow_writer.finish()?;
                wtr.finish().map_err(|e| {
                    DataFusionError::Execution(format!("lz4 compression error: {e}"))
                })?;
            }
            CompressionCodec::Zstd(level) => {
                let encoder = zstd::Encoder::new(&mut self.buffer, *level)?;
                let mut arrow_writer = StreamWriter::try_new(encoder, &batch.schema())?;
                arrow_writer.write(batch)?;
                arrow_writer.finish()?;
                let zstd_encoder = arrow_writer.into_inner()?;
                zstd_encoder.finish()?;
            }
            CompressionCodec::Snappy => {
                let mut wtr = snap::write::FrameEncoder::new(&mut self.buffer);
                let mut arrow_writer = StreamWriter::try_new(&mut wtr, &batch.schema())?;
                arrow_writer.write(batch)?;
                arrow_writer.finish()?;
                wtr.into_inner().map_err(|e| {
                    DataFusionError::Execution(format!("snappy compression error: {e}"))
                })?;
            }
        }

        // Backfill length prefix: total bytes after the 8-byte length field
        let end_pos = self.buffer.len();
        let ipc_length = (end_pos - start_pos - 8) as u64;

        let max_size = i32::MAX as u64;
        if ipc_length > max_size {
            return Err(DataFusionError::Execution(format!(
                "Shuffle block size {ipc_length} exceeds maximum size of {max_size}. \
                Try reducing batch size or increasing compression level"
            )));
        }

        self.buffer[start_pos..start_pos + 8].copy_from_slice(&ipc_length.to_le_bytes());

        Ok(end_pos - start_pos)
    }

    /// Returns the number of bytes currently in the buffer.
    #[cfg(test)]
    fn buffered_bytes(&self) -> usize {
        self.buffer.len()
    }

    /// Takes the buffer contents, leaving the buffer empty.
    fn drain_buffer(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.buffer)
    }

    /// Consumes self and returns the buffer.
    #[cfg(test)]
    fn finish(self) -> Result<Vec<u8>> {
        Ok(self.buffer)
    }
}

struct SpillFile {
    _temp_file: datafusion::execution::disk_manager::RefCountedTempFile,
    file: File,
}

/// A partitioner that immediately writes IPC blocks per partition as batches arrive,
/// rather than buffering all data until shuffle_write. Supports spilling per-partition
/// buffers to disk under memory pressure.
pub(crate) struct ImmediateModePartitioner {
    output_data_file: String,
    output_index_file: String,
    streams: Vec<PartitionOutputStream>,
    spill_files: Vec<Option<SpillFile>>,
    partitioning: CometPartitioning,
    runtime: Arc<RuntimeEnv>,
    reservation: MemoryReservation,
    metrics: ShufflePartitionerMetrics,
    hashes_buf: Vec<u32>,
    partition_ids: Vec<u32>,
    batch_size: usize,
}

impl ImmediateModePartitioner {
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
    ) -> Result<Self> {
        let num_output_partitions = partitioning.partition_count();

        let streams = (0..num_output_partitions)
            .map(|_| PartitionOutputStream::try_new(Arc::clone(&schema), codec.clone(), batch_size))
            .collect::<Result<Vec<_>>>()?;

        let spill_files: Vec<Option<SpillFile>> =
            (0..num_output_partitions).map(|_| None).collect();

        let hashes_buf = match &partitioning {
            CometPartitioning::Hash(_, _) | CometPartitioning::RoundRobin(_, _) => {
                vec![0u32; batch_size]
            }
            _ => vec![],
        };

        let reservation =
            MemoryConsumer::new(format!("ImmediateModePartitioner[{partition}]"))
                .with_can_spill(true)
                .register(&runtime.memory_pool);

        Ok(Self {
            output_data_file,
            output_index_file,
            streams,
            spill_files,
            partitioning,
            runtime,
            reservation,
            metrics,
            hashes_buf,
            partition_ids: vec![0u32; batch_size],
            batch_size,
        })
    }

    /// Compute partition IDs for each row in the batch, storing results in
    /// `self.partition_ids`. Returns the number of output partitions.
    fn compute_partition_ids(&mut self, batch: &RecordBatch) -> Result<usize> {
        let num_rows = batch.num_rows();
        match &self.partitioning {
            CometPartitioning::Hash(exprs, num_output_partitions) => {
                let num_output_partitions = *num_output_partitions;
                let arrays = exprs
                    .iter()
                    .map(|expr| expr.evaluate(batch)?.into_array(num_rows))
                    .collect::<Result<Vec<_>>>()?;

                let hashes_buf = &mut self.hashes_buf[..num_rows];
                hashes_buf.fill(42_u32);
                create_murmur3_hashes(&arrays, hashes_buf)?;

                let partition_ids = &mut self.partition_ids[..num_rows];
                for (idx, hash) in hashes_buf.iter().enumerate() {
                    partition_ids[idx] =
                        comet_partitioning::pmod(*hash, num_output_partitions) as u32;
                }
                Ok(num_output_partitions)
            }
            CometPartitioning::RoundRobin(num_output_partitions, max_hash_columns) => {
                let num_output_partitions = *num_output_partitions;
                let max_hash_columns = *max_hash_columns;
                let num_columns_to_hash = if max_hash_columns == 0 {
                    batch.num_columns()
                } else {
                    max_hash_columns.min(batch.num_columns())
                };
                let columns_to_hash: Vec<ArrayRef> = (0..num_columns_to_hash)
                    .map(|i| Arc::clone(batch.column(i)))
                    .collect();

                let hashes_buf = &mut self.hashes_buf[..num_rows];
                hashes_buf.fill(42_u32);
                create_murmur3_hashes(&columns_to_hash, hashes_buf)?;

                let partition_ids = &mut self.partition_ids[..num_rows];
                for (idx, hash) in hashes_buf.iter().enumerate() {
                    partition_ids[idx] =
                        comet_partitioning::pmod(*hash, num_output_partitions) as u32;
                }
                Ok(num_output_partitions)
            }
            CometPartitioning::RangePartitioning(
                lex_ordering,
                num_output_partitions,
                row_converter,
                bounds,
            ) => {
                let num_output_partitions = *num_output_partitions;
                let arrays = lex_ordering
                    .iter()
                    .map(|expr| expr.expr.evaluate(batch)?.into_array(num_rows))
                    .collect::<Result<Vec<_>>>()?;

                let row_batch = row_converter.convert_columns(arrays.as_slice())?;
                let partition_ids = &mut self.partition_ids[..num_rows];
                for (row_idx, row) in row_batch.iter().enumerate() {
                    partition_ids[row_idx] = bounds
                        .as_slice()
                        .partition_point(|bound| bound.row() <= row)
                        as u32;
                }
                Ok(num_output_partitions)
            }
            other => Err(DataFusionError::NotImplemented(format!(
                "Unsupported shuffle partitioning scheme {other:?}"
            ))),
        }
    }

    /// Route rows to their partition streams using interleave, then write IPC blocks.
    /// Returns total bytes written across all partitions.
    fn write_partitioned_rows(&mut self, batch: &RecordBatch) -> Result<usize> {
        let num_partitions = self.streams.len();
        let num_rows = batch.num_rows();

        // Build per-partition row indices
        let mut partition_row_indices: Vec<Vec<(usize, usize)>> =
            vec![Vec::new(); num_partitions];
        for row_idx in 0..num_rows {
            let pid = self.partition_ids[row_idx] as usize;
            partition_row_indices[pid].push((0, row_idx));
        }

        let batch_refs = [batch];
        let mut total_bytes = 0;

        let mut interleave_timer = self.metrics.interleave_time.timer();
        for (pid, indices) in partition_row_indices.iter().enumerate() {
            if indices.is_empty() {
                continue;
            }
            let sub_batch = interleave_record_batch(&batch_refs, indices)
                .map_err(|e| DataFusionError::ArrowError(Box::from(e), None))?;
            interleave_timer.stop();

            let mut encode_timer = self.metrics.encode_time.timer();
            let bytes = self.streams[pid].write_batch(&sub_batch)?;
            encode_timer.stop();

            total_bytes += bytes;
            interleave_timer = self.metrics.interleave_time.timer();
        }
        interleave_timer.stop();

        Ok(total_bytes)
    }

    /// Spill all partition buffers to per-partition temp files.
    fn spill_all(&mut self) -> Result<()> {
        let mut spilled_bytes = 0usize;

        for pid in 0..self.streams.len() {
            // Flush coalescer so buffered rows are serialized before draining
            self.streams[pid].flush()?;
            let buf = self.streams[pid].drain_buffer();
            if buf.is_empty() {
                continue;
            }

            // Create spill file on first spill for this partition
            if self.spill_files[pid].is_none() {
                let temp_file = self
                    .runtime
                    .disk_manager
                    .create_tmp_file(&format!("imm_shuffle_p{pid}"))?;
                let path = temp_file.path().to_owned();
                let file = OpenOptions::new()
                    .append(true)
                    .open(&path)
                    .map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Failed to open spill file: {e}"
                        ))
                    })?;
                self.spill_files[pid] = Some(SpillFile {
                    _temp_file: temp_file,
                    file,
                });
            }

            if let Some(spill) = &mut self.spill_files[pid] {
                spill.file.write_all(&buf).map_err(|e| {
                    DataFusionError::Execution(format!("Failed to write spill: {e}"))
                })?;
                spilled_bytes += buf.len();
            }
        }

        // Flush all spill files so data is visible when re-opened for reading in shuffle_write
        for spill in self.spill_files.iter_mut().flatten() {
            spill.file.flush()?;
        }

        self.reservation.free();
        if spilled_bytes > 0 {
            self.metrics.spill_count.add(1);
            self.metrics.spilled_bytes.add(spilled_bytes);
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl ShufflePartitioner for ImmediateModePartitioner {
    async fn insert_batch(&mut self, batch: RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let start_time = Instant::now();

        self.metrics.data_size.add(batch.get_array_memory_size());
        self.metrics.baseline.record_output(batch.num_rows());

        let mut start = 0;
        while start < batch.num_rows() {
            let end = (start + self.batch_size).min(batch.num_rows());
            let chunk = batch.slice(start, end - start);

            let repart_start = Instant::now();
            self.compute_partition_ids(&chunk)?;
            self.metrics
                .repart_time
                .add_duration(repart_start.elapsed());

            let bytes_written = self.write_partitioned_rows(&chunk)?;

            if self.reservation.try_grow(bytes_written).is_err() {
                self.spill_all()?;
            }

            start = end;
        }

        self.metrics.input_batches.add(1);
        self.metrics
            .baseline
            .elapsed_compute()
            .add_duration(start_time.elapsed());

        Ok(())
    }

    fn shuffle_write(&mut self) -> Result<()> {
        let start_time = Instant::now();
        let num_output_partitions = self.streams.len();
        let mut offsets = vec![0i64; num_output_partitions + 1];

        let data_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.output_data_file)
            .map_err(|e| {
                DataFusionError::Execution(format!("shuffle write error: {e:?}"))
            })?;
        let mut output_data = BufWriter::new(data_file);

        #[allow(clippy::needless_range_loop)]
        for pid in 0..num_output_partitions {
            offsets[pid] = output_data.stream_position()? as i64;

            // Copy spill file contents if any
            if let Some(spill) = &self.spill_files[pid] {
                let path = spill._temp_file.path().to_owned();
                let spill_reader = File::open(&path).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to open spill file for reading: {e}"
                    ))
                })?;
                let mut reader = BufReader::new(spill_reader);
                let mut write_timer = self.metrics.write_time.timer();
                std::io::copy(&mut reader, &mut output_data)?;
                write_timer.stop();
            }

            // Flush coalescer and write remaining in-memory buffer
            self.streams[pid].flush()?;
            let buf = self.streams[pid].drain_buffer();
            if !buf.is_empty() {
                let mut write_timer = self.metrics.write_time.timer();
                output_data.write_all(&buf)?;
                write_timer.stop();
            }
        }

        let mut write_timer = self.metrics.write_time.timer();
        output_data.flush()?;
        write_timer.stop();

        // Record final offset
        offsets[num_output_partitions] = output_data.stream_position()? as i64;

        // Write index file
        let mut write_timer = self.metrics.write_time.timer();
        let mut output_index = BufWriter::new(
            File::create(&self.output_index_file).map_err(|e| {
                DataFusionError::Execution(format!("shuffle write error: {e:?}"))
            })?,
        );
        for offset in &offsets {
            output_index.write_all(&offset.to_le_bytes())?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::read_ipc_compressed;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::memory_pool::GreedyMemoryPool;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
    use std::sync::Arc;

    fn make_test_batch(values: &[i32]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let array = Int32Array::from(values.to_vec());
        RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()
    }

    #[test]
    fn test_partition_output_stream_write_and_read() {
        let batch = make_test_batch(&[1, 2, 3, 4, 5]);
        let schema = batch.schema();

        for codec in [
            CompressionCodec::None,
            CompressionCodec::Lz4Frame,
            CompressionCodec::Zstd(1),
            CompressionCodec::Snappy,
        ] {
            // Use batch_size=1 to force immediate serialization (no coalescing)
            let mut stream =
                PartitionOutputStream::try_new(Arc::clone(&schema), codec, 1).unwrap();
            stream.write_batch(&batch).unwrap();
            stream.flush().unwrap();

            let buf = stream.finish().unwrap();
            assert!(!buf.is_empty());

            // Parse the first block: 8 bytes length, 8 bytes field_count, then codec+data
            let ipc_length =
                u64::from_le_bytes(buf[0..8].try_into().unwrap()) as usize;
            assert!(ipc_length > 0);

            let field_count =
                usize::from_le_bytes(buf[8..16].try_into().unwrap());
            assert_eq!(field_count, 1); // one field "a"

            // read_ipc_compressed expects data starting at the codec tag
            let block_end = 8 + ipc_length;
            let ipc_data = &buf[16..block_end];
            let batch2 = read_ipc_compressed(ipc_data).unwrap();
            assert!(batch2.num_rows() > 0);
        }
    }

    #[test]
    fn test_partition_output_stream_coalesces_small_batches() {
        let batch1 = make_test_batch(&[1, 2, 3]);
        let batch2 = make_test_batch(&[4, 5, 6, 7]);
        let schema = batch1.schema();

        // batch_size=10 means both batches (3+4=7 rows) fit in one coalesced block
        let mut stream =
            PartitionOutputStream::try_new(schema, CompressionCodec::None, 10).unwrap();

        // Small batches sit in coalescer, no IPC block written yet
        stream.write_batch(&batch1).unwrap();
        assert_eq!(stream.buffered_bytes(), 0);

        stream.write_batch(&batch2).unwrap();
        assert_eq!(stream.buffered_bytes(), 0);

        // Flush produces one coalesced block with all 7 rows
        stream.flush().unwrap();
        let buf = stream.finish().unwrap();
        assert!(!buf.is_empty());

        let ipc_length = u64::from_le_bytes(buf[0..8].try_into().unwrap()) as usize;
        let ipc_data = &buf[16..8 + ipc_length];
        let decoded = read_ipc_compressed(ipc_data).unwrap();
        assert_eq!(decoded.num_rows(), 7);
    }

    #[test]
    fn test_partition_output_stream_empty_batch() {
        let batch = make_test_batch(&[]);
        let schema = batch.schema();

        let mut stream =
            PartitionOutputStream::try_new(schema, CompressionCodec::None, 8192).unwrap();
        let bytes_written = stream.write_batch(&batch).unwrap();
        assert_eq!(bytes_written, 0);
        assert_eq!(stream.buffered_bytes(), 0);
    }

    #[test]
    fn test_partition_output_stream_drain_after_flush() {
        let batch = make_test_batch(&[1, 2, 3]);
        let schema = batch.schema();

        let mut stream =
            PartitionOutputStream::try_new(schema, CompressionCodec::None, 8192).unwrap();
        stream.write_batch(&batch).unwrap();

        // Flush coalescer then drain
        stream.flush().unwrap();
        assert!(stream.buffered_bytes() > 0);

        let drained = stream.drain_buffer();
        assert!(!drained.is_empty());
        assert_eq!(stream.buffered_bytes(), 0);

        // Can still write after drain
        stream.write_batch(&batch).unwrap();
        stream.flush().unwrap();
        assert!(stream.buffered_bytes() > 0);
    }

    fn make_hash_partitioning(col_name: &str, num_partitions: usize) -> CometPartitioning {
        use datafusion::physical_expr::expressions::Column;
        let expr: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
            Arc::new(Column::new(col_name, 0));
        CometPartitioning::Hash(vec![expr], num_partitions)
    }

    #[tokio::test]
    async fn test_immediate_mode_partitioner_hash() {
        let batch = make_test_batch(&[1, 2, 3, 4, 5, 6, 7, 8]);
        let schema = batch.schema();
        let dir = tempfile::tempdir().unwrap();
        let data_path = dir.path().join("data").to_str().unwrap().to_string();
        let index_path = dir.path().join("index").to_str().unwrap().to_string();

        let metrics = ShufflePartitionerMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let runtime = Arc::new(RuntimeEnvBuilder::new().build().unwrap());

        let mut partitioner = ImmediateModePartitioner::try_new(
            0,
            data_path,
            index_path,
            schema,
            make_hash_partitioning("a", 4),
            metrics,
            runtime,
            8192,
            CompressionCodec::None,
        )
        .unwrap();

        partitioner.insert_batch(batch).await.unwrap();

        // Flush coalesced data so it appears in the byte buffer
        for stream in &mut partitioner.streams {
            stream.flush().unwrap();
        }
        let total_buffered: usize = partitioner.streams.iter().map(|s| s.buffered_bytes()).sum();
        assert!(total_buffered > 0, "Expected some buffered bytes");
    }

    #[tokio::test]
    async fn test_immediate_mode_shuffle_write() {
        let batch1 = make_test_batch(&[1, 2, 3, 4, 5, 6]);
        let batch2 = make_test_batch(&[7, 8, 9, 10, 11, 12]);
        let schema = batch1.schema();
        let dir = tempfile::tempdir().unwrap();
        let data_path = dir.path().join("data").to_str().unwrap().to_string();
        let index_path = dir.path().join("index").to_str().unwrap().to_string();

        let num_partitions = 3;
        let metrics = ShufflePartitionerMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let runtime = Arc::new(RuntimeEnvBuilder::new().build().unwrap());

        let mut partitioner = ImmediateModePartitioner::try_new(
            0,
            data_path.clone(),
            index_path.clone(),
            schema,
            make_hash_partitioning("a", num_partitions),
            metrics,
            runtime,
            8192,
            CompressionCodec::None,
        )
        .unwrap();

        partitioner.insert_batch(batch1).await.unwrap();
        partitioner.insert_batch(batch2).await.unwrap();
        partitioner.shuffle_write().unwrap();

        // Verify index file has (num_partitions + 1) * 8 bytes
        let index_data = std::fs::read(&index_path).unwrap();
        assert_eq!(index_data.len(), (num_partitions + 1) * 8);

        // First offset should be 0
        let first_offset = i64::from_le_bytes(index_data[0..8].try_into().unwrap());
        assert_eq!(first_offset, 0);

        // Last offset should equal data file size
        let data_file_size = std::fs::metadata(&data_path).unwrap().len();
        let last_offset = i64::from_le_bytes(
            index_data[num_partitions * 8..(num_partitions + 1) * 8]
                .try_into()
                .unwrap(),
        );
        assert_eq!(last_offset as u64, data_file_size);

        // Data file should be non-empty
        assert!(data_file_size > 0);
    }

    #[tokio::test]
    async fn test_immediate_mode_spill() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let dir = tempfile::tempdir().unwrap();
        let data_path = dir.path().join("data").to_str().unwrap().to_string();
        let index_path = dir.path().join("index").to_str().unwrap().to_string();

        let num_partitions = 2;
        let metrics = ShufflePartitionerMetrics::new(&ExecutionPlanMetricsSet::new(), 0);

        // Use a tiny memory pool to force spilling
        let runtime = Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::new(GreedyMemoryPool::new(256)))
                .build()
                .unwrap(),
        );

        let mut partitioner = ImmediateModePartitioner::try_new(
            0,
            data_path.clone(),
            index_path.clone(),
            Arc::clone(&schema),
            make_hash_partitioning("a", num_partitions),
            metrics,
            runtime,
            8192,
            CompressionCodec::None,
        )
        .unwrap();

        // Insert enough data to exceed the tiny memory pool and trigger spills
        for i in 0..10 {
            let values: Vec<i32> = ((i * 10)..((i + 1) * 10)).collect();
            let batch = make_test_batch(&values);
            partitioner.insert_batch(batch).await.unwrap();
        }

        partitioner.shuffle_write().unwrap();

        // Verify output is valid
        let index_data = std::fs::read(&index_path).unwrap();
        assert_eq!(index_data.len(), (num_partitions + 1) * 8);

        let first_offset = i64::from_le_bytes(index_data[0..8].try_into().unwrap());
        assert_eq!(first_offset, 0);

        let data_file_size = std::fs::metadata(&data_path).unwrap().len();
        let last_offset = i64::from_le_bytes(
            index_data[num_partitions * 8..(num_partitions + 1) * 8]
                .try_into()
                .unwrap(),
        );
        assert_eq!(last_offset as u64, data_file_size);
        assert!(data_file_size > 0);
    }

    #[tokio::test]
    async fn test_block_format_compatible_with_read_ipc_compressed() {
        let batch = make_test_batch(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let schema = batch.schema();
        let dir = tempfile::tempdir().unwrap();
        let data_path = dir.path().join("data").to_str().unwrap().to_string();
        let index_path = dir.path().join("index").to_str().unwrap().to_string();

        let num_partitions = 2;
        let metrics = ShufflePartitionerMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let runtime = Arc::new(RuntimeEnvBuilder::new().build().unwrap());

        let mut partitioner = ImmediateModePartitioner::try_new(
            0,
            data_path.clone(),
            index_path.clone(),
            Arc::clone(&schema),
            make_hash_partitioning("a", num_partitions),
            metrics,
            runtime,
            8192,
            CompressionCodec::Lz4Frame,
        )
        .unwrap();

        partitioner.insert_batch(batch).await.unwrap();
        partitioner.shuffle_write().unwrap();

        // Read index file to get partition offsets
        let index_data = std::fs::read(&index_path).unwrap();
        let mut offsets = Vec::new();
        for i in 0..=num_partitions {
            let offset =
                i64::from_le_bytes(index_data[i * 8..(i + 1) * 8].try_into().unwrap());
            offsets.push(offset as usize);
        }

        // Read entire data file
        let data = std::fs::read(&data_path).unwrap();

        let mut total_rows = 0;
        for pid in 0..num_partitions {
            let partition_start = offsets[pid];
            let partition_end = offsets[pid + 1];
            if partition_start == partition_end {
                continue;
            }

            // Parse blocks within this partition's byte range
            let mut pos = partition_start;
            while pos < partition_end {
                // Read 8-byte length prefix
                let payload_len = u64::from_le_bytes(
                    data[pos..pos + 8].try_into().unwrap(),
                ) as usize;
                assert!(payload_len > 0, "Block payload length should be > 0");

                // Skip 8-byte field_count
                let field_count = u64::from_le_bytes(
                    data[pos + 8..pos + 16].try_into().unwrap(),
                ) as usize;
                assert_eq!(field_count, 1, "Expected 1 field");

                // Pass codec tag + IPC data to read_ipc_compressed
                let block_end = pos + 8 + payload_len;
                let ipc_data = &data[pos + 16..block_end];
                let decoded = read_ipc_compressed(ipc_data).unwrap();

                assert_eq!(decoded.num_columns(), 1);
                assert!(decoded.num_rows() > 0);

                // Verify values are valid Int32
                let col = decoded
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("Expected Int32Array");
                for i in 0..col.len() {
                    let v = col.value(i);
                    assert!(
                        (1..=10).contains(&v),
                        "Value {v} not in expected range 1..=10"
                    );
                }

                total_rows += decoded.num_rows();
                pos = block_end;
            }
            assert_eq!(
                pos, partition_end,
                "Block parsing should consume exactly the partition's bytes"
            );
        }

        assert_eq!(total_rows, 10, "Total decoded rows should match input");
    }
}
