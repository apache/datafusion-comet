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
use crate::partitioners::partition_id::{assign_hash_partition_ids, assign_range_partition_ids};
use crate::partitioners::ShufflePartitioner;
use crate::{CometPartitioning, CompressionCodec};
use arrow::array::builder::{
    make_builder, ArrayBuilder, BinaryBuilder, BinaryViewBuilder, BooleanBuilder,
    LargeBinaryBuilder, LargeStringBuilder, NullBuilder, PrimitiveBuilder, StringBuilder,
    StringViewBuilder,
};
use arrow::array::{Array, ArrayRef, AsArray, RecordBatch, UInt32Array};
use arrow::compute::take;
use arrow::datatypes::{
    DataType, Date32Type, Date64Type, Decimal128Type, Decimal256Type, Float32Type, Float64Type,
    Int16Type, Int32Type, Int64Type, Int8Type, SchemaRef, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt16Type, UInt32Type,
    UInt64Type, UInt8Type,
};
use arrow::ipc::writer::StreamWriter;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryLimit, MemoryReservation};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion_comet_spark_expr::murmur3::create_murmur3_hashes;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Seek, Write};
use std::sync::Arc;
use tokio::time::Instant;

macro_rules! scatter_byte_array {
    ($builder:expr, $source:expr, $indices:expr, $offset_type:ty, $builder_type:ty, $cast:ident) => {{
        let src = $source.$cast::<$offset_type>();
        let dst = $builder
            .as_any_mut()
            .downcast_mut::<$builder_type>()
            .expect("builder type mismatch");
        if src.null_count() == 0 {
            for &idx in $indices {
                dst.append_value(src.value(idx));
            }
        } else {
            for &idx in $indices {
                dst.append_option(src.is_valid(idx).then(|| src.value(idx)));
            }
        }
    }};
}

macro_rules! scatter_byte_view {
    ($builder:expr, $source:expr, $indices:expr, $cast:ident, $builder_type:ty) => {{
        let src = $source.$cast();
        let dst = $builder
            .as_any_mut()
            .downcast_mut::<$builder_type>()
            .expect("builder type mismatch");
        if src.null_count() == 0 {
            for &idx in $indices {
                dst.append_value(src.value(idx));
            }
        } else {
            for &idx in $indices {
                dst.append_option(src.is_valid(idx).then(|| src.value(idx)));
            }
        }
    }};
}

macro_rules! scatter_primitive {
    ($builder:expr, $source:expr, $indices:expr, $arrow_type:ty) => {{
        let src = $source.as_primitive::<$arrow_type>();
        let dst = $builder
            .as_any_mut()
            .downcast_mut::<PrimitiveBuilder<$arrow_type>>()
            .expect("builder type mismatch");
        if src.null_count() == 0 {
            for &idx in $indices {
                dst.append_value(src.value(idx));
            }
        } else {
            for &idx in $indices {
                dst.append_option(src.is_valid(idx).then(|| src.value(idx)));
            }
        }
    }};
}

/// Scatter-append selected rows from `source` into `builder`.
fn scatter_append(
    builder: &mut dyn ArrayBuilder,
    source: &dyn Array,
    indices: &[usize],
) -> Result<()> {
    use DataType::*;
    match source.data_type() {
        Boolean => {
            let src = source.as_boolean();
            let dst = builder
                .as_any_mut()
                .downcast_mut::<BooleanBuilder>()
                .unwrap();
            if src.null_count() == 0 {
                for &idx in indices {
                    dst.append_value(src.value(idx));
                }
            } else {
                for &idx in indices {
                    dst.append_option(src.is_valid(idx).then(|| src.value(idx)));
                }
            }
        }
        Int8 => scatter_primitive!(builder, source, indices, Int8Type),
        Int16 => scatter_primitive!(builder, source, indices, Int16Type),
        Int32 => scatter_primitive!(builder, source, indices, Int32Type),
        Int64 => scatter_primitive!(builder, source, indices, Int64Type),
        UInt8 => scatter_primitive!(builder, source, indices, UInt8Type),
        UInt16 => scatter_primitive!(builder, source, indices, UInt16Type),
        UInt32 => scatter_primitive!(builder, source, indices, UInt32Type),
        UInt64 => scatter_primitive!(builder, source, indices, UInt64Type),
        Float32 => scatter_primitive!(builder, source, indices, Float32Type),
        Float64 => scatter_primitive!(builder, source, indices, Float64Type),
        Date32 => scatter_primitive!(builder, source, indices, Date32Type),
        Date64 => scatter_primitive!(builder, source, indices, Date64Type),
        Timestamp(TimeUnit::Second, _) => {
            scatter_primitive!(builder, source, indices, TimestampSecondType)
        }
        Timestamp(TimeUnit::Millisecond, _) => {
            scatter_primitive!(builder, source, indices, TimestampMillisecondType)
        }
        Timestamp(TimeUnit::Microsecond, _) => {
            scatter_primitive!(builder, source, indices, TimestampMicrosecondType)
        }
        Timestamp(TimeUnit::Nanosecond, _) => {
            scatter_primitive!(builder, source, indices, TimestampNanosecondType)
        }
        Decimal128(_, _) => scatter_primitive!(builder, source, indices, Decimal128Type),
        Decimal256(_, _) => scatter_primitive!(builder, source, indices, Decimal256Type),
        Utf8 => scatter_byte_array!(builder, source, indices, i32, StringBuilder, as_string),
        LargeUtf8 => {
            scatter_byte_array!(builder, source, indices, i64, LargeStringBuilder, as_string)
        }
        Binary => scatter_byte_array!(builder, source, indices, i32, BinaryBuilder, as_binary),
        LargeBinary => {
            scatter_byte_array!(builder, source, indices, i64, LargeBinaryBuilder, as_binary)
        }
        Utf8View => {
            scatter_byte_view!(builder, source, indices, as_string_view, StringViewBuilder)
        }
        BinaryView => {
            scatter_byte_view!(builder, source, indices, as_binary_view, BinaryViewBuilder)
        }
        Null => {
            let dst = builder.as_any_mut().downcast_mut::<NullBuilder>().unwrap();
            dst.append_nulls(indices.len());
        }
        dt => {
            return Err(DataFusionError::NotImplemented(format!(
                "Scatter append not implemented for {dt}"
            )));
        }
    }
    Ok(())
}

/// Per-column strategy: scatter-write via builder for primitive/string types,
/// or accumulate taken sub-arrays for complex types (List, Map, Struct, etc.).
enum ColumnBuffer {
    /// Fast path: direct scatter into a pre-allocated builder.
    Builder(Box<dyn ArrayBuilder>),
    /// Fallback for complex types: accumulate `take`-produced sub-arrays,
    /// concatenate at flush time.
    Accumulator(Vec<ArrayRef>),
}

/// Returns true if `scatter_append` can handle this data type directly.
fn has_scatter_support(dt: &DataType) -> bool {
    use DataType::*;
    matches!(
        dt,
        Boolean
            | Int8
            | Int16
            | Int32
            | Int64
            | UInt8
            | UInt16
            | UInt32
            | UInt64
            | Float32
            | Float64
            | Date32
            | Date64
            | Timestamp(_, _)
            | Decimal128(_, _)
            | Decimal256(_, _)
            | Utf8
            | LargeUtf8
            | Binary
            | LargeBinary
            | Utf8View
            | BinaryView
            | Null
    )
}

struct PartitionBuffer {
    columns: Vec<ColumnBuffer>,
    schema: SchemaRef,
    num_rows: usize,
    target_batch_size: usize,
}

impl PartitionBuffer {
    fn new(schema: &SchemaRef, target_batch_size: usize) -> Self {
        let columns = schema
            .fields()
            .iter()
            .map(|f| {
                if has_scatter_support(f.data_type()) {
                    ColumnBuffer::Builder(make_builder(f.data_type(), target_batch_size))
                } else {
                    ColumnBuffer::Accumulator(Vec::new())
                }
            })
            .collect();
        Self {
            columns,
            schema: Arc::clone(schema),
            num_rows: 0,
            target_batch_size,
        }
    }

    fn is_full(&self) -> bool {
        self.num_rows >= self.target_batch_size
    }

    /// Finish all columns into a RecordBatch. Builders are reset (retaining
    /// capacity); accumulators are concatenated and cleared.
    fn flush(&mut self) -> Result<RecordBatch> {
        let arrays: Vec<ArrayRef> = self
            .columns
            .iter_mut()
            .map(|col| match col {
                ColumnBuffer::Builder(b) => b.finish(),
                ColumnBuffer::Accumulator(chunks) => {
                    let refs: Vec<&dyn Array> = chunks.iter().map(|a| a.as_ref()).collect();
                    let result = arrow::compute::concat(&refs)
                        .expect("concat failed for accumulated arrays");
                    chunks.clear();
                    result
                }
            })
            .collect();
        let batch = RecordBatch::try_new(Arc::clone(&self.schema), arrays)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        self.num_rows = 0;
        Ok(batch)
    }

    fn has_data(&self) -> bool {
        self.num_rows > 0
    }
}

pub(crate) struct PartitionOutputStream {
    schema: SchemaRef,
    codec: CompressionCodec,
    buffer: Vec<u8>,
}

impl PartitionOutputStream {
    pub(crate) fn try_new(schema: SchemaRef, codec: CompressionCodec) -> Result<Self> {
        Ok(Self {
            schema,
            codec,
            buffer: Vec::new(),
        })
    }

    fn write_ipc_block(&mut self, batch: &RecordBatch) -> Result<usize> {
        let start_pos = self.buffer.len();

        self.buffer.extend_from_slice(&0u64.to_le_bytes());
        let field_count = self.schema.fields().len();
        self.buffer
            .extend_from_slice(&(field_count as u64).to_le_bytes());
        let codec_tag: &[u8; 4] = match &self.codec {
            CompressionCodec::Snappy => b"SNAP",
            CompressionCodec::Lz4Frame => b"LZ4_",
            CompressionCodec::Zstd(_) => b"ZSTD",
            CompressionCodec::None => b"NONE",
        };
        self.buffer.extend_from_slice(codec_tag);

        match &self.codec {
            CompressionCodec::None => {
                let mut w = StreamWriter::try_new(&mut self.buffer, &batch.schema())?;
                w.write(batch)?;
                w.finish()?;
                w.into_inner()?;
            }
            CompressionCodec::Lz4Frame => {
                let mut wtr = lz4_flex::frame::FrameEncoder::new(&mut self.buffer);
                let mut w = StreamWriter::try_new(&mut wtr, &batch.schema())?;
                w.write(batch)?;
                w.finish()?;
                wtr.finish().map_err(|e| {
                    DataFusionError::Execution(format!("lz4 compression error: {e}"))
                })?;
            }
            CompressionCodec::Zstd(level) => {
                let enc = zstd::Encoder::new(&mut self.buffer, *level)?;
                let mut w = StreamWriter::try_new(enc, &batch.schema())?;
                w.write(batch)?;
                w.finish()?;
                w.into_inner()?.finish()?;
            }
            CompressionCodec::Snappy => {
                let mut wtr = snap::write::FrameEncoder::new(&mut self.buffer);
                let mut w = StreamWriter::try_new(&mut wtr, &batch.schema())?;
                w.write(batch)?;
                w.finish()?;
                wtr.into_inner().map_err(|e| {
                    DataFusionError::Execution(format!("snappy compression error: {e}"))
                })?;
            }
        }

        let end_pos = self.buffer.len();
        let ipc_length = (end_pos - start_pos - 8) as u64;
        if ipc_length > i32::MAX as u64 {
            return Err(DataFusionError::Execution(format!(
                "Shuffle block size {ipc_length} exceeds maximum size of {}",
                i32::MAX
            )));
        }
        self.buffer[start_pos..start_pos + 8].copy_from_slice(&ipc_length.to_le_bytes());

        Ok(end_pos - start_pos)
    }

    fn drain_buffer(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.buffer)
    }

    #[cfg(test)]
    fn finish(self) -> Result<Vec<u8>> {
        Ok(self.buffer)
    }
}

struct SpillFile {
    _temp_file: datafusion::execution::disk_manager::RefCountedTempFile,
    file: File,
}

/// A partitioner that scatter-writes incoming rows directly into pre-allocated
/// per-partition column builders. When a partition's builders reach
/// `target_batch_size`, the batch is flushed to a compressed IPC block.
/// No intermediate sub-batches or coalescers are created.
pub(crate) struct ImmediateModePartitioner {
    output_data_file: String,
    output_index_file: String,
    partition_buffers: Vec<PartitionBuffer>,
    streams: Vec<PartitionOutputStream>,
    spill_files: Vec<Option<SpillFile>>,
    partitioning: CometPartitioning,
    runtime: Arc<RuntimeEnv>,
    reservation: MemoryReservation,
    metrics: ShufflePartitionerMetrics,
    hashes_buf: Vec<u32>,
    partition_ids: Vec<u32>,
    /// Reusable per-partition row index scratch space.
    partition_row_indices: Vec<Vec<usize>>,
    /// Maximum bytes this partitioner will reserve from the memory pool.
    /// Computed as memory_pool_size * memory_fraction at construction.
    memory_limit: usize,
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

        let partition_buffers = (0..num_output_partitions)
            .map(|_| PartitionBuffer::new(&schema, batch_size))
            .collect();

        let streams = (0..num_output_partitions)
            .map(|_| PartitionOutputStream::try_new(Arc::clone(&schema), codec.clone()))
            .collect::<Result<Vec<_>>>()?;

        let spill_files: Vec<Option<SpillFile>> =
            (0..num_output_partitions).map(|_| None).collect();

        let hashes_buf = match &partitioning {
            CometPartitioning::Hash(_, _) | CometPartitioning::RoundRobin(_, _) => {
                vec![0u32; batch_size]
            }
            _ => vec![],
        };

        let memory_limit = match runtime.memory_pool.memory_limit() {
            MemoryLimit::Finite(pool_size) => pool_size,
            _ => usize::MAX,
        };

        let reservation = MemoryConsumer::new(format!("ImmediateModePartitioner[{partition}]"))
            .with_can_spill(true)
            .register(&runtime.memory_pool);

        let partition_row_indices = (0..num_output_partitions).map(|_| Vec::new()).collect();

        Ok(Self {
            output_data_file,
            output_index_file,
            partition_buffers,
            streams,
            spill_files,
            partitioning,
            runtime,
            reservation,
            metrics,
            hashes_buf,
            partition_ids: vec![0u32; batch_size],
            partition_row_indices,
            memory_limit,
        })
    }

    fn compute_partition_ids(&mut self, batch: &RecordBatch) -> Result<usize> {
        let num_rows = batch.num_rows();

        // Ensure scratch buffers are large enough for this batch
        if self.hashes_buf.len() < num_rows {
            self.hashes_buf.resize(num_rows, 0);
        }
        if self.partition_ids.len() < num_rows {
            self.partition_ids.resize(num_rows, 0);
        }

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
                assign_hash_partition_ids(hashes_buf, partition_ids, num_output_partitions);
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
                assign_hash_partition_ids(hashes_buf, partition_ids, num_output_partitions);
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
                assign_range_partition_ids(&row_batch, partition_ids, bounds);
                Ok(num_output_partitions)
            }
            other => Err(DataFusionError::NotImplemented(format!(
                "Unsupported shuffle partitioning scheme {other:?}"
            ))),
        }
    }

    /// Scatter-write rows from batch into per-partition builders, flushing
    /// any partition that reaches target_batch_size. Returns
    /// `(flushed_builder_bytes, ipc_bytes_written)`.
    ///
    /// Uses column-first iteration so each column's type dispatch happens once
    /// per batch (num_columns times) rather than once per partition per column
    /// (num_columns × num_partitions times).
    fn repartition_batch(&mut self, batch: &RecordBatch) -> Result<(usize, usize)> {
        let num_partitions = self.partition_buffers.len();
        let num_rows = batch.num_rows();

        // Build per-partition row indices, reusing scratch vecs
        for indices in self.partition_row_indices.iter_mut() {
            indices.clear();
        }
        for row_idx in 0..num_rows {
            let pid = self.partition_ids[row_idx] as usize;
            self.partition_row_indices[pid].push(row_idx);
        }

        // Column-first scatter: resolve each column's type once, then
        // scatter across all partitions with the same typed path.
        for col_idx in 0..batch.num_columns() {
            let source = batch.column(col_idx);
            for pid in 0..num_partitions {
                let indices = &self.partition_row_indices[pid];
                if indices.is_empty() {
                    continue;
                }
                match &mut self.partition_buffers[pid].columns[col_idx] {
                    ColumnBuffer::Builder(builder) => {
                        scatter_append(builder.as_mut(), source.as_ref(), indices)?;
                    }
                    ColumnBuffer::Accumulator(chunks) => {
                        let idx_array =
                            UInt32Array::from_iter_values(indices.iter().map(|&i| i as u32));
                        let taken = take(source.as_ref(), &idx_array, None)
                            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
                        chunks.push(taken);
                    }
                }
            }
        }

        // Update row counts and flush full partitions
        let mut flushed_builder_bytes = 0usize;
        let mut ipc_bytes = 0usize;
        for pid in 0..num_partitions {
            let added = self.partition_row_indices[pid].len();
            if added == 0 {
                continue;
            }
            self.partition_buffers[pid].num_rows += added;
            if self.partition_buffers[pid].is_full() {
                let (builder_bytes, written) = self.flush_partition(pid)?;
                flushed_builder_bytes += builder_bytes;
                ipc_bytes += written;
            }
        }

        Ok((flushed_builder_bytes, ipc_bytes))
    }

    /// Flush a partition's builders to an IPC block in its output stream.
    /// Returns `(flushed_batch_memory, ipc_bytes_written)`.
    fn flush_partition(&mut self, pid: usize) -> Result<(usize, usize)> {
        let output_batch = self.partition_buffers[pid].flush()?;
        let batch_mem = output_batch.get_array_memory_size();
        let mut encode_timer = self.metrics.encode_time.timer();
        let ipc_bytes = self.streams[pid].write_ipc_block(&output_batch)?;
        encode_timer.stop();
        Ok((batch_mem, ipc_bytes))
    }

    /// Spill all partition IPC buffers to per-partition temp files.
    fn spill_all(&mut self) -> Result<()> {
        let mut spilled_bytes = 0usize;

        // Flush any partially-filled partition builders
        for pid in 0..self.partition_buffers.len() {
            if self.partition_buffers[pid].has_data() {
                self.flush_partition(pid)?;
            }
        }

        // Drain IPC buffers to disk
        for pid in 0..self.streams.len() {
            let buf = self.streams[pid].drain_buffer();
            if buf.is_empty() {
                continue;
            }

            if self.spill_files[pid].is_none() {
                let temp_file = self
                    .runtime
                    .disk_manager
                    .create_tmp_file(&format!("imm_shuffle_p{pid}"))?;
                let path = temp_file.path().to_owned();
                let file = OpenOptions::new().append(true).open(&path).map_err(|e| {
                    DataFusionError::Execution(format!("Failed to open spill file: {e}"))
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

        let batch_mem = batch.get_array_memory_size();
        self.metrics.data_size.add(batch_mem);
        self.metrics.baseline.record_output(batch.num_rows());

        let repart_start = Instant::now();
        self.compute_partition_ids(&batch)?;
        self.metrics
            .repart_time
            .add_duration(repart_start.elapsed());

        let (flushed_builder_bytes, ipc_growth) = self.repartition_batch(&batch)?;
        let builder_growth = batch_mem;

        // Net memory change: data entered builders, some was flushed to IPC
        let net_growth = (builder_growth + ipc_growth).saturating_sub(flushed_builder_bytes);

        if net_growth > 0 {
            // Use our own memory limit rather than relying solely on the pool,
            // since the pool doesn't see builder allocations directly.
            if self.reservation.size() + net_growth > self.memory_limit
                || self.reservation.try_grow(net_growth).is_err()
            {
                self.spill_all()?;
            }
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

        let mut output_data = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.output_data_file)
            .map_err(|e| DataFusionError::Execution(format!("shuffle write error: {e:?}")))?;

        #[allow(clippy::needless_range_loop)]
        for pid in 0..num_output_partitions {
            offsets[pid] = output_data.stream_position()? as i64;

            if let Some(spill) = &self.spill_files[pid] {
                let path = spill._temp_file.path().to_owned();
                let spill_reader = File::open(&path).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to open spill file for reading: {e}"
                    ))
                })?;
                let mut write_timer = self.metrics.write_time.timer();
                std::io::copy(&mut &spill_reader, &mut output_data)?;
                write_timer.stop();
            }

            if self.partition_buffers[pid].has_data() {
                self.flush_partition(pid)?;
            }

            let buf = self.streams[pid].drain_buffer();
            if !buf.is_empty() {
                let mut write_timer = self.metrics.write_time.timer();
                output_data.write_all(&buf)?;
                write_timer.stop();
            }
        }

        for spill in self.spill_files.iter_mut() {
            *spill = None;
        }

        offsets[num_output_partitions] = output_data.stream_position()? as i64;

        let mut write_timer = self.metrics.write_time.timer();
        let mut output_index = BufWriter::new(
            File::create(&self.output_index_file)
                .map_err(|e| DataFusionError::Execution(format!("shuffle write error: {e:?}")))?,
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
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::memory_pool::GreedyMemoryPool;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;

    fn make_test_batch(values: &[i32]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let array = Int32Array::from(values.to_vec());
        RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()
    }

    #[test]
    fn test_scatter_append_primitives() {
        let array: ArrayRef = Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50]));
        let mut builder = make_builder(&DataType::Int32, 8);
        scatter_append(builder.as_mut(), array.as_ref(), &[0, 2, 4]).unwrap();
        let result = builder.finish();
        let result = result.as_primitive::<Int32Type>();
        assert_eq!(result.values().as_ref(), &[10, 30, 50]);
    }

    #[test]
    fn test_scatter_append_strings() {
        let array: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c", "d"]));
        let mut builder = make_builder(&DataType::Utf8, 4);
        scatter_append(builder.as_mut(), array.as_ref(), &[1, 3]).unwrap();
        let result = builder.finish();
        let result = result.as_string::<i32>();
        assert_eq!(result.value(0), "b");
        assert_eq!(result.value(1), "d");
    }

    #[test]
    fn test_scatter_append_nulls() {
        let array: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)]));
        let mut builder = make_builder(&DataType::Int32, 4);
        scatter_append(builder.as_mut(), array.as_ref(), &[0, 1, 2]).unwrap();
        let result = builder.finish();
        let result = result.as_primitive::<Int32Type>();
        assert!(result.is_valid(0));
        assert!(result.is_null(1));
        assert!(result.is_valid(2));
    }

    #[test]
    fn test_partition_buffer_flush_reuse() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch = make_test_batch(&[1, 2, 3, 4, 5]);

        let mut buf = PartitionBuffer::new(&schema, 3);
        match &mut buf.columns[0] {
            ColumnBuffer::Builder(b) => {
                scatter_append(b.as_mut(), batch.column(0).as_ref(), &[0, 1, 2]).unwrap()
            }
            _ => panic!("expected Builder"),
        }
        buf.num_rows += 3;
        assert!(buf.is_full());

        let flushed = buf.flush().unwrap();
        assert_eq!(flushed.num_rows(), 3);
        assert_eq!(buf.num_rows, 0);

        // Builders are reused after flush
        match &mut buf.columns[0] {
            ColumnBuffer::Builder(b) => {
                scatter_append(b.as_mut(), batch.column(0).as_ref(), &[3, 4]).unwrap()
            }
            _ => panic!("expected Builder"),
        }
        buf.num_rows += 2;
        assert_eq!(buf.num_rows, 2);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_partition_output_stream_write_and_read() {
        let batch = make_test_batch(&[1, 2, 3, 4, 5]);
        let schema = batch.schema();

        for codec in [
            CompressionCodec::None,
            CompressionCodec::Lz4Frame,
            CompressionCodec::Zstd(1),
            CompressionCodec::Snappy,
        ] {
            let mut stream = PartitionOutputStream::try_new(Arc::clone(&schema), codec).unwrap();
            stream.write_ipc_block(&batch).unwrap();

            let buf = stream.finish().unwrap();
            assert!(!buf.is_empty());

            let ipc_length = u64::from_le_bytes(buf[0..8].try_into().unwrap()) as usize;
            assert!(ipc_length > 0);

            let block_end = 8 + ipc_length;
            let ipc_data = &buf[16..block_end];
            let batch2 = read_ipc_compressed(ipc_data).unwrap();
            assert_eq!(batch2.num_rows(), 5);
        }
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

        let total_rows: usize = partitioner
            .partition_buffers
            .iter()
            .map(|b| b.num_rows)
            .sum();
        assert_eq!(total_rows, 8);
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
    #[cfg_attr(miri, ignore)] // spill uses std::io::copy which triggers copy_file_range
    async fn test_immediate_mode_spill() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let dir = tempfile::tempdir().unwrap();
        let data_path = dir.path().join("data").to_str().unwrap().to_string();
        let index_path = dir.path().join("index").to_str().unwrap().to_string();

        let num_partitions = 2;
        let metrics = ShufflePartitionerMetrics::new(&ExecutionPlanMetricsSet::new(), 0);

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

        for i in 0..10 {
            let values: Vec<i32> = ((i * 10)..((i + 1) * 10)).collect();
            let batch = make_test_batch(&values);
            partitioner.insert_batch(batch).await.unwrap();
        }

        partitioner.shuffle_write().unwrap();

        let index_data = std::fs::read(&index_path).unwrap();
        assert_eq!(index_data.len(), (num_partitions + 1) * 8);

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

        // Small target to trigger flush during insert
        let mut partitioner = ImmediateModePartitioner::try_new(
            0,
            data_path.clone(),
            index_path.clone(),
            Arc::clone(&schema),
            make_hash_partitioning("a", num_partitions),
            metrics,
            runtime,
            4,
            CompressionCodec::Lz4Frame,
        )
        .unwrap();

        partitioner.insert_batch(batch).await.unwrap();
        partitioner.shuffle_write().unwrap();

        let index_data = std::fs::read(&index_path).unwrap();
        let mut offsets = Vec::new();
        for i in 0..=num_partitions {
            let offset = i64::from_le_bytes(index_data[i * 8..(i + 1) * 8].try_into().unwrap());
            offsets.push(offset as usize);
        }

        let data = std::fs::read(&data_path).unwrap();
        let mut total_rows = 0;
        for pid in 0..num_partitions {
            let (start, end) = (offsets[pid], offsets[pid + 1]);
            if start == end {
                continue;
            }
            let mut pos = start;
            while pos < end {
                let payload_len =
                    u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap()) as usize;
                assert!(payload_len > 0);
                let block_end = pos + 8 + payload_len;
                let ipc_data = &data[pos + 16..block_end];
                let decoded = read_ipc_compressed(ipc_data).unwrap();
                assert_eq!(decoded.num_columns(), 1);
                assert!(decoded.num_rows() > 0);
                let col = decoded
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                for i in 0..col.len() {
                    assert!((1..=10).contains(&col.value(i)));
                }
                total_rows += decoded.num_rows();
                pos = block_end;
            }
            assert_eq!(pos, end);
        }
        assert_eq!(total_rows, 10);
    }
}
