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

//! Spill files of [`super::SpillingHashJoinExec`].
//!
//! The files are Arrow IPC streams, written the way DataFusion 55.2 writes its own spill files
//! (`IPCStreamWriter`), so that `SpillManager::read_spill_as_stream` reads them back. DataFusion's
//! writer, `InProgressSpillFile`, cannot be named outside DataFusion, and the operator needs one
//! open writer per spilled bucket.
//!
//! Routing splits each input batch into one part per bucket, a sixteenth of a batch each, and a
//! split bucket's parts are split again. Written as they come, every part would cost an IPC
//! message, a compression frame for each of its buffers, and on reading back, a decode and a
//! probe batch of the inner join. So a writer holds the parts it is given until they make a full
//! batch, and writes them as one.

use std::io::{BufWriter, Write};
use std::sync::Arc;

use arrow::array::{layout, Array, ArrayRef, AsArray, BufferSpec, UInt32Array};
use arrow::compute::{concat_batches, take};
use arrow::datatypes::{DataType, Schema, SchemaRef};
use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use arrow::ipc::{CompressionType, MetadataVersion};
use arrow::record_batch::RecordBatch;
use datafusion::common::config::SpillCompression;
use datafusion::common::utils::memory::get_record_batch_memory_size;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::memory_pool::MemoryReservation;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::spill_file::{SpillFile, SpillWriter};
use datafusion::physical_plan::metrics::SpillMetrics;
use datafusion::physical_plan::spill::SpillManager;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;

use super::concat::{self, record_value_bytes, Concatenator};
use super::memory::try_reserve;

/// The most memory a writer holds in parts not yet written. With 16 buckets per side, this caps
/// what the parts of one side hold at 4 MiB, and a batch of narrow rows still reaches the target
/// row count.
const MAX_PENDING_BYTES: usize = 256 * 1024;

/// A finished spill file and what it holds.
#[derive(Clone)]
pub(super) struct SpilledFile {
    file: Arc<dyn SpillFile>,
    pub(super) rows: usize,
    /// The memory the batches held when written, which is what they hold once read back by
    /// [`Self::read_compacted`], as `HashJoinExec` counts them, up to the rounding of buffers.
    pub(super) bytes: usize,
    /// The in-memory size of the largest batch written.
    pub(super) max_batch_bytes: usize,
    /// The value bytes of each column, when every column is one that [`concat`] handles.
    value_bytes: Option<Vec<usize>>,
}

impl SpilledFile {
    /// Reads the file back. `SpillManager` reads ahead by two batches.
    ///
    /// A batch read back can hold up to twice the memory it held when written. DataFusion reads a
    /// spill file in chunks of 128 KiB, and arrow's `StreamDecoder` assembles a message that spans
    /// chunks in a buffer that grows by doubling, then slices the batch's buffers from it. A batch
    /// read from a compressed file holds its decompressed buffers, and also that buffer if any
    /// of its buffers were stored uncompressed.
    pub(super) fn read(&self, manager: &SpillManager) -> Result<SendableRecordBatchStream> {
        manager.read_spill_as_stream(Arc::clone(&self.file), Some(self.max_batch_bytes))
    }

    /// Reads the file back with each batch copied into buffers of its own size, so that the
    /// batches hold [`Self::bytes`] together. For a build side, which is held whole.
    pub(super) fn read_compacted(
        &self,
        manager: &SpillManager,
    ) -> Result<SendableRecordBatchStream> {
        let stream = self.read(manager)?;
        let schema = stream.schema();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream.map(|batch| batch.and_then(|batch| compact(&batch))),
        )))
    }

    /// Whether [`Self::read_as_one_batch`] reads the file back as one batch.
    pub(super) fn reads_as_one_batch(&self) -> bool {
        self.value_bytes.is_some()
    }

    /// Reads the file back as one batch, in buffers allocated once at their final size, which
    /// together hold no more than [`Self::bytes`]. None if its columns are not ones [`concat`]
    /// handles.
    pub(super) async fn read_as_one_batch(
        &self,
        manager: &SpillManager,
    ) -> Result<Option<RecordBatch>> {
        let Some(value_bytes) = &self.value_bytes else {
            return Ok(None);
        };
        let mut stream = self.read(manager)?;
        let mut batch = Concatenator::new(stream.schema(), self.rows, value_bytes);
        while let Some(part) = stream.next().await.transpose()? {
            batch.push(&part)?;
        }
        Ok(Some(batch.finish()?))
    }

    /// The memory reading the file back holds besides the batch being processed: two batches
    /// read ahead, each holding up to twice what it held when written.
    pub(super) fn read_ahead_bytes(&self) -> usize {
        self.max_batch_bytes.saturating_mul(4)
    }
}

/// Creates the spill files of one side of the join.
#[derive(Clone)]
pub(super) struct SpillWriterFactory {
    env: Arc<RuntimeEnv>,
    schema: SchemaRef,
    compression: SpillCompression,
    metrics: SpillMetrics,
    description: &'static str,
    /// The rows of a batch written while memory allows.
    batch_rows: usize,
    /// The memory of the parts that the writers hold, shared by every writer of the join.
    pending: Arc<MemoryReservation>,
}

impl SpillWriterFactory {
    pub(super) fn new(
        env: Arc<RuntimeEnv>,
        schema: SchemaRef,
        compression: SpillCompression,
        metrics: SpillMetrics,
        description: &'static str,
        batch_rows: usize,
        pending: Arc<MemoryReservation>,
    ) -> Self {
        Self {
            env,
            schema,
            compression,
            metrics,
            description,
            batch_rows: batch_rows.max(1),
            pending,
        }
    }

    pub(super) fn create(&self) -> Result<SpillFileWriter> {
        let file = self.env.disk_manager.create_tmp_file(self.description)?;
        let options =
            IpcWriteOptions::try_new(ipc_alignment(&self.schema), false, MetadataVersion::V5)?
                .try_with_compression(Option::<CompressionType>::from(self.compression))?;
        let sink = BufWriter::new(CountingWriter {
            inner: file.open_writer()?,
            written: 0,
        });
        let writer = StreamWriter::try_new_with_options(sink, &self.schema, options)?;
        self.metrics.spill_file_count.add(1);
        Ok(SpillFileWriter {
            file,
            writer,
            schema: Arc::clone(&self.schema),
            parts: Parts {
                batches: Vec::new(),
                rows: 0,
                bytes: 0,
                reservation: Arc::clone(&self.pending),
            },
            batch_rows: self.batch_rows,
            value_bytes: concat::supports(&self.schema)
                .then(|| vec![0; self.schema.fields().len()]),
            rows: 0,
            batches: 0,
            bytes: 0,
            max_batch_bytes: 0,
            metrics: self.metrics.clone(),
        })
    }
}

/// Appends batches to one spill file.
pub(super) struct SpillFileWriter {
    /// Declared before `file`, so that a writer dropped unfinished flushes before the file is
    /// released. DataFusion subtracts a file's bytes from the temp directory's usage when the file
    /// is released, so bytes flushed after that would stay counted.
    writer: StreamWriter<BufWriter<CountingWriter>>,
    file: Arc<dyn SpillFile>,
    schema: SchemaRef,
    parts: Parts,
    batch_rows: usize,
    /// The value bytes of each column so far, when every column is one that [`concat`] handles.
    value_bytes: Option<Vec<usize>>,
    rows: usize,
    batches: usize,
    bytes: usize,
    max_batch_bytes: usize,
    metrics: SpillMetrics,
}

impl SpillFileWriter {
    /// Appends `batch`. It is held with the parts before it until they make a full batch, if the
    /// pool grants the memory, and is otherwise written with them right away.
    pub(super) fn append(&mut self, batch: RecordBatch) -> Result<()> {
        let rows = batch.num_rows();
        if rows == 0 {
            return Ok(());
        }
        if rows >= self.batch_rows {
            self.flush()?;
            return self.write(&batch);
        }
        let size = get_record_batch_memory_size(&batch);
        let full = self.parts.rows + rows >= self.batch_rows
            || self.parts.bytes + size >= MAX_PENDING_BYTES;
        if !full && try_reserve(&self.parts.reservation, size)? {
            self.parts.batches.push(batch);
            self.parts.rows += rows;
            self.parts.bytes += size;
            return Ok(());
        }
        self.parts.batches.push(batch);
        self.flush()
    }

    /// Writes the parts held as one batch.
    fn flush(&mut self) -> Result<()> {
        let batch = match self.parts.batches.len() {
            0 => return Ok(()),
            1 => self.parts.batches.pop().expect("one part"),
            _ => concat_batches(&self.schema, &self.parts.batches)?,
        };
        self.parts.clear();
        self.write(&batch)
    }

    fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        let batch = gc_view_arrays(batch)?;
        let size = get_record_batch_memory_size(&batch);
        if let Some(value_bytes) = &mut self.value_bytes {
            record_value_bytes(&batch, value_bytes);
        }
        self.writer.write(&batch)?;
        self.rows += batch.num_rows();
        self.batches += 1;
        self.bytes += size;
        self.max_batch_bytes = self.max_batch_bytes.max(size);
        self.metrics.spilled_rows.add(batch.num_rows());
        Ok(())
    }

    pub(super) fn finish(mut self) -> Result<SpilledFile> {
        self.flush()?;
        self.writer.finish()?;
        let mut sink = self
            .writer
            .into_inner()?
            .into_inner()
            .map_err(|e| DataFusionError::IoError(e.into_error()))?;
        sink.inner.finish()?;
        self.metrics.spilled_bytes.add(sink.written);
        // A buffer copied when read back can be rounded up differently than the one written.
        let rounding = self
            .batches
            .saturating_mul(buffer_count(&self.schema))
            .saturating_mul(64);
        Ok(SpilledFile {
            file: self.file,
            rows: self.rows,
            bytes: self.bytes.saturating_add(rounding),
            max_batch_bytes: self.max_batch_bytes,
            value_bytes: self.value_bytes,
        })
    }
}

/// The parts a writer holds until they make a full batch: written as one batch once they hold
/// `batch_rows` rows or `MAX_PENDING_BYTES` bytes, or when memory for the next part is refused.
struct Parts {
    batches: Vec<RecordBatch>,
    rows: usize,
    /// The bytes of `batches` reserved in `reservation`, which a part written as soon as it came
    /// does not count in.
    bytes: usize,
    reservation: Arc<MemoryReservation>,
}

impl Parts {
    fn clear(&mut self) {
        self.batches.clear();
        self.reservation.shrink(self.bytes);
        self.rows = 0;
        self.bytes = 0;
    }
}

impl Drop for Parts {
    /// A writer dropped unfinished, after an error or by a cancelled task, releases its parts.
    fn drop(&mut self) {
        self.reservation.shrink(self.bytes);
    }
}

/// Counts the bytes written to disk, for the `spilled_bytes` metric.
struct CountingWriter {
    inner: Box<dyn SpillWriter>,
    written: usize,
}

impl Write for CountingWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(buf)?;
        self.written += written;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// The buffers of a batch of `schema`, nulls included, and those of nested and dictionary values.
fn buffer_count(schema: &Schema) -> usize {
    fn of(data_type: &DataType) -> usize {
        let own = layout(data_type).buffers.len() + 1;
        let nested = match data_type {
            DataType::Dictionary(_, values) => of(values),
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::ListView(field)
            | DataType::LargeListView(field)
            | DataType::FixedSizeList(field, _)
            | DataType::Map(field, _) => of(field.data_type()),
            DataType::Struct(fields) => fields.iter().map(|field| of(field.data_type())).sum(),
            DataType::Union(fields, _) => {
                fields.iter().map(|(_, field)| of(field.data_type())).sum()
            }
            DataType::RunEndEncoded(run_ends, values) => {
                of(run_ends.data_type()) + of(values.data_type())
            }
            _ => 0,
        };
        own + nested
    }
    schema
        .fields()
        .iter()
        .map(|field| of(field.data_type()))
        .sum()
}

/// `get_max_alignment_for_schema`: the largest buffer alignment any column needs, so that reading
/// the file back does not copy buffers to align them.
fn ipc_alignment(schema: &Schema) -> usize {
    let minimum = 8;
    schema
        .fields()
        .iter()
        .flat_map(|field| layout(field.data_type()).buffers)
        .map(|buffer| match buffer {
            BufferSpec::FixedWidth { alignment, .. } => alignment,
            _ => minimum,
        })
        .fold(minimum, usize::max)
}

/// A copy of `batch` in buffers of its own size, which hold what the batch held when it was
/// written.
fn compact(batch: &RecordBatch) -> Result<RecordBatch> {
    let columns = batch
        .columns()
        .iter()
        .map(compact_array)
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(batch.schema(), columns)?)
}

/// `take` allocates exactly what the rows need, but a dictionary taken shares its values and a
/// view array taken shares its data buffers, so those are copied too.
fn compact_array(array: &ArrayRef) -> Result<ArrayRef> {
    let rows = u32::try_from(array.len())
        .map_err(|_| DataFusionError::Internal("a spilled batch holds too many rows".into()))?;
    let copy = take(array, &UInt32Array::from_iter_values(0..rows), None)?;
    Ok(match copy.data_type() {
        DataType::Dictionary(_, _) => {
            let dictionary = copy.as_any_dictionary();
            dictionary.with_values(compact_array(dictionary.values())?)
        }
        DataType::Utf8View => Arc::new(copy.as_string_view().gc()),
        DataType::BinaryView => Arc::new(copy.as_binary_view().gc()),
        _ => copy,
    })
}

/// Compacts view arrays before they are written. A view array taken from a larger one keeps all
/// of its data buffers, so writing it as is would write them all.
fn gc_view_arrays(batch: &RecordBatch) -> Result<RecordBatch> {
    let has_views = batch.columns().iter().any(|column| {
        matches!(
            column.data_type(),
            DataType::Utf8View | DataType::BinaryView
        )
    });
    if !has_views {
        return Ok(batch.clone());
    }
    let columns = batch
        .columns()
        .iter()
        .map(|column| -> ArrayRef {
            match column.data_type() {
                DataType::Utf8View => Arc::new(column.as_string_view().gc()),
                DataType::BinaryView => Arc::new(column.as_binary_view().gc()),
                _ => Arc::clone(column),
            }
        })
        .collect();
    Ok(RecordBatch::try_new(batch.schema(), columns)?)
}
