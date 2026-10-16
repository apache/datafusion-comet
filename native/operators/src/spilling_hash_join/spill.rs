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

use std::io::{BufWriter, Write};
use std::sync::Arc;

use arrow::array::{layout, Array, ArrayRef, AsArray, BufferSpec};
use arrow::datatypes::{DataType, Schema, SchemaRef};
use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use arrow::ipc::{CompressionType, MetadataVersion};
use arrow::record_batch::RecordBatch;
use datafusion::common::config::SpillCompression;
use datafusion::common::utils::memory::get_record_batch_memory_size;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::spill_file::{SpillFile, SpillWriter};
use datafusion::physical_plan::metrics::SpillMetrics;
use datafusion::physical_plan::spill::SpillManager;
use datafusion::physical_plan::SendableRecordBatchStream;

/// A finished spill file and what it holds.
#[derive(Clone)]
pub(super) struct SpilledFile {
    file: Arc<dyn SpillFile>,
    pub(super) rows: usize,
    /// The memory the batches hold once read back, as `HashJoinExec` counts them, which can be
    /// more than they held when written. A batch read back from an uncompressed file shares the
    /// buffer its message was read into, metadata included, so the file's size bounds it. A batch
    /// read back from a compressed file holds its decompressed buffers, and also the buffer of its
    /// compressed message, whose small buffers are stored uncompressed and read without a copy.
    pub(super) bytes: usize,
    /// The in-memory size of the largest batch written, which bounds what reading it back holds.
    pub(super) max_batch_bytes: usize,
}

impl SpilledFile {
    /// Reads the file back. `SpillManager` reads ahead by two batches.
    pub(super) fn read(&self, manager: &SpillManager) -> Result<SendableRecordBatchStream> {
        manager.read_spill_as_stream(Arc::clone(&self.file), Some(self.max_batch_bytes))
    }

    /// The memory reading the file back holds besides the batch being processed.
    pub(super) fn read_ahead_bytes(&self) -> usize {
        self.max_batch_bytes.saturating_mul(2)
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
}

impl SpillWriterFactory {
    pub(super) fn new(
        env: Arc<RuntimeEnv>,
        schema: SchemaRef,
        compression: SpillCompression,
        metrics: SpillMetrics,
        description: &'static str,
    ) -> Self {
        Self {
            env,
            schema,
            compression,
            metrics,
            description,
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
            rows: 0,
            batches: 0,
            bytes: 0,
            max_batch_bytes: 0,
            metrics: self.metrics.clone(),
            compressed: Option::<CompressionType>::from(self.compression).is_some(),
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
    rows: usize,
    batches: usize,
    bytes: usize,
    max_batch_bytes: usize,
    metrics: SpillMetrics,
    compressed: bool,
}

impl SpillFileWriter {
    pub(super) fn append(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let batch = gc_view_arrays(batch)?;
        let size = get_record_batch_memory_size(&batch);
        self.writer.write(&batch)?;
        self.rows += batch.num_rows();
        self.batches += 1;
        self.bytes += size;
        self.max_batch_bytes = self.max_batch_bytes.max(size);
        self.metrics.spilled_rows.add(batch.num_rows());
        Ok(())
    }

    pub(super) fn finish(mut self) -> Result<SpilledFile> {
        self.writer.finish()?;
        let mut sink = self
            .writer
            .into_inner()?
            .into_inner()
            .map_err(|e| DataFusionError::IoError(e.into_error()))?;
        sink.inner.finish()?;
        self.metrics.spilled_bytes.add(sink.written);
        let rounding = self.batches.saturating_mul(64);
        let read_back = if self.compressed {
            self.bytes.saturating_add(sink.written)
        } else {
            self.bytes.max(sink.written)
        };
        Ok(SpilledFile {
            file: self.file,
            rows: self.rows,
            bytes: read_back.saturating_add(rounding),
            max_batch_bytes: self.max_batch_bytes,
        })
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
