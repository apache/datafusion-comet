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

//! Spill file I/O and helper `ExecutionPlan` wrappers used by the Grace Hash
//! Join operator.
//!
//! Contains:
//! - [`SpillWriter`]: incremental append to Arrow IPC spill files.
//! - [`SpillReaderExec`]: streaming `ExecutionPlan` that reads spill files on
//!   a blocking thread and coalesces small sub-batches.
//! - [`StreamSourceExec`]: wraps an existing `SendableRecordBatchStream` so it
//!   can be used as an input to `HashJoinExec` on the fast path.
//! - [`read_spilled_batches`]: eager read helper for small, bounded spill files.

use std::any::Any;
use std::fmt;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::sync::{Arc, Mutex};

use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use arrow::ipc::CompressionType;
use arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::execution::context::TaskContext;
use datafusion::execution::disk_manager::RefCountedTempFile;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use tokio::sync::mpsc;

use super::{SPILL_IO_BUFFER_SIZE, SPILL_READ_COALESCE_TARGET};

// ---------------------------------------------------------------------------
// SpillWriter: incremental append to Arrow IPC spill files
// ---------------------------------------------------------------------------

/// Wraps an Arrow IPC `StreamWriter` for incremental spill writes.
/// Avoids the O(n²) read-rewrite pattern by keeping the writer open.
pub(super) struct SpillWriter {
    writer: StreamWriter<BufWriter<File>>,
    temp_file: RefCountedTempFile,
    bytes_written: usize,
}

impl SpillWriter {
    /// Create a new spill writer backed by a temp file.
    pub(super) fn new(temp_file: RefCountedTempFile, schema: &SchemaRef) -> DFResult<Self> {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(temp_file.path())
            .map_err(|e| DataFusionError::Execution(format!("Failed to open spill file: {e}")))?;
        let buf_writer = BufWriter::with_capacity(SPILL_IO_BUFFER_SIZE, file);
        let write_options =
            IpcWriteOptions::default().try_with_compression(Some(CompressionType::LZ4_FRAME))?;
        let writer = StreamWriter::try_new_with_options(buf_writer, schema, write_options)?;
        Ok(Self {
            writer,
            temp_file,
            bytes_written: 0,
        })
    }

    /// Append a single batch to the spill file.
    pub(super) fn write_batch(&mut self, batch: &RecordBatch) -> DFResult<()> {
        if batch.num_rows() > 0 {
            self.bytes_written += batch.get_array_memory_size();
            self.writer.write(batch)?;
        }
        Ok(())
    }

    /// Append multiple batches to the spill file.
    pub(super) fn write_batches(&mut self, batches: &[RecordBatch]) -> DFResult<()> {
        for batch in batches {
            self.write_batch(batch)?;
        }
        Ok(())
    }

    /// Finish writing. Must be called before reading back.
    pub(super) fn finish(mut self) -> DFResult<(RefCountedTempFile, usize)> {
        self.writer.finish()?;
        Ok((self.temp_file, self.bytes_written))
    }
}

// ---------------------------------------------------------------------------
// SpillReaderExec: streaming ExecutionPlan for reading spill files
// ---------------------------------------------------------------------------

/// An ExecutionPlan that streams record batches from an Arrow IPC spill file.
/// Used during the join phase so that spilled probe data is read on-demand
/// instead of loaded entirely into memory.
#[derive(Debug)]
pub(super) struct SpillReaderExec {
    spill_file: RefCountedTempFile,
    schema: SchemaRef,
    cache: Arc<PlanProperties>,
}

impl SpillReaderExec {
    pub(super) fn new(spill_file: RefCountedTempFile, schema: SchemaRef) -> Self {
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        ));
        Self {
            spill_file,
            schema,
            cache,
        }
    }
}

impl DisplayAs for SpillReaderExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SpillReaderExec")
    }
}

impl ExecutionPlan for SpillReaderExec {
    fn name(&self) -> &str {
        "SpillReaderExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let stream_schema = Arc::clone(&self.schema);
        let coalesce_schema = Arc::clone(&self.schema);
        let path = self.spill_file.path().to_path_buf();
        // Move the spill file handle into the blocking closure to keep
        // the temp file alive until the reader is done.
        let spill_file_handle = self.spill_file.clone();

        // Use a channel so file I/O runs on a blocking thread and doesn't
        // block the async executor. This lets select_all interleave multiple
        // partition streams effectively.
        let (tx, rx) = mpsc::channel::<DFResult<RecordBatch>>(4);

        tokio::task::spawn_blocking(move || {
            let _keep_alive = spill_file_handle;
            let file = match File::open(&path) {
                Ok(f) => f,
                Err(e) => {
                    let _ = tx.blocking_send(Err(DataFusionError::Execution(format!(
                        "Failed to open spill file: {e}"
                    ))));
                    return;
                }
            };
            let reader = match StreamReader::try_new(
                BufReader::with_capacity(SPILL_IO_BUFFER_SIZE, file),
                None,
            ) {
                Ok(r) => r,
                Err(e) => {
                    let _ = tx.blocking_send(Err(DataFusionError::ArrowError(Box::new(e), None)));
                    return;
                }
            };

            // Coalesce small sub-batches into larger ones to reduce per-batch
            // overhead in the downstream hash join.
            let mut pending: Vec<RecordBatch> = Vec::new();
            let mut pending_rows = 0usize;

            for batch_result in reader {
                let batch = match batch_result {
                    Ok(b) => b,
                    Err(e) => {
                        let _ =
                            tx.blocking_send(Err(DataFusionError::ArrowError(Box::new(e), None)));
                        return;
                    }
                };
                if batch.num_rows() == 0 {
                    continue;
                }
                pending_rows += batch.num_rows();
                pending.push(batch);

                if pending_rows >= SPILL_READ_COALESCE_TARGET {
                    let merged = if pending.len() == 1 {
                        Ok(pending.pop().unwrap())
                    } else {
                        concat_batches(&coalesce_schema, &pending)
                            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
                    };
                    pending.clear();
                    pending_rows = 0;
                    if tx.blocking_send(merged).is_err() {
                        return;
                    }
                }
            }

            // Flush remaining
            if !pending.is_empty() {
                let merged = if pending.len() == 1 {
                    Ok(pending.pop().unwrap())
                } else {
                    concat_batches(&coalesce_schema, &pending)
                        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
                };
                let _ = tx.blocking_send(merged);
            }
        });

        let batch_stream = futures::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|batch| (batch, rx))
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            stream_schema,
            batch_stream,
        )))
    }
}

// ---------------------------------------------------------------------------
// StreamSourceExec: wrap an existing stream as an ExecutionPlan
// ---------------------------------------------------------------------------

/// An ExecutionPlan that yields batches from a pre-existing stream.
/// Used in the fast path to feed the probe side's live stream into
/// a `HashJoinExec` without buffering or spilling.
pub(super) struct StreamSourceExec {
    stream: Mutex<Option<SendableRecordBatchStream>>,
    schema: SchemaRef,
    cache: Arc<PlanProperties>,
}

impl StreamSourceExec {
    pub(super) fn new(stream: SendableRecordBatchStream, schema: SchemaRef) -> Self {
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        ));
        Self {
            stream: Mutex::new(Some(stream)),
            schema,
            cache,
        }
    }
}

impl fmt::Debug for StreamSourceExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("StreamSourceExec").finish()
    }
}

impl DisplayAs for StreamSourceExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "StreamSourceExec")
    }
}

impl ExecutionPlan for StreamSourceExec {
    fn name(&self) -> &str {
        "StreamSourceExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        self.stream
            .lock()
            .map_err(|e| DataFusionError::Internal(format!("lock poisoned: {e}")))?
            .take()
            .ok_or_else(|| {
                DataFusionError::Internal("StreamSourceExec: stream already consumed".to_string())
            })
    }
}

// ---------------------------------------------------------------------------
// Spill reading
// ---------------------------------------------------------------------------

/// Read record batches from a finished spill file.
pub(super) fn read_spilled_batches(
    spill_file: &RefCountedTempFile,
) -> DFResult<Vec<RecordBatch>> {
    let file = File::open(spill_file.path())
        .map_err(|e| DataFusionError::Execution(format!("Failed to open spill file: {e}")))?;
    let reader = BufReader::with_capacity(SPILL_IO_BUFFER_SIZE, file);
    let stream_reader = StreamReader::try_new(reader, None)?;
    let batches: Vec<RecordBatch> = stream_reader.into_iter().collect::<Result<Vec<_>, _>>()?;
    Ok(batches)
}
