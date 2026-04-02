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

use arrow::array::RecordBatch;
use arrow::compute::kernels::coalesce::BatchCoalescer;
use arrow::datatypes::SchemaRef;
use arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use datafusion::physical_plan::metrics::Time;
use std::io::Write;

/// Writes batches to a persistent Arrow IPC `StreamWriter`. The schema is written once
/// when the writer is created. Small batches are coalesced via [`BatchCoalescer`] before
/// serialization, producing `batch_size`-row output batches.
pub(crate) struct BufBatchWriter<W: Write> {
    writer: StreamWriter<W>,
    /// Coalesces small batches into target_batch_size before serialization.
    coalescer: BatchCoalescer,
}

impl<W: Write> BufBatchWriter<W> {
    pub(crate) fn try_new(
        target: W,
        schema: SchemaRef,
        write_options: IpcWriteOptions,
        batch_size: usize,
    ) -> datafusion::common::Result<Self> {
        let writer = StreamWriter::try_new_with_options(target, &schema, write_options)?;
        let coalescer = BatchCoalescer::new(schema, batch_size);
        Ok(Self { writer, coalescer })
    }

    pub(crate) fn write(
        &mut self,
        batch: &RecordBatch,
        encode_time: &Time,
        _write_time: &Time,
    ) -> datafusion::common::Result<()> {
        self.coalescer.push_batch(batch.clone())?;

        let mut completed = Vec::new();
        while let Some(batch) = self.coalescer.next_completed_batch() {
            completed.push(batch);
        }

        for batch in &completed {
            let mut timer = encode_time.timer();
            self.writer.write(batch)?;
            timer.stop();
        }
        Ok(())
    }

    pub(crate) fn flush(
        &mut self,
        encode_time: &Time,
        _write_time: &Time,
    ) -> datafusion::common::Result<()> {
        // Finish any remaining buffered rows in the coalescer
        self.coalescer.finish_buffered_batch()?;
        while let Some(batch) = self.coalescer.next_completed_batch() {
            let mut timer = encode_time.timer();
            self.writer.write(&batch)?;
            timer.stop();
        }

        // Finish the IPC stream (writes the end-of-stream marker)
        self.writer.finish()?;
        Ok(())
    }
}
