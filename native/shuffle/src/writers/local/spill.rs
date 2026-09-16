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

use crate::codec_context::ShuffleCodecContext;
use crate::metrics::ShufflePartitionerMetrics;
use crate::writers::BufBatchWriter;
use crate::ShuffleBlockWriter;
use arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::SpillFile as DfSpillFile;
use datafusion::execution::SpillWriter as DfSpillWriter;
use std::sync::Arc;

struct ActiveSpillFile {
    temp_file: Arc<dyn DfSpillFile>,
    writer: Box<dyn DfSpillWriter>,
}

pub(crate) struct SpillWriter {
    shuffle_block_writer: ShuffleBlockWriter,
    write_buffer_size: usize,
    batch_size: usize,
    spill_file: Option<ActiveSpillFile>,
}

impl SpillWriter {
    pub(crate) fn try_new(
        shuffle_block_writer: ShuffleBlockWriter,
        write_buffer_size: usize,
        batch_size: usize,
    ) -> datafusion::common::Result<Self> {
        Ok(Self {
            shuffle_block_writer,
            write_buffer_size,
            batch_size,
            spill_file: None,
        })
    }

    /// Stages the batches from `iter` into this partition's spill file.
    ///
    /// `codec_context` comes from the task-level owner; a `SpillWriter` exists per partition.
    /// `recycled_buffer` is a scratch byte buffer shared by the sequential per-partition
    /// spill writes; it is left drained on return so one buffer's capacity serves every
    /// partition instead of each write regrowing its own.
    pub(crate) fn write<I: Iterator<Item = datafusion::common::Result<RecordBatch>>>(
        &mut self,
        iter: &mut I,
        codec_context: &mut ShuffleCodecContext,
        runtime: &RuntimeEnv,
        metrics: &ShufflePartitionerMetrics,
        recycled_buffer: &mut Vec<u8>,
    ) -> datafusion::common::Result<()> {
        if let Some(batch) = iter.next() {
            self.ensure_spill_file_created(runtime)?;

            let result = (|| {
                let mut buf_batch_writer = BufBatchWriter::new(
                    &mut self.shuffle_block_writer,
                    &mut self.spill_file.as_mut().unwrap().writer,
                    self.write_buffer_size,
                    self.batch_size,
                );
                buf_batch_writer.write(
                    &batch?,
                    recycled_buffer,
                    codec_context,
                    &metrics.encode_time,
                    &metrics.write_time,
                )?;
                for batch in iter.by_ref() {
                    let batch = batch?;
                    buf_batch_writer.write(
                        &batch,
                        recycled_buffer,
                        codec_context,
                        &metrics.encode_time,
                        &metrics.write_time,
                    )?;
                }
                buf_batch_writer.flush(
                    recycled_buffer,
                    codec_context,
                    &metrics.encode_time,
                    &metrics.write_time,
                )?;
                // `SpillWriter` is not `Seek`, so bytes are tracked by the writer itself rather
                // than measured via stream position.
                let bytes_written = buf_batch_writer.bytes_written();
                usize::try_from(bytes_written).map_err(|_| {
                    DataFusionError::Execution(format!(
                        "Spill file byte count exceeds platform capacity: {bytes_written}"
                    ))
                })
            })();
            // An errored spill must hand back a drained buffer, or its bytes leak into
            // the next partition's block.
            let total_bytes_written = result.inspect_err(|_| recycled_buffer.clear())?;
            metrics.spilled_bytes.add(total_bytes_written);
        }
        Ok(())
    }

    fn ensure_spill_file_created(
        &mut self,
        runtime: &RuntimeEnv,
    ) -> datafusion::common::Result<()> {
        if self.spill_file.is_none() {
            // Spill file is not yet created, create it
            let temp_file = runtime
                .disk_manager
                .create_tmp_file("shuffle writer spill")?;
            let writer = temp_file.open_writer()?;
            self.spill_file = Some(ActiveSpillFile { temp_file, writer });
        }
        Ok(())
    }

    /// Local filesystem path holding this partition's spilled bytes.
    ///
    /// * `Ok(None)` — nothing was spilled for this partition.
    /// * `Ok(Some(path))` — the spilled bytes live at `path`.
    /// * `Err(..)` — bytes were spilled but the backend exposes no local path.
    ///
    /// The last case must stay distinct from `Ok(None)`: a caller that treated it as
    /// "nothing to copy" would drop the spilled bytes while still recording the
    /// partition offsets, silently truncating the partition in the shuffle file.
    pub(crate) fn path(&self) -> datafusion::common::Result<Option<&std::path::Path>> {
        match self.spill_file.as_ref() {
            None => Ok(None),
            Some(spill_file) => match spill_file.temp_file.path() {
                Some(path) => Ok(Some(path)),
                None => Err(DataFusionError::Execution(
                    "Shuffle spill file has no local path; the shuffle writer requires a \
                     spill backend backed by local files."
                        .to_string(),
                )),
            },
        }
    }

    #[cfg(test)]
    pub(crate) fn has_spill_file(&self) -> bool {
        self.spill_file.is_some()
    }
}

#[cfg(test)]
pub(crate) mod pathless_backend {
    //! A spill backend that stores bytes somewhere other than the local filesystem, so
    //! `SpillFile::path()` returns `None`. `DiskManager`'s default backend always has a
    //! local path, but `TempFileFactory` is pluggable, so the shuffle writer has to cope
    //! with a backend that does not.

    use bytes::Bytes;
    use datafusion::common::DataFusionError;
    use datafusion::execution::disk_manager::DiskManagerBuilder;
    use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
    use datafusion::execution::{SpillFile, SpillWriter, TempFileFactory};
    use futures::Stream;
    use std::pin::Pin;
    use std::sync::Arc;

    /// A [`RuntimeEnv`] whose spill files report no local path.
    pub(crate) fn runtime() -> RuntimeEnv {
        RuntimeEnvBuilder::new()
            .with_disk_manager_builder(
                DiskManagerBuilder::default().with_temp_file_factory(Arc::new(PathlessFactory)),
            )
            .build()
            .unwrap()
    }

    struct PathlessFactory;

    impl TempFileFactory for PathlessFactory {
        fn create_temp_file(
            &self,
            _description: &str,
        ) -> datafusion::common::Result<Arc<dyn SpillFile>> {
            Ok(Arc::new(PathlessSpillFile))
        }
    }

    struct PathlessSpillFile;

    impl SpillFile for PathlessSpillFile {
        // `path()` is left at the trait default, which returns `None`.

        fn size(&self) -> Option<u64> {
            None
        }

        fn read_stream(
            &self,
        ) -> datafusion::common::Result<
            Pin<Box<dyn Stream<Item = datafusion::common::Result<Bytes>> + Send>>,
        > {
            Err(DataFusionError::NotImplemented(
                "PathlessSpillFile::read_stream".to_string(),
            ))
        }

        fn open_writer(&self) -> datafusion::common::Result<Box<dyn SpillWriter>> {
            Ok(Box::new(SinkWriter))
        }
    }

    /// Accepts and discards every byte, standing in for a backend that writes elsewhere.
    struct SinkWriter;

    impl std::io::Write for SinkWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl SpillWriter for SinkWriter {
        fn finish(&mut self) -> datafusion::common::Result<()> {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CompressionCodec;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
    use std::sync::Arc;

    fn test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from_iter_values(0..100))],
        )
        .unwrap()
    }

    fn spill_writer(batch: &RecordBatch, batch_size: usize) -> SpillWriter {
        let block_writer =
            ShuffleBlockWriter::try_new(batch.schema_ref().as_ref(), CompressionCodec::None)
                .unwrap();
        SpillWriter::try_new(block_writer, 1 << 20, batch_size).unwrap()
    }

    /// A spill whose batch iterator fails after a batch was already encoded must hand
    /// back a drained scratch; leftover bytes would land in the next partition's block.
    #[test]
    fn write_error_drains_recycled_buffer() {
        let batch = test_batch();
        // batch_size below the row count so the first write serializes into the scratch.
        let mut spill = spill_writer(&batch, 10);
        let runtime = RuntimeEnv::default();
        let metrics = ShufflePartitionerMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let mut codec_context = ShuffleCodecContext::default();
        let mut recycled = Vec::new();
        let mut iter = vec![
            Ok(batch),
            Err(DataFusionError::Execution("injected failure".to_string())),
        ]
        .into_iter();

        assert!(spill
            .write(
                &mut iter,
                &mut codec_context,
                &runtime,
                &metrics,
                &mut recycled
            )
            .is_err());
        assert!(
            recycled.is_empty(),
            "errored spill left {} bytes in the recycled buffer",
            recycled.len()
        );
    }

    /// A partition that never spilled has no path, and that is not an error.
    #[test]
    fn path_is_none_when_nothing_spilled() {
        let batch = test_batch();
        let spill = spill_writer(&batch, 10);
        assert!(!spill.has_spill_file());
        assert_eq!(spill.path().unwrap(), None);
    }

    /// Spilling to a backend with no local path must report an error rather than the
    /// `None` that means "nothing spilled" — see `path`'s doc comment.
    #[test]
    fn path_errors_when_backend_has_no_local_path() {
        let batch = test_batch();
        let mut spill = spill_writer(&batch, 10);
        let runtime = pathless_backend::runtime();
        let metrics = ShufflePartitionerMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let mut codec_context = ShuffleCodecContext::default();
        let mut recycled = Vec::new();
        let mut iter = vec![Ok(batch)].into_iter();

        spill
            .write(
                &mut iter,
                &mut codec_context,
                &runtime,
                &metrics,
                &mut recycled,
            )
            .unwrap();
        assert!(spill.has_spill_file());

        let err = spill
            .path()
            .expect_err("a spill file with no local path must not look like an empty partition");
        assert!(
            err.to_string().contains("no local path"),
            "unexpected error: {err}"
        );
    }
}
