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
use std::io::{BufWriter, Write};
use std::ops::Range;
use std::sync::Arc;

struct ActiveSpillFile {
    temp_file: Arc<dyn DfSpillFile>,
    /// Shared by every partition; bytes reach the file when it fills or on
    /// [`PartitionedSpill::flush`].
    writer: BufWriter<Box<dyn DfSpillWriter>>,
}

/// Forwards writes but ignores `flush`, so `BufBatchWriter`'s flush at the end of each partition
/// leaves the bytes buffered.
struct DeferFlush<'a, W: Write>(&'a mut W);

impl<W: Write> Write for DeferFlush<'_, W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.write(buf)
    }

    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.0.write_all(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// One spill file shared by every output partition of a task, with the ranges each partition's
/// blocks occupy.
pub(crate) struct PartitionedSpill {
    shuffle_block_writer: ShuffleBlockWriter,
    write_buffer_size: usize,
    batch_size: usize,
    spill_file: Option<ActiveSpillFile>,
    /// Bytes appended to the spill file so far.
    len: u64,
    /// Per partition, the spill file ranges holding its blocks, in write order.
    ranges: Vec<Vec<Range<u64>>>,
    /// Set when a write fails partway, after which `len` may not match the file.
    failed: bool,
}

impl PartitionedSpill {
    pub(crate) fn new(
        shuffle_block_writer: ShuffleBlockWriter,
        write_buffer_size: usize,
        batch_size: usize,
        num_partitions: usize,
    ) -> Self {
        Self {
            shuffle_block_writer,
            write_buffer_size,
            batch_size,
            spill_file: None,
            len: 0,
            ranges: vec![Vec::new(); num_partitions],
            failed: false,
        }
    }

    /// Appends partition `pid`'s batches to the spill file. `recycled_buffer` is left drained,
    /// including on error.
    pub(crate) fn write<I: Iterator<Item = datafusion::common::Result<RecordBatch>>>(
        &mut self,
        pid: usize,
        iter: &mut I,
        codec_context: &mut ShuffleCodecContext,
        runtime: &RuntimeEnv,
        metrics: &ShufflePartitionerMetrics,
        recycled_buffer: &mut Vec<u8>,
    ) -> datafusion::common::Result<()> {
        self.check_usable()?;
        let Some(batch) = iter.next() else {
            return Ok(());
        };
        self.ensure_spill_file_created(runtime)?;

        let result = (|| {
            let mut buf_batch_writer = BufBatchWriter::new(
                &mut self.shuffle_block_writer,
                DeferFlush(&mut self.spill_file.as_mut().unwrap().writer),
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
            Ok::<_, DataFusionError>(buf_batch_writer.bytes_written())
        })();

        let bytes_written = match result {
            Ok(bytes_written) => bytes_written,
            Err(error) => {
                // bytes may already be in the file, so later ranges could not be trusted
                self.failed = true;
                recycled_buffer.clear();
                return Err(error);
            }
        };

        if bytes_written > 0 {
            let start = self.len;
            self.len += bytes_written;
            self.ranges[pid].push(start..self.len);
        }
        metrics
            .spilled_bytes
            .add(usize::try_from(bytes_written).map_err(|_| {
                DataFusionError::Execution(format!(
                    "Spill file byte count exceeds platform capacity: {bytes_written}"
                ))
            })?);
        Ok(())
    }

    /// The spill file ranges holding partition `pid`'s blocks, in write order.
    pub(crate) fn ranges(&self, pid: usize) -> datafusion::common::Result<&[Range<u64>]> {
        self.check_usable()?;
        Ok(&self.ranges[pid])
    }

    /// Writes buffered spill bytes to the spill file.
    pub(crate) fn flush(&mut self) -> datafusion::common::Result<()> {
        self.check_usable()?;
        if let Some(spill_file) = self.spill_file.as_mut() {
            if let Err(error) = spill_file.writer.flush() {
                // the file holds an unknown prefix of the buffered bytes
                self.failed = true;
                return Err(error.into());
            }
        }
        Ok(())
    }

    /// Local filesystem path of the spill file.
    ///
    /// * `Ok(None)` — nothing was spilled.
    /// * `Ok(Some(path))` — the spilled bytes live at `path`.
    /// * `Err(..)` — bytes were spilled but the backend exposes no local path.
    ///
    /// The last case must stay distinct from `Ok(None)`, or spilled bytes would be dropped while
    /// partition offsets still counted them.
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

    fn check_usable(&self) -> datafusion::common::Result<()> {
        if self.failed {
            return Err(DataFusionError::Execution(
                "Shuffle spill file is unusable after a failed write".to_string(),
            ));
        }
        Ok(())
    }

    fn ensure_spill_file_created(
        &mut self,
        runtime: &RuntimeEnv,
    ) -> datafusion::common::Result<()> {
        if self.spill_file.is_none() {
            let temp_file = runtime
                .disk_manager
                .create_tmp_file("shuffle writer spill")?;
            let writer = BufWriter::with_capacity(self.write_buffer_size, temp_file.open_writer()?);
            self.spill_file = Some(ActiveSpillFile { temp_file, writer });
        }
        Ok(())
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

    fn partitioned_spill(batch: &RecordBatch, num_partitions: usize) -> PartitionedSpill {
        let block_writer =
            ShuffleBlockWriter::try_new(batch.schema_ref().as_ref(), CompressionCodec::None)
                .unwrap();
        // batch_size below the row count so a write serializes into the scratch
        PartitionedSpill::new(block_writer, 1 << 20, 10, num_partitions)
    }

    fn metrics() -> ShufflePartitionerMetrics {
        ShufflePartitionerMetrics::new(&ExecutionPlanMetricsSet::new(), 0)
    }

    fn failing_write(spill: &mut PartitionedSpill, recycled: &mut Vec<u8>) {
        let mut iter = vec![
            Ok(test_batch()),
            Err(DataFusionError::Execution("injected failure".to_string())),
        ]
        .into_iter();
        assert!(spill
            .write(
                0,
                &mut iter,
                &mut ShuffleCodecContext::default(),
                &RuntimeEnv::default(),
                &metrics(),
                recycled
            )
            .is_err());
    }

    #[test]
    fn write_error_drains_recycled_buffer() {
        let mut spill = partitioned_spill(&test_batch(), 2);
        let mut recycled = Vec::new();
        failing_write(&mut spill, &mut recycled);
        assert!(
            recycled.is_empty(),
            "errored spill left {} bytes in the recycled buffer",
            recycled.len()
        );
    }

    #[test]
    fn failed_write_makes_spill_unusable() {
        let mut spill = partitioned_spill(&test_batch(), 2);
        let mut recycled = Vec::new();
        failing_write(&mut spill, &mut recycled);

        let err = spill
            .write(
                1,
                &mut vec![Ok(test_batch())].into_iter(),
                &mut ShuffleCodecContext::default(),
                &RuntimeEnv::default(),
                &metrics(),
                &mut recycled,
            )
            .expect_err("write after a failed write");
        assert!(
            err.to_string().contains("unusable"),
            "unexpected error: {err}"
        );
        assert!(spill.ranges(1).is_err());
    }

    #[test]
    fn partitions_share_one_file_in_write_order() {
        let mut spill = partitioned_spill(&test_batch(), 2);
        let runtime = RuntimeEnv::default();
        let mut codec_context = ShuffleCodecContext::default();
        let mut recycled = Vec::new();
        for pid in [1, 0, 1] {
            spill
                .write(
                    pid,
                    &mut vec![Ok(test_batch())].into_iter(),
                    &mut codec_context,
                    &runtime,
                    &metrics(),
                    &mut recycled,
                )
                .unwrap();
        }

        let first = spill.ranges(1).unwrap().to_vec();
        let second = spill.ranges(0).unwrap().to_vec();
        assert_eq!(first.len(), 2);
        assert_eq!(second.len(), 1);
        assert_eq!(first[0].start, 0);
        assert_eq!(second[0].start, first[0].end);
        assert_eq!(first[1].start, second[0].end);
    }

    #[test]
    fn writes_stay_buffered_until_flush() {
        let mut spill = partitioned_spill(&test_batch(), 2);
        let runtime = RuntimeEnv::default();
        let mut codec_context = ShuffleCodecContext::default();
        let mut recycled = Vec::new();
        for pid in [0, 1] {
            spill
                .write(
                    pid,
                    &mut vec![Ok(test_batch())].into_iter(),
                    &mut codec_context,
                    &runtime,
                    &metrics(),
                    &mut recycled,
                )
                .unwrap();
        }
        let path = spill.path().unwrap().unwrap().to_path_buf();
        let spilled = spill.ranges(1).unwrap()[0].end;

        assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
        spill.flush().unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), spilled);
    }

    #[test]
    fn empty_write_records_no_range() {
        let mut spill = partitioned_spill(&test_batch(), 2);
        spill
            .write(
                0,
                &mut std::iter::empty(),
                &mut ShuffleCodecContext::default(),
                &RuntimeEnv::default(),
                &metrics(),
                &mut Vec::new(),
            )
            .unwrap();
        assert!(!spill.has_spill_file());
        assert!(spill.ranges(0).unwrap().is_empty());
    }

    #[test]
    fn path_is_none_when_nothing_spilled() {
        let spill = partitioned_spill(&test_batch(), 2);
        assert!(!spill.has_spill_file());
        assert_eq!(spill.path().unwrap(), None);
    }

    #[test]
    fn path_errors_when_backend_has_no_local_path() {
        let mut spill = partitioned_spill(&test_batch(), 2);
        spill
            .write(
                0,
                &mut vec![Ok(test_batch())].into_iter(),
                &mut ShuffleCodecContext::default(),
                &pathless_backend::runtime(),
                &metrics(),
                &mut Vec::new(),
            )
            .unwrap();
        assert!(spill.has_spill_file());

        let err = spill
            .path()
            .expect_err("a spill file with no local path must not look like nothing spilled");
        assert!(
            err.to_string().contains("no local path"),
            "unexpected error: {err}"
        );
    }
}
