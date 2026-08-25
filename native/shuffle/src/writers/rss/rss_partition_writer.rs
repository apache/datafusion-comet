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

use std::io::{self, Cursor, Seek, SeekFrom, Write};

use arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use datafusion::physical_plan::metrics::Time;
use datafusion_comet_jni_bridge::shuffle_partition_pusher::JavaShufflePartitionPusher;

use crate::metrics::ShufflePartitionerMetrics;
use crate::writers::PartitionWriter;
use crate::ShuffleBlockWriter;

/// Backend-neutral, all-or-error transport for one complete Comet frame.
///
/// Acceptance is not a remote map commit. Implementations own asynchronous admission, retry,
/// cancellation, and commit; they must not split a frame into independently interleavable pushes.
pub trait PartitionPusher: Send + Sync {
    fn push_partition_data(&self, partition_id: usize, frame: &[u8]) -> Result<()>;
}

impl PartitionPusher for JavaShufflePartitionPusher {
    fn push_partition_data(&self, partition_id: usize, frame: &[u8]) -> Result<()> {
        JavaShufflePartitionPusher::push_partition_data(self, partition_id, frame)
            .map_err(Into::into)
    }
}

/// Encodes already-partitioned batches using the existing Comet format and sends one frame per
/// callback. This foundation is not selected by the native planner yet.
///
/// There are no retained per-reducer buffers. The byte limit caps encoded output, not Arrow's
/// encoding scratch space or the backend's asynchronous memory. Production admission, row
/// splitting, integrity, and map-commit handling must be supplied before enabling remote plans.
pub struct RssPartitionWriter<P: PartitionPusher> {
    pusher: P,
    block_writer: ShuffleBlockWriter,
    num_partitions: usize,
    max_frame_bytes: usize,
    next_partition: usize,
    finished: bool,
    failed: bool,
}

impl<P: PartitionPusher> RssPartitionWriter<P> {
    pub fn try_new(
        pusher: P,
        block_writer: ShuffleBlockWriter,
        num_partitions: usize,
        max_frame_bytes: usize,
    ) -> Result<Self> {
        if num_partitions == 0 || num_partitions > i32::MAX as usize {
            return Err(DataFusionError::Configuration(
                "Invalid RSS partition count".into(),
            ));
        }
        if !(20..=i32::MAX as usize).contains(&max_frame_bytes) {
            return Err(DataFusionError::Configuration(
                "Invalid RSS frame byte limit".into(),
            ));
        }
        Ok(Self {
            pusher,
            block_writer,
            num_partitions,
            max_frame_bytes,
            next_partition: 0,
            finished: false,
            failed: false,
        })
    }

    /// Encodes and synchronously submits one batch. A failed writer cannot be reused.
    pub fn write_batch(
        &mut self,
        partition_id: usize,
        batch: &RecordBatch,
        encode_time: &Time,
        write_time: &Time,
    ) -> Result<()> {
        let result = (|| {
            self.check_partition(partition_id)?;
            let mut output = BoundedBuffer::new(self.max_frame_bytes);
            self.block_writer
                .write_batch(batch, &mut output, encode_time)?;
            let frame = output.inner.get_ref();
            if !frame.is_empty() {
                let _timer = write_time.timer();
                self.pusher.push_partition_data(partition_id, frame)?;
            }
            Ok(())
        })();
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    fn check_partition(&self, partition_id: usize) -> Result<()> {
        if self.failed || self.finished {
            return Err(DataFusionError::Execution("RSS writer is closed".into()));
        }
        if partition_id >= self.num_partitions || partition_id < self.next_partition {
            return Err(DataFusionError::Execution(format!(
                "RSS partition {partition_id} is invalid or already finalized"
            )));
        }
        Ok(())
    }

    fn write_batches<I>(
        &mut self,
        partition_id: usize,
        iter: &mut I,
        metrics: &ShufflePartitionerMetrics,
    ) -> Result<()>
    where
        I: Iterator<Item = Result<RecordBatch>>,
    {
        let result = (|| {
            self.check_partition(partition_id)?;
            for batch in iter {
                self.write_batch(
                    partition_id,
                    &batch?,
                    &metrics.encode_time,
                    &metrics.write_time,
                )?;
            }
            Ok(())
        })();
        if result.is_err() {
            // Earlier batches may already have been accepted. An input error must not allow
            // this attempt to be resumed or finalized as if its output were complete.
            self.failed = true;
        }
        result
    }
}

impl<P: PartitionPusher> PartitionWriter for RssPartitionWriter<P> {
    fn write<I>(
        &mut self,
        pid: usize,
        iter: &mut I,
        metrics: &ShufflePartitionerMetrics,
    ) -> Result<()>
    where
        I: Iterator<Item = Result<RecordBatch>>,
    {
        self.write_batches(pid, iter, metrics)
    }

    fn finish_partition<I>(
        &mut self,
        pid: usize,
        iter: &mut I,
        metrics: &ShufflePartitionerMetrics,
    ) -> Result<()>
    where
        I: Iterator<Item = Result<RecordBatch>>,
    {
        if pid != self.next_partition {
            return Err(DataFusionError::Execution(format!(
                "Expected RSS partition {}, got {pid}",
                self.next_partition
            )));
        }
        self.write_batches(pid, iter, metrics)?;
        self.next_partition += 1;
        Ok(())
    }

    fn finish_all(&mut self, _metrics: &ShufflePartitionerMetrics) -> Result<()> {
        if self.failed || self.finished || self.next_partition != self.num_partitions {
            return Err(DataFusionError::Execution(
                "RSS partitions are not ready for finalization".into(),
            ));
        }
        // Remote map commit belongs to the task-owned JVM adapter, not this encoder.
        self.finished = true;
        Ok(())
    }
}

/// A seekable encoder destination that rejects an oversized frame before growing its buffer.
struct BoundedBuffer {
    inner: Cursor<Vec<u8>>,
    limit: usize,
}

impl BoundedBuffer {
    fn new(limit: usize) -> Self {
        Self {
            inner: Cursor::new(Vec::new()),
            limit,
        }
    }

    fn limit_error() -> io::Error {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "RSS frame exceeds its byte limit",
        )
    }
}

impl Write for BoundedBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let end = self.inner.position().checked_add(bytes.len() as u64);
        if end.is_none_or(|end| end > self.limit as u64) {
            return Err(Self::limit_error());
        }
        self.inner.write(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl Seek for BoundedBuffer {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        let previous = self.inner.position();
        let next = self.inner.seek(position)?;
        if next > self.limit as u64 {
            self.inner.set_position(previous);
            return Err(Self::limit_error());
        }
        Ok(next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;

    use crate::{read_ipc_compressed, CompressionCodec};

    type CapturedFrames = Arc<Mutex<Vec<(usize, Vec<u8>)>>>;

    #[derive(Clone, Default)]
    struct RecordingPusher(CapturedFrames);

    impl PartitionPusher for RecordingPusher {
        fn push_partition_data(&self, pid: usize, frame: &[u8]) -> Result<()> {
            self.0.lock().unwrap().push((pid, frame.to_vec()));
            Ok(())
        }
    }

    fn batch(values: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(values))],
        )
        .unwrap()
    }

    fn metrics() -> ShufflePartitionerMetrics {
        ShufflePartitionerMetrics::new(&ExecutionPlanMetricsSet::new(), 0)
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Miri cannot call ZSTD_createCCtx.
    fn rss_partition_writer_routes_complete_frames() {
        let first = batch(vec![1, 2]);
        let second = batch(vec![3]);
        for codec in [
            CompressionCodec::None,
            CompressionCodec::Lz4Frame,
            CompressionCodec::Snappy,
            CompressionCodec::Zstd(1),
        ] {
            let pusher = RecordingPusher::default();
            let captured = Arc::clone(&pusher.0);
            let encoder = ShuffleBlockWriter::try_new(first.schema().as_ref(), codec).unwrap();
            let mut writer = RssPartitionWriter::try_new(pusher, encoder, 3, 1024 * 1024).unwrap();
            let metrics = metrics();
            writer
                .write(1, &mut [Ok(second.clone())].into_iter(), &metrics)
                .unwrap();
            writer
                .finish_partition(0, &mut [Ok(first.clone())].into_iter(), &metrics)
                .unwrap();
            writer
                .finish_partition(1, &mut std::iter::empty(), &metrics)
                .unwrap();
            writer
                .finish_partition(2, &mut std::iter::empty(), &metrics)
                .unwrap();
            writer.finish_all(&metrics).unwrap();

            let frames = captured.lock().unwrap();
            assert_eq!(frames.len(), 2);
            for ((pid, frame), (expected_pid, expected)) in
                frames.iter().zip([(1, &second), (0, &first)])
            {
                assert_eq!(*pid, expected_pid);
                let declared = u64::from_le_bytes(frame[..8].try_into().unwrap());
                assert_eq!(declared + 8, frame.len() as u64);
                assert_eq!(u64::from_le_bytes(frame[8..16].try_into().unwrap()), 1);
                assert_eq!(&read_ipc_compressed(&frame[16..]).unwrap(), expected);
            }
        }
    }

    #[test]
    fn rss_partition_writer_checks_lifecycle_and_empty_batches() {
        let input = batch(vec![]);
        let pusher = RecordingPusher::default();
        let captured = Arc::clone(&pusher.0);
        let encoder =
            ShuffleBlockWriter::try_new(input.schema().as_ref(), CompressionCodec::None).unwrap();
        for (partitions, limit) in [
            (0, 1024),
            (i32::MAX as usize + 1, 1024),
            (2, 19),
            (2, i32::MAX as usize + 1),
        ] {
            assert!(RssPartitionWriter::try_new(
                pusher.clone(),
                encoder.clone(),
                partitions,
                limit,
            )
            .is_err());
        }
        let mut writer = RssPartitionWriter::try_new(pusher, encoder, 2, 1024).unwrap();
        let metrics = metrics();
        assert!(writer.finish_all(&metrics).is_err());
        assert!(writer
            .finish_partition(1, &mut std::iter::empty(), &metrics)
            .is_err());
        writer
            .finish_partition(0, &mut [Ok(input)].into_iter(), &metrics)
            .unwrap();
        assert!(writer
            .finish_partition(0, &mut std::iter::empty(), &metrics)
            .is_err());
        writer
            .finish_partition(1, &mut std::iter::empty(), &metrics)
            .unwrap();
        writer.finish_all(&metrics).unwrap();
        assert!(writer.finish_all(&metrics).is_err());
        assert!(captured.lock().unwrap().is_empty());
    }

    #[test]
    fn rss_partition_writer_rejects_oversized_frames_before_push() {
        let input = batch(vec![1]);
        let pusher = RecordingPusher::default();
        let captured = Arc::clone(&pusher.0);
        let encoder =
            ShuffleBlockWriter::try_new(input.schema().as_ref(), CompressionCodec::None).unwrap();
        let mut writer = RssPartitionWriter::try_new(pusher, encoder, 1, 20).unwrap();
        let metrics = metrics();
        let error = writer
            .write(0, &mut [Ok(input)].into_iter(), &metrics)
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("RSS frame exceeds its byte limit"));
        assert!(captured.lock().unwrap().is_empty());
        assert!(writer
            .finish_partition(0, &mut std::iter::empty(), &metrics)
            .is_err());
    }

    #[test]
    fn rss_partition_writer_preserves_transport_errors() {
        struct FailingPusher;
        impl PartitionPusher for FailingPusher {
            fn push_partition_data(&self, _pid: usize, _frame: &[u8]) -> Result<()> {
                Err(DataFusionError::External(Box::new(io::Error::other(
                    "push failed",
                ))))
            }
        }
        let input = batch(vec![1]);
        let encoder =
            ShuffleBlockWriter::try_new(input.schema().as_ref(), CompressionCodec::None).unwrap();
        let mut writer = RssPartitionWriter::try_new(FailingPusher, encoder, 1, 1024).unwrap();
        let metrics = metrics();
        let error = writer
            .write(0, &mut [Ok(input)].into_iter(), &metrics)
            .unwrap_err();
        let DataFusionError::External(source) = error else {
            panic!("transport error was wrapped or stringified");
        };
        assert_eq!(
            source.downcast_ref::<io::Error>().unwrap().to_string(),
            "push failed"
        );
    }

    #[test]
    fn rss_partition_writer_stays_failed_after_input_error() {
        let input = batch(vec![1]);
        let pusher = RecordingPusher::default();
        let captured = Arc::clone(&pusher.0);
        let encoder =
            ShuffleBlockWriter::try_new(input.schema().as_ref(), CompressionCodec::None).unwrap();
        let mut writer = RssPartitionWriter::try_new(pusher, encoder, 1, 1024).unwrap();
        let metrics = metrics();
        let error = DataFusionError::External(Box::new(io::Error::other("input failed")));
        let error = writer
            .write(
                0,
                &mut [Ok(input.clone()), Err(error)].into_iter(),
                &metrics,
            )
            .unwrap_err();
        let DataFusionError::External(source) = error else {
            panic!("input error was wrapped or stringified");
        };
        assert_eq!(
            source.downcast_ref::<io::Error>().unwrap().to_string(),
            "input failed"
        );
        assert_eq!(captured.lock().unwrap().len(), 1);
        assert!(writer
            .write(0, &mut [Ok(input)].into_iter(), &metrics)
            .is_err());
        assert!(writer
            .finish_partition(0, &mut std::iter::empty(), &metrics)
            .is_err());
        assert!(writer.finish_all(&metrics).is_err());
        assert_eq!(captured.lock().unwrap().len(), 1);
    }

    #[test]
    fn rss_partition_writer_buffer_bounds_seek_and_write() {
        let mut buffer = BoundedBuffer::new(4);
        buffer.write_all(&[1, 2, 3, 4]).unwrap();
        assert!(buffer.write_all(&[5]).is_err());
        assert!(buffer.seek(SeekFrom::Start(5)).is_err());
        buffer.seek(SeekFrom::Start(0)).unwrap();
        buffer.write_all(&[9]).unwrap();
        assert_eq!(buffer.inner.into_inner(), vec![9, 2, 3, 4]);
    }
}
