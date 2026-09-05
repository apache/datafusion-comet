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

pub(crate) mod rss_partition_writer;

#[cfg(test)]
mod tests {
    use super::rss_partition_writer::RssPartitionWriter;
    use crate::metrics::ShufflePartitionerMetrics;
    use crate::writers::PartitionWriter;
    use crate::{read_ipc_compressed, CompressionCodec, ShuffleBlockWriter};
    use arrow::array::{
        Array, ArrayRef, DictionaryArray, Int32Array, ListArray, MapArray, StringArray, StructArray,
    };
    use arrow::buffer::OffsetBuffer;
    use arrow::compute::cast;
    use arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use arrow::ipc::writer::CompressionContext;
    use arrow::record_batch::RecordBatch;
    use datafusion::common::{DataFusionError, Result};
    use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, Time};
    use datafusion_comet_jni_bridge::errors::CometError;
    use datafusion_comet_jni_bridge::ShufflePartitionPusher;
    use std::collections::HashMap;
    use std::io::{self, Cursor};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Condvar, Mutex};
    use std::thread::{self, ThreadId};
    use std::time::Duration;

    /// Test-only allocation observation on a synchronous encoder thread. Production execution
    /// does not use thread-local state. Zstd's C allocations are covered separately by its public
    /// streaming-workspace estimate; this observes Rust buffers and their realloc overlap.
    mod allocations {
        use std::alloc::{GlobalAlloc, Layout, System};
        use std::cell::Cell;

        #[derive(Clone, Copy, Default)]
        struct Counters {
            live: isize,
            peak: usize,
            admission: Option<(isize, usize)>,
            excess: usize,
        }

        thread_local! {
            static COUNTERS: Cell<Option<Counters>> = const { Cell::new(None) };
        }

        struct ObservedAllocator;

        #[global_allocator]
        static ALLOCATOR: ObservedAllocator = ObservedAllocator;

        fn record(added: usize, removed: usize) {
            let _ = COUNTERS.try_with(|counter| {
                if let Some(mut value) = counter.get() {
                    value.live = value
                        .live
                        .saturating_add(added as isize)
                        .saturating_sub(removed as isize);
                    value.peak = value.peak.max(value.live.max(0) as usize);
                    if let Some((baseline, admitted)) = value.admission {
                        let used = value.live.saturating_sub(baseline).max(0) as usize;
                        value.excess = value.excess.max(used.saturating_sub(admitted));
                    }
                    counter.set(Some(value));
                }
            });
        }

        // SAFETY: every operation is forwarded to the system allocator with the unchanged
        // pointer/layout. The observation uses only allocation-free thread-local Cells.
        unsafe impl GlobalAlloc for ObservedAllocator {
            unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
                let pointer = unsafe { System.alloc(layout) };
                if !pointer.is_null() {
                    record(layout.size(), 0);
                }
                pointer
            }

            unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
                let pointer = unsafe { System.alloc_zeroed(layout) };
                if !pointer.is_null() {
                    record(layout.size(), 0);
                }
                pointer
            }

            unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
                unsafe { System.dealloc(pointer, layout) };
                record(0, layout.size());
            }

            unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
                let result = unsafe { System.realloc(pointer, layout, size) };
                if !result.is_null() {
                    // A moving realloc may hold old and new buffers at once. Count that peak
                    // even when the system allocator happened to resize in place in this run.
                    record(size, 0);
                    record(0, layout.size());
                }
                result
            }
        }

        pub(super) fn live() -> isize {
            COUNTERS.with(|counter| counter.get().unwrap().live)
        }

        pub(super) fn admit(bytes: usize) {
            COUNTERS.with(|counter| {
                if let Some(mut value) = counter.get() {
                    assert!(value.admission.is_none());
                    value.admission = Some((value.live, bytes));
                    counter.set(Some(value));
                }
            });
        }

        pub(super) fn retire() {
            COUNTERS.with(|counter| {
                if let Some(mut value) = counter.get() {
                    value.admission = None;
                    counter.set(Some(value));
                }
            });
        }

        pub(super) fn measure<T>(run: impl FnOnce() -> T) -> (T, usize) {
            struct Reset;
            impl Drop for Reset {
                fn drop(&mut self) {
                    COUNTERS.with(|counter| counter.set(None));
                }
            }
            COUNTERS.with(|counter| assert!(counter.replace(Some(Counters::default())).is_none()));
            let reset = Reset;
            let result = run();
            let observed = COUNTERS.with(|counter| counter.get().unwrap());
            drop(reset);
            assert_eq!(
                observed.excess, 0,
                "a planning or encoding phase exceeded its live reservation"
            );
            (result, observed.peak)
        }
    }

    type RecordedFrame = (i32, Vec<u8>);

    #[derive(Default)]
    struct RecordingPusher {
        frames: Mutex<Vec<RecordedFrame>>,
    }

    impl RecordingPusher {
        fn frames(&self) -> Vec<RecordedFrame> {
            self.frames.lock().unwrap().clone()
        }
    }

    impl ShufflePartitionPusher for RecordingPusher {
        fn push_partition_data(&self, partition_id: i32, data: &[u8]) -> Result<()> {
            self.frames
                .lock()
                .unwrap()
                .push((partition_id, data.to_vec()));
            Ok(())
        }
    }

    struct FailingPusher;

    impl ShufflePartitionPusher for FailingPusher {
        fn push_partition_data(&self, _partition_id: i32, _data: &[u8]) -> Result<()> {
            Err(DataFusionError::Execution(
                "remote shuffle callback rejected the frame".to_string(),
            ))
        }
    }

    #[derive(Default)]
    struct ReservationRecordingPusher {
        frames: Mutex<Vec<RecordedFrame>>,
        reservations: Mutex<Vec<usize>>,
        outstanding: Mutex<Option<usize>>,
        releases: AtomicUsize,
        capacity: Option<usize>,
        max_reservation: Option<usize>,
    }

    impl ShufflePartitionPusher for ReservationRecordingPusher {
        fn max_reservation_size(&self) -> usize {
            self.max_reservation.unwrap_or(usize::MAX)
        }

        fn reserve_partition_data(&self, bytes: usize) -> Result<()> {
            if self.capacity.is_some_and(|capacity| bytes > capacity) {
                return Err(DataFusionError::External(Box::new(io::Error::other(
                    "native encoding admission denied",
                ))));
            }
            let mut outstanding = self.outstanding.lock().unwrap();
            assert!(outstanding.replace(bytes).is_none());
            allocations::admit(bytes);
            self.reservations.lock().unwrap().push(bytes);
            Ok(())
        }

        fn release_partition_data_reservation(&self) -> Result<()> {
            if self.outstanding.lock().unwrap().take().is_some() {
                self.releases.fetch_add(1, Ordering::Relaxed);
                allocations::retire();
            }
            Ok(())
        }

        fn push_partition_data(&self, partition_id: i32, bytes: &[u8]) -> Result<()> {
            assert!(
                self.outstanding.lock().unwrap().is_some(),
                "encoding must be admitted before a frame reaches its transport"
            );
            self.frames
                .lock()
                .unwrap()
                .push((partition_id, bytes.to_vec()));
            Ok(())
        }
    }

    #[derive(Default)]
    struct ConcurrentAdmissionState {
        reservations: HashMap<ThreadId, usize>,
        rounds: HashMap<ThreadId, usize>,
        total: usize,
        admitted: usize,
        failed: bool,
    }

    struct ConcurrentAdmissionPusher {
        capacity: usize,
        state: Mutex<ConcurrentAdmissionState>,
        ready: Condvar,
        peak: AtomicUsize,
        frames: AtomicUsize,
    }

    impl ConcurrentAdmissionPusher {
        fn new(capacity: usize) -> Self {
            Self {
                capacity,
                state: Mutex::new(ConcurrentAdmissionState::default()),
                ready: Condvar::new(),
                peak: AtomicUsize::new(0),
                frames: AtomicUsize::new(0),
            }
        }
    }

    impl ShufflePartitionPusher for ConcurrentAdmissionPusher {
        fn reserve_partition_data(&self, bytes: usize) -> Result<()> {
            let current = thread::current().id();
            let mut state = self.state.lock().unwrap();
            if state.total.saturating_add(bytes) > self.capacity {
                state.failed = true;
                self.ready.notify_all();
                return Err(DataFusionError::Execution(
                    "concurrent native encoding exhausted executor admission".to_string(),
                ));
            }

            assert!(state.reservations.insert(current, bytes).is_none());
            state.total += bytes;
            self.peak.fetch_max(state.total, Ordering::Relaxed);
            let round = state.rounds.entry(current).or_default();
            *round += 1;
            if *round == 1 {
                // The first lease only protects descriptor-based size estimation. Synchronize
                // the following full encoding reservations, which must really coexist.
                return Ok(());
            }
            state.admitted += 1;
            self.ready.notify_all();

            while state.admitted < 2 && !state.failed {
                let (updated, timeout) = self
                    .ready
                    .wait_timeout(state, Duration::from_secs(10))
                    .unwrap();
                state = updated;
                if timeout.timed_out() && state.admitted < 2 {
                    state.failed = true;
                    self.ready.notify_all();
                }
            }
            if state.failed {
                state.total -= state.reservations.remove(&current).unwrap();
                return Err(DataFusionError::Execution(
                    "concurrent native encoders could not share executor admission".to_string(),
                ));
            }
            Ok(())
        }

        fn release_partition_data_reservation(&self) -> Result<()> {
            let mut state = self.state.lock().unwrap();
            if let Some(bytes) = state.reservations.remove(&thread::current().id()) {
                state.total -= bytes;
            }
            Ok(())
        }

        fn push_partition_data(&self, _partition_id: i32, data: &[u8]) -> Result<()> {
            let state = self.state.lock().unwrap();
            let bytes = state
                .reservations
                .get(&thread::current().id())
                .expect("shuffle frame must keep its encoding reservation");
            assert!(
                *bytes >= data.len() * 3,
                "native IPC, JNI, and transport copies must all be admitted"
            );
            self.frames.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    fn sample_batch(start: i32, rows: i32) -> RecordBatch {
        let values = Int32Array::from_iter_values(start..start + rows);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(values)]).unwrap()
    }

    fn metrics() -> ShufflePartitionerMetrics {
        ShufflePartitionerMetrics::new(&ExecutionPlanMetricsSet::new(), 0)
    }

    fn writer(
        batch: &RecordBatch,
        codec: CompressionCodec,
        pusher: Arc<dyn ShufflePartitionPusher>,
        partitions: usize,
        max_frame_size: usize,
    ) -> RssPartitionWriter {
        let block_writer = ShuffleBlockWriter::try_new_rss(batch.schema(), codec).unwrap();
        RssPartitionWriter::try_new(block_writer, pusher, partitions, max_frame_size).unwrap()
    }

    fn write_batches(
        writer: &mut RssPartitionWriter,
        partition_id: usize,
        batches: Vec<RecordBatch>,
        metrics: &ShufflePartitionerMetrics,
    ) -> Result<()> {
        let mut batches = batches.into_iter().map(Ok);
        writer.write(partition_id, &mut batches, metrics)
    }

    fn finish_partition(
        writer: &mut RssPartitionWriter,
        partition_id: usize,
        batches: Vec<RecordBatch>,
        metrics: &ShufflePartitionerMetrics,
    ) -> Result<()> {
        let mut batches = batches.into_iter().map(Ok);
        writer.finish_partition(partition_id, &mut batches, metrics)
    }

    fn decode_frame(frame: &[u8]) -> RecordBatch {
        assert!(frame.len() >= 20, "shuffle frame must include its header");
        let payload_length = u64::from_le_bytes(frame[..8].try_into().unwrap());
        assert_eq!(
            usize::try_from(payload_length).unwrap() + 8,
            frame.len(),
            "each callback must receive exactly one complete shuffle frame"
        );
        read_ipc_compressed(&frame[16..]).unwrap()
    }

    fn encoded_frame_size(batch: &RecordBatch) -> usize {
        let block_writer =
            ShuffleBlockWriter::try_new(batch.schema().as_ref(), CompressionCodec::None).unwrap();
        let mut frame = Cursor::new(Vec::new());
        let mut compression_context = CompressionContext::default();
        block_writer
            .write_batch(
                batch,
                &mut frame,
                &mut compression_context,
                &Time::default(),
            )
            .unwrap()
    }

    fn single_string_row(bytes: usize) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Utf8,
                false,
            )])),
            vec![Arc::new(StringArray::from(vec!["x".repeat(bytes)]))],
        )
        .unwrap()
    }

    fn size_limit_message(error: &DataFusionError) -> &str {
        let DataFusionError::External(cause) = error else {
            panic!("expected a typed shuffle size limit, got {error}");
        };
        let Some(CometError::ShuffleSizeLimit(message)) = cause.downcast_ref::<CometError>() else {
            panic!("expected a typed shuffle size limit, got {error}");
        };
        message
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn round_trips_all_supported_compression_codecs() {
        let batch = sample_batch(10, 128);

        for codec in [
            CompressionCodec::None,
            CompressionCodec::Lz4Frame,
            CompressionCodec::Snappy,
            CompressionCodec::Zstd(1),
        ] {
            let pusher = Arc::new(RecordingPusher::default());
            let mut writer = writer(&batch, codec, pusher.clone(), 1, 1024 * 1024);
            let metrics = metrics();

            write_batches(&mut writer, 0, vec![batch.clone()], &metrics).unwrap();
            finish_partition(&mut writer, 0, vec![], &metrics).unwrap();
            writer.finish_all(&metrics).unwrap();

            let frames = pusher.frames();
            assert_eq!(frames.len(), 1);
            assert_eq!(frames[0].0, 0);
            assert_eq!(decode_frame(&frames[0].1), batch);
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn preserves_dictionary_encoded_batches() {
        let values = ["alpha", "beta", "alpha", "gamma"];
        let dictionary: DictionaryArray<Int32Type> = values.iter().copied().collect();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "dictionary",
            dictionary.data_type().clone(),
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(dictionary)]).unwrap();
        let pusher = Arc::new(RecordingPusher::default());
        let mut writer = writer(
            &batch,
            CompressionCodec::Zstd(1),
            pusher.clone(),
            1,
            1024 * 1024,
        );
        let metrics = metrics();

        finish_partition(&mut writer, 0, vec![batch.clone()], &metrics).unwrap();
        writer.finish_all(&metrics).unwrap();

        let frames = pusher.frames();
        assert_eq!(frames.len(), 1);
        assert_eq!(decode_frame(&frames[0].1), batch);
    }

    #[test]
    fn sends_each_batch_as_one_complete_frame() {
        let first = sample_batch(0, 16);
        let second = sample_batch(16, 16);
        let third = sample_batch(32, 16);
        let pusher = Arc::new(RecordingPusher::default());
        let mut writer = writer(
            &first,
            CompressionCodec::None,
            pusher.clone(),
            1,
            1024 * 1024,
        );
        let metrics = metrics();

        write_batches(
            &mut writer,
            0,
            vec![first.clone(), second.clone()],
            &metrics,
        )
        .unwrap();
        finish_partition(&mut writer, 0, vec![third.clone()], &metrics).unwrap();
        writer.finish_all(&metrics).unwrap();

        let frames = pusher.frames();
        assert_eq!(frames.len(), 3);
        for ((partition_id, frame), expected) in frames.iter().zip([first, second, third].iter()) {
            assert_eq!(*partition_id, 0);
            assert_eq!(decode_frame(frame), *expected);
        }
    }

    #[test]
    fn routes_out_of_order_writes_to_the_correct_partition() {
        let first = sample_batch(0, 8);
        let second = sample_batch(8, 8);
        let third = sample_batch(16, 8);
        let pusher = Arc::new(RecordingPusher::default());
        let mut writer = writer(
            &first,
            CompressionCodec::None,
            pusher.clone(),
            3,
            1024 * 1024,
        );
        let metrics = metrics();

        write_batches(&mut writer, 2, vec![third.clone()], &metrics).unwrap();
        write_batches(&mut writer, 0, vec![first.clone()], &metrics).unwrap();
        write_batches(&mut writer, 1, vec![second.clone()], &metrics).unwrap();
        for partition_id in 0..3 {
            finish_partition(&mut writer, partition_id, vec![], &metrics).unwrap();
        }
        writer.finish_all(&metrics).unwrap();

        let frames = pusher.frames();
        assert_eq!(frames.len(), 3);
        for ((partition_id, frame), (expected_id, expected_batch)) in frames
            .iter()
            .zip([(2, third), (0, first), (1, second)].iter())
        {
            assert_eq!(partition_id, expected_id);
            assert_eq!(decode_frame(frame), *expected_batch);
        }
    }

    #[test]
    fn empty_batches_do_not_invoke_the_callback() {
        let batch = sample_batch(0, 4);
        let empty = RecordBatch::new_empty(batch.schema());
        let pusher = Arc::new(RecordingPusher::default());
        let mut writer = writer(
            &batch,
            CompressionCodec::None,
            pusher.clone(),
            2,
            1024 * 1024,
        );
        let metrics = metrics();

        write_batches(&mut writer, 0, vec![empty.clone()], &metrics).unwrap();
        finish_partition(&mut writer, 0, vec![empty.clone()], &metrics).unwrap();
        finish_partition(&mut writer, 1, vec![empty], &metrics).unwrap();
        writer.finish_all(&metrics).unwrap();

        assert!(pusher.frames().is_empty());
    }

    #[test]
    fn callback_failures_are_preserved() {
        let batch = sample_batch(0, 4);
        let mut writer = writer(
            &batch,
            CompressionCodec::None,
            Arc::new(FailingPusher),
            1,
            1024 * 1024,
        );

        let error = write_batches(&mut writer, 0, vec![batch], &metrics()).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("remote shuffle callback rejected the frame"),
            "callback error should not be replaced: {error}"
        );
    }

    #[test]
    fn iterator_failures_are_preserved() {
        let batch = sample_batch(0, 4);
        let pusher = Arc::new(RecordingPusher::default());
        let mut writer = writer(
            &batch,
            CompressionCodec::None,
            pusher.clone(),
            1,
            1024 * 1024,
        );
        let mut failed_batches = std::iter::once(Err(DataFusionError::Execution(
            "upstream shuffle input failed".to_string(),
        )));

        let error = writer
            .write(0, &mut failed_batches, &metrics())
            .unwrap_err();
        assert!(error.to_string().contains("upstream shuffle input failed"));
        assert!(pusher.frames().is_empty());
    }

    #[test]
    fn accepts_frames_that_exactly_match_the_maximum_size() {
        let batch = sample_batch(0, 16);
        let frame_size = encoded_frame_size(&batch);
        let pusher = Arc::new(RecordingPusher::default());
        let mut writer = writer(
            &batch,
            CompressionCodec::None,
            pusher.clone(),
            1,
            frame_size,
        );
        let metrics = metrics();

        finish_partition(&mut writer, 0, vec![batch.clone()], &metrics).unwrap();
        writer.finish_all(&metrics).unwrap();

        let frames = pusher.frames();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].1.len(), frame_size);
        assert_eq!(decode_frame(&frames[0].1), batch);
    }

    #[test]
    fn rejects_irreducible_single_row_frames_without_fragmenting_or_pushing() {
        let batch = sample_batch(0, 1);
        let frame_size = encoded_frame_size(&batch);
        let pusher = Arc::new(RecordingPusher::default());
        let mut writer = writer(
            &batch,
            CompressionCodec::None,
            pusher.clone(),
            1,
            frame_size - 1,
        );

        let error = write_batches(&mut writer, 0, vec![batch], &metrics()).unwrap_err();
        let message = size_limit_message(&error);
        assert!(message.contains(&format!(
            "effective frame limit from spark.comet.shuffle.rss.maxFrameBytes \
             and spark.comet.shuffle.rss.maxInFlightBytes is {} bytes",
            frame_size - 1
        )));
        assert!(message.contains("spark.comet.shuffle.rss.maxInFlightBytes"));
        assert!(message.contains(&format!("frame needs at least {frame_size} bytes")));
        assert!(
            pusher.frames().is_empty(),
            "a single oversized Arrow IPC row must never be fragmented"
        );
    }

    #[test]
    fn constructor_rejects_invalid_limits() {
        let batch = sample_batch(0, 1);
        let pusher: Arc<dyn ShufflePartitionPusher> = Arc::new(RecordingPusher::default());
        let block_writer =
            ShuffleBlockWriter::try_new(batch.schema().as_ref(), CompressionCodec::None).unwrap();

        assert!(
            RssPartitionWriter::try_new(block_writer.clone(), pusher.clone(), 0, 1024).is_err()
        );
        assert!(RssPartitionWriter::try_new(block_writer.clone(), pusher.clone(), 1, 0).is_err());

        let excessive_partitions = usize::try_from(i32::MAX).unwrap() + 2;
        assert!(
            RssPartitionWriter::try_new(block_writer, pusher, excessive_partitions, 1024).is_err()
        );
    }

    #[test]
    fn rejects_partition_ids_outside_the_configured_range() {
        let batch = sample_batch(0, 4);
        let pusher = Arc::new(RecordingPusher::default());
        let mut writer = writer(
            &batch,
            CompressionCodec::None,
            pusher.clone(),
            2,
            1024 * 1024,
        );
        let metrics = metrics();

        assert!(write_batches(&mut writer, 2, vec![batch.clone()], &metrics).is_err());
        assert!(finish_partition(&mut writer, 2, vec![batch], &metrics).is_err());
        assert!(pusher.frames().is_empty());
    }

    #[test]
    fn partition_finalization_must_be_in_ascending_order() {
        let batch = sample_batch(0, 4);
        let pusher = Arc::new(RecordingPusher::default());
        let mut writer = writer(&batch, CompressionCodec::None, pusher, 2, 1024 * 1024);
        let metrics = metrics();

        assert!(finish_partition(&mut writer, 1, vec![], &metrics).is_err());
        finish_partition(&mut writer, 0, vec![], &metrics).unwrap();
        assert!(finish_partition(&mut writer, 0, vec![], &metrics).is_err());
        finish_partition(&mut writer, 1, vec![], &metrics).unwrap();
        writer.finish_all(&metrics).unwrap();
    }

    #[test]
    fn rejects_writes_to_a_finalized_partition() {
        let batch = sample_batch(0, 4);
        let pusher = Arc::new(RecordingPusher::default());
        let mut writer = writer(
            &batch,
            CompressionCodec::None,
            pusher.clone(),
            2,
            1024 * 1024,
        );
        let metrics = metrics();

        finish_partition(&mut writer, 0, vec![], &metrics).unwrap();
        assert!(write_batches(&mut writer, 0, vec![batch], &metrics).is_err());
        assert!(pusher.frames().is_empty());
    }

    #[test]
    fn finish_all_requires_every_partition_to_be_finalized() {
        let batch = sample_batch(0, 4);
        let pusher = Arc::new(RecordingPusher::default());
        let mut writer = writer(&batch, CompressionCodec::None, pusher, 2, 1024 * 1024);
        let metrics = metrics();

        assert!(writer.finish_all(&metrics).is_err());
        finish_partition(&mut writer, 0, vec![], &metrics).unwrap();
        assert!(writer.finish_all(&metrics).is_err());
        finish_partition(&mut writer, 1, vec![], &metrics).unwrap();
        writer.finish_all(&metrics).unwrap();
    }

    #[test]
    fn rejects_operations_after_finish_all() {
        let batch = sample_batch(0, 4);
        let pusher = Arc::new(RecordingPusher::default());
        let mut writer = writer(
            &batch,
            CompressionCodec::None,
            pusher.clone(),
            1,
            1024 * 1024,
        );
        let metrics = metrics();

        finish_partition(&mut writer, 0, vec![], &metrics).unwrap();
        writer.finish_all(&metrics).unwrap();

        assert!(write_batches(&mut writer, 0, vec![batch], &metrics).is_err());
        assert!(finish_partition(&mut writer, 0, vec![], &metrics).is_err());
        assert!(writer.finish_all(&metrics).is_err());
        assert!(pusher.frames().is_empty());
    }

    #[test]
    fn splits_oversized_batches_into_complete_row_aligned_frames() {
        let batch = sample_batch(0, 512);
        let limit = encoded_frame_size(&sample_batch(0, 32));
        let pusher = Arc::new(ReservationRecordingPusher::default());
        let mut writer = writer(&batch, CompressionCodec::None, pusher.clone(), 1, limit);
        let metrics = metrics();

        finish_partition(&mut writer, 0, vec![batch], &metrics).unwrap();
        writer.finish_all(&metrics).unwrap();

        let frames = pusher.frames.lock().unwrap();
        assert!(frames.len() > 1);
        let mut values = Vec::new();
        for (partition, frame) in frames.iter() {
            assert_eq!(*partition, 0);
            assert!(frame.len() <= limit);
            let decoded = decode_frame(frame);
            values.extend(
                decoded
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied(),
            );
        }
        assert_eq!(values, (0..512).collect::<Vec<_>>());
        assert!(pusher
            .reservations
            .lock()
            .unwrap()
            .iter()
            .all(|reservation| *reservation >= 3 * limit));
        assert!(pusher.outstanding.lock().unwrap().is_none());
    }

    #[test]
    fn small_batches_encode_concurrently_with_default_frame_and_admission_limits() {
        let frame_limit = 64 * 1024 * 1024;
        let admission_limit = 512 * 1024 * 1024;
        let pusher = Arc::new(ConcurrentAdmissionPusher::new(admission_limit));

        thread::scope(|scope| {
            let tasks = (0..2)
                .map(|partition| {
                    let pusher = Arc::clone(&pusher);
                    scope.spawn(move || {
                        let batch = sample_batch(partition * 8, 8);
                        let mut writer =
                            writer(&batch, CompressionCodec::None, pusher, 1, frame_limit);
                        write_batches(&mut writer, 0, vec![batch], &metrics())
                    })
                })
                .collect::<Vec<_>>();

            for task in tasks {
                task.join().unwrap().unwrap();
            }
        });

        assert_eq!(pusher.frames.load(Ordering::Relaxed), 2);
        assert!(
            pusher.peak.load(Ordering::Relaxed) < admission_limit / 2,
            "tiny frames must not each reserve three copies of the maximum frame"
        );
        assert_eq!(pusher.state.lock().unwrap().total, 0);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn large_single_rows_fit_default_frame_and_admission_limits() {
        let frame_limit = 64 * 1024 * 1024;
        // Celeborn reserves its 16-byte request header separately from the native reservation.
        let reservation_limit = 512 * 1024 * 1024 - 16;
        for row_size_mib in [38, 50, 63] {
            let batch = single_string_row(row_size_mib * 1024 * 1024);
            let pusher = Arc::new(ReservationRecordingPusher {
                max_reservation: Some(reservation_limit),
                capacity: Some(reservation_limit),
                ..ReservationRecordingPusher::default()
            });
            let mut writer = writer(
                &batch,
                CompressionCodec::None,
                pusher.clone(),
                1,
                frame_limit,
            );
            let metrics = metrics();

            finish_partition(&mut writer, 0, vec![batch.clone()], &metrics).unwrap();
            writer.finish_all(&metrics).unwrap();

            let frames = pusher.frames.lock().unwrap();
            assert_eq!(frames.len(), 1);
            assert!(frames[0].1.len() <= frame_limit);
            assert_eq!(decode_frame(&frames[0].1), batch);
            assert!(pusher.outstanding.lock().unwrap().is_none());
            assert!(pusher
                .reservations
                .lock()
                .unwrap()
                .iter()
                .all(|bytes| *bytes <= reservation_limit));
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn reports_admission_limit_when_a_single_row_fits_the_configured_frame_limit() {
        let batch = single_string_row(38 * 1024 * 1024);
        let frame_limit = 64 * 1024 * 1024;
        let reservation_limit = 256 * 1024 * 1024 - 16;
        let pusher = Arc::new(ReservationRecordingPusher {
            max_reservation: Some(reservation_limit),
            capacity: Some(reservation_limit),
            ..ReservationRecordingPusher::default()
        });
        let mut writer = writer(
            &batch,
            CompressionCodec::None,
            pusher.clone(),
            1,
            frame_limit,
        );
        assert!(encoded_frame_size(&batch) < frame_limit);

        let error = write_batches(&mut writer, 0, vec![batch], &metrics()).unwrap_err();

        let message = size_limit_message(&error);
        assert!(
            message.contains("effective frame limit from spark.comet.shuffle.rss.maxFrameBytes")
        );
        assert!(message.contains(&format!(": {frame_limit} bytes)")));
        assert!(message.contains("spark.comet.shuffle.rss.maxInFlightBytes"));
        assert!(message.contains(&format!("{reservation_limit}-byte reservation")));
        assert!(message.contains("encoded bytes fit alongside"));
        assert!(message.contains("required reservation is at least"));
        assert!(pusher.frames.lock().unwrap().is_empty());
        assert!(pusher.outstanding.lock().unwrap().is_none());
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn reports_workspace_limit_before_encoding_a_single_row() {
        let batch = single_string_row(20 * 1024 * 1024);
        let frame_limit = 64 * 1024 * 1024;
        let reservation_limit = 64 * 1024 * 1024 - 16;
        let pusher = Arc::new(ReservationRecordingPusher {
            max_reservation: Some(reservation_limit),
            capacity: Some(reservation_limit),
            ..ReservationRecordingPusher::default()
        });
        let mut writer = writer(
            &batch,
            CompressionCodec::None,
            pusher.clone(),
            1,
            frame_limit,
        );

        let error = write_batches(&mut writer, 0, vec![batch], &metrics()).unwrap_err();

        let message = size_limit_message(&error);
        assert!(message.contains("encoding workspace for a single row requires"));
        assert!(message.contains("bytes of reservation are needed"));
        assert!(message.contains(&format!("only {reservation_limit} bytes per frame")));
        assert!(message.contains("spark.comet.shuffle.rss.maxInFlightBytes"));
        assert!(
            message.contains("effective frame limit from spark.comet.shuffle.rss.maxFrameBytes")
        );
        assert!(message.contains(&format!("is {frame_limit} bytes")));
        assert_eq!(pusher.reservations.lock().unwrap().len(), 1);
        assert!(pusher.frames.lock().unwrap().is_empty());
        assert!(pusher.outstanding.lock().unwrap().is_none());
    }

    #[test]
    fn codec_workspace_is_rejected_before_encoding_under_a_tiny_budget() {
        let batch = sample_batch(0, 1);
        for codec in [
            CompressionCodec::Lz4Frame,
            CompressionCodec::Snappy,
            CompressionCodec::Zstd(1),
        ] {
            let pusher = Arc::new(ReservationRecordingPusher {
                max_reservation: Some(16_384),
                capacity: Some(16_384),
                ..ReservationRecordingPusher::default()
            });
            let mut writer = writer(&batch, codec, pusher.clone(), 1, 5_456);
            let error = write_batches(&mut writer, 0, vec![batch.clone()], &metrics()).unwrap_err();
            let message = size_limit_message(&error);
            assert!(message.contains("encoding workspace"));
            assert!(message.contains("spark.comet.shuffle.rss.maxInFlightBytes"));
            assert!(message
                .contains("effective frame limit from spark.comet.shuffle.rss.maxFrameBytes"));
            assert!(message.contains("is 5456 bytes"));
            assert!(pusher.reservations.lock().unwrap().is_empty());
            assert!(pusher.frames.lock().unwrap().is_empty());
        }
    }

    #[test]
    fn rss_construction_retains_schema_without_allocating_schema_sized_buffers() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "large-field-name".repeat(65_536),
            DataType::Int32,
            false,
        )]));
        let (writer, peak) = allocations::measure(|| {
            ShuffleBlockWriter::try_new_rss(Arc::clone(&schema), CompressionCodec::Lz4Frame)
        });
        writer.unwrap();
        assert!(
            peak < 1_024,
            "RSS constructor pre-encoded or cloned its schema: {peak} bytes"
        );
    }

    #[test]
    fn splits_when_full_encoding_workspace_cannot_fit_one_reservation() {
        let batch = sample_batch(0, 16_384);
        let pusher = Arc::new(ReservationRecordingPusher {
            max_reservation: Some(262_144),
            capacity: Some(262_144),
            ..ReservationRecordingPusher::default()
        });
        let mut writer = writer(&batch, CompressionCodec::None, pusher.clone(), 1, 67_996);
        write_batches(&mut writer, 0, vec![batch.clone()], &metrics()).unwrap();
        let frames = pusher.frames.lock().unwrap();
        assert!(frames.len() > 1);
        let decoded = frames
            .iter()
            .map(|(_, frame)| decode_frame(frame))
            .collect::<Vec<_>>();
        assert_eq!(
            arrow::compute::concat_batches(&batch.schema(), &decoded).unwrap(),
            batch
        );
        assert!(pusher
            .reservations
            .lock()
            .unwrap()
            .iter()
            .all(|bytes| *bytes <= 262_144));
        assert!(pusher.outstanding.lock().unwrap().is_none());
    }

    #[test]
    fn schema_scratch_is_charged_before_a_compressible_dictionary_schema_is_encoded() {
        let dictionary: DictionaryArray<Int32Type> = ["value"].into_iter().collect();
        let field = Field::new(
            "long-field".repeat(16_384),
            dictionary.data_type().clone(),
            false,
        )
        .with_metadata(HashMap::from([(
            "metadata".to_string(),
            "x".repeat(65_536),
        )]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![field])),
            vec![Arc::new(dictionary)],
        )
        .unwrap();
        let pusher = Arc::new(ReservationRecordingPusher {
            max_reservation: Some(512 * 1024),
            capacity: Some(512 * 1024),
            ..ReservationRecordingPusher::default()
        });
        let mut writer = writer(
            &batch,
            CompressionCodec::Lz4Frame,
            pusher.clone(),
            1,
            64 * 1024,
        );
        let error = write_batches(&mut writer, 0, vec![batch], &metrics()).unwrap_err();
        assert!(error.to_string().contains("encoding workspace"));
        assert!(pusher.reservations.lock().unwrap().is_empty());
        assert!(pusher.frames.lock().unwrap().is_empty());
    }

    #[test]
    fn measured_encoding_allocations_fit_admitted_workspace_for_all_codecs() {
        let dictionary: DictionaryArray<Int32Type> = ["value"].into_iter().collect();
        let dictionary_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "wide-name".repeat(8_192),
                dictionary.data_type().clone(),
                false,
            )
            .with_metadata(HashMap::from([(
                "key".to_string(),
                "metadata".repeat(4_096),
            )]))])),
            vec![Arc::new(dictionary)],
        )
        .unwrap();
        let mut nested: ArrayRef = Arc::new(Int32Array::from_iter_values(0..128));
        for _ in 0..16 {
            let field = Arc::new(Field::new("item", nested.data_type().clone(), false));
            let offsets = OffsetBuffer::new(vec![0, nested.len() as i32].into());
            nested = Arc::new(ListArray::try_new(field, offsets, nested, None).unwrap());
        }
        let nested_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "nested",
                nested.data_type().clone(),
                false,
            )])),
            vec![nested],
        )
        .unwrap();
        let wide_batch = RecordBatch::try_new(
            Arc::new(Schema::new(
                (0..256)
                    .map(|index| Field::new(format!("field{index}"), DataType::Int32, false))
                    .collect::<Vec<_>>(),
            )),
            (0..256)
                .map(|_| Arc::new(Int32Array::from(vec![1])) as ArrayRef)
                .collect(),
        )
        .unwrap();
        for batch in [
            sample_batch(0, 1),
            sample_batch(0, 16_384),
            dictionary_batch,
            nested_batch,
            wide_batch,
        ] {
            for codec in [
                CompressionCodec::None,
                CompressionCodec::Lz4Frame,
                CompressionCodec::Snappy,
                CompressionCodec::Zstd(1),
            ] {
                let pusher = Arc::new(ReservationRecordingPusher {
                    max_reservation: Some(16 * 1024 * 1024),
                    capacity: Some(16 * 1024 * 1024),
                    ..ReservationRecordingPusher::default()
                });
                let mut writer = writer(&batch, codec.clone(), pusher.clone(), 1, 1024 * 1024);
                let metrics = metrics();
                let (result, peak) = allocations::measure(|| {
                    write_batches(&mut writer, 0, vec![batch.clone()], &metrics)
                });
                result.unwrap();
                let reserved = *pusher.reservations.lock().unwrap().iter().max().unwrap();
                assert!(
                    peak <= reserved,
                    "{codec:?}: peak allocation {peak} exceeds reservation {reserved}"
                );
                assert_eq!(decode_frame(&pusher.frames.lock().unwrap()[0].1), batch);
            }
        }
    }

    #[test]
    fn native_capacity_is_freed_before_the_successful_callback_is_acknowledged() {
        use std::sync::atomic::{AtomicBool, AtomicIsize};

        #[derive(Default)]
        struct Pusher {
            at_push: AtomicIsize,
            frame_length: AtomicUsize,
            acknowledged: AtomicBool,
        }
        impl ShufflePartitionPusher for Pusher {
            fn push_partition_data(&self, _partition: i32, bytes: &[u8]) -> Result<()> {
                self.at_push.store(allocations::live(), Ordering::Relaxed);
                self.frame_length.store(bytes.len(), Ordering::Relaxed);
                Ok(())
            }
            fn release_partition_data_reservation(&self) -> Result<()> {
                if self.frame_length.load(Ordering::Relaxed) == 0 {
                    // Planning descriptors are released before the actual frame is submitted.
                    return Ok(());
                }
                assert!(
                    allocations::live()
                        <= self.at_push.load(Ordering::Relaxed)
                            - self.frame_length.load(Ordering::Relaxed) as isize,
                    "the output allocation must die before native ownership is acknowledged"
                );
                self.acknowledged.store(true, Ordering::Relaxed);
                Ok(())
            }
        }
        let batch = sample_batch(0, 2_048);
        let pusher = Arc::new(Pusher::default());
        let mut writer = writer(&batch, CompressionCodec::None, pusher.clone(), 1, 64 * 1024);
        let metrics = metrics();
        allocations::measure(|| write_batches(&mut writer, 0, vec![batch], &metrics))
            .0
            .unwrap();
        assert!(pusher.acknowledged.load(Ordering::Relaxed));
    }

    #[test]
    fn oversized_schema_metadata_retries_with_a_fresh_larger_atomic_reservation() {
        // Keep the schema difficult to compress so every supported codec must retry its
        // underestimated metadata rather than fitting it into the initial 4 KiB allowance.
        let mut random = 0x1234_5678_9abc_def0u64;
        let field_name = (0..16_384)
            .map(|_| {
                random = random
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1);
                char::from(b'a' + ((random >> 32) % 26) as u8)
            })
            .collect::<String>();
        let schema = Arc::new(Schema::new(vec![Field::new(
            field_name.clone(),
            DataType::Int32,
            false,
        )]));
        let plain_batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![7]))]).unwrap();
        let dictionary: DictionaryArray<Int32Type> = ["dictionary-value"].into_iter().collect();
        let dictionary_schema = Arc::new(Schema::new(vec![Field::new(
            field_name,
            dictionary.data_type().clone(),
            false,
        )]));
        let dictionary_batch =
            RecordBatch::try_new(dictionary_schema, vec![Arc::new(dictionary)]).unwrap();

        for batch in [plain_batch, dictionary_batch] {
            for codec in [
                CompressionCodec::None,
                CompressionCodec::Lz4Frame,
                CompressionCodec::Snappy,
                CompressionCodec::Zstd(1),
            ] {
                let pusher = Arc::new(ReservationRecordingPusher::default());
                let mut writer = writer(&batch, codec, pusher.clone(), 1, 64 * 1024);

                write_batches(&mut writer, 0, vec![batch.clone()], &metrics()).unwrap();

                let frames = pusher.frames.lock().unwrap();
                assert_eq!(frames.len(), 1);
                assert_eq!(decode_frame(&frames[0].1), batch);
                let reservations = pusher.reservations.lock().unwrap();
                assert!(reservations.len() > 1);
                assert!(reservations.windows(2).all(|pair| pair[0] < pair[1]));
                assert!(reservations.last().unwrap() >= &(frames[0].1.len() * 3));
                assert_eq!(
                    pusher.releases.load(Ordering::Relaxed),
                    reservations.len(),
                    "every retry and successful frame must acknowledge native ownership release"
                );
                assert!(pusher.outstanding.lock().unwrap().is_none());
            }
        }
    }

    #[test]
    fn denied_pre_encoding_reservation_preserves_original_error_without_pushing() {
        let batch = sample_batch(0, 8);
        let limit = 1_024;
        let pusher = Arc::new(ReservationRecordingPusher {
            capacity: Some(3 * limit - 1),
            ..ReservationRecordingPusher::default()
        });
        let mut writer = writer(&batch, CompressionCodec::None, pusher.clone(), 1, limit);

        let error = write_batches(&mut writer, 0, vec![batch], &metrics()).unwrap_err();
        let DataFusionError::External(cause) = error else {
            panic!("encoding admission failure was wrapped or stringified");
        };
        assert_eq!(cause.to_string(), "native encoding admission denied");
        assert!(pusher.frames.lock().unwrap().is_empty());
        assert_eq!(pusher.releases.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn oversized_single_rows_release_encoding_admission_without_fragmentation() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["x".repeat(16_384)]))],
        )
        .unwrap();
        let pusher = Arc::new(ReservationRecordingPusher::default());
        let mut writer = writer(&batch, CompressionCodec::None, pusher.clone(), 1, 512);

        let error = write_batches(&mut writer, 0, vec![batch], &metrics()).unwrap_err();
        assert!(error.to_string().contains("single row"));
        assert!(pusher.frames.lock().unwrap().is_empty());
        assert_eq!(pusher.releases.load(Ordering::Relaxed), 2);
        assert!(pusher.outstanding.lock().unwrap().is_none());
    }

    #[test]
    fn compacts_dictionaries_nested_inside_sliced_lists_and_structs() {
        let values = (0..128)
            .map(|index| format!("value-{index:03}-{}", "x".repeat(128)))
            .collect::<Vec<_>>();
        let dictionary_type =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let dictionary = DictionaryArray::<Int32Type>::try_new(
            Int32Array::from((0..32).collect::<Vec<_>>()),
            Arc::new(StringArray::from(values.clone())),
        )
        .unwrap();
        let item = Arc::new(Field::new("item", dictionary_type, false));
        let list = ListArray::try_new(
            Arc::clone(&item),
            OffsetBuffer::new((0..=32).collect::<Vec<i32>>().into()),
            Arc::new(dictionary),
            None,
        )
        .unwrap();
        let nested = Arc::new(Field::new("items", DataType::List(item), false));
        let structure =
            StructArray::try_new(vec![Arc::clone(&nested)].into(), vec![Arc::new(list)], None)
                .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "nested",
            DataType::Struct(vec![nested].into()),
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(structure)])
            .unwrap()
            .slice(7, 16);
        let pusher = Arc::new(ReservationRecordingPusher::default());
        let mut writer = writer(&batch, CompressionCodec::None, pusher.clone(), 1, 2_048);

        finish_partition(&mut writer, 0, vec![batch], &metrics()).unwrap();

        let frames = pusher.frames.lock().unwrap();
        assert!(frames.len() > 1);
        let mut actual = Vec::new();
        for (_, frame) in frames.iter() {
            assert!(frame.len() <= 2_048);
            let decoded = decode_frame(frame);
            let structure = decoded
                .column(0)
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();
            let lists = structure
                .column(0)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            assert_eq!(lists.value_offsets()[0], 0);
            let dictionary = lists
                .values()
                .as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .unwrap();
            assert!(dictionary.values().len() < values.len());
            let plain = cast(dictionary, &DataType::Utf8).unwrap();
            actual.extend(
                plain
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .map(|value| value.unwrap().to_owned()),
            );
        }
        assert_eq!(actual, values[7..23].to_vec());
    }

    #[test]
    fn sliced_lists_and_maps_charge_only_their_live_nested_entries() {
        let count = 16_384;
        let values: ArrayRef = Arc::new(Int32Array::from_iter_values(0..count));
        let offsets = OffsetBuffer::new((0..=count).collect::<Vec<i32>>().into());
        let item = Arc::new(Field::new("item", DataType::Int32, false));
        let list = ListArray::try_new(
            Arc::clone(&item),
            offsets.clone(),
            Arc::clone(&values),
            None,
        )
        .unwrap()
        .slice(count as usize / 2, 1);
        let list_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "items",
                DataType::List(item),
                false,
            )])),
            vec![Arc::new(list)],
        )
        .unwrap();

        let fields = vec![
            Arc::new(Field::new("key", DataType::Int32, false)),
            Arc::new(Field::new("value", DataType::Int32, false)),
        ];
        let entries = StructArray::try_new(
            fields.clone().into(),
            vec![Arc::clone(&values), Arc::clone(&values)],
            None,
        )
        .unwrap();
        let entry = Arc::new(Field::new(
            "entries",
            DataType::Struct(fields.into()),
            false,
        ));
        let map = MapArray::try_new(Arc::clone(&entry), offsets, entries, None, false)
            .unwrap()
            .slice(count as usize / 2, 1);
        let map_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "entries",
                DataType::Map(entry, false),
                false,
            )])),
            vec![Arc::new(map)],
        )
        .unwrap();

        let limit = 2_048;
        for batch in [list_batch, map_batch] {
            let pusher = Arc::new(ReservationRecordingPusher {
                capacity: Some(64 * 1024),
                max_reservation: Some(64 * 1024),
                ..ReservationRecordingPusher::default()
            });
            let mut writer = writer(&batch, CompressionCodec::None, pusher.clone(), 1, limit);
            finish_partition(&mut writer, 0, vec![batch.clone()], &metrics()).unwrap();
            let frames = pusher.frames.lock().unwrap();
            assert_eq!(frames.len(), 1);
            assert_eq!(decode_frame(&frames[0].1), batch);
        }
    }

    #[test]
    fn input_failure_permanently_poisons_a_partially_pushed_map() {
        let batch = sample_batch(0, 4);
        let pusher = Arc::new(RecordingPusher::default());
        let mut writer = writer(&batch, CompressionCodec::None, pusher.clone(), 1, 1_024);
        let metrics = metrics();
        let upstream_failure =
            DataFusionError::External(Box::new(io::Error::other("shuffle input failed")));
        let mut batches = [Ok(batch.clone()), Err(upstream_failure)].into_iter();

        let error = writer.write(0, &mut batches, &metrics).unwrap_err();
        let DataFusionError::External(cause) = error else {
            panic!("upstream error was wrapped or stringified");
        };
        assert_eq!(cause.to_string(), "shuffle input failed");
        assert_eq!(pusher.frames().len(), 1);
        assert!(write_batches(&mut writer, 0, vec![batch], &metrics).is_err());
        assert!(finish_partition(&mut writer, 0, vec![], &metrics).is_err());
        assert!(writer.finish_all(&metrics).is_err());
        assert_eq!(pusher.frames().len(), 1);
    }

    #[test]
    fn legacy_jvm_frame_limit_uses_a_representable_java_encoding_reservation() {
        struct IntegerLimitedPusher {
            reservation: Mutex<Option<usize>>,
            frames: Mutex<Vec<RecordedFrame>>,
        }

        impl ShufflePartitionPusher for IntegerLimitedPusher {
            fn max_reservation_size(&self) -> usize {
                i32::MAX as usize
            }

            fn reserve_partition_data(&self, bytes: usize) -> Result<()> {
                assert!(i32::try_from(bytes).is_ok());
                *self.reservation.lock().unwrap() = Some(bytes);
                Ok(())
            }

            fn push_partition_data(&self, partition: i32, frame: &[u8]) -> Result<()> {
                self.frames
                    .lock()
                    .unwrap()
                    .push((partition, frame.to_vec()));
                Ok(())
            }
        }

        let batch = sample_batch(0, 4);
        let pusher = Arc::new(IntegerLimitedPusher {
            reservation: Mutex::new(None),
            frames: Mutex::new(Vec::new()),
        });
        let mut writer = writer(
            &batch,
            CompressionCodec::None,
            pusher.clone(),
            1,
            (i32::MAX - 8) as usize,
        );

        finish_partition(&mut writer, 0, vec![batch.clone()], &metrics()).unwrap();
        let frames = pusher.frames.lock().unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(decode_frame(&frames[0].1), batch);
        let reservation = pusher.reservation.lock().unwrap().unwrap();
        assert!(reservation >= frames[0].1.len() * 3);
        assert!(
            reservation < 64 * 1024,
            "legacy callbacks should charge the actual batch instead of the JVM array maximum"
        );
    }
}
