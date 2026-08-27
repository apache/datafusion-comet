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
    use arrow::array::{Array, DictionaryArray, Int32Array};
    use arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use arrow::ipc::writer::CompressionContext;
    use arrow::record_batch::RecordBatch;
    use datafusion::common::{DataFusionError, Result};
    use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, Time};
    use datafusion_comet_jni_bridge::ShufflePartitionPusher;
    use std::io::Cursor;
    use std::sync::{Arc, Mutex};

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
        let block_writer = ShuffleBlockWriter::try_new(batch.schema().as_ref(), codec).unwrap();
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
    fn rejects_oversized_frames_without_splitting_or_pushing() {
        let batch = sample_batch(0, 16);
        let frame_size = encoded_frame_size(&batch);
        let pusher = Arc::new(RecordingPusher::default());
        let mut writer = writer(
            &batch,
            CompressionCodec::None,
            pusher.clone(),
            1,
            frame_size - 1,
        );

        assert!(write_batches(&mut writer, 0, vec![batch], &metrics()).is_err());
        assert!(
            pusher.frames().is_empty(),
            "an oversized Arrow IPC frame must never be fragmented"
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
}
