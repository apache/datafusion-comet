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

use crate::{
    read_ipc_compressed, CometPartitioning, CompressionCodec, ShuffleWriterDestination,
    ShuffleWriterExec,
};
use arrow::array::{Array, Int32Array, RecordBatch, RecordBatchOptions};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::row::{RowConverter, SortField};
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::config::SessionConfig;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::common::collect;
use datafusion::physical_plan::{ChildrenPropertiesMode, ExecutionPlan, ReplaceChildrenOptions};
use datafusion::prelude::SessionContext;
use datafusion_comet_jni_bridge::ShufflePartitionPusher;
use std::error::Error;
use std::fmt::{Display, Formatter};
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

#[derive(Debug)]
struct CallbackSentinel;

impl Display for CallbackSentinel {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("original typed shuffle callback failure")
    }
}

impl Error for CallbackSentinel {}

struct ExternalFailingPusher;

impl ShufflePartitionPusher for ExternalFailingPusher {
    fn push_partition_data(&self, _partition_id: i32, _data: &[u8]) -> Result<()> {
        Err(DataFusionError::External(Box::new(CallbackSentinel)))
    }
}

fn int_batch(start: i32, count: i32) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int32,
        false,
    )]));
    let values = Int32Array::from_iter_values(start..start + count);
    RecordBatch::try_new(schema, vec![Arc::new(values)]).unwrap()
}

fn memory_input(batches: Vec<RecordBatch>, schema: SchemaRef) -> Arc<dyn ExecutionPlan> {
    let config = MemorySourceConfig::try_new(std::slice::from_ref(&batches), schema, None).unwrap();
    Arc::new(DataSourceExec::new(Arc::new(config)))
}

fn rss_execution(
    batches: Vec<RecordBatch>,
    schema: SchemaRef,
    partitioning: CometPartitioning,
    pusher: Arc<dyn ShufflePartitionPusher>,
    codec: CompressionCodec,
    max_frame_size: usize,
    max_buffer_bytes: Option<usize>,
) -> ShuffleWriterExec {
    ShuffleWriterExec::try_new_with_destination(
        memory_input(batches, schema),
        partitioning,
        codec,
        ShuffleWriterDestination::Rss {
            pusher,
            max_frame_size,
        },
        false,
        1024 * 1024,
        max_buffer_bytes,
    )
    .unwrap()
}

fn run_execution(plan: &dyn ExecutionPlan) -> Result<Vec<RecordBatch>> {
    let runtime = Arc::new(
        RuntimeEnvBuilder::new()
            .with_memory_limit(1024 * 1024 * 1024, 1.0)
            .build()
            .unwrap(),
    );
    let context = SessionContext::new_with_config_rt(SessionConfig::new(), runtime);
    let stream = plan.execute(0, context.task_ctx())?;
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(collect(stream))
}

fn decode_frame(frame: &[u8]) -> RecordBatch {
    assert!(frame.len() >= 16, "shuffle frame must contain its header");
    let encoded_length = u64::from_le_bytes(frame[..8].try_into().unwrap());
    assert_eq!(
        usize::try_from(encoded_length).unwrap() + 8,
        frame.len(),
        "each callback must receive one complete length-prefixed frame"
    );
    read_ipc_compressed(&frame[16..]).unwrap()
}

fn values_in_frames(frames: &[RecordedFrame]) -> Vec<i32> {
    frames
        .iter()
        .flat_map(|(_, frame)| {
            let batch = decode_frame(frame);
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect()
}

fn metric_value(execution: &ShuffleWriterExec, name: &str) -> usize {
    execution
        .metrics()
        .unwrap()
        .iter()
        .find(|metric| metric.value().name() == name)
        .map(|metric| metric.value().as_usize())
        .unwrap_or_default()
}

fn assert_original_callback_failure(error: DataFusionError) {
    match error {
        DataFusionError::External(original) => assert!(
            original.downcast_ref::<CallbackSentinel>().is_some(),
            "the original typed callback error must remain downcastable"
        ),
        other => panic!("callback failure was wrapped or replaced: {other}"),
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn rss_single_partition_preserves_complete_frames_and_codecs() {
    let first = int_batch(0, 16);
    let second = int_batch(16, 16);

    for codec in [
        CompressionCodec::None,
        CompressionCodec::Lz4Frame,
        CompressionCodec::Snappy,
        CompressionCodec::Zstd(1),
    ] {
        let pusher = Arc::new(RecordingPusher::default());
        let execution = rss_execution(
            vec![first.clone(), second.clone()],
            first.schema(),
            CometPartitioning::SinglePartition,
            pusher.clone(),
            codec,
            1024 * 1024,
            None,
        );

        assert!(run_execution(&execution).unwrap().is_empty());
        let frames = pusher.frames();
        assert_eq!(frames.len(), 2);
        assert!(frames.iter().all(|(partition_id, _)| *partition_id == 0));
        assert_eq!(values_in_frames(&frames), (0..32).collect::<Vec<_>>());
        assert_eq!(metric_value(&execution, "input_batches"), 2);
        assert_eq!(metric_value(&execution, "output_rows"), 32);
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn rss_multi_partition_supports_hash_range_and_round_robin() {
    let batch = int_batch(0, 100);
    let expression = Arc::new(Column::new("value", 0));
    let ordering =
        LexOrdering::new(vec![PhysicalSortExpr::new_default(expression.clone())]).unwrap();
    let converter = RowConverter::new(vec![SortField::new(DataType::Int32)]).unwrap();
    let boundary_values: Arc<dyn Array> = Arc::new(Int32Array::from(vec![25, 50, 75]));
    let boundaries = converter
        .convert_columns(&[boundary_values])
        .unwrap()
        .iter()
        .map(|row| row.owned())
        .collect();

    for partitioning in [
        CometPartitioning::Hash(vec![expression], 4),
        CometPartitioning::RangePartitioning(ordering, 4, Arc::new(converter), boundaries),
        CometPartitioning::RoundRobin(4, 0),
    ] {
        let pusher = Arc::new(RecordingPusher::default());
        let execution = rss_execution(
            vec![batch.clone()],
            batch.schema(),
            partitioning,
            pusher.clone(),
            CompressionCodec::Lz4Frame,
            1024 * 1024,
            None,
        );

        run_execution(&execution).unwrap();
        let frames = pusher.frames();
        assert!(!frames.is_empty());
        assert!(frames
            .iter()
            .all(|(partition_id, _)| (0..4).contains(partition_id)));
        let mut values = values_in_frames(&frames);
        values.sort_unstable();
        assert_eq!(values, (0..100).collect::<Vec<_>>());
        assert_eq!(metric_value(&execution, "output_rows"), 100);
    }
}

#[test]
#[cfg_attr(miri, ignore)]
fn rss_empty_schema_preserves_row_counts_in_partition_zero() {
    let schema = Arc::new(Schema::empty());
    let batch = RecordBatch::try_new_with_options(
        schema.clone(),
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(37)),
    )
    .unwrap();
    let pusher = Arc::new(RecordingPusher::default());
    let execution = rss_execution(
        vec![batch.clone(), batch],
        schema,
        CometPartitioning::RoundRobin(4, 0),
        pusher.clone(),
        CompressionCodec::None,
        1024 * 1024,
        None,
    );

    run_execution(&execution).unwrap();
    let frames = pusher.frames();
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].0, 0);
    assert_eq!(decode_frame(&frames[0].1).num_rows(), 74);
    assert_eq!(metric_value(&execution, "output_rows"), 74);
}

#[test]
fn rss_empty_schema_without_rows_does_not_push_frames() {
    let schema = Arc::new(Schema::empty());
    let batch = RecordBatch::try_new_with_options(
        schema.clone(),
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(0)),
    )
    .unwrap();
    let pusher = Arc::new(RecordingPusher::default());
    let execution = rss_execution(
        vec![batch],
        schema,
        CometPartitioning::RoundRobin(4, 0),
        pusher.clone(),
        CompressionCodec::None,
        1024 * 1024,
        None,
    );

    run_execution(&execution).unwrap();
    assert!(pusher.frames().is_empty());
}

#[test]
#[cfg_attr(miri, ignore)]
fn rss_spill_pushes_remotely_without_reporting_disk_bytes() {
    let batches = (0..6)
        .map(|index| int_batch(index * 512, 512))
        .collect::<Vec<_>>();
    let schema = batches[0].schema();
    let pusher = Arc::new(RecordingPusher::default());
    let execution = rss_execution(
        batches,
        schema,
        CometPartitioning::Hash(vec![Arc::new(Column::new("value", 0))], 4),
        pusher.clone(),
        CompressionCodec::Lz4Frame,
        1024 * 1024,
        Some(256),
    );

    run_execution(&execution).unwrap();
    assert!(metric_value(&execution, "spill_count") > 0);
    assert!(metric_value(&execution, "memory_spilled_bytes") > 0);
    assert_eq!(metric_value(&execution, "spilled_bytes"), 0);
    let mut values = values_in_frames(&pusher.frames());
    values.sort_unstable();
    assert_eq!(values, (0..3072).collect::<Vec<_>>());
}

#[test]
fn rss_preserves_typed_callback_failures_while_inserting() {
    let batch = int_batch(0, 8);
    let execution = rss_execution(
        vec![batch.clone()],
        batch.schema(),
        CometPartitioning::SinglePartition,
        Arc::new(ExternalFailingPusher),
        CompressionCodec::None,
        1024 * 1024,
        None,
    );

    assert_original_callback_failure(run_execution(&execution).unwrap_err());
}

#[test]
fn rss_preserves_typed_callback_failures_while_finalizing_and_spilling() {
    for max_buffer_bytes in [None, Some(1)] {
        let batch = int_batch(0, 64);
        let execution = rss_execution(
            vec![batch.clone()],
            batch.schema(),
            CometPartitioning::Hash(vec![Arc::new(Column::new("value", 0))], 4),
            Arc::new(ExternalFailingPusher),
            CompressionCodec::None,
            1024 * 1024,
            max_buffer_bytes,
        );

        assert_original_callback_failure(run_execution(&execution).unwrap_err());
    }
}

#[test]
fn rss_rejects_invalid_and_oversized_frames_without_pushing() {
    for max_frame_size in [0, 1] {
        let batch = int_batch(0, 8);
        let pusher = Arc::new(RecordingPusher::default());
        let execution = rss_execution(
            vec![batch.clone()],
            batch.schema(),
            CometPartitioning::SinglePartition,
            pusher.clone(),
            CompressionCodec::None,
            max_frame_size,
            None,
        );

        let error = run_execution(&execution).unwrap_err();
        assert!(error.to_string().contains("frame"));
        assert!(pusher.frames().is_empty());
    }
}

#[test]
fn rss_callback_survives_execution_plan_child_replacement() {
    let original = int_batch(0, 4);
    let replacement = int_batch(10, 6);
    let pusher = Arc::new(RecordingPusher::default());
    let execution = rss_execution(
        vec![original.clone()],
        original.schema(),
        CometPartitioning::SinglePartition,
        pusher.clone(),
        CompressionCodec::None,
        1024 * 1024,
        None,
    );
    let replacement_input = memory_input(vec![replacement.clone()], replacement.schema());
    let rewritten = Arc::new(execution)
        .replace_children(
            vec![replacement_input],
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
        .unwrap();

    run_execution(rewritten.as_ref()).unwrap();
    assert_eq!(
        values_in_frames(&pusher.frames()),
        (10..16).collect::<Vec<_>>()
    );
}

#[test]
fn explicit_local_destination_preserves_data_and_index_files() {
    let batch = int_batch(0, 8);
    let directory = tempfile::tempdir().unwrap();
    let data_file = directory.path().join("shuffle.data");
    let index_file = directory.path().join("shuffle.index");
    let execution = ShuffleWriterExec::try_new_with_destination(
        memory_input(vec![batch.clone()], batch.schema()),
        CometPartitioning::SinglePartition,
        CompressionCodec::None,
        ShuffleWriterDestination::Local {
            output_data_file: data_file.to_str().unwrap().to_string(),
            output_index_file: index_file.to_str().unwrap().to_string(),
        },
        false,
        1024 * 1024,
        None,
    )
    .unwrap();

    run_execution(&execution).unwrap();
    let frame = std::fs::read(data_file).unwrap();
    assert_eq!(decode_frame(&frame).num_rows(), 8);
    assert_eq!(std::fs::read(index_file).unwrap().len(), 16);
}
