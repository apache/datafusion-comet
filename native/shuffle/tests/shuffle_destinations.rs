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

use std::error::Error;
use std::fmt;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, Int64Array, RecordBatch, RecordBatchOptions};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::row::{RowConverter, SortField};
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::config::SessionConfig;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::common::collect;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_comet_shuffle::{
    read_ipc_compressed, CometPartitioning, CompressionCodec, PartitionPusher, ShuffleDestination,
    ShuffleWriterExec,
};
use datafusion_comet_spark_expr::murmur3::create_murmur3_hashes;

const MAX_FRAME_BYTES: usize = 1024 * 1024;
const RANGE_BOUNDS: [i64; 3] = [17, 41, 63];

#[derive(Default)]
struct RecordingPusher(Mutex<Vec<(usize, Vec<u8>)>>);

impl PartitionPusher for RecordingPusher {
    fn push_partition_data(&self, partition_id: usize, frame: &[u8]) -> Result<()> {
        self.0.lock().unwrap().push((partition_id, frame.to_vec()));
        Ok(())
    }
}

impl RecordingPusher {
    fn decoded(&self, num_partitions: usize) -> Vec<Vec<RecordBatch>> {
        let mut partitions = vec![vec![]; num_partitions];
        for (partition_id, frame) in self.0.lock().unwrap().iter() {
            assert!(*partition_id < num_partitions);
            partitions[*partition_id].push(decode_frame(frame));
        }
        partitions
    }
}

fn int_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

fn batch(values: impl IntoIterator<Item = i64>) -> RecordBatch {
    RecordBatch::try_new(
        int_schema(),
        vec![Arc::new(Int64Array::from_iter_values(values))],
    )
    .unwrap()
}

fn empty_schema_batch(num_rows: usize) -> RecordBatch {
    RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(num_rows)),
    )
    .unwrap()
}

fn source(schema: SchemaRef, batches: &[RecordBatch]) -> Arc<dyn ExecutionPlan> {
    Arc::new(DataSourceExec::new(Arc::new(
        MemorySourceConfig::try_new(&[batches.to_vec()], schema, None).unwrap(),
    )))
}

fn hash_partitioning(num_partitions: usize) -> CometPartitioning {
    CometPartitioning::Hash(vec![Arc::new(Column::new("id", 0))], num_partitions)
}

fn partitionings() -> [CometPartitioning; 4] {
    let ordering = LexOrdering::new(vec![PhysicalSortExpr::new_default(Arc::new(Column::new(
        "id", 0,
    )))])
    .unwrap();
    let converter = RowConverter::new(vec![SortField::new(DataType::Int64)]).unwrap();
    let bounds: ArrayRef = Arc::new(Int64Array::from(RANGE_BOUNDS.to_vec()));
    let bounds = converter
        .convert_columns(&[bounds])
        .unwrap()
        .iter()
        .map(|row| row.owned())
        .collect();
    [
        CometPartitioning::SinglePartition,
        hash_partitioning(4),
        CometPartitioning::RoundRobin(4, 0),
        CometPartitioning::RangePartitioning(ordering, 4, Arc::new(converter), bounds),
    ]
}

fn codecs() -> [CompressionCodec; 4] {
    [
        CompressionCodec::None,
        CompressionCodec::Lz4Frame,
        CompressionCodec::Snappy,
        CompressionCodec::Zstd(1),
    ]
}

fn writer(
    input: Arc<dyn ExecutionPlan>,
    partitioning: CometPartitioning,
    codec: CompressionCodec,
    shuffle_destination: ShuffleDestination,
    max_buffer_bytes: Option<usize>,
) -> ShuffleWriterExec {
    ShuffleWriterExec::try_new_with_destination(
        input,
        partitioning,
        codec,
        shuffle_destination,
        false,
        max_buffer_bytes,
    )
    .unwrap()
}

fn rss(pusher: Arc<dyn PartitionPusher>) -> ShuffleDestination {
    ShuffleDestination::Rss {
        pusher,
        max_frame_bytes: MAX_FRAME_BYTES,
    }
}

async fn execute(plan: &dyn ExecutionPlan) -> Result<()> {
    let context = SessionContext::new_with_config(SessionConfig::new().with_batch_size(32));
    execute_with_context(plan, &context).await
}

async fn execute_with_context(plan: &dyn ExecutionPlan, context: &SessionContext) -> Result<()> {
    let output = collect(plan.execute(0, context.task_ctx())?).await?;
    assert!(output.is_empty(), "shuffle is a sink operator");
    Ok(())
}

fn paths(dir: &Path, tag: &str) -> (String, String) {
    (
        dir.join(format!("{tag}.data")).to_str().unwrap().to_owned(),
        dir.join(format!("{tag}.index"))
            .to_str()
            .unwrap()
            .to_owned(),
    )
}

fn local(output_paths: &(String, String)) -> ShuffleDestination {
    ShuffleDestination::Local {
        output_data_file: output_paths.0.clone(),
        output_index_file: output_paths.1.clone(),
        write_buffer_size: 4096,
    }
}

fn decode_frame(frame: &[u8]) -> RecordBatch {
    assert!(frame.len() >= 20, "missing Comet frame header");
    let declared_length = u64::from_le_bytes(frame[..8].try_into().unwrap());
    assert_eq!(declared_length + 8, frame.len() as u64);
    let batch = read_ipc_compressed(&frame[16..]).unwrap();
    let field_count = u64::from_le_bytes(frame[8..16].try_into().unwrap());
    assert_eq!(field_count, batch.num_columns() as u64);
    batch
}

fn decode_frames(mut bytes: &[u8]) -> Vec<RecordBatch> {
    let mut batches = vec![];
    while !bytes.is_empty() {
        let length = u64::from_le_bytes(bytes[..8].try_into().unwrap()) as usize + 8;
        let (frame, rest) = bytes.split_at(length);
        batches.push(decode_frame(frame));
        bytes = rest;
    }
    batches
}

fn read_local(output_paths: &(String, String), num_partitions: usize) -> Vec<Vec<RecordBatch>> {
    let data = std::fs::read(&output_paths.0).unwrap();
    let index = std::fs::read(&output_paths.1).unwrap();
    assert_eq!(index.len(), (num_partitions + 1) * 8);
    let offsets: Vec<usize> = index
        .chunks_exact(8)
        .map(|bytes| u64::from_le_bytes(bytes.try_into().unwrap()) as usize)
        .collect();
    assert_eq!(offsets[0], 0);
    assert_eq!(offsets[num_partitions], data.len());
    offsets
        .windows(2)
        .map(|range| {
            assert!(range[0] <= range[1]);
            decode_frames(&data[range[0]..range[1]])
        })
        .collect()
}

fn values(partitions: &[Vec<RecordBatch>]) -> Vec<Vec<i64>> {
    partitions
        .iter()
        .map(|batches| {
            let mut values: Vec<i64> = batches
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .values()
                        .iter()
                        .copied()
                })
                .collect();
            // Repartitioning does not promise output order or identical IPC batch boundaries.
            values.sort_unstable();
            values
        })
        .collect()
}

fn expected_values(partitioning: &CometPartitioning, batches: &[RecordBatch]) -> Vec<Vec<i64>> {
    let num_partitions = partitioning.partition_count();
    let mut expected = vec![vec![]; num_partitions];
    for batch in batches {
        let mut hashes = vec![42; batch.num_rows()];
        create_murmur3_hashes(batch.columns(), &mut hashes).unwrap();
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for (value, hash) in column.values().iter().zip(hashes) {
            let partition_id = match partitioning {
                CometPartitioning::SinglePartition => 0,
                CometPartitioning::Hash(_, _) | CometPartitioning::RoundRobin(_, _) => {
                    (hash as i32).rem_euclid(num_partitions as i32) as usize
                }
                CometPartitioning::RangePartitioning(_, _, _, _) => {
                    RANGE_BOUNDS.partition_point(|bound| bound <= value)
                }
            };
            expected[partition_id].push(*value);
        }
    }
    for partition in &mut expected {
        partition.sort_unstable();
    }
    expected
}

#[tokio::test]
#[cfg_attr(miri, ignore)] // Miri cannot call the compression libraries.
async fn destinations_preserve_local_bytes_and_partition_rows() {
    let batches = vec![batch(0..31), batch(31..79), batch([]), batch(79..95)];
    let input = source(int_schema(), &batches);
    let dir = tempfile::tempdir().unwrap();

    for (codec_id, codec) in codecs().into_iter().enumerate() {
        for (partitioning_id, partitioning) in partitionings().into_iter().enumerate() {
            let num_partitions = partitioning.partition_count();
            let expected = expected_values(&partitioning, &batches);
            let pusher = Arc::new(RecordingPusher::default());
            let remote = writer(
                Arc::clone(&input),
                partitioning.clone(),
                codec.clone(),
                rss(pusher.clone()),
                None,
            );
            execute(&remote).await.unwrap();
            assert_eq!(values(&pusher.decoded(num_partitions)), expected);

            let tag = format!("{codec_id}-{partitioning_id}");
            let legacy_paths = paths(dir.path(), &format!("legacy-{tag}"));
            let explicit_paths = paths(dir.path(), &format!("explicit-{tag}"));
            let legacy = ShuffleWriterExec::try_new(
                Arc::clone(&input),
                partitioning.clone(),
                codec.clone(),
                legacy_paths.0.clone(),
                legacy_paths.1.clone(),
                false,
                4096,
                None,
            )
            .unwrap();
            let explicit = writer(
                Arc::clone(&input),
                partitioning,
                codec.clone(),
                local(&explicit_paths),
                None,
            );
            execute(&legacy).await.unwrap();
            execute(&explicit).await.unwrap();
            assert_eq!(
                std::fs::read(&legacy_paths.0).unwrap(),
                std::fs::read(&explicit_paths.0).unwrap()
            );
            assert_eq!(
                std::fs::read(&legacy_paths.1).unwrap(),
                std::fs::read(&explicit_paths.1).unwrap()
            );
            assert_eq!(
                values(&read_local(&explicit_paths, num_partitions)),
                expected
            );
        }
    }
}

#[tokio::test]
async fn rss_buffer_limit_spills_without_changing_partition_rows() {
    let batches: Vec<_> = (0..16).map(|i| batch(i * 16..(i + 1) * 16)).collect();
    let input = source(int_schema(), &batches);
    let partitioning = hash_partitioning(4);
    let expected = expected_values(&partitioning, &batches);
    // RSS flushing must not ask DataFusion to create a local spill file.
    let runtime = RuntimeEnvBuilder::new()
        .with_disk_manager_builder(
            DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
        )
        .build()
        .unwrap();
    let context = SessionContext::new_with_config_rt(
        SessionConfig::new().with_batch_size(32),
        Arc::new(runtime),
    );

    for max_buffer_bytes in [None, Some(1)] {
        let pusher = Arc::new(RecordingPusher::default());
        let exec = writer(
            Arc::clone(&input),
            partitioning.clone(),
            CompressionCodec::None,
            rss(pusher.clone()),
            max_buffer_bytes,
        );
        execute_with_context(&exec, &context).await.unwrap();
        assert_eq!(values(&pusher.decoded(4)), expected);
        let spills = exec.metrics().unwrap().spill_count().unwrap();
        if max_buffer_bytes.is_some() {
            assert!(spills > 0, "the test must exercise insertion-time flushing");
        } else {
            assert_eq!(spills, 0);
        }
    }
}

#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn rss_empty_schema_preserves_row_counts() {
    let batches = vec![
        empty_schema_batch(7),
        empty_schema_batch(0),
        empty_schema_batch(11),
    ];
    for codec in codecs() {
        for partitioning in [
            CometPartitioning::SinglePartition,
            CometPartitioning::RoundRobin(4, 0),
        ] {
            let num_partitions = partitioning.partition_count();
            let pusher = Arc::new(RecordingPusher::default());
            let exec = writer(
                source(batches[0].schema(), &batches),
                partitioning,
                codec.clone(),
                rss(pusher.clone()),
                None,
            );
            execute(&exec).await.unwrap();
            let decoded = pusher.decoded(num_partitions);
            assert_eq!(decoded[0].len(), 1);
            assert_eq!(decoded[0][0].num_columns(), 0);
            assert_eq!(decoded[0][0].num_rows(), 18);
            assert!(decoded[1..].iter().all(Vec::is_empty));
        }
    }
}

#[tokio::test]
async fn empty_input_produces_no_remote_frames_or_local_data() {
    let dir = tempfile::tempdir().unwrap();
    for (schema_id, schema) in [int_schema(), Arc::new(Schema::empty())]
        .into_iter()
        .enumerate()
    {
        for (partitioning_id, partitioning) in [
            CometPartitioning::SinglePartition,
            CometPartitioning::RoundRobin(4, 0),
        ]
        .into_iter()
        .enumerate()
        {
            // A missing batch and a present zero-row batch must behave identically.
            for (batch_id, batches) in [vec![], vec![RecordBatch::new_empty(Arc::clone(&schema))]]
                .into_iter()
                .enumerate()
            {
                let num_partitions = partitioning.partition_count();
                let input = source(Arc::clone(&schema), &batches);
                let pusher = Arc::new(RecordingPusher::default());
                execute(&writer(
                    Arc::clone(&input),
                    partitioning.clone(),
                    CompressionCodec::None,
                    rss(pusher.clone()),
                    None,
                ))
                .await
                .unwrap();
                assert!(pusher.0.lock().unwrap().is_empty());

                let output_paths = paths(
                    dir.path(),
                    &format!("empty-{schema_id}-{partitioning_id}-{batch_id}"),
                );
                execute(&writer(
                    input,
                    partitioning.clone(),
                    CompressionCodec::None,
                    local(&output_paths),
                    None,
                ))
                .await
                .unwrap();
                assert!(read_local(&output_paths, num_partitions)
                    .iter()
                    .all(Vec::is_empty));
            }
        }
    }
}

#[tokio::test]
async fn replacing_children_keeps_the_selected_destination() {
    let input = source(int_schema(), &[batch([1, 2])]);
    let replacement = source(int_schema(), &[batch([8, 9, 10])]);
    let pusher = Arc::new(RecordingPusher::default());
    let dir = tempfile::tempdir().unwrap();
    let output_paths = paths(dir.path(), "replacement");

    for shuffle_destination in [rss(pusher.clone()), local(&output_paths)] {
        let original = Arc::new(writer(
            Arc::clone(&input),
            CometPartitioning::SinglePartition,
            CompressionCodec::None,
            shuffle_destination,
            None,
        ));
        let rewritten = original
            .with_new_children(vec![Arc::clone(&replacement)])
            .unwrap();
        execute(rewritten.as_ref()).await.unwrap();
    }
    assert_eq!(values(&pusher.decoded(1)), vec![vec![8, 9, 10]]);
    assert_eq!(values(&read_local(&output_paths, 1)), vec![vec![8, 9, 10]]);
}

#[test]
fn rss_configuration_is_rejected_before_execution() {
    let input = source(int_schema(), &[]);
    let pusher = Arc::new(RecordingPusher::default());
    for (num_partitions, max_frame_bytes) in [
        (0, MAX_FRAME_BYTES),
        (i32::MAX as usize + 1, MAX_FRAME_BYTES),
        (1, 0),
        (1, 19),
        (1, i32::MAX as usize + 1),
    ] {
        let result = ShuffleWriterExec::try_new_with_destination(
            Arc::clone(&input),
            CometPartitioning::RoundRobin(num_partitions, 0),
            CompressionCodec::None,
            ShuffleDestination::Rss {
                pusher: pusher.clone(),
                max_frame_bytes,
            },
            false,
            None,
        );
        assert!(matches!(result, Err(DataFusionError::Configuration(_))));
    }
    assert!(pusher.0.lock().unwrap().is_empty());
}

#[tokio::test]
async fn oversized_frame_fails_before_calling_the_pusher() {
    let pusher = Arc::new(RecordingPusher::default());
    let exec = writer(
        source(int_schema(), &[batch(0..64)]),
        CometPartitioning::SinglePartition,
        CompressionCodec::None,
        ShuffleDestination::Rss {
            pusher: pusher.clone(),
            max_frame_bytes: 20,
        },
        None,
    );
    let error = execute(&exec).await.unwrap_err();
    assert!(error
        .to_string()
        .contains("RSS frame exceeds its byte limit"));
    assert!(pusher.0.lock().unwrap().is_empty());
}

#[derive(Debug)]
struct CallbackFailure(Arc<()>);

impl fmt::Display for CallbackFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("test callback failure")
    }
}

impl Error for CallbackFailure {}

struct FailingPusher {
    marker: Arc<()>,
    calls: AtomicUsize,
}

impl PartitionPusher for FailingPusher {
    fn push_partition_data(&self, _partition_id: usize, _frame: &[u8]) -> Result<()> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Err(DataFusionError::External(Box::new(CallbackFailure(
            Arc::clone(&self.marker),
        ))))
    }
}

#[tokio::test]
async fn callback_errors_keep_their_type_and_identity() {
    for (partitioning, batches, max_buffer_bytes) in [
        // Single-partition writes happen while inserting the input batch.
        (CometPartitioning::SinglePartition, vec![batch(0..64)], None),
        // Multi-partition output is written at finalization unless a spill is forced.
        (hash_partitioning(4), vec![batch(0..64)], None),
        (hash_partitioning(4), vec![batch(0..64)], Some(1)),
        // Empty-schema output is also written during finalization.
        (
            CometPartitioning::RoundRobin(4, 0),
            vec![empty_schema_batch(3)],
            None,
        ),
    ] {
        let marker = Arc::new(());
        let pusher = Arc::new(FailingPusher {
            marker: Arc::clone(&marker),
            calls: AtomicUsize::new(0),
        });
        let exec = writer(
            source(batches[0].schema(), &batches),
            partitioning,
            CompressionCodec::None,
            rss(pusher.clone()),
            max_buffer_bytes,
        );
        let error = execute(&exec).await.unwrap_err();
        let DataFusionError::External(error) = error else {
            panic!("callback error was wrapped or stringified: {error:?}");
        };
        let callback_error = error.downcast_ref::<CallbackFailure>().unwrap();
        assert!(Arc::ptr_eq(&callback_error.0, &marker));
        assert_eq!(pusher.calls.load(Ordering::Relaxed), 1);
    }
}
