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

//! Defines the External shuffle repartition plan.

use crate::metrics::ShufflePartitionerMetrics;
use crate::partitioners::{
    EmptySchemaShufflePartitioner, MultiPartitionShuffleRepartitioner, ShufflePartitioner,
    SinglePartitionShufflePartitioner,
};
use crate::{CometPartitioning, CompressionCodec};
use async_trait::async_trait;
use datafusion::common::exec_datafusion_err;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::EmptyRecordBatchStream;
use datafusion::{
    arrow::{datatypes::SchemaRef, error::ArrowError},
    error::Result,
    execution::context::TaskContext,
    physical_plan::{
        metrics::{ExecutionPlanMetricsSet, MetricsSet},
        stream::RecordBatchStreamAdapter,
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
    },
};
use datafusion_comet_common::tracing::with_trace_async;
use futures::{StreamExt, TryFutureExt, TryStreamExt};
use std::{
    any::Any,
    fmt,
    fmt::{Debug, Formatter},
    sync::Arc,
};

/// The shuffle writer operator maps each input partition to M output partitions based on a
/// partitioning scheme. No guarantees are made about the order of the resulting partitions.
#[derive(Debug)]
pub struct ShuffleWriterExec {
    /// Input execution plan
    input: Arc<dyn ExecutionPlan>,
    /// Partitioning scheme to use
    partitioning: CometPartitioning,
    /// Output data file path
    output_data_file: String,
    /// Output index file path
    output_index_file: String,
    /// Metrics
    metrics: ExecutionPlanMetricsSet,
    /// Cache for expensive-to-compute plan properties
    cache: Arc<PlanProperties>,
    /// The compression codec to use when compressing shuffle blocks
    codec: CompressionCodec,
    tracing_enabled: bool,
    /// Size of the write buffer in bytes
    write_buffer_size: usize,
}

impl ShuffleWriterExec {
    /// Create a new ShuffleWriterExec
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        partitioning: CometPartitioning,
        codec: CompressionCodec,
        output_data_file: String,
        output_index_file: String,
        tracing_enabled: bool,
        write_buffer_size: usize,
    ) -> Result<Self> {
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&input.schema())),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));

        Ok(ShuffleWriterExec {
            input,
            partitioning,
            metrics: ExecutionPlanMetricsSet::new(),
            output_data_file,
            output_index_file,
            cache,
            codec,
            tracing_enabled,
            write_buffer_size,
        })
    }
}

impl DisplayAs for ShuffleWriterExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "ShuffleWriterExec: partitioning={:?}, compression={:?}",
                    self.partitioning, self.codec
                )
            }
            DisplayFormatType::TreeRender => unimplemented!(),
        }
    }
}

#[async_trait]
impl ExecutionPlan for ShuffleWriterExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ShuffleWriterExec"
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.len() {
            1 => Ok(Arc::new(ShuffleWriterExec::try_new(
                Arc::clone(&children[0]),
                self.partitioning.clone(),
                self.codec.clone(),
                self.output_data_file.clone(),
                self.output_index_file.clone(),
                self.tracing_enabled,
                self.write_buffer_size,
            )?)),
            _ => panic!("ShuffleWriterExec wrong number of children"),
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, Arc::clone(&context))?;
        let metrics = ShufflePartitionerMetrics::new(&self.metrics, 0);

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            futures::stream::once(
                external_shuffle(
                    input,
                    partition,
                    self.output_data_file.clone(),
                    self.output_index_file.clone(),
                    self.partitioning.clone(),
                    metrics,
                    context,
                    self.codec.clone(),
                    self.tracing_enabled,
                    self.write_buffer_size,
                )
                .map_err(|e| ArrowError::ExternalError(Box::new(e))),
            )
            .try_flatten(),
        )))
    }
}

#[allow(clippy::too_many_arguments)]
async fn external_shuffle(
    mut input: SendableRecordBatchStream,
    partition: usize,
    output_data_file: String,
    output_index_file: String,
    partitioning: CometPartitioning,
    metrics: ShufflePartitionerMetrics,
    context: Arc<TaskContext>,
    codec: CompressionCodec,
    tracing_enabled: bool,
    write_buffer_size: usize,
) -> Result<SendableRecordBatchStream> {
    with_trace_async("external_shuffle", tracing_enabled, || async {
        let schema = input.schema();

        let mut repartitioner: Box<dyn ShufflePartitioner> = match &partitioning {
            _ if schema.fields().is_empty() => {
                log::debug!("found empty schema, overriding {partitioning:?} partitioning with EmptySchemaShufflePartitioner");
                Box::new(EmptySchemaShufflePartitioner::try_new(
                    output_data_file,
                    output_index_file,
                    Arc::clone(&schema),
                    partitioning.partition_count(),
                    metrics,
                    codec,
                )?)
            }
            any if any.partition_count() == 1 => {
                Box::new(SinglePartitionShufflePartitioner::try_new(
                    output_data_file,
                    output_index_file,
                    Arc::clone(&schema),
                    metrics,
                    context.session_config().batch_size(),
                    codec,
                    write_buffer_size,
                )?)
            }
            _ => Box::new(MultiPartitionShuffleRepartitioner::try_new(
                partition,
                output_data_file,
                output_index_file,
                Arc::clone(&schema),
                partitioning,
                metrics,
                context.runtime_env(),
                context.session_config().batch_size(),
                codec,
                tracing_enabled,
                write_buffer_size,
            )?),
        };

        while let Some(batch) = input.next().await {
            // Await the repartitioner to insert the batch and shuffle the rows
            // into the corresponding partition buffer.
            // Otherwise, pull the next batch from the input stream might overwrite the
            // current batch in the repartitioner.
            repartitioner
                .insert_batch(batch?)
                .await
                .map_err(|err| exec_datafusion_err!("Error inserting batch: {err}"))?;
        }

        repartitioner
            .shuffle_write()
            .map_err(|err| exec_datafusion_err!("Error in shuffle write: {err}"))?;

        // shuffle writer always has empty output
        Ok(Box::pin(EmptyRecordBatchStream::new(Arc::clone(&schema))) as SendableRecordBatchStream)
    })
    .await
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{read_ipc_compressed, ShuffleBlockWriter};
    use arrow::array::{Array, StringArray, StringBuilder};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use arrow::row::{RowConverter, SortField};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::execution::config::SessionConfig;
    use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
    use datafusion::physical_expr::expressions::{col, Column};
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion::physical_plan::common::collect;
    use datafusion::physical_plan::metrics::Time;
    use datafusion::prelude::SessionContext;
    use itertools::Itertools;
    use std::io::Cursor;
    use tokio::runtime::Runtime;

    #[test]
    #[cfg_attr(miri, ignore)] // miri can't call foreign function `ZSTD_createCCtx`
    fn roundtrip_ipc() {
        let batch = create_batch(8192);
        for codec in &[
            CompressionCodec::None,
            CompressionCodec::Zstd(1),
            CompressionCodec::Snappy,
            CompressionCodec::Lz4Frame,
        ] {
            let mut output = vec![];
            let mut cursor = Cursor::new(&mut output);
            let writer =
                ShuffleBlockWriter::try_new(batch.schema().as_ref(), codec.clone()).unwrap();
            let length = writer
                .write_batch(&batch, &mut cursor, &Time::default())
                .unwrap();
            assert_eq!(length, output.len());

            let ipc_without_length_prefix = &output[16..];
            let batch2 = read_ipc_compressed(ipc_without_length_prefix).unwrap();
            assert_eq!(batch, batch2);
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)] // miri can't call foreign function `ZSTD_createCCtx`
    fn test_single_partition_shuffle_writer() {
        shuffle_write_test(1000, 100, 1, None);
        shuffle_write_test(10000, 10, 1, None);
    }

    #[test]
    #[cfg_attr(miri, ignore)] // miri can't call foreign function `ZSTD_createCCtx`
    fn test_insert_larger_batch() {
        shuffle_write_test(10000, 1, 16, None);
    }

    #[test]
    #[cfg_attr(miri, ignore)] // miri can't call foreign function `ZSTD_createCCtx`
    fn test_insert_smaller_batch() {
        shuffle_write_test(1000, 1, 16, None);
        shuffle_write_test(1000, 10, 16, None);
    }

    #[test]
    #[cfg_attr(miri, ignore)] // miri can't call foreign function `ZSTD_createCCtx`
    fn test_large_number_of_partitions() {
        shuffle_write_test(10000, 10, 200, Some(10 * 1024 * 1024));
        shuffle_write_test(10000, 10, 2000, Some(10 * 1024 * 1024));
    }

    #[test]
    #[cfg_attr(miri, ignore)] // miri can't call foreign function `ZSTD_createCCtx`
    fn test_large_number_of_partitions_spilling() {
        shuffle_write_test(10000, 100, 200, Some(10 * 1024 * 1024));
    }

    #[tokio::test]
    async fn shuffle_partitioner_memory() {
        let batch = create_batch(900);
        assert_eq!(8316, batch.get_array_memory_size()); // Not stable across Arrow versions

        let memory_limit = 512 * 1024;
        let num_partitions = 2;
        let runtime_env = create_runtime(memory_limit);
        let metrics_set = ExecutionPlanMetricsSet::new();
        let mut repartitioner = MultiPartitionShuffleRepartitioner::try_new(
            0,
            "/tmp/data.out".to_string(),
            "/tmp/index.out".to_string(),
            batch.schema(),
            CometPartitioning::Hash(vec![Arc::new(Column::new("a", 0))], num_partitions),
            ShufflePartitionerMetrics::new(&metrics_set, 0),
            runtime_env,
            1024,
            CompressionCodec::Lz4Frame,
            false,
            1024 * 1024, // write_buffer_size: 1MB default
        )
        .unwrap();

        repartitioner.insert_batch(batch.clone()).await.unwrap();

        // before spill, no spill files should exist
        assert_eq!(repartitioner.spill_count_files(), 0);

        repartitioner.spill().unwrap();

        // after spill, exactly one combined spill file should exist (not one per partition)
        assert_eq!(repartitioner.spill_count_files(), 1);

        // insert another batch after spilling
        repartitioner.insert_batch(batch.clone()).await.unwrap();

        // spill again -- should create a second combined spill file
        repartitioner.spill().unwrap();
        assert_eq!(repartitioner.spill_count_files(), 2);
    }

    fn create_runtime(memory_limit: usize) -> Arc<RuntimeEnv> {
        Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_limit(memory_limit, 1.0)
                .build()
                .unwrap(),
        )
    }

    fn shuffle_write_test(
        batch_size: usize,
        num_batches: usize,
        num_partitions: usize,
        memory_limit: Option<usize>,
    ) {
        let batch = create_batch(batch_size);

        let lex_ordering = LexOrdering::new(vec![PhysicalSortExpr::new_default(
            col("a", batch.schema().as_ref()).unwrap(),
        )])
        .unwrap();

        let sort_fields: Vec<SortField> = batch
            .columns()
            .iter()
            .zip(&lex_ordering)
            .map(|(array, sort_expr)| {
                SortField::new_with_options(array.data_type().clone(), sort_expr.options)
            })
            .collect();
        let row_converter = RowConverter::new(sort_fields).unwrap();

        let owned_rows = if num_partitions == 1 {
            vec![]
        } else {
            // Determine range boundaries based on create_batch implementation. We just divide the
            // domain of values in the batch equally to find partition bounds.
            let bounds_strings = {
                let mut boundaries = Vec::with_capacity(num_partitions - 1);
                let step = batch_size as f64 / num_partitions as f64;

                for i in 1..(num_partitions) {
                    boundaries.push(Some((step * i as f64).round().to_string()));
                }
                boundaries
            };
            let bounds_array: Arc<dyn Array> = Arc::new(StringArray::from(bounds_strings));
            let bounds_rows = row_converter
                .convert_columns(vec![bounds_array].as_slice())
                .unwrap();

            let owned_rows_vec = bounds_rows.iter().map(|row| row.owned()).collect_vec();
            owned_rows_vec
        };

        for partitioning in [
            CometPartitioning::Hash(vec![Arc::new(Column::new("a", 0))], num_partitions),
            CometPartitioning::RangePartitioning(
                lex_ordering,
                num_partitions,
                Arc::new(row_converter),
                owned_rows,
            ),
            CometPartitioning::RoundRobin(num_partitions, 0),
        ] {
            let batches = (0..num_batches).map(|_| batch.clone()).collect::<Vec<_>>();

            let partitions = &[batches];
            let exec = ShuffleWriterExec::try_new(
                Arc::new(DataSourceExec::new(Arc::new(
                    MemorySourceConfig::try_new(partitions, batch.schema(), None).unwrap(),
                ))),
                partitioning,
                CompressionCodec::Zstd(1),
                "/tmp/data.out".to_string(),
                "/tmp/index.out".to_string(),
                false,
                1024 * 1024, // write_buffer_size: 1MB default
            )
            .unwrap();

            // 10MB memory should be enough for running this test
            let config = SessionConfig::new();
            let mut runtime_env_builder = RuntimeEnvBuilder::new();
            runtime_env_builder = match memory_limit {
                Some(limit) => runtime_env_builder.with_memory_limit(limit, 1.0),
                None => runtime_env_builder,
            };
            let runtime_env = Arc::new(runtime_env_builder.build().unwrap());
            let ctx = SessionContext::new_with_config_rt(config, runtime_env);
            let task_ctx = ctx.task_ctx();
            let stream = exec.execute(0, task_ctx).unwrap();
            let rt = Runtime::new().unwrap();
            rt.block_on(collect(stream)).unwrap();
        }
    }

    fn create_batch(batch_size: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)]));
        let mut b = StringBuilder::new();
        for i in 0..batch_size {
            b.append_value(format!("{i}"));
        }
        let array = b.finish();
        RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(array)]).unwrap()
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_round_robin_deterministic() {
        // Test that round robin partitioning produces identical results when run multiple times
        use std::fs;
        use std::io::Read;

        let batch_size = 1000;
        let num_batches = 10;
        let num_partitions = 8;

        let batch = create_batch(batch_size);
        let batches = (0..num_batches).map(|_| batch.clone()).collect::<Vec<_>>();

        // Run shuffle twice and compare results
        for run in 0..2 {
            let data_file = format!("/tmp/rr_data_{}.out", run);
            let index_file = format!("/tmp/rr_index_{}.out", run);

            let partitions = std::slice::from_ref(&batches);
            let exec = ShuffleWriterExec::try_new(
                Arc::new(DataSourceExec::new(Arc::new(
                    MemorySourceConfig::try_new(partitions, batch.schema(), None).unwrap(),
                ))),
                CometPartitioning::RoundRobin(num_partitions, 0),
                CompressionCodec::Zstd(1),
                data_file.clone(),
                index_file.clone(),
                false,
                1024 * 1024,
            )
            .unwrap();

            let config = SessionConfig::new();
            let runtime_env = Arc::new(
                RuntimeEnvBuilder::new()
                    .with_memory_limit(10 * 1024 * 1024, 1.0)
                    .build()
                    .unwrap(),
            );
            let session_ctx = Arc::new(SessionContext::new_with_config_rt(config, runtime_env));
            let task_ctx = Arc::new(TaskContext::from(session_ctx.as_ref()));

            // Execute the shuffle
            futures::executor::block_on(async {
                let mut stream = exec.execute(0, Arc::clone(&task_ctx)).unwrap();
                while stream.next().await.is_some() {}
            });

            if run == 1 {
                // Compare data files
                let mut data0 = Vec::new();
                fs::File::open("/tmp/rr_data_0.out")
                    .unwrap()
                    .read_to_end(&mut data0)
                    .unwrap();
                let mut data1 = Vec::new();
                fs::File::open("/tmp/rr_data_1.out")
                    .unwrap()
                    .read_to_end(&mut data1)
                    .unwrap();
                assert_eq!(
                    data0, data1,
                    "Round robin shuffle data should be identical across runs"
                );

                // Compare index files
                let mut index0 = Vec::new();
                fs::File::open("/tmp/rr_index_0.out")
                    .unwrap()
                    .read_to_end(&mut index0)
                    .unwrap();
                let mut index1 = Vec::new();
                fs::File::open("/tmp/rr_index_1.out")
                    .unwrap()
                    .read_to_end(&mut index1)
                    .unwrap();
                assert_eq!(
                    index0, index1,
                    "Round robin shuffle index should be identical across runs"
                );
            }
        }

        // Clean up
        let _ = fs::remove_file("/tmp/rr_data_0.out");
        let _ = fs::remove_file("/tmp/rr_index_0.out");
        let _ = fs::remove_file("/tmp/rr_data_1.out");
        let _ = fs::remove_file("/tmp/rr_index_1.out");
    }

    /// Test that batch coalescing in BufBatchWriter reduces output size by
    /// writing fewer, larger IPC blocks instead of many small ones.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_batch_coalescing_reduces_size() {
        use crate::writers::BufBatchWriter;
        use arrow::array::Int32Array;

        // Create a wide schema to amplify per-block schema overhead
        let fields: Vec<Field> = (0..20)
            .map(|i| Field::new(format!("col_{i}"), DataType::Int32, false))
            .collect();
        let schema = Arc::new(Schema::new(fields));

        // Create many small batches (50 rows each)
        let small_batches: Vec<RecordBatch> = (0..100)
            .map(|batch_idx| {
                let columns: Vec<Arc<dyn Array>> = (0..20)
                    .map(|col_idx| {
                        let values: Vec<i32> = (0..50)
                            .map(|row| batch_idx * 50 + row + col_idx * 1000)
                            .collect();
                        Arc::new(Int32Array::from(values)) as Arc<dyn Array>
                    })
                    .collect();
                RecordBatch::try_new(Arc::clone(&schema), columns).unwrap()
            })
            .collect();

        let codec = CompressionCodec::Lz4Frame;
        let encode_time = Time::default();
        let write_time = Time::default();

        // Write with coalescing (batch_size=8192)
        let mut coalesced_output = Vec::new();
        {
            let mut writer = ShuffleBlockWriter::try_new(schema.as_ref(), codec.clone()).unwrap();
            let mut buf_writer = BufBatchWriter::new(
                &mut writer,
                Cursor::new(&mut coalesced_output),
                1024 * 1024,
                8192,
            );
            for batch in &small_batches {
                buf_writer.write(batch, &encode_time, &write_time).unwrap();
            }
            buf_writer.flush(&encode_time, &write_time).unwrap();
        }

        // Write without coalescing (batch_size=1)
        let mut uncoalesced_output = Vec::new();
        {
            let mut writer = ShuffleBlockWriter::try_new(schema.as_ref(), codec.clone()).unwrap();
            let mut buf_writer = BufBatchWriter::new(
                &mut writer,
                Cursor::new(&mut uncoalesced_output),
                1024 * 1024,
                1,
            );
            for batch in &small_batches {
                buf_writer.write(batch, &encode_time, &write_time).unwrap();
            }
            buf_writer.flush(&encode_time, &write_time).unwrap();
        }

        // Coalesced output should be smaller due to fewer IPC schema blocks
        assert!(
            coalesced_output.len() < uncoalesced_output.len(),
            "Coalesced output ({} bytes) should be smaller than uncoalesced ({} bytes)",
            coalesced_output.len(),
            uncoalesced_output.len()
        );

        // Verify both roundtrip correctly by reading all IPC blocks
        let coalesced_rows = read_all_ipc_blocks(&coalesced_output);
        let uncoalesced_rows = read_all_ipc_blocks(&uncoalesced_output);
        assert_eq!(
            coalesced_rows, 5000,
            "Coalesced should contain all 5000 rows"
        );
        assert_eq!(
            uncoalesced_rows, 5000,
            "Uncoalesced should contain all 5000 rows"
        );
    }

    /// Read all IPC blocks from a byte buffer written by BufBatchWriter/ShuffleBlockWriter,
    /// returning the total number of rows.
    fn read_all_ipc_blocks(data: &[u8]) -> usize {
        let mut offset = 0;
        let mut total_rows = 0;
        while offset < data.len() {
            // First 8 bytes are the IPC length (little-endian u64)
            let ipc_length =
                u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap()) as usize;
            // Skip the 8-byte length prefix; the next 8 bytes are field_count + codec header
            let block_start = offset + 8;
            let block_end = block_start + ipc_length;
            // read_ipc_compressed expects data starting after the 16-byte header
            // (i.e., after length + field_count), at the codec tag
            let ipc_data = &data[block_start + 8..block_end];
            let batch = read_ipc_compressed(ipc_data).unwrap();
            total_rows += batch.num_rows();
            offset = block_end;
        }
        total_rows
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_empty_schema_shuffle_writer() {
        use std::fs;
        use std::io::Read;

        let num_rows = 1000;
        let num_batches = 5;
        let num_partitions = 10;

        let schema = Arc::new(Schema::new(Vec::<Field>::new()));
        let batch = RecordBatch::try_new_with_options(
            Arc::clone(&schema),
            vec![],
            &arrow::array::RecordBatchOptions::new().with_row_count(Some(num_rows)),
        )
        .unwrap();

        let batches = (0..num_batches).map(|_| batch.clone()).collect::<Vec<_>>();
        let partitions = &[batches];

        let dir = tempfile::tempdir().unwrap();
        let data_file = dir.path().join("data.out");
        let index_file = dir.path().join("index.out");

        let exec = ShuffleWriterExec::try_new(
            Arc::new(DataSourceExec::new(Arc::new(
                MemorySourceConfig::try_new(partitions, Arc::clone(&schema), None).unwrap(),
            ))),
            CometPartitioning::RoundRobin(num_partitions, 0),
            CompressionCodec::Zstd(1),
            data_file.to_str().unwrap().to_string(),
            index_file.to_str().unwrap().to_string(),
            false,
            1024 * 1024,
        )
        .unwrap();

        let config = SessionConfig::new();
        let runtime_env = Arc::new(RuntimeEnvBuilder::new().build().unwrap());
        let ctx = SessionContext::new_with_config_rt(config, runtime_env);
        let task_ctx = ctx.task_ctx();
        let stream = exec.execute(0, task_ctx).unwrap();
        let rt = Runtime::new().unwrap();
        rt.block_on(collect(stream)).unwrap();

        // Verify data file is non-empty (contains IPC batch with row count)
        let mut data = Vec::new();
        fs::File::open(&data_file)
            .unwrap()
            .read_to_end(&mut data)
            .unwrap();
        assert!(!data.is_empty(), "Data file should contain IPC data");

        // Verify row count survives roundtrip
        let total_rows = read_all_ipc_blocks(&data);
        assert_eq!(
            total_rows,
            num_rows * num_batches,
            "Row count should survive roundtrip"
        );

        // Verify index file structure: num_partitions + 1 offsets
        let mut index_data = Vec::new();
        fs::File::open(&index_file)
            .unwrap()
            .read_to_end(&mut index_data)
            .unwrap();
        let expected_index_size = (num_partitions + 1) * 8;
        assert_eq!(index_data.len(), expected_index_size);

        // First offset should be 0
        let first_offset = i64::from_le_bytes(index_data[0..8].try_into().unwrap());
        assert_eq!(first_offset, 0);

        // Second offset should equal data file length (partition 0 holds all data)
        let data_len = data.len() as i64;
        let second_offset = i64::from_le_bytes(index_data[8..16].try_into().unwrap());
        assert_eq!(second_offset, data_len);

        // All remaining offsets should equal data file length (empty partitions)
        for i in 2..=num_partitions {
            let offset = i64::from_le_bytes(index_data[i * 8..(i + 1) * 8].try_into().unwrap());
            assert_eq!(
                offset, data_len,
                "Partition {i} offset should equal data length"
            );
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_empty_schema_shuffle_writer_zero_rows() {
        use std::fs;
        use std::io::Read;

        let num_partitions = 4;

        let schema = Arc::new(Schema::new(Vec::<Field>::new()));
        let batch = RecordBatch::try_new_with_options(
            Arc::clone(&schema),
            vec![],
            &arrow::array::RecordBatchOptions::new().with_row_count(Some(0)),
        )
        .unwrap();

        let batches = vec![batch];
        let partitions = &[batches];

        let dir = tempfile::tempdir().unwrap();
        let data_file = dir.path().join("data.out");
        let index_file = dir.path().join("index.out");

        let exec = ShuffleWriterExec::try_new(
            Arc::new(DataSourceExec::new(Arc::new(
                MemorySourceConfig::try_new(partitions, Arc::clone(&schema), None).unwrap(),
            ))),
            CometPartitioning::RoundRobin(num_partitions, 0),
            CompressionCodec::Zstd(1),
            data_file.to_str().unwrap().to_string(),
            index_file.to_str().unwrap().to_string(),
            false,
            1024 * 1024,
        )
        .unwrap();

        let config = SessionConfig::new();
        let runtime_env = Arc::new(RuntimeEnvBuilder::new().build().unwrap());
        let ctx = SessionContext::new_with_config_rt(config, runtime_env);
        let task_ctx = ctx.task_ctx();
        let stream = exec.execute(0, task_ctx).unwrap();
        let rt = Runtime::new().unwrap();
        rt.block_on(collect(stream)).unwrap();

        // Data file should be empty (no rows to write)
        let mut data = Vec::new();
        fs::File::open(&data_file)
            .unwrap()
            .read_to_end(&mut data)
            .unwrap();
        assert!(data.is_empty(), "Data file should be empty with zero rows");

        // Index file should have all-zero offsets
        let mut index_data = Vec::new();
        fs::File::open(&index_file)
            .unwrap()
            .read_to_end(&mut index_data)
            .unwrap();
        let expected_index_size = (num_partitions + 1) * 8;
        assert_eq!(index_data.len(), expected_index_size);
        for i in 0..=num_partitions {
            let offset = i64::from_le_bytes(index_data[i * 8..(i + 1) * 8].try_into().unwrap());
            assert_eq!(offset, 0, "All offsets should be 0 with zero rows");
        }
    }

    /// Verify that spilling an empty repartitioner produces no spill files.
    #[tokio::test]
    async fn spill_empty_buffers_produces_no_file() {
        let batch = create_batch(100);
        let memory_limit = 512 * 1024;
        let num_partitions = 4;
        let runtime_env = create_runtime(memory_limit);
        let metrics_set = ExecutionPlanMetricsSet::new();
        let mut repartitioner = MultiPartitionShuffleRepartitioner::try_new(
            0,
            "/tmp/spill_empty_data.out".to_string(),
            "/tmp/spill_empty_index.out".to_string(),
            batch.schema(),
            CometPartitioning::Hash(vec![Arc::new(Column::new("a", 0))], num_partitions),
            ShufflePartitionerMetrics::new(&metrics_set, 0),
            runtime_env,
            1024,
            CompressionCodec::Lz4Frame,
            false,
            1024 * 1024,
        )
        .unwrap();

        // Spill with no data inserted -- should be a no-op
        repartitioner.spill().unwrap();
        assert_eq!(repartitioner.spill_count_files(), 0);
    }

    /// Verify that spilling with many partitions (some empty) still creates
    /// exactly one spill file per spill event, and that shuffle_write succeeds.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_spill_with_sparse_partitions() {
        // 100 rows across 50 partitions -- many partitions will be empty
        shuffle_write_test(100, 5, 50, Some(10 * 1024));
    }

    /// Verify that the output of a spill-based shuffle contains the same total
    /// row count and valid partition structure as a non-spill shuffle.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_spill_output_matches_non_spill() {
        use std::fs;

        let batch_size = 1000;
        let num_batches = 10;
        let num_partitions = 8;
        let total_rows = batch_size * num_batches;

        let batch = create_batch(batch_size);
        let batches = (0..num_batches).map(|_| batch.clone()).collect::<Vec<_>>();

        let parse_offsets = |index_data: &[u8]| -> Vec<i64> {
            index_data
                .chunks(8)
                .map(|chunk| i64::from_le_bytes(chunk.try_into().unwrap()))
                .collect()
        };

        let count_rows_in_partition = |data: &[u8], start: i64, end: i64| -> usize {
            if start == end {
                return 0;
            }
            read_all_ipc_blocks(&data[start as usize..end as usize])
        };

        // Run 1: no spilling (large memory limit)
        {
            let partitions = std::slice::from_ref(&batches);
            let exec = ShuffleWriterExec::try_new(
                Arc::new(DataSourceExec::new(Arc::new(
                    MemorySourceConfig::try_new(partitions, batch.schema(), None).unwrap(),
                ))),
                CometPartitioning::Hash(vec![Arc::new(Column::new("a", 0))], num_partitions),
                CompressionCodec::Zstd(1),
                "/tmp/no_spill_data.out".to_string(),
                "/tmp/no_spill_index.out".to_string(),
                false,
                1024 * 1024,
            )
            .unwrap();

            let config = SessionConfig::new();
            let runtime_env = Arc::new(
                RuntimeEnvBuilder::new()
                    .with_memory_limit(100 * 1024 * 1024, 1.0)
                    .build()
                    .unwrap(),
            );
            let ctx = SessionContext::new_with_config_rt(config, runtime_env);
            let task_ctx = ctx.task_ctx();
            let stream = exec.execute(0, task_ctx).unwrap();
            let rt = Runtime::new().unwrap();
            rt.block_on(collect(stream)).unwrap();
        }

        // Run 2: with spilling (memory limit forces spilling during insert_batch)
        {
            let partitions = std::slice::from_ref(&batches);
            let exec = ShuffleWriterExec::try_new(
                Arc::new(DataSourceExec::new(Arc::new(
                    MemorySourceConfig::try_new(partitions, batch.schema(), None).unwrap(),
                ))),
                CometPartitioning::Hash(vec![Arc::new(Column::new("a", 0))], num_partitions),
                CompressionCodec::Zstd(1),
                "/tmp/spill_data.out".to_string(),
                "/tmp/spill_index.out".to_string(),
                false,
                1024 * 1024,
            )
            .unwrap();

            let config = SessionConfig::new();
            let runtime_env = Arc::new(
                RuntimeEnvBuilder::new()
                    .with_memory_limit(512 * 1024, 1.0)
                    .build()
                    .unwrap(),
            );
            let ctx = SessionContext::new_with_config_rt(config, runtime_env);
            let task_ctx = ctx.task_ctx();
            let stream = exec.execute(0, task_ctx).unwrap();
            let rt = Runtime::new().unwrap();
            rt.block_on(collect(stream)).unwrap();
        }

        let no_spill_index = fs::read("/tmp/no_spill_index.out").unwrap();
        let spill_index = fs::read("/tmp/spill_index.out").unwrap();

        assert_eq!(
            no_spill_index.len(),
            spill_index.len(),
            "Index files should have the same number of entries"
        );

        let no_spill_offsets = parse_offsets(&no_spill_index);
        let spill_offsets = parse_offsets(&spill_index);

        let no_spill_data = fs::read("/tmp/no_spill_data.out").unwrap();
        let spill_data = fs::read("/tmp/spill_data.out").unwrap();

        // Verify row counts per partition match between spill and non-spill runs
        let mut no_spill_total_rows = 0;
        let mut spill_total_rows = 0;
        for i in 0..num_partitions {
            let ns_rows = count_rows_in_partition(
                &no_spill_data,
                no_spill_offsets[i],
                no_spill_offsets[i + 1],
            );
            let s_rows =
                count_rows_in_partition(&spill_data, spill_offsets[i], spill_offsets[i + 1]);
            assert_eq!(
                ns_rows, s_rows,
                "Partition {i} row count mismatch: no_spill={ns_rows}, spill={s_rows}"
            );
            no_spill_total_rows += ns_rows;
            spill_total_rows += s_rows;
        }

        assert_eq!(
            no_spill_total_rows, total_rows,
            "Non-spill total row count mismatch"
        );
        assert_eq!(
            spill_total_rows, total_rows,
            "Spill total row count mismatch"
        );

        // Cleanup
        let _ = fs::remove_file("/tmp/no_spill_data.out");
        let _ = fs::remove_file("/tmp/no_spill_index.out");
        let _ = fs::remove_file("/tmp/spill_data.out");
        let _ = fs::remove_file("/tmp/spill_index.out");
    }

    /// Verify that spill output file size matches non-spill output.
    /// This specifically tests that the byte range tracking in SpillInfo
    /// correctly accounts for all bytes written during BufBatchWriter flush,
    /// including the final coalescer batch that is emitted only during flush().
    /// A bug here would cause the output file to be much smaller than expected
    /// because copy_partition_with_handle() would copy truncated byte ranges.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_spill_output_file_size_matches_non_spill() {
        use std::fs;

        let batch_size = 100;
        let num_batches = 50;
        let num_partitions = 16;

        let batch = create_batch(batch_size);
        let batches = (0..num_batches).map(|_| batch.clone()).collect::<Vec<_>>();

        // Run 1: no spilling
        {
            let partitions = std::slice::from_ref(&batches);
            let exec = ShuffleWriterExec::try_new(
                Arc::new(DataSourceExec::new(Arc::new(
                    MemorySourceConfig::try_new(partitions, batch.schema(), None).unwrap(),
                ))),
                CometPartitioning::Hash(vec![Arc::new(Column::new("a", 0))], num_partitions),
                CompressionCodec::Zstd(1),
                "/tmp/size_no_spill_data.out".to_string(),
                "/tmp/size_no_spill_index.out".to_string(),
                false,
                1024 * 1024,
            )
            .unwrap();

            let config = SessionConfig::new();
            let runtime_env = Arc::new(
                RuntimeEnvBuilder::new()
                    .with_memory_limit(100 * 1024 * 1024, 1.0)
                    .build()
                    .unwrap(),
            );
            let ctx = SessionContext::new_with_config_rt(config, runtime_env);
            let task_ctx = ctx.task_ctx();
            let stream = exec.execute(0, task_ctx).unwrap();
            let rt = Runtime::new().unwrap();
            rt.block_on(collect(stream)).unwrap();
        }

        // Run 2: with spilling (very small memory limit to force many spills
        // with small batches that exercise the coalescer flush path)
        {
            let partitions = std::slice::from_ref(&batches);
            let exec = ShuffleWriterExec::try_new(
                Arc::new(DataSourceExec::new(Arc::new(
                    MemorySourceConfig::try_new(partitions, batch.schema(), None).unwrap(),
                ))),
                CometPartitioning::Hash(vec![Arc::new(Column::new("a", 0))], num_partitions),
                CompressionCodec::Zstd(1),
                "/tmp/size_spill_data.out".to_string(),
                "/tmp/size_spill_index.out".to_string(),
                false,
                1024 * 1024,
            )
            .unwrap();

            let config = SessionConfig::new();
            let runtime_env = Arc::new(
                RuntimeEnvBuilder::new()
                    .with_memory_limit(256 * 1024, 1.0)
                    .build()
                    .unwrap(),
            );
            let ctx = SessionContext::new_with_config_rt(config, runtime_env);
            let task_ctx = ctx.task_ctx();
            let stream = exec.execute(0, task_ctx).unwrap();
            let rt = Runtime::new().unwrap();
            rt.block_on(collect(stream)).unwrap();
        }

        let no_spill_data_size = fs::metadata("/tmp/size_no_spill_data.out").unwrap().len();
        let spill_data_size = fs::metadata("/tmp/size_spill_data.out").unwrap().len();

        // The spill output may differ slightly due to batch coalescing boundaries
        // affecting compression ratios, but it must be within a reasonable range.
        // A data-loss bug would produce a file that is drastically smaller (e.g. <10%).
        let ratio = spill_data_size as f64 / no_spill_data_size as f64;
        assert!(
            ratio > 0.5 && ratio < 2.0,
            "Spill output size ({spill_data_size}) should be comparable to \
             non-spill output size ({no_spill_data_size}), ratio={ratio:.3}. \
             A very small ratio indicates data loss in spill byte range tracking."
        );

        // Also verify row counts match
        let parse_offsets = |index_data: &[u8]| -> Vec<i64> {
            index_data
                .chunks(8)
                .map(|chunk| i64::from_le_bytes(chunk.try_into().unwrap()))
                .collect()
        };

        let no_spill_index = fs::read("/tmp/size_no_spill_index.out").unwrap();
        let spill_index = fs::read("/tmp/size_spill_index.out").unwrap();
        let no_spill_offsets = parse_offsets(&no_spill_index);
        let spill_offsets = parse_offsets(&spill_index);
        let no_spill_data = fs::read("/tmp/size_no_spill_data.out").unwrap();
        let spill_data = fs::read("/tmp/size_spill_data.out").unwrap();

        let total_rows = batch_size * num_batches;
        let mut ns_total = 0;
        let mut s_total = 0;
        for i in 0..num_partitions {
            let ns_rows = read_all_ipc_blocks(
                &no_spill_data[no_spill_offsets[i] as usize..no_spill_offsets[i + 1] as usize],
            );
            let s_rows = read_all_ipc_blocks(
                &spill_data[spill_offsets[i] as usize..spill_offsets[i + 1] as usize],
            );
            assert_eq!(
                ns_rows, s_rows,
                "Partition {i} row count mismatch: no_spill={ns_rows}, spill={s_rows}"
            );
            ns_total += ns_rows;
            s_total += s_rows;
        }
        assert_eq!(ns_total, total_rows, "Non-spill total row count mismatch");
        assert_eq!(s_total, total_rows, "Spill total row count mismatch");

        // Cleanup
        let _ = fs::remove_file("/tmp/size_no_spill_data.out");
        let _ = fs::remove_file("/tmp/size_no_spill_index.out");
        let _ = fs::remove_file("/tmp/size_spill_data.out");
        let _ = fs::remove_file("/tmp/size_spill_index.out");
    }

    /// Verify multiple spill events with subsequent insert_batch calls
    /// produce correct output.
    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn test_multiple_spills_then_write() {
        let batch = create_batch(500);
        let memory_limit = 512 * 1024;
        let num_partitions = 4;
        let runtime_env = create_runtime(memory_limit);
        let metrics_set = ExecutionPlanMetricsSet::new();
        let mut repartitioner = MultiPartitionShuffleRepartitioner::try_new(
            0,
            "/tmp/multi_spill_data.out".to_string(),
            "/tmp/multi_spill_index.out".to_string(),
            batch.schema(),
            CometPartitioning::Hash(vec![Arc::new(Column::new("a", 0))], num_partitions),
            ShufflePartitionerMetrics::new(&metrics_set, 0),
            runtime_env,
            1024,
            CompressionCodec::Lz4Frame,
            false,
            1024 * 1024,
        )
        .unwrap();

        // Insert -> spill -> insert -> spill -> insert (3 inserts, 2 spills)
        repartitioner.insert_batch(batch.clone()).await.unwrap();
        repartitioner.spill().unwrap();
        assert_eq!(repartitioner.spill_count_files(), 1);

        repartitioner.insert_batch(batch.clone()).await.unwrap();
        repartitioner.spill().unwrap();
        assert_eq!(repartitioner.spill_count_files(), 2);

        repartitioner.insert_batch(batch.clone()).await.unwrap();
        // Final shuffle_write merges 2 spill files + in-memory data
        repartitioner.shuffle_write().unwrap();

        // Verify output files exist and are non-empty
        let data = std::fs::read("/tmp/multi_spill_data.out").unwrap();
        assert!(!data.is_empty(), "Output data file should be non-empty");

        let index = std::fs::read("/tmp/multi_spill_index.out").unwrap();
        // Index should have (num_partitions + 1) * 8 bytes
        assert_eq!(
            index.len(),
            (num_partitions + 1) * 8,
            "Index file should have correct number of offset entries"
        );

        // Parse offsets and verify they are monotonically non-decreasing
        let offsets: Vec<i64> = index
            .chunks(8)
            .map(|chunk| i64::from_le_bytes(chunk.try_into().unwrap()))
            .collect();
        assert_eq!(offsets[0], 0, "First offset should be 0");
        for i in 1..offsets.len() {
            assert!(
                offsets[i] >= offsets[i - 1],
                "Offsets must be monotonically non-decreasing: offset[{}]={} < offset[{}]={}",
                i,
                offsets[i],
                i - 1,
                offsets[i - 1]
            );
        }
        assert_eq!(
            *offsets.last().unwrap() as usize,
            data.len(),
            "Last offset should equal data file size"
        );

        // Cleanup
        let _ = std::fs::remove_file("/tmp/multi_spill_data.out");
        let _ = std::fs::remove_file("/tmp/multi_spill_index.out");
    }
}
