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

use crate::execution::shuffle::metrics::ShufflePartitionerMetrics;
use crate::execution::shuffle::partitioners::{
    MultiPartitionShuffleRepartitioner, ShufflePartitioner, SinglePartitionShufflePartitioner,
};
use crate::execution::shuffle::{CometPartitioning, CompressionCodec};
use crate::execution::tracing::with_trace_async;
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
        Statistics,
    },
};
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
    cache: PlanProperties,
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
        let cache = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&input.schema())),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );

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

    fn statistics(&self) -> Result<Statistics> {
        self.input.partition_statistics(None)
    }

    fn properties(&self) -> &PlanProperties {
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
    use crate::execution::shuffle::{read_ipc_compressed, ShuffleBlockWriter};
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

        {
            let partition_writers = repartitioner.partition_writers();
            assert_eq!(partition_writers.len(), 2);

            assert!(!partition_writers[0].has_spill_file());
            assert!(!partition_writers[1].has_spill_file());
        }

        repartitioner.spill().unwrap();

        // after spill, there should be spill files
        {
            let partition_writers = repartitioner.partition_writers();
            assert!(partition_writers[0].has_spill_file());
            assert!(partition_writers[1].has_spill_file());
        }

        // insert another batch after spilling
        repartitioner.insert_batch(batch.clone()).await.unwrap();
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
        use crate::execution::shuffle::writers::BufBatchWriter;
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
}
