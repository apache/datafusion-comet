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

use crate::execution::shuffle::range_partitioner::RangePartitioner;
use crate::execution::shuffle::{CometPartitioning, CompressionCodec, ShuffleBlockWriter};
use crate::execution::tracing::{with_trace, with_trace_async};
use arrow::compute::interleave_record_batch;
use arrow::row::{OwnedRow, RowConverter};
use async_trait::async_trait;
use datafusion::common::utils::proxy::VecAllocExt;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::EmptyRecordBatchStream;
use datafusion::{
    arrow::{array::*, datatypes::SchemaRef, error::ArrowError, record_batch::RecordBatch},
    error::{DataFusionError, Result},
    execution::{
        context::TaskContext,
        disk_manager::RefCountedTempFile,
        memory_pool::{MemoryConsumer, MemoryReservation},
        runtime_env::RuntimeEnv,
    },
    physical_plan::{
        metrics::{
            BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet, Time,
        },
        stream::RecordBatchStreamAdapter,
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
        Statistics,
    },
};
use datafusion_comet_spark_expr::hash_funcs::murmur3::create_murmur3_hashes;
use futures::executor::block_on;
use futures::{StreamExt, TryFutureExt, TryStreamExt};
use itertools::Itertools;
use std::borrow::Borrow;
use std::io::{Cursor, Error, SeekFrom};
use std::{
    any::Any,
    fmt,
    fmt::{Debug, Formatter},
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Seek, Write},
    sync::Arc,
};
use tokio::time::Instant;

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
}

impl ShuffleWriterExec {
    /// Create a new ShuffleWriterExec
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        partitioning: CometPartitioning,
        codec: CompressionCodec,
        output_data_file: String,
        output_index_file: String,
        tracing_enabled: bool,
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
        let metrics = ShuffleRepartitionerMetrics::new(&self.metrics, 0);

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
    metrics: ShuffleRepartitionerMetrics,
    context: Arc<TaskContext>,
    codec: CompressionCodec,
    tracing_enabled: bool,
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
            )?),
        };

        while let Some(batch) = input.next().await {
            // Block on the repartitioner to insert the batch and shuffle the rows
            // into the corresponding partition buffer.
            // Otherwise, pull the next batch from the input stream might overwrite the
            // current batch in the repartitioner.
            block_on(repartitioner.insert_batch(batch?))?;
        }

        repartitioner.shuffle_write()?;

        // shuffle writer always has empty output
        Ok(Box::pin(EmptyRecordBatchStream::new(Arc::clone(&schema))) as SendableRecordBatchStream)
    })
    .await
}

struct ShuffleRepartitionerMetrics {
    /// metrics
    baseline: BaselineMetrics,

    /// Time to perform repartitioning
    repart_time: Time,

    /// Time interacting with memory pool
    mempool_time: Time,

    /// Time encoding batches to IPC format
    encode_time: Time,

    /// Time spent writing to disk. Maps to "shuffleWriteTime" in Spark SQL Metrics.
    write_time: Time,

    /// Number of input batches
    input_batches: Count,

    /// count of spills during the execution of the operator
    spill_count: Count,

    /// total spilled bytes during the execution of the operator
    spilled_bytes: Count,

    /// The original size of spilled data. Different to `spilled_bytes` because of compression.
    data_size: Count,
}

impl ShuffleRepartitionerMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            baseline: BaselineMetrics::new(metrics, partition),
            repart_time: MetricBuilder::new(metrics).subset_time("repart_time", partition),
            mempool_time: MetricBuilder::new(metrics).subset_time("mempool_time", partition),
            encode_time: MetricBuilder::new(metrics).subset_time("encode_time", partition),
            write_time: MetricBuilder::new(metrics).subset_time("write_time", partition),
            input_batches: MetricBuilder::new(metrics).counter("input_batches", partition),
            spill_count: MetricBuilder::new(metrics).spill_count(partition),
            spilled_bytes: MetricBuilder::new(metrics).spilled_bytes(partition),
            data_size: MetricBuilder::new(metrics).counter("data_size", partition),
        }
    }
}

#[async_trait::async_trait]
trait ShufflePartitioner: Send + Sync {
    /// Insert a batch into the partitioner
    async fn insert_batch(&mut self, batch: RecordBatch) -> Result<()>;
    /// Write shuffle data and shuffle index file to disk
    fn shuffle_write(&mut self) -> Result<()>;
}

/// A partitioner that uses a hash function to partition data into multiple partitions
struct MultiPartitionShuffleRepartitioner {
    output_data_file: String,
    output_index_file: String,
    buffered_batches: Vec<RecordBatch>,
    partition_indices: Vec<Vec<(u32, u32)>>,
    partition_writers: Vec<PartitionWriter>,
    shuffle_block_writer: ShuffleBlockWriter,
    /// Partitioning scheme to use
    partitioning: CometPartitioning,
    runtime: Arc<RuntimeEnv>,
    metrics: ShuffleRepartitionerMetrics,
    /// Reused scratch space for computing partition indices
    scratch: ScratchSpace,
    /// The configured batch size
    batch_size: usize,
    /// Reservation for repartitioning
    reservation: MemoryReservation,
    tracing_enabled: bool,
    /// RangePartitioning-specific state
    bounds_rows: Option<Vec<OwnedRow>>,
    row_converter: Option<RowConverter>,
    seed: u64,
}

#[derive(Default)]
struct ScratchSpace {
    /// Hashes for each row in the current batch.
    hashes_buf: Vec<u32>,
    /// Partition ids for each row in the current batch.
    partition_ids: Vec<u32>,
    /// The row indices of the rows in each partition. This array is conceptually divided into
    /// partitions, where each partition contains the row indices of the rows in that partition.
    /// The length of this array is the same as the number of rows in the batch.
    partition_row_indices: Vec<u32>,
    /// The start indices of partitions in partition_row_indices. partition_starts[K] and
    /// partition_starts[K + 1] are the start and end indices of partition K in partition_row_indices.
    /// The length of this array is 1 + the number of partitions.
    partition_starts: Vec<u32>,
}

impl MultiPartitionShuffleRepartitioner {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        partition: usize,
        output_data_file: String,
        output_index_file: String,
        schema: SchemaRef,
        partitioning: CometPartitioning,
        metrics: ShuffleRepartitionerMetrics,
        runtime: Arc<RuntimeEnv>,
        batch_size: usize,
        codec: CompressionCodec,
        tracing_enabled: bool,
    ) -> Result<Self> {
        let num_output_partitions = partitioning.partition_count();
        assert_ne!(
            num_output_partitions, 1,
            "Use SinglePartitionShufflePartitioner for 1 output partition."
        );

        // Vectors in the scratch space will be filled with valid values before being used, this
        // initialization code is simply initializing the vectors to the desired size.
        // The initial values are not used.
        let scratch = ScratchSpace {
            hashes_buf: match partitioning {
                // Only allocate the hashes_buf if hash partitioning.
                CometPartitioning::Hash(_, _) => vec![0; batch_size],
                _ => vec![],
            },
            partition_ids: vec![0; batch_size],
            partition_row_indices: vec![0; batch_size],
            partition_starts: vec![0; num_output_partitions + 1],
        };

        let shuffle_block_writer = ShuffleBlockWriter::try_new(schema.as_ref(), codec.clone())?;

        let partition_writers = (0..num_output_partitions)
            .map(|_| PartitionWriter::try_new(shuffle_block_writer.clone()))
            .collect::<Result<Vec<_>>>()?;

        let reservation = MemoryConsumer::new(format!("ShuffleRepartitioner[{partition}]"))
            .with_can_spill(true)
            .register(&runtime.memory_pool);

        Ok(Self {
            output_data_file,
            output_index_file,
            buffered_batches: vec![],
            partition_indices: vec![vec![]; num_output_partitions],
            partition_writers,
            shuffle_block_writer,
            partitioning,
            runtime,
            metrics,
            scratch,
            batch_size,
            reservation,
            tracing_enabled,
            bounds_rows: None,
            row_converter: None,
            // Spark RangePartitioner seeds off of partition number.
            seed: partition as u64,
        })
    }

    /// Shuffles rows in input batch into corresponding partition buffer.
    /// This function first calculates hashes for rows and then takes rows in same
    /// partition as a record batch which is appended into partition buffer.
    /// This should not be called directly. Use `insert_batch` instead.
    async fn partitioning_batch(&mut self, input: RecordBatch) -> Result<()> {
        if input.num_rows() == 0 {
            // skip empty batch
            return Ok(());
        }

        fn map_partition_ids_to_starts_and_indices(
            scratch: &mut ScratchSpace,
            num_output_partitions: usize,
            num_rows: usize,
        ) {
            let partition_ids = &mut scratch.partition_ids[..num_rows];

            // count each partition size, while leaving the last extra element as 0
            let partition_counters = &mut scratch.partition_starts;
            partition_counters.resize(num_output_partitions + 1, 0);
            partition_counters.fill(0);
            partition_ids
                .iter()
                .for_each(|partition_id| partition_counters[*partition_id as usize] += 1);

            // accumulate partition counters into partition ends
            // e.g. partition counter: [1, 3, 2, 1, 0] => [1, 4, 6, 7, 7]
            let partition_ends = partition_counters;
            let mut accum = 0;
            partition_ends.iter_mut().for_each(|v| {
                *v += accum;
                accum = *v;
            });

            // calculate partition row indices and partition starts
            // e.g. partition ids: [3, 1, 1, 1, 2, 2, 0] will produce the following partition_row_indices
            // and partition_starts arrays:
            //
            //  partition_row_indices: [6, 1, 2, 3, 4, 5, 0]
            //  partition_starts: [0, 1, 4, 6, 7]
            //
            // partition_starts conceptually splits partition_row_indices into smaller slices.
            // Each slice partition_row_indices[partition_starts[K]..partition_starts[K + 1]] contains the
            // row indices of the input batch that are partitioned into partition K. For example,
            // first partition 0 has one row index [6], partition 1 has row indices [1, 2, 3], etc.
            let partition_row_indices = &mut scratch.partition_row_indices;
            partition_row_indices.resize(num_rows, 0);
            for (index, partition_id) in partition_ids.iter().enumerate().rev() {
                partition_ends[*partition_id as usize] -= 1;
                let end = partition_ends[*partition_id as usize];
                partition_row_indices[end as usize] = index as u32;
            }

            // after calculating, partition ends become partition starts
        }

        if input.num_rows() > self.batch_size {
            return Err(DataFusionError::Internal(
                "Input batch size exceeds configured batch size. Call `insert_batch` instead."
                    .to_string(),
            ));
        }

        // Update data size metric
        self.metrics.data_size.add(input.get_array_memory_size());

        // NOTE: in shuffle writer exec, the output_rows metrics represents the
        // number of rows those are written to output data file.
        self.metrics.baseline.record_output(input.num_rows());

        match &self.partitioning {
            CometPartitioning::Hash(exprs, num_output_partitions) => {
                let mut scratch = std::mem::take(&mut self.scratch);
                let (partition_starts, partition_row_indices): (&Vec<u32>, &Vec<u32>) = {
                    let mut timer = self.metrics.repart_time.timer();

                    // Evaluate partition expressions to get rows to apply partitioning scheme.
                    let arrays = exprs
                        .iter()
                        .map(|expr| expr.evaluate(&input)?.into_array(input.num_rows()))
                        .collect::<Result<Vec<_>>>()?;

                    let num_rows = arrays[0].len();

                    // Use identical seed as Spark hash partitioning.
                    let hashes_buf = &mut scratch.hashes_buf[..num_rows];
                    hashes_buf.fill(42_u32);

                    // Generate partition ids for every row.
                    {
                        // Hash arrays and compute partition ids based on number of partitions.
                        let partition_ids = &mut scratch.partition_ids[..num_rows];
                        create_murmur3_hashes(&arrays, hashes_buf)?
                            .iter()
                            .enumerate()
                            .for_each(|(idx, hash)| {
                                partition_ids[idx] = pmod(*hash, *num_output_partitions) as u32;
                            });
                    }

                    // We now have partition ids for every input row, map that to partition starts
                    // and partition indices to eventually right these rows to partition buffers.
                    map_partition_ids_to_starts_and_indices(
                        &mut scratch,
                        *num_output_partitions,
                        num_rows,
                    );

                    timer.stop();
                    Ok::<(&Vec<u32>, &Vec<u32>), DataFusionError>((
                        &scratch.partition_starts,
                        &scratch.partition_row_indices,
                    ))
                }?;

                self.buffer_partitioned_batch_may_spill(
                    input,
                    partition_row_indices,
                    partition_starts,
                )
                .await?;
                self.scratch = scratch;
            }
            CometPartitioning::RangePartitioning(
                lex_ordering,
                num_output_partitions,
                sample_size,
            ) => {
                let mut scratch = std::mem::take(&mut self.scratch);
                let (partition_starts, partition_row_indices): (&Vec<u32>, &Vec<u32>) = {
                    let mut timer = self.metrics.repart_time.timer();

                    // Evaluate partition expressions for values to apply partitioning scheme on.
                    let arrays = lex_ordering
                        .iter()
                        .map(|expr| expr.expr.evaluate(&input)?.into_array(input.num_rows()))
                        .collect::<Result<Vec<_>>>()?;

                    let num_rows = arrays[0].len();

                    // If necessary (i.e., when first batch arrives) generate the bounds (as Rows)
                    // for range partitioning based on randomly reservoir sampling the batch.
                    if self.row_converter.is_none() {
                        let (bounds_rows, row_converter) = RangePartitioner::generate_bounds(
                            &arrays,
                            lex_ordering,
                            *num_output_partitions,
                            input.num_rows(),
                            *sample_size,
                            self.seed,
                        )?;

                        self.bounds_rows =
                            Some(bounds_rows.iter().map(|row| row.owned()).collect_vec());
                        self.row_converter = Some(row_converter);
                    }

                    // Generate partition ids for every row, first by converting the partition
                    // arrays to Rows, and then doing binary search for each Row against the
                    // bounds Rows.
                    let row_batch = self
                        .row_converter
                        .as_ref()
                        .unwrap()
                        .convert_columns(arrays.as_slice())?;

                    RangePartitioner::partition_indices_for_batch(
                        &row_batch,
                        self.bounds_rows.as_ref().unwrap().as_slice(),
                        &mut scratch.partition_ids[..num_rows],
                    );

                    // We now have partition ids for every input row, map that to partition starts
                    // and partition indices to eventually right these rows to partition buffers.
                    map_partition_ids_to_starts_and_indices(
                        &mut scratch,
                        *num_output_partitions,
                        num_rows,
                    );

                    timer.stop();
                    Ok::<(&Vec<u32>, &Vec<u32>), DataFusionError>((
                        &scratch.partition_starts,
                        &scratch.partition_row_indices,
                    ))
                }?;

                self.buffer_partitioned_batch_may_spill(
                    input,
                    partition_row_indices,
                    partition_starts,
                )
                .await?;
                self.scratch = scratch;
            }
            other => {
                // this should be unreachable as long as the validation logic
                // in the constructor is kept up-to-date
                return Err(DataFusionError::NotImplemented(format!(
                    "Unsupported shuffle partitioning scheme {other:?}"
                )));
            }
        }
        Ok(())
    }

    async fn buffer_partitioned_batch_may_spill(
        &mut self,
        input: RecordBatch,
        partition_row_indices: &[u32],
        partition_starts: &[u32],
    ) -> Result<()> {
        let mut mem_growth: usize = input.get_array_memory_size();
        let buffered_partition_idx = self.buffered_batches.len() as u32;
        self.buffered_batches.push(input);

        // partition_starts conceptually slices partition_row_indices into smaller slices,
        // each slice contains the indices of rows in input that will go into the corresponding
        // partition. The following loop iterates over the slices and put the row indices into
        // the indices array of the corresponding partition.
        for (partition_id, (&start, &end)) in partition_starts
            .iter()
            .tuple_windows()
            .enumerate()
            .filter(|(_, (start, end))| start < end)
        {
            let row_indices = &partition_row_indices[start as usize..end as usize];

            // Put row indices for the current partition into the indices array of that partition.
            // This indices array will be used for calling interleave_record_batch to produce
            // shuffled batches.
            let indices = &mut self.partition_indices[partition_id];
            let before_size = indices.allocated_size();
            indices.reserve(row_indices.len());
            for row_idx in row_indices {
                indices.push((buffered_partition_idx, *row_idx));
            }
            let after_size = indices.allocated_size();
            mem_growth += after_size.saturating_sub(before_size);
        }

        let grow_result = {
            let mut timer = self.metrics.mempool_time.timer();
            let result = self.reservation.try_grow(mem_growth);
            timer.stop();
            result
        };
        if grow_result.is_err() {
            self.spill()?;
        }

        Ok(())
    }

    fn shuffle_write_partition(
        partition_iter: &mut PartitionedBatchIterator,
        shuffle_block_writer: &mut ShuffleBlockWriter,
        output_data: &mut BufWriter<File>,
        encode_time: &Time,
        write_time: &Time,
    ) -> Result<()> {
        let mut buf_batch_writer = BufBatchWriter::new(shuffle_block_writer, output_data);
        for batch in partition_iter {
            let batch = batch?;
            buf_batch_writer.write(&batch, encode_time, write_time)?;
        }
        buf_batch_writer.flush(write_time)?;
        Ok(())
    }

    fn used(&self) -> usize {
        self.reservation.size()
    }

    fn spilled_bytes(&self) -> usize {
        self.metrics.spilled_bytes.value()
    }

    fn spill_count(&self) -> usize {
        self.metrics.spill_count.value()
    }

    fn data_size(&self) -> usize {
        self.metrics.data_size.value()
    }

    /// This function transfers the ownership of the buffered batches and partition indices from the
    /// ShuffleRepartitioner to a new PartitionedBatches struct. The returned PartitionedBatches struct
    /// can be used to produce shuffled batches.
    fn partitioned_batches(&mut self) -> PartitionedBatchesProducer {
        let num_output_partitions = self.partition_indices.len();
        let buffered_batches = std::mem::take(&mut self.buffered_batches);
        // let indices = std::mem::take(&mut self.partition_indices);
        let indices = std::mem::replace(
            &mut self.partition_indices,
            vec![vec![]; num_output_partitions],
        );
        PartitionedBatchesProducer::new(buffered_batches, indices, self.batch_size)
    }

    fn spill(&mut self) -> Result<()> {
        log::debug!(
            "ShuffleRepartitioner spilling shuffle data of {} to disk while inserting ({} time(s) so far)",
            self.used(),
            self.spill_count()
        );

        // we could always get a chance to free some memory as long as we are holding some
        if self.buffered_batches.is_empty() {
            return Ok(());
        }

        with_trace("shuffle_spill", self.tracing_enabled, || {
            let num_output_partitions = self.partition_writers.len();
            let mut partitioned_batches = self.partitioned_batches();
            let mut spilled_bytes = 0;

            for partition_id in 0..num_output_partitions {
                let partition_writer = &mut self.partition_writers[partition_id];
                let mut iter = partitioned_batches.produce(partition_id);
                spilled_bytes += partition_writer.spill(&mut iter, &self.runtime, &self.metrics)?;
            }

            let mut timer = self.metrics.mempool_time.timer();
            self.reservation.free();
            timer.stop();
            self.metrics.spill_count.add(1);
            self.metrics.spilled_bytes.add(spilled_bytes);
            Ok(())
        })
    }
}

#[async_trait::async_trait]
impl ShufflePartitioner for MultiPartitionShuffleRepartitioner {
    /// Shuffles rows in input batch into corresponding partition buffer.
    /// This function will slice input batch according to configured batch size and then
    /// shuffle rows into corresponding partition buffer.
    async fn insert_batch(&mut self, batch: RecordBatch) -> Result<()> {
        with_trace_async("shuffle_insert_batch", self.tracing_enabled, || async {
            let start_time = Instant::now();
            let mut start = 0;
            while start < batch.num_rows() {
                let end = (start + self.batch_size).min(batch.num_rows());
                let batch = batch.slice(start, end - start);
                self.partitioning_batch(batch).await?;
                start = end;
            }
            self.metrics.input_batches.add(1);
            self.metrics
                .baseline
                .elapsed_compute()
                .add_duration(start_time.elapsed());
            Ok(())
        })
        .await
    }

    /// Writes buffered shuffled record batches into Arrow IPC bytes.
    fn shuffle_write(&mut self) -> Result<()> {
        with_trace("shuffle_write", self.tracing_enabled, || {
            let start_time = Instant::now();

            let mut partitioned_batches = self.partitioned_batches();
            let num_output_partitions = self.partition_indices.len();
            let mut offsets = vec![0; num_output_partitions + 1];

            let data_file = self.output_data_file.clone();
            let index_file = self.output_index_file.clone();

            let output_data = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(data_file)
                .map_err(|e| DataFusionError::Execution(format!("shuffle write error: {e:?}")))?;

            let mut output_data = BufWriter::new(output_data);

            #[allow(clippy::needless_range_loop)]
            for i in 0..num_output_partitions {
                offsets[i] = output_data.stream_position()?;

                // if we wrote a spill file for this partition then copy the
                // contents into the shuffle file
                if let Some(spill_data) = self.partition_writers[i].spill_file.as_ref() {
                    let mut spill_file =
                        BufReader::new(File::open(spill_data.temp_file.path()).map_err(to_df_err)?);
                    let mut write_timer = self.metrics.write_time.timer();
                    std::io::copy(&mut spill_file, &mut output_data).map_err(to_df_err)?;
                    write_timer.stop();
                }

                // Write in memory batches to output data file
                let mut partition_iter = partitioned_batches.produce(i);
                Self::shuffle_write_partition(
                    &mut partition_iter,
                    &mut self.shuffle_block_writer,
                    &mut output_data,
                    &self.metrics.encode_time,
                    &self.metrics.write_time,
                )?;
            }

            let mut write_timer = self.metrics.write_time.timer();
            output_data.flush()?;
            write_timer.stop();

            // add one extra offset at last to ease partition length computation
            offsets[num_output_partitions] = output_data.stream_position().map_err(to_df_err)?;

            let mut write_timer = self.metrics.write_time.timer();
            let mut output_index =
                BufWriter::new(File::create(index_file).map_err(|e| {
                    DataFusionError::Execution(format!("shuffle write error: {e:?}"))
                })?);
            for offset in offsets {
                output_index
                    .write_all(&(offset as i64).to_le_bytes()[..])
                    .map_err(to_df_err)?;
            }
            output_index.flush()?;
            write_timer.stop();

            self.metrics
                .baseline
                .elapsed_compute()
                .add_duration(start_time.elapsed());

            Ok(())
        })
    }
}

impl Debug for MultiPartitionShuffleRepartitioner {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShuffleRepartitioner")
            .field("memory_used", &self.used())
            .field("spilled_bytes", &self.spilled_bytes())
            .field("spilled_count", &self.spill_count())
            .field("data_size", &self.data_size())
            .finish()
    }
}

/// A partitioner that writes all shuffle data to a single file and a single index file
struct SinglePartitionShufflePartitioner {
    // output_data_file: File,
    output_data_writer: BufBatchWriter<ShuffleBlockWriter, File>,
    output_index_path: String,
    /// Batches that are smaller than the batch size and to be concatenated
    buffered_batches: Vec<RecordBatch>,
    /// Number of rows in the concatenating batches
    num_buffered_rows: usize,
    /// Metrics for the repartitioner
    metrics: ShuffleRepartitionerMetrics,
    /// The configured batch size
    batch_size: usize,
}

impl SinglePartitionShufflePartitioner {
    fn try_new(
        output_data_path: String,
        output_index_path: String,
        schema: SchemaRef,
        metrics: ShuffleRepartitionerMetrics,
        batch_size: usize,
        codec: CompressionCodec,
    ) -> Result<Self> {
        let shuffle_block_writer = ShuffleBlockWriter::try_new(schema.as_ref(), codec.clone())?;

        let output_data_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(output_data_path)
            .map_err(to_df_err)?;

        let output_data_writer = BufBatchWriter::new(shuffle_block_writer, output_data_file);

        Ok(Self {
            output_data_writer,
            output_index_path,
            buffered_batches: vec![],
            num_buffered_rows: 0,
            metrics,
            batch_size,
        })
    }

    /// Add a batch to the buffer of the partitioner, these buffered batches will be concatenated
    /// and written to the output data file when the number of rows in the buffer reaches the batch size.
    fn add_buffered_batch(&mut self, batch: RecordBatch) {
        self.num_buffered_rows += batch.num_rows();
        self.buffered_batches.push(batch);
    }

    /// Consumes buffered batches and return a concatenated batch if successful
    fn concat_buffered_batches(&mut self) -> Result<Option<RecordBatch>> {
        if self.buffered_batches.is_empty() {
            Ok(None)
        } else if self.buffered_batches.len() == 1 {
            let batch = self.buffered_batches.remove(0);
            self.num_buffered_rows = 0;
            Ok(Some(batch))
        } else {
            let schema = &self.buffered_batches[0].schema();
            match arrow::compute::concat_batches(schema, self.buffered_batches.iter()) {
                Ok(concatenated) => {
                    self.buffered_batches.clear();
                    self.num_buffered_rows = 0;
                    Ok(Some(concatenated))
                }
                Err(e) => Err(DataFusionError::ArrowError(
                    Box::new(e),
                    Some(DataFusionError::get_back_trace()),
                )),
            }
        }
    }
}

#[async_trait::async_trait]
impl ShufflePartitioner for SinglePartitionShufflePartitioner {
    async fn insert_batch(&mut self, batch: RecordBatch) -> Result<()> {
        let start_time = Instant::now();
        let num_rows = batch.num_rows();

        if num_rows > 0 {
            self.metrics.data_size.add(batch.get_array_memory_size());
            self.metrics.baseline.record_output(num_rows);

            if num_rows >= self.batch_size || num_rows + self.num_buffered_rows > self.batch_size {
                let concatenated_batch = self.concat_buffered_batches()?;

                let write_start_time = Instant::now();

                // Write the concatenated buffered batch
                if let Some(batch) = concatenated_batch {
                    self.output_data_writer.write(
                        &batch,
                        &self.metrics.encode_time,
                        &self.metrics.write_time,
                    )?;
                }

                if num_rows >= self.batch_size {
                    // Write the new batch
                    self.output_data_writer.write(
                        &batch,
                        &self.metrics.encode_time,
                        &self.metrics.write_time,
                    )?;
                } else {
                    // Add the new batch to the buffer
                    self.add_buffered_batch(batch);
                }

                self.metrics
                    .write_time
                    .add_duration(write_start_time.elapsed());
            } else {
                self.add_buffered_batch(batch);
            }
        }

        self.metrics.input_batches.add(1);
        self.metrics
            .baseline
            .elapsed_compute()
            .add_duration(start_time.elapsed());
        Ok(())
    }

    fn shuffle_write(&mut self) -> Result<()> {
        let start_time = Instant::now();
        let concatenated_batch = self.concat_buffered_batches()?;

        // Write the concatenated buffered batch
        if let Some(batch) = concatenated_batch {
            self.output_data_writer.write(
                &batch,
                &self.metrics.encode_time,
                &self.metrics.write_time,
            )?;
        }
        self.output_data_writer.flush(&self.metrics.write_time)?;

        // Write index file. It should only contain 2 entries: 0 and the total number of bytes written
        let mut index_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(self.output_index_path.clone())
            .map_err(|e| DataFusionError::Execution(format!("shuffle write error: {e:?}")))?;
        let data_file_length = self
            .output_data_writer
            .writer
            .stream_position()
            .map_err(to_df_err)?;
        for offset in [0, data_file_length] {
            index_file
                .write_all(&(offset as i64).to_le_bytes()[..])
                .map_err(to_df_err)?;
        }
        index_file.flush()?;

        self.metrics
            .baseline
            .elapsed_compute()
            .add_duration(start_time.elapsed());
        Ok(())
    }
}

fn to_df_err(e: Error) -> DataFusionError {
    DataFusionError::Execution(format!("shuffle write error: {e:?}"))
}

/// A helper struct to produce shuffled batches.
/// This struct takes ownership of the buffered batches and partition indices from the
/// ShuffleRepartitioner, and provides an iterator over the batches in the specified partitions.
struct PartitionedBatchesProducer {
    buffered_batches: Vec<RecordBatch>,
    partition_indices: Vec<Vec<(u32, u32)>>,
    batch_size: usize,
}

impl PartitionedBatchesProducer {
    fn new(
        buffered_batches: Vec<RecordBatch>,
        indices: Vec<Vec<(u32, u32)>>,
        batch_size: usize,
    ) -> Self {
        Self {
            partition_indices: indices,
            buffered_batches,
            batch_size,
        }
    }

    fn produce(&mut self, partition_id: usize) -> PartitionedBatchIterator {
        PartitionedBatchIterator::new(
            &self.partition_indices[partition_id],
            &self.buffered_batches,
            self.batch_size,
        )
    }
}

struct PartitionedBatchIterator<'a> {
    record_batches: Vec<&'a RecordBatch>,
    batch_size: usize,
    indices: Vec<(usize, usize)>,
    pos: usize,
}

impl<'a> PartitionedBatchIterator<'a> {
    fn new(
        indices: &'a [(u32, u32)],
        buffered_batches: &'a [RecordBatch],
        batch_size: usize,
    ) -> Self {
        if indices.is_empty() {
            // Avoid unnecessary allocations when the partition is empty
            return Self {
                record_batches: vec![],
                batch_size,
                indices: vec![],
                pos: 0,
            };
        }
        let record_batches = buffered_batches.iter().collect::<Vec<_>>();
        let current_indices = indices
            .iter()
            .map(|(i_batch, i_row)| (*i_batch as usize, *i_row as usize))
            .collect::<Vec<_>>();
        Self {
            record_batches,
            batch_size,
            indices: current_indices,
            pos: 0,
        }
    }
}

impl Iterator for PartitionedBatchIterator<'_> {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.indices.len() {
            return None;
        }

        let indices_end = std::cmp::min(self.pos + self.batch_size, self.indices.len());
        let indices = &self.indices[self.pos..indices_end];
        match interleave_record_batch(&self.record_batches, indices) {
            Ok(batch) => {
                self.pos = indices_end;
                Some(Ok(batch))
            }
            Err(e) => Some(Err(DataFusionError::ArrowError(
                Box::new(e),
                Some(DataFusionError::get_back_trace()),
            ))),
        }
    }
}

struct PartitionWriter {
    /// Spill file for intermediate shuffle output for this partition. Each spill event
    /// will append to this file and the contents will be copied to the shuffle file at
    /// the end of processing.
    spill_file: Option<SpillFile>,
    /// Writer that performs encoding and compression
    shuffle_block_writer: ShuffleBlockWriter,
}

struct SpillFile {
    temp_file: RefCountedTempFile,
    file: File,
}

impl PartitionWriter {
    fn try_new(shuffle_block_writer: ShuffleBlockWriter) -> Result<Self> {
        Ok(Self {
            spill_file: None,
            shuffle_block_writer,
        })
    }

    fn spill(
        &mut self,
        iter: &mut PartitionedBatchIterator,
        runtime: &RuntimeEnv,
        metrics: &ShuffleRepartitionerMetrics,
    ) -> Result<usize> {
        if let Some(batch) = iter.next() {
            self.ensure_spill_file_created(runtime)?;

            let total_bytes_written = {
                let mut buf_batch_writer = BufBatchWriter::new(
                    &mut self.shuffle_block_writer,
                    &mut self.spill_file.as_mut().unwrap().file,
                );
                let mut bytes_written =
                    buf_batch_writer.write(&batch?, &metrics.encode_time, &metrics.write_time)?;
                for batch in iter {
                    let batch = batch?;
                    bytes_written += buf_batch_writer.write(
                        &batch,
                        &metrics.encode_time,
                        &metrics.write_time,
                    )?;
                }
                buf_batch_writer.flush(&metrics.write_time)?;
                bytes_written
            };

            Ok(total_bytes_written)
        } else {
            Ok(0)
        }
    }

    fn ensure_spill_file_created(&mut self, runtime: &RuntimeEnv) -> Result<()> {
        if self.spill_file.is_none() {
            // Spill file is not yet created, create it
            let spill_file = runtime
                .disk_manager
                .create_tmp_file("shuffle writer spill")?;
            let spill_data = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(spill_file.path())
                .map_err(|e| {
                    DataFusionError::Execution(format!("Error occurred while spilling {e}"))
                })?;
            self.spill_file = Some(SpillFile {
                temp_file: spill_file,
                file: spill_data,
            });
        }
        Ok(())
    }
}

/// Write batches to writer while using a buffer to avoid frequent system calls.
/// The record batches were first written by ShuffleBlockWriter into an internal buffer.
/// Once the buffer exceeds the max size, the buffer will be flushed to the writer.
struct BufBatchWriter<S: Borrow<ShuffleBlockWriter>, W: Write> {
    shuffle_block_writer: S,
    writer: W,
    buffer: Vec<u8>,
    buffer_max_size: usize,
}

impl<S: Borrow<ShuffleBlockWriter>, W: Write> BufBatchWriter<S, W> {
    fn new(shuffle_block_writer: S, writer: W) -> Self {
        // 1MB should be good enough to avoid frequent system calls,
        // and also won't cause too much memory usage
        let buffer_max_size = 1024 * 1024;
        Self {
            shuffle_block_writer,
            writer,
            buffer: vec![],
            buffer_max_size,
        }
    }

    fn write(
        &mut self,
        batch: &RecordBatch,
        encode_time: &Time,
        write_time: &Time,
    ) -> Result<usize> {
        let mut cursor = Cursor::new(&mut self.buffer);
        cursor.seek(SeekFrom::End(0))?;
        let bytes_written =
            self.shuffle_block_writer
                .borrow()
                .write_batch(batch, &mut cursor, encode_time)?;
        let pos = cursor.position();
        if pos >= self.buffer_max_size as u64 {
            let mut write_timer = write_time.timer();
            self.writer.write_all(&self.buffer)?;
            write_timer.stop();
            self.buffer.clear();
        }
        Ok(bytes_written)
    }

    fn flush(&mut self, write_time: &Time) -> Result<()> {
        let mut write_timer = write_time.timer();
        if !self.buffer.is_empty() {
            self.writer.write_all(&self.buffer)?;
        }
        self.writer.flush()?;
        write_timer.stop();
        self.buffer.clear();
        Ok(())
    }
}

fn pmod(hash: u32, n: usize) -> usize {
    let hash = hash as i32;
    let n = n as i32;
    let r = hash % n;
    let result = if r < 0 { (r + n) % n } else { r };
    result as usize
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::execution::shuffle::read_ipc_compressed;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::execution::config::SessionConfig;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::physical_expr::expressions::{col, Column};
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion::physical_plan::common::collect;
    use datafusion::prelude::SessionContext;
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
    async fn shuffle_repartitioner_memory() {
        let batch = create_batch(900);
        assert_eq!(8376, batch.get_array_memory_size());

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
            ShuffleRepartitionerMetrics::new(&metrics_set, 0),
            runtime_env,
            1024,
            CompressionCodec::Lz4Frame,
            false,
        )
        .unwrap();

        repartitioner.insert_batch(batch.clone()).await.unwrap();

        assert_eq!(2, repartitioner.partition_writers.len());

        assert!(repartitioner.partition_writers[0].spill_file.is_none());
        assert!(repartitioner.partition_writers[1].spill_file.is_none());

        repartitioner.spill().unwrap();

        // after spill, there should be spill files
        assert!(repartitioner.partition_writers[0].spill_file.is_some());
        assert!(repartitioner.partition_writers[1].spill_file.is_some());

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

        for partitioning in [
            CometPartitioning::Hash(vec![Arc::new(Column::new("a", 0))], num_partitions),
            CometPartitioning::RangePartitioning(
                LexOrdering::new(vec![PhysicalSortExpr::new_default(
                    col("a", batch.schema().as_ref()).unwrap(),
                )])
                .unwrap(),
                num_partitions,
                100,
            ),
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
    fn test_pmod() {
        let i: Vec<u32> = vec![0x99f0149d, 0x9c67b85d, 0xc8008529, 0xa05b5d7b, 0xcd1e64fb];
        let result = i.into_iter().map(|i| pmod(i, 200)).collect::<Vec<usize>>();

        // expected partition from Spark with n=200
        let expected = vec![69, 5, 193, 171, 115];
        assert_eq!(result, expected);
    }
}
