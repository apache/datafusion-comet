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

use crate::execution::shuffle::{CompressionCodec, ShuffleBlockWriter};
use arrow::compute::interleave_record_batch;
use async_trait::async_trait;
use datafusion::common::utils::proxy::VecAllocExt;
use datafusion::physical_expr::EquivalenceProperties;
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
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        SendableRecordBatchStream, Statistics,
    },
};
use datafusion_comet_spark_expr::hash_funcs::murmur3::create_murmur3_hashes;
use futures::executor::block_on;
use futures::{StreamExt, TryFutureExt, TryStreamExt};
use itertools::Itertools;
use std::io::Error;
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
    partitioning: Partitioning,
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
    /// When true, Comet will use a fast proprietary encoding rather than using Arrow IPC
    enable_fast_encoding: bool,
}

impl ShuffleWriterExec {
    /// Create a new ShuffleWriterExec
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        partitioning: Partitioning,
        codec: CompressionCodec,
        output_data_file: String,
        output_index_file: String,
        enable_fast_encoding: bool,
    ) -> Result<Self> {
        let cache = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&input.schema())),
            partitioning.clone(),
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
            enable_fast_encoding,
        })
    }
}

impl DisplayAs for ShuffleWriterExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "ShuffleWriterExec: partitioning={:?}, fast_encoding={}, compression={:?}",
                    self.partitioning, self.enable_fast_encoding, self.codec
                )
            }
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
        self.input.statistics()
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
                self.enable_fast_encoding,
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
                    self.enable_fast_encoding,
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
    partitioning: Partitioning,
    metrics: ShuffleRepartitionerMetrics,
    context: Arc<TaskContext>,
    codec: CompressionCodec,
    enable_fast_encoding: bool,
) -> Result<SendableRecordBatchStream> {
    let schema = input.schema();
    let mut repartitioner = ShuffleRepartitioner::try_new(
        partition,
        output_data_file,
        output_index_file,
        Arc::clone(&schema),
        partitioning,
        metrics,
        context.runtime_env(),
        context.session_config().batch_size(),
        codec,
        enable_fast_encoding,
    )?;

    while let Some(batch) = input.next().await {
        // Block on the repartitioner to insert the batch and shuffle the rows
        // into the corresponding partition buffer.
        // Otherwise, pull the next batch from the input stream might overwrite the
        // current batch in the repartitioner.
        block_on(repartitioner.insert_batch(batch?))?;
    }
    repartitioner.shuffle_write().await
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

struct ShuffleRepartitioner {
    output_data_file: String,
    output_index_file: String,
    schema: SchemaRef,
    buffered_batches: Vec<RecordBatch>,
    partition_indices: Vec<Vec<(u32, u32)>>,
    partition_writers: Vec<PartitionWriter>,
    shuffle_block_writer: ShuffleBlockWriter,
    /// Partitioning scheme to use
    partitioning: Partitioning,
    runtime: Arc<RuntimeEnv>,
    metrics: ShuffleRepartitionerMetrics,
    /// Reused scratch space for computing partition indices
    scratch: ScratchSpace,
    /// The configured batch size
    batch_size: usize,
    /// Reservation for repartitioning
    reservation: MemoryReservation,
}

#[derive(Default)]
struct ScratchSpace {
    /// Hashes for each row in the current batch.
    hashes_buf: Vec<u32>,
    /// Partition ids for each row in the current batch.
    partition_ids: Vec<u32>,
    /// The row indices of the rows in each partition. This array is conceptually dividied into
    /// partitions, where each partition contains the row indices of the rows in that partition.
    /// The length of this array is the same as the number of rows in the batch.
    shuffle_partition_ids: Vec<u32>,
    /// The start indices of partitions in shuffle_partition_ids. The length of this array is the
    /// same as the number of partitions.
    partition_starts: Vec<u32>,
}

impl ShuffleRepartitioner {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        partition: usize,
        output_data_file: String,
        output_index_file: String,
        schema: SchemaRef,
        partitioning: Partitioning,
        metrics: ShuffleRepartitionerMetrics,
        runtime: Arc<RuntimeEnv>,
        batch_size: usize,
        codec: CompressionCodec,
        enable_fast_encoding: bool,
    ) -> Result<Self> {
        let num_output_partitions = partitioning.partition_count();

        // Vectors in the scratch space will be filled with valid values before being used, this
        // initialization code is simply initializing the vectors to the desired size.
        // The initial values are not used.
        let scratch = ScratchSpace {
            hashes_buf: vec![0; batch_size],
            partition_ids: vec![0; batch_size],
            shuffle_partition_ids: vec![0; batch_size],
            partition_starts: vec![0; num_output_partitions + 1],
        };

        let shuffle_block_writer =
            ShuffleBlockWriter::try_new(schema.as_ref(), enable_fast_encoding, codec.clone())?;

        let partition_writers = (0..num_output_partitions)
            .map(|_| PartitionWriter::try_new(shuffle_block_writer.clone()))
            .collect::<Result<Vec<_>>>()?;

        let reservation = MemoryConsumer::new(format!("ShuffleRepartitioner[{}]", partition))
            .with_can_spill(true)
            .register(&runtime.memory_pool);

        Ok(Self {
            output_data_file,
            output_index_file,
            schema: Arc::clone(&schema),
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
        })
    }

    /// Shuffles rows in input batch into corresponding partition buffer.
    /// This function will slice input batch according to configured batch size and then
    /// shuffle rows into corresponding partition buffer.
    async fn insert_batch(&mut self, batch: RecordBatch) -> Result<()> {
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
            any if any.partition_count() == 1 => {
                assert_eq!(self.partition_writers.len(), 1, "Expected 1 partition");

                // TODO the single partition case could be optimized to avoid appending all
                // rows from the batch into builders and then recreating the batch
                // https://github.com/apache/datafusion-comet/issues/1453

                self.buffer_batch_may_spill(input).await?;
            }
            Partitioning::Hash(exprs, num_output_partitions) => {
                let mut scratch = std::mem::take(&mut self.scratch);
                let (partition_starts, shuffled_partition_ids): (&Vec<u32>, &Vec<u32>) = {
                    let mut timer = self.metrics.repart_time.timer();

                    // evaluate partition expressions
                    let arrays = exprs
                        .iter()
                        .map(|expr| expr.evaluate(&input)?.into_array(input.num_rows()))
                        .collect::<Result<Vec<_>>>()?;

                    // use identical seed as spark hash partition
                    let hashes_buf = &mut scratch.hashes_buf[..arrays[0].len()];
                    hashes_buf.fill(42_u32);

                    // Hash arrays and compute buckets based on number of partitions
                    let partition_ids = &mut scratch.partition_ids[..arrays[0].len()];
                    create_murmur3_hashes(&arrays, hashes_buf)?
                        .iter()
                        .enumerate()
                        .for_each(|(idx, hash)| {
                            partition_ids[idx] = pmod(*hash, *num_output_partitions) as u32;
                        });

                    // count each partition size
                    let partition_counters = &mut scratch.partition_starts;
                    partition_counters.resize(num_output_partitions + 1, 0);
                    partition_counters.fill(0);
                    partition_ids
                        .iter()
                        .for_each(|partition_id| partition_counters[*partition_id as usize] += 1);

                    // accumulate partition counters into partition ends
                    // e.g. partition counter: [1, 3, 2, 1] => [1, 4, 6, 7]
                    let partition_ends = partition_counters;
                    let mut accum = 0;
                    partition_ends.iter_mut().for_each(|v| {
                        *v += accum;
                        accum = *v;
                    });

                    // calculate shuffled partition ids
                    // e.g. partition ids: [3, 1, 1, 1, 2, 2, 0] => [6, 1, 2, 3, 4, 5, 0] which is the
                    // row indices for rows ordered by their partition id. For example, first partition
                    // 0 has one row index [6], partition 1 has row indices [1, 2, 3], etc.
                    let shuffled_partition_ids = &mut scratch.shuffle_partition_ids;
                    shuffled_partition_ids.resize(input.num_rows(), 0);
                    for (index, partition_id) in partition_ids.iter().enumerate().rev() {
                        partition_ends[*partition_id as usize] -= 1;
                        let end = partition_ends[*partition_id as usize];
                        shuffled_partition_ids[end as usize] = index as u32;
                    }

                    // after calculating, partition ends become partition starts
                    let partition_starts = partition_ends;
                    timer.stop();
                    Ok::<(&Vec<u32>, &Vec<u32>), DataFusionError>((
                        partition_starts,
                        shuffled_partition_ids,
                    ))
                }?;

                self.buffer_partitioned_batch_may_spill(
                    input,
                    shuffled_partition_ids,
                    partition_starts,
                )
                .await?;
                self.scratch = scratch;
            }
            other => {
                // this should be unreachable as long as the validation logic
                // in the constructor is kept up-to-date
                return Err(DataFusionError::NotImplemented(format!(
                    "Unsupported repartitioning scheme {:?}",
                    other
                )));
            }
        }
        Ok(())
    }

    async fn buffer_partitioned_batch_may_spill(
        &mut self,
        input: RecordBatch,
        shuffled_partition_ids: &[u32],
        partition_starts: &[u32],
    ) -> Result<()> {
        let mut mem_growth: usize = input.get_array_memory_size();
        let buffered_partition_idx = self.buffered_batches.len() as u32;
        self.buffered_batches.push(input);

        for (partition_id, (&start, &end)) in partition_starts
            .iter()
            .tuple_windows()
            .enumerate()
            .filter(|(_, (start, end))| start < end)
        {
            let row_indices = &shuffled_partition_ids[start as usize..end as usize];
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
            self.spill().await?;
        }

        Ok(())
    }

    async fn buffer_batch_may_spill(&mut self, input: RecordBatch) -> Result<()> {
        let size = input.get_array_memory_size();
        self.buffered_batches.push(input);

        let grow_result = {
            let mut timer = self.metrics.mempool_time.timer();
            let result = self.reservation.try_grow(size);
            timer.stop();
            result
        };
        if grow_result.is_err() {
            self.spill().await?;
        }
        Ok(())
    }

    /// Writes buffered shuffled record batches into Arrow IPC bytes.
    async fn shuffle_write(&mut self) -> Result<SendableRecordBatchStream> {
        let mut partitioned_batches = self.partitioned_batches();
        let mut elapsed_compute = self.metrics.baseline.elapsed_compute().timer();
        let num_output_partitions = self.partition_indices.len();
        let mut offsets = vec![0; num_output_partitions + 1];

        let data_file = self.output_data_file.clone();
        let index_file = self.output_index_file.clone();

        let mut write_time = self.metrics.write_time.timer();

        let output_data = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(data_file)
            .map_err(|e| DataFusionError::Execution(format!("shuffle write error: {:?}", e)))?;

        let mut output_data = BufWriter::new(output_data);

        #[allow(clippy::needless_range_loop)]
        for i in 0..num_output_partitions {
            offsets[i] = output_data.stream_position()?;

            // Write in memory batches to output data file
            let partition_iter = partitioned_batches.produce(i);
            for batch in partition_iter {
                let batch = batch?;
                self.shuffle_block_writer.write_batch(
                    &batch,
                    &mut output_data,
                    &self.metrics.encode_time,
                )?;
            }

            // if we wrote a spill file for this partition then copy the
            // contents into the shuffle file
            if let Some(spill_data) = self.partition_writers[i].spill_file.as_ref() {
                let mut spill_file = BufReader::new(
                    File::open(spill_data.temp_file.path()).map_err(Self::to_df_err)?,
                );
                std::io::copy(&mut spill_file, &mut output_data).map_err(Self::to_df_err)?;
            }
        }
        output_data.flush()?;

        // add one extra offset at last to ease partition length computation
        offsets[num_output_partitions] = output_data.stream_position().map_err(Self::to_df_err)?;

        let mut output_index =
            BufWriter::new(File::create(index_file).map_err(|e| {
                DataFusionError::Execution(format!("shuffle write error: {:?}", e))
            })?);
        for offset in offsets {
            output_index
                .write_all(&(offset as i64).to_le_bytes()[..])
                .map_err(Self::to_df_err)?;
        }
        output_index.flush()?;

        write_time.stop();

        elapsed_compute.stop();

        // shuffle writer always has empty output
        Ok(Box::pin(EmptyRecordBatchStream::new(Arc::clone(
            &self.schema,
        ))))
    }

    fn to_df_err(e: Error) -> DataFusionError {
        DataFusionError::Execution(format!("shuffle write error: {:?}", e))
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
        let single_partition_mode = num_output_partitions == 1;
        PartitionedBatchesProducer::new(
            buffered_batches,
            indices,
            self.batch_size,
            single_partition_mode,
        )
    }

    async fn spill(&mut self) -> Result<()> {
        log::debug!(
            "ShuffleRepartitioner spilling shuffle data of {} to disk while inserting ({} time(s) so far)",
            self.used(),
            self.spill_count()
        );

        // we could always get a chance to free some memory as long as we are holding some
        if self.buffered_batches.is_empty() {
            return Ok(());
        }

        let num_output_partitions = self.partition_writers.len();
        let mut partitioned_batches = self.partitioned_batches();
        let mut spilled_bytes = 0;

        for partition_id in 0..num_output_partitions {
            let partition_writer = &mut self.partition_writers[partition_id];
            let iter = partitioned_batches.produce(partition_id);
            for batch in iter {
                let batch = batch?;
                spilled_bytes += partition_writer.spill(&batch, &self.runtime, &self.metrics)?;
            }
        }

        let mut timer = self.metrics.mempool_time.timer();
        self.reservation.free();
        timer.stop();
        self.metrics.spill_count.add(1);
        self.metrics.spilled_bytes.add(spilled_bytes);
        Ok(())
    }
}

impl Debug for ShuffleRepartitioner {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShuffleRepartitioner")
            .field("memory_used", &self.used())
            .field("spilled_bytes", &self.spilled_bytes())
            .field("spilled_count", &self.spill_count())
            .field("data_size", &self.data_size())
            .finish()
    }
}

/// A helper struct to produce shuffled batches.
/// This struct takes ownership of the buffered batches and partition indices from the
/// ShuffleRepartitioner, and provides an iterator over the batches in the specified partitions.
struct PartitionedBatchesProducer {
    buffered_batches: Vec<RecordBatch>,
    partition_indices: Vec<Vec<(u32, u32)>>,
    batch_size: usize,
    single_partition_mode: bool,
}

impl PartitionedBatchesProducer {
    fn new(
        buffered_batches: Vec<RecordBatch>,
        indices: Vec<Vec<(u32, u32)>>,
        batch_size: usize,
        single_partition_mode: bool,
    ) -> Self {
        Self {
            partition_indices: indices,
            buffered_batches,
            batch_size,
            single_partition_mode,
        }
    }

    fn produce(&mut self, partition_id: usize) -> BatchIterator {
        if self.single_partition_mode {
            assert!(
                partition_id == 0,
                "Single partition mode can only be used for the first partition"
            );
            BatchIterator::SinglePartition(SinglePartitionBatchIterator::new(
                std::mem::take(&mut self.buffered_batches),
                self.batch_size,
            ))
        } else {
            BatchIterator::Partitioned(PartitionedBatchIterator::new(
                &self.partition_indices[partition_id],
                &self.buffered_batches,
                self.batch_size,
            ))
        }
    }
}

enum BatchIterator<'a> {
    Partitioned(PartitionedBatchIterator<'a>),
    SinglePartition(SinglePartitionBatchIterator),
}

struct SinglePartitionBatchIterator {
    buffered_batches: std::vec::IntoIter<RecordBatch>,
    batch_size: usize,
    pending_batch: Option<RecordBatch>,
    concatenating_batches: Vec<RecordBatch>,
}

struct PartitionedBatchIterator<'a> {
    record_batches: Vec<&'a RecordBatch>,
    batch_size: usize,
    indices: Vec<(usize, usize)>,
    pos: usize,
}

impl Iterator for BatchIterator<'_> {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            BatchIterator::Partitioned(iter) => iter.next(),
            BatchIterator::SinglePartition(iter) => iter.next(),
        }
    }
}

impl SinglePartitionBatchIterator {
    fn new(buffered_batches: Vec<RecordBatch>, batch_size: usize) -> Self {
        Self {
            buffered_batches: buffered_batches.into_iter(),
            batch_size,
            pending_batch: None,
            concatenating_batches: vec![],
        }
    }

    fn next(&mut self) -> Option<Result<RecordBatch>> {
        if self.pending_batch.is_none() {
            // Get the first batch if we don't have a pending batch
            self.pending_batch = self.buffered_batches.next();
            self.pending_batch.as_ref()?;
        }

        let pending_batch = self.pending_batch.take().unwrap();
        let mut current_row_count = pending_batch.num_rows();
        let schema = pending_batch.schema();
        self.concatenating_batches.clear();
        self.concatenating_batches.push(pending_batch);

        // Keep accumulating batches until we reach/exceed batch_size or run out of batches
        while current_row_count < self.batch_size {
            if let Some(next_batch) = self.buffered_batches.next() {
                let next_row_count = next_batch.num_rows();

                // If adding this batch would exceed batch_size, save it for next time
                current_row_count += next_row_count;
                if current_row_count > self.batch_size {
                    self.pending_batch = Some(next_batch);
                    break;
                }

                // Otherwise, concatenate the batches
                self.concatenating_batches.push(next_batch);
            } else {
                // No more batches to concatenate
                break;
            }
        }

        if self.concatenating_batches.len() > 1 {
            match arrow::compute::concat_batches(&schema, self.concatenating_batches.iter()) {
                Ok(concatenated) => Some(Ok(concatenated)),
                Err(e) => {
                    // If concatenation fails, still return what we have so far
                    Some(Err(DataFusionError::ArrowError(
                        e,
                        Some(DataFusionError::get_back_trace()),
                    )))
                }
            }
        } else {
            Some(Ok(self.concatenating_batches.pop().unwrap()))
        }
    }
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

    fn next(&mut self) -> Option<Result<RecordBatch>> {
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
                e,
                Some(DataFusionError::get_back_trace()),
            ))),
        }
    }
}

#[derive(Default)]
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
        batch: &RecordBatch,
        runtime: &RuntimeEnv,
        metrics: &ShuffleRepartitionerMetrics,
    ) -> Result<usize> {
        let mut write_timer = metrics.write_time.timer();
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
                    DataFusionError::Execution(format!("Error occurred while spilling {}", e))
                })?;
            self.spill_file = Some(SpillFile {
                temp_file: spill_file,
                file: spill_data,
            });
        }

        let bytes_written = {
            let mut writer = BufWriter::new(&self.spill_file.as_mut().unwrap().file);
            let bytes_written =
                self.shuffle_block_writer
                    .write_batch(batch, &mut writer, &metrics.encode_time)?;
            writer.flush()?;
            bytes_written
        };

        write_timer.stop();

        Ok(bytes_written)
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
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::common::collect;
    use datafusion::prelude::SessionContext;
    use std::io::Cursor;
    use tokio::runtime::Runtime;

    #[test]
    #[cfg_attr(miri, ignore)] // miri can't call foreign function `ZSTD_createCCtx`
    fn roundtrip_ipc() {
        let batch = create_batch(8192);
        for fast_encoding in [true, false] {
            for codec in &[
                CompressionCodec::None,
                CompressionCodec::Zstd(1),
                CompressionCodec::Snappy,
                CompressionCodec::Lz4Frame,
            ] {
                let mut output = vec![];
                let mut cursor = Cursor::new(&mut output);
                let writer = ShuffleBlockWriter::try_new(
                    batch.schema().as_ref(),
                    fast_encoding,
                    codec.clone(),
                )
                .unwrap();
                let length = writer
                    .write_batch(&batch, &mut cursor, &Time::default())
                    .unwrap();
                assert_eq!(length, output.len());

                let ipc_without_length_prefix = &output[16..];
                let batch2 = read_ipc_compressed(ipc_without_length_prefix).unwrap();
                assert_eq!(batch, batch2);
            }
        }
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
        let mut repartitioner = ShuffleRepartitioner::try_new(
            0,
            "/tmp/data.out".to_string(),
            "/tmp/index.out".to_string(),
            batch.schema(),
            Partitioning::Hash(vec![Arc::new(Column::new("a", 0))], num_partitions),
            ShuffleRepartitionerMetrics::new(&metrics_set, 0),
            runtime_env,
            1024,
            CompressionCodec::Lz4Frame,
            true,
        )
        .unwrap();

        repartitioner.insert_batch(batch.clone()).await.unwrap();

        assert_eq!(2, repartitioner.partition_writers.len());

        assert!(repartitioner.partition_writers[0].spill_file.is_none());
        assert!(repartitioner.partition_writers[1].spill_file.is_none());

        repartitioner.spill().await.unwrap();

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

        let batches = (0..num_batches).map(|_| batch.clone()).collect::<Vec<_>>();

        let partitions = &[batches];
        let exec = ShuffleWriterExec::try_new(
            Arc::new(DataSourceExec::new(Arc::new(
                MemorySourceConfig::try_new(partitions, batch.schema(), None).unwrap(),
            ))),
            Partitioning::Hash(vec![Arc::new(Column::new("a", 0))], num_partitions),
            CompressionCodec::Zstd(1),
            "/tmp/data.out".to_string(),
            "/tmp/index.out".to_string(),
            true,
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
