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

use crate::execution::shuffle::builders::{
    append_columns, make_batch, new_array_builders, slot_size,
};
use crate::execution::shuffle::{CompressionCodec, ShuffleBlockWriter};
use async_trait::async_trait;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
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
        RecordBatchStream, SendableRecordBatchStream, Statistics,
    },
};
use datafusion_comet_spark_expr::hash_funcs::murmur3::create_murmur3_hashes;
use datafusion_physical_expr::EquivalenceProperties;
use futures::executor::block_on;
use futures::{Stream, StreamExt, TryFutureExt, TryStreamExt};
use itertools::Itertools;
use parquet::file::reader::Length;
use std::io::Error;
use std::{
    any::Any,
    fmt,
    fmt::{Debug, Formatter},
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Cursor, Seek, SeekFrom, Write},
    sync::Arc,
    task::{Context, Poll},
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
    partition_id: usize,
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
        partition_id,
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
    buffered_partitions: Vec<PartitionBuffer>,
    /// Sort expressions
    /// Partitioning scheme to use
    partitioning: Partitioning,
    num_output_partitions: usize,
    runtime: Arc<RuntimeEnv>,
    metrics: ShuffleRepartitionerMetrics,
    reservation: MemoryReservation,
    /// Hashes for each row in the current batch
    hashes_buf: Vec<u32>,
    /// Partition ids for each row in the current batch
    partition_ids: Vec<u64>,
    /// The configured batch size
    batch_size: usize,
}

impl ShuffleRepartitioner {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        partition_id: usize,
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
        let reservation = MemoryConsumer::new(format!("ShuffleRepartitioner[{}]", partition_id))
            .with_can_spill(true)
            .register(&runtime.memory_pool);

        let mut hashes_buf = Vec::with_capacity(batch_size);
        let mut partition_ids = Vec::with_capacity(batch_size);

        // Safety: `hashes_buf` will be filled with valid values before being used.
        // `partition_ids` will be filled with valid values before being used.
        unsafe {
            hashes_buf.set_len(batch_size);
            partition_ids.set_len(batch_size);
        }

        Ok(Self {
            output_data_file,
            output_index_file,
            schema: Arc::clone(&schema),
            buffered_partitions: (0..num_output_partitions)
                .map(|partition_id| {
                    PartitionBuffer::try_new(
                        Arc::clone(&schema),
                        batch_size,
                        partition_id,
                        &runtime,
                        codec.clone(),
                        enable_fast_encoding,
                    )
                })
                .collect::<Result<Vec<_>>>()?,
            partitioning,
            num_output_partitions,
            runtime,
            metrics,
            reservation,
            hashes_buf,
            partition_ids,
            batch_size,
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

        let num_output_partitions = self.num_output_partitions;
        match &self.partitioning {
            Partitioning::Hash(exprs, _) => {
                let (partition_starts, shuffled_partition_ids): (Vec<usize>, Vec<usize>) = {
                    let mut timer = self.metrics.repart_time.timer();

                    // evaluate partition expressions
                    let arrays = exprs
                        .iter()
                        .map(|expr| expr.evaluate(&input)?.into_array(input.num_rows()))
                        .collect::<Result<Vec<_>>>()?;

                    // use identical seed as spark hash partition
                    let hashes_buf = &mut self.hashes_buf[..arrays[0].len()];
                    hashes_buf.fill(42_u32);

                    // Hash arrays and compute buckets based on number of partitions
                    let partition_ids = &mut self.partition_ids[..arrays[0].len()];
                    create_murmur3_hashes(&arrays, hashes_buf)?
                        .iter()
                        .enumerate()
                        .for_each(|(idx, hash)| {
                            partition_ids[idx] = pmod(*hash, num_output_partitions) as u64
                        });

                    // count each partition size
                    let mut partition_counters = vec![0usize; num_output_partitions];
                    partition_ids
                        .iter()
                        .for_each(|partition_id| partition_counters[*partition_id as usize] += 1);

                    // accumulate partition counters into partition ends
                    // e.g. partition counter: [1, 3, 2, 1] => [1, 4, 6, 7]
                    let mut partition_ends = partition_counters;
                    let mut accum = 0;
                    partition_ends.iter_mut().for_each(|v| {
                        *v += accum;
                        accum = *v;
                    });

                    // calculate shuffled partition ids
                    // e.g. partition ids: [3, 1, 1, 1, 2, 2, 0] => [6, 1, 2, 3, 4, 5, 0] which is the
                    // row indices for rows ordered by their partition id. For example, first partition
                    // 0 has one row index [6], partition 1 has row indices [1, 2, 3], etc.
                    let mut shuffled_partition_ids = vec![0usize; input.num_rows()];
                    for (index, partition_id) in partition_ids.iter().enumerate().rev() {
                        partition_ends[*partition_id as usize] -= 1;
                        let end = partition_ends[*partition_id as usize];
                        shuffled_partition_ids[end] = index;
                    }

                    // after calculating, partition ends become partition starts
                    let mut partition_starts = partition_ends;
                    partition_starts.push(input.num_rows());
                    timer.stop();
                    Ok::<(Vec<usize>, Vec<usize>), DataFusionError>((
                        partition_starts,
                        shuffled_partition_ids,
                    ))
                }?;

                // For each interval of row indices of partition, taking rows from input batch and
                // appending into output buffer.
                for (partition_id, (&start, &end)) in partition_starts
                    .iter()
                    .tuple_windows()
                    .enumerate()
                    .filter(|(_, (start, end))| start < end)
                {
                    let mut mem_diff = self
                        .append_rows_to_partition(
                            input.columns(),
                            &shuffled_partition_ids[start..end],
                            partition_id,
                        )
                        .await?;

                    if mem_diff > 0 {
                        let mem_increase = mem_diff as usize;

                        let try_grow = {
                            let mut mempool_timer = self.metrics.mempool_time.timer();
                            let result = self.reservation.try_grow(mem_increase);
                            mempool_timer.stop();
                            result
                        };

                        if try_grow.is_err() {
                            self.spill().await?;
                            let mut mempool_timer = self.metrics.mempool_time.timer();
                            self.reservation.free();
                            self.reservation.try_grow(mem_increase)?;
                            mempool_timer.stop();
                            mem_diff = 0;
                        }
                    }

                    if mem_diff < 0 {
                        let mem_used = self.reservation.size();
                        let mem_decrease = mem_used.min(-mem_diff as usize);
                        let mut mempool_timer = self.metrics.mempool_time.timer();
                        self.reservation.shrink(mem_decrease);
                        mempool_timer.stop();
                    }
                }
            }
            Partitioning::UnknownPartitioning(n) if *n == 1 => {
                let buffered_partitions = &mut self.buffered_partitions;

                assert!(
                    buffered_partitions.len() == 1,
                    "Expected 1 partition but got {}",
                    buffered_partitions.len()
                );

                let indices = (0..input.num_rows()).collect::<Vec<usize>>();

                self.append_rows_to_partition(input.columns(), &indices, 0)
                    .await?;
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

    /// Writes buffered shuffled record batches into Arrow IPC bytes.
    async fn shuffle_write(&mut self) -> Result<SendableRecordBatchStream> {
        let mut elapsed_compute = self.metrics.baseline.elapsed_compute().timer();
        let num_output_partitions = self.num_output_partitions;
        let buffered_partitions = &mut self.buffered_partitions;
        let mut output_batches: Vec<Vec<u8>> = vec![vec![]; num_output_partitions];
        let mut offsets = vec![0; num_output_partitions + 1];
        for i in 0..num_output_partitions {
            buffered_partitions[i].flush(&self.metrics)?;
            output_batches[i] = std::mem::take(&mut buffered_partitions[i].frozen);
        }

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

        for i in 0..num_output_partitions {
            offsets[i] = output_data.stream_position()?;
            output_data.write_all(&output_batches[i])?;
            output_batches[i].clear();

            if let Some(spill_data) = self.buffered_partitions[i].spill_file.as_ref() {
                if spill_data.file.len() > 0 {
                    let mut spill_file = BufReader::new(
                        File::open(spill_data.temp_file.path()).map_err(Self::to_df_err)?,
                    );
                    std::io::copy(&mut spill_file, &mut output_data).map_err(Self::to_df_err)?;
                }
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

        let mut mempool_timer = self.metrics.mempool_time.timer();
        let used = self.reservation.size();
        self.reservation.shrink(used);
        mempool_timer.stop();

        elapsed_compute.stop();

        // shuffle writer always has empty output
        Ok(Box::pin(EmptyStream::try_new(Arc::clone(&self.schema))?))
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

    async fn spill(&mut self) -> Result<usize> {
        log::debug!(
            "ShuffleRepartitioner spilling shuffle data of {} to disk while inserting ({} time(s) so far)",
            self.used(),
            self.spill_count()
        );

        // we could always get a chance to free some memory as long as we are holding some
        if self.buffered_partitions.is_empty() {
            return Ok(0);
        }

        for p in &mut self.buffered_partitions {
            p.spill(&self.runtime, &self.metrics)?;
        }

        let used = self.reservation.size();
        self.metrics.spill_count.add(1);
        // self.metrics.spilled_bytes.add(used);
        Ok(used)
    }

    /// Appends rows of specified indices from columns into active array builders in the specified partition.
    async fn append_rows_to_partition(
        &mut self,
        columns: &[ArrayRef],
        indices: &[usize],
        partition_id: usize,
    ) -> Result<isize> {
        let mut mem_diff = 0;

        let output = &mut self.buffered_partitions[partition_id];

        // If the range of indices is not big enough, just appending the rows into
        // active array builders instead of directly adding them as a record batch.
        let mut start_index: usize = 0;
        let mut output_ret = output.append_rows(columns, indices, start_index, &self.metrics);

        loop {
            match output_ret {
                AppendRowStatus::MemDiff(l) => {
                    mem_diff += l?;
                    break;
                }
                AppendRowStatus::StartIndex(new_start) => {
                    // Cannot allocate enough memory for the array builders in the partition,
                    // spill partitions and retry.
                    self.spill().await?;

                    let mut mempool_timer = self.metrics.mempool_time.timer();
                    self.reservation.free();
                    let output = &mut self.buffered_partitions[partition_id];
                    output.reservation.free();
                    mempool_timer.stop();

                    start_index = new_start;
                    output_ret = output.append_rows(columns, indices, start_index, &self.metrics);

                    if let AppendRowStatus::StartIndex(new_start) = output_ret {
                        if new_start == start_index {
                            // If the start index is not updated, it means that the partition
                            // is still not able to allocate enough memory for the array builders.
                            return Err(DataFusionError::Internal(
                                "Partition is still not able to allocate enough memory for the array builders after spilling."
                                    .to_string(),
                            ));
                        }
                    }
                }
            }
        }

        Ok(mem_diff)
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

/// The status of appending rows to a partition buffer.
enum AppendRowStatus {
    /// The difference in memory usage after appending rows
    MemDiff(Result<isize>),
    /// The index of the next row to append
    StartIndex(usize),
}

struct PartitionBuffer {
    /// The schema of batches to be partitioned.
    schema: SchemaRef,
    /// The "frozen" Arrow IPC bytes of active data. They are frozen when `flush` is called.
    frozen: Vec<u8>,
    /// Array builders for appending rows into buffering batches.
    active: Vec<Box<dyn ArrayBuilder>>,
    /// The estimation of memory size of active builders in bytes when they are filled.
    active_slots_mem_size: usize,
    /// Number of rows in active builders.
    num_active_rows: usize,
    /// The maximum number of rows in a batch. Once `num_active_rows` reaches `batch_size`,
    /// the active array builders will be frozen and appended to frozen buffer `frozen`.
    batch_size: usize,
    /// Memory reservation for this partition buffer.
    reservation: MemoryReservation,
    /// Spill file for intermediate shuffle output for this partition
    spill_file: Option<SpillFile>,
    /// Spill file for intermediate shuffle output for this partition
    shuffle_block_writer: ShuffleBlockWriter,
}

struct SpillFile {
    temp_file: RefCountedTempFile,
    file: File,
}

impl PartitionBuffer {
    fn try_new(
        schema: SchemaRef,
        batch_size: usize,
        partition_id: usize,
        runtime: &Arc<RuntimeEnv>,
        codec: CompressionCodec,
        enable_fast_encoding: bool,
    ) -> Result<Self> {
        let reservation = MemoryConsumer::new(format!("PartitionBuffer[{}]", partition_id))
            .with_can_spill(true)
            .register(&runtime.memory_pool);
        let shuffle_block_writer =
            ShuffleBlockWriter::try_new(schema.as_ref(), enable_fast_encoding, codec)?;

        Ok(Self {
            schema,
            frozen: vec![],
            active: vec![],
            active_slots_mem_size: 0,
            num_active_rows: 0,
            batch_size,
            reservation,
            spill_file: None,
            shuffle_block_writer,
        })
    }

    /// Initializes active builders if necessary.
    /// Returns error if memory reservation fails.
    fn init_active_if_necessary(&mut self, metrics: &ShuffleRepartitionerMetrics) -> Result<isize> {
        let mut mem_diff = 0;

        if self.active.is_empty() {
            // Estimate the memory size of active builders
            if self.active_slots_mem_size == 0 {
                self.active_slots_mem_size = self
                    .schema
                    .fields()
                    .iter()
                    .map(|field| slot_size(self.batch_size, field.data_type()))
                    .sum::<usize>();
            }

            let mut mempool_timer = metrics.mempool_time.timer();
            self.reservation.try_grow(self.active_slots_mem_size)?;
            mempool_timer.stop();

            let mut repart_timer = metrics.repart_time.timer();
            self.active = new_array_builders(&self.schema, self.batch_size);
            repart_timer.stop();

            mem_diff += self.active_slots_mem_size as isize;
        }
        Ok(mem_diff)
    }

    /// Appends rows of specified indices from columns into active array builders.
    fn append_rows(
        &mut self,
        columns: &[ArrayRef],
        indices: &[usize],
        start_index: usize,
        metrics: &ShuffleRepartitionerMetrics,
    ) -> AppendRowStatus {
        let mut mem_diff = 0;
        let mut start = start_index;

        // lazy init because some partition may be empty
        let init = self.init_active_if_necessary(metrics);
        if init.is_err() {
            return AppendRowStatus::StartIndex(start);
        }
        mem_diff += init.unwrap();

        while start < indices.len() {
            let end = (start + self.batch_size).min(indices.len());

            let mut repart_timer = metrics.repart_time.timer();
            self.active
                .iter_mut()
                .zip(columns)
                .for_each(|(builder, column)| {
                    append_columns(builder, column, &indices[start..end], column.data_type());
                });
            self.num_active_rows += end - start;
            repart_timer.stop();

            if self.num_active_rows >= self.batch_size {
                let flush = self.flush(metrics);
                if let Err(e) = flush {
                    return AppendRowStatus::MemDiff(Err(e));
                }
                mem_diff += flush.unwrap();

                let init = self.init_active_if_necessary(metrics);
                if init.is_err() {
                    return AppendRowStatus::StartIndex(end);
                }
                mem_diff += init.unwrap();
            }
            start = end;
        }
        AppendRowStatus::MemDiff(Ok(mem_diff))
    }

    /// flush active data into frozen bytes
    fn flush(&mut self, metrics: &ShuffleRepartitionerMetrics) -> Result<isize> {
        if self.num_active_rows == 0 {
            return Ok(0);
        }
        let mut mem_diff = 0isize;

        // active -> staging
        let active = std::mem::take(&mut self.active);
        let num_rows = self.num_active_rows;
        self.num_active_rows = 0;

        let mut mempool_timer = metrics.mempool_time.timer();
        self.reservation.try_shrink(self.active_slots_mem_size)?;
        mempool_timer.stop();

        let mut repart_timer = metrics.repart_time.timer();
        let frozen_batch = make_batch(Arc::clone(&self.schema), active, num_rows)?;
        repart_timer.stop();

        let frozen_capacity_old = self.frozen.capacity();
        let mut cursor = Cursor::new(&mut self.frozen);
        cursor.seek(SeekFrom::End(0))?;
        self.shuffle_block_writer
            .write_batch(&frozen_batch, &mut cursor, &metrics.encode_time)?;

        mem_diff += (self.frozen.capacity() - frozen_capacity_old) as isize;
        Ok(mem_diff)
    }

    fn spill(&mut self, runtime: &RuntimeEnv, metrics: &ShuffleRepartitionerMetrics) -> Result<()> {
        self.flush(metrics).unwrap();
        let output_batches = std::mem::take(&mut self.frozen);
        let mut write_timer = metrics.write_time.timer();
        if self.spill_file.is_none() {
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
        self.spill_file
            .as_mut()
            .unwrap()
            .file
            .write_all(&output_batches)?;
        write_timer.stop();
        Ok(())
    }
}

/// A stream that yields no record batches which represent end of output.
pub struct EmptyStream {
    /// Schema representing the data
    schema: SchemaRef,
}

impl EmptyStream {
    /// Create an iterator for a vector of record batches
    pub fn try_new(schema: SchemaRef) -> Result<Self> {
        Ok(Self { schema })
    }
}

impl Stream for EmptyStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: std::pin::Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}

impl RecordBatchStream for EmptyStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
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
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::physical_plan::common::collect;
    use datafusion::physical_plan::memory::MemoryExec;
    use datafusion::prelude::SessionContext;
    use datafusion_execution::config::SessionConfig;
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;
    use datafusion_physical_expr::expressions::Column;
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
    fn test_slot_size() {
        let batch_size = 1usize;
        // not inclusive of all supported types, but enough to test the function
        let supported_primitive_types = [
            DataType::Int32,
            DataType::Int64,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Float32,
            DataType::Float64,
            DataType::Boolean,
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Binary,
            DataType::LargeBinary,
            DataType::FixedSizeBinary(16),
        ];
        let expected_slot_size = [4, 8, 4, 8, 4, 8, 1, 104, 108, 104, 108, 16];
        supported_primitive_types
            .iter()
            .zip(expected_slot_size.iter())
            .for_each(|(data_type, expected)| {
                let slot_size = slot_size(batch_size, data_type);
                assert_eq!(slot_size, *expected);
            })
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
    #[cfg(not(target_os = "macos"))] // Github MacOS runner fails with "Too many open files".
    fn test_large_number_of_partitions() {
        shuffle_write_test(10000, 10, 200, Some(10 * 1024 * 1024));
        shuffle_write_test(10000, 10, 2000, Some(10 * 1024 * 1024));
    }

    #[test]
    #[cfg_attr(miri, ignore)] // miri can't call foreign function `ZSTD_createCCtx`
    #[cfg(not(target_os = "macos"))] // Github MacOS runner fails with "Too many open files".
    fn test_large_number_of_partitions_spilling() {
        shuffle_write_test(10000, 100, 200, Some(10 * 1024 * 1024));
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
            Arc::new(MemoryExec::try_new(partitions, batch.schema(), None).unwrap()),
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
