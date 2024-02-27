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

use std::{
    any::Any,
    fmt,
    fmt::{Debug, Formatter},
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Cursor, Read, Seek, SeekFrom, Write},
    path::Path,
    sync::Arc,
    task::{Context, Poll},
};

use arrow::{datatypes::*, ipc::writer::StreamWriter};
use async_trait::async_trait;
use bytes::Buf;
use crc32fast::Hasher;
use datafusion::{
    arrow::{
        array::*,
        datatypes::{DataType, SchemaRef, TimeUnit},
        error::{ArrowError, Result as ArrowResult},
        record_batch::RecordBatch,
    },
    error::{DataFusionError, Result},
    execution::{
        context::TaskContext,
        disk_manager::RefCountedTempFile,
        memory_pool::{MemoryConsumer, MemoryReservation},
        runtime_env::RuntimeEnv,
    },
    physical_plan::{
        expressions::PhysicalSortExpr,
        metrics::{BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet},
        stream::RecordBatchStreamAdapter,
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream,
        SendableRecordBatchStream, Statistics,
    },
};
use futures::{lock::Mutex, Stream, StreamExt, TryFutureExt, TryStreamExt};
use itertools::Itertools;
use simd_adler32::Adler32;
use tokio::task;

use crate::{
    common::bit::ceil,
    errors::{CometError, CometResult},
    execution::datafusion::spark_hash::{create_hashes, pmod},
};

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
}

impl DisplayAs for ShuffleWriterExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "ShuffleWriterExec: partitioning={:?}", self.partitioning)
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

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.partitioning.clone()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.len() {
            1 => Ok(Arc::new(ShuffleWriterExec::try_new(
                children[0].clone(),
                self.partitioning.clone(),
                self.output_data_file.clone(),
                self.output_index_file.clone(),
            )?)),
            _ => panic!("ShuffleWriterExec wrong number of children"),
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context.clone())?;
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
                )
                .map_err(|e| ArrowError::ExternalError(Box::new(e))),
            )
            .try_flatten(),
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        self.input.statistics()
    }
}

impl ShuffleWriterExec {
    /// Create a new ShuffleWriterExec
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        partitioning: Partitioning,
        output_data_file: String,
        output_index_file: String,
    ) -> Result<Self> {
        Ok(ShuffleWriterExec {
            input,
            partitioning,
            metrics: ExecutionPlanMetricsSet::new(),
            output_data_file,
            output_index_file,
        })
    }
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
}

impl PartitionBuffer {
    fn new(schema: SchemaRef, batch_size: usize) -> Self {
        Self {
            schema,
            frozen: vec![],
            active: vec![],
            active_slots_mem_size: 0,
            num_active_rows: 0,
            batch_size,
        }
    }

    /// Initializes active builders if necessary.
    fn init_active_if_necessary(&mut self) -> Result<isize> {
        let mut mem_diff = 0;

        if self.active.is_empty() {
            self.active = new_array_builders(&self.schema, self.batch_size);
            if self.active_slots_mem_size == 0 {
                self.active_slots_mem_size = self
                    .active
                    .iter()
                    .zip(self.schema.fields())
                    .map(|(_ab, field)| slot_size(self.batch_size, field.data_type()))
                    .sum::<usize>();
            }
            mem_diff += self.active_slots_mem_size as isize;
        }
        Ok(mem_diff)
    }

    /// Appends all rows of given batch into active array builders.
    fn append_batch(&mut self, batch: &RecordBatch) -> Result<isize> {
        let columns = batch.columns();
        let indices = (0..batch.num_rows()).collect::<Vec<usize>>();
        self.append_rows(columns, &indices)
    }

    /// Appends rows of specified indices from columns into active array builders.
    fn append_rows(&mut self, columns: &[ArrayRef], indices: &[usize]) -> Result<isize> {
        let mut mem_diff = 0;
        let mut start = 0;

        // lazy init because some partition may be empty
        mem_diff += self.init_active_if_necessary()?;

        while start < indices.len() {
            let end = (start + self.batch_size).min(indices.len());
            self.active
                .iter_mut()
                .zip(columns)
                .for_each(|(builder, column)| {
                    append_columns(builder, column, &indices[start..end], column.data_type());
                });
            self.num_active_rows += end - start;
            if self.num_active_rows >= self.batch_size {
                mem_diff += self.flush()?;
                mem_diff += self.init_active_if_necessary()?;
            }
            start = end;
        }
        Ok(mem_diff)
    }

    /// flush active data into frozen bytes
    fn flush(&mut self) -> Result<isize> {
        if self.num_active_rows == 0 {
            return Ok(0);
        }
        let mut mem_diff = 0isize;

        // active -> staging
        let active = std::mem::take(&mut self.active);
        let num_rows = self.num_active_rows;
        self.num_active_rows = 0;
        mem_diff -= self.active_slots_mem_size as isize;

        let frozen_batch = make_batch(self.schema.clone(), active, num_rows)?;

        let frozen_capacity_old = self.frozen.capacity();
        let mut cursor = Cursor::new(&mut self.frozen);
        cursor.seek(SeekFrom::End(0))?;
        write_ipc_compressed(&frozen_batch, &mut cursor)?;

        mem_diff += (self.frozen.capacity() - frozen_capacity_old) as isize;
        Ok(mem_diff)
    }
}

fn slot_size(len: usize, data_type: &DataType) -> usize {
    match data_type {
        DataType::Boolean => ceil(len, 8),
        DataType::Int8 => len,
        DataType::Int16 => len * 2,
        DataType::Int32 => len * 4,
        DataType::Int64 => len * 8,
        DataType::UInt8 => len,
        DataType::UInt16 => len * 2,
        DataType::UInt32 => len * 4,
        DataType::UInt64 => len * 8,
        DataType::Float32 => len * 4,
        DataType::Float64 => len * 8,
        DataType::Date32 => len * 4,
        DataType::Date64 => len * 8,
        DataType::Time32(TimeUnit::Second) => len * 4,
        DataType::Time32(TimeUnit::Millisecond) => len * 4,
        DataType::Time64(TimeUnit::Microsecond) => len * 8,
        DataType::Time64(TimeUnit::Nanosecond) => len * 8,
        // TODO: this is not accurate, but should be good enough for now
        DataType::Utf8 => len * 100 + len * 4,
        DataType::LargeUtf8 => len * 100 + len * 8,
        DataType::Decimal128(_, _) => len * 16,
        DataType::Dictionary(key_type, value_type) => {
            // TODO: this is not accurate, but should be good enough for now
            slot_size(len, key_type.as_ref()) + slot_size(len / 10, value_type.as_ref())
        }
        // TODO: this is not accurate, but should be good enough for now
        DataType::Binary => len * 100 + len * 4,
        DataType::LargeBinary => len * 100 + len * 8,
        DataType::FixedSizeBinary(s) => len * (*s as usize),
        DataType::Timestamp(_, _) => len * 8,
        dt => unimplemented!(
            "{}",
            format!("data type {dt} not supported in shuffle write")
        ),
    }
}

fn append_columns(
    to: &mut Box<dyn ArrayBuilder>,
    from: &Arc<dyn Array>,
    indices: &[usize],
    data_type: &DataType,
) {
    /// Append values from `from` to `to` using `indices`.
    macro_rules! append {
        ($arrowty:ident) => {{
            type B = paste::paste! {[< $arrowty Builder >]};
            type A = paste::paste! {[< $arrowty Array >]};
            let t = to.as_any_mut().downcast_mut::<B>().unwrap();
            let f = from.as_any().downcast_ref::<A>().unwrap();
            for &i in indices {
                if f.is_valid(i) {
                    t.append_value(f.value(i));
                } else {
                    t.append_null();
                }
            }
        }};
    }

    /// Some array builder (e.g. `FixedSizeBinary`) its `append_value` method returning
    /// a `Result`.
    macro_rules! append_unwrap {
        ($arrowty:ident) => {{
            type B = paste::paste! {[< $arrowty Builder >]};
            type A = paste::paste! {[< $arrowty Array >]};
            let t = to.as_any_mut().downcast_mut::<B>().unwrap();
            let f = from.as_any().downcast_ref::<A>().unwrap();
            for &i in indices {
                if f.is_valid(i) {
                    t.append_value(f.value(i)).unwrap();
                } else {
                    t.append_null();
                }
            }
        }};
    }

    /// Appends values from a dictionary array to a dictionary builder.
    macro_rules! append_dict {
        ($kt:ty, $builder:ty, $dict_array:ty) => {{
            let t = to.as_any_mut().downcast_mut::<$builder>().unwrap();
            let f = from
                .as_any()
                .downcast_ref::<DictionaryArray<$kt>>()
                .unwrap()
                .downcast_dict::<$dict_array>()
                .unwrap();
            for &i in indices {
                if f.is_valid(i) {
                    t.append_value(f.value(i));
                } else {
                    t.append_null();
                }
            }
        }};
    }

    macro_rules! append_dict_helper {
        ($kt:ident, $ty:ty, $dict_array:ty) => {{
            match $kt.as_ref() {
                DataType::Int8 => append_dict!(Int8Type, PrimitiveDictionaryBuilder<Int8Type, $ty>, $dict_array),
                DataType::Int16 => append_dict!(Int16Type, PrimitiveDictionaryBuilder<Int16Type, $ty>, $dict_array),
                DataType::Int32 => append_dict!(Int32Type, PrimitiveDictionaryBuilder<Int32Type, $ty>, $dict_array),
                DataType::Int64 => append_dict!(Int64Type, PrimitiveDictionaryBuilder<Int64Type, $ty>, $dict_array),
                DataType::UInt8 => append_dict!(UInt8Type, PrimitiveDictionaryBuilder<UInt8Type, $ty>, $dict_array),
                DataType::UInt16 => {
                    append_dict!(UInt16Type, PrimitiveDictionaryBuilder<UInt16Type, $ty>, $dict_array)
                }
                DataType::UInt32 => {
                    append_dict!(UInt32Type, PrimitiveDictionaryBuilder<UInt32Type, $ty>, $dict_array)
                }
                DataType::UInt64 => {
                    append_dict!(UInt64Type, PrimitiveDictionaryBuilder<UInt64Type, $ty>, $dict_array)
                }
                _ => unreachable!("Unknown key type for dictionary"),
            }
        }};
    }

    macro_rules! primitive_append_dict_helper {
        ($kt:ident, $vt:ident) => {
            match $vt.as_ref() {
                DataType::Int8 => {
                    append_dict_helper!($kt, Int8Type, Int8Array)
                }
                DataType::Int16 => {
                    append_dict_helper!($kt, Int16Type, Int16Array)
                }
                DataType::Int32 => {
                    append_dict_helper!($kt, Int32Type, Int32Array)
                }
                DataType::Int64 => {
                    append_dict_helper!($kt, Int64Type, Int64Array)
                }
                DataType::UInt8 => {
                    append_dict_helper!($kt, UInt8Type, UInt8Array)
                }
                DataType::UInt16 => {
                    append_dict_helper!($kt, UInt16Type, UInt16Array)
                }
                DataType::UInt32 => {
                    append_dict_helper!($kt, UInt32Type, UInt32Array)
                }
                DataType::UInt64 => {
                    append_dict_helper!($kt, UInt64Type, UInt64Array)
                }
                DataType::Float32 => {
                    append_dict_helper!($kt, Float32Type, Float32Array)
                }
                DataType::Float64 => {
                    append_dict_helper!($kt, Float64Type, Float64Array)
                }
                DataType::Decimal128(_, _) => {
                    append_dict_helper!($kt, Decimal128Type, Decimal128Array)
                }
                DataType::Timestamp(TimeUnit::Microsecond, _) => {
                    append_dict_helper!($kt, TimestampMicrosecondType, TimestampMicrosecondArray)
                }
                DataType::Date32 => {
                    append_dict_helper!($kt, Date32Type, Date32Array)
                }
                DataType::Date64 => {
                    append_dict_helper!($kt, Date64Type, Date64Array)
                }
                t => unimplemented!("{:?} is not supported for appending dictionary builder", t),
            }
        };
    }

    macro_rules! append_byte_dict {
        ($kt:ident, $byte_type:ty, $array_type:ty) => {{
            match $kt.as_ref() {
                DataType::Int8 => {
                    append_dict!(Int8Type, GenericByteDictionaryBuilder<Int8Type, $byte_type>, $array_type)
                }
                DataType::Int16 => {
                    append_dict!(Int16Type,  GenericByteDictionaryBuilder<Int16Type, $byte_type>, $array_type)
                }
                DataType::Int32 => {
                    append_dict!(Int32Type,  GenericByteDictionaryBuilder<Int32Type, $byte_type>, $array_type)
                }
                DataType::Int64 => {
                    append_dict!(Int64Type,  GenericByteDictionaryBuilder<Int64Type, $byte_type>, $array_type)
                }
                DataType::UInt8 => {
                    append_dict!(UInt8Type,  GenericByteDictionaryBuilder<UInt8Type, $byte_type>, $array_type)
                }
                DataType::UInt16 => {
                    append_dict!(UInt16Type, GenericByteDictionaryBuilder<UInt16Type, $byte_type>, $array_type)
                }
                DataType::UInt32 => {
                    append_dict!(UInt32Type, GenericByteDictionaryBuilder<UInt32Type, $byte_type>, $array_type)
                }
                DataType::UInt64 => {
                    append_dict!(UInt64Type, GenericByteDictionaryBuilder<UInt64Type, $byte_type>, $array_type)
                }
                _ => unreachable!("Unknown key type for dictionary"),
            }
        }};
    }

    match data_type {
        DataType::Boolean => append!(Boolean),
        DataType::Int8 => append!(Int8),
        DataType::Int16 => append!(Int16),
        DataType::Int32 => append!(Int32),
        DataType::Int64 => append!(Int64),
        DataType::UInt8 => append!(UInt8),
        DataType::UInt16 => append!(UInt16),
        DataType::UInt32 => append!(UInt32),
        DataType::UInt64 => append!(UInt64),
        DataType::Float32 => append!(Float32),
        DataType::Float64 => append!(Float64),
        DataType::Date32 => append!(Date32),
        DataType::Date64 => append!(Date64),
        DataType::Time32(TimeUnit::Second) => append!(Time32Second),
        DataType::Time32(TimeUnit::Millisecond) => append!(Time32Millisecond),
        DataType::Time64(TimeUnit::Microsecond) => append!(Time64Microsecond),
        DataType::Time64(TimeUnit::Nanosecond) => append!(Time64Nanosecond),
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            append!(TimestampMicrosecond)
        }
        DataType::Utf8 => append!(String),
        DataType::LargeUtf8 => append!(LargeString),
        DataType::Decimal128(_, _) => append!(Decimal128),
        DataType::Dictionary(key_type, value_type) if value_type.is_primitive() => {
            primitive_append_dict_helper!(key_type, value_type)
        }
        DataType::Dictionary(key_type, value_type)
            if matches!(value_type.as_ref(), DataType::Utf8) =>
        {
            append_byte_dict!(key_type, GenericStringType<i32>, StringArray)
        }
        DataType::Dictionary(key_type, value_type)
            if matches!(value_type.as_ref(), DataType::LargeUtf8) =>
        {
            append_byte_dict!(key_type, GenericStringType<i64>, LargeStringArray)
        }
        DataType::Dictionary(key_type, value_type)
            if matches!(value_type.as_ref(), DataType::Binary) =>
        {
            append_byte_dict!(key_type, GenericBinaryType<i32>, BinaryArray)
        }
        DataType::Dictionary(key_type, value_type)
            if matches!(value_type.as_ref(), DataType::LargeBinary) =>
        {
            append_byte_dict!(key_type, GenericBinaryType<i64>, LargeBinaryArray)
        }
        DataType::Binary => append!(Binary),
        DataType::LargeBinary => append!(LargeBinary),
        DataType::FixedSizeBinary(_) => append_unwrap!(FixedSizeBinary),
        t => unimplemented!(
            "{}",
            format!("data type {} not supported in shuffle write", t)
        ),
    }
}

struct SpillInfo {
    file: RefCountedTempFile,
    offsets: Vec<u64>,
}

struct ShuffleRepartitioner {
    output_data_file: String,
    output_index_file: String,
    schema: SchemaRef,
    buffered_partitions: Mutex<Vec<PartitionBuffer>>,
    spills: Mutex<Vec<SpillInfo>>,
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
}

struct ShuffleRepartitionerMetrics {
    /// metrics
    baseline: BaselineMetrics,

    /// count of spills during the execution of the operator
    spill_count: Count,

    /// total spilled bytes during the execution of the operator
    spilled_bytes: Count,
}

impl ShuffleRepartitionerMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            baseline: BaselineMetrics::new(metrics, partition),
            spill_count: MetricBuilder::new(metrics).spill_count(partition),
            spilled_bytes: MetricBuilder::new(metrics).spilled_bytes(partition),
        }
    }
}

impl ShuffleRepartitioner {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        partition_id: usize,
        output_data_file: String,
        output_index_file: String,
        schema: SchemaRef,
        partitioning: Partitioning,
        metrics: ShuffleRepartitionerMetrics,
        runtime: Arc<RuntimeEnv>,
        batch_size: usize,
    ) -> Self {
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

        Self {
            output_data_file,
            output_index_file,
            schema: schema.clone(),
            buffered_partitions: Mutex::new(
                (0..num_output_partitions)
                    .map(|_| PartitionBuffer::new(schema.clone(), batch_size))
                    .collect::<Vec<_>>(),
            ),
            spills: Mutex::new(vec![]),
            partitioning,
            num_output_partitions,
            runtime,
            metrics,
            reservation,
            hashes_buf,
            partition_ids,
        }
    }

    /// Shuffles rows in input batch into corresponding partition buffer.
    /// This function first calculates hashes for rows and then takes rows in same
    /// partition as a record batch which is appended into partition buffer.
    async fn insert_batch(&mut self, input: RecordBatch) -> Result<()> {
        if input.num_rows() == 0 {
            // skip empty batch
            return Ok(());
        }
        let _timer = self.metrics.baseline.elapsed_compute().timer();

        // NOTE: in shuffle writer exec, the output_rows metrics represents the
        // number of rows those are written to output data file.
        self.metrics.baseline.record_output(input.num_rows());

        let num_output_partitions = self.num_output_partitions;
        match &self.partitioning {
            Partitioning::Hash(exprs, _) => {
                let arrays = exprs
                    .iter()
                    .map(|expr| expr.evaluate(&input)?.into_array(input.num_rows()))
                    .collect::<Result<Vec<_>>>()?;

                // use identical seed as spark hash partition
                let hashes_buf = &mut self.hashes_buf[..arrays[0].len()];
                hashes_buf.fill(42_u32);

                // Hash arrays and compute buckets based on number of partitions
                let partition_ids = &mut self.partition_ids[..arrays[0].len()];
                create_hashes(&arrays, hashes_buf)?
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

                let mut mem_diff = 0;
                // For each interval of row indices of partition, taking rows from input batch and
                // appending into output buffer.
                for (partition_id, (&start, &end)) in partition_starts
                    .iter()
                    .tuple_windows()
                    .enumerate()
                    .filter(|(_, (start, end))| start < end)
                {
                    let mut buffered_partitions = self.buffered_partitions.lock().await;
                    let output = &mut buffered_partitions[partition_id];

                    // If the range of indices is not big enough, just appending the rows into
                    // active array builders instead of directly adding them as a record batch.
                    mem_diff +=
                        output.append_rows(input.columns(), &shuffled_partition_ids[start..end])?;
                }

                if mem_diff > 0 {
                    let mem_increase = mem_diff as usize;
                    if self.reservation.try_grow(mem_increase).is_err() {
                        self.spill().await?;
                        self.reservation.free();
                        self.reservation.try_grow(mem_increase)?;
                    }
                }
                if mem_diff < 0 {
                    let mem_used = self.reservation.size();
                    let mem_decrease = mem_used.min(-mem_diff as usize);
                    self.reservation.shrink(mem_decrease);
                }
            }
            Partitioning::UnknownPartitioning(n) if *n == 1 => {
                let mut buffered_partitions = self.buffered_partitions.lock().await;

                assert!(
                    buffered_partitions.len() == 1,
                    "Expected 1 partition but got {}",
                    buffered_partitions.len()
                );

                let output = &mut buffered_partitions[0];
                output.append_batch(&input)?;
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
        let _timer = self.metrics.baseline.elapsed_compute().timer();
        let num_output_partitions = self.num_output_partitions;
        let mut buffered_partitions = self.buffered_partitions.lock().await;
        let mut output_batches: Vec<Vec<u8>> = vec![vec![]; num_output_partitions];

        for i in 0..num_output_partitions {
            buffered_partitions[i].flush()?;
            output_batches[i] = std::mem::take(&mut buffered_partitions[i].frozen);
        }

        let mut spills = self.spills.lock().await;
        let output_spills = spills.drain(..).collect::<Vec<_>>();

        let data_file = self.output_data_file.clone();
        let index_file = self.output_index_file.clone();

        let mut offsets = vec![0; num_output_partitions + 1];
        let mut output_data = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(data_file)
            .map_err(|e| DataFusionError::Execution(format!("shuffle write error: {:?}", e)))?;

        for i in 0..num_output_partitions {
            offsets[i] = output_data.stream_position()?;
            output_data.write_all(&output_batches[i])?;
            output_batches[i].clear();

            // append partition in each spills
            for spill in &output_spills {
                let length = spill.offsets[i + 1] - spill.offsets[i];
                if length > 0 {
                    let mut spill_file =
                        BufReader::new(File::open(spill.file.path()).map_err(|e| {
                            DataFusionError::Execution(format!("shuffle write error: {:?}", e))
                        })?);
                    spill_file.seek(SeekFrom::Start(spill.offsets[i]))?;
                    std::io::copy(&mut spill_file.take(length), &mut output_data).map_err(|e| {
                        DataFusionError::Execution(format!("shuffle write error: {:?}", e))
                    })?;
                }
            }
        }
        output_data.flush()?;

        // add one extra offset at last to ease partition length computation
        offsets[num_output_partitions] = output_data
            .stream_position()
            .map_err(|e| DataFusionError::Execution(format!("shuffle write error: {:?}", e)))?;
        let mut output_index =
            BufWriter::new(File::create(index_file).map_err(|e| {
                DataFusionError::Execution(format!("shuffle write error: {:?}", e))
            })?);
        for offset in offsets {
            output_index
                .write_all(&(offset as i64).to_le_bytes()[..])
                .map_err(|e| DataFusionError::Execution(format!("shuffle write error: {:?}", e)))?;
        }
        output_index.flush()?;

        let used = self.reservation.size();
        self.reservation.shrink(used);

        // shuffle writer always has empty output
        Ok(Box::pin(EmptyStream::try_new(self.schema.clone())?))
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

    async fn spill(&self) -> Result<usize> {
        log::debug!(
            "ShuffleRepartitioner spilling shuffle data of {} to disk while inserting ({} time(s) so far)",
            self.used(),
            self.spill_count()
        );

        let mut buffered_partitions = self.buffered_partitions.lock().await;
        // we could always get a chance to free some memory as long as we are holding some
        if buffered_partitions.len() == 0 {
            return Ok(0);
        }

        let spillfile = self
            .runtime
            .disk_manager
            .create_tmp_file("shuffle writer spill")?;
        let offsets = spill_into(
            &mut buffered_partitions,
            spillfile.path(),
            self.num_output_partitions,
        )
        .await?;

        let mut spills = self.spills.lock().await;
        let used = self.reservation.size();
        self.metrics.spill_count.add(1);
        self.metrics.spilled_bytes.add(used);
        spills.push(SpillInfo {
            file: spillfile,
            offsets,
        });
        Ok(used)
    }
}

/// consume the `buffered_partitions` and do spill into a single temp shuffle output file
async fn spill_into(
    buffered_partitions: &mut [PartitionBuffer],
    path: &Path,
    num_output_partitions: usize,
) -> Result<Vec<u64>> {
    let mut output_batches: Vec<Vec<u8>> = vec![vec![]; num_output_partitions];

    for i in 0..num_output_partitions {
        buffered_partitions[i].flush()?;
        output_batches[i] = std::mem::take(&mut buffered_partitions[i].frozen);
    }
    let path = path.to_owned();

    task::spawn_blocking(move || {
        let mut offsets = vec![0; num_output_partitions + 1];
        let mut spill_data = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;

        for i in 0..num_output_partitions {
            offsets[i] = spill_data.stream_position()?;
            spill_data.write_all(&output_batches[i])?;
            output_batches[i].clear();
        }
        // add one extra offset at last to ease partition length computation
        offsets[num_output_partitions] = spill_data.stream_position()?;
        Ok(offsets)
    })
    .await
    .map_err(|e| DataFusionError::Execution(format!("Error occurred while spilling {}", e)))?
}

impl Debug for ShuffleRepartitioner {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShuffleRepartitioner")
            .field("memory_used", &self.used())
            .field("spilled_bytes", &self.spilled_bytes())
            .field("spilled_count", &self.spill_count())
            .finish()
    }
}

async fn external_shuffle(
    mut input: SendableRecordBatchStream,
    partition_id: usize,
    output_data_file: String,
    output_index_file: String,
    partitioning: Partitioning,
    metrics: ShuffleRepartitionerMetrics,
    context: Arc<TaskContext>,
) -> Result<SendableRecordBatchStream> {
    let schema = input.schema();
    let mut repartitioner = ShuffleRepartitioner::new(
        partition_id,
        output_data_file,
        output_index_file,
        schema.clone(),
        partitioning,
        metrics,
        context.runtime_env(),
        context.session_config().batch_size(),
    );

    while let Some(batch) = input.next().await {
        let batch = batch?;
        repartitioner.insert_batch(batch).await?;
    }
    repartitioner.shuffle_write().await
}

fn new_array_builders(schema: &SchemaRef, batch_size: usize) -> Vec<Box<dyn ArrayBuilder>> {
    schema
        .fields()
        .iter()
        .map(|field| {
            let dt = field.data_type();
            if matches!(dt, DataType::Dictionary(_, _)) {
                make_dict_builder(dt, batch_size)
            } else {
                make_builder(dt, batch_size)
            }
        })
        .collect::<Vec<_>>()
}

macro_rules! primitive_dict_builder_inner_helper {
    ($kt:ty, $vt:ty, $capacity:ident) => {
        Box::new(PrimitiveDictionaryBuilder::<$kt, $vt>::with_capacity(
            $capacity,
            $capacity / 100,
        ))
    };
}

macro_rules! primitive_dict_builder_helper {
    ($kt:ty, $vt:ident, $capacity:ident) => {
        match $vt.as_ref() {
            DataType::Int8 => {
                primitive_dict_builder_inner_helper!($kt, Int8Type, $capacity)
            }
            DataType::Int16 => {
                primitive_dict_builder_inner_helper!($kt, Int16Type, $capacity)
            }
            DataType::Int32 => {
                primitive_dict_builder_inner_helper!($kt, Int32Type, $capacity)
            }
            DataType::Int64 => {
                primitive_dict_builder_inner_helper!($kt, Int64Type, $capacity)
            }
            DataType::UInt8 => {
                primitive_dict_builder_inner_helper!($kt, UInt8Type, $capacity)
            }
            DataType::UInt16 => {
                primitive_dict_builder_inner_helper!($kt, UInt16Type, $capacity)
            }
            DataType::UInt32 => {
                primitive_dict_builder_inner_helper!($kt, UInt32Type, $capacity)
            }
            DataType::UInt64 => {
                primitive_dict_builder_inner_helper!($kt, UInt64Type, $capacity)
            }
            DataType::Float32 => {
                primitive_dict_builder_inner_helper!($kt, Float32Type, $capacity)
            }
            DataType::Float64 => {
                primitive_dict_builder_inner_helper!($kt, Float64Type, $capacity)
            }
            DataType::Decimal128(p, s) => {
                let keys_builder = PrimitiveBuilder::<$kt>::new();
                let values_builder =
                    Decimal128Builder::new().with_data_type(DataType::Decimal128(*p, *s));
                Box::new(
                    PrimitiveDictionaryBuilder::<$kt, Decimal128Type>::new_from_empty_builders(
                        keys_builder,
                        values_builder,
                    ),
                )
            }
            DataType::Timestamp(TimeUnit::Microsecond, timezone) => {
                let keys_builder = PrimitiveBuilder::<$kt>::new();
                let values_builder = TimestampMicrosecondBuilder::new()
                    .with_data_type(DataType::Timestamp(TimeUnit::Microsecond, timezone.clone()));
                Box::new(
                    PrimitiveDictionaryBuilder::<$kt, TimestampMicrosecondType>::new_from_empty_builders(
                        keys_builder,
                        values_builder,
                    ),
                )
            }
            DataType::Date32 => {
                primitive_dict_builder_inner_helper!($kt, Date32Type, $capacity)
            }
            DataType::Date64 => {
                primitive_dict_builder_inner_helper!($kt, Date64Type, $capacity)
            }
            t => unimplemented!("{:?} is not supported", t),
        }
    };
}

macro_rules! byte_dict_builder_inner_helper {
    ($kt:ty, $capacity:ident, $builder:ident) => {
        Box::new($builder::<$kt>::with_capacity(
            $capacity,
            $capacity / 100,
            $capacity,
        ))
    };
}

/// Returns a dictionary array builder with capacity `capacity` that corresponds to the datatype
/// `DataType` This function is useful to construct arrays from an arbitrary vectors with
/// known/expected schema.
/// TODO: move this to the upstream.
fn make_dict_builder(datatype: &DataType, capacity: usize) -> Box<dyn ArrayBuilder> {
    match datatype {
        DataType::Dictionary(key_type, value_type) if value_type.is_primitive() => {
            match key_type.as_ref() {
                DataType::Int8 => primitive_dict_builder_helper!(Int8Type, value_type, capacity),
                DataType::Int16 => primitive_dict_builder_helper!(Int16Type, value_type, capacity),
                DataType::Int32 => primitive_dict_builder_helper!(Int32Type, value_type, capacity),
                DataType::Int64 => primitive_dict_builder_helper!(Int64Type, value_type, capacity),
                DataType::UInt8 => primitive_dict_builder_helper!(UInt8Type, value_type, capacity),
                DataType::UInt16 => {
                    primitive_dict_builder_helper!(UInt16Type, value_type, capacity)
                }
                DataType::UInt32 => {
                    primitive_dict_builder_helper!(UInt32Type, value_type, capacity)
                }
                DataType::UInt64 => {
                    primitive_dict_builder_helper!(UInt64Type, value_type, capacity)
                }
                _ => unreachable!(""),
            }
        }
        DataType::Dictionary(key_type, value_type)
            if matches!(value_type.as_ref(), DataType::Utf8) =>
        {
            match key_type.as_ref() {
                DataType::Int8 => {
                    byte_dict_builder_inner_helper!(Int8Type, capacity, StringDictionaryBuilder)
                }
                DataType::Int16 => {
                    byte_dict_builder_inner_helper!(Int16Type, capacity, StringDictionaryBuilder)
                }
                DataType::Int32 => {
                    byte_dict_builder_inner_helper!(Int32Type, capacity, StringDictionaryBuilder)
                }
                DataType::Int64 => {
                    byte_dict_builder_inner_helper!(Int64Type, capacity, StringDictionaryBuilder)
                }
                DataType::UInt8 => {
                    byte_dict_builder_inner_helper!(UInt8Type, capacity, StringDictionaryBuilder)
                }
                DataType::UInt16 => {
                    byte_dict_builder_inner_helper!(UInt16Type, capacity, StringDictionaryBuilder)
                }
                DataType::UInt32 => {
                    byte_dict_builder_inner_helper!(UInt32Type, capacity, StringDictionaryBuilder)
                }
                DataType::UInt64 => {
                    byte_dict_builder_inner_helper!(UInt64Type, capacity, StringDictionaryBuilder)
                }
                _ => unreachable!(""),
            }
        }
        DataType::Dictionary(key_type, value_type)
            if matches!(value_type.as_ref(), DataType::LargeUtf8) =>
        {
            match key_type.as_ref() {
                DataType::Int8 => byte_dict_builder_inner_helper!(
                    Int8Type,
                    capacity,
                    LargeStringDictionaryBuilder
                ),
                DataType::Int16 => byte_dict_builder_inner_helper!(
                    Int16Type,
                    capacity,
                    LargeStringDictionaryBuilder
                ),
                DataType::Int32 => byte_dict_builder_inner_helper!(
                    Int32Type,
                    capacity,
                    LargeStringDictionaryBuilder
                ),
                DataType::Int64 => byte_dict_builder_inner_helper!(
                    Int64Type,
                    capacity,
                    LargeStringDictionaryBuilder
                ),
                DataType::UInt8 => byte_dict_builder_inner_helper!(
                    UInt8Type,
                    capacity,
                    LargeStringDictionaryBuilder
                ),
                DataType::UInt16 => {
                    byte_dict_builder_inner_helper!(
                        UInt16Type,
                        capacity,
                        LargeStringDictionaryBuilder
                    )
                }
                DataType::UInt32 => {
                    byte_dict_builder_inner_helper!(
                        UInt32Type,
                        capacity,
                        LargeStringDictionaryBuilder
                    )
                }
                DataType::UInt64 => {
                    byte_dict_builder_inner_helper!(
                        UInt64Type,
                        capacity,
                        LargeStringDictionaryBuilder
                    )
                }
                _ => unreachable!(""),
            }
        }
        DataType::Dictionary(key_type, value_type)
            if matches!(value_type.as_ref(), DataType::Binary) =>
        {
            match key_type.as_ref() {
                DataType::Int8 => {
                    byte_dict_builder_inner_helper!(Int8Type, capacity, BinaryDictionaryBuilder)
                }
                DataType::Int16 => {
                    byte_dict_builder_inner_helper!(Int16Type, capacity, BinaryDictionaryBuilder)
                }
                DataType::Int32 => {
                    byte_dict_builder_inner_helper!(Int32Type, capacity, BinaryDictionaryBuilder)
                }
                DataType::Int64 => {
                    byte_dict_builder_inner_helper!(Int64Type, capacity, BinaryDictionaryBuilder)
                }
                DataType::UInt8 => {
                    byte_dict_builder_inner_helper!(UInt8Type, capacity, BinaryDictionaryBuilder)
                }
                DataType::UInt16 => {
                    byte_dict_builder_inner_helper!(UInt16Type, capacity, BinaryDictionaryBuilder)
                }
                DataType::UInt32 => {
                    byte_dict_builder_inner_helper!(UInt32Type, capacity, BinaryDictionaryBuilder)
                }
                DataType::UInt64 => {
                    byte_dict_builder_inner_helper!(UInt64Type, capacity, BinaryDictionaryBuilder)
                }
                _ => unreachable!(""),
            }
        }
        DataType::Dictionary(key_type, value_type)
            if matches!(value_type.as_ref(), DataType::LargeBinary) =>
        {
            match key_type.as_ref() {
                DataType::Int8 => byte_dict_builder_inner_helper!(
                    Int8Type,
                    capacity,
                    LargeBinaryDictionaryBuilder
                ),
                DataType::Int16 => byte_dict_builder_inner_helper!(
                    Int16Type,
                    capacity,
                    LargeBinaryDictionaryBuilder
                ),
                DataType::Int32 => byte_dict_builder_inner_helper!(
                    Int32Type,
                    capacity,
                    LargeBinaryDictionaryBuilder
                ),
                DataType::Int64 => byte_dict_builder_inner_helper!(
                    Int64Type,
                    capacity,
                    LargeBinaryDictionaryBuilder
                ),
                DataType::UInt8 => byte_dict_builder_inner_helper!(
                    UInt8Type,
                    capacity,
                    LargeBinaryDictionaryBuilder
                ),
                DataType::UInt16 => {
                    byte_dict_builder_inner_helper!(
                        UInt16Type,
                        capacity,
                        LargeBinaryDictionaryBuilder
                    )
                }
                DataType::UInt32 => {
                    byte_dict_builder_inner_helper!(
                        UInt32Type,
                        capacity,
                        LargeBinaryDictionaryBuilder
                    )
                }
                DataType::UInt64 => {
                    byte_dict_builder_inner_helper!(
                        UInt64Type,
                        capacity,
                        LargeBinaryDictionaryBuilder
                    )
                }
                _ => unreachable!(""),
            }
        }
        t => panic!("Data type {t:?} is not currently supported"),
    }
}

fn make_batch(
    schema: SchemaRef,
    mut arrays: Vec<Box<dyn ArrayBuilder>>,
    row_count: usize,
) -> ArrowResult<RecordBatch> {
    let columns = arrays.iter_mut().map(|array| array.finish()).collect();
    let options = RecordBatchOptions::new().with_row_count(Option::from(row_count));
    RecordBatch::try_new_with_options(schema, columns, &options)
}

/// Checksum algorithms for writing IPC bytes.
#[derive(Clone)]
pub(crate) enum Checksum {
    /// CRC32 checksum algorithm.
    CRC32(Hasher),
    /// Adler32 checksum algorithm.
    Adler32(Adler32),
}

impl Checksum {
    pub(crate) fn try_new(algo: i32, initial_opt: Option<u32>) -> CometResult<Self> {
        match algo {
            0 => {
                let hasher = if let Some(initial) = initial_opt {
                    Hasher::new_with_initial(initial)
                } else {
                    Hasher::new()
                };
                Ok(Checksum::CRC32(hasher))
            }
            1 => {
                let hasher = if let Some(initial) = initial_opt {
                    // Note that Adler32 initial state is not zero.
                    // i.e., `Adler32::from_checksum(0)` is not the same as `Adler32::new()`.
                    Adler32::from_checksum(initial)
                } else {
                    Adler32::new()
                };
                Ok(Checksum::Adler32(hasher))
            }
            _ => Err(CometError::Internal(
                "Unsupported checksum algorithm".to_string(),
            )),
        }
    }

    pub(crate) fn update(&mut self, cursor: &mut Cursor<&mut Vec<u8>>) -> CometResult<()> {
        match self {
            Checksum::CRC32(hasher) => {
                std::io::Seek::seek(cursor, SeekFrom::Start(0))?;
                hasher.update(cursor.chunk());
                Ok(())
            }
            Checksum::Adler32(hasher) => {
                std::io::Seek::seek(cursor, SeekFrom::Start(0))?;
                hasher.write(cursor.chunk());
                Ok(())
            }
        }
    }

    pub(crate) fn finalize(self) -> u32 {
        match self {
            Checksum::CRC32(hasher) => hasher.finalize(),
            Checksum::Adler32(hasher) => hasher.finish(),
        }
    }
}

/// Writes given record batch as Arrow IPC bytes into given writer.
/// Returns number of bytes written.
pub(crate) fn write_ipc_compressed<W: Write + Seek>(
    batch: &RecordBatch,
    output: &mut W,
) -> Result<usize> {
    if batch.num_rows() == 0 {
        return Ok(0);
    }
    let start_pos = output.stream_position()?;

    // write ipc_length placeholder
    output.write_all(&[0u8; 8])?;

    // write ipc data
    // TODO: make compression level configurable
    let mut arrow_writer = StreamWriter::try_new(zstd::Encoder::new(output, 1)?, &batch.schema())?;
    arrow_writer.write(batch)?;
    arrow_writer.finish()?;

    let zwriter = arrow_writer.into_inner()?;
    let output = zwriter.finish()?;
    let end_pos = output.stream_position()?;
    let ipc_length = end_pos - start_pos - 8;

    // fill ipc length
    output.seek(SeekFrom::Start(start_pos))?;
    output.write_all(&ipc_length.to_le_bytes()[..])?;

    output.seek(SeekFrom::Start(end_pos))?;
    Ok((end_pos - start_pos) as usize)
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
        self.schema.clone()
    }
}

#[cfg(test)]
mod test {
    use super::*;

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
}
