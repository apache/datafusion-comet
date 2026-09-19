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

//! Prepares a broadcast hash table once for tasks probing the same build at the
//! same time. `BroadcastInputExec` captures the Spark broadcast's identity,
//! schema and storage owner without decoding rows during planning. On a miss,
//! one task opens its JVM stream, copies admitted Arrow batches into native
//! buffers and asks DataFusion to prepare an immutable hash build. Other tasks
//! with a compatible join can use that build with their own probe and metrics.
//!
//! The lookup retains only weak references. Active probe streams keep the
//! prepared build alive. The last lease frees it and returns its Spark storage
//! charge while the owner is active; executor retirement can return the charge
//! earlier without revoking probes. Rejected admission or an ineligible join
//! uses an ordinary task-local hash join with a fresh broadcast stream.

use std::fmt::{Debug, Display, Formatter};
use std::mem::size_of;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{
    make_array, Array, ArrayRef, Capacities, MutableArrayData, RecordBatch, RecordBatchOptions,
    StringArray,
};
use arrow::compute::{cast_with_options, CastOptions};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::ffi_stream::FFI_ArrowArrayStream;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{
    internal_err, resources_datafusion_err, DataFusionError, JoinType, Result,
};
use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{
    Boundedness, ChildrenPropertiesMode, EmissionType, ReplaceChildrenOptions,
};
use datafusion::physical_plan::joins::PreparedHashJoinBuild;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use futures::{Stream, TryStreamExt};
use jni::objects::{Global, JObject, JValue};
use once_cell::sync::Lazy;
use parking_lot::Mutex;

use super::broadcast_cache::{BuildCache, BuildKey};
use super::scan::import_column;
use super::{copy_array, AlignedArrowStreamReader};
use crate::errors::{CometError, CometResult};
use crate::jvm_bridge::JVMClasses;

type PreparedCache = BuildCache<PreparedHashJoinBuild>;
type CacheGeneration = Option<(i64, Arc<PreparedCache>)>;

// DataFusion charges each retained batch before polling for the next one.
// Bound the extra charge held by the producer during that handoff.
const MAX_COPY_BATCH_BYTES: usize = 32 * 1024 * 1024;

// Lookup crosses Spark task/plan boundaries while builds are active. This
// executor-lifetime registry holds only weak references, capped at 64 keys.
// The registry fences SparkEnv generations; keys use actual Broadcast.id, not
// logical plan identity. Plugin shutdown retires the cache before SparkEnv.
static CACHE: Lazy<Mutex<CacheGeneration>> = Lazy::new(|| Mutex::new(None));

/// Returns this runtime's bounded cache, creating metadata only. Existing
/// probes retain their prepared builds after shutdown; a new runtime gets a new cache.
fn cache(generation: i64) -> Arc<PreparedCache> {
    let (current, retired) = {
        let mut cache = CACHE.lock();
        if let Some((existing, current)) = &*cache {
            if *existing == generation {
                return Arc::clone(current);
            }
            if *existing > generation {
                // An old task racing environment replacement must never retire
                // the newer environment or publish into its cache.
                return Arc::new(BuildCache::new(0));
            }
        }
        let current = Arc::new(BuildCache::new(64));
        let retired = cache.replace((generation, Arc::clone(&current)));
        (current, retired)
    };
    if let Some((_, retired)) = retired {
        retired.clear();
    }
    current
}

/// Retires all native broadcast lookups before Spark releases storage accounting.
/// Prepared values still leased by outputs/tasks retain their original owner.
pub(crate) fn clear_broadcast_cache() {
    let old = CACHE.lock().take();
    if let Some((_, old)) = old {
        old.clear();
    }
}

/// Charges temporary broadcast copies and the prepared hash table to Spark
/// executor storage instead of a task's memory manager. The captured JVM
/// owner enforces one cap across concurrent builds and live leases in this
/// SparkEnv generation, even when their lookup entries have been retired.
#[derive(Debug)]
struct BroadcastMemoryPool {
    owner: Arc<Global<JObject<'static>>>,
    limit: usize,
    used: AtomicUsize,
}

impl BroadcastMemoryPool {
    /// Requests an all-or-nothing storage grant without waiting for active probes.
    fn acquire(&self, bytes: usize) -> CometResult<i64> {
        JVMClasses::with_env(|env| {
            Ok(env
                .call_method(
                    self.owner.as_obj(),
                    jni::jni_str!("acquireMemory"),
                    jni::jni_sig!("(J)J"),
                    &[JValue::Long(bytes as i64)],
                )?
                .j()?)
        })
    }

    /// Returns an existing grant to its captured environment. A retired JVM
    /// owner accounts late releases locally without touching a replacement env.
    fn release(&self, bytes: usize) -> CometResult<()> {
        JVMClasses::with_env(|env| {
            env.call_method(
                self.owner.as_obj(),
                jni::jni_str!("releaseMemory"),
                jni::jni_sig!("(J)V"),
                &[JValue::Long(bytes as i64)],
            )?;
            Ok(())
        })
    }
}

impl Display for BroadcastMemoryPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(self, f)
    }
}

/// DataFusion reservations acquire storage in full or fail preparation. Shrinking
/// a reservation releases its grant through the captured SparkEnv owner.
impl MemoryPool for BroadcastMemoryPool {
    fn name(&self) -> &str {
        "CometBroadcastMemoryPool"
    }

    fn grow(&self, reservation: &MemoryReservation, bytes: usize) {
        self.try_grow(reservation, bytes)
            .expect("broadcast allocations must be admitted");
    }

    fn shrink(&self, _: &MemoryReservation, bytes: usize) {
        self.release(bytes)
            .expect("broadcast storage grant release failed");
        self.used.fetch_sub(bytes, Ordering::Relaxed);
    }

    fn try_grow(&self, _: &MemoryReservation, bytes: usize) -> Result<()> {
        if bytes == 0 {
            return Ok(());
        }
        // Reject a build allocation above its byte cap before asking Spark.
        // Spark's owner still checks the total across concurrent loader pools.
        if bytes > self.limit.saturating_sub(self.reserved()) {
            return Err(DataFusionError::ResourcesExhausted(
                "Broadcast build allocation exceeds its byte cap".into(),
            ));
        }
        if self.acquire(bytes)? == 0 {
            return Err(resources_datafusion_err!(
                "Broadcast cache storage admission denied"
            ));
        }
        self.used.fetch_add(bytes, Ordering::Relaxed);
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }
    fn memory_limit(&self) -> MemoryLimit {
        MemoryLimit::Finite(self.limit)
    }
}

/// Task-owned broadcast input whose declared schema permits planning without
/// decoding Arrow. Each `open` creates a new stream, so a task can replay its
/// broadcast after a refused cache build. Prepared cache values never retain
/// the JVM input or its decoder.
#[derive(Clone, Debug)]
pub(crate) struct BroadcastInputExec {
    input: Arc<Global<JObject<'static>>>,
    owner: Arc<Global<JObject<'static>>>,
    generation: i64,
    broadcast_id: i64,
    limit: usize,
    properties: Arc<PlanProperties>,
}

impl BroadcastInputExec {
    /// Recognizes a JVM carrier and captures immutable metadata without opening
    /// its stream. Ordinary Arrow inputs return `None`. Invalid carrier metadata
    /// or JNI failure returns an error before ownership of any stream transfers.
    pub fn try_new(
        input: Arc<Global<JObject<'static>>>,
        types: &[DataType],
    ) -> CometResult<Option<Self>> {
        let metadata = JVMClasses::with_env(|env| {
            if !env.is_instance_of(
                input.as_obj(),
                jni::strings::JNIString::new("org/apache/spark/sql/comet/CometBroadcastInput"),
            )? {
                return Ok::<_, CometError>(None);
            }
            let broadcast_id = env
                .call_method(
                    input.as_obj(),
                    jni::jni_str!("getBroadcastId"),
                    jni::jni_sig!("()J"),
                    &[],
                )?
                .j()?;
            let owner = env
                .call_method(
                    input.as_obj(),
                    jni::jni_str!("getMemoryManager"),
                    jni::jni_sig!("()Lorg/apache/spark/CometBroadcastMemoryManager;"),
                    &[],
                )?
                .l()?;
            let generation = env
                .call_method(
                    &owner,
                    jni::jni_str!("getGeneration"),
                    jni::jni_sig!("()J"),
                    &[],
                )?
                .j()?;
            let limit = env
                .call_method(&owner, jni::jni_str!("getLimit"), jni::jni_sig!("()J"), &[])?
                .j()?;
            let owner = Arc::new(jni_new_global_ref!(env, owner)?);
            Ok(Some((owner, generation, broadcast_id, limit)))
        })?;
        let Some((owner, generation, broadcast_id, limit)) = metadata else {
            return Ok(None);
        };
        let limit = usize::try_from(limit)
            .map_err(|_| CometError::Config("Invalid broadcast cache cap".into()))?;
        let schema = Arc::new(Schema::new(
            types
                .iter()
                .enumerate()
                .map(|(i, dt)| Field::new(format!("col_{i}"), dt.clone(), true))
                .collect::<Vec<_>>(),
        ));
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Some(Self {
            input,
            owner,
            generation,
            broadcast_id,
            limit,
            properties,
        }))
    }

    /// Opens a fresh JVM decoder and transfers its C stream into this task's
    /// native reader. Java retains only the emptied wrapper for completion cleanup.
    fn open(&self) -> CometResult<AlignedArrowStreamReader> {
        JVMClasses::with_env(|env| {
            let stream = env
                .call_method(
                    self.input.as_obj(),
                    jni::jni_str!("openStream"),
                    jni::jni_sig!("()Lorg/apache/arrow/c/ArrowArrayStream;"),
                    &[],
                )?
                .l()?;
            let address =
                unsafe { jni_call!(env, arrow_array_stream(&stream).memory_address() -> i64) }?;
            // SAFETY: openStream returns a fresh owned C stream. from_raw moves
            // its release callback out, leaving the Java wrapper empty.
            unsafe { AlignedArrowStreamReader::from_raw(address as *mut FFI_ArrowArrayStream) }
                .map_err(CometError::from)
        })
    }

    /// Produces a lazy stream. With `pool`, every payload is deeply copied into
    /// admitted native buffers; without it, this is ordinary task-local decoding.
    fn stream(self: &Arc<Self>, pool: Option<Arc<dyn MemoryPool>>) -> SendableRecordBatchStream {
        let copy_reservation = pool
            .as_ref()
            .map(|pool| MemoryConsumer::new("Broadcast native copy").register(pool));
        Box::pin(BroadcastStream {
            source: Arc::clone(self),
            reader: None,
            pending_batch: None,
            copy_reservation,
        })
    }
}

impl DisplayAs for BroadcastInputExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CometBroadcastInputExec: broadcast={}",
            self.broadcast_id
        )
    }
}

impl ExecutionPlan for BroadcastInputExec {
    fn name(&self) -> &str {
        "CometBroadcastInputExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return internal_err!("Broadcast input has no children");
        }
        Ok(self)
    }
    fn execute(&self, partition: usize, _: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("Broadcast input has one partition");
        }
        Ok(Arc::new(self.clone()).stream(None))
    }
}

/// Task-local lazy decoder stream. During preparation, its temporary copy
/// grant overlaps DataFusion's reservation of the previous batch until the
/// next poll. Ordinary joins read the same source without a copy grant.
/// Neither the stream nor its JVM reader is cached.
struct BroadcastStream {
    source: Arc<BroadcastInputExec>,
    reader: Option<AlignedArrowStreamReader>,
    pending_batch: Option<RecordBatch>,
    copy_reservation: Option<MemoryReservation>,
}

impl BroadcastStream {
    /// Releases the last temporary copy grant and reads or continues splitting
    /// one batch. Cache preparation admits a conservative bound before copying
    /// supported physical buffers. An ineligible encoding returns a resource
    /// error so the caller can reopen the broadcast for ordinary execution.
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if let Some(reservation) = &mut self.copy_reservation {
            reservation.free();
        }
        if self.reader.is_none() {
            self.reader = Some(self.source.open()?);
        }
        let already_split = self.pending_batch.is_some();
        let batch = match self.pending_batch.take() {
            Some(batch) => batch,
            None => {
                let Some(batch) = self.reader.as_mut().unwrap().next() else {
                    return Ok(None);
                };
                batch?
            }
        };
        let schema = self.source.schema();
        if batch.num_columns() != schema.fields().len() {
            return internal_err!("Broadcast column count mismatch");
        }
        let rows = if self.copy_reservation.is_some() {
            copy_batch_rows(&batch, &schema, MAX_COPY_BATCH_BYTES, already_split)?
        } else {
            batch.num_rows()
        };
        let batch = if rows < batch.num_rows() {
            self.pending_batch = Some(batch.slice(rows, batch.num_rows() - rows));
            batch.slice(0, rows)
        } else {
            batch
        };
        let columns = if let Some(reservation) = &mut self.copy_reservation {
            copy_broadcast_columns(&batch, &schema, reservation)?
        } else {
            cast_uncached_broadcast_columns(&batch, &schema)?
        };
        Ok(Some(RecordBatch::try_new_with_options(
            schema,
            columns,
            &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
        )?))
    }
}

/// Replay through ScanExec's UTF-8 decoding and dictionary unpacking before
/// ScanStream's safe casts. Cache-ineligible input still needs the ordinary
/// FFI validation, and its physical encoding may differ from the declared schema.
fn cast_uncached_broadcast_columns(batch: &RecordBatch, schema: &Schema) -> Result<Vec<ArrayRef>> {
    let options = CastOptions::default();
    batch
        .columns()
        .iter()
        .zip(schema.fields())
        .map(|(array, field)| {
            let array = import_column(array)?;
            if array.data_type() == field.data_type() {
                Ok(array)
            } else {
                Ok(cast_with_options(&array, field.data_type(), &options)?)
            }
        })
        .collect()
}

impl Stream for BroadcastStream {
    type Item = Result<RecordBatch>;
    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.next_batch().transpose())
    }
}

impl RecordBatchStream for BroadcastStream {
    fn schema(&self) -> SchemaRef {
        self.source.schema()
    }
}

/// Active Utf8 bytes include bytes behind null rows, but exclude a slice's
/// unrelated prefix/suffix. Read offsets only, before allocating the copy.
fn utf8_value_span(array: &StringArray) -> usize {
    let offsets = array.value_offsets();
    (offsets[offsets.len() - 1] - offsets[0]) as usize
}

/// Bound one explicitly sized Utf8 copy, including offsets, optional validity,
/// up to 64-byte padding per buffer, and the existing per-array metadata allowance.
fn utf8_copy_bytes(rows: usize, values: usize) -> Result<usize> {
    rows.checked_add(1)
        .and_then(|rows| rows.checked_mul(size_of::<i32>()))
        .and_then(|bytes| bytes.checked_add(values))
        .and_then(|bytes| bytes.checked_add(rows.div_ceil(8)))
        .and_then(|bytes| bytes.checked_add(3 * 64 + 1024))
        .ok_or_else(|| resources_datafusion_err!("Broadcast copy size overflow"))
}

/// Bound copied bytes, not source capacity: slices share their original
/// buffers but own only their rows after copying.
fn broadcast_copy_bytes(batch: &RecordBatch, schema: &Schema) -> Result<usize> {
    let mut bound = 0_usize;
    for (column, field) in batch.columns().iter().zip(schema.fields()) {
        if column.data_type() != field.data_type() {
            return Err(resources_datafusion_err!(
                "Broadcast physical encoding cannot be cached"
            ));
        }
        let bytes = if column.data_type() == &DataType::Utf8 {
            let array = column
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Utf8 array");
            utf8_copy_bytes(array.len(), utf8_value_span(array))?
        } else {
            let rows = column.len();
            let values = match column.data_type() {
                DataType::Boolean => Some(rows.div_ceil(8)),
                DataType::Null => Some(0),
                DataType::FixedSizeBinary(width) => usize::try_from(*width)
                    .ok()
                    .and_then(|width| rows.checked_mul(width)),
                ty => ty
                    .primitive_width()
                    .and_then(|width| rows.checked_mul(width)),
            };
            values
                .and_then(|values| values.checked_add(rows.div_ceil(8)))
                .and_then(|bytes| bytes.checked_add(3 * 64 + 1024))
                .ok_or_else(|| resources_datafusion_err!("Broadcast copy size overflow"))?
        };
        bound = bound
            .checked_add(bytes)
            .ok_or_else(|| resources_datafusion_err!("Broadcast copy size overflow"))?;
    }
    Ok(bound)
}

/// Largest prefix whose independent native copy fits one handoff.
/// Keep small batches whole to avoid extra concatenation work. Once a large
/// batch is split, continue until all its rows are copied. DataFusion separately
/// admits the compact output while the copied input batches are still live.
fn copy_batch_rows(
    batch: &RecordBatch,
    schema: &Schema,
    target: usize,
    already_split: bool,
) -> Result<usize> {
    if batch.num_rows() == 0 {
        return Ok(0);
    }
    let bytes = broadcast_copy_bytes(batch, schema)?;
    if bytes <= target || (!already_split && bytes <= 2 * target) {
        return Ok(batch.num_rows());
    }
    let mut fits = 1;
    let mut too_large = batch.num_rows();
    while fits + 1 < too_large {
        let middle = fits + (too_large - fits) / 2;
        if broadcast_copy_bytes(&batch.slice(0, middle), schema)? <= target {
            fits = middle;
        } else {
            too_large = middle;
        }
    }
    Ok(fits)
}

/// Validate every column and admit all copies before allocating any of them.
/// Count aliases separately: each output column owns an independent copy.
fn copy_broadcast_columns(
    batch: &RecordBatch,
    schema: &Schema,
    reservation: &mut MemoryReservation,
) -> Result<Vec<ArrayRef>> {
    // FFI imports do not validate UTF-8. Check raw ArrayData before copying or
    // constructing any string values, without allocating a decoded payload.
    // Invalid Spark strings use ordinary replay, whose decoder preserves Spark
    // semantics without charging an unbounded repair allocation to this cache.
    for column in batch.columns() {
        if column.data_type() == &DataType::Utf8 {
            column.to_data().validate_data().map_err(|_| {
                resources_datafusion_err!("Broadcast UTF-8 requires ordinary decoding")
            })?;
        }
    }
    let bound = broadcast_copy_bytes(batch, schema)?;
    reservation.try_grow(bound)?;
    batch
        .columns()
        .iter()
        .map(|column| {
            if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
                let data = array.to_data();
                // Generic `new(rows)` reserves `rows` value bytes and may double
                // that capacity on growth. Explicit capacities instead match the
                // admitted span, even for empty strings and sliced null values.
                let mut copy = MutableArrayData::with_capacities(
                    vec![&data],
                    false,
                    Capacities::Binary(array.len(), Some(utf8_value_span(array))),
                );
                copy.try_extend(0, 0, array.len())?;
                Ok(make_array(copy.freeze()))
            } else {
                Ok(copy_array(column.as_ref())?)
            }
        })
        .collect()
}

/// Restricts retained payloads to arrays whose gather output owns its values.
fn cacheable_type(dt: &DataType) -> bool {
    dt.is_primitive()
        || matches!(
            dt,
            DataType::Boolean | DataType::Null | DataType::FixedSizeBinary(_) | DataType::Utf8
        )
}

/// Wraps an eligible inner hash join without caching its execution plan. Only
/// the immutable prepared build is shared; each task has its own probe child,
/// dynamic filter and execution metrics.
#[derive(Debug)]
struct CachedBroadcastJoinExec {
    join: Arc<HashJoinExec>,
    source: Arc<BroadcastInputExec>,
    cache: Arc<PreparedCache>,
    metrics: ExecutionPlanMetricsSet,
}

impl DisplayAs for CachedBroadcastJoinExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.join.fmt_as(t, f)
    }
}

impl ExecutionPlan for CachedBroadcastJoinExec {
    fn name(&self) -> &str {
        "CometCachedBroadcastHashJoinExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.join.properties()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.join.children()
    }
    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.join.apply_expressions(f)
    }
    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // A rewrite must re-establish exact build identity and compatibility.
        Arc::clone(&self.join).replace_children(children, options)
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
    /// Defers lookup until this probe stream is polled. One caller prepares a
    /// missing build while matching tasks wait or reuse an active build. Failed
    /// admission uses a fresh stream over this task's broadcast for the ordinary
    /// join; successful probes hold a lease until their output stream is dropped.
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("Cached broadcast join has one probe partition");
        }
        let join = Arc::clone(&self.join);
        let source = Arc::clone(&self.source);
        let cache = Arc::clone(&self.cache);
        let metrics = self.metrics.clone();
        let hits =
            MetricBuilder::new(&self.metrics).counter("broadcast_build_cache_hits", partition);
        let misses =
            MetricBuilder::new(&self.metrics).counter("broadcast_build_cache_misses", partition);
        let fallbacks =
            MetricBuilder::new(&self.metrics).counter("broadcast_build_cache_fallbacks", partition);
        let prepare_time = MetricBuilder::new(&self.metrics)
            .subset_time("broadcast_build_prepare_time", partition);
        let prepare_rows =
            MetricBuilder::new(&self.metrics).counter("broadcast_build_prepare_rows", partition);
        let prepare_bytes =
            MetricBuilder::new(&self.metrics).gauge("broadcast_build_prepare_bytes", partition);
        let key = BuildKey {
            broadcast_id: source.broadcast_id,
            schema: source.schema(),
            probe_schema: join.right().schema(),
            key_columns: join
                .on()
                .iter()
                .map(|(left, _)| {
                    left.downcast_ref::<Column>()
                        .expect("eligible direct key")
                        .index()
                })
                .collect(),
        };
        let schema = self.schema();
        let stream = futures::stream::once(async move {
            let pool: Arc<dyn MemoryPool> = Arc::new(BroadcastMemoryPool {
                owner: Arc::clone(&source.owner),
                limit: source.limit,
                used: AtomicUsize::new(0),
            });
            let result = cache
                .get_or_load(key, || async {
                    let _timer = prepare_time.timer();
                    // Only runtime filter joins use membership, and they already
                    // disable IN-list construction. Direct joins need no list.
                    let mut config = context.session_config().options().as_ref().clone();
                    config.optimizer.hash_join_inlist_pushdown_max_size = 0;
                    config
                        .optimizer
                        .hash_join_inlist_pushdown_max_distinct_values = 0;
                    let prepared = join
                        .prepare_build(
                            source.stream(Some(Arc::clone(&pool))),
                            pool,
                            Arc::new(config),
                        )
                        .await?;
                    prepare_rows.add(prepared.num_rows());
                    prepare_bytes.set(prepared.reserved_bytes());
                    Ok(prepared)
                })
                .await;
            let (execution, lease) = match result {
                Ok((prepared, hit)) => {
                    if hit {
                        hits.add(1);
                    } else {
                        misses.add(1);
                    }
                    let execution = Arc::new(
                        join.builder()
                            .with_prepared_build(Arc::clone(&prepared))?
                            .build()?,
                    );
                    (execution, Some(prepared))
                }
                Err(DataFusionError::ResourcesExhausted(_)) => {
                    // The source is replayable. Cache failure cannot consume this
                    // task's ordinary build input or wait on its other leases.
                    fallbacks.add(1);
                    (Arc::clone(&join), None)
                }
                Err(error) => return Err(error),
            };
            let result = execution.execute(partition, context);
            for metric in execution.metrics().unwrap_or_default().iter() {
                metrics.register(Arc::clone(metric));
            }
            let stream = result?;
            match lease {
                Some(prepared) => Ok(Box::pin(PreparedProbeStream {
                    stream,
                    _lease: prepared,
                }) as SendableRecordBatchStream),
                None => Ok(stream),
            }
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// Keeps the prepared build alive through the probe, even after its plan is
/// dropped. The final lease frees the table; Spark's owner returns its storage
/// charge then, or earlier on executor retirement.
struct PreparedProbeStream {
    stream: SendableRecordBatchStream,
    _lease: Arc<PreparedHashJoinBuild>,
}

impl Stream for PreparedProbeStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.as_mut().poll_next(context)
    }
}

impl RecordBatchStream for PreparedProbeStream {
    fn schema(&self) -> SchemaRef {
        self.stream.schema()
    }
}

/// Wraps only a final, physical inner broadcast join with direct column keys and
/// flat payloads. Call after build-side swapping. A dynamic-filter wrapper must
/// instead call the shared-metrics variant on its fresh runtime join. Unsupported
/// joins retain the ordinary execution path.
pub(crate) fn reuse_broadcast_build(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    reuse_broadcast_build_with_metrics(plan, ExecutionPlanMetricsSet::new())
}

/// Attaches reuse while registering asynchronous execution counters in the
/// caller's metric set. The set owns no execution state. An ineligible direct
/// join returns the same Arc so the caller can forward ordinary metrics itself.
pub(crate) fn reuse_broadcast_build_with_metrics(
    plan: Arc<dyn ExecutionPlan>,
    metrics: ExecutionPlanMetricsSet,
) -> Result<Arc<dyn ExecutionPlan>> {
    if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
        let child = reuse_broadcast_build_with_metrics(Arc::clone(projection.input()), metrics)?;
        return if Arc::ptr_eq(&child, projection.input()) {
            Ok(plan)
        } else {
            plan.replace_children(
                vec![child],
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )
        };
    }
    let Some(join) = plan.downcast_ref::<HashJoinExec>() else {
        return Ok(plan);
    };
    let Some(source) = join.left().downcast_ref::<BroadcastInputExec>() else {
        return Ok(plan);
    };
    if *join.join_type() != JoinType::Inner
        || !join
            .left()
            .schema()
            .fields()
            .iter()
            .all(|field| cacheable_type(field.data_type()))
        || !join.on().iter().all(|(left, right)| {
            left.downcast_ref::<Column>().is_some() && right.downcast_ref::<Column>().is_some()
        })
    {
        return Ok(plan);
    }
    let source = Arc::new(source.clone());
    let join = Arc::new(
        join.builder()
            .with_partition_mode(PartitionMode::CollectLeft)
            .build()?,
    );
    Ok(Arc::new(CachedBroadcastJoinExec {
        join,
        cache: cache(source.generation),
        source,
        metrics,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        ArrayData, Decimal128Array, DictionaryArray, Int64Array, Int8Array, StringBuilder,
    };
    use arrow::buffer::Buffer;
    use arrow::datatypes::Int8Type;
    use datafusion::common::utils::memory::get_record_batch_memory_size;
    use datafusion::execution::memory_pool::GreedyMemoryPool;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::joins::HashJoinExecBuilder;
    use std::ptr::NonNull;
    use std::sync::Weak;

    fn batch(columns: Vec<ArrayRef>) -> RecordBatch {
        let fields = columns
            .iter()
            .enumerate()
            .map(|(i, array)| Field::new(format!("c{i}"), array.data_type().clone(), true))
            .collect::<Vec<_>>();
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    fn pool(limit: usize) -> Arc<dyn MemoryPool> {
        Arc::new(GreedyMemoryPool::new(limit))
    }

    #[tokio::test]
    async fn large_broadcast_build_admits_compaction_and_releases_storage() -> Result<()> {
        let rows = 280;
        let payload = "x".repeat(256 * 1024);
        let mut strings = StringBuilder::with_capacity(rows, rows * payload.len());
        for _ in 0..rows {
            strings.append_value(&payload);
        }
        let source = batch(vec![
            Arc::new(Int64Array::from_iter_values(0..rows as i64)),
            Arc::new(strings.finish()),
        ]);
        let schema = source.schema();
        let source_bytes = get_record_batch_memory_size(&source);
        assert!(source_bytes > 64 * 1024 * 1024);
        let bound = broadcast_copy_bytes(&source, &schema)?;
        let limit = bound + source_bytes / 2;
        let medium = source.slice(0, rows / 2);
        assert!(broadcast_copy_bytes(&medium, &schema)? > MAX_COPY_BATCH_BYTES);
        assert_eq!(
            copy_batch_rows(&medium, &schema, MAX_COPY_BATCH_BYTES, false)?,
            medium.num_rows()
        );
        let join = HashJoinExecBuilder::new(
            Arc::new(EmptyExec::new(Arc::clone(&schema))),
            Arc::new(EmptyExec::new(Arc::new(Schema::new(vec![Field::new(
                "c0",
                DataType::Int64,
                true,
            )])))),
            vec![(
                Arc::new(Column::new("c0", 0)),
                Arc::new(Column::new("c0", 0)),
            )],
            JoinType::Inner,
        )
        .with_partition_mode(PartitionMode::CollectLeft)
        .build()?;
        let copied_stream = |target, pool: &Arc<dyn MemoryPool>| {
            let source = source.clone();
            let schema = Arc::clone(&schema);
            let reservation = MemoryConsumer::new("test broadcast copy").register(pool);
            let stream = futures::stream::try_unfold(
                (source, 0, reservation),
                move |(source, offset, mut reservation)| async move {
                    reservation.free();
                    if offset == source.num_rows() {
                        return Ok::<_, DataFusionError>(None);
                    }
                    let remainder = source.slice(offset, source.num_rows() - offset);
                    let rows =
                        copy_batch_rows(&remainder, &remainder.schema(), target, offset != 0)?;
                    let columns = copy_broadcast_columns(
                        &remainder.slice(0, rows),
                        &remainder.schema(),
                        &mut reservation,
                    )?;
                    let copied = RecordBatch::try_new(remainder.schema(), columns)?;
                    Ok(Some((copied, (source, offset + rows, reservation))))
                },
            );
            Box::pin(RecordBatchStreamAdapter::new(schema, stream)) as SendableRecordBatchStream
        };
        let constrained = pool(limit);
        let whole = join
            .prepare_build(
                copied_stream(usize::MAX, &constrained),
                Arc::clone(&constrained),
                Arc::new(Default::default()),
            )
            .await;
        assert!(matches!(whole, Err(DataFusionError::ResourcesExhausted(_))));
        assert_eq!(constrained.reserved(), 0);

        // Public DataFusion compacts the copied inputs into one batch. Its
        // reservation must also admit that output while inputs remain alive.
        // Chunking the JVM-to-native handoff does not remove this overlap.
        let chunked = join
            .prepare_build(
                copied_stream(MAX_COPY_BATCH_BYTES, &constrained),
                Arc::clone(&constrained),
                Arc::new(Default::default()),
            )
            .await;
        assert!(matches!(
            chunked,
            Err(DataFusionError::ResourcesExhausted(_))
        ));
        assert_eq!(constrained.reserved(), 0);

        let compact_pool = pool(3 * bound);
        let prepared = join
            .prepare_build(
                copied_stream(MAX_COPY_BATCH_BYTES, &compact_pool),
                Arc::clone(&compact_pool),
                Arc::new(Default::default()),
            )
            .await?;
        assert_eq!(prepared.num_rows(), rows);
        assert_eq!(compact_pool.reserved(), prepared.reserved_bytes());
        assert!(prepared.reserved_bytes() < 3 * bound);
        drop(prepared);
        assert_eq!(compact_pool.reserved(), 0);
        Ok(())
    }

    #[test]
    fn uncached_broadcast_replay_uses_safe_scan_casts_for_schema_drift() {
        let decimal: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(12300), Some(24400), None, Some(-100)])
                .with_precision_and_scale(38, 2)
                .unwrap(),
        );
        let unchanged: ArrayRef = Arc::new(Int64Array::from(vec![7, 8, 9, 10]));
        let input = batch(vec![decimal, Arc::clone(&unchanged)]);
        let schema = Schema::new(vec![
            Field::new("small_value", DataType::Int8, true),
            Field::new("unchanged", DataType::Int64, true),
        ]);

        // The physical decimal cannot be copied into the declared Int8 cache
        // layout. An admission miss must replay this same input without error.
        let pool = pool(usize::MAX);
        let mut reservation = MemoryConsumer::new("test schema drift").register(&pool);
        assert!(matches!(
            copy_broadcast_columns(&input, &schema, &mut reservation),
            Err(DataFusionError::ResourcesExhausted(_))
        ));
        assert_eq!(reservation.size(), 0);

        let columns = cast_uncached_broadcast_columns(&input, &schema).unwrap();
        let small = columns[0].as_any().downcast_ref::<Int8Array>().unwrap();
        assert_eq!(
            small,
            &Int8Array::from(vec![Some(123), None, None, Some(-1)])
        );
        assert!(Arc::ptr_eq(&columns[1], &unchanged));
    }

    /// Model unchecked FFI string buffers without ever reading them as &str.
    fn unchecked_utf8(values: &[u8], offsets: &[i32]) -> ArrayRef {
        let data = unsafe {
            ArrayData::builder(DataType::Utf8)
                .len(offsets.len() - 1)
                .add_buffer(Buffer::from_slice_ref(offsets))
                .add_buffer(Buffer::from(values.to_vec()))
                .build_unchecked()
        };
        make_array(data)
    }

    #[test]
    fn invalid_broadcast_utf8_declines_cache_and_replays_spark_decoding() {
        // The second case is valid UTF-8 as a whole but each row splits a codepoint.
        for (values, expected) in [
            (&[0xff, b'a'][..], vec!["\u{FFFD}", "a"]),
            (&[0xc3, 0xa9][..], vec!["\u{FFFD}", "\u{FFFD}"]),
        ] {
            let input = batch(vec![unchecked_utf8(values, &[0, 1, 2])]);
            let pool = pool(usize::MAX);
            let mut reservation = MemoryConsumer::new("test invalid UTF-8").register(&pool);
            assert!(matches!(
                copy_broadcast_columns(&input, &input.schema(), &mut reservation),
                Err(DataFusionError::ResourcesExhausted(_))
            ));
            assert_eq!(pool.reserved(), 0);

            let columns = cast_uncached_broadcast_columns(&input, &input.schema()).unwrap();
            let strings = columns[0].as_any().downcast_ref::<StringArray>().unwrap();
            assert_eq!(
                strings.iter().collect::<Vec<_>>(),
                expected.into_iter().map(Some).collect::<Vec<_>>()
            );
            columns[0].to_data().validate_full().unwrap();
        }
    }

    #[test]
    fn uncached_broadcast_decodes_dictionary_strings_before_unpacking() {
        let values = unchecked_utf8(&[0xff, b'a'], &[0, 1, 2]);
        let dictionary =
            DictionaryArray::<Int8Type>::new(Int8Array::from(vec![Some(1), None, Some(0)]), values);
        let input = batch(vec![Arc::new(dictionary)]);
        let schema = Schema::new(vec![Field::new("decoded", DataType::Utf8, true)]);
        let columns = cast_uncached_broadcast_columns(&input, &schema).unwrap();
        let strings = columns[0].as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(
            strings.iter().collect::<Vec<_>>(),
            vec![Some("a"), None, Some("\u{FFFD}")]
        );
        columns[0].to_data().validate_full().unwrap();
    }

    #[test]
    fn broadcast_utf8_copy_bounds_empty_and_short_values() {
        for value in ["", "é"] {
            let rows = 4096;
            let source = StringArray::from(vec![value; rows]);
            let bound = utf8_copy_bytes(rows, utf8_value_span(&source)).unwrap();
            let input = batch(vec![Arc::new(source)]);
            let pool = pool(bound);
            let mut reservation = MemoryConsumer::new("test broadcast copy").register(&pool);
            let copies = copy_broadcast_columns(&input, &input.schema(), &mut reservation).unwrap();
            let copied = copies[0].as_any().downcast_ref::<StringArray>().unwrap();
            assert_eq!(copied.iter().collect::<Vec<_>>(), vec![Some(value); rows]);
            assert_eq!(pool.reserved(), bound);
            assert!(copied.get_array_memory_size() <= bound);
            if value.is_empty() {
                assert_eq!(copied.values().capacity(), 0);
            }
            drop(copies);
            drop(reservation);
            assert_eq!(pool.reserved(), 0);
        }
    }

    struct ForeignUtf8 {
        offsets: Vec<i32>,
        values: Vec<u8>,
        validity: Vec<u8>,
    }

    fn foreign_utf8_slice() -> (StringArray, Weak<ForeignUtf8>) {
        let owner = Arc::new(ForeignUtf8 {
            offsets: vec![0, 6, 6, 17, 20, 24],
            values: "prefixhidden-nullaétail".as_bytes().to_vec(),
            validity: vec![0b0001_1011],
        });
        let buffer = |ptr: *const u8, len: usize| {
            // The immutable vectors remain allocated at these addresses until
            // the last custom Buffer drops its Arc<ForeignUtf8> owner.
            unsafe {
                Buffer::from_custom_allocation(
                    NonNull::new(ptr.cast_mut()).unwrap(),
                    len,
                    Arc::<ForeignUtf8>::clone(&owner),
                )
            }
        };
        let data = ArrayData::builder(DataType::Utf8)
            .len(5)
            .add_buffer(buffer(
                owner.offsets.as_ptr().cast(),
                owner.offsets.len() * size_of::<i32>(),
            ))
            .add_buffer(buffer(owner.values.as_ptr(), owner.values.len()))
            .null_bit_buffer(Some(buffer(owner.validity.as_ptr(), owner.validity.len())))
            .build()
            .unwrap();
        (StringArray::from(data).slice(1, 3), Arc::downgrade(&owner))
    }

    #[test]
    fn broadcast_utf8_copy_owns_sliced_foreign_buffers() {
        let (source, owner) = foreign_utf8_slice();
        assert_eq!(utf8_value_span(&source), "hidden-nullaé".len());
        let bound = utf8_copy_bytes(source.len(), utf8_value_span(&source)).unwrap();
        let input = batch(vec![Arc::new(source)]);
        let source_data = input.column(0).to_data();
        let source_ptrs = [
            source_data.buffers()[0].as_ptr(),
            source_data.buffers()[1].as_ptr(),
            source_data.nulls().unwrap().buffer().as_ptr(),
        ];
        drop(source_data);
        let pool = pool(bound);
        let mut reservation = MemoryConsumer::new("test broadcast copy").register(&pool);
        let copies = copy_broadcast_columns(&input, &input.schema(), &mut reservation).unwrap();
        assert_eq!(pool.reserved(), bound);
        let strings = copies[0].as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(
            strings.iter().collect::<Vec<_>>(),
            vec![Some(""), None, Some("aé")]
        );
        assert_eq!(strings.value_offsets(), &[0, 0, 11, 14]);
        assert_eq!(strings.values().as_slice(), "hidden-nullaé".as_bytes());
        assert!(strings.get_array_memory_size() <= bound);
        let data = strings.to_data();
        let copied_ptrs = [
            data.buffers()[0].as_ptr(),
            data.buffers()[1].as_ptr(),
            data.nulls().unwrap().buffer().as_ptr(),
        ];
        for (source, copied) in source_ptrs.iter().zip(copied_ptrs.iter()) {
            assert_ne!(source, copied);
        }
        drop(input);
        assert!(
            owner.upgrade().is_none(),
            "Copied column retained the foreign task owner"
        );
        drop(copies);
        drop(reservation);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn broadcast_utf8_copy_denied_admission_keeps_input_replayable() {
        let source: ArrayRef = Arc::new(StringArray::from(vec![Some("aé"), None, Some("")]));
        let input = batch(vec![Arc::clone(&source), source]);
        let bound = 2 * utf8_copy_bytes(3, "aé".len()).unwrap();
        let denied = pool(bound - 1);
        let mut reservation = MemoryConsumer::new("test denied copy").register(&denied);
        assert!(matches!(
            copy_broadcast_columns(&input, &input.schema(), &mut reservation),
            Err(DataFusionError::ResourcesExhausted(_))
        ));
        assert_eq!(reservation.size(), 0);
        assert_eq!(denied.reserved(), 0);
        // Failure leaves the source intact for ordinary replay or a fresh grant.
        let admitted = pool(bound);
        let mut reservation = MemoryConsumer::new("test admitted copy").register(&admitted);
        let copies = copy_broadcast_columns(&input, &input.schema(), &mut reservation).unwrap();
        for (source, copied) in input.columns().iter().zip(&copies) {
            assert_eq!(source.to_data(), copied.to_data());
        }
        drop(copies);
        drop(reservation);
        assert_eq!(admitted.reserved(), 0);
    }
}
