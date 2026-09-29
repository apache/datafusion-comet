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

//! Execution-owned transport of completed join domains through lazy Spark Union inputs.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};

use super::broadcast::broadcast_copy_bytes;
use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Schema, SchemaRef};
use arrow::ffi_stream::FFI_ArrowArrayStream;
use datafusion::common::{internal_err, Result};
use datafusion::physical_expr::expressions::DynamicFilterPhysicalExpr;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::joins::PreparedHashJoinBuild;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use futures::FutureExt;
use jni::objects::{Global, JLongArray, JObject, JValue, ReleaseMode};
use parking_lot::Mutex;

use super::AlignedArrowStreamReader;
use crate::errors::{CometError, CometResult};
use crate::jvm_bridge::JVMClasses;

/// The immutable build lease keeps every allocation referenced by membership expressions
/// charged until the last consumer has dropped its plan and predicate.
#[derive(Debug)]
pub(crate) struct UnionFilterDomain {
    pub predicate: Arc<DynamicFilterPhysicalExpr>,
    pub build: Arc<PreparedHashJoinBuild>,
    pub key_types: Vec<DataType>,
}

/// A handle is a Box<Arc<UnionFilterBundle>>, never a process-wide lookup key. The JVM
/// marker retains its own box before leaving openStream and releases it with that stream.
#[derive(Debug)]
pub(crate) struct UnionFilterBundle {
    pub domains: Vec<Arc<UnionFilterDomain>>,
    pub root_plan_ids: Vec<u32>,
    pub task_attempt_id: i64,
}

impl UnionFilterBundle {
    pub fn accepts(&self, root_plan_id: u32, task_attempt_id: i64) -> bool {
        self.task_attempt_id == task_attempt_id && self.root_plan_ids.contains(&root_plan_id)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct UnionFilterTarget {
    pub source: Arc<UnionInput>,
    pub predicate: Arc<DynamicFilterPhysicalExpr>,
}

impl UnionFilterTarget {
    pub fn publish(&self, build: Arc<PreparedHashJoinBuild>) -> Arc<UnionFilterDomain> {
        let domain = Arc::new(UnionFilterDomain {
            predicate: Arc::clone(&self.predicate),
            build,
            key_types: self
                .predicate
                .children()
                .iter()
                .map(|expr| {
                    let column = expr
                        .downcast_ref::<datafusion::physical_expr::expressions::Column>()
                        .expect("transport target has direct columns");
                    self.source.data_types[column.index()].clone()
                })
                .collect(),
        });
        let mut domains = self.source.domains.lock();
        domains.retain(|domain| domain.strong_count() > 0);
        domains.push(Arc::downgrade(&domain));
        domain
    }
}

/// ScanExec still owns demand/batch handoff. Only its driving JNI feeder calls next;
/// Tokio polls merely set requested, and can never enter Spark task-local iterator code.
#[derive(Debug)]
pub(crate) struct UnionInput {
    marker: Arc<Global<JObject<'static>>>,
    root_plan_ids: Vec<u32>,
    task_attempt_id: i64,
    data_types: Vec<DataType>,
    requested: AtomicBool,
    domains: Mutex<Vec<Weak<UnionFilterDomain>>>,
    reader: Mutex<Option<AlignedArrowStreamReader>>,
    opened: AtomicBool,
}

impl UnionInput {
    pub fn try_new(
        marker: Arc<Global<JObject<'static>>>,
        data_types: Vec<DataType>,
    ) -> CometResult<Option<Arc<Self>>> {
        let metadata = JVMClasses::with_env(|env| {
            if !env.is_instance_of(
                marker.as_obj(),
                jni::strings::JNIString::new("org/apache/spark/sql/comet/CometUnionInput"),
            )? {
                return Ok::<_, CometError>(None);
            }
            let roots = env
                .call_method(
                    marker.as_obj(),
                    jni::jni_str!("nativeRootPlanIds"),
                    jni::jni_sig!("()[J"),
                    &[],
                )?
                .l()?;
            let roots = JLongArray::cast_local(env, roots)?;
            let roots = unsafe { roots.get_elements(env, ReleaseMode::NoCopyBack)? };
            let root_plan_ids = roots
                .iter()
                .filter_map(|id| u32::try_from(*id).ok())
                .collect();
            drop(roots);
            let task_attempt_id = env
                .call_method(
                    marker.as_obj(),
                    jni::jni_str!("taskAttemptId"),
                    jni::jni_sig!("()J"),
                    &[],
                )?
                .j()?;
            Ok(Some((root_plan_ids, task_attempt_id)))
        })?;
        Ok(metadata.map(|(root_plan_ids, task_attempt_id)| {
            Arc::new(Self {
                marker,
                root_plan_ids,
                task_attempt_id,
                data_types,
                requested: AtomicBool::new(false),
                domains: Mutex::new(vec![]),
                reader: Mutex::new(None),
                opened: AtomicBool::new(false),
            })
        }))
    }

    pub fn request(&self) {
        self.requested.store(true, Ordering::Release);
    }

    pub fn requested(&self) -> bool {
        self.requested.load(Ordering::Acquire)
    }

    fn completed_bundle(&self) -> Arc<UnionFilterBundle> {
        // Zero bounded wait: a missing or incomplete producer never blocks the input.
        // A hash join only requests its probe after publication, so its normal path is ready.
        Arc::new(UnionFilterBundle {
            domains: self
                .domains
                .lock()
                .iter()
                .filter_map(Weak::upgrade)
                .filter(|domain| domain.predicate.wait_complete().now_or_never().is_some())
                .collect(),
            root_plan_ids: self.root_plan_ids.clone(),
            task_attempt_id: self.task_attempt_id,
        })
    }

    pub fn close(&self) -> CometResult<()> {
        // End C stream callbacks before entering JVM child cleanup. Do not hold the reader
        // mutex while nested native plans recursively close their own Union inputs.
        let reader = self.reader.lock().take();
        drop(reader);
        JVMClasses::with_env(|env| {
            let result = env.call_method(
                self.marker.as_obj(),
                jni::jni_str!("closeAfterExecution"),
                jni::jni_sig!("()V"),
                &[],
            );
            // Clear a Java failure before returning it so later Union owners can still
            // invoke their cleanup callbacks on this same JNI thread.
            if let Some(exception) = crate::jvm_bridge::check_exception(env)? {
                return Err(exception);
            }
            result?;
            Ok(())
        })
    }

    pub fn next(&self) -> CometResult<Option<RecordBatch>> {
        let mut reader = self
            .reader
            .try_lock()
            .ok_or_else(|| CometError::Internal("Union input reader contended".into()))?;
        if !self.opened.swap(true, Ordering::AcqRel) {
            let bundle = self.completed_bundle();
            let handle = Box::new(bundle);
            let address = (&*handle as *const Arc<UnionFilterBundle>) as i64;
            // The marker clones this handle before returning; the temporary box can then drop.
            let stream = JVMClasses::with_env(|env| {
                let stream = env
                    .call_method(
                        self.marker.as_obj(),
                        jni::jni_str!("openStream"),
                        jni::jni_sig!("(J)Lorg/apache/arrow/c/ArrowArrayStream;"),
                        &[JValue::Long(address)],
                    )?
                    .l()?;
                let address = env
                    .call_method(
                        &stream,
                        jni::jni_str!("memoryAddress"),
                        jni::jni_sig!("()J"),
                        &[],
                    )?
                    .j()?;
                Ok::<_, CometError>(address)
            })?;
            *reader = Some(unsafe {
                AlignedArrowStreamReader::from_raw(stream as *mut FFI_ArrowArrayStream)
            }?);
        }
        let result = reader.as_mut().and_then(Iterator::next).transpose()?;
        if result.is_none() {
            reader.take();
        }
        Ok(result)
    }
}

pub(crate) fn transport_build_supported(schema: &SchemaRef) -> bool {
    schema.fields().iter().all(|field| {
        let ty = field.data_type();
        ty.primitive_width().is_some()
            || matches!(
                ty,
                DataType::Null | DataType::Boolean | DataType::FixedSizeBinary(_) | DataType::Utf8
            )
    })
}

pub(crate) fn remap_domain(
    domain: &UnionFilterDomain,
    schema: &SchemaRef,
) -> Result<Arc<DynamicFilterPhysicalExpr>> {
    remap_predicate(&domain.predicate, &domain.key_types, schema)
}

fn remap_predicate(
    predicate: &Arc<DynamicFilterPhysicalExpr>,
    key_types: &[DataType],
    schema: &SchemaRef,
) -> Result<Arc<DynamicFilterPhysicalExpr>> {
    use datafusion::physical_expr::expressions::Column;
    if predicate.children().len() != key_types.len() {
        return internal_err!("Union runtime filter key metadata does not match the predicate");
    }
    let mut children = vec![];
    for (expr, key_type) in predicate.children().into_iter().zip(key_types) {
        let Some(column) = expr.downcast_ref::<Column>() else {
            return internal_err!("Union runtime filter requires direct column bindings");
        };
        let Some(field) = schema.fields().get(column.index()) else {
            return internal_err!("Union runtime filter column is outside consumer schema");
        };
        if field.data_type() != key_type
            || !matches!(
                field.data_type(),
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
            )
        {
            return internal_err!("Union runtime filter consumer key must be a signed integer");
        }
        children.push(Arc::new(Column::new(field.name(), column.index())) as Arc<dyn PhysicalExpr>);
    }
    let predicate = Arc::clone(predicate).with_new_children(children)?;
    Arc::downcast::<DynamicFilterPhysicalExpr>(predicate).map_err(|_| {
        datafusion::common::internal_datafusion_err!("Dynamic filter remapping changed type")
    })
}

/// Prepare one task-owned build before demanding any Union probe input. The same prepared
/// build is installed back into the original join; publication never builds a second domain.
pub(crate) fn prepare_union_join(
    join: &datafusion::physical_plan::joins::HashJoinExec,
    partition: usize,
    context: Arc<datafusion::execution::TaskContext>,
    targets: Vec<UnionFilterTarget>,
    metrics: ExecutionPlanMetricsSet,
) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
    use datafusion::execution::memory_pool::MemoryConsumer;
    use datafusion::physical_plan::joins::PartitionMode;
    use datafusion::physical_plan::metrics::MetricBuilder;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::physical_plan::ExecutionPlan;
    use futures::{StreamExt, TryStreamExt};

    let join = join
        .builder()
        .with_partition_mode(PartitionMode::CollectLeft)
        .build()?;
    let schema = join.schema();
    // prepare_build does not expose its metrics. Preserve the task-local join's build
    // counters here; the execution below only measures probing an already prepared build.
    let build_time = MetricBuilder::new(&metrics).subset_time("build_time", partition);
    let build_rows = MetricBuilder::new(&metrics).counter("build_input_rows", partition);
    let build_batches = MetricBuilder::new(&metrics).counter("build_input_batches", partition);
    let build_memory = MetricBuilder::new(&metrics).peak_memory_usage("build_mem_used", partition);
    let stream = futures::stream::once(async move {
        let build_timer = build_time.timer();
        let pool = Arc::clone(context.memory_pool());
        let reservation = MemoryConsumer::new("UnionFilterBuildCopy").register(&pool);
        let input = join.left().execute(0, Arc::clone(&context))?;
        let input_schema = input.schema();
        let input = input.inspect_ok(move |batch| {
            // Count source batches, not the pieces produced by copy_batch_rows.
            build_batches.add(1);
            build_rows.add(batch.num_rows());
        });
        let copied = futures::stream::try_unfold(
            (input, reservation, None::<RecordBatch>),
            |(mut input, mut reservation, pending)| async move {
                // PreparedHashJoinBuild has admitted the prior batch before polling again.
                reservation.free();
                let already_split = pending.is_some();
                let batch = match pending {
                    Some(batch) => batch,
                    None => match input.next().await {
                        Some(batch) => batch?,
                        None => return Ok(None),
                    },
                };
                let rows =
                    copy_batch_rows(&batch, &batch.schema(), 32 * 1024 * 1024, already_split)?;
                let pending =
                    (rows < batch.num_rows()).then(|| batch.slice(rows, batch.num_rows() - rows));
                let batch = batch.slice(0, rows);
                let columns = super::broadcast::copy_broadcast_columns(
                    &batch,
                    &batch.schema(),
                    &mut reservation,
                )?;
                let batch = RecordBatch::try_new_with_options(
                    batch.schema(),
                    columns,
                    &arrow::array::RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
                )?;
                Ok(Some((batch, (input, reservation, pending))))
            },
        );
        let copied = Box::pin(RecordBatchStreamAdapter::new(input_schema, copied));
        let build = join
            .prepare_build(copied, pool, Arc::clone(context.session_config().options()))
            .await?;
        build_memory.set(build.reserved_bytes());
        build_timer.done();
        let domains = targets
            .iter()
            .map(|target| target.publish(Arc::clone(&build)))
            .collect::<Vec<_>>();
        let execution = join
            .builder()
            .with_prepared_build(Arc::clone(&build))
            .build()?;
        let stream = execution.execute(partition, context)?;
        for metric in execution.metrics().unwrap_or_default().iter() {
            if !matches!(
                metric.value().name(),
                "build_time" | "build_input_rows" | "build_input_batches" | "build_mem_used"
            ) {
                metrics.register(Arc::clone(metric));
            }
        }
        // Consumers can retain the prepared build after this stream has finished.
        let output_schema = stream.schema();
        let stream = stream.map(move |batch| {
            let _ = &domains;
            batch
        });
        Ok::<_, datafusion::common::DataFusionError>(Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
            stream,
        ))
            as datafusion::physical_plan::SendableRecordBatchStream)
    })
    .try_flatten();
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::Field;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{lit, BinaryExpr, Column};

    #[test]
    fn bundle_requires_both_authorized_root_and_task_attempt() {
        let bundle = UnionFilterBundle {
            domains: vec![],
            root_plan_ids: vec![2, 7],
            task_attempt_id: 11,
        };
        assert!(bundle.accepts(2, 11));
        assert!(bundle.accepts(7, 11));
        assert!(!bundle.accepts(3, 11));
        assert!(!bundle.accepts(2, 12));
        assert!(!bundle.accepts(7, -1));
    }

    #[test]
    fn branch_remapping_preserves_live_updates_and_rejects_incompatible_keys() {
        let predicate = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::new(Column::new("union_key", 1))],
            lit(true),
        ));
        let schema = Arc::new(Schema::new(vec![
            Field::new("payload", DataType::Int32, false),
            Field::new("branch_key", DataType::Int32, false),
        ]));
        let remapped = remap_predicate(&predicate, &[DataType::Int32], &schema).unwrap();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![5, 6])),
                Arc::new(Int32Array::from(vec![1, 2])),
            ],
        )
        .unwrap();
        // Rebinding must share the producer's state, including a later completion.
        predicate
            .update(Arc::new(BinaryExpr::new(
                Arc::new(Column::new("union_key", 1)),
                Operator::Eq,
                lit(2_i32),
            )))
            .unwrap();
        predicate.mark_complete();
        assert!(remapped.wait_complete().now_or_never().is_some());
        let result = remapped.evaluate(&batch).unwrap().into_array(2).unwrap();
        assert_eq!(
            result.as_ref(),
            &arrow::array::BooleanArray::from(vec![false, true])
        );
        assert!(remap_predicate(&predicate, &[DataType::Int64], &schema).is_err());
        assert!(remap_predicate(&predicate, &[], &schema).is_err());
        let short = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Int32,
            false,
        )]));
        assert!(remap_predicate(&predicate, &[DataType::Int32], &short).is_err());
    }

    #[test]
    fn copy_chunks_bound_large_handoffs_without_losing_rows() {
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, false)]));
        let mut batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from_iter_values(0..10000))],
        )
        .unwrap();
        // A single row is still returned when fixed allocation overhead exceeds the target.
        assert_eq!(
            copy_batch_rows(&batch, &batch.schema(), 256, false).unwrap(),
            1
        );
        let target = 4096;
        let mut rows = 0;
        let mut already_split = false;
        while batch.num_rows() > 0 {
            let size = copy_batch_rows(&batch, &batch.schema(), target, already_split).unwrap();
            assert!(size > 0);
            let chunk = batch.slice(0, size);
            assert!(broadcast_copy_bytes(&chunk, &chunk.schema()).unwrap() <= target);
            rows += size;
            batch = batch.slice(size, batch.num_rows() - size);
            already_split = true;
        }
        assert_eq!(rows, 10000);
    }
}
