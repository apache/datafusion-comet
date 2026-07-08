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

//! Driver-side "offload to Ballista" submission entry.
//!
//! The JVM hands us a serialized Comet `Operator` proto; we run it on an
//! **in-process standalone Ballista** engine (no Spark executors) and hand the
//! resulting Arrow batches back to the JVM over the Arrow C Data Interface —
//! the same FFI mechanism Comet already uses in `jni_api::prepare_output`
//! (`ArrayData` → caller-allocated `FFI_ArrowArray`/`FFI_ArrowSchema`).

use std::sync::Arc;

use ballista::prelude::{SessionConfigExt, SessionContextExt};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};

use datafusion_comet_proto::spark_operator::{CometBallistaOffloadPlan, Operator};
use prost::Message;

use super::scan::CometScanExec;
use super::{CometFragmentExec, CometLogicalCodec, CometPhysicalCodec, CometTableProvider};

/// Run a Comet `Operator` proto on an in-process standalone Ballista engine and
/// return the collected Arrow batches plus the result schema.
///
/// This reuses the "proto → standalone Ballista → RecordBatches" recipe
/// validated in `tests/distributed.rs`, running `SELECT * FROM t` (no shuffle)
/// over a table provider that carries the whole Comet plan proto — so any
/// operators above the scan (filter/project/aggregate) run natively too.
///
/// The result schema is derived from the **built** Comet plan's `schema()`
/// (not from the scan proto's `required_schema`), so plans with operators above
/// the scan report their true output schema rather than the raw scan schema.
pub fn execute_comet_proto(proto: &[u8]) -> Result<(SchemaRef, Vec<RecordBatch>), String> {
    // Validate the proto decodes before spinning up the engine.
    Operator::decode(proto).map_err(|e| format!("failed to decode Operator proto: {e}"))?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("failed to build tokio runtime: {e}"))?;

    runtime.block_on(async move {
        // Build the whole Comet plan once (inside the Tokio runtime, which
        // `CometScanExec::try_new` requires) so we can read its true output
        // schema — the result schema comes from the built plan, not the
        // NativeScan proto's `required_schema`.
        let built: Arc<dyn ExecutionPlan> = Arc::new(
            CometScanExec::try_new(proto.to_vec())
                .map_err(|e| format!("failed to build Comet plan: {e}"))?,
        );
        let schema = built.schema();

        // In-process standalone Ballista cluster (scheduler + executor) with the
        // Comet codecs registered so the Comet leaf survives serialization.
        let config = SessionConfig::new_with_ballista()
            .with_target_partitions(1)
            .with_ballista_standalone_parallelism(1)
            .with_ballista_physical_extension_codec(Arc::new(CometPhysicalCodec::default()))
            .with_ballista_logical_extension_codec(Arc::new(CometLogicalCodec::default()));
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .build();
        let ctx = SessionContext::standalone_with_state(state)
            .await
            .map_err(|e| format!("failed to start standalone Ballista: {e}"))?;

        ctx.register_table(
            "comet_t",
            Arc::new(CometTableProvider::new(proto.to_vec(), Arc::clone(&schema))),
        )
        .map_err(|e| format!("failed to register Comet table: {e}"))?;

        let df = ctx
            .sql("SELECT * FROM comet_t")
            .await
            .map_err(|e| format!("failed to plan query: {e}"))?;
        let batches = df
            .collect()
            .await
            .map_err(|e| format!("failed to execute query: {e}"))?;
        Ok((schema, batches))
    })
}

// ---------------------------------------------------------------------------
// R3: general DAG offload (`CometBallistaOffloadPlan`)
// ---------------------------------------------------------------------------

use std::time::Duration;

use ballista_core::config::BallistaConfig;
use ballista_core::execution_plans::execute_physical_plan;
use ballista_core::serde::protobuf::scheduler_grpc_client::SchedulerGrpcClient;
use datafusion::execution::SessionState;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::Partitioning;
use datafusion_proto::protobuf::PhysicalPlanNode;
use futures::TryStreamExt;

/// Count the `Scan` (#100) input leaves in a serialized Comet `Operator` block —
/// the same leaves `build_native_fragment` (`native/core/src/execution/fragment.rs`)
/// expects one child stream per, in DFS order. Used as a build-time guard so a
/// mismatched `OffloadFragment.inputs` count fails fast in `build_offload_plan`
/// rather than lazily inside `CometFragmentExec::execute`.
fn comet_offload_scan_leaf_count(block_proto: &[u8]) -> Result<usize, String> {
    use datafusion_comet_proto::spark_operator::{operator::OpStruct, Operator};
    fn count(op: &Operator) -> usize {
        if matches!(op.op_struct, Some(OpStruct::Scan(_))) {
            return 1;
        }
        op.children.iter().map(count).sum()
    }
    let op = Operator::decode(block_proto).map_err(|e| format!("decode block: {e}"))?;
    Ok(count(&op))
}

/// Fold a serialized `CometBallistaOffloadPlan` into a Ballista physical plan: a DAG
/// of `CometFragmentExec` nodes whose inputs are `RepartitionExec(Hash)` over the
/// producer fragments. Fragments are processed in topological order; the last is the
/// root. Ballista's planner then splits at each hash repartition into a stage.
pub fn build_offload_plan(plan_bytes: &[u8]) -> Result<Arc<dyn ExecutionPlan>, String> {
    let plan = CometBallistaOffloadPlan::decode(plan_bytes)
        .map_err(|e| format!("failed to decode CometBallistaOffloadPlan: {e}"))?;
    if plan.fragments.is_empty() {
        return Err("CometBallistaOffloadPlan has no fragments".to_string());
    }
    let n = plan.num_partitions.max(1) as usize;

    let mut built: Vec<Arc<dyn ExecutionPlan>> = Vec::with_capacity(plan.fragments.len());
    for (idx, frag) in plan.fragments.iter().enumerate() {
        // Build-time guard: the block's actual `Scan`(#100) leaf count must match
        // the descriptor's declared input count, or `CometFragmentExec::execute`
        // would fail lazily (or silently under-drive leaves) later.
        let leaf_count = comet_offload_scan_leaf_count(&frag.block_proto)
            .map_err(|e| format!("fragment {idx}: {e}"))?;
        if leaf_count != frag.inputs.len() {
            return Err(format!(
                "fragment {idx}: block has {leaf_count} Scan input leaves but the descriptor \
                 declares {} inputs",
                frag.inputs.len()
            ));
        }

        // Build each input edge as a hash repartition over an already-built producer.
        let mut children: Vec<Arc<dyn ExecutionPlan>> = Vec::with_capacity(frag.inputs.len());
        for input in &frag.inputs {
            let producer_idx = input.producer as usize;
            if producer_idx >= idx {
                return Err(format!(
                    "fragment {idx} references producer {producer_idx} that is not earlier in \
                     topological order"
                ));
            }
            let producer = Arc::clone(&built[producer_idx]);
            let producer_schema = producer.schema();
            let hash_exprs: Vec<Arc<dyn PhysicalExpr>> = input
                .hash_key_ordinals
                .iter()
                .map(|&ord| {
                    let ord = ord as usize;
                    if ord >= producer_schema.fields().len() {
                        return Err(format!(
                            "fragment {idx} input hash key ordinal {ord} out of range for \
                             producer {producer_idx} with {} columns",
                            producer_schema.fields().len()
                        ));
                    }
                    Ok(
                        Arc::new(Column::new(producer_schema.field(ord).name(), ord))
                            as Arc<dyn PhysicalExpr>,
                    )
                })
                .collect::<Result<_, String>>()?;
            let repart = RepartitionExec::try_new(producer, Partitioning::Hash(hash_exprs, n))
                .map_err(|e| {
                    format!("fragment {idx}: failed to build hash RepartitionExec: {e}")
                })?;
            children.push(Arc::new(repart));
        }
        let fragment = CometFragmentExec::try_new(frag.block_proto.clone(), children)
            .map_err(|e| format!("fragment {idx}: failed to build CometFragmentExec: {e}"))?;
        built.push(Arc::new(fragment));
    }
    Ok(built.pop().expect("fragments non-empty"))
}

/// Build and submit a general `CometBallistaOffloadPlan` DAG to a Ballista
/// cluster, returning the collected Arrow result batches plus the result schema.
///
/// The plan is an arbitrary DAG of `CometFragmentExec` nodes (folded by
/// [`build_offload_plan`]), not a fixed two-stage shape. The shuffle width `n`
/// is read directly from the descriptor's `num_partitions` field — the
/// authoritative parallelism for every hash repartition `build_offload_plan`
/// builds — so `build_offload_plan` itself keeps its single-return signature.
///
/// An empty `scheduler_url` starts an in-process standalone cluster; a
/// non-empty one submits to that external scheduler instead.
pub fn execute_offload_plan(
    plan_bytes: &[u8],
    scheduler_url: &str,
) -> Result<(SchemaRef, Vec<RecordBatch>), String> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("failed to build tokio runtime: {e}"))?;

    runtime.block_on(async move {
        // The descriptor carries the authoritative shuffle width; decode it for `n`.
        let n = CometBallistaOffloadPlan::decode(plan_bytes)
            .map_err(|e| format!("failed to decode CometBallistaOffloadPlan: {e}"))?
            .num_partitions
            .max(1) as usize;

        // Build the plan inside the runtime: the fragments' NativeScan leaves
        // build via Comet's planner, which requires an active Tokio runtime.
        let plan = build_offload_plan(plan_bytes)?;
        let config = SessionConfig::new_with_ballista()
            .with_target_partitions(n)
            .with_ballista_standalone_parallelism(n)
            .with_ballista_physical_extension_codec(Arc::new(CometPhysicalCodec::default()))
            .with_ballista_logical_extension_codec(Arc::new(CometLogicalCodec::default()));
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .build();
        let schema = plan.schema();

        // Empty URL => in-process standalone; non-empty => external cluster.
        let scheduler_url = if scheduler_url.is_empty() {
            log::debug!("[comet-ballista R3] submitting to in-process standalone cluster");
            start_standalone_from_state(&state).await?
        } else {
            log::debug!("[comet-ballista R3] submitting to external cluster at {scheduler_url}");
            scheduler_url.to_string()
        };

        let session_config = state.config().clone();
        let codec = CometPhysicalCodec::default();
        let session_id = state.session_id().to_string();

        let stream = execute_physical_plan::<PhysicalPlanNode>(
            scheduler_url,
            &BallistaConfig::default(),
            plan,
            &codec,
            session_id,
            session_config,
        )
        .await
        .map_err(|e| format!("failed to submit offload plan: {e}"))?;

        let batches = stream
            .try_collect::<Vec<_>>()
            .await
            .map_err(|e| format!("failed to collect distributed results: {e}"))?;

        Ok((schema, batches))
    })
}

/// Run the general DAG offload plan and export the (single, concatenated)
/// result batch into the JVM-allocated FFI structs. Returns the row count.
///
/// # Safety
/// See [`export_batch_to_addresses`].
pub unsafe fn submit_and_export_offload(
    plan_bytes: &[u8],
    scheduler_url: &str,
    array_addrs: &[i64],
    schema_addrs: &[i64],
) -> Result<i64, String> {
    let (schema, batches) = execute_offload_plan(plan_bytes, scheduler_url)?;
    // The final fragment's partitions are concatenated into one batch so the
    // JVM imports exactly one set of column structs (same contract as R1/R2).
    let batch = concat_batches(&schema, &batches)
        .map_err(|e| format!("failed to concatenate result batches: {e}"))?;
    export_batch_to_addresses(&batch, array_addrs, schema_addrs)?;
    Ok(batch.num_rows() as i64)
}

/// Start an in-process standalone Ballista cluster (scheduler + executor) from
/// `state`, so the Comet extension codecs registered on the state's config reach
/// both sides. Mirrors `ballista::extension`'s private `setup_standalone`, but
/// returns the scheduler URL for the direct physical-plan submission path.
async fn start_standalone_from_state(state: &SessionState) -> Result<String, String> {
    let addr = ballista_scheduler::standalone::new_standalone_scheduler_from_state(state)
        .await
        .map_err(|e| format!("failed to start standalone scheduler: {e}"))?;
    let scheduler_url = format!("http://localhost:{}", addr.port());

    let mut retries = 50;
    let scheduler = loop {
        match SchedulerGrpcClient::connect(scheduler_url.clone()).await {
            Ok(s) => break s,
            Err(e) if retries > 0 => {
                retries -= 1;
                tokio::time::sleep(Duration::from_millis(100)).await;
                let _ = e;
            }
            Err(e) => return Err(format!("could not connect to standalone scheduler: {e}")),
        }
    };

    let concurrent_tasks = state.config().ballista_standalone_parallelism();
    ballista_executor::new_standalone_executor_from_state(scheduler, concurrent_tasks, state)
        .await
        .map_err(|e| format!("failed to start standalone executor: {e}"))?;

    Ok(scheduler_url)
}

/// Export one Arrow batch into caller-allocated `FFI_ArrowArray` /
/// `FFI_ArrowSchema` structs, one per column, whose addresses were allocated by
/// the JVM (Arrow Java `ArrowArray.allocateNew` / `ArrowSchema.allocateNew`).
///
/// This mirrors `jni_api::prepare_output`: the JVM owns the C Data structs and
/// imports them with its `ArrowImporter` after this call returns.
///
/// # Safety
/// `array_addrs[i]` / `schema_addrs[i]` must be valid, writable pointers to
/// uninitialized `FFI_ArrowArray` / `FFI_ArrowSchema` for each column.
unsafe fn export_batch_to_addresses(
    batch: &RecordBatch,
    array_addrs: &[i64],
    schema_addrs: &[i64],
) -> Result<(), String> {
    let num_cols = batch.num_columns();
    if array_addrs.len() != num_cols || schema_addrs.len() != num_cols {
        return Err(format!(
            "column count mismatch: batch has {num_cols}, got {} array / {} schema addresses",
            array_addrs.len(),
            schema_addrs.len()
        ));
    }
    // Export every column first; only once *all* succeed do we write into the
    // JVM-owned structs. Exporting can fail mid-loop (e.g. an unsupported data
    // type); writing incrementally would then leave already-written structs that
    // the JVM never imports (and thus never releases) — a leak. Staging into a
    // local Vec makes the write phase below infallible, so it is all-or-nothing.
    let mut exported = Vec::with_capacity(num_cols);
    for i in 0..num_cols {
        let data = batch.column(i).to_data();
        let schema = FFI_ArrowSchema::try_from(data.data_type())
            .map_err(|e| format!("failed to export schema for column {i}: {e}"))?;
        let array = FFI_ArrowArray::new(&data);
        exported.push((array, schema));
    }
    // The JVM allocated these structs; write the exported values into them. This
    // phase cannot fail, so no partial write is possible.
    for (i, (array, schema)) in exported.into_iter().enumerate() {
        std::ptr::write(array_addrs[i] as *mut FFI_ArrowArray, array);
        std::ptr::write(schema_addrs[i] as *mut FFI_ArrowSchema, schema);
    }
    Ok(())
}

/// Run the proto and export the (single) result batch into the JVM-allocated
/// FFI structs. Returns the row count, or `Err` with a message.
///
/// # Safety
/// See [`export_batch_to_addresses`].
pub unsafe fn submit_and_export(
    proto: &[u8],
    array_addrs: &[i64],
    schema_addrs: &[i64],
) -> Result<i64, String> {
    let (schema, batches) = execute_comet_proto(proto)?;
    // Concatenate to one batch so the JVM imports exactly one set of column
    // structs.
    let batch = concat_batches(&schema, &batches)
        .map_err(|e| format!("failed to concatenate result batches: {e}"))?;
    export_batch_to_addresses(&batch, array_addrs, schema_addrs)?;
    Ok(batch.num_rows() as i64)
}

// ---------------------------------------------------------------------------
// JNI entry point
// ---------------------------------------------------------------------------

mod jni_entry {
    use super::submit_and_export_offload;
    use crate::errors::{try_unwrap_or_throw, CometError};
    use jni::objects::{JByteArray, JClass, JLongArray, JString, ReleaseMode};
    use jni::sys::jlong;
    use jni::EnvUnowned;

    /// JVM entry: a no-op whose only purpose is symbol resolution. It is compiled
    /// only into a `--features ballista` `libcomet`, so the JVM side can detect
    /// whether the offload is present by resolving this symbol (see
    /// `NativeBallista.isAvailable`); a feature-less library lacks it and yields an
    /// `UnsatisfiedLinkError`.
    ///
    /// # Safety
    /// Called from the JVM via JNI.
    #[no_mangle]
    pub unsafe extern "system" fn Java_org_apache_comet_ballista_NativeBallista_probeAvailable(
        _e: EnvUnowned,
        _class: JClass,
    ) {
    }

    /// JVM entry: run a general DAG offload (R3), a `CometBallistaOffloadPlan`
    /// describing an arbitrary DAG of `CometFragmentExec` nodes joined by hash
    /// shuffles (folded by `build_offload_plan`). Submits it to a Ballista
    /// cluster — in-process standalone if `schedulerUrl` is empty, or the named
    /// external scheduler otherwise — and exports the concatenated result batch
    /// into the JVM-allocated Arrow C Data structs, returning the number of rows.
    ///
    /// # Safety
    /// Called from the JVM via JNI; the address arrays must reference valid
    /// caller-allocated `FFI_ArrowArray`/`FFI_ArrowSchema` structs (one per
    /// output column of the plan's final fragment).
    #[no_mangle]
    pub unsafe extern "system" fn Java_org_apache_comet_ballista_NativeBallista_executeOffloadPlan(
        e: EnvUnowned,
        _class: JClass,
        plan: JByteArray,
        array_addrs: JLongArray,
        schema_addrs: JLongArray,
        scheduler_url: JString,
    ) -> jlong {
        try_unwrap_or_throw(&e, |env| {
            let plan_bytes = env.convert_byte_array(plan)?;
            let scheduler_url: String = scheduler_url.try_to_string(env)?;

            let arrays = unsafe { array_addrs.get_elements(env, ReleaseMode::NoCopyBack)? };
            let schemas = unsafe { schema_addrs.get_elements(env, ReleaseMode::NoCopyBack)? };

            let num_rows = unsafe {
                submit_and_export_offload(&plan_bytes, &scheduler_url, &arrays, &schemas)
            }
            .map_err(CometError::Internal)?;
            Ok(num_rows as jlong)
        })
    }
}
