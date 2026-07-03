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
//!
//! This is a SPIKE. It proves the round trip JVM → native → Ballista → JVM.

use std::sync::Arc;

use ballista::prelude::{SessionConfigExt, SessionContextExt};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};

use datafusion_comet_proto::spark_operator::{operator::OpStruct, Operator};
use prost::Message;

/// Build the fixed spike test proto Rust-side: a single `NativeScan` over a
/// freshly written Parquet file with one int32 column `a` = [1..=5]. Returned to
/// the JVM so the JVM test can hand it straight back to [`Java_org_apache_comet_ballista_NativeBallista_executeQuery`]
/// without needing the generated proto Java classes. This is the same proto the
/// Rust `tests/` build.
pub fn build_test_proto() -> Result<Vec<u8>, String> {
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::DataType as ArrowDataType;
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion_comet_proto::spark_expression::{data_type::DataTypeId, DataType};
    use datafusion_comet_proto::spark_operator::{
        NativeScan, NativeScanCommon, SparkFilePartition, SparkPartitionedFile, SparkStructField,
    };

    let parquet = std::env::temp_dir().join("comet_ffi_ballista_jvm_spike.parquet");
    let arrow_schema = Arc::new(Schema::new(vec![Field::new(
        "a",
        ArrowDataType::Int32,
        true,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&arrow_schema),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))],
    )
    .map_err(|e| format!("failed to build test batch: {e}"))?;
    let file =
        std::fs::File::create(&parquet).map_err(|e| format!("failed to create parquet: {e}"))?;
    let mut writer = ArrowWriter::try_new(file, arrow_schema, None)
        .map_err(|e| format!("failed to open parquet writer: {e}"))?;
    writer
        .write(&batch)
        .map_err(|e| format!("failed to write parquet: {e}"))?;
    writer
        .close()
        .map_err(|e| format!("failed to close parquet: {e}"))?;

    let int32 = DataType {
        type_id: DataTypeId::Int32 as i32,
        type_info: None,
    };
    let field_a = SparkStructField {
        name: "a".to_string(),
        data_type: Some(int32),
        nullable: true,
        metadata: Default::default(),
    };
    let common = NativeScanCommon {
        required_schema: vec![field_a.clone()],
        data_schema: vec![field_a],
        projection_vector: vec![0],
        session_timezone: "UTC".to_string(),
        source: "comet-ffi-ballista-jvm-spike".to_string(),
        ..Default::default()
    };
    let file_size = std::fs::metadata(&parquet)
        .map_err(|e| format!("failed to stat parquet: {e}"))?
        .len() as i64;
    let partitioned_file = SparkPartitionedFile {
        file_path: format!("file://{}", parquet.display()),
        start: 0,
        length: file_size,
        file_size,
        partition_values: vec![],
    };
    let native_scan = NativeScan {
        common: Some(common),
        file_partition: Some(SparkFilePartition {
            partitioned_file: vec![partitioned_file],
        }),
    };
    let op = Operator {
        children: vec![],
        plan_id: 0,
        op_struct: Some(OpStruct::NativeScan(native_scan)),
    };
    Ok(op.encode_to_vec())
}

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
        // schema. This is the fix for the T1 spike's scan-schema shortcut: the
        // result schema now comes from the plan, not the NativeScan proto.
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
// R2: two-stage (distributed) GROUP BY offload
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

/// Build the R2 two-stage physical plan for a distributed GROUP BY:
///
/// ```text
/// CometFragmentExec(block2, children=[
///     RepartitionExec::Hash( CometFragmentExec(block1, children=[]), keys=0..num_group_keys, N )
/// ])
/// ```
///
/// `block1` is the partial aggregate (self-contained `NativeScan` leaf); its
/// output is `[group_keys..., agg_states...]`, so the group keys are columns
/// `0..num_group_keys`. Hash-repartitioning on those columns co-locates every
/// row for a group key on one downstream task, which is what lets the final
/// aggregate in `block2` compose across the shuffle. Ballista's
/// `DistributedPlanner` splits this plan at the `RepartitionExec(Hash)` into two
/// stages (block1 -> ShuffleWriter; ShuffleReader -> block2), and at stage 2 the
/// ShuffleReader becomes `block2`'s `Scan` (#100) input leaf.
fn build_two_stage_plan(
    block1_proto: &[u8],
    block2_proto: &[u8],
    num_group_keys: usize,
    num_partitions: usize,
) -> Result<Arc<dyn ExecutionPlan>, String> {
    let block1: Arc<dyn ExecutionPlan> = Arc::new(
        CometFragmentExec::try_new(block1_proto.to_vec(), vec![])
            .map_err(|e| format!("failed to build block1 (partial-agg) fragment: {e}"))?,
    );

    let schema1 = block1.schema();
    if num_group_keys == 0 || num_group_keys > schema1.fields().len() {
        return Err(format!(
            "invalid num_group_keys {num_group_keys}: block1 output has {} columns ({:?})",
            schema1.fields().len(),
            schema1
                .fields()
                .iter()
                .map(|f| f.name())
                .collect::<Vec<_>>()
        ));
    }

    // Investigation aid: the schema of the batches that cross Ballista's IPC
    // shuffle. block2's `Scan` (#100) leaf schema (derived from the exchange
    // output on the JVM side) must match this for the aggregate to compose.
    eprintln!(
        "[comet-ballista R2] block1 (partial-agg) output schema = {:?}",
        schema1
    );

    let hash_exprs: Vec<Arc<dyn PhysicalExpr>> = (0..num_group_keys)
        .map(|i| Arc::new(Column::new(schema1.field(i).name(), i)) as Arc<dyn PhysicalExpr>)
        .collect();

    let repart: Arc<dyn ExecutionPlan> = Arc::new(
        RepartitionExec::try_new(
            block1,
            Partitioning::Hash(hash_exprs, num_partitions.max(1)),
        )
        .map_err(|e| format!("failed to build hash RepartitionExec: {e}"))?,
    );

    let block2: Arc<dyn ExecutionPlan> = Arc::new(
        CometFragmentExec::try_new(block2_proto.to_vec(), vec![repart])
            .map_err(|e| format!("failed to build block2 (final-agg) fragment: {e}"))?,
    );

    eprintln!(
        "[comet-ballista R2] block2 (final-agg) output schema = {:?}",
        block2.schema()
    );

    Ok(block2)
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

/// Build and submit the R2 two-stage plan to a Ballista cluster, returning the
/// collected Arrow result batches plus the result schema.
///
/// If `scheduler_url` is empty, an **in-process standalone** cluster (scheduler +
/// executor spun up inside this process) is used, as before. If it is non-empty
/// (e.g. `http://localhost:50050`), the plan is submitted to that **external**
/// scheduler instead — a genuinely separate scheduler+executor deployment
/// (`comet-scheduler` / `comet-executor`). Either way the plan
/// is the identical `CometFragment → hash-shuffle → CometFragment`.
///
/// The Comet logical+physical codecs are registered on the SessionConfig so both
/// the `CometFragmentExec` nodes and Ballista's own shuffle operators survive
/// serialization to the scheduler/executor. On the external path the codecs must
/// *also* be registered on the scheduler and executor processes (the Comet-
/// flavored binaries do this via their config overrides) so the shipped Comet
/// nodes can be decoded there. The plan is submitted through T1's
/// `execute_physical_plan`, which fetches **all** output partitions of the final
/// stage (not just partition 0), so the returned batches cover every group.
pub fn execute_two_stage(
    block1_proto: &[u8],
    block2_proto: &[u8],
    num_group_keys: usize,
    num_partitions: usize,
    scheduler_url: &str,
) -> Result<(SchemaRef, Vec<RecordBatch>), String> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("failed to build tokio runtime: {e}"))?;

    runtime.block_on(async move {
        let n = num_partitions.max(1);
        let config = SessionConfig::new_with_ballista()
            .with_target_partitions(n)
            .with_ballista_standalone_parallelism(n)
            .with_ballista_physical_extension_codec(Arc::new(CometPhysicalCodec::default()))
            .with_ballista_logical_extension_codec(Arc::new(CometLogicalCodec::default()));
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .build();

        // Build the plan inside the runtime: the fragments' NativeScan leaf builds
        // via Comet's planner, which requires an active Tokio runtime.
        let plan = build_two_stage_plan(block1_proto, block2_proto, num_group_keys, n)?;
        let schema = plan.schema();

        // Empty URL => in-process standalone; non-empty => external cluster. For
        // the external path the scheduler creates the session from the submitted
        // settings + its own (Comet) codecs, so we do not start a local cluster.
        let scheduler_url = if scheduler_url.is_empty() {
            eprintln!("[comet-ballista R2] submitting to in-process standalone cluster");
            start_standalone_from_state(&state).await?
        } else {
            eprintln!("[comet-ballista R2] submitting to external cluster at {scheduler_url}");
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
        .map_err(|e| format!("failed to submit two-stage physical plan: {e}"))?;

        let batches = stream
            .try_collect::<Vec<_>>()
            .await
            .map_err(|e| format!("failed to collect distributed results: {e}"))?;

        Ok((schema, batches))
    })
}

/// Run the R2 two-stage plan and export the (single, concatenated) result batch
/// into the JVM-allocated FFI structs. Returns the row count.
///
/// # Safety
/// See [`export_batch_to_addresses`].
pub unsafe fn submit_and_export_distributed(
    block1_proto: &[u8],
    block2_proto: &[u8],
    num_group_keys: usize,
    num_partitions: usize,
    scheduler_url: &str,
    array_addrs: &[i64],
    schema_addrs: &[i64],
) -> Result<i64, String> {
    let (schema, batches) = execute_two_stage(
        block1_proto,
        block2_proto,
        num_group_keys,
        num_partitions,
        scheduler_url,
    )?;
    // The final stage's partitions are concatenated into one batch so the JVM
    // imports exactly one set of column structs (same contract as R1).
    let batch = concat_batches(&schema, &batches)
        .map_err(|e| format!("failed to concatenate result batches: {e}"))?;
    export_batch_to_addresses(&batch, array_addrs, schema_addrs)?;
    Ok(batch.num_rows() as i64)
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
    for i in 0..num_cols {
        let data = batch.column(i).to_data();
        let schema = FFI_ArrowSchema::try_from(data.data_type())
            .map_err(|e| format!("failed to export schema for column {i}: {e}"))?;
        let array = FFI_ArrowArray::new(&data);
        // The JVM allocated these structs; write the exported values into them.
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
    // The spike offloads a single small scan; concatenate to one batch so the
    // JVM imports exactly one set of column structs.
    let batch = concat_batches(&schema, &batches)
        .map_err(|e| format!("failed to concatenate result batches: {e}"))?;
    export_batch_to_addresses(&batch, array_addrs, schema_addrs)?;
    Ok(batch.num_rows() as i64)
}

// ---------------------------------------------------------------------------
// JNI entry point
// ---------------------------------------------------------------------------

mod jni_entry {
    use super::{build_test_proto, submit_and_export, submit_and_export_distributed};
    use crate::errors::{try_unwrap_or_throw, CometError};
    use jni::objects::{JByteArray, JClass, JLongArray, JString, ReleaseMode};
    use jni::sys::{jbyteArray, jint, jlong};
    use jni::EnvUnowned;

    /// JVM entry: build the fixed spike test proto Rust-side and return its
    /// bytes, so the JVM test does not need the generated proto Java classes.
    ///
    /// # Safety
    /// Called from the JVM via JNI.
    #[no_mangle]
    pub unsafe extern "system" fn Java_org_apache_comet_ballista_NativeBallista_buildTestProto(
        e: EnvUnowned,
        _class: JClass,
    ) -> jbyteArray {
        try_unwrap_or_throw(&e, |env| {
            let bytes = build_test_proto().map_err(CometError::Internal)?;
            let arr = env.byte_array_from_slice(&bytes)?;
            Ok(arr.into_raw())
        })
    }

    /// JVM entry: run a Comet `Operator` proto on in-process standalone Ballista
    /// and export the result batch into the JVM-allocated Arrow C Data structs
    /// (`FFI_ArrowArray`/`FFI_ArrowSchema`), returning the number of rows. This
    /// mirrors `Java_org_apache_comet_Native_executePlan`'s use of
    /// `prepare_output` — the JVM allocates the structs and imports them after
    /// this call returns.
    ///
    /// # Safety
    /// Called from the JVM via JNI; the address arrays must reference valid
    /// caller-allocated `FFI_ArrowArray`/`FFI_ArrowSchema` structs.
    #[no_mangle]
    pub unsafe extern "system" fn Java_org_apache_comet_ballista_NativeBallista_executeQuery(
        e: EnvUnowned,
        _class: JClass,
        proto: JByteArray,
        array_addrs: JLongArray,
        schema_addrs: JLongArray,
    ) -> jlong {
        try_unwrap_or_throw(&e, |env| {
            let proto_bytes = env.convert_byte_array(proto)?;

            let arrays = unsafe { array_addrs.get_elements(env, ReleaseMode::NoCopyBack)? };
            let schemas = unsafe { schema_addrs.get_elements(env, ReleaseMode::NoCopyBack)? };

            // SAFETY: the JVM allocated these FFI structs (Arrow Java
            // ArrowArray/ArrowSchema.allocateNew); we write the exported values
            // into them and the JVM imports them after this returns.
            let num_rows = unsafe { submit_and_export(&proto_bytes, &arrays, &schemas) }
                .map_err(CometError::Internal)?;
            Ok(num_rows as jlong)
        })
    }

    /// JVM entry: run a distributed (R2) two-stage GROUP BY offload. Builds
    /// `CometFragmentExec(block2, [Hash-Repartition(CometFragmentExec(block1))])`,
    /// submits it to an in-process standalone Ballista cluster (which splits it at
    /// the hash-repartition into a partial-agg stage and a final-agg stage over a
    /// shuffle), and exports the concatenated result batch into the JVM-allocated
    /// Arrow C Data structs, returning the number of rows.
    ///
    /// # Safety
    /// Called from the JVM via JNI; the address arrays must reference valid
    /// caller-allocated `FFI_ArrowArray`/`FFI_ArrowSchema` structs (one per output
    /// column of `block2`).
    #[no_mangle]
    pub unsafe extern "system" fn Java_org_apache_comet_ballista_NativeBallista_executeQueryDistributed(
        e: EnvUnowned,
        _class: JClass,
        block1: JByteArray,
        block2: JByteArray,
        num_group_keys: jint,
        num_partitions: jint,
        scheduler_url: JString,
        array_addrs: JLongArray,
        schema_addrs: JLongArray,
    ) -> jlong {
        try_unwrap_or_throw(&e, |env| {
            let block1_bytes = env.convert_byte_array(block1)?;
            let block2_bytes = env.convert_byte_array(block2)?;
            // Empty => in-process standalone (as before); non-empty (e.g.
            // "http://host:50050") => submit to that external scheduler.
            let scheduler_url: String = scheduler_url.try_to_string(env)?;

            let arrays = unsafe { array_addrs.get_elements(env, ReleaseMode::NoCopyBack)? };
            let schemas = unsafe { schema_addrs.get_elements(env, ReleaseMode::NoCopyBack)? };

            let num_rows = unsafe {
                submit_and_export_distributed(
                    &block1_bytes,
                    &block2_bytes,
                    num_group_keys as usize,
                    num_partitions as usize,
                    &scheduler_url,
                    &arrays,
                    &schemas,
                )
            }
            .map_err(CometError::Internal)?;
            Ok(num_rows as jlong)
        })
    }
}
