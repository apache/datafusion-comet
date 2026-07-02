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

use crate::scan::CometScanExec;
use crate::{CometLogicalCodec, CometPhysicalCodec, CometTableProvider};

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
    use super::{build_test_proto, submit_and_export};
    use comet::errors::{try_unwrap_or_throw, CometError};
    use jni::objects::{JByteArray, JClass, JLongArray, ReleaseMode};
    use jni::sys::{jbyteArray, jlong};
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
}
