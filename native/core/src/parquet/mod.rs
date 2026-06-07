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

pub mod encryption_support;

pub mod parquet_exec;
pub mod parquet_read_cached_factory;
pub mod parquet_support;
pub mod schema_adapter;
pub mod util;

mod cast_column;
pub(crate) mod objectstore;

use std::collections::HashMap;
use std::task::Poll;
use std::{boxed::Box, sync::Arc};

use crate::errors::{try_unwrap_or_throw, CometError};

/// JNI exposed methods
use jni::{
    objects::{Global, JClass},
    sys::{jboolean, jint, jlong},
    Env, EnvUnowned,
};

use crate::execution::jni_api::get_runtime;
use crate::execution::metrics::utils::update_comet_metric;
use crate::execution::operators::ExecutionError;
use crate::execution::planner::PhysicalPlanner;
use crate::execution::serde;
use crate::execution::spark_plan::SparkPlan;
use crate::execution::utils::SparkArrowConvert;
use crate::jvm_bridge::JVMClasses;
use crate::parquet::encryption_support::{CometEncryptionFactory, ENCRYPTION_FACTORY_ID};
use crate::parquet::parquet_exec::init_datasource_exec;
use crate::parquet::parquet_support::prepare_object_store_with_configs;
use arrow::array::{Array, RecordBatch};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::{poll, StreamExt};
use jni::objects::{JByteArray, JLongArray, JMap, JObject, JString, ReleaseMode};
use jni::sys::JNI_FALSE;
use object_store::path::Path;
use util::jni::deserialize_schema;

// TODO: (ARROW NATIVE) remove this if not needed.
enum ParquetReaderState {
    Init,
    Reading,
    Complete,
}
/// Parquet read context maintained across multiple JNI calls.
struct BatchContext {
    native_plan: Arc<SparkPlan>,
    metrics_node: Arc<Global<JObject<'static>>>,
    batch_stream: Option<SendableRecordBatchStream>,
    current_batch: Option<RecordBatch>,
    reader_state: ParquetReaderState,
}

#[inline]
fn get_batch_context<'a>(handle: jlong) -> Result<&'a mut BatchContext, CometError> {
    unsafe {
        (handle as *mut BatchContext)
            .as_mut()
            .ok_or_else(|| CometError::NullPointer("null batch context handle".to_string()))
    }
}

fn get_file_groups_single_file(
    path: &Path,
    file_size: u64,
    starts: &mut [i64],
    lengths: &mut [i64],
) -> Vec<Vec<PartitionedFile>> {
    assert!(!starts.is_empty() && starts.len() == lengths.len());
    let mut groups: Vec<PartitionedFile> = Vec::with_capacity(starts.len());
    for (i, &start) in starts.iter().enumerate() {
        let mut partitioned_file = PartitionedFile::new_with_range(
            String::new(), // Dummy file path. We will override this with our path so that url encoding does not occur
            file_size,
            start,
            start + lengths[i],
        );
        partitioned_file.object_meta.location = (*path).clone();
        groups.push(partitioned_file);
    }
    vec![groups]
}

pub fn get_object_store_options(
    env: &mut Env,
    map_object: JObject,
) -> Result<HashMap<String, String>, CometError> {
    let map = env.cast_local::<JMap>(map_object)?;
    // Convert to a HashMap
    let mut collected_map = HashMap::new();
    map.iter(env).and_then(|mut iter| {
        while let Some(entry) = iter.next(env)? {
            let key = entry.key(env)?;
            let value = entry.value(env)?;
            let key = unsafe { JString::from_raw(env, key.into_raw()) };
            let value = unsafe { JString::from_raw(env, value.into_raw()) };
            let key_string = key.try_to_string(env)?;
            let value_string = value.try_to_string(env)?;
            collected_map.insert(key_string, value_string);
        }
        Ok(())
    })?;

    Ok(collected_map)
}

/// # Safety
/// This function is inherently unsafe since it deals with raw pointers passed from JNI.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_comet_parquet_Native_initRecordBatchReader(
    e: EnvUnowned,
    _jclass: JClass,
    file_path: JString,
    file_size: jlong,
    starts: JLongArray,
    lengths: JLongArray,
    filter: JByteArray,
    required_schema: JByteArray,
    data_schema: JByteArray,
    session_timezone: JString,
    batch_size: jint,
    case_sensitive: jboolean,
    return_null_struct_if_all_fields_missing: jboolean,
    object_store_options: JObject,
    key_unwrapper_obj: JObject,
    metrics_node: JObject,
) -> jlong {
    try_unwrap_or_throw(&e, |env| unsafe {
        JVMClasses::init(env);
        let session_config = SessionConfig::new().with_batch_size(batch_size as usize);
        let planner =
            PhysicalPlanner::new(Arc::new(SessionContext::new_with_config(session_config)), 0);
        let session_ctx = planner.session_ctx();

        let path: String = file_path.try_to_string(env).unwrap();

        let object_store_config = get_object_store_options(env, object_store_options)?;
        let (object_store_url, object_store_path) = prepare_object_store_with_configs(
            session_ctx.runtime_env(),
            path.clone(),
            &object_store_config,
        )?;

        let required_schema_buffer = env.convert_byte_array(&required_schema)?;
        let required_schema = Arc::new(deserialize_schema(&required_schema_buffer)?);

        let data_schema_buffer = env.convert_byte_array(&data_schema)?;
        let data_schema = Arc::new(deserialize_schema(&data_schema_buffer)?);

        let data_filters = if !filter.is_null() {
            let filter_buffer = env.convert_byte_array(&filter)?;
            let filter_expr = serde::deserialize_expr(filter_buffer.as_slice())?;
            Some(vec![
                planner.create_expr(&filter_expr, Arc::clone(&data_schema))?
            ])
        } else {
            None
        };
        let starts = starts.get_elements(env, ReleaseMode::NoCopyBack)?;
        let starts = core::slice::from_raw_parts_mut(starts.as_ptr(), starts.len());

        let lengths = lengths.get_elements(env, ReleaseMode::NoCopyBack)?;
        let lengths = core::slice::from_raw_parts_mut(lengths.as_ptr(), lengths.len());

        let file_groups =
            get_file_groups_single_file(&object_store_path, file_size as u64, starts, lengths);

        let session_timezone: String = session_timezone.try_to_string(env).unwrap();

        // Handle key unwrapper for encrypted files
        let encryption_enabled = if !key_unwrapper_obj.is_null() {
            let encryption_factory = CometEncryptionFactory {
                key_unwrapper: Arc::new(jni_new_global_ref!(env, key_unwrapper_obj)?),
            };
            session_ctx
                .runtime_env()
                .register_parquet_encryption_factory(
                    ENCRYPTION_FACTORY_ID,
                    Arc::new(encryption_factory),
                );
            true
        } else {
            false
        };

        let scan = init_datasource_exec(
            required_schema,
            Some(data_schema),
            None,
            object_store_url,
            file_groups,
            None,
            data_filters,
            None,
            session_timezone.as_str(),
            case_sensitive != JNI_FALSE,
            return_null_struct_if_all_fields_missing != JNI_FALSE,
            true, // allow_type_promotion: JVM side already validated via TypeUtil.checkParquetType
            session_ctx,
            encryption_enabled,
            // The iceberg-compat path resolves IDs in the JVM via NativeBatchReader,
            // so the native side does not need to do field-ID matching here.
            false,
            false,
        )?;

        let partition_index: usize = 0;
        let batch_stream = scan.execute(partition_index, session_ctx.task_ctx())?;

        let ctx = BatchContext {
            native_plan: Arc::new(SparkPlan::new(0, scan, vec![])),
            metrics_node: Arc::new(jni_new_global_ref!(env, metrics_node)?),
            batch_stream: Some(batch_stream),
            current_batch: None,
            reader_state: ParquetReaderState::Init,
        };
        let res = Box::new(ctx);

        Ok(Box::into_raw(res) as i64)
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_comet_parquet_Native_readNextRecordBatch(
    e: EnvUnowned,
    _jclass: JClass,
    handle: jlong,
) -> jint {
    try_unwrap_or_throw(&e, |env| {
        let context = get_batch_context(handle)?;
        let mut rows_read: i32 = 0;
        let batch_stream = context.batch_stream.as_mut().unwrap();
        let runtime = get_runtime();

        loop {
            let next_item = batch_stream.next();
            let poll_batch: Poll<Option<datafusion::common::Result<RecordBatch>>> =
                runtime.block_on(async { poll!(next_item) });

            match poll_batch {
                Poll::Ready(Some(batch)) => {
                    let batch = batch?;
                    rows_read = batch.num_rows() as i32;
                    context.current_batch = Some(batch);
                    context.reader_state = ParquetReaderState::Reading;
                    break;
                }
                Poll::Ready(None) => {
                    // EOF

                    update_comet_metric(env, context.metrics_node.as_obj(), &context.native_plan)?;

                    context.current_batch = None;
                    context.reader_state = ParquetReaderState::Complete;
                    break;
                }
                Poll::Pending => {
                    // TODO: (ARROW NATIVE): Just keeping polling??
                    // Ideally we want to yield to avoid consuming CPU while blocked on IO ??
                    continue;
                }
            }
        }
        Ok(rows_read)
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_comet_parquet_Native_currentColumnBatch(
    e: EnvUnowned,
    _jclass: JClass,
    handle: jlong,
    column_idx: jint,
    array_addr: jlong,
    schema_addr: jlong,
) {
    try_unwrap_or_throw(&e, |_env| {
        let context = get_batch_context(handle)?;
        let batch_reader = context
            .current_batch
            .as_mut()
            .ok_or_else(|| CometError::Execution {
                source: ExecutionError::GeneralError("There is no more data to read".to_string()),
            });
        let data = batch_reader?.column(column_idx as usize).into_data();
        data.move_to_spark(array_addr, schema_addr)
            .map_err(|e| e.into())
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_comet_parquet_Native_closeRecordBatchReader(
    env: EnvUnowned,
    _jclass: JClass,
    handle: jlong,
) {
    try_unwrap_or_throw(&env, |_| {
        unsafe {
            let ctx = get_batch_context(handle)?;
            let _ = Box::from_raw(ctx);
        };
        Ok(())
    })
}
