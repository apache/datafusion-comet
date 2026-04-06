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

pub mod data_type;
pub mod encryption_support;
pub mod mutable_vector;
pub use mutable_vector::*;

#[macro_use]
pub mod util;
pub mod parquet_exec;
pub mod parquet_read_cached_factory;
pub mod parquet_support;
pub mod read;
pub mod schema_adapter;

mod cast_column;
mod objectstore;

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

use self::util::jni::TypePromotionInfo;
use crate::execution::jni_api::get_runtime;
use crate::execution::metrics::utils::update_comet_metric;
use crate::execution::operators::ExecutionError;
use crate::execution::planner::PhysicalPlanner;
use crate::execution::serde;
use crate::execution::spark_plan::SparkPlan;
use crate::execution::utils::SparkArrowConvert;
use crate::jvm_bridge::JVMClasses;
use crate::parquet::data_type::AsBytes;
use crate::parquet::encryption_support::{CometEncryptionFactory, ENCRYPTION_FACTORY_ID};
use crate::parquet::parquet_exec::init_datasource_exec;
use crate::parquet::parquet_support::prepare_object_store_with_configs;
use arrow::array::{Array, RecordBatch};
use arrow::buffer::MutableBuffer;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::{poll, StreamExt};
use jni::objects::{JByteArray, JLongArray, JMap, JObject, JObjectArray, JString, ReleaseMode};
use jni::sys::{jintArray, JNI_FALSE};
use object_store::path::Path;
use read::ColumnReader;
use util::jni::{convert_column_descriptor, convert_encoding, deserialize_schema};

/// Parquet read context maintained across multiple JNI calls.
struct Context {
    pub column_reader: ColumnReader,
}

#[no_mangle]
pub extern "system" fn Java_org_apache_comet_parquet_Native_initColumnReader(
    e: EnvUnowned,
    _jclass: JClass,
    primitive_type: jint,
    logical_type: jint,
    read_primitive_type: jint,
    jni_path: JObjectArray,
    max_dl: jint,
    max_rl: jint,
    bit_width: jint,
    read_bit_width: jint,
    is_signed: jboolean,
    type_length: jint,
    precision: jint,
    read_precision: jint,
    scale: jint,
    read_scale: jint,
    time_unit: jint,
    is_adjusted_utc: jboolean,
    batch_size: jint,
    use_decimal_128: jboolean,
    use_legacy_date_timestamp: jboolean,
) -> jlong {
    try_unwrap_or_throw(&e, |env| {
        let desc = convert_column_descriptor(
            env,
            primitive_type,
            logical_type,
            max_dl,
            max_rl,
            bit_width,
            is_signed,
            type_length,
            precision,
            scale,
            time_unit,
            is_adjusted_utc,
            jni_path,
        )?;
        let promotion_info = TypePromotionInfo::new_from_jni(
            read_primitive_type,
            read_precision,
            read_scale,
            read_bit_width,
        );
        let ctx = Context {
            column_reader: ColumnReader::get(
                desc,
                promotion_info,
                batch_size as usize,
                use_decimal_128,
                use_legacy_date_timestamp,
            ),
        };
        let res = Box::new(ctx);
        Ok(Box::into_raw(res) as i64)
    })
}

/// # Safety
/// This function is inheritly unsafe since it deals with raw pointers passed from JNI.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_comet_parquet_Native_setDictionaryPage(
    e: EnvUnowned,
    _jclass: JClass,
    handle: jlong,
    page_value_count: jint,
    page_data: JByteArray,
    encoding: jint,
) {
    try_unwrap_or_throw(&e, |env| {
        let reader = get_reader(handle)?;

        // convert value encoding ordinal to the native encoding definition
        let encoding = convert_encoding(encoding);

        // copy the input on-heap buffer to native
        let page_len = page_data.len(env)?;
        let mut buffer = MutableBuffer::from_len_zeroed(page_len);
        page_data.get_region(env, 0, from_u8_slice(buffer.as_slice_mut()))?;

        reader.set_dictionary_page(page_value_count as usize, buffer.into(), encoding);
        Ok(())
    })
}

/// # Safety
/// This function is inheritly unsafe since it deals with raw pointers passed from JNI.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_comet_parquet_Native_setPageV1(
    e: EnvUnowned,
    _jclass: JClass,
    handle: jlong,
    page_value_count: jint,
    page_data: JByteArray,
    value_encoding: jint,
) {
    try_unwrap_or_throw(&e, |env| {
        let reader = get_reader(handle)?;

        // convert value encoding ordinal to the native encoding definition
        let encoding = convert_encoding(value_encoding);

        // copy the input on-heap buffer to native
        let page_len = page_data.len(env)?;
        let mut buffer = MutableBuffer::from_len_zeroed(page_len);
        page_data.get_region(env, 0, from_u8_slice(buffer.as_slice_mut()))?;

        reader.set_page_v1(page_value_count as usize, buffer.into(), encoding);
        Ok(())
    })
}

/// # Safety
/// This function is inheritly unsafe since it deals with raw pointers passed from JNI.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_comet_parquet_Native_setPageV2(
    e: EnvUnowned,
    _jclass: JClass,
    handle: jlong,
    page_value_count: jint,
    def_level_data: JByteArray,
    rep_level_data: JByteArray,
    value_data: JByteArray,
    value_encoding: jint,
) {
    try_unwrap_or_throw(&e, |env| {
        let reader = get_reader(handle)?;

        // convert value encoding ordinal to the native encoding definition
        let encoding = convert_encoding(value_encoding);

        // copy the input on-heap buffer to native
        let dl_len = def_level_data.len(env)?;
        let mut dl_buffer = MutableBuffer::from_len_zeroed(dl_len);
        def_level_data.get_region(env, 0, from_u8_slice(dl_buffer.as_slice_mut()))?;

        let rl_len = rep_level_data.len(env)?;
        let mut rl_buffer = MutableBuffer::from_len_zeroed(rl_len);
        rep_level_data.get_region(env, 0, from_u8_slice(rl_buffer.as_slice_mut()))?;

        let v_len = value_data.len(env)?;
        let mut v_buffer = MutableBuffer::from_len_zeroed(v_len);
        value_data.get_region(env, 0, from_u8_slice(v_buffer.as_slice_mut()))?;

        reader.set_page_v2(
            page_value_count as usize,
            dl_buffer.into(),
            rl_buffer.into(),
            v_buffer.into(),
            encoding,
        );
        Ok(())
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_comet_parquet_Native_resetBatch(
    env: EnvUnowned,
    _jclass: JClass,
    handle: jlong,
) {
    try_unwrap_or_throw(&env, |_| {
        let reader = get_reader(handle)?;
        reader.reset_batch();
        Ok(())
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_comet_parquet_Native_readBatch(
    e: EnvUnowned,
    _jclass: JClass,
    handle: jlong,
    batch_size: jint,
    null_pad_size: jint,
) -> jintArray {
    try_unwrap_or_throw(&e, |env| {
        let reader = get_reader(handle)?;
        let (num_values, num_nulls) =
            reader.read_batch(batch_size as usize, null_pad_size as usize);
        let res = env.new_int_array(2)?;
        let buf: [i32; 2] = [num_values as i32, num_nulls as i32];
        res.set_region(env, 0, &buf)?;
        Ok(res.into_raw())
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_comet_parquet_Native_skipBatch(
    env: EnvUnowned,
    _jclass: JClass,
    handle: jlong,
    batch_size: jint,
    discard: jboolean,
) -> jint {
    try_unwrap_or_throw(&env, |_| {
        let reader = get_reader(handle)?;
        Ok(reader.skip_batch(batch_size as usize, discard) as jint)
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_comet_parquet_Native_currentBatch(
    e: EnvUnowned,
    _jclass: JClass,
    handle: jlong,
    array_addr: jlong,
    schema_addr: jlong,
) {
    try_unwrap_or_throw(&e, |_env| {
        let ctx = get_context(handle)?;
        let reader = &mut ctx.column_reader;
        let data = reader.current_batch()?;
        data.move_to_spark(array_addr, schema_addr)
            .map_err(|e| e.into())
    })
}

#[inline]
fn get_context<'a>(handle: jlong) -> Result<&'a mut Context, CometError> {
    unsafe {
        (handle as *mut Context)
            .as_mut()
            .ok_or_else(|| CometError::NullPointer("null context handle".to_string()))
    }
}

#[inline]
fn get_reader<'a>(handle: jlong) -> Result<&'a mut ColumnReader, CometError> {
    Ok(&mut get_context(handle)?.column_reader)
}

#[no_mangle]
pub extern "system" fn Java_org_apache_comet_parquet_Native_closeColumnReader(
    env: EnvUnowned,
    _jclass: JClass,
    handle: jlong,
) {
    try_unwrap_or_throw(&env, |_| {
        unsafe {
            let ctx = get_context(handle)?;
            let _ = Box::from_raw(ctx);
        };
        Ok(())
    })
}

fn from_u8_slice(src: &mut [u8]) -> &mut [i8] {
    let raw_ptr = src.as_mut_ptr() as *mut i8;
    unsafe { std::slice::from_raw_parts_mut(raw_ptr, src.len()) }
}

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
pub unsafe extern "system" fn Java_org_apache_comet_parquet_Native_validateObjectStoreConfig(
    e: EnvUnowned,
    _jclass: JClass,
    file_path: JString,
    object_store_options: JObject,
) {
    try_unwrap_or_throw(&e, |env| {
        let session_config = SessionConfig::new();
        let planner =
            PhysicalPlanner::new(Arc::new(SessionContext::new_with_config(session_config)), 0);
        let session_ctx = planner.session_ctx();
        let path: String = file_path.try_to_string(env).unwrap();
        let object_store_config = get_object_store_options(env, object_store_options)?;
        let (_, _) = prepare_object_store_with_configs(
            session_ctx.runtime_env(),
            path.clone(),
            &object_store_config,
        )?;
        Ok(())
    })
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
        let required_schema = Arc::new(deserialize_schema(required_schema_buffer.as_bytes())?);

        let data_schema_buffer = env.convert_byte_array(&data_schema)?;
        let data_schema = Arc::new(deserialize_schema(data_schema_buffer.as_bytes())?);

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
            session_ctx,
            encryption_enabled,
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
