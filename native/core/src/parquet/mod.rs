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
pub mod mutable_vector;
pub use mutable_vector::*;

#[macro_use]
pub mod util;
pub mod read;

use std::task::Poll;
use std::{boxed::Box, ptr::NonNull, sync::Arc};

use crate::errors::{try_unwrap_or_throw, CometError};

use arrow::ffi::FFI_ArrowArray;

/// JNI exposed methods
use jni::JNIEnv;
use jni::{
    objects::{GlobalRef, JByteBuffer, JClass},
    sys::{
        jboolean, jbooleanArray, jbyte, jbyteArray, jdouble, jfloat, jint, jintArray, jlong,
        jlongArray, jobject, jobjectArray, jshort,
    },
};

use crate::execution::operators::ExecutionError;
use crate::execution::utils::SparkArrowConvert;
use crate::parquet::data_type::AsBytes;
use arrow::buffer::{Buffer, MutableBuffer};
use arrow_array::{Array, RecordBatch};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::parquet::ParquetExecBuilder;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_comet_spark_expr::parquet_support::SparkParquetOptions;
use datafusion_comet_spark_expr::{EvalMode, SparkSchemaAdapterFactory};
use datafusion_common::config::TableParquetOptions;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use futures::{poll, StreamExt};
use jni::objects::{JBooleanArray, JByteArray, JLongArray, JPrimitiveArray, JString, ReleaseMode};
use jni::sys::jstring;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use read::ColumnReader;
use util::jni::{convert_column_descriptor, convert_encoding, deserialize_schema, get_file_path};

use self::util::jni::TypePromotionInfo;

/// Parquet read context maintained across multiple JNI calls.
struct Context {
    pub column_reader: ColumnReader,
    last_data_page: Option<GlobalRef>,
}

#[no_mangle]
pub extern "system" fn Java_org_apache_comet_parquet_Native_initColumnReader(
    e: JNIEnv,
    _jclass: JClass,
    primitive_type: jint,
    logical_type: jint,
    read_primitive_type: jint,
    jni_path: jobjectArray,
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
    try_unwrap_or_throw(&e, |mut env| {
        let desc = convert_column_descriptor(
            &mut env,
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
                use_decimal_128 != 0,
                use_legacy_date_timestamp != 0,
            ),
            last_data_page: None,
        };
        let res = Box::new(ctx);
        Ok(Box::into_raw(res) as i64)
    })
}

/// # Safety
/// This function is inheritly unsafe since it deals with raw pointers passed from JNI.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_comet_parquet_Native_setDictionaryPage(
    e: JNIEnv,
    _jclass: JClass,
    handle: jlong,
    page_value_count: jint,
    page_data: jbyteArray,
    encoding: jint,
) {
    try_unwrap_or_throw(&e, |env| {
        let reader = get_reader(handle)?;

        // convert value encoding ordinal to the native encoding definition
        let encoding = convert_encoding(encoding);

        // copy the input on-heap buffer to native
        let page_data_array = unsafe { JPrimitiveArray::from_raw(page_data) };
        let page_len = env.get_array_length(&page_data_array)?;
        let mut buffer = MutableBuffer::from_len_zeroed(page_len as usize);
        env.get_byte_array_region(&page_data_array, 0, from_u8_slice(buffer.as_slice_mut()))?;

        reader.set_dictionary_page(page_value_count as usize, buffer.into(), encoding);
        Ok(())
    })
}

/// # Safety
/// This function is inheritly unsafe since it deals with raw pointers passed from JNI.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_comet_parquet_Native_setPageV1(
    e: JNIEnv,
    _jclass: JClass,
    handle: jlong,
    page_value_count: jint,
    page_data: jbyteArray,
    value_encoding: jint,
) {
    try_unwrap_or_throw(&e, |env| {
        let reader = get_reader(handle)?;

        // convert value encoding ordinal to the native encoding definition
        let encoding = convert_encoding(value_encoding);

        // copy the input on-heap buffer to native
        let page_data_array = unsafe { JPrimitiveArray::from_raw(page_data) };
        let page_len = env.get_array_length(&page_data_array)?;
        let mut buffer = MutableBuffer::from_len_zeroed(page_len as usize);
        env.get_byte_array_region(&page_data_array, 0, from_u8_slice(buffer.as_slice_mut()))?;

        reader.set_page_v1(page_value_count as usize, buffer.into(), encoding);
        Ok(())
    })
}

/// # Safety
/// This function is inheritly unsafe since it deals with raw pointers passed from JNI.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_comet_parquet_Native_setPageBufferV1(
    e: JNIEnv,
    _jclass: JClass,
    handle: jlong,
    page_value_count: jint,
    buffer: jobject,
    value_encoding: jint,
) {
    try_unwrap_or_throw(&e, |env| {
        let ctx = get_context(handle)?;
        let reader = &mut ctx.column_reader;

        // convert value encoding ordinal to the native encoding definition
        let encoding = convert_encoding(value_encoding);

        // Get slices from Java DirectByteBuffer
        let jbuffer = unsafe { JByteBuffer::from_raw(buffer) };

        // Convert the page to global reference so it won't get GC'd by Java. Also free the last
        // page if there is any.
        ctx.last_data_page = Some(env.new_global_ref(&jbuffer)?);

        let buf_slice = env.get_direct_buffer_address(&jbuffer)?;
        let buf_capacity = env.get_direct_buffer_capacity(&jbuffer)?;

        unsafe {
            let page_ptr = NonNull::new_unchecked(buf_slice);
            let buffer = Buffer::from_custom_allocation(
                page_ptr,
                buf_capacity,
                Arc::new(FFI_ArrowArray::empty()),
            );
            reader.set_page_v1(page_value_count as usize, buffer, encoding);
        }
        Ok(())
    })
}

/// # Safety
/// This function is inheritly unsafe since it deals with raw pointers passed from JNI.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_comet_parquet_Native_setPageV2(
    e: JNIEnv,
    _jclass: JClass,
    handle: jlong,
    page_value_count: jint,
    def_level_data: jbyteArray,
    rep_level_data: jbyteArray,
    value_data: jbyteArray,
    value_encoding: jint,
) {
    try_unwrap_or_throw(&e, |env| {
        let reader = get_reader(handle)?;

        // convert value encoding ordinal to the native encoding definition
        let encoding = convert_encoding(value_encoding);

        // copy the input on-heap buffer to native
        let def_level_array = unsafe { JPrimitiveArray::from_raw(def_level_data) };
        let dl_len = env.get_array_length(&def_level_array)?;
        let mut dl_buffer = MutableBuffer::from_len_zeroed(dl_len as usize);
        env.get_byte_array_region(&def_level_array, 0, from_u8_slice(dl_buffer.as_slice_mut()))?;

        let rep_level_array = unsafe { JPrimitiveArray::from_raw(rep_level_data) };
        let rl_len = env.get_array_length(&rep_level_array)?;
        let mut rl_buffer = MutableBuffer::from_len_zeroed(rl_len as usize);
        env.get_byte_array_region(&rep_level_array, 0, from_u8_slice(rl_buffer.as_slice_mut()))?;

        let value_array = unsafe { JPrimitiveArray::from_raw(value_data) };
        let v_len = env.get_array_length(&value_array)?;
        let mut v_buffer = MutableBuffer::from_len_zeroed(v_len as usize);
        env.get_byte_array_region(&value_array, 0, from_u8_slice(v_buffer.as_slice_mut()))?;

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
pub extern "system" fn Java_org_apache_comet_parquet_Native_setNull(
    env: JNIEnv,
    _jclass: JClass,
    handle: jlong,
) {
    try_unwrap_or_throw(&env, |_| {
        let reader = get_reader(handle)?;
        reader.set_null();
        Ok(())
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_comet_parquet_Native_setBoolean(
    env: JNIEnv,
    _jclass: JClass,
    handle: jlong,
    value: jboolean,
) {
    try_unwrap_or_throw(&env, |_| {
        let reader = get_reader(handle)?;
        reader.set_boolean(value != 0);
        Ok(())
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_comet_parquet_Native_setByte(
    env: JNIEnv,
    _jclass: JClass,
    handle: jlong,
    value: jbyte,
) {
    try_unwrap_or_throw(&env, |_| {
        let reader = get_reader(handle)?;
        reader.set_fixed::<i8>(value);
        Ok(())
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_comet_parquet_Native_setShort(
    env: JNIEnv,
    _jclass: JClass,
    handle: jlong,
    value: jshort,
) {
    try_unwrap_or_throw(&env, |_| {
        let reader = get_reader(handle)?;
        reader.set_fixed::<i16>(value);
        Ok(())
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_comet_parquet_Native_setInt(
    env: JNIEnv,
    _jclass: JClass,
    handle: jlong,
    value: jint,
) {
    try_unwrap_or_throw(&env, |_| {
        let reader = get_reader(handle)?;
        reader.set_fixed::<i32>(value);
        Ok(())
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_comet_parquet_Native_setLong(
    env: JNIEnv,
    _jclass: JClass,
    handle: jlong,
    value: jlong,
) {
    try_unwrap_or_throw(&env, |_| {
        let reader = get_reader(handle)?;
        reader.set_fixed::<i64>(value);
        Ok(())
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_comet_parquet_Native_setFloat(
    env: JNIEnv,
    _jclass: JClass,
    handle: jlong,
    value: jfloat,
) {
    try_unwrap_or_throw(&env, |_| {
        let reader = get_reader(handle)?;
        reader.set_fixed::<f32>(value);
        Ok(())
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_comet_parquet_Native_setDouble(
    env: JNIEnv,
    _jclass: JClass,
    handle: jlong,
    value: jdouble,
) {
    try_unwrap_or_throw(&env, |_| {
        let reader = get_reader(handle)?;
        reader.set_fixed::<f64>(value);
        Ok(())
    })
}

/// # Safety
/// This function is inheritly unsafe since it deals with raw pointers passed from JNI.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_comet_parquet_Native_setBinary(
    e: JNIEnv,
    _jclass: JClass,
    handle: jlong,
    value: jbyteArray,
) {
    try_unwrap_or_throw(&e, |env| {
        let reader = get_reader(handle)?;

        let value_array = unsafe { JPrimitiveArray::from_raw(value) };
        let len = env.get_array_length(&value_array)?;
        let mut buffer = MutableBuffer::from_len_zeroed(len as usize);
        env.get_byte_array_region(&value_array, 0, from_u8_slice(buffer.as_slice_mut()))?;
        reader.set_binary(buffer);
        Ok(())
    })
}

/// # Safety
/// This function is inheritly unsafe since it deals with raw pointers passed from JNI.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_comet_parquet_Native_setDecimal(
    e: JNIEnv,
    _jclass: JClass,
    handle: jlong,
    value: jbyteArray,
) {
    try_unwrap_or_throw(&e, |env| {
        let reader = get_reader(handle)?;

        let value_array = unsafe { JPrimitiveArray::from_raw(value) };
        let len = env.get_array_length(&value_array)?;
        let mut buffer = MutableBuffer::from_len_zeroed(len as usize);
        env.get_byte_array_region(&value_array, 0, from_u8_slice(buffer.as_slice_mut()))?;
        reader.set_decimal_flba(buffer);
        Ok(())
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_comet_parquet_Native_setPosition(
    env: JNIEnv,
    _jclass: JClass,
    handle: jlong,
    value: jlong,
    size: jint,
) {
    try_unwrap_or_throw(&env, |_| {
        let reader = get_reader(handle)?;
        reader.set_position(value, size as usize);
        Ok(())
    })
}

/// # Safety
/// This function is inheritly unsafe since it deals with raw pointers passed from JNI.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_comet_parquet_Native_setIndices(
    e: JNIEnv,
    _jclass: JClass,
    handle: jlong,
    offset: jlong,
    batch_size: jint,
    indices: jlongArray,
) -> jlong {
    try_unwrap_or_throw(&e, |mut env| {
        let reader = get_reader(handle)?;
        let indice_array = unsafe { JLongArray::from_raw(indices) };
        let indices = unsafe { env.get_array_elements(&indice_array, ReleaseMode::NoCopyBack)? };
        let len = indices.len();
        // paris alternately contains start index and length of continuous indices
        let pairs = unsafe { core::slice::from_raw_parts_mut(indices.as_ptr(), len) };
        let mut skipped = 0;
        let mut filled = 0;
        for i in (0..len).step_by(2) {
            let index = pairs[i];
            let count = pairs[i + 1];
            let skip = std::cmp::min(count, offset - skipped);
            skipped += skip;
            if count == skip {
                continue;
            } else if batch_size as i64 == filled {
                break;
            }
            let count = std::cmp::min(count - skip, batch_size as i64 - filled);
            filled += count;
            reader.set_position(index + skip, count as usize);
        }
        Ok(filled)
    })
}

/// # Safety
/// This function is inheritly unsafe since it deals with raw pointers passed from JNI.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_comet_parquet_Native_setIsDeleted(
    e: JNIEnv,
    _jclass: JClass,
    handle: jlong,
    is_deleted: jbooleanArray,
) {
    try_unwrap_or_throw(&e, |env| {
        let reader = get_reader(handle)?;

        let is_deleted_array = unsafe { JBooleanArray::from_raw(is_deleted) };
        let len = env.get_array_length(&is_deleted_array)?;
        let mut buffer = MutableBuffer::from_len_zeroed(len as usize);
        env.get_boolean_array_region(&is_deleted_array, 0, buffer.as_slice_mut())?;
        reader.set_is_deleted(buffer);
        Ok(())
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_comet_parquet_Native_resetBatch(
    env: JNIEnv,
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
    e: JNIEnv,
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
        env.set_int_array_region(&res, 0, &buf)?;
        Ok(res.into_raw())
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_comet_parquet_Native_skipBatch(
    env: JNIEnv,
    _jclass: JClass,
    handle: jlong,
    batch_size: jint,
    discard: jboolean,
) -> jint {
    try_unwrap_or_throw(&env, |_| {
        let reader = get_reader(handle)?;
        Ok(reader.skip_batch(batch_size as usize, discard == 0) as jint)
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_comet_parquet_Native_currentBatch(
    e: JNIEnv,
    _jclass: JClass,
    handle: jlong,
    array_addr: jlong,
    schema_addr: jlong,
) {
    try_unwrap_or_throw(&e, |_env| {
        let ctx = get_context(handle)?;
        let reader = &mut ctx.column_reader;
        let data = reader.current_batch();
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
    env: JNIEnv,
    _jclass: JClass,
    handle: jlong,
) {
    try_unwrap_or_throw(&env, |_| {
        unsafe {
            let ctx = handle as *mut Context;
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
    runtime: tokio::runtime::Runtime,
    batch_stream: Option<SendableRecordBatchStream>,
    batch_reader: Option<ParquetRecordBatchReader>,
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

/*
#[inline]
fn get_batch_reader<'a>(handle: jlong) -> Result<&'a mut ParquetRecordBatchReader, CometError> {
    Ok(&mut get_batch_context(handle)?.batch_reader.unwrap())
}
*/

/// # Safety
/// This function is inherently unsafe since it deals with raw pointers passed from JNI.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_comet_parquet_Native_initRecordBatchReader(
    e: JNIEnv,
    _jclass: JClass,
    file_path: jstring,
    file_size: jlong,
    start: jlong,
    length: jlong,
    required_schema: jbyteArray,
) -> jlong {
    try_unwrap_or_throw(&e, |mut env| unsafe {
        let path: String = env
            .get_string(&JString::from_raw(file_path))
            .unwrap()
            .into();
        let batch_stream: Option<SendableRecordBatchStream>;
        let batch_reader: Option<ParquetRecordBatchReader> = None;
        // TODO: (ARROW NATIVE) Use the common global runtime
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;

        // EXPERIMENTAL - BEGIN
        //TODO: Need an execution context and a spark plan equivalent so that we can reuse
        // code from jni_api.rs
        let (object_store_url, object_store_path) = get_file_path(path.clone()).unwrap();
        // TODO: (ARROW NATIVE) - Remove code duplication between this and POC 1
        // copy the input on-heap buffer to native
        let required_schema_array = JByteArray::from_raw(required_schema);
        let required_schema_buffer = env.convert_byte_array(&required_schema_array)?;
        let required_schema_arrow = deserialize_schema(required_schema_buffer.as_bytes())?;
        let mut partitioned_file = PartitionedFile::new_with_range(
            String::new(), // Dummy file path. We will override this with our path so that url encoding does not occur
            file_size as u64,
            start,
            start + length,
        );
        partitioned_file.object_meta.location = object_store_path;
        // We build the file scan config with the *required* schema so that the reader knows
        // the output schema we want
        let file_scan_config = FileScanConfig::new(object_store_url, Arc::new(required_schema_arrow))
                .with_file(partitioned_file)
                // TODO: (ARROW NATIVE) - do partition columns in native
                //   - will need partition schema and partition values to do so
                // .with_table_partition_cols(partition_fields)
                ;
        let mut table_parquet_options = TableParquetOptions::new();
        // TODO: Maybe these are configs?
        table_parquet_options.global.pushdown_filters = true;
        table_parquet_options.global.reorder_filters = true;

        let mut spark_parquet_options = SparkParquetOptions::new(EvalMode::Legacy, "UTC", false);
        spark_parquet_options.allow_cast_unsigned_ints = true;

        let builder2 = ParquetExecBuilder::new(file_scan_config)
            .with_table_parquet_options(table_parquet_options)
            .with_schema_adapter_factory(Arc::new(SparkSchemaAdapterFactory::new(
                spark_parquet_options,
            )));

        //TODO: (ARROW NATIVE) - predicate pushdown??
        // builder = builder.with_predicate(filter);

        let scan = builder2.build();
        let ctx = TaskContext::default();
        let partition_index: usize = 0;
        batch_stream = Some(scan.execute(partition_index, Arc::new(ctx))?);

        // EXPERIMENTAL - END

        let ctx = BatchContext {
            runtime,
            batch_stream,
            batch_reader,
            current_batch: None,
            reader_state: ParquetReaderState::Init,
        };
        let res = Box::new(ctx);
        Ok(Box::into_raw(res) as i64)
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_comet_parquet_Native_readNextRecordBatch(
    e: JNIEnv,
    _jclass: JClass,
    handle: jlong,
) -> jint {
    try_unwrap_or_throw(&e, |_env| {
        let context = get_batch_context(handle)?;
        let mut rows_read: i32 = 0;
        let batch_stream = context.batch_stream.as_mut().unwrap();
        let runtime = &context.runtime;

        // let mut stream = batch_stream.as_mut();
        loop {
            let next_item = batch_stream.next();
            let poll_batch: Poll<Option<datafusion_common::Result<RecordBatch>>> =
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

                    // TODO: (ARROW NATIVE) We can update metrics here
                    // crate::execution::jni_api::update_metrics(&mut env, exec_context)?;

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
    e: JNIEnv,
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
    env: JNIEnv,
    _jclass: JClass,
    handle: jlong,
) {
    try_unwrap_or_throw(&env, |_| {
        unsafe {
            let ctx = handle as *mut BatchContext;
            let _ = Box::from_raw(ctx);
        };
        Ok(())
    })
}
