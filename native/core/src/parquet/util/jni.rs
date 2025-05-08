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

use std::sync::Arc;

use jni::{
    errors::Result as JNIResult,
    objects::{JObjectArray, JString},
    sys::{jboolean, jint, jobjectArray},
    JNIEnv,
};

use arrow::error::ArrowError;
use arrow::ipc::reader::StreamReader;
use datafusion::execution::object_store::ObjectStoreUrl;
use object_store::path::Path;
use parquet::{
    basic::{Encoding, LogicalType, TimeUnit, Type as PhysicalType},
    format::{MicroSeconds, MilliSeconds, NanoSeconds},
    schema::types::{ColumnDescriptor, ColumnPath, PrimitiveTypeBuilder},
};
use url::{ParseError, Url};

/// Convert primitives from Spark side into a `ColumnDescriptor`.
#[allow(clippy::too_many_arguments)]
pub fn convert_column_descriptor(
    env: &mut JNIEnv,
    physical_type_id: jint,
    logical_type_id: jint,
    max_dl: jint,
    max_rl: jint,
    bit_width: jint,
    is_signed: jboolean,
    type_length: jint,
    precision: jint,
    scale: jint,
    time_unit: jint,
    is_adjusted_utc: jboolean,
    jni_path: jobjectArray,
) -> JNIResult<ColumnDescriptor> {
    let physical_type = convert_physical_type(physical_type_id);
    let type_length = fix_type_length(&physical_type, type_length);
    let logical_type = if logical_type_id >= 0 {
        Some(convert_logical_type(
            logical_type_id,
            bit_width,
            is_signed,
            precision,
            scale,
            time_unit,
            is_adjusted_utc,
        ))
    } else {
        // id < 0 means there is no logical type associated
        None
    };

    // We don't care the column name here
    let ty = PrimitiveTypeBuilder::new("f", physical_type)
        .with_logical_type(logical_type)
        .with_length(type_length)
        .with_precision(precision) // Parquet crate requires to set this even with logical type
        .with_scale(scale)
        .build()
        .unwrap(); // TODO: convert Parquet errot to JNI error
    let path = convert_column_path(env, jni_path).unwrap();

    let result = ColumnDescriptor::new(Arc::new(ty), max_dl as i16, max_rl as i16, path);
    Ok(result)
}

pub fn convert_encoding(ordinal: jint) -> Encoding {
    match ordinal {
        0 => Encoding::PLAIN,
        1 => Encoding::RLE,
        #[allow(deprecated)]
        3 => Encoding::BIT_PACKED,
        4 => Encoding::PLAIN_DICTIONARY,
        5 => Encoding::DELTA_BINARY_PACKED,
        6 => Encoding::DELTA_LENGTH_BYTE_ARRAY,
        7 => Encoding::DELTA_BYTE_ARRAY,
        8 => Encoding::RLE_DICTIONARY,
        _ => panic!("Invalid Java Encoding ordinal: {}", ordinal),
    }
}

#[derive(Debug)]
pub struct TypePromotionInfo {
    pub(crate) physical_type: PhysicalType,
    pub(crate) precision: i32,
    pub(crate) scale: i32,
    pub(crate) bit_width: i32,
}

impl TypePromotionInfo {
    pub fn new_from_jni(
        physical_type_id: jint,
        precision: jint,
        scale: jint,
        bit_width: jint,
    ) -> Self {
        let physical_type = convert_physical_type(physical_type_id);
        Self {
            physical_type,
            precision,
            scale,
            bit_width,
        }
    }

    pub fn new(physical_type: PhysicalType, precision: i32, scale: i32, bit_width: i32) -> Self {
        Self {
            physical_type,
            precision,
            scale,
            bit_width,
        }
    }
}

fn convert_column_path(env: &mut JNIEnv, path: jobjectArray) -> JNIResult<ColumnPath> {
    let path_array = unsafe { JObjectArray::from_raw(path) };
    let array_len = env.get_array_length(&path_array)?;
    let mut res: Vec<String> = Vec::new();
    for i in 0..array_len {
        let p: JString = env.get_object_array_element(&path_array, i)?.into();
        res.push(env.get_string(&p)?.into());
    }
    Ok(ColumnPath::new(res))
}

fn convert_physical_type(id: jint) -> PhysicalType {
    match id {
        0 => PhysicalType::BOOLEAN,
        1 => PhysicalType::INT32,
        2 => PhysicalType::INT64,
        3 => PhysicalType::INT96,
        4 => PhysicalType::FLOAT,
        5 => PhysicalType::DOUBLE,
        6 => PhysicalType::BYTE_ARRAY,
        7 => PhysicalType::FIXED_LEN_BYTE_ARRAY,
        _ => panic!("Invalid id for Parquet physical type: {} ", id),
    }
}

fn convert_logical_type(
    id: jint,
    bit_width: jint,
    is_signed: jboolean,
    precision: jint,
    scale: jint,
    time_unit: jint,
    is_adjusted_utc: jboolean,
) -> LogicalType {
    match id {
        0 => LogicalType::Integer {
            bit_width: bit_width as i8,
            is_signed: is_signed != 0,
        },
        1 => LogicalType::String,
        2 => LogicalType::Decimal { scale, precision },
        3 => LogicalType::Date,
        4 => LogicalType::Timestamp {
            is_adjusted_to_u_t_c: is_adjusted_utc != 0,
            unit: convert_time_unit(time_unit),
        },
        5 => LogicalType::Enum,
        6 => LogicalType::Uuid,
        _ => panic!("Invalid id for Parquet logical type: {}", id),
    }
}

fn convert_time_unit(time_unit: jint) -> TimeUnit {
    match time_unit {
        0 => TimeUnit::MILLIS(MilliSeconds::new()),
        1 => TimeUnit::MICROS(MicroSeconds::new()),
        2 => TimeUnit::NANOS(NanoSeconds::new()),
        _ => panic!("Invalid time unit id for Parquet: {}", time_unit),
    }
}

/// Fixes the type length in case they are not set (Parquet only explicitly set it for
/// FIXED_LEN_BYTE_ARRAY type).
fn fix_type_length(t: &PhysicalType, type_length: i32) -> i32 {
    match t {
        PhysicalType::INT32 | PhysicalType::FLOAT => 4,
        PhysicalType::INT64 | PhysicalType::DOUBLE => 8,
        PhysicalType::INT96 => 12,
        _ => type_length,
    }
}

pub fn deserialize_schema(ipc_bytes: &[u8]) -> Result<arrow::datatypes::Schema, ArrowError> {
    let reader = unsafe {
        StreamReader::try_new(std::io::Cursor::new(ipc_bytes), None)?.with_skip_validation(true)
    };
    let schema = reader.schema().as_ref().clone();
    Ok(schema)
}

// parses the url and returns a tuple of the scheme and object store path
pub fn get_file_path(url_: String) -> Result<(ObjectStoreUrl, Path), ParseError> {
    // we define origin of a url as scheme + "://" + authority + ["/" + bucket]
    let url = Url::parse(url_.as_ref()).unwrap();
    let mut object_store_origin = url.scheme().to_owned();
    let mut object_store_path = Path::from_url_path(url.path()).unwrap();
    if object_store_origin == "s3a" {
        object_store_origin = "s3".to_string();
        object_store_origin.push_str("://");
        object_store_origin.push_str(url.authority());
        object_store_origin.push('/');
        let path_splits = url.path_segments().map(|c| c.collect::<Vec<_>>()).unwrap();
        object_store_origin.push_str(path_splits.first().unwrap());
        let new_path = path_splits[1..path_splits.len() - 1].join("/");
        //TODO: (ARROW NATIVE) check the use of unwrap here
        object_store_path = Path::from_url_path(new_path.clone().as_str()).unwrap();
    } else {
        object_store_origin.push_str("://");
        object_store_origin.push_str(url.authority());
        object_store_origin.push('/');
    }
    Ok((
        ObjectStoreUrl::parse(object_store_origin).unwrap(),
        object_store_path,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_file_path() {
        let inp = "file:///comet/spark-warehouse/t1/part1=2019-01-01%2011%253A11%253A11/part-00000-84d7ed74-8f28-456c-9270-f45376eea144.c000.snappy.parquet";
        let expected = "comet/spark-warehouse/t1/part1=2019-01-01 11%3A11%3A11/part-00000-84d7ed74-8f28-456c-9270-f45376eea144.c000.snappy.parquet";

        if let Ok((_obj_store_url, path)) = get_file_path(inp.to_string()) {
            let actual = path.to_string();
            assert_eq!(actual, expected);
        }
    }
}
