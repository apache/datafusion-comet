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

//! Helpers shared by the cast-from-string benchmarks, pulled in with
//! `#[path = "common/mod.rs"] mod common;`. This lives in a subdirectory so that Cargo's bench
//! auto-discovery, which only looks at `benches/*.rs`, does not treat it as a bench target.
#![allow(dead_code)]

use arrow::array::{
    builder::{BooleanBuilder, ListBuilder, StringBuilder},
    ArrayRef, Float64Array, Int64Array, ListArray, RecordBatch,
};
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Date32Type, Decimal128Type, Field, Float32Type, Float64Type,
    Int16Type, Int32Type, Int64Type, Int8Type, Schema,
};
use datafusion::common::ScalarValue;
use std::sync::Arc;

pub const ROW_COUNTS: [usize; 3] = [8_192, 65_536, 524_288];

pub const NULL_RATIOS: [(f64, &str); 3] = [(0.0, "no_nulls"), (0.1, "sparse"), (1.0, "all_null")];

pub fn is_null(i: usize, null_ratio: f64) -> bool {
    if null_ratio <= 0.0 {
        false
    } else if null_ratio >= 1.0 {
        true
    } else {
        let stride = (1.0 / null_ratio).round() as usize;
        stride != 0 && i.is_multiple_of(stride)
    }
}

pub fn f64_array(rows: usize, null_ratio: f64, value: impl Fn(usize) -> f64) -> ArrayRef {
    let arr: Float64Array = (0..rows)
        .map(|i| {
            if is_null(i, null_ratio) {
                None
            } else {
                Some(value(i))
            }
        })
        .collect();
    Arc::new(arr)
}

pub fn i64_array(rows: usize, null_ratio: f64, value: impl Fn(usize) -> i64) -> ArrayRef {
    let arr: Int64Array = (0..rows)
        .map(|i| {
            if is_null(i, null_ratio) {
                None
            } else {
                Some(value(i))
            }
        })
        .collect();
    Arc::new(arr)
}

/// A single-column `Utf8` batch of `rows` rows, where row `i` holds `value(i)` unless
/// `i % null_modulus == 0`, in which case it is null.
pub fn string_batch(
    rows: usize,
    null_modulus: usize,
    mut value: impl FnMut(usize) -> String,
) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)]));
    let mut builder = StringBuilder::new();
    for i in 0..rows {
        if i % null_modulus == 0 {
            builder.append_null();
        } else {
            builder.append_value(value(i));
        }
    }
    RecordBatch::try_new(schema, vec![Arc::new(builder.finish())]).unwrap()
}

/// A `List<T>` array of `rows` rows, each `elems` values from `value`, rows nulled per `null_ratio`.
pub fn primitive_list_array<T: ArrowPrimitiveType>(
    rows: usize,
    null_ratio: f64,
    elems: usize,
    value: impl Fn(usize) -> T::Native,
) -> ArrayRef {
    Arc::new(ListArray::from_iter_primitive::<T, _, _>((0..rows).map(
        |i| {
            if is_null(i, null_ratio) {
                None
            } else {
                Some(
                    (0..elems)
                        .map(|j| Some(value(i * elems + j)))
                        .collect::<Vec<_>>(),
                )
            }
        },
    )))
}

/// A `List<Utf8>` array of `rows` rows, each `elems` values from `value`, rows nulled per `null_ratio`.
pub fn utf8_list_array(
    rows: usize,
    null_ratio: f64,
    elems: usize,
    value: impl Fn(usize) -> String,
) -> ArrayRef {
    let mut b = ListBuilder::new(StringBuilder::new());
    for i in 0..rows {
        if is_null(i, null_ratio) {
            b.append(false);
        } else {
            (0..elems).for_each(|j| b.values().append_value(value(i * elems + j)));
            b.append(true);
        }
    }
    Arc::new(b.finish())
}

/// A `List<Boolean>` array of `rows` rows, each `elems` values, rows nulled per `null_ratio`.
pub fn bool_list_array(rows: usize, null_ratio: f64, elems: usize) -> ArrayRef {
    let mut b = ListBuilder::new(BooleanBuilder::new());
    for i in 0..rows {
        if is_null(i, null_ratio) {
            b.append(false);
        } else {
            (0..elems).for_each(|j| b.values().append_value((i + j) % 2 == 0));
            b.append(true);
        }
    }
    Arc::new(b.finish())
}

/// One `List<T>` per element type the list kernels support, tagged, paired with a sample element of
/// that type (used as a search/insert value by kernels that need one; ignored by the rest).
pub fn list_arrays(
    rows: usize,
    null_ratio: f64,
    elems: usize,
) -> Vec<(&'static str, ArrayRef, ScalarValue)> {
    vec![
        (
            "int8",
            primitive_list_array::<Int8Type>(rows, null_ratio, elems, |i| (i % 100) as i8),
            ScalarValue::Int8(Some(50)),
        ),
        (
            "int16",
            primitive_list_array::<Int16Type>(rows, null_ratio, elems, |i| (i % 1000) as i16),
            ScalarValue::Int16(Some(500)),
        ),
        (
            "int32",
            primitive_list_array::<Int32Type>(rows, null_ratio, elems, |i| (i % 1000) as i32),
            ScalarValue::Int32(Some(500)),
        ),
        (
            "int64",
            primitive_list_array::<Int64Type>(rows, null_ratio, elems, |i| (i % 1000) as i64),
            ScalarValue::Int64(Some(500)),
        ),
        (
            "float32",
            primitive_list_array::<Float32Type>(rows, null_ratio, elems, |i| (i % 1000) as f32),
            ScalarValue::Float32(Some(500.0)),
        ),
        (
            "float64",
            primitive_list_array::<Float64Type>(rows, null_ratio, elems, |i| (i % 1000) as f64),
            ScalarValue::Float64(Some(500.0)),
        ),
        (
            "date32",
            primitive_list_array::<Date32Type>(rows, null_ratio, elems, |i| (i % 1000) as i32),
            ScalarValue::Date32(Some(500)),
        ),
        (
            "decimal128",
            primitive_list_array::<Decimal128Type>(rows, null_ratio, elems, |i| (i % 1000) as i128),
            ScalarValue::Decimal128(Some(500), 38, 10),
        ),
        (
            "bool",
            bool_list_array(rows, null_ratio, elems),
            ScalarValue::Boolean(Some(true)),
        ),
        (
            "utf8",
            utf8_list_array(rows, null_ratio, elems, |i| format!("k{}", i % 1000)),
            ScalarValue::Utf8(Some("k500".to_string())),
        ),
    ]
}
