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

use arrow::array::{builder::StringBuilder, ArrayRef, Float64Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

pub const ROW_COUNTS: [usize; 3] = [8_192, 65_536, 524_288];

pub const NULL_RATIOS: [(f64, &str); 3] = [(0.0, "no_nulls"), (0.1, "sparse"), (1.0, "all_null")];

fn is_null(i: usize, null_ratio: f64) -> bool {
    if null_ratio <= 0.0 {
        false
    } else if null_ratio >= 1.0 {
        true
    } else {
        let stride = (1.0 / null_ratio).round() as usize;
        stride != 0 && i % stride == 0
    }
}

pub fn f64_array(rows: usize, null_ratio: f64, value: impl Fn(usize) -> f64) -> ArrayRef {
    let arr: Float64Array = (0..rows)
        .map(|i| if is_null(i, null_ratio) { None } else { Some(value(i)) })
        .collect();
    Arc::new(arr)
}

pub fn i64_array(rows: usize, null_ratio: f64, value: impl Fn(usize) -> i64) -> ArrayRef {
    let arr: Int64Array = (0..rows)
        .map(|i| if is_null(i, null_ratio) { None } else { Some(value(i)) })
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
