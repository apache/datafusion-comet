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

use arrow::array::builder::{Float32Builder, Float64Builder};
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::physical_plan::ColumnarValue;
use datafusion_comet_spark_expr::{spark_cast, EvalMode, SparkCastOptions};
use std::hint::black_box;
use std::sync::Arc;

/// Mix of values that exercise both the plain and the scientific notation paths.
fn create_f64_array(size: usize) -> ArrayRef {
    let mut builder = Float64Builder::with_capacity(size);
    for i in 0..size {
        match i % 10 {
            0 => builder.append_null(),
            1 => builder.append_value(0.0),
            2 => builder.append_value(i as f64),
            3 => builder.append_value(i as f64 + 0.125),
            4 => builder.append_value(-(i as f64) * 1.5),
            5 => builder.append_value(1e20 * i as f64),
            6 => builder.append_value(1e-12 * i as f64),
            7 => builder.append_value(f64::NAN),
            8 => builder.append_value(1234.5678),
            _ => builder.append_value(-0.001234),
        }
    }
    Arc::new(builder.finish())
}

fn create_f32_array(size: usize) -> ArrayRef {
    let mut builder = Float32Builder::with_capacity(size);
    for i in 0..size {
        match i % 10 {
            0 => builder.append_null(),
            1 => builder.append_value(0.0),
            2 => builder.append_value(i as f32),
            3 => builder.append_value(i as f32 + 0.125),
            4 => builder.append_value(-(i as f32) * 1.5),
            5 => builder.append_value(1e20 * i as f32),
            6 => builder.append_value(1e-12 * i as f32),
            7 => builder.append_value(f32::NAN),
            8 => builder.append_value(1234.5678),
            _ => builder.append_value(-0.001234),
        }
    }
    Arc::new(builder.finish())
}

fn criterion_benchmark(c: &mut Criterion) {
    let size = 8192;
    let f64_array = create_f64_array(size);
    let f32_array = create_f32_array(size);
    let cast_options = SparkCastOptions::new_without_timezone(EvalMode::Legacy, false);

    let mut group = c.benchmark_group("cast_float_to_string");
    group.bench_function("cast_f64_to_utf8", |b| {
        b.iter(|| {
            black_box(
                spark_cast(
                    ColumnarValue::Array(Arc::clone(&f64_array)),
                    &DataType::Utf8,
                    &cast_options,
                )
                .unwrap(),
            )
        })
    });
    group.bench_function("cast_f32_to_utf8", |b| {
        b.iter(|| {
            black_box(
                spark_cast(
                    ColumnarValue::Array(Arc::clone(&f32_array)),
                    &DataType::Utf8,
                    &cast_options,
                )
                .unwrap(),
            )
        })
    });
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
