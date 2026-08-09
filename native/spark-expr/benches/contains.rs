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

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::common::ScalarValue;
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_comet_spark_expr::SparkContains;

use std::sync::Arc;

fn generate_string_array(size: usize) -> ArrayRef {
    let data: Vec<Option<String>> = (0..size)
        .map(|i| {
            if i % 10 == 0 {
                None
            } else {
                Some(format!(
                    "hello string data sample number {} with some text",
                    i
                ))
            }
        })
        .collect();
    Arc::new(StringArray::from(data))
}

fn bench_contains(c: &mut Criterion) {
    let rows = 8192;
    let udf = SparkContains::new();

    let mut group = c.benchmark_group("string_funcs/contains");

    let haystack_array = generate_string_array(rows);
    let needle_scalar = ColumnarValue::Scalar(ScalarValue::Utf8(Some("sample".to_string())));
    let needle_array = generate_string_array(rows);

    // Общие метаданные для ScalarFunctionArgs
    let arg_fields = vec![
        Arc::new(Field::new("haystack", DataType::Utf8, true)),
        Arc::new(Field::new("needle", DataType::Utf8, true)),
    ];
    let return_field = Arc::new(Field::new("result", DataType::Boolean, true));
    let config_options = Arc::new(ConfigOptions::new());

    // 1. Array haystack vs Scalar needle (optimized path)
    group.bench_function(&format!("array_vs_scalar_size_{}", rows), |b| {
        b.iter(|| {
            let args = ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Array(haystack_array.clone()),
                    needle_scalar.clone(),
                ],
                arg_fields: arg_fields.clone(),
                number_rows: rows,
                return_field: return_field.clone(),
                config_options: config_options.clone(),
            };
            std::hint::black_box(udf.invoke_with_args(args).unwrap());
        });
    });

    // 2. Array haystack vs Array needle
    group.bench_function(&format!("array_vs_array_size_{}", rows), |b| {
        b.iter(|| {
            let args = ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Array(haystack_array.clone()),
                    ColumnarValue::Array(needle_array.clone()),
                ],
                arg_fields: arg_fields.clone(),
                number_rows: rows,
                return_field: return_field.clone(),
                config_options: config_options.clone(),
            };
            std::hint::black_box(udf.invoke_with_args(args).unwrap());
        });
    });

    let haystack_scalar_val = ColumnarValue::Scalar(ScalarValue::Utf8(Some("sample".to_string())));
    group.bench_function(&format!("scalar_vs_array_size_{}", rows), |b| {
        b.iter(|| {
            let args = ScalarFunctionArgs {
                args: vec![
                    haystack_scalar_val.clone(),
                    ColumnarValue::Array(needle_array.clone()),
                ],
                arg_fields: arg_fields.clone(),
                number_rows: rows,
                return_field: return_field.clone(),
                config_options: config_options.clone(),
            };
            std::hint::black_box(udf.invoke_with_args(args).unwrap());
        });
    });

    group.finish();
}

criterion_group!(benches, bench_contains);
criterion_main!(benches);
