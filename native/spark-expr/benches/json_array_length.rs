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

//! Benchmark for the Comet-owned `json_array_length` kernel
//! (`native/spark-expr/src/json_funcs/json_array_length.rs`), a `ScalarUDFImpl`
//! that parses each JSON string and returns the top-level array length. Varied
//! array sizes and some non-array / malformed inputs exercise the parse path.

use arrow::datatypes::{DataType, Field};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::common::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_comet_spark_expr::JsonArrayLength;
use std::hint::black_box;
use std::sync::Arc;

#[path = "common/mod.rs"]
mod common;
use common::{string_array, NULL_RATIOS, ROW_COUNTS};

fn criterion_benchmark(c: &mut Criterion) {
    let udf = JsonArrayLength::new();
    let mut group = c.benchmark_group("json_array_length");
    for rows in ROW_COUNTS {
        for (null_ratio, tag) in NULL_RATIOS {
            // Mix of JSON arrays of varying length and a non-array row, so both
            // the happy path and the "not an array -> null" path are exercised.
            let arr = string_array(rows, null_ratio, |i| match i % 4 {
                0 => "[]".to_string(),
                1 => format!("[{}]", i),
                2 => format!("[{},{},{},{}]", i, i + 1, i + 2, i + 3),
                _ => format!("{{\"k\":{}}}", i),
            });
            let args = vec![ColumnarValue::Array(arr)];
            group.bench_with_input(
                BenchmarkId::from_parameter(format!("{rows}/{tag}")),
                &args,
                |b, args| {
                    b.iter(|| {
                        black_box(
                            udf.invoke_with_args(ScalarFunctionArgs {
                                args: args.clone(),
                                arg_fields: vec![],
                                number_rows: rows,
                                return_field: Arc::new(Field::new("result", DataType::Int32, true)),
                                config_options: Arc::new(ConfigOptions::default()),
                            })
                            .unwrap(),
                        )
                    })
                },
            );
        }
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
