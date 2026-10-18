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

//! Compare ordinary-data extrema with DataFusion; special-value semantics belong in tests.

use arrow::array::{ArrayRef, Float32Array, Float64Array, ListArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::Field;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::common::config::ConfigOptions;
use datafusion::functions_nested::min_max::{array_max_udf, array_min_udf};
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDF};
use datafusion_comet_spark_expr::SparkArrayExtrema;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

fn list(values: ArrayRef, len: usize, nullable: bool) -> ArrayRef {
    let rows = values.len() / len;
    Arc::new(ListArray::new(
        Arc::new(Field::new_list_field(values.data_type().clone(), true)),
        OffsetBuffer::from_lengths(std::iter::repeat_n(len, rows)),
        values,
        nullable.then(|| NullBuffer::from_iter((0..rows).map(|i| i % 10 != 0))),
    ))
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_extrema");
    group.sample_size(20);
    group.warm_up_time(Duration::from_millis(250));
    group.measurement_time(Duration::from_secs(1));
    for len in [8, 1024] {
        for nullable in [false, true] {
            let values = (0..64 * len)
                .map(|i| (!nullable || i % 10 != 0).then_some(((i * 17) % 1000 + 1) as f64));
            let inputs = [
                (
                    "float32",
                    list(
                        Arc::new(Float32Array::from_iter(
                            values.clone().map(|v| v.map(|v| v as f32)),
                        )),
                        len,
                        nullable,
                    ),
                ),
                (
                    "float64",
                    list(Arc::new(Float64Array::from_iter(values)), len, nullable),
                ),
                (
                    "nested",
                    // Null list elements are skipped by both engines. Keep inner floats non-null
                    // so the fixture does not depend on their different nested null ordering.
                    list(
                        list(
                            Arc::new(Float64Array::from_iter_values(
                                (0..64 * len * 4).map(|i| ((i * 17) % 1000 + 1) as f64),
                            )),
                            4,
                            nullable,
                        ),
                        len,
                        nullable,
                    ),
                ),
            ];
            for (kind, input) in inputs {
                for is_min in [true, false] {
                    let comet = ScalarUDF::from(SparkArrayExtrema::new(is_min));
                    let datafusion = if is_min {
                        array_min_udf()
                    } else {
                        array_max_udf()
                    };
                    let args = ScalarFunctionArgs {
                        args: vec![ColumnarValue::Array(Arc::clone(&input))],
                        arg_fields: vec![Arc::new(Field::new(
                            "input",
                            input.data_type().clone(),
                            true,
                        ))],
                        number_rows: input.len(),
                        return_field: Arc::new(Field::new(
                            "result",
                            comet.return_type(&[input.data_type().clone()]).unwrap(),
                            true,
                        )),
                        config_options: Arc::new(ConfigOptions::default()),
                    };
                    let evaluate = |udf: &ScalarUDF| {
                        udf.invoke_with_args(args.clone())
                            .unwrap()
                            .into_array(input.len())
                            .unwrap()
                    };
                    assert_eq!(evaluate(&comet).to_data(), evaluate(&datafusion).to_data());
                    let op = if is_min { "min" } else { "max" };
                    for (engine, udf) in [("comet", &comet), ("datafusion", datafusion.as_ref())] {
                        group.bench_function(
                            BenchmarkId::new(
                                format!("{kind}_{op}_{engine}"),
                                format!("len={len}_null={nullable}"),
                            ),
                            |b| {
                                b.iter(|| {
                                    black_box(
                                        udf.invoke_with_args(black_box(args.clone())).unwrap(),
                                    )
                                })
                            },
                        );
                    }
                }
            }
        }
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
