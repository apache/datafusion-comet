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

use arrow::array::{ArrayRef, ListArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::common::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_comet_spark_expr::SparkFlatten;
use std::hint::black_box;
use std::sync::Arc;

#[path = "common/mod.rs"]
mod common;
use common::{is_null, list_arrays, NULL_RATIOS, ROW_COUNTS};

const INNER_PER_ROW: usize = 4;

// Wrap `rows * INNER_PER_ROW` inner lists into `rows` outer rows, so the result is List<List<T>>.
fn nest(inner: ArrayRef, rows: usize, null_ratio: f64) -> ArrayRef {
    let field = Arc::new(Field::new_list_field(inner.data_type().clone(), true));
    let offsets = OffsetBuffer::<i32>::from_lengths((0..rows).map(|_| INNER_PER_ROW));
    let nulls = NullBuffer::from(
        (0..rows)
            .map(|i| !is_null(i, null_ratio))
            .collect::<Vec<bool>>(),
    );
    Arc::new(ListArray::new(field, offsets, inner, Some(nulls)))
}

fn criterion_benchmark(c: &mut Criterion) {
    let udf = SparkFlatten::new();
    let mut group = c.benchmark_group("flatten");
    for rows in ROW_COUNTS {
        for (null_ratio, tag) in NULL_RATIOS {
            for (ty, inner, _) in list_arrays(rows * INNER_PER_ROW, null_ratio, 4) {
                let nested = nest(inner, rows, null_ratio);
                let args = vec![ColumnarValue::Array(nested)];
                group.bench_with_input(
                    BenchmarkId::from_parameter(format!("{ty}/{rows}/{tag}")),
                    &args,
                    |b, args| {
                        b.iter(|| {
                            black_box(
                                udf.invoke_with_args(ScalarFunctionArgs {
                                    args: args.clone(),
                                    arg_fields: vec![],
                                    number_rows: rows,
                                    return_field: Arc::new(Field::new(
                                        "result",
                                        DataType::Null,
                                        true,
                                    )),
                                    config_options: Arc::new(ConfigOptions::default()),
                                })
                                .unwrap(),
                            )
                        })
                    },
                );
            }
        }
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
