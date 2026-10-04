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

use arrow::array::builder::{ListBuilder, StringBuilder};
use arrow::array::ArrayRef;
use arrow::datatypes::Field;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::common::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_comet_spark_expr::SparkMapFromArrays;
use std::hint::black_box;
use std::sync::Arc;

const BATCH_SIZE: usize = 8192;

/// A `List<Utf8>` column of `BATCH_SIZE` rows, each holding `entries` strings. Values are made
/// distinct per row so the map constructor never hits a duplicate key.
fn string_list(entries: usize, prefix: &str) -> ArrayRef {
    let mut builder = ListBuilder::new(StringBuilder::new());
    for row in 0..BATCH_SIZE {
        for e in 0..entries {
            builder.values().append_value(format!("{prefix}{row}_{e}"));
        }
        builder.append(true);
    }
    Arc::new(builder.finish())
}

fn criterion_benchmark(c: &mut Criterion) {
    let udf = SparkMapFromArrays::default();
    let mut group = c.benchmark_group("map_from_arrays");
    for entries in [2usize, 8, 32] {
        let keys = string_list(entries, "k");
        let values = string_list(entries, "v");
        let return_type = udf
            .return_type(&[keys.data_type().clone(), values.data_type().clone()])
            .unwrap();
        let args = vec![
            ColumnarValue::Array(Arc::clone(&keys)),
            ColumnarValue::Array(Arc::clone(&values)),
        ];
        group.bench_with_input(BenchmarkId::from_parameter(entries), &args, |b, args| {
            b.iter(|| {
                black_box(
                    udf.invoke_with_args(ScalarFunctionArgs {
                        args: args.to_vec(),
                        arg_fields: vec![],
                        number_rows: BATCH_SIZE,
                        return_field: Arc::new(Field::new("result", return_type.clone(), true)),
                        config_options: Arc::new(ConfigOptions::default()),
                    })
                    .unwrap(),
                )
            })
        });
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
