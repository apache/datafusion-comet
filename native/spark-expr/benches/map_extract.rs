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

//! Benchmarks for the map lookup behind `GetMapValue` and `element_at(<map>, key)`.
//!
//! Each shape is run against both Comet's `SparkMapExtract` and the
//! `datafusion-functions-nested` `map_extract` it overrides, so the gap that motivated
//! <https://github.com/apache/datafusion-comet/issues/5795> stays visible.

use arrow::array::builder::{MapBuilder, StringBuilder};
use arrow::array::{ArrayRef, MapFieldNames, StringArray};
use arrow::datatypes::{DataType, Field};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::common::config::ConfigOptions;
use datafusion::common::ScalarValue;
use datafusion::functions_nested::map_extract::map_extract_udf;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_comet_spark_expr::SparkMapExtract;
use std::hint::black_box;
use std::sync::Arc;

const BATCH_SIZE: usize = 8192;
/// Distinct keys per map column, as in the issue's `attrs map<string, string>` dataset.
const DISTINCT_KEYS: usize = 60;

/// `BATCH_SIZE` rows of `map<string, string>`, every tenth row NULL, each non-null row holding
/// `entries_per_map` entries drawn from `DISTINCT_KEYS` keys. The stride is coprime with
/// `DISTINCT_KEYS` so a given lookup key lands at a different entry position in every row rather
/// than always being found (or missed) at the same depth.
fn string_map(entries_per_map: usize) -> ArrayRef {
    let mut builder = MapBuilder::new(
        Some(MapFieldNames {
            entry: "entries".into(),
            key: "key".into(),
            value: "value".into(),
        }),
        StringBuilder::new(),
        StringBuilder::new(),
    );
    for row in 0..BATCH_SIZE {
        if row % 10 == 0 {
            builder.append(false).unwrap();
            continue;
        }
        for entry in 0..entries_per_map {
            builder
                .keys()
                .append_value(format!("a{}", (row * 13 + entry * 7) % DISTINCT_KEYS));
            builder.values().append_value(format!("v{}", row % 400));
        }
        builder.append(true).unwrap();
    }
    Arc::new(builder.finish())
}

/// One lookup key per row, so the key cannot be hoisted out of the comparison.
fn per_row_keys() -> ArrayRef {
    Arc::new(StringArray::from_iter_values(
        (0..BATCH_SIZE).map(|row| format!("a{}", (row * 29 + 11) % DISTINCT_KEYS)),
    ))
}

fn call(udf: &dyn ScalarUDFImpl, args: &[ColumnarValue]) {
    black_box(
        udf.invoke_with_args(ScalarFunctionArgs {
            args: args.to_vec(),
            arg_fields: vec![],
            number_rows: BATCH_SIZE,
            return_field: Arc::new(Field::new("result", DataType::Utf8, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .unwrap(),
    );
}

fn criterion_benchmark(c: &mut Criterion) {
    let comet = SparkMapExtract::new();
    let datafusion = map_extract_udf();
    let mut group = c.benchmark_group("map_extract");

    for entries in [2usize, 8, 32] {
        let map = string_map(entries);
        let cases: [(&str, Vec<ColumnarValue>); 2] = [
            (
                "constant_key",
                vec![
                    ColumnarValue::Array(Arc::clone(&map)),
                    ColumnarValue::Scalar(ScalarValue::Utf8(Some("a1".to_string()))),
                ],
            ),
            (
                "per_row_key",
                vec![
                    ColumnarValue::Array(Arc::clone(&map)),
                    ColumnarValue::Array(per_row_keys()),
                ],
            ),
        ];
        for (case, args) in cases {
            group.bench_with_input(
                BenchmarkId::new(format!("comet/{case}"), entries),
                &args,
                |b, args| b.iter(|| call(&comet, args)),
            );
            group.bench_with_input(
                BenchmarkId::new(format!("datafusion/{case}"), entries),
                &args,
                |b, args| b.iter(|| call(datafusion.inner().as_ref(), args)),
            );
        }
    }

    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
