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
//!
//! Read the ratio as a measurement of the pinned DataFusion 55.1.0, not as a permanent gap.
//! DataFusion main has since rewritten `general_map_extract_inner` around a single
//! `make_comparator` over the batch, so the per-comparison `ArrayRef` slicing that dominates the
//! baseline here is specific to the version Comet ships today, and the baseline arm will get much
//! faster at the next DataFusion bump. What survives that bump is the rest of the case for this
//! kernel: one `eq` plus one gather, and the `ListExtract` unwrapping pass this removes.

use arrow::array::builder::{MapBuilder, StringBuilder};
use arrow::array::{
    Array, ArrayRef, Int32Array, LargeListArray, ListArray, MapArray, MapFieldNames, StringArray,
    StructArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
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

fn call(udf: &dyn ScalarUDFImpl, args: &[ColumnarValue], return_field: &Arc<Field>) {
    black_box(
        udf.invoke_with_args(ScalarFunctionArgs {
            args: args.to_vec(),
            arg_fields: vec![],
            number_rows: BATCH_SIZE,
            return_field: Arc::clone(return_field),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .unwrap(),
    );
}

fn criterion_benchmark(c: &mut Criterion) {
    let comet = SparkMapExtract::new();
    let datafusion = map_extract_udf();
    let mut group = c.benchmark_group("map_extract");
    let string_field = Arc::new(Field::new("result", DataType::Utf8, true));

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
                |b, args| b.iter(|| call(&comet, args, &string_field)),
            );
            group.bench_with_input(
                BenchmarkId::new(format!("datafusion/{case}"), entries),
                &args,
                |b, args| b.iter(|| call(datafusion.inner().as_ref(), args, &string_field)),
            );
        }
    }

    group.finish();
}

/// Two entries per row: key 1 selects a short value; key 2 holds an unselected long value.
/// Equal middle lengths in the deep case expose capacity propagated to grandchildren.
/// NULL outer maps deliberately retain their entries, a valid Arrow representation that
/// also exercises dense-null selections without changing the candidate distribution.
fn nested_map(selected_len: usize, null_percent: usize, shape: &str) -> MapArray {
    let width = if shape.starts_with("deep-list") { 8 } else { 1 };
    let offsets = OffsetBuffer::<i32>::from_lengths((0..BATCH_SIZE).flat_map(|_| {
        std::iter::repeat_n(selected_len, width).chain(std::iter::repeat_n(128, width))
    }));
    let count = *offsets.last().unwrap();
    let lists: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        offsets,
        Arc::new(if shape.ends_with("child-nulls") {
            Int32Array::from_iter((0..count).map(|i| (i % 4 != 0).then_some(i)))
        } else {
            Int32Array::from_iter_values(0..count)
        }),
        None,
    ));
    let values: ArrayRef = match shape {
        "large-list" => Arc::new(LargeListArray::new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            OffsetBuffer::new(
                lists
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .unwrap()
                    .value_offsets()
                    .iter()
                    .map(|&o| o as i64)
                    .collect::<Vec<_>>()
                    .into(),
            ),
            Arc::clone(lists.as_any().downcast_ref::<ListArray>().unwrap().values()),
            None,
        )),
        "map" => {
            let lists = lists.as_any().downcast_ref::<ListArray>().unwrap();
            let fields = vec![
                Arc::new(Field::new("key", DataType::Int32, false)),
                Arc::new(Field::new("value", DataType::Int32, true)),
            ];
            Arc::new(MapArray::new(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(fields.clone().into()),
                    false,
                )),
                lists.offsets().clone(),
                StructArray::new(
                    fields.into(),
                    vec![Arc::clone(lists.values()), Arc::clone(lists.values())],
                    None,
                ),
                None,
                false,
            ))
        }
        "deep-list" | "deep-list-child-nulls" => Arc::new(ListArray::new(
            Arc::new(Field::new("item", lists.data_type().clone(), true)),
            OffsetBuffer::from_lengths(std::iter::repeat_n(width, 2 * BATCH_SIZE)),
            lists,
            None,
        )),
        "struct-list" => Arc::new(StructArray::new(
            vec![Arc::new(Field::new(
                "items",
                lists.data_type().clone(),
                true,
            ))]
            .into(),
            vec![lists],
            None,
        )),
        _ => lists,
    };
    let fields = vec![
        Arc::new(Field::new("key", DataType::Int32, false)),
        Arc::new(Field::new("value", values.data_type().clone(), true)),
    ];
    let entries = StructArray::new(
        fields.clone().into(),
        vec![
            Arc::new(Int32Array::from_iter_values(
                (0..BATCH_SIZE).flat_map(|_| [1, 2]),
            )),
            values,
        ],
        None,
    );
    MapArray::new(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(fields.into()),
            false,
        )),
        OffsetBuffer::from_lengths(std::iter::repeat_n(2, BATCH_SIZE)),
        entries,
        (null_percent > 0).then(|| {
            (0..BATCH_SIZE)
                .map(|row| row % 100 >= null_percent)
                .collect::<NullBuffer>()
        }),
        false,
    )
}

fn nested_benchmark(c: &mut Criterion) {
    let comet = SparkMapExtract::new();
    let mut group = c.benchmark_group("map_extract_nested");
    for shape in [
        "list",
        "large-list",
        "deep-list",
        "struct-list",
        "map",
        "list-child-nulls",
        "deep-list-child-nulls",
    ] {
        for selected_len in [0, 1, 128] {
            for null_percent in [0, 25, 75] {
                let map = nested_map(selected_len, null_percent, shape);
                let field = Arc::new(Field::new("result", map.value_type().clone(), true));
                let map = ColumnarValue::Array(Arc::new(map));
                for (key_shape, key) in [
                    ("scalar", ColumnarValue::Scalar(ScalarValue::Int32(Some(1)))),
                    (
                        "column",
                        ColumnarValue::Array(Arc::new(Int32Array::from_iter((0..BATCH_SIZE).map(
                            |row| match row % 20 {
                                0 => None,
                                1 => Some(3),
                                _ => Some(1),
                            },
                        )))),
                    ),
                ] {
                    let args = vec![map.clone(), key];
                    group.bench_with_input(
                        BenchmarkId::new(
                            format!("{shape}/{key_shape}/{selected_len}-selected"),
                            format!("{null_percent}%-nulls"),
                        ),
                        &args,
                        |b, args| b.iter(|| call(&comet, args, &field)),
                    );
                }
            }
        }
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark, nested_benchmark);
criterion_main!(benches);
