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

use arrow::array::{
    Array, ArrayRef, Int32Array, ListArray, MapArray, RecordBatch, StringArray, StructArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::common::ScalarValue;
use datafusion::physical_expr::expressions::{Column, Literal};
use datafusion::physical_expr::PhysicalExpr;
use datafusion_comet_spark_expr::{create_query_context_map, ListExtract};
use std::hint::black_box;
use std::sync::Arc;

#[path = "common/mod.rs"]
mod common;
use common::{list_arrays, NULL_RATIOS, ROW_COUNTS};

fn single_col_batch(col: ArrayRef) -> RecordBatch {
    let schema = Schema::new(vec![Field::new("c0", col.data_type().clone(), true)]);
    RecordBatch::try_new(Arc::new(schema), vec![col]).unwrap()
}

fn criterion_benchmark(c: &mut Criterion) {
    // list_extract(list, ordinal = 2): one-based element access (Spark element_at), non-ANSI.
    let expr = ListExtract::new(
        Arc::new(Column::new("c0", 0)),
        Arc::new(Literal::new(ScalarValue::Int32(Some(2)))),
        None,
        true,
        false,
        None,
        create_query_context_map(),
    );
    let mut group = c.benchmark_group("list_extract");
    for rows in ROW_COUNTS {
        for (null_ratio, tag) in NULL_RATIOS {
            for (ty, list, _) in list_arrays(rows, null_ratio, 8) {
                let batch = single_col_batch(list);
                group.bench_with_input(
                    BenchmarkId::from_parameter(format!("{ty}/{rows}/{tag}")),
                    &batch,
                    |b, batch| b.iter(|| black_box(expr.evaluate(batch).unwrap())),
                );
            }
        }
    }
    group.finish();
}

const ROWS: usize = 8192;
const ELEMENTS_PER_ROW: usize = 5;

fn list_of(values: ArrayRef, with_nulls: bool) -> ArrayRef {
    let offsets = (0..=ROWS)
        .map(|i| (i * ELEMENTS_PER_ROW) as i32)
        .collect::<Vec<_>>();
    let nulls = with_nulls.then(|| (0..ROWS).map(|row| row % 4 != 0).collect::<NullBuffer>());
    let field = Arc::new(Field::new("item", values.data_type().clone(), true));
    Arc::new(ListArray::new(
        field,
        OffsetBuffer::new(offsets.into()),
        values,
        nulls,
    ))
}

fn bench_case(
    c: &mut Criterion,
    name: &str,
    list: ArrayRef,
    oob: bool,
    default: Option<ScalarValue>,
    with_null_ordinals: bool,
) {
    let indices = Arc::new(Int32Array::from_iter((0..ROWS).map(|row| {
        if with_null_ordinals && row % 4 == 1 {
            None
        } else {
            Some(if oob && row % 2 == 0 {
                ELEMENTS_PER_ROW as i32 + 1
            } else {
                3
            })
        }
    })));
    let schema = Arc::new(Schema::new(vec![
        Field::new("list", list.data_type().clone(), list.null_count() > 0),
        Field::new("index", DataType::Int32, indices.null_count() > 0),
    ]));
    let batch = RecordBatch::try_new(schema, vec![list, indices]).unwrap();
    let default = default.map(|value| Arc::new(Literal::new(value)) as Arc<dyn PhysicalExpr>);
    let expr = ListExtract::new(
        Arc::new(Column::new("list", 0)),
        Arc::new(Column::new("index", 1)),
        default,
        true,
        false,
        None,
        create_query_context_map(),
    );

    c.bench_function(name, |b| {
        b.iter(|| black_box(expr.evaluate(black_box(&batch)).unwrap()))
    });
}

/// Covers the axes the `take`-based fast path is sensitive to: whether a default is
/// present, how often the ordinal falls out of bounds, and null lists/ordinals.
fn bench_defaults(c: &mut Criterion) {
    let total = ROWS * ELEMENTS_PER_ROW;
    let int_values = Arc::new(Int32Array::from_iter_values(0..total as i32));
    let string_values = Arc::new(StringArray::from_iter_values(
        (0..total).map(|i| format!("value-{i}")),
    ));
    let ints = list_of(int_values.clone(), false);
    let strings = list_of(string_values.clone(), false);
    let nullable_ints = list_of(int_values, true);
    let nullable_strings = list_of(string_values, true);

    for oob in [false, true] {
        let suffix = if oob { "50%-oob" } else { "0%-oob" };
        bench_case(
            c,
            &format!("list_extract_defaults/int32/no-default/{suffix}"),
            Arc::clone(&ints),
            oob,
            None,
            false,
        );
        bench_case(
            c,
            &format!("list_extract_defaults/int32/null-default/{suffix}"),
            Arc::clone(&ints),
            oob,
            Some(ScalarValue::Int32(None)),
            false,
        );
        bench_case(
            c,
            &format!("list_extract_defaults/int32/non-null-default/{suffix}"),
            Arc::clone(&ints),
            oob,
            Some(ScalarValue::Int32(Some(0))),
            false,
        );
        bench_case(
            c,
            &format!("list_extract_defaults/utf8/no-default/{suffix}"),
            Arc::clone(&strings),
            oob,
            None,
            false,
        );
        bench_case(
            c,
            &format!("list_extract_defaults/utf8/null-default/{suffix}"),
            Arc::clone(&strings),
            oob,
            Some(ScalarValue::Utf8(None)),
            false,
        );
        bench_case(
            c,
            &format!("list_extract_defaults/utf8/non-null-default/{suffix}"),
            Arc::clone(&strings),
            oob,
            Some(ScalarValue::Utf8(Some(String::new()))),
            false,
        );
    }

    bench_case(
        c,
        "list_extract_defaults/int32/no-default/25%-null-lists-25%-null-ordinals",
        Arc::clone(&nullable_ints),
        false,
        None,
        true,
    );
    bench_case(
        c,
        "list_extract_defaults/int32/non-null-default/25%-null-lists-25%-null-ordinals",
        nullable_ints,
        false,
        Some(ScalarValue::Int32(Some(0))),
        true,
    );
    bench_case(
        c,
        "list_extract_defaults/utf8/no-default/25%-null-lists-25%-null-ordinals",
        Arc::clone(&nullable_strings),
        false,
        None,
        true,
    );
    bench_case(
        c,
        "list_extract_defaults/utf8/non-null-default/25%-null-lists-25%-null-ordinals",
        nullable_strings,
        false,
        Some(ScalarValue::Utf8(Some(String::new()))),
        true,
    );
}

/// Skewed nested lengths expose `take`'s reservation based on the entire input,
/// rather than the selected children. Include the opposite (long selection), too.
fn bench_nested(c: &mut Criterion) {
    let mut group = c.benchmark_group("list_extract_nested");
    for selected_len in [0, 1, 128] {
        let lengths = (0..ROWS).flat_map(|_| [selected_len, 128]);
        let offsets = OffsetBuffer::<i32>::from_lengths(lengths);
        let count = *offsets.last().unwrap() as usize;
        let ints: ArrayRef = Arc::new(Int32Array::from_iter_values(0..count as i32));
        let field = Arc::new(Field::new("item", DataType::Int32, true));
        let lists = Arc::new(ListArray::new(field, offsets.clone(), ints.clone(), None));
        let fields = vec![
            Arc::new(Field::new("key", DataType::Int32, false)),
            Arc::new(Field::new("value", DataType::Int32, true)),
        ];
        let entries = StructArray::new(fields.into(), vec![ints.clone(), ints], None);
        let maps = Arc::new(MapArray::new(
            Arc::new(Field::new("entries", entries.data_type().clone(), false)),
            offsets,
            entries,
            None,
            false,
        ));
        let structs = Arc::new(StructArray::new(
            vec![Arc::new(Field::new(
                "nested",
                lists.data_type().clone(),
                true,
            ))]
            .into(),
            vec![lists.clone()],
            None,
        ));

        bench_nested_values(
            &mut group,
            selected_len,
            [
                ("list", lists as ArrayRef),
                ("map", maps as ArrayRef),
                ("struct-list", structs as ArrayRef),
            ],
        );
    }
    group.finish();
}

fn bench_nested_values(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    selected_len: usize,
    values: impl IntoIterator<Item = (&'static str, ArrayRef)>,
) {
    for (ty, values) in values {
        for null_percent in [0, 25, 75] {
            let nulls = (null_percent > 0).then(|| {
                (0..ROWS)
                    .map(|row| row % 4 >= null_percent / 25)
                    .collect::<NullBuffer>()
            });
            let outer: ArrayRef = Arc::new(ListArray::new(
                Arc::new(Field::new("item", values.data_type().clone(), true)),
                OffsetBuffer::from_lengths(std::iter::repeat_n(2, ROWS)),
                Arc::clone(&values),
                nulls,
            ));
            let batch = single_col_batch(outer);
            let expr = ListExtract::new(
                Arc::new(Column::new("c0", 0)),
                Arc::new(Literal::new(ScalarValue::Int32(Some(0)))),
                None,
                false,
                false,
                None,
                create_query_context_map(),
            );
            group.bench_function(
                format!("{ty}/{selected_len}-selected/{null_percent}%-nulls"),
                |b| b.iter(|| black_box(expr.evaluate(black_box(&batch)).unwrap())),
            );
        }
    }
}

/// Middle lists have equal lengths. Only the deeper selected lists are short,
/// exposing reservations propagated into their primitive buffers.
fn bench_deep_nested(c: &mut Criterion) {
    const WIDTH: usize = 32;
    let mut group = c.benchmark_group("list_extract_deep_nested");
    for selected_len in [0, 1, 16] {
        let offsets = OffsetBuffer::from_lengths((0..ROWS).flat_map(|_| {
            std::iter::repeat_n(selected_len, WIDTH).chain(std::iter::repeat_n(16, WIDTH))
        }));
        let count = *offsets.last().unwrap();
        let inner: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            offsets,
            Arc::new(Int32Array::from_iter_values(0..count)),
            None,
        ));
        let field = Arc::new(Field::new("item", inner.data_type().clone(), true));
        let lists: ArrayRef = Arc::new(ListArray::new(
            Arc::clone(&field),
            OffsetBuffer::from_lengths(std::iter::repeat_n(WIDTH, 2 * ROWS)),
            Arc::clone(&inner),
            None,
        ));
        let fixed_lists: ArrayRef = Arc::new(arrow::array::FixedSizeListArray::new(
            field,
            WIDTH as i32,
            Arc::clone(&inner),
            None,
        ));
        let entries = StructArray::new(
            vec![
                Arc::new(Field::new("key", DataType::Int32, false)),
                Arc::new(Field::new("value", inner.data_type().clone(), true)),
            ]
            .into(),
            vec![
                Arc::new(Int32Array::from_iter_values(
                    (0..2 * ROWS).flat_map(|_| 0..WIDTH as i32),
                )),
                inner,
            ],
            None,
        );
        let maps: ArrayRef = Arc::new(MapArray::new(
            Arc::new(Field::new("entries", entries.data_type().clone(), false)),
            OffsetBuffer::from_lengths(std::iter::repeat_n(WIDTH, 2 * ROWS)),
            entries,
            None,
            false,
        ));
        bench_nested_values(
            &mut group,
            selected_len,
            [
                ("list-list", lists),
                ("map-list", maps),
                ("fixed-list", fixed_lists),
            ],
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    criterion_benchmark,
    bench_defaults,
    bench_nested,
    bench_deep_nested
);
criterion_main!(benches);
