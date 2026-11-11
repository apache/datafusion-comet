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

//! Benchmarks for the nested `cast` paths: struct->struct (child retyped), map->map (value
//! retyped), and array->string. Each builds an 8192-row input and casts it through the public
//! `Cast` physical expression, mirroring the scalar cast benches.

use arrow::array::{
    ArrayRef, Int32Array, ListArray, MapArray, RecordBatch, StringArray, StructArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Fields, Int32Type, Schema};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_comet_spark_expr::{Cast, EvalMode, SparkCastOptions};
use std::hint::black_box;
use std::sync::Arc;

const NUM_ROWS: usize = 8192;
const ENTRIES_PER_ROW: usize = 2;

/// Single-column batch named `c` wrapping `array`.
fn batch(array: ArrayRef) -> RecordBatch {
    let schema = Schema::new(vec![Field::new("c", array.data_type().clone(), true)]);
    RecordBatch::try_new(Arc::new(schema), vec![array]).unwrap()
}

/// `Struct<a: Int32, b: Utf8>` with `NUM_ROWS` rows.
fn struct_input() -> ArrayRef {
    let a = Arc::new(Int32Array::from((0..NUM_ROWS as i32).collect::<Vec<_>>())) as ArrayRef;
    let b = Arc::new(StringArray::from(
        (0..NUM_ROWS).map(|i| format!("s{i}")).collect::<Vec<_>>(),
    )) as ArrayRef;
    let fields = Fields::from(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Utf8, true),
    ]);
    Arc::new(StructArray::new(fields, vec![a, b], None))
}

/// `Map<Utf8, Int32>` with `NUM_ROWS` rows of `ENTRIES_PER_ROW` entries each.
fn map_input() -> ArrayRef {
    let total = NUM_ROWS * ENTRIES_PER_ROW;
    let keys = Arc::new(StringArray::from(
        (0..total).map(|i| format!("k{i}")).collect::<Vec<_>>(),
    )) as ArrayRef;
    let values = Arc::new(Int32Array::from((0..total as i32).collect::<Vec<_>>())) as ArrayRef;
    let entries_fields = Fields::from(vec![
        Field::new("key_value_key", DataType::Utf8, false),
        Field::new("key_value_value", DataType::Int32, true),
    ]);
    let entries = StructArray::new(entries_fields, vec![keys, values], None);
    let offsets = OffsetBuffer::<i32>::new(
        (0..=NUM_ROWS)
            .map(|i| (i * ENTRIES_PER_ROW) as i32)
            .collect::<Vec<_>>()
            .into(),
    );
    let entries_field = Arc::new(Field::new(
        "key_value",
        DataType::Struct(entries.fields().clone()),
        false,
    ));
    Arc::new(MapArray::new(entries_field, offsets, entries, None, false))
}

/// `List<Int32>` with `NUM_ROWS` rows whose lengths cycle through 0..=3.
fn list_input() -> ArrayRef {
    let data = (0..NUM_ROWS).map(|i| {
        let len = i % 4;
        Some(
            (0..len)
                .map(|j| Some((i * 4 + j) as i32))
                .collect::<Vec<_>>(),
        )
    });
    Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(data))
}

fn criterion_benchmark(c: &mut Criterion) {
    let options = SparkCastOptions::new(EvalMode::Legacy, "UTC");
    let col: Arc<dyn PhysicalExpr> = Arc::new(Column::new("c", 0));

    let mut group = c.benchmark_group("cast_nested");

    // struct<a: Int32, b: Utf8> -> struct<a: Int64, b: Utf8>: retypes the first child.
    let struct_batch = batch(struct_input());
    let struct_target = DataType::Struct(Fields::from(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Utf8, true),
    ]));
    let cast_struct = Cast::new(col.clone(), struct_target, options.clone(), None, None);
    group.bench_function("struct_to_struct", |b| {
        b.iter(|| black_box(cast_struct.evaluate(black_box(&struct_batch)).unwrap()))
    });

    // map<Utf8, Int32> -> map<Utf8, Int64>: retypes the value field.
    let map_batch = batch(map_input());
    let map_target = DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int64, true),
            ])),
            false,
        )),
        false,
    );
    let cast_map = Cast::new(col.clone(), map_target, options.clone(), None, None);
    group.bench_function("map_to_map", |b| {
        b.iter(|| black_box(cast_map.evaluate(black_box(&map_batch)).unwrap()))
    });

    // list<Int32> -> Utf8: renders each list to its Spark string form.
    let list_batch = batch(list_input());
    let cast_list = Cast::new(col, DataType::Utf8, options, None, None);
    group.bench_function("array_to_string", |b| {
        b.iter(|| black_box(cast_list.evaluate(black_box(&list_batch)).unwrap()))
    });

    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
