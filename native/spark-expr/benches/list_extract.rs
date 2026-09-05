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

use arrow::array::{Array, ArrayRef, Int32Array, ListArray, StringArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::common::ScalarValue;
use datafusion::physical_expr::expressions::{Column, Literal};
use datafusion::physical_expr::PhysicalExpr;
use datafusion_comet_spark_expr::{create_query_context_map, ListExtract};
use std::hint::black_box;
use std::sync::Arc;

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

fn criterion_benchmark(c: &mut Criterion) {
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
            &format!("list_extract/int32/no-default/{suffix}"),
            Arc::clone(&ints),
            oob,
            None,
            false,
        );
        bench_case(
            c,
            &format!("list_extract/int32/null-default/{suffix}"),
            Arc::clone(&ints),
            oob,
            Some(ScalarValue::Int32(None)),
            false,
        );
        bench_case(
            c,
            &format!("list_extract/int32/non-null-default/{suffix}"),
            Arc::clone(&ints),
            oob,
            Some(ScalarValue::Int32(Some(0))),
            false,
        );
        bench_case(
            c,
            &format!("list_extract/utf8/no-default/{suffix}"),
            Arc::clone(&strings),
            oob,
            None,
            false,
        );
        bench_case(
            c,
            &format!("list_extract/utf8/null-default/{suffix}"),
            Arc::clone(&strings),
            oob,
            Some(ScalarValue::Utf8(None)),
            false,
        );
        bench_case(
            c,
            &format!("list_extract/utf8/non-null-default/{suffix}"),
            Arc::clone(&strings),
            oob,
            Some(ScalarValue::Utf8(Some(String::new()))),
            false,
        );
    }

    bench_case(
        c,
        "list_extract/int32/no-default/25%-null-lists-25%-null-ordinals",
        Arc::clone(&nullable_ints),
        false,
        None,
        true,
    );
    bench_case(
        c,
        "list_extract/int32/non-null-default/25%-null-lists-25%-null-ordinals",
        nullable_ints,
        false,
        Some(ScalarValue::Int32(Some(0))),
        true,
    );
    bench_case(
        c,
        "list_extract/utf8/no-default/25%-null-lists-25%-null-ordinals",
        Arc::clone(&nullable_strings),
        false,
        None,
        true,
    );
    bench_case(
        c,
        "list_extract/utf8/non-null-default/25%-null-lists-25%-null-ordinals",
        nullable_strings,
        false,
        Some(ScalarValue::Utf8(Some(String::new()))),
        true,
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
