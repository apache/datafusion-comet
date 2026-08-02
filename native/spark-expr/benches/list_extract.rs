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

use arrow::array::{ArrayRef, Int32Array, ListArray, StringArray};
use arrow::buffer::OffsetBuffer;
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

fn list_of(values: ArrayRef) -> ArrayRef {
    let offsets = (0..=ROWS)
        .map(|i| (i * ELEMENTS_PER_ROW) as i32)
        .collect::<Vec<_>>();
    let field = Arc::new(Field::new("item", values.data_type().clone(), true));
    Arc::new(ListArray::new(
        field,
        OffsetBuffer::new(offsets.into()),
        values,
        None,
    ))
}

fn bench_case(
    c: &mut Criterion,
    name: &str,
    list: ArrayRef,
    oob: bool,
    default: Option<ScalarValue>,
) {
    let indices = Arc::new(Int32Array::from_iter_values((0..ROWS).map(|row| {
        if oob && row % 2 == 0 {
            ELEMENTS_PER_ROW as i32 + 1
        } else {
            3
        }
    })));
    let schema = Arc::new(Schema::new(vec![
        Field::new("list", list.data_type().clone(), false),
        Field::new("index", DataType::Int32, false),
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
    let ints = list_of(Arc::new(Int32Array::from_iter_values(0..total as i32)));
    let strings = list_of(Arc::new(StringArray::from_iter_values(
        (0..total).map(|i| format!("value-{i}")),
    )));

    for oob in [false, true] {
        let suffix = if oob { "50%-oob" } else { "0%-oob" };
        bench_case(
            c,
            &format!("list_extract/int32/null-default/{suffix}"),
            Arc::clone(&ints),
            oob,
            None,
        );
        bench_case(
            c,
            &format!("list_extract/int32/non-null-default/{suffix}"),
            Arc::clone(&ints),
            oob,
            Some(ScalarValue::Int32(Some(0))),
        );
        bench_case(
            c,
            &format!("list_extract/utf8/null-default/{suffix}"),
            Arc::clone(&strings),
            oob,
            None,
        );
        bench_case(
            c,
            &format!("list_extract/utf8/non-null-default/{suffix}"),
            Arc::clone(&strings),
            oob,
            Some(ScalarValue::Utf8(Some(String::new()))),
        );
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
