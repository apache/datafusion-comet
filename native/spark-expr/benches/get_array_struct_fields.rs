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

use arrow::array::{Array, ArrayRef, Int64Array, ListArray, RecordBatch, StringArray, StructArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{Field, Int32Type, Schema};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_comet_spark_expr::GetArrayStructFields;
use std::hint::black_box;
use std::sync::Arc;

#[path = "common/mod.rs"]
mod common;
use common::{is_null, NULL_RATIOS, ROW_COUNTS};

const ELEMS: usize = 8;

// A List<Struct<a: Int64, b: Utf8, c: List<Int32>>>: covers a fixed-width, variable-width, and
// nested child field to project out.
fn struct_list(rows: usize, null_ratio: f64) -> ArrayRef {
    let n = rows * ELEMS;
    let a: ArrayRef = Arc::new(Int64Array::from_iter_values((0..n).map(|i| i as i64)));
    let b: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..n).map(|i| format!("s{}", i % 128)),
    ));
    let c: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
        (0..n).map(|k| Some((0..4).map(|j| Some((k + j) as i32)).collect::<Vec<_>>())),
    ));
    let structs = StructArray::from(vec![
        (Arc::new(Field::new("a", a.data_type().clone(), false)), a),
        (Arc::new(Field::new("b", b.data_type().clone(), false)), b),
        (Arc::new(Field::new("c", c.data_type().clone(), true)), c),
    ]);
    let field = Arc::new(Field::new_list_field(structs.data_type().clone(), true));
    let offsets = OffsetBuffer::<i32>::from_lengths((0..rows).map(|_| ELEMS));
    let nulls = NullBuffer::from(
        (0..rows)
            .map(|i| !is_null(i, null_ratio))
            .collect::<Vec<bool>>(),
    );
    Arc::new(ListArray::new(
        field,
        offsets,
        Arc::new(structs),
        Some(nulls),
    ))
}

fn criterion_benchmark(c: &mut Criterion) {
    let fields = [(0usize, "int64"), (1, "utf8"), (2, "list")];
    let mut group = c.benchmark_group("get_array_struct_fields");
    for rows in ROW_COUNTS {
        for (null_ratio, tag) in NULL_RATIOS {
            let list = struct_list(rows, null_ratio);
            let schema = Schema::new(vec![Field::new("c0", list.data_type().clone(), true)]);
            let batch = RecordBatch::try_new(Arc::new(schema), vec![list]).unwrap();
            for (ordinal, ty) in fields {
                let expr = GetArrayStructFields::new(Arc::new(Column::new("c0", 0)), ordinal);
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

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
