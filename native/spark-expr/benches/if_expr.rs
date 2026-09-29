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

use std::{hint::black_box, sync::Arc};

use arrow::array::{
    ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, RecordBatch,
    StringArray,
};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::common::ScalarValue;
use datafusion::physical_expr::{
    expressions::{Column, Literal},
    PhysicalExpr,
};
use datafusion_comet_spark_expr::IfExpr;

fn hash(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e3779b97f4a7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
    value ^ (value >> 31)
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("if_expr");
    for rows in [1, 63, 8192] {
        for kind in ["f32", "f64", "i32", "i64", "utf8", "long_utf8"] {
            // Uniform masks cover the zero-copy paths; mixed masks cover both selection directions.
            for (selected, mask_nulls, value_nulls) in
                [(0, 0, 0), (5, 0, 0), (50, 5, 5), (95, 50, 50), (100, 0, 0)]
            {
                if rows != 8192 && kind != "f64" {
                    continue;
                }
                let mask = BooleanArray::from_iter((0..rows).map(|i| {
                    (hash(i as u64 + 8) % 100 >= mask_nulls)
                        .then(|| hash(i as u64) % 100 < selected)
                }));
                let valid = |i: usize| hash(i as u64 + 5) % 100 >= value_nulls;
                let (array, scalar): (ArrayRef, ScalarValue) = match kind {
                    "f32" => (
                        Arc::new(Float32Array::from_iter(
                            (0..rows).map(|i| valid(i).then_some(i as f32)),
                        )),
                        ScalarValue::Float32(Some(0.0)),
                    ),
                    "f64" => (
                        Arc::new(Float64Array::from_iter(
                            (0..rows).map(|i| valid(i).then_some(i as f64)),
                        )),
                        ScalarValue::Float64(Some(0.0)),
                    ),
                    "i32" => (
                        Arc::new(Int32Array::from_iter(
                            (0..rows).map(|i| valid(i).then_some(i as i32)),
                        )),
                        ScalarValue::Int32(Some(0)),
                    ),
                    "i64" => (
                        Arc::new(Int64Array::from_iter(
                            (0..rows).map(|i| valid(i).then_some(i as i64)),
                        )),
                        ScalarValue::Int64(Some(0)),
                    ),
                    _ => {
                        let value = if kind == "long_utf8" {
                            "中文".repeat(100)
                        } else {
                            "value".to_string()
                        };
                        (
                            Arc::new(StringArray::from_iter(
                                (0..rows).map(|i| valid(i).then_some(value.as_str())),
                            )),
                            ScalarValue::Utf8(Some("replacement".to_string())),
                        )
                    }
                };
                let null = ScalarValue::try_new_null(array.data_type()).unwrap();
                let batch = RecordBatch::try_from_iter(vec![
                    ("value", array),
                    ("mask", Arc::new(mask) as ArrayRef),
                ])
                .unwrap();
                let column = Arc::new(Column::new("value", 0)) as Arc<dyn PhysicalExpr>;
                let literal = Arc::new(Literal::new(scalar)) as Arc<dyn PhysicalExpr>;
                for branches in ["scalar_col", "col_scalar", "col_null", "scalar_scalar"] {
                    let (t, f) = match branches {
                        "scalar_col" => (Arc::clone(&literal), Arc::clone(&column)),
                        "col_scalar" => (Arc::clone(&column), Arc::clone(&literal)),
                        "col_null" => (
                            Arc::clone(&column),
                            Arc::new(Literal::new(null.clone())) as Arc<dyn PhysicalExpr>,
                        ),
                        _ => (Arc::clone(&literal), Arc::clone(&literal)),
                    };
                    let expr = IfExpr::new(Arc::new(Column::new("mask", 1)), t, f);
                    let name = format!("{kind}/{rows}/true{selected}_masknull{mask_nulls}_valuenull{value_nulls}/{branches}");
                    group.bench_function(name, |b| {
                        b.iter(|| black_box(expr.evaluate(black_box(&batch)).unwrap()))
                    });
                }
            }
        }
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
