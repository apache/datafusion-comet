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


use std::sync::Arc;
use arrow::datatypes::Int32Type;
use arrow::error::ArrowError;
use arrow_array::{builder::StringBuilder, builder::StringDictionaryBuilder, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use comet::execution::datafusion::expressions::regexp::RLike;
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::common::ScalarValue;
use datafusion_physical_expr::{expressions::Column, expressions::Literal, PhysicalExpr, expressions::LikeExpr};

fn criterion_benchmark(c: &mut Criterion) {
    let batch = create_utf8_batch().unwrap();
    let child_expr = Arc::new(Column::new("foo", 0));
    let pattern_expr = Arc::new(Literal::new(ScalarValue::Utf8(Some("5[0-9]5".to_string()))));
    let rlike  = RLike::new(child_expr.clone(), pattern_expr.clone());
    let df_rlike  = LikeExpr::new(false, false, child_expr, pattern_expr);

    let mut group = c.benchmark_group("regexp");
    group.bench_function("regexp_comet_rlike", |b| {
        b.iter(|| rlike.evaluate(&batch).unwrap());
    });
    group.bench_function("regexp_datafusion_rlike", |b| {
        b.iter(|| df_rlike.evaluate(&batch).unwrap());
    });
}

fn create_utf8_batch() -> Result<RecordBatch, ArrowError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, true),
        Field::new("b", DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)), true)
    ]));
    let mut string_builder = StringBuilder::new();
    let mut string_dict_builder = StringDictionaryBuilder::<Int32Type>::new();
    for i in 0..1000 {
        if i % 10 == 0 {
            string_builder.append_null();
            string_dict_builder.append_null();
        } else {
            string_builder.append_value(format!("{}", i));
            string_dict_builder.append_value(format!("{}", i));
        }
    }
    let string_array = string_builder.finish();
    let string_dict_array2 = string_dict_builder.finish();
    RecordBatch::try_new(schema.clone(), vec![Arc::new(string_array), Arc::new(string_dict_array2)])
}

fn config() -> Criterion {
    Criterion::default()
}

criterion_group! {
    name = benches;
    config = config();
    targets = criterion_benchmark
}
criterion_main!(benches);
