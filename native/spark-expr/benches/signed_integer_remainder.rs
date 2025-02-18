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

use arrow_array::builder::Int64Builder;
use arrow_schema::DataType;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion_comet_spark_expr::spark_signed_integer_remainder;
use datafusion_expr_common::columnar_value::ColumnarValue;
use std::sync::Arc;

fn criterion_benchmark(c: &mut Criterion) {
    // create input data
    let mut c1 = Int64Builder::new();
    let mut c2 = Int64Builder::new();
    for i in 0..1000 {
        c1.append_value(99999999 + i);
        c2.append_value(88888888 - i);
        c1.append_value(i64::MIN);
        c2.append_value(-1);
    }
    let c1 = Arc::new(c1.finish());
    let c2 = Arc::new(c2.finish());

    let args = [ColumnarValue::Array(c1), ColumnarValue::Array(c2)];
    c.bench_function("signed_integer_remainder", |b| {
        b.iter(|| {
            black_box(spark_signed_integer_remainder(
                black_box(&args),
                black_box(&DataType::Int64),
            ))
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
