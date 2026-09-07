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

use arrow::array::StringArray;
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::ColumnarValue;
use datafusion_comet_spark_expr::spark_get_json_object;
use std::hint::black_box;
use std::sync::Arc;

const N: usize = 8192;

fn json_docs() -> ColumnarValue {
    let values: Vec<String> = (0..N)
        .map(|i| {
            format!(
                r#"{{"id":{i},"name":"user{i}","email":"user{i}@example.com","tags":["a","b","c"],"address":{{"city":"city{i}","zip":"{i:05}"}},"active":{},"score":{}.5,"notes":"some longer free text field for row {i}"}}"#,
                i % 2 == 0,
                i % 100
            )
        })
        .collect();
    ColumnarValue::Array(Arc::new(StringArray::from(values)))
}

fn path(p: &str) -> ColumnarValue {
    ColumnarValue::Scalar(ScalarValue::Utf8(Some(p.to_string())))
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_json_object");
    let docs = json_docs();

    // The common Spark shape: extract one field from each document using a
    // constant path.
    for (name, p) in [
        ("top_level_field", "$.name"),
        ("nested_field", "$.address.city"),
        ("array_index", "$.tags[1]"),
    ] {
        let path = path(p);
        group.bench_function(name, |b| {
            b.iter(|| {
                black_box(spark_get_json_object(&[docs.clone(), path.clone()]).unwrap());
            });
        });
    }

    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
