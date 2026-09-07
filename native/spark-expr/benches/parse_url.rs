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
use arrow::datatypes::{DataType, Field};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::common::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_comet_spark_expr::url_funcs::CometParseUrl;
use std::sync::Arc;

const N: usize = 10_000;

fn call(udf: &CometParseUrl, args: Vec<ColumnarValue>) -> ColumnarValue {
    let return_field = Arc::new(Field::new("parse_url", DataType::Utf8, true));
    let sfa = ScalarFunctionArgs {
        args,
        number_rows: N,
        return_field,
        config_options: Arc::new(ConfigOptions::default()),
        arg_fields: vec![],
    };
    udf.invoke_with_args(sfa).unwrap()
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_url");

    let udf = CometParseUrl::default();

    // The common Spark shape: extract a query parameter using a constant key
    // (`parse_url(col, 'QUERY', 'v')`). The key is broadcast to every row.
    let urls = StringArray::from(
        (0..N)
            .map(|i| format!("https://example.com/path?a={i}&v=value{i}&b={i}"))
            .collect::<Vec<_>>(),
    );
    let urls = ColumnarValue::Array(Arc::new(urls));
    let part = ColumnarValue::Scalar(datafusion::common::ScalarValue::Utf8(Some(
        "QUERY".to_string(),
    )));
    let key = ColumnarValue::Scalar(datafusion::common::ScalarValue::Utf8(Some("v".to_string())));

    group.bench_function("query_key", |b| {
        b.iter(|| call(&udf, vec![urls.clone(), part.clone(), key.clone()]));
    });

    group.finish();
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
