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

use arrow::array::{ArrayRef, StringArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field};
use criterion::measurement::WallTime;
use criterion::{criterion_group, criterion_main, BenchmarkGroup, Criterion, Throughput};
use datafusion::common::config::ConfigOptions;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_comet_spark_expr::url_funcs::{CometParseUrl, CometTryParseUrl};
use std::hint::black_box;
use std::sync::Arc;

const N: usize = 8192;

fn urls(scheme: &str, path: &str) -> StringArray {
    StringArray::from(
        (0..N)
            .map(|i| {
                format!(
                    "{scheme}://site{}.example.com{path}/{i}?a={i}&v=value{i}&b={i}#section{i}",
                    i % 100
                )
            })
            .collect::<Vec<_>>(),
    )
}

fn bench(
    group: &mut BenchmarkGroup<'_, WallTime>,
    name: &str,
    udf: &dyn ScalarUDFImpl,
    urls: &ArrayRef,
    part: &str,
    key: Option<&str>,
) {
    // The scalar arguments and output have the same string layout as the URL column.
    let scalar = |value: &str| {
        let value = Some(value.to_string());
        ColumnarValue::Scalar(match urls.data_type() {
            DataType::Utf8 => ScalarValue::Utf8(value),
            DataType::LargeUtf8 => ScalarValue::LargeUtf8(value),
            DataType::Utf8View => ScalarValue::Utf8View(value),
            _ => unreachable!(),
        })
    };
    let mut args = vec![ColumnarValue::Array(Arc::clone(urls)), scalar(part)];
    if let Some(key) = key {
        args.push(scalar(key));
    }
    let return_field = Arc::new(Field::new("parse_url", urls.data_type().clone(), true));
    let config_options = Arc::new(ConfigOptions::default());
    group.bench_function(name, |b| {
        b.iter(|| {
            black_box(
                udf.invoke_with_args(ScalarFunctionArgs {
                    args: black_box(args.clone()),
                    number_rows: urls.len(),
                    return_field: Arc::clone(&return_field),
                    config_options: Arc::clone(&config_options),
                    arg_fields: vec![],
                })
                .unwrap(),
            )
        });
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_url");
    group.throughput(Throughput::Elements(N as u64));
    let udf = CometParseUrl::default();
    let short = urls("https", "/path");
    let https: ArrayRef = Arc::new(short.clone());
    let long: ArrayRef = Arc::new(urls("https", &format!("/{}", "x".repeat(2048))));
    let ftp: ArrayRef = Arc::new(urls("ftp", "/path"));

    // Measure complete UDF calls, including constructing the output Arrow array.
    for layout in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
        let input = cast(&https, &layout).unwrap();
        for part in ["HOST", "PATH", "FILE"] {
            bench(
                &mut group,
                &format!("https/{layout}/{part}"),
                &udf,
                &input,
                part,
                None,
            );
        }
        for (name, urls, part) in [("https_long", &long, "PATH"), ("ftp", &ftp, "HOST")] {
            bench(
                &mut group,
                &format!("{name}/{layout}/{part}"),
                &udf,
                &cast(urls, &layout).unwrap(),
                part,
                None,
            );
        }
    }

    for part in ["QUERY", "REF", "PROTOCOL", "AUTHORITY"] {
        bench(
            &mut group,
            &format!("https/Utf8/{part}"),
            &udf,
            &https,
            part,
            None,
        );
    }
    let http: ArrayRef = Arc::new(urls("http", "/path"));
    bench(&mut group, "http/Utf8/HOST", &udf, &http, "HOST", None);

    // Retain the constant query-key workload alongside component extraction.
    bench(&mut group, "query_key", &udf, &https, "QUERY", Some("v"));

    for (name, nulls_per_ten) in [("sparse_nulls", 1), ("dense_nulls", 9)] {
        let input: ArrayRef = Arc::new(
            short
                .iter()
                .enumerate()
                .map(|(i, value)| if i % 10 < nulls_per_ten { None } else { value })
                .collect::<StringArray>(),
        );
        bench(
            &mut group,
            &format!("{name}/Utf8/PATH"),
            &udf,
            &input,
            "PATH",
            None,
        );
    }
    let unicode: ArrayRef = Arc::new(urls("https", "/café/東京"));
    bench(
        &mut group,
        "unicode/Utf8/PATH",
        &udf,
        &unicode,
        "PATH",
        None,
    );
    let invalid: ArrayRef = Arc::new(
        short
            .iter()
            .enumerate()
            .map(|(i, value)| {
                if i % 10 == 0 {
                    Some("https://example.com/invalid path")
                } else {
                    value
                }
            })
            .collect::<StringArray>(),
    );
    bench(
        &mut group,
        "try_invalid/Utf8/PATH",
        &CometTryParseUrl::default(),
        &invalid,
        "PATH",
        None,
    );

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
