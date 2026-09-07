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

use arrow::array::{ArrayRef, Date32Array, StringArray};
use arrow::datatypes::{DataType, Field};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::common::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_comet_spark_expr::SparkDateTrunc;
use std::hint::black_box;
use std::sync::Arc;

const SIZE: usize = 8192;

fn create_date_array() -> ArrayRef {
    let dates: Vec<i32> = (0..SIZE).map(|i| ((i * 2) % 19000) as i32).collect();
    Arc::new(Date32Array::from(dates))
}

/// A constant format column, which is what a query like `trunc(d, fmt)` produces when the
/// format comes from a column rather than a literal.
fn create_constant_format_array(format: &str) -> ArrayRef {
    Arc::new(StringArray::from(vec![format; SIZE]))
}

/// A low-cardinality format column mixing the supported formats.
fn create_mixed_format_array() -> ArrayRef {
    let formats = ["YEAR", "quarter", "MONTH", "week"];
    let values: Vec<&str> = (0..SIZE).map(|i| formats[i % formats.len()]).collect();
    Arc::new(StringArray::from(values))
}

fn invoke(udf: &SparkDateTrunc, dates: &ArrayRef, formats: &ArrayRef) -> ColumnarValue {
    let args = ScalarFunctionArgs {
        args: vec![
            ColumnarValue::Array(Arc::clone(dates)),
            ColumnarValue::Array(Arc::clone(formats)),
        ],
        number_rows: SIZE,
        return_field: Arc::new(Field::new("date_trunc", DataType::Date32, true)),
        arg_fields: vec![],
        config_options: Arc::new(ConfigOptions::default()),
    };
    udf.invoke_with_args(args).unwrap()
}

fn criterion_benchmark(c: &mut Criterion) {
    let udf = SparkDateTrunc::new();
    let dates = create_date_array();

    let mut group = c.benchmark_group("date_trunc_array_fmt");

    for format in ["YEAR", "QUARTER", "MONTH", "WEEK"] {
        let formats = create_constant_format_array(format);
        group.bench_function(format.to_lowercase(), |b| {
            b.iter(|| black_box(invoke(&udf, &dates, &formats)))
        });
    }

    let formats = create_mixed_format_array();
    group.bench_function("mixed", |b| {
        b.iter(|| black_box(invoke(&udf, &dates, &formats)))
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
