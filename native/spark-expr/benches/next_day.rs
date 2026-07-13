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

//! Benchmarks for the Spark-compatible `next_day` expression.

use arrow::array::{ArrayRef, Date32Array, StringArray};
use arrow::datatypes::{DataType, Field};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_comet_spark_expr::SparkNextDay;
use std::hint::black_box;
use std::sync::Arc;

const BATCH_SIZE: usize = 8192;

fn date_array() -> ArrayRef {
    // A spread of valid Date32 values (days since epoch) around the year 2024.
    let values: Vec<i32> = (0..BATCH_SIZE as i32).map(|i| 19_000 + i % 400).collect();
    Arc::new(Date32Array::from(values))
}

/// Column of day-of-week names cycling through recognized names in varied casing.
fn day_of_week_array() -> ArrayRef {
    let names = [
        "Mon",
        "TUESDAY",
        "wed",
        "Th",
        "friday",
        "SA",
        "sun",
        "Monday",
        "TUE",
        "Wednesday",
    ];
    let values: Vec<&str> = (0..BATCH_SIZE).map(|i| names[i % names.len()]).collect();
    Arc::new(StringArray::from(values))
}

fn invoke(udf: &SparkNextDay, args: Vec<ColumnarValue>, number_rows: usize) -> ColumnarValue {
    let return_field = Arc::new(Field::new("next_day", DataType::Date32, true));
    udf.invoke_with_args(ScalarFunctionArgs {
        args,
        number_rows,
        return_field,
        arg_fields: vec![],
        config_options: Arc::new(ConfigOptions::default()),
    })
    .unwrap()
}

fn criterion_benchmark(c: &mut Criterion) {
    let udf = SparkNextDay::new(false);
    let dates = date_array();
    let dows = day_of_week_array();

    // Scalar day-of-week literal (the most common usage, e.g. next_day(d, 'Sunday')).
    c.bench_function("next_day: scalar day-of-week", |b| {
        b.iter(|| {
            let args = vec![
                ColumnarValue::Array(Arc::clone(&dates)),
                ColumnarValue::Scalar(datafusion::common::ScalarValue::Utf8(Some(
                    "Sunday".to_string(),
                ))),
            ];
            black_box(invoke(&udf, args, BATCH_SIZE))
        })
    });

    // Array day-of-week column with mixed casing.
    c.bench_function("next_day: array day-of-week", |b| {
        b.iter(|| {
            let args = vec![
                ColumnarValue::Array(Arc::clone(&dates)),
                ColumnarValue::Array(Arc::clone(&dows)),
            ];
            black_box(invoke(&udf, args, BATCH_SIZE))
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
