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

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::common::ScalarValue;
use datafusion::physical_plan::ColumnarValue;
use datafusion_comet_spark_expr::{spark_split, spark_split_sql};
use std::hint::black_box;

#[path = "common/mod.rs"]
mod common;
use common::{string_array, NULL_RATIOS, ROW_COUNTS};

const INPUT: &str = "apple,banana,cherry";

fn criterion_benchmark(c: &mut Criterion) {
    let sep = ColumnarValue::Scalar(ScalarValue::Utf8(Some(",".to_string())));

    let mut split_group = c.benchmark_group("spark_split");
    for rows in ROW_COUNTS {
        for (null_ratio, tag) in NULL_RATIOS {
            let args = vec![
                ColumnarValue::Array(string_array(rows, null_ratio, |_| INPUT.to_string())),
                sep.clone(),
            ];
            split_group.bench_with_input(
                BenchmarkId::from_parameter(format!("{rows}/{tag}")),
                &args,
                |b, args| b.iter(|| black_box(spark_split(black_box(args)).unwrap())),
            );
        }
    }
    split_group.finish();

    let mut split_sql_group = c.benchmark_group("spark_split_sql");
    for rows in ROW_COUNTS {
        for (null_ratio, tag) in NULL_RATIOS {
            let args = vec![
                ColumnarValue::Array(string_array(rows, null_ratio, |_| INPUT.to_string())),
                sep.clone(),
            ];
            split_sql_group.bench_with_input(
                BenchmarkId::from_parameter(format!("{rows}/{tag}")),
                &args,
                |b, args| b.iter(|| black_box(spark_split_sql(black_box(args)).unwrap())),
            );
        }
    }
    split_sql_group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
