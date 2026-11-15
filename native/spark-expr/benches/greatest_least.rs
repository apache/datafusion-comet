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

//! Compare Spark's float `greatest` and `least` with DataFusion's on ordinary data, where both
//! return the same values, which is checked before timing; special-value semantics belong in
//! tests.

use arrow::array::{Array, ArrayRef, Float64Array};
use arrow::datatypes::{DataType, Field};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::common::config::ConfigOptions;
use datafusion::functions::core::{greatest, least};
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDF};
use datafusion_comet_spark_expr::SparkGreatestLeast;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

const ROWS: usize = 8192;
const ARGS: usize = 3;

/// Doubles in no particular order, different for each `seed`, with every tenth row null if
/// `nulls`.
fn column(seed: usize, nulls: bool) -> ArrayRef {
    Arc::new(Float64Array::from_iter((0..ROWS).map(|i| {
        (!nulls || !(i + seed).is_multiple_of(10))
            .then_some(((i * 7919 + seed * 104729) % 10007) as f64 * 0.5 - 2000.0)
    })))
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("greatest_least");
    group.sample_size(20);
    group.warm_up_time(Duration::from_millis(250));
    group.measurement_time(Duration::from_secs(1));
    for is_greatest in [true, false] {
        let op = if is_greatest { "greatest" } else { "least" };
        let comet = ScalarUDF::new_from_impl(SparkGreatestLeast::new(is_greatest));
        let datafusion = if is_greatest { greatest() } else { least() };
        for nulls in [false, true] {
            let args = ScalarFunctionArgs {
                args: (0..ARGS)
                    .map(|seed| ColumnarValue::Array(column(seed, nulls)))
                    .collect(),
                arg_fields: (0..ARGS)
                    .map(|i| Arc::new(Field::new(format!("c{i}"), DataType::Float64, true)))
                    .collect(),
                number_rows: ROWS,
                return_field: Arc::new(Field::new("result", DataType::Float64, true)),
                config_options: Arc::new(ConfigOptions::default()),
            };
            let evaluate = |udf: &ScalarUDF| {
                udf.invoke_with_args(args.clone())
                    .unwrap()
                    .into_array(ROWS)
                    .unwrap()
            };
            assert_eq!(evaluate(&comet).to_data(), evaluate(&datafusion).to_data());
            let data = if nulls { "null_10pct" } else { "no_nulls" };
            for (engine, udf) in [("comet", &comet), ("datafusion", datafusion.as_ref())] {
                group.bench_function(
                    BenchmarkId::new(format!("{op}_{ARGS}_args_{engine}"), data),
                    |b| {
                        b.iter(|| black_box(udf.invoke_with_args(black_box(args.clone())).unwrap()))
                    },
                );
            }
        }
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
