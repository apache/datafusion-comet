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

use arrow::array::{ArrayRef, Int32Array, Int64Array};

use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::common::ScalarValue;
use datafusion::physical_plan::ColumnarValue;
use datafusion_comet_spark_expr::spark_round;
use std::hint::black_box;
use std::sync::Arc;

const ROWS: usize = 8192;

/// Long values spread over the whole i64 range, with every 10th row null.
fn int64_array() -> ArrayRef {
    let values: Vec<Option<i64>> = (0..ROWS)
        .map(|i| {
            if i % 10 == 0 {
                None
            } else {
                let v = (i as i64).wrapping_mul(1_125_899_906_842_624);
                Some(if i % 2 == 0 { v } else { -v })
            }
        })
        .collect();
    Arc::new(Int64Array::from(values))
}

fn int32_array() -> ArrayRef {
    let values: Vec<Option<i32>> = (0..ROWS)
        .map(|i| {
            if i % 10 == 0 {
                None
            } else {
                let v = (i as i32).wrapping_mul(262_144);
                Some(if i % 2 == 0 { v } else { -v })
            }
        })
        .collect();
    Arc::new(Int32Array::from(values))
}

fn bench_array(c: &mut Criterion, name: &str, array: &ArrayRef, point: i64, fail_on_error: bool) {
    let data_type = array.data_type().clone();
    c.bench_function(name, |b| {
        let args = vec![
            ColumnarValue::Array(Arc::clone(array)),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(point))),
        ];
        b.iter(|| black_box(spark_round(black_box(&args), &data_type, fail_on_error).unwrap()))
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    let longs = int64_array();
    // Small negative scale: 10^(-point) fits in i64, the common case.
    bench_array(c, "spark_round: int64 scale=-1 legacy", &longs, -1, false);
    bench_array(c, "spark_round: int64 scale=-1 ansi", &longs, -1, true);
    bench_array(c, "spark_round: int64 scale=-9 legacy", &longs, -9, false);
    // 10^(-point) overflows i64 but fits i128, the band fixed by #5070. Values
    // here are below the rounding threshold, so no overflow is raised.
    bench_array(c, "spark_round: int64 scale=-19 legacy", &longs, -19, false);
    // 10^(-point) overflows i128 too: every long rounds to 0.
    bench_array(c, "spark_round: int64 scale=-40 legacy", &longs, -40, false);

    let ints = int32_array();
    bench_array(c, "spark_round: int32 scale=-1 legacy", &ints, -1, false);
    bench_array(c, "spark_round: int32 scale=-1 ansi", &ints, -1, true);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
