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

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::DataType;
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::physical_plan::ColumnarValue;
use datafusion_comet_spark_expr::spark_make_decimal;
use std::hint::black_box;
use std::sync::Arc;

// Target Decimal128(18, 2): the widest precision DecimalAggregates produces for MakeDecimal.
// At precision 18 every i64 fits, so all values in these benches are in the no-overflow
// common path — matching the shape Andy measured in the PR review.
const TARGET_PRECISION: u8 = 18;
const TARGET_SCALE: i8 = 2;

/// Build an Int64 column of `rows` rows, with every `null_every`-th row null
/// (`null_every == 0` means no nulls).
fn create_int64_array(rows: usize, null_every: usize) -> ArrayRef {
    let arr: Int64Array = (0..rows)
        .map(|i| {
            if null_every != 0 && i % null_every == 0 {
                None
            } else {
                Some((i as i64 % 100_000) * 100)
            }
        })
        .collect();
    Arc::new(arr)
}

fn criterion_benchmark(c: &mut Criterion) {
    let rows = 8192;
    let target = DataType::Decimal128(TARGET_PRECISION, TARGET_SCALE);

    let mut bench = |name: &str, arr: &ArrayRef, fail_on_error: bool| {
        let args = vec![ColumnarValue::Array(Arc::clone(arr))];
        c.bench_function(name, |b| {
            b.iter(|| {
                black_box(
                    spark_make_decimal(black_box(&args), black_box(&target), fail_on_error)
                        .unwrap(),
                )
            })
        });
    };

    let no_nulls = create_int64_array(rows, 0);
    let sparse_nulls = create_int64_array(rows, 10);
    let dense_nulls = create_int64_array(rows, 2);

    bench("spark_make_decimal: no nulls", &no_nulls, false);
    bench("spark_make_decimal: sparse nulls", &sparse_nulls, false);
    bench("spark_make_decimal: dense nulls", &dense_nulls, false);
    bench("spark_make_decimal: ansi no nulls", &no_nulls, true);
    bench("spark_make_decimal: ansi sparse nulls", &sparse_nulls, true);
    bench("spark_make_decimal: ansi dense nulls", &dense_nulls, true);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
