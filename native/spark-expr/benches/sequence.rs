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

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::common::{config::ConfigOptions, ScalarValue};
use datafusion::logical_expr::{ScalarFunctionArgs, ScalarUDFImpl};
use datafusion::physical_plan::ColumnarValue;
use datafusion_comet_spark_expr::{SequenceMemoryPool, SparkSequence};
use std::hint::black_box;
use std::sync::Arc;

const NUM_ROWS: usize = 8192;

fn list_of_i64() -> DataType {
    DataType::List(Arc::new(Field::new_list_field(DataType::Int64, false)))
}

/// start/stop columns generating `elems_per_row` elements per row (ascending, step 1).
/// When `null_every` is Some(n), every nth row of `start` is null.
fn args_with_len(elems_per_row: i64, null_every: Option<usize>) -> Vec<ColumnarValue> {
    let start = Int64Array::from(
        (0..NUM_ROWS)
            .map(|i| match null_every {
                Some(n) if i % n == 0 => None,
                _ => Some(i as i64),
            })
            .collect::<Vec<_>>(),
    );
    let stop = Int64Array::from(
        (0..NUM_ROWS)
            .map(|i| Some(i as i64 + elems_per_row - 1))
            .collect::<Vec<_>>(),
    );
    vec![
        ColumnarValue::Array(Arc::new(start)),
        ColumnarValue::Array(Arc::new(stop)),
    ]
}

fn criterion_benchmark(c: &mut Criterion) {
    let return_type = list_of_i64();
    // Raise the isolated benchmark allowance explicitly: 8192 x 10000 i64 elements exceed
    // the 256 MiB production default. The safety ceiling has separate refusal tests.
    let udf = SparkSequence::new(
        return_type.clone(),
        SequenceMemoryPool::new(1024 * 1024 * 1024),
    );

    let mut group = c.benchmark_group("sequence");

    // Short sequences: per-row overhead dominates.
    for elems in [2i64, 5] {
        let args = args_with_len(elems, None);
        group.bench_function(format!("short_{elems}_elems"), |b| {
            b.iter(|| black_box(udf.evaluate(&args, NUM_ROWS).unwrap()))
        });
    }

    // Long sequences: element throughput dominates. 365 is the date-spine shape from the
    // issue; 10k stresses the child buffer reservation.
    for elems in [365i64, 10_000] {
        let args = args_with_len(elems, None);
        group.bench_function(format!("long_{elems}_elems"), |b| {
            b.iter(|| black_box(udf.evaluate(&args, NUM_ROWS).unwrap()))
        });
    }

    // Descending with explicit negative step.
    {
        let start = Int64Array::from((0..NUM_ROWS).map(|i| i as i64 + 364).collect::<Vec<_>>());
        let stop = Int64Array::from((0..NUM_ROWS).map(|i| i as i64).collect::<Vec<_>>());
        let step = Int64Array::from(vec![-1i64; NUM_ROWS]);
        let args = vec![
            ColumnarValue::Array(Arc::new(start)),
            ColumnarValue::Array(Arc::new(stop)),
            ColumnarValue::Array(Arc::new(step)),
        ];
        group.bench_function("descending_365_elems", |b| {
            b.iter(|| black_box(udf.evaluate(&args, NUM_ROWS).unwrap()))
        });
    }

    // Zero step with start == stop: single-element rows through the step==0 path.
    {
        let start = Int64Array::from((0..NUM_ROWS).map(|i| i as i64).collect::<Vec<_>>());
        let stop = Int64Array::from((0..NUM_ROWS).map(|i| i as i64).collect::<Vec<_>>());
        let step = Int64Array::from(vec![0i64; NUM_ROWS]);
        let args = vec![
            ColumnarValue::Array(Arc::new(start)),
            ColumnarValue::Array(Arc::new(stop)),
            ColumnarValue::Array(Arc::new(step)),
        ];
        group.bench_function("zero_step_start_eq_stop", |b| {
            b.iter(|| black_box(udf.evaluate(&args, NUM_ROWS).unwrap()))
        });
    }

    // Sparse (every 10th row) and dense (every 2nd row) nulls over the date-spine shape.
    for (label, every) in [("sparse_nulls", 10usize), ("dense_nulls", 2)] {
        let args = args_with_len(365, Some(every));
        group.bench_function(format!("{label}_365_elems"), |b| {
            b.iter(|| black_box(udf.evaluate(&args, NUM_ROWS).unwrap()))
        });
    }

    // Error path: the boundary check rejects the first row.
    {
        let start = Int64Array::from(vec![0i64; NUM_ROWS]);
        let stop = Int64Array::from(vec![100i64; NUM_ROWS]);
        let step = Int64Array::from(vec![-1i64; NUM_ROWS]);
        let args = vec![
            ColumnarValue::Array(Arc::new(start)),
            ColumnarValue::Array(Arc::new(stop)),
            ColumnarValue::Array(Arc::new(step)),
        ];
        group.bench_function("error_illegal_boundaries", |b| {
            b.iter(|| black_box(udf.evaluate(&args, NUM_ROWS).unwrap_err()))
        });
    }

    // Scalar steps must not allocate a full input column.
    for elems in [2i64, 5, 365, 10_000] {
        let mut args = args_with_len(elems, None);
        args.push(ColumnarValue::Scalar(ScalarValue::Int64(Some(1))));
        group.bench_function(format!("scalar_step_{elems}_elems"), |b| {
            b.iter(|| black_box(udf.evaluate(&args, NUM_ROWS).unwrap()))
        });
    }

    // Exercise the physical UDF and the caller's expansion of a scalar result. Both the
    // baseline and candidate must produce the complete requested batch.
    let return_field = Arc::new(Field::new("result", return_type.clone(), true));
    let config_options = Arc::new(ConfigOptions::default());
    for rows in [1, NUM_ROWS] {
        for elems in [2i64, 5, 365, 10_000] {
            let args = vec![
                ColumnarValue::Scalar(ScalarValue::Int64(Some(0))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(elems - 1))),
            ];
            group.bench_function(format!("all_scalar_{rows}_rows_{elems}_elems"), |b| {
                b.iter(|| {
                    black_box(
                        udf.invoke_with_args(ScalarFunctionArgs {
                            args: args.clone(),
                            arg_fields: vec![],
                            number_rows: rows,
                            return_field: Arc::clone(&return_field),
                            config_options: Arc::clone(&config_options),
                        })
                        .unwrap()
                        .into_array(rows)
                        .unwrap(),
                    )
                })
            });
        }
    }

    for (label, every) in [("sparse_nulls", 10usize), ("dense_nulls", 2)] {
        for elems in [2i64, 5] {
            let args = args_with_len(elems, Some(every));
            group.bench_function(format!("{label}_{elems}_elems"), |b| {
                b.iter(|| black_box(udf.evaluate(&args, NUM_ROWS).unwrap()))
            });
        }
    }

    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
