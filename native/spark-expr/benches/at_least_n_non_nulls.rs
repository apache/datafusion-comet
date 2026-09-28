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

use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, AsArray, Float32Array, Float64Array, RecordBatch, StringArray};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_comet_spark_expr::AtLeastNNonNulls;
use rand::{rngs::StdRng, RngExt, SeedableRng};

fn benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("at_least_n_non_nulls");
    group.sample_size(30);
    group.warm_up_time(Duration::from_millis(200));
    group.measurement_time(Duration::from_secs(1));
    for rows in [1, 63, 64, 65, 1024, 8192] {
        for kind in ["f64", "f32", "utf8"] {
            for width in [4, 32] {
                for (nulls, nans) in [
                    (0, 0),
                    (0, 5),
                    (0, 9),
                    (0, 10),
                    (0, 11),
                    (0, 19),
                    (0, 50),
                    (0, 95),
                    (0, 100),
                    (5, 0),
                    (5, 5),
                    (9, 0),
                    (10, 0),
                    (11, 0),
                    (19, 0),
                    (50, 0),
                    (50, 5),
                    (95, 0),
                    (100, 0),
                ] {
                    if kind == "utf8" && nans != 0 {
                        continue;
                    }
                    // Keep smaller batch controls focused; the default batch size covers
                    // the full NULL/NaN density matrix.
                    if rows != 8192 && (kind == "f32" || !matches!((nulls, nans), (0, 0) | (50, 0)))
                    {
                        continue;
                    }
                    let mut rng = StdRng::seed_from_u64(42);
                    let mut counts = vec![0; rows];
                    let arrays = (0..width)
                        .map(|column| {
                            let values = counts
                                .iter_mut()
                                .enumerate()
                                .map(|(row, count)| {
                                    let bucket = rng.random_range(0..100);
                                    *count += usize::from(bucket >= nulls + nans);
                                    if bucket < nulls {
                                        None
                                    } else if bucket < nulls + nans {
                                        Some(f64::NAN)
                                    } else {
                                        Some((row + column) as f64)
                                    }
                                })
                                .collect::<Vec<_>>();
                            let array: ArrayRef = match kind {
                                "f64" => Arc::new(Float64Array::from(values)),
                                "f32" => Arc::new(Float32Array::from_iter(
                                    values.into_iter().map(|v| v.map(|v| v as f32)),
                                )),
                                _ => Arc::new(StringArray::from_iter(
                                    values.into_iter().map(|v| v.map(|_| "value")),
                                )),
                            };
                            (format!("c{column}"), array)
                        })
                        .collect::<Vec<_>>();
                    let batch = RecordBatch::try_from_iter(arrays).unwrap();
                    for n in [1, width / 2, width] {
                        if rows != 8192 && n != width / 2 {
                            continue;
                        }
                        let children = (0..width)
                            .map(|i| Arc::new(Column::new(&format!("c{i}"), i)) as _)
                            .collect();
                        let expr = AtLeastNNonNulls::new(n as i32, children);
                        let expected = counts.iter().map(|&count| count >= n).collect::<Vec<_>>();
                        let result = expr
                            .evaluate(&batch)
                            .unwrap()
                            .into_array(batch.num_rows())
                            .unwrap();
                        assert_eq!(result.null_count(), 0);
                        assert_eq!(
                            result.as_boolean().values().iter().collect::<Vec<_>>(),
                            expected
                        );
                        group.bench_with_input(
                            BenchmarkId::new(
                                format!("{rows}/{kind}_{width}_null{nulls}_nan{nans}"),
                                n,
                            ),
                            &batch,
                            |b, batch| {
                                b.iter(|| black_box(expr.evaluate(black_box(batch)).unwrap()))
                            },
                        );
                    }
                }
            }
        }
    }
    group.finish();
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
