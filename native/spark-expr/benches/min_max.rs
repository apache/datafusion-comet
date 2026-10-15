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

//! Compare Spark's float `min` and `max` with DataFusion's; special-value semantics belong in
//! tests. Both return the same values on the data without NaNs, which is checked before timing.
//!
//! The grouped case updates 1024 groups from eight batches. Its loop scatters into the groups, so
//! it cannot vectorize, and the comparison form and bounds checks in `MinMaxGroupsAccumulator`
//! decide its speed.

use arrow::array::{Array, ArrayRef, Float64Array};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::common::ScalarValue;
use datafusion::functions_aggregate::min_max::{max_udaf, min_udaf};
use datafusion::logical_expr::function::AccumulatorArgs;
use datafusion::logical_expr::{AggregateUDF, EmitTo};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_comet_spark_expr::SparkMinMax;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

const ROWS: usize = 8192;
const GROUPS: usize = 1024;
const BATCHES: usize = 8;

/// Doubles in no particular order, with a NaN in every `nan_every`-th row if it is not zero.
fn values(nan_every: usize) -> ArrayRef {
    Arc::new(Float64Array::from_iter_values((0..ROWS).map(|i| {
        if nan_every > 0 && i % nan_every == 3 {
            f64::NAN
        } else {
            ((i * 7919) % 10007) as f64 * 0.5 - 2000.0
        }
    })))
}

fn criterion_benchmark(c: &mut Criterion) {
    let schema = Schema::new(vec![Field::new("d", DataType::Float64, true)]);
    let return_field = Arc::new(Field::new("d", DataType::Float64, true));
    let exprs: [Arc<dyn PhysicalExpr>; 1] = [Arc::new(Column::new("d", 0))];
    let args = |name| AccumulatorArgs {
        return_field: Arc::clone(&return_field),
        schema: &schema,
        expr_fields: &[],
        ignore_nulls: false,
        order_bys: &[],
        is_reversed: false,
        name,
        is_distinct: false,
        exprs: &exprs,
    };
    let group_indices: Vec<usize> = (0..ROWS).map(|i| (i * 31) % GROUPS).collect();

    let mut group = c.benchmark_group("min_max");
    group.sample_size(20);
    group.warm_up_time(Duration::from_millis(250));
    group.measurement_time(Duration::from_secs(1));
    for is_max in [true, false] {
        let op = if is_max { "max" } else { "min" };
        let comet = AggregateUDF::new_from_impl(SparkMinMax::new(is_max));
        let datafusion = if is_max { max_udaf() } else { min_udaf() };
        for (data, nan_every) in [("no_nan", 0), ("nan_10pct", 10)] {
            let batch = values(nan_every);
            let ungrouped = |udaf: &AggregateUDF| -> ScalarValue {
                let mut acc = udaf.accumulator(args(op)).unwrap();
                acc.update_batch(&[Arc::clone(&batch)]).unwrap();
                acc.evaluate().unwrap()
            };
            let grouped = |udaf: &AggregateUDF| -> ArrayRef {
                let mut acc = udaf.create_groups_accumulator(args(op)).unwrap();
                for _ in 0..BATCHES {
                    acc.update_batch(&[Arc::clone(&batch)], &group_indices, None, GROUPS)
                        .unwrap();
                }
                acc.evaluate(EmitTo::All).unwrap()
            };
            if nan_every == 0 {
                assert_eq!(ungrouped(&comet), ungrouped(&datafusion));
                assert_eq!(grouped(&comet).to_data(), grouped(&datafusion).to_data());
            }
            for (engine, udaf) in [("comet", &comet), ("datafusion", datafusion.as_ref())] {
                group.bench_function(
                    BenchmarkId::new(format!("{op}_ungrouped_{engine}"), data),
                    |b| b.iter(|| black_box(ungrouped(udaf))),
                );
                group.bench_function(
                    BenchmarkId::new(format!("{op}_grouped_{engine}"), data),
                    |b| b.iter(|| black_box(grouped(udaf))),
                );
            }
        }
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
