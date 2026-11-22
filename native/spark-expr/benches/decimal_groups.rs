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

//! Grouped decimal SUM and AVG accumulators, driven directly through
//! `GroupsAccumulator::update_batch` so the numbers cover the accumulators and not grouping or
//! the execution framework.

use arrow::array::{ArrayRef, Decimal128Array};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::functions_aggregate::sum::sum_udaf;
use datafusion::logical_expr::function::AccumulatorArgs;
use datafusion::logical_expr::{AggregateUDFImpl, EmitTo, GroupsAccumulator};
use datafusion_comet_spark_expr::{create_query_context_map, AvgDecimal, EvalMode, SumDecimal};
use std::hint::black_box;
use std::sync::Arc;

const ROWS: usize = 8192;
const BATCHES: usize = 10;

/// TPC-H Q1 grouping: 4 groups in random order, weighted 25%, 0.6%, 50% and 25%.
fn q1_groups() -> Vec<usize> {
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    (0..ROWS)
        .map(|_| {
            x = x
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            match (x >> 33) % 1000 {
                0..=249 => 0,
                250..=255 => 1,
                256..=755 => 2,
                _ => 3,
            }
        })
        .collect()
}

fn decimals(precision: u8, scale: i8, value: impl Fn(usize) -> Option<i128>) -> ArrayRef {
    Arc::new(
        (0..ROWS)
            .map(value)
            .collect::<Decimal128Array>()
            .with_precision_and_scale(precision, scale)
            .unwrap(),
    )
}

/// One input shape: values, group ids, and the (precision, scale) of the input and of the sum.
struct Shape<'a> {
    name: &'a str,
    values: &'a ArrayRef,
    groups: &'a [usize],
    num_groups: usize,
    types: ((u8, i8), (u8, i8)),
}

impl<'a> Shape<'a> {
    fn new(
        name: &'a str,
        values: &'a ArrayRef,
        groups: &'a [usize],
        num_groups: usize,
        types: ((u8, i8), (u8, i8)),
    ) -> Self {
        Self {
            name,
            values,
            groups,
            num_groups,
            types,
        }
    }
}

fn bench(
    c: &mut Criterion,
    name: &str,
    make: &dyn Fn() -> Box<dyn GroupsAccumulator>,
    shape: &Shape,
) {
    let values = [Arc::clone(shape.values)];
    c.bench_function(name, |b| {
        b.iter(|| {
            let mut acc = make();
            for _ in 0..BATCHES {
                acc.update_batch(&values, shape.groups, None, shape.num_groups)
                    .unwrap();
            }
            black_box(acc.evaluate(EmitTo::All).unwrap())
        })
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    let q1 = q1_groups();
    let cyclic: Vec<usize> = (0..ROWS).map(|i| i % 1024).collect();
    // A bijection on 0..65536, so each batch touches 8192 scattered groups.
    let scattered: Vec<usize> = (0..ROWS)
        .map(|i| (i as u64 * 2654435761 % 65536) as usize)
        .collect();

    // Q1's `l_quantity`: DECIMAL(15,2) values 1.00..50.00 summed as DECIMAL(25,2).
    let quantity = decimals(15, 2, |i| Some(((i % 50) as i128 + 1) * 100));
    let quantity_nulls = decimals(15, 2, |i| {
        (i % 10 != 0).then(|| ((i % 50) as i128 + 1) * 100)
    });
    // A DECIMAL(38,6) input keeps the sum at the capped precision 38. Values near 1e5 stay far
    // from it. Values of 1e27 to 5e28 have absolute values that sum to 2.1e32 per batch, past
    // the 1e32 limit, so a bound on a batch's total can't skip the per-row check. Each group's
    // rows share one sign, and no running sum passes 3% of the limit, so no group latches.
    let wide = decimals(38, 6, |i| Some(((i % 50) as i128 + 1) * 1_000_000_000));
    let near_limit = decimals(38, 6, |i| {
        let v = ((i % 50) as i128 + 1) * 10_i128.pow(33);
        Some(if i % 2 == 0 { v } else { -v })
    });

    let narrow = ((15, 2), (25, 2));
    let capped = ((38, 6), (38, 6));
    let shapes = [
        Shape::new("(25,2), 4 groups", &quantity, &q1, 4, narrow),
        Shape::new("(25,2), 1024 groups", &quantity, &cyclic, 1024, narrow),
        Shape::new("(25,2), 64K groups", &quantity, &scattered, 65536, narrow),
        Shape::new(
            "(25,2), 1024 groups, 10% nulls",
            &quantity_nulls,
            &cyclic,
            1024,
            narrow,
        ),
        Shape::new("(38,6), 1024 groups", &wide, &cyclic, 1024, capped),
        Shape::new(
            "(38,6), 1024 groups, near limit",
            &near_limit,
            &cyclic,
            1024,
            capped,
        ),
    ];

    for shape in &shapes {
        let ((p, s), (sum_p, sum_s)) = shape.types;
        let input_type = DataType::Decimal128(p, s);
        let sum_type = DataType::Decimal128(sum_p, sum_s);
        // Spark's AVG result type: DECIMAL(min(38, p + 4), min(38, s + 4)).
        let avg_type = DataType::Decimal128((p + 4).min(38), (s + 4).min(38));
        let schema = Schema::new(vec![Field::new("c0", input_type.clone(), true)]);
        let expr_fields = [Arc::new(Field::new("c0", input_type.clone(), true))];
        let args = |result_type: &DataType| AccumulatorArgs {
            return_field: Arc::new(Field::new("r", result_type.clone(), true)),
            schema: &schema,
            ignore_nulls: false,
            order_bys: &[],
            name: "agg",
            is_distinct: false,
            is_reversed: false,
            exprs: &[],
            expr_fields: &expr_fields,
        };

        let sum = SumDecimal::try_new(
            sum_type.clone(),
            EvalMode::Legacy,
            None,
            create_query_context_map(),
        )
        .unwrap();
        bench(
            c,
            &format!("sum {}", shape.name),
            &|| sum.create_groups_accumulator(args(&sum_type)).unwrap(),
            shape,
        );

        let avg = AvgDecimal::new(
            avg_type.clone(),
            sum_type.clone(),
            EvalMode::Legacy,
            None,
            create_query_context_map(),
        );
        bench(
            c,
            &format!("avg {}", shape.name),
            &|| avg.create_groups_accumulator(args(&avg_type)).unwrap(),
            shape,
        );
    }

    // DataFusion's SUM checks no precision per row, so it marks the floor.
    let shape = &shapes[1];
    let sum_type = DataType::Decimal128(25, 2);
    let schema = Schema::new(vec![Field::new("c0", DataType::Decimal128(15, 2), true)]);
    let expr_fields = [Arc::new(Field::new(
        "c0",
        DataType::Decimal128(15, 2),
        true,
    ))];
    let datafusion = sum_udaf();
    bench(
        c,
        "datafusion sum (25,2), 1024 groups",
        &|| {
            datafusion
                .create_groups_accumulator(AccumulatorArgs {
                    return_field: Arc::new(Field::new("r", sum_type.clone(), true)),
                    schema: &schema,
                    ignore_nulls: false,
                    order_bys: &[],
                    name: "sum",
                    is_distinct: false,
                    is_reversed: false,
                    exprs: &[],
                    expr_fields: &expr_fields,
                })
                .unwrap()
        },
        shape,
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
