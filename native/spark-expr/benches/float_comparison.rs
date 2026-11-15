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

//! Compare Spark's float comparisons, which follow Spark's SQL ordering without normalizing their
//! operands, with comparing normalized copies of the operands and with DataFusion's comparison, on
//! the same data, and time `normalize_floats` on its own. All three return the same answer except
//! that DataFusion's differs on a NaN with the sign bit set, which is checked before timing;
//! special-value semantics belong in tests.

use arrow::array::{Array, ArrayRef, Float64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
use datafusion::physical_expr::PhysicalExpr;
use datafusion_comet_spark_expr::{
    normalize_floats, spark_comparison, FloatOperands, NormalizeNaNAndZero,
};
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

const ROWS: usize = 8192;

/// A NaN with the sign bit set, which arithmetic produces on x86-64.
const NEGATIVE_NAN: f64 = f64::from_bits(0xfff8_0000_0000_0000);

/// Two columns of doubles, with `special` in every tenth row of each if given.
fn batch(special: Option<f64>) -> RecordBatch {
    let column = |seed: usize| -> ArrayRef {
        Arc::new(Float64Array::from_iter_values((0..ROWS).map(
            |i| match special {
                Some(value) if i % 10 == seed => value,
                _ => ((i * 7 + seed) % 1000) as f64 * 1.5,
            },
        )))
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Float64, true),
        Field::new("b", DataType::Float64, true),
    ]));
    RecordBatch::try_new(schema, vec![column(1), column(3)]).unwrap()
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("float_comparison");
    group.sample_size(20);
    group.warm_up_time(Duration::from_millis(250));
    group.measurement_time(Duration::from_secs(1));
    for (data, special) in [
        ("no_special", None),
        ("neg_zero_10pct", Some(-0.0)),
        ("nan_10pct", Some(f64::NAN)),
        ("sign_bit_nan_10pct", Some(NEGATIVE_NAN)),
    ] {
        let batch = batch(special);
        let schema = batch.schema();
        let a: Arc<dyn PhysicalExpr> = Arc::new(Column::new("a", 0));
        let b: Arc<dyn PhysicalExpr> = Arc::new(Column::new("b", 1));
        let literal: Arc<dyn PhysicalExpr> =
            Arc::new(Literal::new(ScalarValue::Float64(Some(500.0))));
        for (op_name, op) in [
            ("lt", Operator::Lt),
            ("eq", Operator::Eq),
            ("not_distinct", Operator::IsNotDistinctFrom),
        ] {
            for (shape, right) in [("column_column", &b), ("column_literal", &literal)] {
                let datafusion: Arc<dyn PhysicalExpr> =
                    Arc::new(BinaryExpr::new(Arc::clone(&a), op, Arc::clone(right)));
                // Comparing normalized copies of the operands, as `spark_comparison` did before it
                // compared them in place. It normalized a literal while planning, and 500.0 is
                // already normal.
                let normalize = |operand: &Arc<dyn PhysicalExpr>| -> Arc<dyn PhysicalExpr> {
                    if operand.downcast_ref::<Literal>().is_some() {
                        Arc::clone(operand)
                    } else {
                        NormalizeNaNAndZero::wrap_if_needed(Arc::clone(operand), &schema).unwrap()
                    }
                };
                let normalized: Arc<dyn PhysicalExpr> =
                    Arc::new(BinaryExpr::new(normalize(&a), op, normalize(right)));
                let spark = spark_comparison(
                    Arc::clone(&a),
                    op,
                    Arc::clone(right),
                    &schema,
                    FloatOperands::Normalize,
                )
                .unwrap();
                let evaluate = |expr: &Arc<dyn PhysicalExpr>| {
                    expr.evaluate(&batch)
                        .unwrap()
                        .into_array(ROWS)
                        .unwrap()
                        .to_data()
                };
                assert_eq!(evaluate(&spark), evaluate(&normalized));
                if special.map(f64::to_bits) != Some(NEGATIVE_NAN.to_bits()) {
                    assert_eq!(evaluate(&spark), evaluate(&datafusion));
                }
                for (engine, expr) in [
                    ("spark", &spark),
                    ("normalized", &normalized),
                    ("datafusion", &datafusion),
                ] {
                    group.bench_function(
                        BenchmarkId::new(format!("{op_name}_{shape}_{engine}"), data),
                        |b| b.iter(|| black_box(expr.evaluate(black_box(&batch)).unwrap())),
                    );
                }
            }
        }
        let column = Arc::clone(batch.column(0));
        group.bench_function(BenchmarkId::new("normalize_floats", data), |b| {
            b.iter(|| black_box(normalize_floats(black_box(&column))))
        });
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
