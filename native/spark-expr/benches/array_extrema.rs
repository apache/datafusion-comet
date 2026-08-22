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

//! Compares the Spark-compatible UDF with the DataFusion version pinned by Cargo.lock.
//! All timed inputs contain ordinary finite, nonzero values. Nested elements contain
//! no inner nulls: mixed zero signs, NaN payloads, and nested null ordering deliberately
//! differ from DataFusion and belong in the correctness tests, not parity benchmarks.
//! The null percentage controls both outer-row and immediate-child validity.
//!
//! Run the whole bounded matrix, or filter (for example) by float64 or nested_float64:
//! cargo bench -p datafusion-comet-spark-expr --bench array_extrema -- float64

use arrow::array::{ArrayRef, Float32Array, Float64Array, Int32Array, ListArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::Field;
use criterion::{
    criterion_group, criterion_main, measurement::WallTime, BenchmarkGroup, BenchmarkId, Criterion,
    Throughput,
};
use datafusion::common::config::ConfigOptions;
use datafusion::functions_nested::min_max::{array_max_udf, array_min_udf};
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDF};
use datafusion_comet_spark_expr::SparkArrayExtrema;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

fn valid(index: usize, null_percent: usize) -> bool {
    (index * 17 + 23) % 100 >= null_percent
}

fn list(values: ArrayRef, rows: usize, len: usize, null_percent: usize) -> ArrayRef {
    let offsets: Vec<i32> = (0..=rows).map(|row| (row * len) as i32).collect();
    let nulls = (null_percent != 0).then(|| {
        NullBuffer::from(
            (0..rows)
                .map(|row| valid(row, null_percent))
                .collect::<Vec<_>>(),
        )
    });
    Arc::new(ListArray::new(
        Arc::new(Field::new_list_field(values.data_type().clone(), true)),
        OffsetBuffer::new(offsets.into()),
        values,
        nulls,
    ))
}

fn finite_value(index: usize) -> i32 {
    ((index * 104_729 + 51) % 1_000_003 + 1) as i32
}

fn primitive_input(kind: &str, rows: usize, len: usize, null_percent: usize) -> ArrayRef {
    let values = (0..rows * len).map(|i| valid(i, null_percent).then(|| finite_value(i)));
    let values: ArrayRef = match kind {
        "float32" => Arc::new(Float32Array::from_iter(
            values.map(|v| v.map(|v| v as f32 / 8.0)),
        )),
        "float64" => Arc::new(Float64Array::from_iter(
            values.map(|v| v.map(|v| f64::from(v) / 8.0)),
        )),
        "int32_control" => Arc::new(Int32Array::from_iter(values)),
        _ => unreachable!(),
    };
    list(values, rows, len, null_percent)
}

fn nested_input(rows: usize, len: usize, null_percent: usize) -> ArrayRef {
    let children = rows * len;
    let values: ArrayRef = Arc::new(Float64Array::from_iter_values(
        (0..children * 4).map(|i| f64::from(finite_value(i)) / 8.0),
    ));
    // Null immediate children are skipped by both implementations; there are no
    // null float values inside a valid child list, so recursive ordering agrees.
    list(
        list(values, children, 4, null_percent),
        rows,
        len,
        null_percent,
    )
}

fn args(input: &ArrayRef, udf: &ScalarUDF) -> ScalarFunctionArgs {
    ScalarFunctionArgs {
        args: vec![ColumnarValue::Array(Arc::clone(input))],
        arg_fields: vec![Arc::new(Field::new(
            "input",
            input.data_type().clone(),
            true,
        ))],
        number_rows: input.len(),
        return_field: Arc::new(Field::new(
            "result",
            udf.return_type(&[input.data_type().clone()]).unwrap(),
            true,
        )),
        config_options: Arc::new(ConfigOptions::default()),
    }
}

fn bench_case(
    group: &mut BenchmarkGroup<'_, WallTime>,
    input: ArrayRef,
    len: usize,
    null_percent: usize,
) {
    let case = format!("rows={}_len={len}_null={null_percent}pct", input.len());
    group.throughput(Throughput::Elements((input.len() * len) as u64));
    for is_min in [true, false] {
        let operation = if is_min { "min" } else { "max" };
        let comet = ScalarUDF::from(SparkArrayExtrema::new(is_min));
        let datafusion = if is_min {
            array_min_udf()
        } else {
            array_max_udf()
        };
        let args = args(&input, &comet);
        // Validate every fixture before timing. This is a parity/control check for
        // ordinary data only, not an oracle for Spark's special-value semantics.
        let comet_result = comet
            .invoke_with_args(args.clone())
            .unwrap()
            .into_array(input.len())
            .unwrap();
        let datafusion_result = datafusion
            .invoke_with_args(args.clone())
            .unwrap()
            .into_array(input.len())
            .unwrap();
        assert_eq!(
            comet_result.to_data(),
            datafusion_result.to_data(),
            "{operation}/{case}"
        );
        for (name, udf) in [("comet", &comet), ("datafusion", datafusion.as_ref())] {
            group.bench_function(
                BenchmarkId::new(format!("{operation}_{name}"), &case),
                |b| b.iter(|| black_box(udf.invoke_with_args(black_box(args.clone())).unwrap())),
            );
        }
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    // 104 cases with one-second measurements and a short warmup: roughly two
    // minutes on an idle machine. CLI filters can select individual dimensions.
    for kind in ["float32", "float64", "int32_control", "nested_float64"] {
        let mut group = c.benchmark_group(format!("array_extrema/{kind}"));
        group.sample_size(20);
        group.warm_up_time(Duration::from_millis(250));
        group.measurement_time(Duration::from_secs(1));
        let (lengths, null_percentages): (&[usize], &[usize]) = match kind {
            "float32" | "float64" => (&[8, 32, 1024], &[0, 10, 50]),
            "int32_control" => (&[8, 1024], &[0, 50]),
            _ => (&[8, 64], &[0, 50]),
        };
        for &len in lengths {
            let rows = (65_536 / len).min(4096);
            for &null_percent in null_percentages {
                let input = if kind == "nested_float64" {
                    nested_input(rows, len, null_percent)
                } else {
                    primitive_input(kind, rows, len, null_percent)
                };
                bench_case(&mut group, input, len, null_percent);
            }
        }
        group.finish();
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
