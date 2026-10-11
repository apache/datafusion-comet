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

//! Compare Comet's float `sort_array` and `array_remove` with DataFusion's `array_sort` and
//! `array_remove_all` on doubles without zeros or NaNs, where both return the same arrays, which
//! is checked before timing; special-value semantics belong in tests.

use arrow::array::{Array, ArrayRef, Float64Array, ListArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::common::config::ConfigOptions;
use datafusion::common::ScalarValue;
use datafusion::functions_nested::remove::array_remove_all_udf;
use datafusion::functions_nested::sort::array_sort_udf;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDF};
use datafusion_comet_spark_expr::{SparkArrayRemove, SparkFloatArrayContains, SparkSortArray};
use datafusion_spark::function::array::array_contains::SparkArrayContains;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

const ROWS: usize = 8192;

/// The value of element `i`. Values repeat so that `array_remove` finds something to remove.
fn value(i: usize) -> f64 {
    ((i * 7919) % 101) as f64 * 0.5 + 1.0
}

/// `ROWS` lists of `len` doubles, with every tenth list and every seventh element null if
/// `nulls`.
fn lists(len: usize, nulls: bool) -> ArrayRef {
    let values = if nulls {
        Float64Array::from_iter((0..ROWS * len).map(|i| (i % 7 != 0).then(|| value(i))))
    } else {
        Float64Array::from_iter_values((0..ROWS * len).map(value))
    };
    Arc::new(ListArray::new(
        Arc::new(Field::new_list_field(DataType::Float64, true)),
        OffsetBuffer::from_lengths(std::iter::repeat_n(len, ROWS)),
        Arc::new(values),
        nulls.then(|| NullBuffer::from_iter((0..ROWS).map(|row| row % 10 != 0))),
    ))
}

fn args(args: Vec<ColumnarValue>, list: &ArrayRef) -> ScalarFunctionArgs {
    ScalarFunctionArgs {
        arg_fields: args
            .iter()
            .map(|arg| Arc::new(Field::new("arg", arg.data_type(), true)))
            .collect(),
        args,
        number_rows: ROWS,
        return_field: Arc::new(Field::new("result", list.data_type().clone(), true)),
        config_options: Arc::new(ConfigOptions::default()),
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("float_arrays");
    group.sample_size(20);
    group.warm_up_time(Duration::from_millis(250));
    group.measurement_time(Duration::from_secs(1));
    let comet_sort = ScalarUDF::new_from_impl(SparkSortArray::default());
    let comet_remove = ScalarUDF::new_from_impl(SparkArrayRemove::default());
    let comet_contains = ScalarUDF::new_from_impl(SparkFloatArrayContains::default());
    // The native path other element types take: array_has plus Spark's null semantics.
    let datafusion_contains = Arc::new(ScalarUDF::new_from_impl(SparkArrayContains::default()));
    let (datafusion_sort, datafusion_remove) = (array_sort_udf(), array_remove_all_udf());
    for len in [8, 50] {
        for nulls in [false, true] {
            let list = lists(len, nulls);
            let data = format!("len={len}_null={nulls}");
            let column = ColumnarValue::Array(Arc::clone(&list));
            let scalar = |value: ScalarValue| ColumnarValue::Scalar(value);
            let comet_sort_args = args(
                vec![
                    column.clone(),
                    scalar(ScalarValue::Boolean(Some(true))),
                    scalar(ScalarValue::Boolean(Some(false))),
                ],
                &list,
            );
            let datafusion_sort_args = args(
                vec![
                    column.clone(),
                    scalar(ScalarValue::from("ASC")),
                    scalar(ScalarValue::from("NULLS FIRST")),
                ],
                &list,
            );
            let remove_args = args(
                vec![column.clone(), scalar(ScalarValue::Float64(Some(26.0)))],
                &list,
            );
            // A value per row: the row's second element.
            let row_values =
                Float64Array::from_iter_values((0..ROWS).map(|row| value(row * len + 1)));
            let remove_column_args = args(
                vec![column.clone(), ColumnarValue::Array(Arc::new(row_values))],
                &list,
            );
            let contains_args = args(
                vec![column.clone(), scalar(ScalarValue::Float64(Some(26.0)))],
                &list,
            );
            let row_values =
                Float64Array::from_iter_values((0..ROWS).map(|row| value(row * len + 1)));
            let contains_column_args = args(
                vec![column, ColumnarValue::Array(Arc::new(row_values))],
                &list,
            );
            let cases = [
                (
                    "sort_array",
                    &comet_sort,
                    &datafusion_sort,
                    &comet_sort_args,
                    &datafusion_sort_args,
                ),
                (
                    "array_remove",
                    &comet_remove,
                    &datafusion_remove,
                    &remove_args,
                    &remove_args,
                ),
                (
                    "array_remove_column",
                    &comet_remove,
                    &datafusion_remove,
                    &remove_column_args,
                    &remove_column_args,
                ),
                (
                    "array_contains",
                    &comet_contains,
                    &datafusion_contains,
                    &contains_args,
                    &contains_args,
                ),
                (
                    "array_contains_column",
                    &comet_contains,
                    &datafusion_contains,
                    &contains_column_args,
                    &contains_column_args,
                ),
            ];
            for (op, comet, datafusion, comet_args, datafusion_args) in cases {
                let evaluate = |udf: &ScalarUDF, args: &ScalarFunctionArgs| {
                    udf.invoke_with_args(args.clone())
                        .unwrap()
                        .into_array(ROWS)
                        .unwrap()
                };
                assert_eq!(
                    evaluate(comet, comet_args).to_data(),
                    evaluate(datafusion, datafusion_args).to_data(),
                    "{op} {data}"
                );
                for (engine, udf, udf_args) in [
                    ("comet", comet, comet_args),
                    ("datafusion", datafusion.as_ref(), datafusion_args),
                ] {
                    group.bench_function(BenchmarkId::new(format!("{op}_{engine}"), &data), |b| {
                        b.iter(|| {
                            black_box(udf.invoke_with_args(black_box(udf_args.clone())).unwrap())
                        })
                    });
                }
            }
        }
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
