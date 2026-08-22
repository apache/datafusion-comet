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

use super::SparkArrayExtrema;
use arrow::array::{
    Array, ArrayRef, BinaryArray, DictionaryArray, FixedSizeListArray, Float32Array, Float64Array,
    Int32Array, Int8Array, LargeListArray, LargeListViewArray, ListArray, ListViewArray, NullArray,
    PrimitiveArray, StringArray, StructArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field, Float32Type, Float64Type, Int32Type, Int8Type};
use datafusion::common::{config::ConfigOptions, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl};
use std::cmp::Ordering;
use std::sync::Arc;

fn invoke(input: ColumnarValue, is_min: bool, number_rows: usize) -> ColumnarValue {
    let udf = SparkArrayExtrema::new(is_min);
    let input_type = input.data_type();
    let return_type = udf.return_type(std::slice::from_ref(&input_type)).unwrap();
    udf.invoke_with_args(ScalarFunctionArgs {
        args: vec![input],
        arg_fields: vec![Arc::new(Field::new("input", input_type, true))],
        number_rows,
        return_field: Arc::new(Field::new("result", return_type, true)),
        config_options: Arc::new(ConfigOptions::default()),
    })
    .unwrap()
}

fn extrema(input: &dyn Array, is_min: bool) -> ArrayRef {
    let result = invoke(
        ColumnarValue::Array(input.slice(0, input.len())),
        is_min,
        input.len(),
    );
    let ColumnarValue::Array(result) = result else {
        panic!("array input must produce array output")
    };
    result
}

fn list(values: ArrayRef, offsets: &[i32], validity: Option<Vec<bool>>) -> ListArray {
    ListArray::new(
        Arc::new(Field::new_list_field(values.data_type().clone(), true)),
        OffsetBuffer::new(offsets.to_vec().into()),
        values,
        validity.map(NullBuffer::from),
    )
}

fn large_list(input: &ListArray) -> LargeListArray {
    LargeListArray::new(
        Arc::new(Field::new_list_field(input.value_type(), true)),
        OffsetBuffer::new(input.offsets().iter().map(|&x| i64::from(x)).collect()),
        Arc::clone(input.values()),
        input.nulls().cloned(),
    )
}

/// Arrow's value equality alone does not establish signed-zero or NaN-payload preservation.
fn assert_same_value(actual: &dyn Array, ai: usize, expected: &dyn Array, ei: usize) {
    assert_eq!(actual.data_type(), expected.data_type());
    let actual_null = actual.logical_nulls().is_some_and(|n| n.is_null(ai));
    let expected_null = expected.logical_nulls().is_some_and(|n| n.is_null(ei));
    assert_eq!(actual_null, expected_null);
    if actual_null {
        return;
    }
    match actual.data_type() {
        DataType::Float32 => assert_eq!(
            actual
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(ai)
                .to_bits(),
            expected
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(ei)
                .to_bits(),
        ),
        DataType::Float64 => assert_eq!(
            actual
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(ai)
                .to_bits(),
            expected
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(ei)
                .to_bits(),
        ),
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::FixedSizeList(_, _)
        | DataType::ListView(_)
        | DataType::LargeListView(_) => {
            let value = |array: &dyn Array, index| match array.data_type() {
                DataType::List(_) => array
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .unwrap()
                    .value(index),
                DataType::LargeList(_) => array
                    .as_any()
                    .downcast_ref::<LargeListArray>()
                    .unwrap()
                    .value(index),
                DataType::FixedSizeList(_, _) => array
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .unwrap()
                    .value(index),
                DataType::ListView(_) => array
                    .as_any()
                    .downcast_ref::<ListViewArray>()
                    .unwrap()
                    .value(index),
                _ => array
                    .as_any()
                    .downcast_ref::<LargeListViewArray>()
                    .unwrap()
                    .value(index),
            };
            let actual = value(actual, ai);
            let expected = value(expected, ei);
            assert_eq!(actual.len(), expected.len());
            for i in 0..actual.len() {
                assert_same_value(actual.as_ref(), i, expected.as_ref(), i);
            }
        }
        DataType::Struct(_) => {
            let actual = actual.as_any().downcast_ref::<StructArray>().unwrap();
            let expected = expected.as_any().downcast_ref::<StructArray>().unwrap();
            for (actual, expected) in actual.columns().iter().zip(expected.columns()) {
                assert_same_value(actual.as_ref(), ai, expected.as_ref(), ei);
            }
        }
        DataType::Dictionary(_, _) => {
            let actual = actual
                .as_any()
                .downcast_ref::<DictionaryArray<Int8Type>>()
                .unwrap();
            let expected = expected
                .as_any()
                .downcast_ref::<DictionaryArray<Int8Type>>()
                .unwrap();
            // Taking the original element should preserve both the dictionary key and its value.
            assert_eq!(actual.keys().value(ai), expected.keys().value(ei));
            assert_same_value(
                actual.values().as_ref(),
                actual.key(ai).unwrap(),
                expected.values().as_ref(),
                expected.key(ei).unwrap(),
            );
        }
        _ => assert_eq!(
            ScalarValue::try_from_array(actual, ai).unwrap(),
            ScalarValue::try_from_array(expected, ei).unwrap(),
        ),
    }
}

fn assert_winners(
    input: &dyn Array,
    children: &dyn Array,
    minima: &[Option<usize>],
    maxima: &[Option<usize>],
) {
    for (is_min, expected) in [(true, minima), (false, maxima)] {
        let result = extrema(input, is_min);
        assert_eq!(result.len(), expected.len());
        assert_eq!(result.data_type(), children.data_type());
        for (row, expected) in expected.iter().enumerate() {
            match expected {
                Some(index) => assert_same_value(result.as_ref(), row, children, *index),
                None => assert!(result.logical_nulls().unwrap().is_null(row)),
            }
        }
    }
}

macro_rules! float_tests {
    ($module:ident, $arrow_type:ty, $native:ident, $positive:expr, $negative:expr, $signaling:expr, $negative_signaling:expr) => {
        mod $module {
            use super::*;

            type Row = Option<Vec<Option<$native>>>;

            fn nans() -> [$native; 4] {
                [
                    $native::from_bits($positive),
                    $native::from_bits($negative),
                    $native::from_bits($signaling),
                    $native::from_bits($negative_signaling),
                ]
            }

            // Independent stable-sort oracle, not the production scan or DataFusion extrema.
            // Spark 3.5/4.0 SQLOrderingUtil compares all NaNs equal and greater than numbers;
            // its x == y check makes the two zeros equal. ArrayMin/ArrayMax replace only on a
            // strict comparison, so a stable sort in either direction must keep the first tie.
            fn reference(row: &Row, is_min: bool) -> Option<$native> {
                let mut values: Vec<_> = row.as_ref()?.iter().flatten().copied().collect();
                values.sort_by(|a, b| {
                    let order = match (a.is_nan(), b.is_nan()) {
                        (true, true) => Ordering::Equal,
                        (true, false) => Ordering::Greater,
                        (false, true) => Ordering::Less,
                        (false, false) => a.partial_cmp(b).unwrap(),
                    };
                    if is_min {
                        order
                    } else {
                        order.reverse()
                    }
                });
                values.first().copied()
            }

            fn assert_bits(actual: &dyn Array, expected: &[Option<$native>]) {
                let actual = actual
                    .as_any()
                    .downcast_ref::<PrimitiveArray<$arrow_type>>()
                    .unwrap();
                let actual: Vec<_> = actual.iter().map(|x| x.map($native::to_bits)).collect();
                let expected: Vec<_> = expected.iter().map(|x| x.map($native::to_bits)).collect();
                assert_eq!(actual, expected);
            }

            fn check_rows(rows: Vec<Row>) {
                let input = ListArray::from_iter_primitive::<$arrow_type, _, _>(rows.clone());
                if rows.iter().flatten().flatten().all(Option::is_some) {
                    assert!(input.values().nulls().is_none());
                }
                for is_min in [true, false] {
                    let expected: Vec<_> = rows.iter().map(|row| reference(row, is_min)).collect();
                    assert_bits(extrema(&input, is_min).as_ref(), &expected);
                    assert_bits(extrema(&large_list(&input), is_min).as_ref(), &expected);
                }
            }

            #[test]
            fn explicit_zero_nan_infinity_subnormal_and_null_cases() {
                let [positive, negative, signaling, negative_signaling] = nans();
                let subnormal = $native::from_bits(1);
                let cases: Vec<(Row, Option<$native>, Option<$native>)> = vec![
                    (Some(vec![Some(0.0), Some(-0.0)]), Some(0.0), Some(0.0)),
                    (Some(vec![Some(-0.0), Some(0.0)]), Some(-0.0), Some(-0.0)),
                    (
                        Some(vec![None, Some(0.0), None, Some(-0.0)]),
                        Some(0.0),
                        Some(0.0),
                    ),
                    (
                        Some(vec![None, Some(-0.0), None, Some(0.0)]),
                        Some(-0.0),
                        Some(-0.0),
                    ),
                    (
                        Some(vec![Some(positive), Some(negative), Some(signaling)]),
                        Some(positive),
                        Some(positive),
                    ),
                    (
                        Some(vec![Some(negative), Some(signaling), Some(positive)]),
                        Some(negative),
                        Some(negative),
                    ),
                    (
                        Some(vec![Some(signaling), Some(negative)]),
                        Some(signaling),
                        Some(signaling),
                    ),
                    (
                        Some(vec![Some(negative_signaling), Some(positive)]),
                        Some(negative_signaling),
                        Some(negative_signaling),
                    ),
                    (
                        Some(vec![
                            Some(negative),
                            Some($native::INFINITY),
                            Some($native::NEG_INFINITY),
                        ]),
                        Some($native::NEG_INFINITY),
                        Some(negative),
                    ),
                    (
                        Some(vec![
                            None,
                            Some(signaling),
                            Some(-subnormal),
                            Some(-0.0),
                            Some(subnormal),
                        ]),
                        Some(-subnormal),
                        Some(signaling),
                    ),
                    (
                        Some(vec![Some(subnormal), Some(0.0), Some(-0.0)]),
                        Some(0.0),
                        Some(subnormal),
                    ),
                    (
                        Some(vec![Some(-subnormal), Some(-0.0), Some(0.0)]),
                        Some(-subnormal),
                        Some(-0.0),
                    ),
                    (
                        Some(vec![Some(3.0), None, Some(-2.0), Some(3.0)]),
                        Some(-2.0),
                        Some(3.0),
                    ),
                    (None, None, None),
                    (Some(vec![]), None, None),
                    (Some(vec![None, None]), None, None),
                    (Some(vec![None, Some(-0.0)]), Some(-0.0), Some(-0.0)),
                ];
                let rows: Vec<_> = cases.iter().map(|(row, _, _)| row.clone()).collect();
                let input = ListArray::from_iter_primitive::<$arrow_type, _, _>(rows.clone());
                for is_min in [true, false] {
                    let expected: Vec<_> = cases
                        .iter()
                        .map(|(_, min, max)| if is_min { *min } else { *max })
                        .collect();
                    assert_bits(extrema(&input, is_min).as_ref(), &expected);
                }
                check_rows(rows);
            }

            #[test]
            fn all_special_value_triples_match_stable_spark_ordering() {
                let [positive, negative, signaling, negative_signaling] = nans();
                let values = [
                    None,
                    Some(0.0),
                    Some(-0.0),
                    Some(positive),
                    Some(negative),
                    Some(signaling),
                    Some(negative_signaling),
                    Some($native::INFINITY),
                    Some($native::NEG_INFINITY),
                    Some($native::from_bits(1)),
                    Some(-$native::from_bits(1)),
                    Some(1.0),
                    Some(-1.0),
                ];
                let mut rows = Vec::new();
                for a in values {
                    for b in values {
                        for c in values {
                            rows.push(Some(vec![a, b, c]));
                        }
                    }
                }
                // Repeat every non-null triple in long arrays as well, so future
                // chunked reductions cannot change tie selection or special-value ordering.
                let long_rows = rows
                    .iter()
                    .flatten()
                    .filter(|values| values.iter().all(Option::is_some))
                    .map(|values| Some(values.iter().copied().cycle().take(67).collect()))
                    .collect();
                check_rows(long_rows);
                check_rows(rows);
            }

            #[test]
            fn long_null_free_batches_handle_short_rows_and_slices() {
                let [positive, negative, signaling, _] = nans();
                let mut long = vec![Some(1.0); 1024];
                long[7] = Some(-0.0);
                long[8] = Some(0.0);
                let rows = vec![
                    Some(vec![Some(123.0); 128]),
                    None,
                    Some(vec![]),
                    Some(vec![Some(-0.0)]),
                    Some(vec![Some(-0.0), Some(0.0)]),
                    Some(vec![Some(negative), Some(signaling), Some(positive)]),
                    Some(long),
                ];
                let input = ListArray::from_iter_primitive::<$arrow_type, _, _>(rows.clone());
                assert!(input.values().nulls().is_none());
                for is_min in [true, false] {
                    let expected: Vec<_> = rows.iter().map(|row| reference(row, is_min)).collect();
                    assert_bits(extrema(&input, is_min).as_ref(), &expected);
                    assert_bits(
                        extrema(&input.slice(1, rows.len() - 1), is_min).as_ref(),
                        &expected[1..],
                    );
                    assert_bits(
                        extrema(&large_list(&input).slice(1, rows.len() - 1), is_min).as_ref(),
                        &expected[1..],
                    );
                }
            }

            #[test]
            fn first_ties_survive_thresholds_and_lane_positions() {
                let [positive, negative, signaling, _] = nans();
                for len in [31, 32, 33, 63, 64, 65, 66, 67, 257, 4096] {
                    let mut positions = vec![0, 7, 8, 15, 16, 31, 32, 63, 64, 65, 66, len - 1];
                    positions.retain(|&p| p < len);
                    positions.sort_unstable();
                    positions.dedup();
                    let mut rows = vec![Some(vec![None; len])];
                    for first_nan in [positive, negative, signaling] {
                        let mut row = vec![Some(positive); len];
                        row[0] = Some(first_nan);
                        rows.push(Some(row));
                    }
                    for &first in &positions {
                        for &second in positions.iter().filter(|&&p| p > first) {
                            for zero in [0.0, -0.0] {
                                for background in [1.0, -1.0] {
                                    let mut row = vec![Some(background); len];
                                    row[first] = Some(zero);
                                    row[second] = Some(-zero);
                                    rows.push(Some(row.clone()));
                                    for (i, value) in row.iter_mut().enumerate() {
                                        if i % 3 == 0 && i != first && i != second {
                                            *value = None;
                                        }
                                    }
                                    rows.push(Some(row));
                                }
                            }
                            let mut row = vec![Some(1.0); len];
                            row[first] = Some(negative);
                            row[second] = Some(positive);
                            rows.push(Some(row));
                            let mut row = vec![None; len];
                            row[first] = Some(signaling);
                            row[second] = Some(negative);
                            rows.push(Some(row));
                        }
                    }
                    // Also exercise child buffers without a null bitmap; mixing nullable
                    // and non-null rows in one batch does not establish that coverage.
                    let non_null_rows = rows
                        .iter()
                        .filter(|row| {
                            row.as_ref()
                                .is_some_and(|values| values.iter().all(Option::is_some))
                        })
                        .cloned()
                        .collect();
                    check_rows(non_null_rows);
                    check_rows(rows);
                }
            }

            #[test]
            fn sliced_children_outer_validity_and_hidden_null_children() {
                let child = PrimitiveArray::<$arrow_type>::from_iter([
                    Some(-999.0),
                    Some(999.0),
                    Some(0.0),
                    Some(-0.0),
                    Some($native::NEG_INFINITY),
                    Some(nans()[0]),
                    None,
                    None,
                    Some(-0.0),
                    Some(0.0),
                    Some(999.0),
                ])
                .slice(1, 9);
                let input = list(
                    Arc::new(child),
                    &[1, 3, 5, 5, 7, 9],
                    Some(vec![true, false, true, true, true]),
                );
                for is_min in [true, false] {
                    assert_bits(
                        extrema(&input, is_min).as_ref(),
                        &[Some(0.0), None, None, None, Some(-0.0)],
                    );
                    let sliced = input.slice(1, 4);
                    assert_bits(
                        extrema(&sliced, is_min).as_ref(),
                        &[None, None, None, Some(-0.0)],
                    );
                    assert_bits(
                        extrema(&large_list(&input).slice(1, 4), is_min).as_ref(),
                        &[None, None, None, Some(-0.0)],
                    );
                    let empty = input.slice(0, 0);
                    assert_bits(extrema(&empty, is_min).as_ref(), &[]);
                    assert_bits(extrema(&large_list(&empty), is_min).as_ref(), &[]);
                }
            }

            #[test]
            fn scalar_input_returns_scalar_with_original_bits() {
                let rows = vec![
                    Some(vec![Some(-0.0), Some(0.0)]),
                    Some(vec![Some(nans()[2]), Some(nans()[1])]),
                    None,
                    Some(vec![]),
                    Some(vec![None]),
                ];
                let input = ListArray::from_iter_primitive::<$arrow_type, _, _>(rows.clone());
                let large_input = large_list(&input);
                for input in [&input as &dyn Array, &large_input] {
                    for (row, values) in rows.iter().enumerate() {
                        for is_min in [true, false] {
                            let scalar = ScalarValue::try_from_array(input, row).unwrap();
                            let result = invoke(ColumnarValue::Scalar(scalar), is_min, 17);
                            let ColumnarValue::Scalar(result) = result else {
                                panic!("scalar input must produce scalar output")
                            };
                            assert_bits(
                                result.to_array_of_size(1).unwrap().as_ref(),
                                &[reference(values, is_min)],
                            );
                        }
                    }
                }
            }

            #[test]
            fn dictionary_float_children_preserve_keys_bits_and_logical_nulls() {
                let [positive, negative, signaling, _] = nans();
                let values: ArrayRef = Arc::new(PrimitiveArray::<$arrow_type>::from_iter([
                    Some(0.0),
                    Some(-0.0),
                    Some(positive),
                    Some(negative),
                    Some(signaling),
                    None,
                    Some($native::NEG_INFINITY),
                    Some($native::INFINITY),
                ]));
                let keys = Int8Array::from(vec![
                    Some(7), // Excluded by the child slice.
                    Some(0),
                    Some(1),
                    Some(1),
                    Some(0),
                    Some(2),
                    Some(3),
                    Some(4),
                    Some(5),
                    None,
                    Some(4),
                    Some(5),
                    None,
                    Some(3),
                    Some(7),
                    Some(6),
                    Some(6),
                    Some(7),
                ]);
                let dictionary =
                    DictionaryArray::<Int8Type>::new(keys, Arc::clone(&values)).slice(1, 17);
                let input = list(
                    Arc::new(dictionary),
                    &[0, 2, 4, 7, 10, 12, 15, 17, 17],
                    Some(vec![true, true, true, true, true, true, false, true]),
                );
                assert_winners(
                    &input,
                    input.values().as_ref(),
                    &[
                        Some(0),
                        Some(2),
                        Some(4),
                        Some(9),
                        None,
                        Some(14),
                        None,
                        None,
                    ],
                    &[
                        Some(0),
                        Some(2),
                        Some(4),
                        Some(9),
                        None,
                        Some(12),
                        None,
                        None,
                    ],
                );

                // A valid dictionary key referring to a null value is null during nested
                // comparison too: [null] sorts before [-infinity], not after it.
                let dictionary = DictionaryArray::<Int8Type>::new(
                    Int8Array::from(vec![5, 6, 0, 2, 1, 3]),
                    values,
                );
                let inner = list(Arc::new(dictionary), &[0, 1, 2, 4, 6], None);
                let outer = list(Arc::new(inner), &[0, 2, 4], None);
                assert_winners(
                    &outer,
                    outer.values().as_ref(),
                    &[Some(0), Some(2)],
                    &[Some(1), Some(2)],
                );
            }
        }
    };
}

float_tests!(
    float32,
    Float32Type,
    f32,
    0x7fc0_0001,
    0xffc0_0002,
    0x7f80_0001,
    0xff80_0002
);
float_tests!(
    float64,
    Float64Type,
    f64,
    0x7ff8_0000_0000_0001,
    0xfff8_0000_0000_0002,
    0x7ff0_0000_0000_0001,
    0xfff0_0000_0000_0002
);

#[test]
fn nested_lists_keep_first_equal_bits_and_use_nulls_first() {
    let positive = f64::from_bits(0x7ff8_0000_0000_0001);
    let negative = f64::from_bits(0xfff8_0000_0000_0002);
    let signaling = f64::from_bits(0x7ff0_0000_0000_0001);
    let inner = ListArray::from_iter_primitive::<Float64Type, _, _>([
        Some(vec![Some(0.0), Some(positive), None]),
        Some(vec![Some(-0.0), Some(negative), None]),
        Some(vec![Some(-0.0), Some(negative)]),
        Some(vec![Some(0.0), Some(signaling)]),
        Some(vec![None, Some(f64::INFINITY)]),
        Some(vec![Some(f64::NEG_INFINITY), None]),
        Some(vec![]),
        Some(vec![None]),
        None,
        Some(vec![Some(-0.0)]),
        Some(vec![Some(0.0)]),
        Some(vec![Some(-0.0), None]),
        Some(vec![Some(negative)]),
        Some(vec![Some(f64::INFINITY)]),
        None,
        None,
    ]);
    let minima = [
        Some(0),
        Some(2),
        Some(4),
        Some(6),
        Some(9),
        Some(10),
        Some(13),
        None,
    ];
    let maxima = [
        Some(0),
        Some(2),
        Some(5),
        Some(7),
        Some(9),
        Some(11),
        Some(12),
        None,
    ];
    for inner in [
        Arc::new(inner.clone()) as ArrayRef,
        Arc::new(large_list(&inner)),
    ] {
        let input = list(inner, &[0, 2, 4, 6, 8, 10, 12, 14, 16], None);
        assert_winners(&input, input.values().as_ref(), &minima, &maxima);
        assert_winners(
            &large_list(&input),
            input.values().as_ref(),
            &minima,
            &maxima,
        );
        assert_winners(&input.slice(0, 0), input.values().as_ref(), &[], &[]);
    }
}

#[test]
fn nested_slices_and_null_parents_keep_original_children() {
    let positive = f32::from_bits(0x7fc0_0001);
    let negative = f32::from_bits(0xffc0_0002);
    let inner = ListArray::from_iter_primitive::<Float32Type, _, _>([
        Some(vec![Some(999.0)]),
        Some(vec![Some(0.0), Some(positive)]),
        Some(vec![Some(-0.0), Some(negative)]),
        Some(vec![Some(f32::NEG_INFINITY)]),
        Some(vec![Some(f32::INFINITY)]),
        Some(vec![None]),
        Some(vec![Some(-0.0)]),
    ])
    .slice(1, 6);
    let input = list(
        Arc::new(inner),
        &[0, 2, 4, 6],
        Some(vec![true, false, true]),
    );
    assert_winners(
        &input,
        input.values().as_ref(),
        &[Some(0), None, Some(4)],
        &[Some(0), None, Some(5)],
    );
    let sliced = input.slice(1, 2);
    assert_winners(
        &sliced,
        sliced.values().as_ref(),
        &[None, Some(4)],
        &[None, Some(5)],
    );
}

#[test]
fn structs_compare_later_fields_and_preserve_original_nested_payloads() {
    let positive32 = f32::from_bits(0x7fc0_0001);
    let negative32 = f32::from_bits(0xffc0_0002);
    let positive64 = f64::from_bits(0x7ff0_0000_0000_0001);
    let negative64 = f64::from_bits(0xfff8_0000_0000_0002);
    let floats: ArrayRef = Arc::new(Float32Array::from(vec![
        Some(0.0),
        Some(-0.0),
        Some(-0.0),
        Some(0.0),
        Some(0.0),
        Some(-0.0),
        Some(0.0),
        Some(-0.0),
        None,
        Some(f32::NEG_INFINITY),
        Some(positive32),
        Some(negative32),
        Some(f32::NEG_INFINITY),
        Some(-0.0),
        Some(0.0),
        Some(-0.0),
    ]));
    let ints: ArrayRef = Arc::new(Int32Array::from(vec![
        1, 1, 2, 1, 1, 1, 1, 1, 100, -100, 1, 1, 1, 1, 1, 1,
    ]));
    let strings: ArrayRef = Arc::new(StringArray::from(vec![
        "a", "a", "a", "a", "z", "a", "a", "a", "z", "a", "a", "a", "a", "a", "a", "a",
    ]));
    let binaries: ArrayRef = Arc::new(BinaryArray::from(vec![
        b"a".as_slice(),
        b"a",
        b"a",
        b"a",
        b"a",
        b"z",
        b"\x80",
        b"\x7f",
        b"a",
        b"a",
        b"a",
        b"a",
        b"a",
        b"a",
        b"a",
        b"a",
    ]));
    let mut tails = vec![Some(vec![Some(0.0)]); 16];
    tails[0] = Some(vec![Some(positive64), Some(-0.0)]);
    tails[1] = Some(vec![Some(negative64), Some(0.0)]);
    tails[10] = Some(vec![None]);
    tails[11] = Some(vec![Some(f64::NEG_INFINITY)]);
    let tails: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>(tails));
    let columns = vec![floats, ints, strings, binaries, tails];
    let fields: Vec<_> = columns
        .iter()
        .enumerate()
        .map(|(i, array)| {
            Arc::new(Field::new(
                format!("field_{i}"),
                array.data_type().clone(),
                true,
            ))
        })
        .collect();
    let mut validity = vec![true; 16];
    for i in [12, 14, 15] {
        validity[i] = false;
    }
    let structs = StructArray::new(fields.into(), columns, Some(NullBuffer::from(validity)));
    let input = list(Arc::new(structs), &[0, 2, 4, 6, 8, 10, 12, 14, 16], None);
    assert_winners(
        &input,
        input.values().as_ref(),
        &[
            Some(0),
            Some(3),
            Some(5),
            Some(7),
            Some(8),
            Some(10),
            Some(13),
            None,
        ],
        &[
            Some(0),
            Some(2),
            Some(4),
            Some(6),
            Some(9),
            Some(11),
            Some(13),
            None,
        ],
    );
    assert_winners(&input.slice(0, 0), input.values().as_ref(), &[], &[]);
    for is_min in [true, false] {
        let scalar = ScalarValue::try_from_array(&input, 0).unwrap();
        let ColumnarValue::Scalar(result) = invoke(ColumnarValue::Scalar(scalar), is_min, 1) else {
            panic!("nested scalar input must produce scalar output")
        };
        assert_same_value(
            result.to_array_of_size(1).unwrap().as_ref(),
            0,
            input.values().as_ref(),
            0,
        );
    }
}

#[test]
fn non_floating_integer_and_nested_null_order_controls() {
    let input = ListArray::from_iter_primitive::<Int32Type, _, _>([
        Some(vec![Some(3), None, Some(-2), Some(3)]),
        Some(vec![]),
        Some(vec![None]),
        None,
        Some((0..64).map(|i| Some(i - 32)).collect()),
    ]);
    for (is_min, expected) in [
        (true, vec![Some(-2), None, None, None, Some(-32)]),
        (false, vec![Some(3), None, None, None, Some(31)]),
    ] {
        let result = extrema(&input, is_min);
        assert_eq!(
            result.as_any().downcast_ref::<Int32Array>().unwrap(),
            &Int32Array::from(expected)
        );
    }
    let children = ListArray::from_iter_primitive::<Int32Type, _, _>([
        Some(vec![None]),
        Some(vec![Some(-1)]),
        Some(vec![]),
        Some(vec![None]),
    ]);
    let nested = list(Arc::new(children), &[0, 2, 4], None);
    assert_winners(
        &nested,
        nested.values().as_ref(),
        &[Some(0), Some(2)],
        &[Some(1), Some(3)],
    );
    let nulls = list(Arc::new(NullArray::new(3)), &[0, 3, 3], None);
    for is_min in [true, false] {
        let result = extrema(&nulls, is_min);
        assert_eq!(result.data_type(), &DataType::Null);
        assert_eq!(result.len(), 2);
        // NullArray has no physical null bitmap; every element is logically null.
        assert_eq!(result.logical_null_count(), 2);
    }
}

#[test]
fn empty_non_primitive_batches_retain_element_type() {
    let children: [ArrayRef; 2] = [
        Arc::new(StringArray::from(Vec::<&str>::new())),
        Arc::new(BinaryArray::from(Vec::<&[u8]>::new())),
    ];
    for children in children {
        let input = list(children, &[0], None);
        assert_winners(&input, input.values().as_ref(), &[], &[]);
        assert_winners(&large_list(&input), input.values().as_ref(), &[], &[]);
    }
}

#[test]
fn sliced_fixed_size_list_children_preserve_ties_and_null_order() {
    let positive = f64::from_bits(0x7ff8_0000_0000_0001);
    let negative = f64::from_bits(0xfff0_0000_0000_0002);
    let field = Arc::new(Field::new_list_field(DataType::Float64, true));
    let values: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(999.0),
        Some(999.0),
        Some(0.0),
        Some(positive),
        Some(-0.0),
        Some(negative),
        None,
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
        None,
        Some(0.0),
        Some(0.0),
        Some(-0.0),
        Some(0.0),
    ]));
    let children = FixedSizeListArray::new(
        Arc::clone(&field),
        2,
        values,
        Some(NullBuffer::from(vec![
            true, true, true, true, true, false, true,
        ])),
    )
    .slice(1, 6);
    let input = list(Arc::new(children), &[0, 2, 4, 6], None);
    assert_winners(
        &input,
        input.values().as_ref(),
        &[Some(0), Some(2), Some(5)],
        &[Some(0), Some(3), Some(5)],
    );
    assert_winners(
        &input.slice(1, 2),
        input.values().as_ref(),
        &[Some(2), Some(5)],
        &[Some(3), Some(5)],
    );

    let empty_children = FixedSizeListArray::new(
        field,
        0,
        Arc::new(Float64Array::from(Vec::<f64>::new())),
        Some(NullBuffer::from(vec![true, false, true])),
    );
    let input = list(Arc::new(empty_children), &[0, 2, 3], None);
    assert_winners(
        &input,
        input.values().as_ref(),
        &[Some(0), Some(2)],
        &[Some(0), Some(2)],
    );
}

#[test]
fn sliced_list_view_children_support_nonmonotone_offsets() {
    let positive = f64::from_bits(0x7ff8_0000_0000_0001);
    let negative = f64::from_bits(0xfff0_0000_0000_0002);
    let field = Arc::new(Field::new_list_field(DataType::Float64, true));
    let values: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(0.0),
        Some(positive),
        Some(-0.0),
        Some(negative),
        None,
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
        None,
    ]));
    let validity = Some(NullBuffer::from(vec![
        true, true, true, true, true, true, true, false, true,
    ]));
    let children = ListViewArray::new(
        Arc::clone(&field),
        vec![8, 2, 0, 4, 6, 0, 0, 0, 0].into(),
        vec![0, 2, 2, 2, 2, 0, 1, 2, 2].into(),
        Arc::clone(&values),
        validity.clone(),
    )
    .slice(1, 8);
    let large_children = LargeListViewArray::new(
        field,
        vec![8, 2, 0, 4, 6, 0, 0, 0, 0].into(),
        vec![0, 2, 2, 2, 2, 0, 1, 2, 2].into(),
        values,
        validity,
    )
    .slice(1, 8);
    for children in [Arc::new(children) as ArrayRef, Arc::new(large_children)] {
        let input = list(children, &[0, 2, 4, 6, 8], None);
        assert_winners(
            &input,
            input.values().as_ref(),
            &[Some(0), Some(2), Some(4), Some(7)],
            &[Some(0), Some(3), Some(5), Some(7)],
        );
        assert_winners(
            &large_list(&input).slice(1, 3),
            input.values().as_ref(),
            &[Some(2), Some(4), Some(7)],
            &[Some(3), Some(5), Some(7)],
        );
    }
}

#[test]
fn return_field_is_nullable_and_retains_nested_field_metadata() {
    let field = Field::new("value", DataType::Float64, false)
        .with_metadata([(String::from("source"), String::from("extrema-test"))].into());
    let element_type = DataType::Struct(vec![field].into());
    let element_field = Arc::new(Field::new_list_field(element_type.clone(), false));
    for input_type in [
        DataType::List(Arc::clone(&element_field)),
        DataType::LargeList(element_field),
    ] {
        let args = [Arc::new(Field::new("input", input_type, false))];
        for is_min in [true, false] {
            let udf = SparkArrayExtrema::new(is_min);
            assert_eq!(udf.name(), if is_min { "array_min" } else { "array_max" });
            let result = udf
                .return_field_from_args(ReturnFieldArgs {
                    arg_fields: &args,
                    scalar_arguments: &[None],
                })
                .unwrap();
            // Even a non-null input can be empty, so the result must remain nullable.
            assert!(result.is_nullable());
            assert_eq!(result.data_type(), &element_type);
        }
    }
}
