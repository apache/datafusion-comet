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

use super::{SparkArrayExtrema, Utf8Collation};
use arrow::array::{
    Array, ArrayRef, Float64Array, Int32Array, LargeStringArray, ListArray, PrimitiveArray,
    StringArray, StringViewArray, StructArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field, Float32Type, Float64Type, Int32Type};
use datafusion::common::{config::ConfigOptions, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use std::sync::Arc;

fn invoke(input: ColumnarValue, is_min: bool) -> ColumnarValue {
    invoke_udf(input, &SparkArrayExtrema::new(is_min))
}

fn invoke_udf(input: ColumnarValue, udf: &SparkArrayExtrema) -> ColumnarValue {
    let input_type = input.data_type();
    let return_type = udf.return_type(std::slice::from_ref(&input_type)).unwrap();
    let number_rows = match &input {
        ColumnarValue::Array(array) => array.len(),
        ColumnarValue::Scalar(_) => 1,
    };
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
    let ColumnarValue::Array(result) =
        invoke(ColumnarValue::Array(input.slice(0, input.len())), is_min)
    else {
        panic!("array input must produce array output")
    };
    result
}

fn list(values: ArrayRef, offsets: &[i32]) -> ListArray {
    ListArray::new(
        Arc::new(Field::new_list_field(values.data_type().clone(), true)),
        OffsetBuffer::new(offsets.to_vec().into()),
        values,
        None,
    )
}

fn float64_bits(array: &dyn Array) -> Vec<Option<u64>> {
    array
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .iter()
        .map(|value| value.map(f64::to_bits))
        .collect()
}

macro_rules! float_tests {
    ($name:ident, $arrow_type:ty, $native:ident, $positive:expr, $negative:expr, $signaling:expr) => {
        #[test]
        fn $name() {
            let positive = $native::from_bits($positive);
            let negative = $native::from_bits($negative);
            let signaling = $native::from_bits($signaling);
            let mut long_nan = vec![Some(1.0); 67];
            long_nan[7] = Some(negative);
            long_nan[8] = Some(positive);
            let mut long_min_zero = vec![Some(1.0); 67];
            long_min_zero[7] = Some(-0.0);
            long_min_zero[8] = Some(0.0);
            let mut long_max_zero = vec![Some(-1.0); 67];
            long_max_zero[7] = Some(0.0);
            long_max_zero[8] = Some(-0.0);
            let cases = [
                (Some(vec![Some(0.0), Some(-0.0)]), Some(0.0), Some(0.0)),
                (
                    Some(vec![None, Some(-0.0), Some(0.0)]),
                    Some(-0.0),
                    Some(-0.0),
                ),
                (
                    Some(vec![Some(positive), Some(negative)]),
                    Some(positive),
                    Some(positive),
                ),
                (
                    Some(vec![Some(negative), Some(positive)]),
                    Some(negative),
                    Some(negative),
                ),
                (
                    Some(vec![Some(signaling), Some(negative)]),
                    Some(signaling),
                    Some(signaling),
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
                    Some(vec![Some(3.0), None, Some(-2.0)]),
                    Some(-2.0),
                    Some(3.0),
                ),
                (None, None, None),
                (Some(vec![]), None, None),
                (Some(vec![None, None]), None, None),
                // Exercise long, null-free lists: the old Arrow reduction changed winners.
                (Some(long_nan), Some(1.0), Some(negative)),
                (Some(long_min_zero), Some(-0.0), Some(1.0)),
                (Some(long_max_zero), Some(-1.0), Some(0.0)),
            ];
            for (row, min, max) in cases {
                let input = ListArray::from_iter_primitive::<$arrow_type, _, _>([row]);
                for (is_min, expected) in [(true, min), (false, max)] {
                    let result = extrema(&input, is_min);
                    let result = result
                        .as_any()
                        .downcast_ref::<PrimitiveArray<$arrow_type>>()
                        .unwrap();
                    assert_eq!(
                        result.iter().next().unwrap().map($native::to_bits),
                        expected.map($native::to_bits),
                    );
                }
            }
        }
    };
}

float_tests!(
    float32_preserves_first_winner,
    Float32Type,
    f32,
    0x7fc0_0001,
    0xffc0_0002,
    0x7f80_0001
);
float_tests!(
    float64_preserves_first_winner,
    Float64Type,
    f64,
    0x7ff8_0000_0000_0001,
    0xfff8_0000_0000_0002,
    0x7ff0_0000_0000_0001
);

#[test]
fn scalar_and_sliced_float_input() {
    let input = ListArray::from_iter_primitive::<Float64Type, _, _>([
        Some(vec![Some(99.0)]),
        Some(vec![Some(-0.0), None, Some(0.0)]),
        None,
    ])
    .slice(1, 2);
    for is_min in [true, false] {
        assert_eq!(
            float64_bits(extrema(&input, is_min).as_ref()),
            vec![Some((-0.0f64).to_bits()), None]
        );
        let scalar = ScalarValue::try_from_array(&input, 0).unwrap();
        let ColumnarValue::Scalar(result) = invoke(ColumnarValue::Scalar(scalar), is_min) else {
            panic!("scalar input must produce scalar output")
        };
        assert_eq!(
            float64_bits(result.to_array().unwrap().as_ref()),
            vec![Some((-0.0f64).to_bits())]
        );
    }
}

#[test]
fn nested_lists_use_spark_lexicographic_order_and_keep_original_bits() {
    let positive = f64::from_bits(0x7ff8_0000_0000_0001);
    let negative = f64::from_bits(0xfff8_0000_0000_0002);
    let children = ListArray::from_iter_primitive::<Float64Type, _, _>([
        Some(vec![Some(0.0), Some(positive)]),
        Some(vec![Some(-0.0), Some(negative)]),
        Some(vec![None]),
        Some(vec![Some(f64::NEG_INFINITY)]),
        Some(vec![]),
        Some(vec![None]),
        Some(vec![Some(-0.0)]),
        Some(vec![Some(0.0), None]),
        Some(vec![Some(negative)]),
        Some(vec![Some(f64::INFINITY)]),
        None,
        Some(vec![Some(1.0)]),
        None,
        None,
    ]);
    let input = list(Arc::new(children.clone()), &[0, 2, 4, 6, 8, 10, 12, 14]);
    for (is_min, winners) in [
        (
            true,
            [Some(0), Some(2), Some(4), Some(6), Some(9), Some(11), None],
        ),
        (
            false,
            [Some(0), Some(3), Some(5), Some(7), Some(8), Some(11), None],
        ),
    ] {
        let result = extrema(&input, is_min);
        let result = result.as_any().downcast_ref::<ListArray>().unwrap();
        for (row, winner) in winners.into_iter().enumerate() {
            match winner {
                Some(winner) => assert_eq!(
                    float64_bits(result.value(row).as_ref()),
                    float64_bits(children.value(winner).as_ref()),
                ),
                None => assert!(result.is_null(row)),
            }
        }
    }
}

#[test]
fn structs_compare_later_fields_without_normalizing_tied_floats() {
    let nan = f64::from_bits(0xfff8_0000_0000_0002);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Float64Array::from(vec![
            Some(-0.0),
            Some(0.0),
            Some(nan),
            Some(f64::NAN),
            None,
            Some(f64::NEG_INFINITY),
        ])),
        Arc::new(Int32Array::from(vec![2, 1, 1, 1, 100, -100])),
    ];
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
    let children = StructArray::new(fields.into(), columns, None);
    let input = list(Arc::new(children.clone()), &[0, 2, 4, 6]);
    for (is_min, winners) in [(true, [1, 2, 4]), (false, [0, 2, 5])] {
        let result = extrema(&input, is_min);
        let result = result.as_any().downcast_ref::<StructArray>().unwrap();
        let floats = float64_bits(result.column(0).as_ref());
        let expected = float64_bits(children.column(0).as_ref());
        let ints = result
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let expected_ints = children
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for (row, winner) in winners.into_iter().enumerate() {
            assert_eq!(floats[row], expected[winner]);
            assert_eq!(ints.value(row), expected_ints.value(winner));
        }
    }
}

#[test]
fn sliced_structs_preserve_nullable_list_fields() {
    // A null list can cover nonempty child data. It must stay null without copying that data.
    let items = ListArray::new(
        Arc::new(Field::new_list_field(DataType::Float64, true)),
        OffsetBuffer::new(vec![0, 1, 3, 5, 5, 6].into()),
        Arc::new(Float64Array::from(vec![
            Some(99.0),
            Some(-0.0),
            None,
            Some(88.0),
            Some(77.0),
            Some(0.0),
        ])),
        Some(NullBuffer::from(vec![true, true, false, true, true])),
    );
    let children = StructArray::new(
        vec![
            Arc::new(Field::new("rank", DataType::Int32, false)),
            Arc::new(Field::new("items", items.data_type().clone(), true)),
        ]
        .into(),
        vec![
            Arc::new(Int32Array::from(vec![99, 2, 1, 3, 4])),
            Arc::new(items),
        ],
        None,
    )
    .slice(1, 4);
    let input = list(Arc::new(children), &[0, 2, 4, 4]);
    for is_min in [true, false] {
        let result = extrema(&input, is_min);
        let result = result.as_any().downcast_ref::<StructArray>().unwrap();
        assert!(result.is_valid(0));
        assert!(result.is_valid(1));
        assert!(result.is_null(2));
        let items = result
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert!(items.is_valid(1));
        assert!(items.is_null(2));
        if is_min {
            assert!(items.is_null(0));
            assert_eq!(items.value_length(1), 0);
            assert_eq!(items.values().len(), 0);
        } else {
            assert!(items.is_valid(0));
            assert_eq!(
                float64_bits(items.value(0).as_ref()),
                vec![Some((-0.0f64).to_bits()), None]
            );
            assert_eq!(
                float64_bits(items.value(1).as_ref()),
                vec![Some(0.0f64.to_bits())]
            );
            assert_eq!(items.values().len(), 3);
        }
    }
}

#[test]
fn nested_results_do_not_retain_losing_values() {
    let rows = 512;
    for (is_min, winner) in [(true, None), (true, Some(-0.0)), (false, Some(2.0))] {
        let mut leaves = Vec::new();
        let mut offsets = vec![0];
        for _ in 0..rows {
            leaves.extend(winner);
            offsets.push(leaves.len() as i32);
            leaves.extend(std::iter::repeat_n(1.0, 128));
            offsets.push(leaves.len() as i32);
        }
        let children = list(Arc::new(Float64Array::from(leaves)), &offsets);
        let offsets: Vec<i32> = (0..=rows).map(|row| (row * 2) as i32).collect();
        let input = list(Arc::new(children), &offsets);
        let result = extrema(&input, is_min);
        let lists = result.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(lists.null_count(), 0);
        for row in 0..rows {
            assert_eq!(
                float64_bits(lists.value(row).as_ref()),
                winner
                    .into_iter()
                    .map(|v| Some(v.to_bits()))
                    .collect::<Vec<_>>()
            );
        }
        assert!(result.get_buffer_memory_size() < 32 * 1024);
    }
}

#[test]
fn sparse_list_and_struct_results_bound_child_capacity() {
    let rows = 8192;
    let leaves = Arc::new(Float64Array::from(vec![-0.0; 1000]));
    let child: ArrayRef = Arc::new(list(leaves, &[0, 1000]));
    let fields = vec![Arc::new(Field::new(
        "items",
        child.data_type().clone(),
        true,
    ))];
    let structure: ArrayRef = Arc::new(StructArray::new(
        fields.into(),
        vec![Arc::clone(&child)],
        None,
    ));
    let mut offsets = vec![1; rows + 1];
    offsets[0] = 0;
    for child in [child, structure] {
        let input = list(Arc::clone(&child), &offsets);
        for is_min in [true, false] {
            let result = extrema(&input, is_min);
            assert_eq!(result.len(), rows);
            assert_eq!(result.null_count(), rows - 1);
            let selected = if let Some(structure) = result.as_any().downcast_ref::<StructArray>() {
                structure.column(0).as_ref()
            } else {
                result.as_ref()
            };
            let selected = selected
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .value(0);
            assert_eq!(
                float64_bits(selected.as_ref()),
                vec![Some((-0.0f64).to_bits()); 1000]
            );
            assert!(result.get_buffer_memory_size() < 256 * 1024);
        }
    }
}

#[test]
fn delegates_non_floating_values() {
    let input = ListArray::from_iter_primitive::<Int32Type, _, _>([
        Some(vec![Some(3), None, Some(-2)]),
        Some(vec![]),
    ]);
    for (is_min, expected) in [(true, vec![Some(-2), None]), (false, vec![Some(3), None])] {
        let result = extrema(&input, is_min);
        assert_eq!(
            result.as_any().downcast_ref::<Int32Array>().unwrap(),
            &Int32Array::from(expected)
        );
    }
}

#[test]
fn empty_string_batch_retains_element_type() {
    let input = list(Arc::new(StringArray::from(Vec::<&str>::new())), &[0]);
    for is_min in [true, false] {
        let result = extrema(&input, is_min);
        assert!(result.is_empty());
        assert_eq!(result.data_type(), &input.value_type());
    }
}

#[test]
fn utf8_collations_preserve_original_winners_across_string_layouts() {
    let values = vec![
        Some("unused"),
        Some("B"),
        Some("a"),
        Some("x "),
        Some("x"),
        None,
        None,
    ];
    let layouts: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(values.clone())),
        Arc::new(LargeStringArray::from(values.clone())),
        Arc::new(StringViewArray::from(values)),
    ];
    for values in layouts {
        let input = ListArray::new(
            Arc::new(Field::new_list_field(values.data_type().clone(), true)),
            OffsetBuffer::new(vec![0, 1, 3, 5, 7, 7, 7].into()),
            values,
            Some(NullBuffer::from(vec![true, true, true, true, true, false])),
        )
        .slice(1, 5);
        for (name, min, max) in [
            (None, [Some("B"), Some("x")], [Some("a"), Some("x ")]),
            (
                Some("UTF8_BINARY"),
                [Some("B"), Some("x")],
                [Some("a"), Some("x ")],
            ),
            (
                Some("UTF8_BINARY_RTRIM"),
                [Some("B"), Some("x ")],
                [Some("a"), Some("x ")],
            ),
            (
                Some("UTF8_LCASE"),
                [Some("a"), Some("x")],
                [Some("B"), Some("x ")],
            ),
            (
                Some("UTF8_LCASE_RTRIM"),
                [Some("a"), Some("x ")],
                [Some("B"), Some("x ")],
            ),
        ] {
            for (is_min, expected) in [(true, min), (false, max)] {
                let udf = match name {
                    Some(name) => {
                        SparkArrayExtrema::with_collations(is_min, &[name.into()], 16).unwrap()
                    }
                    None => SparkArrayExtrema::new(is_min),
                };
                let ColumnarValue::Array(result) =
                    invoke_udf(ColumnarValue::Array(Arc::new(input.clone())), &udf)
                else {
                    panic!("expected array result")
                };
                for row in 0..input.len() {
                    let scalar = ScalarValue::try_from_array(&input, row).unwrap();
                    let ColumnarValue::Scalar(actual) =
                        invoke_udf(ColumnarValue::Scalar(scalar), &udf)
                    else {
                        panic!("expected scalar result")
                    };
                    assert_eq!(actual, ScalarValue::try_from_array(&result, row).unwrap());
                }
                let result =
                    arrow::compute::cast(&result, &arrow::datatypes::DataType::Utf8).unwrap();
                let result = result.as_any().downcast_ref::<StringArray>().unwrap();
                assert_eq!(
                    result.iter().collect::<Vec<_>>(),
                    vec![expected[0], expected[1], None, None, None]
                );
            }
        }
    }
}

#[test]
fn string_view_results_release_losing_buffers() {
    for is_min in [true, false] {
        let (winning, losing) = if is_min { ("A", "z") } else { ("z", "A") };
        let loser = losing.repeat(1024 * 1024);
        for winner in [winning.to_owned(), winning.repeat(64)] {
            let input = list(
                Arc::new(StringViewArray::from(vec![winner.as_str(), loser.as_str()])),
                &[0, 2],
            );
            for udf in [
                SparkArrayExtrema::new(is_min),
                SparkArrayExtrema::with_collations(is_min, &["UTF8_LCASE".into()], 16).unwrap(),
            ] {
                let ColumnarValue::Array(result) =
                    invoke_udf(ColumnarValue::Array(Arc::new(input.clone())), &udf)
                else {
                    panic!("expected array result")
                };
                let result = result.as_any().downcast_ref::<StringViewArray>().unwrap();
                assert_eq!(result.value(0), winner);
                assert!(result.get_buffer_memory_size() < 1024);
            }
        }
    }
}

#[test]
fn utf8_lcase_uses_spark_unicode_version_and_space_trimming() {
    use std::cmp::Ordering::{Equal, Greater, Less};
    for version in [16, 17] {
        for (left, right, expected) in [
            ("İ", "i\u{307}", Equal),
            ("ς", "σ", Equal),
            ("K", "k", Equal),
            ("abİx", "ABi\u{307}y", Less),
            ("abK", "ABk", Equal),
            ("ab", "ABΣ", Less),
            ("aΣ", "B", Less),
            ("\u{10400}", "\u{10428}", Equal),
            ("é", "e", Greater),
            ("A ", "a", Greater),
        ] {
            assert_eq!(Utf8Collation::Lcase.compare(left, right, version), expected);
            assert_eq!(
                Utf8Collation::Lcase.compare(right, left, version),
                expected.reverse()
            );
        }
        assert_eq!(Utf8Collation::LcaseRtrim.compare("A ", "a", version), Equal);
        assert_eq!(
            Utf8Collation::LcaseRtrim.compare("A\t", "a", version),
            Greater
        );
        assert_eq!(
            Utf8Collation::LcaseRtrim.compare("A\u{a0}", "a", version),
            Greater
        );
    }
}

#[test]
fn utf8_lcase_unicode_17_mappings_are_version_gated() {
    use std::cmp::Ordering::{Equal, Less};
    let pairs = [
        ("\u{a7ce}", "\u{a7cf}"),
        ("\u{a7d2}", "\u{a7d3}"),
        ("\u{a7d4}", "\u{a7d5}"),
        ("\u{16ea0}", "\u{16ebb}"),
        ("\u{16ea1}", "\u{16ebc}"),
        ("\u{16ea2}", "\u{16ebd}"),
        ("\u{16ea3}", "\u{16ebe}"),
        ("\u{16ea4}", "\u{16ebf}"),
        ("\u{16ea5}", "\u{16ec0}"),
        ("\u{16ea6}", "\u{16ec1}"),
        ("\u{16ea7}", "\u{16ec2}"),
        ("\u{16ea8}", "\u{16ec3}"),
        ("\u{16ea9}", "\u{16ec4}"),
        ("\u{16eaa}", "\u{16ec5}"),
        ("\u{16eab}", "\u{16ec6}"),
        ("\u{16eac}", "\u{16ec7}"),
        ("\u{16ead}", "\u{16ec8}"),
        ("\u{16eae}", "\u{16ec9}"),
        ("\u{16eaf}", "\u{16eca}"),
        ("\u{16eb0}", "\u{16ecb}"),
        ("\u{16eb1}", "\u{16ecc}"),
        ("\u{16eb2}", "\u{16ecd}"),
        ("\u{16eb3}", "\u{16ece}"),
        ("\u{16eb4}", "\u{16ecf}"),
        ("\u{16eb5}", "\u{16ed0}"),
        ("\u{16eb6}", "\u{16ed1}"),
        ("\u{16eb7}", "\u{16ed2}"),
        ("\u{16eb8}", "\u{16ed3}"),
    ];
    for (upper, lower) in pairs {
        for (version, expected) in [(16, Less), (17, Equal)] {
            assert_eq!(
                Utf8Collation::Lcase.compare(upper, lower, version),
                expected
            );
            assert_eq!(
                Utf8Collation::Lcase.compare(lower, upper, version),
                expected.reverse()
            );
        }
    }
}
