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

//! Differential tests for Comet's `xxhash64` kernel and
//! `datafusion_spark::function::hash::xxhash64::SparkXxhash64`.
//!
//! These tests compare the two implementations only at Spark's fixed seed of 42. Custom seeds
//! are covered separately at the Comet kernel level. Production expression routing is deliberately
//! outside the scope of this module.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array,
    FixedSizeBinaryArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, LargeBinaryArray, LargeStringArray, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field};
use datafusion::common::{Result, ScalarValue};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_spark::function::hash::xxhash64::SparkXxhash64;
use twox_hash::XxHash64;

use super::create_xxhash64_hashes;

const SPARK_DEFAULT_SEED: u64 = 42;

fn comet_kernel(arrays: &[ArrayRef], starting_seed: u64) -> Result<Vec<u64>> {
    let row_count = arrays.first().map_or(0, |array| array.len());
    let mut hashes = vec![starting_seed; row_count];
    create_xxhash64_hashes(arrays, &mut hashes)?;
    Ok(hashes)
}

fn spark_xxhash64_upstream(arrays: &[ArrayRef]) -> Result<Vec<u64>> {
    let row_count = arrays.first().map_or(0, |array| array.len());
    let args = arrays
        .iter()
        .map(|array| ColumnarValue::Array(Arc::clone(array)))
        .collect();
    let arg_fields = arrays
        .iter()
        .enumerate()
        .map(|(index, array)| {
            Arc::new(Field::new(
                format!("arg_{index}"),
                array.data_type().clone(),
                true,
            ))
        })
        .collect();

    let result = SparkXxhash64::new().invoke_with_args(ScalarFunctionArgs {
        args,
        arg_fields,
        number_rows: row_count,
        return_field: Arc::new(Field::new("xxhash64", DataType::Int64, false)),
        config_options: Arc::new(ConfigOptions::default()),
    })?;
    Ok(columnar_u64s(result, row_count))
}

fn columnar_u64s(value: ColumnarValue, row_count: usize) -> Vec<u64> {
    match value {
        ColumnarValue::Scalar(ScalarValue::Int64(Some(value))) => {
            vec![value as u64; row_count]
        }
        ColumnarValue::Array(array) => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("SparkXxhash64 must return Int64")
            .values()
            .iter()
            .map(|value| *value as u64)
            .collect(),
        other => panic!("unexpected SparkXxhash64 result: {other:?}"),
    }
}

fn assert_compatible(label: &str, arrays: &[ArrayRef]) {
    let comet = comet_kernel(arrays, SPARK_DEFAULT_SEED)
        .unwrap_or_else(|error| panic!("{label}: Comet kernel failed: {error}"));
    let upstream = spark_xxhash64_upstream(arrays)
        .unwrap_or_else(|error| panic!("{label}: SparkXxhash64 failed: {error}"));
    assert_eq!(comet, upstream, "{label}: hash mismatch");
}

fn array_ref(array: impl Array + 'static) -> ArrayRef {
    Arc::new(array)
}

fn decimal128(precision: u8, scale: i8, values: Vec<Option<i128>>) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(values)
            .with_precision_and_scale(precision, scale)
            .unwrap(),
    )
}

#[test]
fn primitive_flat_types_match_upstream() {
    let cases: Vec<(&str, ArrayRef)> = vec![
        (
            "Boolean",
            array_ref(BooleanArray::from(vec![Some(true), Some(false), None])),
        ),
        (
            "Int8",
            array_ref(Int8Array::from(vec![
                Some(0),
                Some(1),
                Some(-1),
                Some(i8::MIN),
                Some(i8::MAX),
                None,
            ])),
        ),
        (
            "Int16",
            array_ref(Int16Array::from(vec![
                Some(0),
                Some(1),
                Some(-1),
                Some(i16::MIN),
                Some(i16::MAX),
                None,
            ])),
        ),
        (
            "Int32",
            array_ref(Int32Array::from(vec![
                Some(0),
                Some(1),
                Some(-1),
                Some(i32::MIN),
                Some(i32::MAX),
                None,
            ])),
        ),
        (
            "Int64",
            array_ref(Int64Array::from(vec![
                Some(0),
                Some(1),
                Some(-1),
                Some(i64::MIN),
                Some(i64::MAX),
                None,
            ])),
        ),
        (
            "Float32",
            array_ref(Float32Array::from(vec![
                Some(0.0),
                Some(-0.0),
                Some(1.5),
                Some(-1.5),
                Some(f32::NAN),
                Some(f32::INFINITY),
                Some(f32::NEG_INFINITY),
                None,
            ])),
        ),
        (
            "Float64",
            array_ref(Float64Array::from(vec![
                Some(0.0),
                Some(-0.0),
                Some(1.5),
                Some(-1.5),
                Some(f64::NAN),
                Some(f64::INFINITY),
                Some(f64::NEG_INFINITY),
                None,
            ])),
        ),
    ];

    for (label, array) in cases {
        assert_compatible(label, &[array]);
    }
}

#[test]
fn empty_flat_array_matches_upstream() {
    assert_compatible(
        "empty Int32",
        &[array_ref(Int32Array::from(Vec::<i32>::new()))],
    );
}

#[test]
fn single_row_scalar_result_matches_upstream() {
    assert_compatible(
        "single-row Int32",
        &[array_ref(Int32Array::from(vec![Some(1)]))],
    );
}

#[test]
fn variable_width_flat_types_match_upstream() {
    // Empty, ASCII, Unicode, and variable-length binary, plus a null, for Utf8, Binary,
    // their Large* variants, and FixedSizeBinary widths 1/4/8. FixedSizeBinary has no
    // empty payload, so all-zero bytes stand in. `empty_flat_array_matches_upstream`
    // only covers a zero-row primitive array, not empty string/binary payloads.
    let strings = vec![
        Some(""),
        Some("hello"),
        Some("abc"),
        Some("😁"),
        Some("天地"),
        None,
    ];
    let binaries: Vec<Option<&[u8]>> = vec![
        Some(b"".as_slice()),
        Some(b"\x00".as_slice()),
        Some(b"hello".as_slice()),
        Some(&[0u8, 1, 2][..]),
        Some(&[0u8, 1, 2, 3, 4, 5, 6, 7][..]),
        None,
    ];

    assert_compatible("Utf8", &[array_ref(StringArray::from(strings.clone()))]);
    assert_compatible("LargeUtf8", &[array_ref(LargeStringArray::from(strings))]);
    assert_compatible(
        "Binary",
        &[array_ref(BinaryArray::from_opt_vec(binaries.clone()))],
    );
    assert_compatible(
        "LargeBinary",
        &[array_ref(LargeBinaryArray::from_opt_vec(binaries))],
    );
    assert_compatible(
        "FixedSizeBinary(1)",
        &[array_ref(
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                vec![Some(&[0x00][..]), Some(b"A".as_slice()), None].into_iter(),
                1,
            )
            .unwrap(),
        )],
    );
    assert_compatible(
        "FixedSizeBinary(4)",
        &[array_ref(
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                vec![
                    Some(&[0x00, 0x00, 0x00, 0x00][..]),
                    Some(b"abcd".as_slice()),
                    Some("😁".as_bytes()),
                    None,
                ]
                .into_iter(),
                4,
            )
            .unwrap(),
        )],
    );
    assert_compatible(
        "FixedSizeBinary(8)",
        &[array_ref(
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                vec![
                    Some(&[0u8; 8][..]),
                    Some(b"hello\0\0\0".as_slice()),
                    Some(&[0u8, 1, 2, 3, 4, 5, 6, 7][..]),
                    None,
                ]
                .into_iter(),
                8,
            )
            .unwrap(),
        )],
    );
}

#[test]
fn temporal_flat_types_match_upstream() {
    assert_compatible(
        "Date32",
        &[array_ref(Date32Array::from(vec![
            Some(0),
            Some(1),
            Some(-1),
            None,
        ]))],
    );
    assert_compatible(
        "Date64",
        &[array_ref(Date64Array::from(vec![
            Some(0),
            Some(86_400_000),
            Some(-86_400_000),
            None,
        ]))],
    );

    let timestamps = vec![Some(0), Some(1), Some(-1), Some(-1_234_567_890), None];
    assert_compatible(
        "TimestampMicrosecond without timezone",
        &[array_ref(TimestampMicrosecondArray::from(
            timestamps.clone(),
        ))],
    );
    assert_compatible(
        "TimestampMicrosecond with UTC annotation",
        &[array_ref(
            TimestampMicrosecondArray::from(timestamps).with_timezone("UTC"),
        )],
    );
}

#[test]
fn decimal128_small_and_large_precisions_match_upstream() {
    let cases = [
        (
            "Decimal128(10, 2)",
            decimal128(
                10,
                2,
                vec![
                    Some(0),
                    Some(12_345),
                    Some(-12_345),
                    Some(9_999_999_999),
                    None,
                ],
            ),
        ),
        (
            "Decimal128(18, 0)",
            decimal128(
                18,
                0,
                vec![
                    Some(0),
                    Some(999_999_999_999_999_999),
                    Some(-999_999_999_999_999_999),
                    None,
                ],
            ),
        ),
        (
            "Decimal128(20, 6)",
            decimal128(
                20,
                6,
                vec![
                    Some(0),
                    Some(10_000_000_000_000_000_000),
                    Some(-10_000_000_000_000_000_000),
                    None,
                ],
            ),
        ),
        (
            "Decimal128(38, 18)",
            decimal128(
                38,
                18,
                vec![
                    Some(0),
                    Some(99_999_999_999_999_999_999_999_999_999_999_999_999),
                    Some(-99_999_999_999_999_999_999_999_999_999_999_999_999),
                    None,
                ],
            ),
        ),
    ];

    for (label, array) in cases {
        assert_compatible(label, &[array]);
    }
}

#[test]
fn multi_column_chaining_matches_sequential_updates_and_upstream() {
    let columns = vec![
        array_ref(Int32Array::from(vec![Some(1), None, Some(-7), Some(42)])),
        array_ref(Float64Array::from(vec![
            Some(1.5),
            Some(-0.0),
            None,
            Some(f64::NAN),
        ])),
        decimal128(10, 2, vec![Some(123), Some(-456), Some(0), None]),
    ];

    assert_compatible("multi-column chaining", &columns);

    let chained = comet_kernel(&columns, SPARK_DEFAULT_SEED).unwrap();
    let mut sequential = vec![SPARK_DEFAULT_SEED; columns[0].len()];
    for column in &columns {
        create_xxhash64_hashes(&[Arc::clone(column)], &mut sequential).unwrap();
    }
    assert_eq!(chained, sequential);
    assert_eq!(chained, spark_xxhash64_upstream(&columns).unwrap());
}

#[test]
fn custom_starting_seeds_are_honored_by_the_kernel() {
    let values: ArrayRef = array_ref(Int32Array::from(vec![Some(1), Some(0), None, Some(-1)]));

    for signed_seed in [0_i64, 1, 7, 42, -1, i64::MIN] {
        let seed = signed_seed as u64;
        let actual = comet_kernel(&[Arc::clone(&values)], seed).unwrap();
        let expected = vec![
            XxHash64::oneshot(seed, &1_i32.to_le_bytes()),
            XxHash64::oneshot(seed, &0_i32.to_le_bytes()),
            seed,
            XxHash64::oneshot(seed, &(-1_i32).to_le_bytes()),
        ];
        assert_eq!(actual, expected, "starting seed {signed_seed}");
    }

    assert_eq!(
        comet_kernel(&[values], SPARK_DEFAULT_SEED).unwrap(),
        spark_xxhash64_upstream(&[array_ref(Int32Array::from(vec![
            Some(1),
            Some(0),
            None,
            Some(-1),
        ]))])
        .unwrap(),
        "the upstream API uses Spark's fixed seed of 42",
    );
}

#[test]
fn each_row_can_have_a_different_starting_seed() {
    let values: ArrayRef = array_ref(Int32Array::from(vec![Some(1), Some(1), None, Some(-1)]));
    let starting_seeds = vec![0_u64, 1, 7, i64::MIN as u64];
    let mut actual = starting_seeds.clone();

    create_xxhash64_hashes(&[values], &mut actual).unwrap();

    let expected = vec![
        XxHash64::oneshot(starting_seeds[0], &1_i32.to_le_bytes()),
        XxHash64::oneshot(starting_seeds[1], &1_i32.to_le_bytes()),
        starting_seeds[2],
        XxHash64::oneshot(starting_seeds[3], &(-1_i32).to_le_bytes()),
    ];
    assert_eq!(actual, expected);
}
