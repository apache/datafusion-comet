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

//! Differential coverage of Comet `xxhash64` against `datafusion-spark::SparkXxhash64`.
//!
//! `SparkXxhash64` always starts from Spark's default seed (`42`) and hashes every argument;
//! Comet's native UDF takes the seed as a trailing Int64 scalar. Kernel comparisons therefore
//! go through [`create_xxhash64_hashes`] (seed 42) vs `SparkXxhash64::invoke_with_args`.
//!
//! Compatibility at seed 42 (bit-identical):
//!
//! | Type | Compatible |
//! | --- | --- |
//! | Boolean, Int8/16/32/64, Float32/64 | yes |
//! | Utf8, LargeUtf8, Binary, LargeBinary, FixedSizeBinary | yes |
//! | Date32, Date64, Timestamp | yes |
//! | Decimal128 precision ≤ 18 | yes |
//! | Decimal128 precision > 18 | yes |
//! | Dictionary (top-level) | yes |
//! | List / LargeList / FixedSizeList of primitives | yes |
//! | Map&lt;Utf8, Int32&gt; / Map&lt;Int32, Utf8&gt; / Map&lt;Utf8, Utf8&gt; / Map&lt;Int32, Int32&gt; | yes |
//! | Map&lt;Utf8, Decimal128&gt; | yes |
//! | Struct (non-null parent) | yes, but not routed (see below) |
//! | Struct NULL with hidden children | **no** — `SparkXxhash64` hashes hidden children |
//! | List&lt;Dictionary&gt; | **no** — upstream restarts from seed 42 |
//! | Time64(ns) | **no** — upstream does not dispatch |
//! | custom seed | **no** — `SparkXxhash64` hardcodes 42 |

use super::{create_xxhash64_hashes, spark_xxhash64};
use arrow::array::builder::{
    Decimal128Builder, Int32Builder, ListBuilder, MapBuilder, StringBuilder, StructBuilder,
};
use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, DictionaryArray, FixedSizeBinaryArray,
    FixedSizeListArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    LargeBinaryArray, LargeListArray, LargeStringArray, ListArray, StringArray, StructArray,
    Time64NanosecondArray, TimestampMicrosecondArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field, Fields, Int32Type, Int8Type};
use datafusion::common::{Result, ScalarValue};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_spark::function::hash::xxhash64::SparkXxhash64;
use std::sync::Arc;

const SPARK_DEFAULT_SEED: u64 = 42;

fn comet_kernel(arrays: &[ArrayRef], seed: u64) -> Result<Vec<u64>> {
    let n = arrays.first().map(|a| a.len()).unwrap_or(0);
    let mut hashes = vec![seed; n];
    create_xxhash64_hashes(arrays, &mut hashes)?;
    Ok(hashes)
}

fn columnar_u64s(value: ColumnarValue, n: usize) -> Vec<u64> {
    match value {
        ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) => vec![v as u64; n.max(1)],
        ColumnarValue::Array(array) => {
            let typed = array
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("xxhash64 result is Int64");
            typed.values().iter().map(|v| *v as u64).collect()
        }
        other => panic!("unexpected xxhash64 result: {other:?}"),
    }
}

fn spark_xxhash64_upstream(arrays: &[ArrayRef]) -> Result<Vec<u64>> {
    let n = arrays.first().map(|a| a.len()).unwrap_or(1);
    let args: Vec<ColumnarValue> = arrays
        .iter()
        .map(|a| ColumnarValue::Array(Arc::clone(a)))
        .collect();
    let arg_fields = arrays
        .iter()
        .enumerate()
        .map(|(i, a)| Arc::new(Field::new(format!("c{i}"), a.data_type().clone(), true)))
        .collect();
    let result = SparkXxhash64::new().invoke_with_args(ScalarFunctionArgs {
        args,
        arg_fields,
        number_rows: n,
        return_field: Arc::new(Field::new("xxhash64", DataType::Int64, false)),
        config_options: Arc::new(ConfigOptions::default()),
    })?;
    Ok(columnar_u64s(result, n))
}

fn comet_expr(arrays: &[ArrayRef], seed: i64) -> Result<Vec<u64>> {
    let n = arrays.first().map(|a| a.len()).unwrap_or(1);
    let mut args: Vec<ColumnarValue> = arrays
        .iter()
        .map(|a| ColumnarValue::Array(Arc::clone(a)))
        .collect();
    args.push(ColumnarValue::Scalar(ScalarValue::Int64(Some(seed))));
    Ok(columnar_u64s(spark_xxhash64(&args)?, n))
}

/// Compare Comet's kernel (seed 42) with `SparkXxhash64` on the same columns.
fn assert_compatible(label: &str, arrays: &[ArrayRef]) {
    let comet = comet_kernel(arrays, SPARK_DEFAULT_SEED)
        .unwrap_or_else(|e| panic!("{label}: Comet kernel failed: {e}"));
    let upstream = spark_xxhash64_upstream(arrays)
        .unwrap_or_else(|e| panic!("{label}: SparkXxhash64 failed: {e}"));
    assert_eq!(comet, upstream, "{label}: kernel mismatch");
    let expr = comet_expr(arrays, SPARK_DEFAULT_SEED as i64)
        .unwrap_or_else(|e| panic!("{label}: Comet expression failed: {e}"));
    assert_eq!(expr, upstream, "{label}: expression mismatch");
}

fn col(array: impl Array + 'static) -> Vec<ArrayRef> {
    vec![Arc::new(array) as ArrayRef]
}

fn list_i32(rows: Vec<Option<Vec<Option<i32>>>>) -> ArrayRef {
    let mut b = ListBuilder::new(Int32Builder::new());
    for row in rows {
        match row {
            None => b.append(false),
            Some(values) => {
                for v in values {
                    match v {
                        Some(x) => b.values().append_value(x),
                        None => b.values().append_null(),
                    }
                }
                b.append(true);
            }
        }
    }
    Arc::new(b.finish())
}

fn large_list_i32(rows: Vec<Option<Vec<Option<i32>>>>) -> ArrayRef {
    let mut offsets = vec![0i64];
    let mut values: Vec<Option<i32>> = Vec::new();
    let mut validity = Vec::new();
    for row in rows {
        match row {
            None => {
                validity.push(false);
                offsets.push(values.len() as i64);
            }
            Some(elems) => {
                validity.push(true);
                values.extend(elems);
                offsets.push(values.len() as i64);
            }
        }
    }
    let values = Int32Array::from(values);
    Arc::new(LargeListArray::new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        OffsetBuffer::new(offsets.into()),
        Arc::new(values),
        Some(NullBuffer::from(validity)),
    ))
}

fn decimal128(precision: u8, scale: i8, values: Vec<Option<i128>>) -> ArrayRef {
    let mut b = Decimal128Builder::with_capacity(values.len())
        .with_data_type(DataType::Decimal128(precision, scale));
    for v in values {
        match v {
            Some(x) => b.append_value(x),
            None => b.append_null(),
        }
    }
    Arc::new(b.finish())
}

fn struct_ab(a: Vec<Option<i32>>, b: Vec<Option<&str>>, nulls: Option<Vec<bool>>) -> ArrayRef {
    let fields: Fields = vec![
        Arc::new(Field::new("a", DataType::Int32, true)),
        Arc::new(Field::new("b", DataType::Utf8, true)),
    ]
    .into();
    let children: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(a)),
        Arc::new(StringArray::from(b)),
    ];
    let nulls = nulls.map(NullBuffer::from);
    Arc::new(StructArray::new(fields, children, nulls))
}

type Utf8I32Entries = Vec<(&'static str, Option<i32>)>;
type I32Utf8Entries = Vec<(i32, Option<&'static str>)>;
type Utf8Utf8Entries = Vec<(&'static str, Option<&'static str>)>;
type I32I32Entries = Vec<(i32, Option<i32>)>;
type Utf8DecimalEntries = Vec<(&'static str, Option<i128>)>;

fn map_utf8_i32(rows: Vec<Option<Utf8I32Entries>>) -> ArrayRef {
    let mut mb = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
    for row in rows {
        match row {
            None => {
                mb.append(false).unwrap();
            }
            Some(entries) => {
                for (k, v) in entries {
                    mb.keys().append_value(k);
                    match v {
                        Some(x) => mb.values().append_value(x),
                        None => mb.values().append_null(),
                    }
                }
                mb.append(true).unwrap();
            }
        }
    }
    Arc::new(mb.finish())
}

fn map_i32_utf8(rows: Vec<Option<I32Utf8Entries>>) -> ArrayRef {
    let mut mb = MapBuilder::new(None, Int32Builder::new(), StringBuilder::new());
    for row in rows {
        match row {
            None => {
                mb.append(false).unwrap();
            }
            Some(entries) => {
                for (k, v) in entries {
                    mb.keys().append_value(k);
                    match v {
                        Some(x) => mb.values().append_value(x),
                        None => mb.values().append_null(),
                    }
                }
                mb.append(true).unwrap();
            }
        }
    }
    Arc::new(mb.finish())
}

fn map_utf8_utf8(rows: Vec<Option<Utf8Utf8Entries>>) -> ArrayRef {
    let mut mb = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    for row in rows {
        match row {
            None => {
                mb.append(false).unwrap();
            }
            Some(entries) => {
                for (k, v) in entries {
                    mb.keys().append_value(k);
                    match v {
                        Some(x) => mb.values().append_value(x),
                        None => mb.values().append_null(),
                    }
                }
                mb.append(true).unwrap();
            }
        }
    }
    Arc::new(mb.finish())
}

fn map_i32_i32(rows: Vec<Option<I32I32Entries>>) -> ArrayRef {
    let mut mb = MapBuilder::new(None, Int32Builder::new(), Int32Builder::new());
    for row in rows {
        match row {
            None => {
                mb.append(false).unwrap();
            }
            Some(entries) => {
                for (k, v) in entries {
                    mb.keys().append_value(k);
                    match v {
                        Some(x) => mb.values().append_value(x),
                        None => mb.values().append_null(),
                    }
                }
                mb.append(true).unwrap();
            }
        }
    }
    Arc::new(mb.finish())
}

fn map_utf8_decimal(precision: u8, scale: i8, rows: Vec<Option<Utf8DecimalEntries>>) -> ArrayRef {
    let mut mb = MapBuilder::new(
        None,
        StringBuilder::new(),
        Decimal128Builder::new().with_data_type(DataType::Decimal128(precision, scale)),
    );
    for row in rows {
        match row {
            None => {
                mb.append(false).unwrap();
            }
            Some(entries) => {
                for (k, v) in entries {
                    mb.keys().append_value(k);
                    match v {
                        Some(x) => mb.values().append_value(x),
                        None => mb.values().append_null(),
                    }
                }
                mb.append(true).unwrap();
            }
        }
    }
    Arc::new(mb.finish())
}

// ---------------------------------------------------------------------------
// Primitive types
// ---------------------------------------------------------------------------

#[test]
fn boolean() {
    assert_compatible(
        "Boolean",
        &col(BooleanArray::from(vec![
            Some(true),
            Some(false),
            None,
            Some(true),
        ])),
    );
}

#[test]
fn int8() {
    assert_compatible(
        "Int8",
        &col(Int8Array::from(vec![
            Some(1),
            Some(0),
            Some(-1),
            Some(i8::MAX),
            Some(i8::MIN),
            None,
        ])),
    );
}

#[test]
fn int16() {
    assert_compatible(
        "Int16",
        &col(Int16Array::from(vec![
            Some(1),
            Some(0),
            Some(-1),
            Some(i16::MAX),
            Some(i16::MIN),
            None,
        ])),
    );
}

#[test]
fn int32() {
    assert_compatible(
        "Int32",
        &col(Int32Array::from(vec![
            Some(1),
            Some(0),
            Some(-1),
            Some(i32::MAX),
            Some(i32::MIN),
            None,
        ])),
    );
}

#[test]
fn int64() {
    assert_compatible(
        "Int64",
        &col(Int64Array::from(vec![
            Some(1),
            Some(0),
            Some(-1),
            Some(i64::MAX),
            Some(i64::MIN),
            None,
        ])),
    );
}

#[test]
fn float32() {
    assert_compatible(
        "Float32",
        &col(Float32Array::from(vec![
            Some(1.0),
            Some(0.0),
            Some(-0.0),
            Some(-1.0),
            Some(f32::NAN),
            Some(f32::INFINITY),
            Some(f32::NEG_INFINITY),
            None,
        ])),
    );
}

#[test]
fn float64() {
    assert_compatible(
        "Float64",
        &col(Float64Array::from(vec![
            Some(1.0),
            Some(0.0),
            Some(-0.0),
            Some(-1.0),
            Some(f64::NAN),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            None,
        ])),
    );
}

#[test]
fn utf8() {
    assert_compatible(
        "Utf8",
        &col(StringArray::from(vec![
            Some("hello"),
            Some(""),
            Some("😁"),
            Some("天地"),
            Some("abc"),
            None,
        ])),
    );
}

#[test]
fn large_utf8() {
    assert_compatible(
        "LargeUtf8",
        &col(LargeStringArray::from(vec![
            Some("hello"),
            Some(""),
            Some("😁"),
            Some("天地"),
            None,
        ])),
    );
}

#[test]
fn binary() {
    assert_compatible(
        "Binary",
        &col(BinaryArray::from_opt_vec(vec![
            Some(b"hello".as_slice()),
            Some(b"".as_slice()),
            Some(&[0u8, 1, 2][..]),
            None,
        ])),
    );
}

#[test]
fn large_binary() {
    assert_compatible(
        "LargeBinary",
        &col(LargeBinaryArray::from_opt_vec(vec![
            Some(b"hello".as_slice()),
            Some(b"".as_slice()),
            Some(&[0u8, 1, 2][..]),
            None,
        ])),
    );
}

#[test]
fn fixed_size_binary() {
    let array = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
        vec![
            Some(&[0x01, 0x02, 0x03, 0x04][..]),
            Some(&[0x00, 0x00, 0x00, 0x00][..]),
            None,
        ]
        .into_iter(),
        4,
    )
    .unwrap();
    assert_compatible("FixedSizeBinary", &col(array));
}

#[test]
fn date32() {
    assert_compatible(
        "Date32",
        &col(Date32Array::from(vec![
            Some(0),
            Some(1),
            Some(-1),
            Some(i32::MAX),
            Some(i32::MIN),
            None,
        ])),
    );
}

#[test]
fn date64() {
    assert_compatible(
        "Date64",
        &col(arrow::array::Date64Array::from(vec![
            Some(0),
            Some(86_400_000),
            Some(-86_400_000),
            None,
        ])),
    );
}

#[test]
fn timestamp_microsecond() {
    let values = vec![
        Some(0i64),
        Some(1),
        Some(-1),
        Some(i64::MAX),
        Some(i64::MIN),
        None,
    ];
    assert_compatible(
        "Timestamp(us, None)",
        &col(TimestampMicrosecondArray::from(values.clone())),
    );
    let tz = TimestampMicrosecondArray::from(values).with_timezone("UTC");
    assert_compatible("Timestamp(us, UTC)", &col(tz));
}

// ---------------------------------------------------------------------------
// Decimal128 (small / large split)
// ---------------------------------------------------------------------------

#[test]
fn decimal128_precision_10_fits_i64() {
    assert_compatible(
        "Decimal128(10,2)",
        &col(decimal128(
            10,
            2,
            vec![Some(0), Some(123), Some(-123), Some(9_999_999_999), None],
        )),
    );
}

#[test]
fn decimal128_precision_18_fits_i64() {
    assert_compatible(
        "Decimal128(18,2)",
        &col(decimal128(
            18,
            2,
            vec![
                Some(0),
                Some(123),
                Some(-123),
                Some(i64::MAX as i128),
                Some(i64::MIN as i128),
                None,
            ],
        )),
    );
}

#[test]
fn decimal128_precision_20_does_not_fit_i64() {
    let too_big = 10_000_000_000_000_000_000i128; // 1e19, beyond i64::MAX
    assert_compatible(
        "Decimal128(20,2)",
        &col(decimal128(
            20,
            2,
            vec![
                Some(0),
                Some(123),
                Some(-123),
                Some(too_big),
                Some(-too_big),
                None,
            ],
        )),
    );
}

#[test]
fn decimal128_precision_38() {
    let wide = 10i128.pow(28);
    assert_compatible(
        "Decimal128(38,10)",
        &col(decimal128(
            38,
            10,
            vec![
                Some(0),
                Some(123),
                Some(-123),
                Some(wide),
                Some(-wide),
                None,
            ],
        )),
    );
}

// ---------------------------------------------------------------------------
// Dictionary
// ---------------------------------------------------------------------------

#[test]
fn dictionary_int8_utf8() {
    let values: ArrayRef = Arc::new(StringArray::from(vec!["hello", "world", "abc"]));
    let keys = Int8Array::from(vec![Some(0), Some(1), Some(2), Some(0), None, Some(1)]);
    let dict = DictionaryArray::<Int8Type>::try_new(keys, values).unwrap();
    assert_compatible("Dictionary<Int8, Utf8>", &col(dict));
}

#[test]
fn dictionary_int32_utf8() {
    let values: ArrayRef = Arc::new(StringArray::from(vec!["hello", "world"]));
    let keys = Int32Array::from(vec![Some(0), Some(1), Some(0), None, Some(1)]);
    let dict = DictionaryArray::<Int32Type>::try_new(keys, values).unwrap();
    assert_compatible("Dictionary<Int32, Utf8>", &col(dict));
}

#[test]
fn dictionary_int32_int64() {
    let values: ArrayRef = Arc::new(Int64Array::from(vec![Some(10), Some(20), None]));
    let keys = Int32Array::from(vec![Some(0), Some(1), Some(2), None, Some(0)]);
    let dict = DictionaryArray::<Int32Type>::try_new(keys, values).unwrap();
    assert_compatible("Dictionary<Int32, Int64>", &col(dict));
}

#[test]
fn dictionary_matches_decoded() {
    let values: ArrayRef = Arc::new(Int32Array::from(vec![10, 20, 30]));
    let keys = Int8Array::from(vec![Some(0), Some(1), Some(2), Some(0), None]);
    let dict: ArrayRef =
        Arc::new(DictionaryArray::<Int8Type>::try_new(keys, Arc::clone(&values)).unwrap());
    let decoded: ArrayRef = Arc::new(Int32Array::from(vec![
        Some(10),
        Some(20),
        Some(30),
        Some(10),
        None,
    ]));
    let from_dict = comet_kernel(&[Arc::clone(&dict)], SPARK_DEFAULT_SEED).unwrap();
    let from_decoded = comet_kernel(&[Arc::clone(&decoded)], SPARK_DEFAULT_SEED).unwrap();
    assert_eq!(from_dict, from_decoded);
    assert_compatible("Dictionary decoded equivalent", &[dict]);
    assert_compatible("decoded Int32", &[decoded]);
}

#[test]
fn dictionary_nonuniform_seeds_match_decoded() {
    let values: ArrayRef = Arc::new(Int32Array::from(vec![Some(10), Some(20), None]));
    let keys = Int8Array::from(vec![Some(0), Some(1), Some(2), None, Some(0)]);
    let dict: ArrayRef = Arc::new(DictionaryArray::<Int8Type>::try_new(keys, values).unwrap());
    let decoded: ArrayRef = Arc::new(Int32Array::from(vec![
        Some(10),
        Some(20),
        None,
        None,
        Some(10),
    ]));
    let seeds = vec![7u64, 38, 69, 100, 131];
    let mut from_dict = seeds.clone();
    create_xxhash64_hashes(&[dict], &mut from_dict).unwrap();
    let mut from_decoded = seeds;
    create_xxhash64_hashes(&[decoded], &mut from_decoded).unwrap();
    assert_eq!(from_dict, from_decoded);
}

// ---------------------------------------------------------------------------
// Struct
// ---------------------------------------------------------------------------

#[test]
fn struct_non_null() {
    assert_compatible(
        "Struct<a:Int32,b:Utf8> non-null",
        &[struct_ab(
            vec![Some(1), Some(2), Some(3)],
            vec![Some("a"), Some("b"), Some("c")],
            None,
        )],
    );
}

#[test]
fn struct_null_fields() {
    assert_compatible(
        "Struct with null fields",
        &[struct_ab(
            vec![Some(1), None, Some(3)],
            vec![None, Some("b"), Some("c")],
            None,
        )],
    );
}

/// Spark-compatible: a null struct must ignore hidden child values.
/// `SparkXxhash64` hashes the child buffers without pushing the parent null mask, so this
/// case is *not* routed upstream.
#[test]
fn null_struct_ignores_hidden_child_values_comet() {
    let fields: Fields = vec![Arc::new(Field::new("a", DataType::Int32, true))].into();
    let nulls = NullBuffer::from(vec![true, false]);
    let hidden: ArrayRef = Arc::new(StructArray::new(
        fields.clone(),
        vec![Arc::new(Int32Array::from(vec![Some(1), Some(999)])) as ArrayRef],
        Some(nulls.clone()),
    ));
    let plain: ArrayRef = Arc::new(StructArray::new(
        fields,
        vec![Arc::new(Int32Array::from(vec![Some(1), None])) as ArrayRef],
        Some(nulls),
    ));

    let mut a = vec![SPARK_DEFAULT_SEED; 2];
    create_xxhash64_hashes(&[hidden], &mut a).unwrap();
    let mut b = vec![SPARK_DEFAULT_SEED; 2];
    create_xxhash64_hashes(&[plain], &mut b).unwrap();
    assert_eq!(a, b, "a null struct must hash the same either way");
    assert_eq!(
        a[1], SPARK_DEFAULT_SEED,
        "a null struct must leave the seed untouched"
    );
}

#[test]
fn null_struct_hidden_children_diverge_from_spark_xxhash64() {
    let fields: Fields = vec![Arc::new(Field::new("a", DataType::Int32, true))].into();
    let nulls = NullBuffer::from(vec![true, false]);
    let hidden: ArrayRef = Arc::new(StructArray::new(
        fields,
        vec![Arc::new(Int32Array::from(vec![Some(1), Some(999)])) as ArrayRef],
        Some(nulls),
    ));
    let comet = comet_kernel(&[Arc::clone(&hidden)], SPARK_DEFAULT_SEED).unwrap();
    let upstream = spark_xxhash64_upstream(&[hidden]).unwrap();
    assert_eq!(comet[0], upstream[0], "non-null struct row still matches");
    assert_eq!(comet[1], SPARK_DEFAULT_SEED);
    assert_ne!(
        comet[1], upstream[1],
        "SparkXxhash64 hashes hidden child values of a NULL struct"
    );
}

// ---------------------------------------------------------------------------
// List / LargeList / FixedSizeList
// ---------------------------------------------------------------------------

#[test]
fn list_int32() {
    assert_compatible(
        "List<Int32>",
        &[list_i32(vec![
            Some(vec![Some(1), Some(2)]),
            Some(vec![]),
            None,
            Some(vec![Some(1), None, Some(3)]),
            Some(vec![Some(-1)]),
        ])],
    );
}

#[test]
fn large_list_int32() {
    assert_compatible(
        "LargeList<Int32>",
        &[large_list_i32(vec![
            Some(vec![Some(1), Some(2)]),
            Some(vec![]),
            None,
            Some(vec![Some(1), None, Some(3)]),
        ])],
    );
}

#[test]
fn fixed_size_list_int32() {
    let values = Int32Array::from(vec![
        Some(1),
        Some(2),
        Some(3),
        Some(4),
        None,
        Some(6),
        Some(0),
        Some(0),
        Some(0),
    ]);
    let mut validity = arrow::array::BooleanBufferBuilder::new(3);
    validity.append(true);
    validity.append(true);
    validity.append(false);
    let array = FixedSizeListArray::new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        3,
        Arc::new(values),
        Some(NullBuffer::new(validity.finish())),
    );
    assert_compatible("FixedSizeList<Int32>", &col(array));
}

#[test]
fn nested_list() {
    let inner = ListBuilder::new(Int32Builder::new());
    let mut outer = ListBuilder::new(inner);
    // [[1,2],[3]]
    {
        let inner = outer.values();
        inner.values().append_value(1);
        inner.values().append_value(2);
        inner.append(true);
        inner.values().append_value(3);
        inner.append(true);
        outer.append(true);
    }
    // []
    outer.append(true);
    // NULL
    outer.append(false);
    assert_compatible("List<List<Int32>>", &[Arc::new(outer.finish())]);
}

// ---------------------------------------------------------------------------
// Map
// ---------------------------------------------------------------------------

#[test]
fn map_utf8_int32() {
    assert_compatible(
        "Map<Utf8,Int32>",
        &[map_utf8_i32(vec![
            Some(vec![("a", Some(1)), ("b", Some(2))]),
            Some(vec![]),
            None,
            Some(vec![("k", None)]),
            Some(vec![("x", Some(0)), ("y", Some(-1))]),
        ])],
    );
}

#[test]
fn map_int32_utf8() {
    assert_compatible(
        "Map<Int32,Utf8>",
        &[map_i32_utf8(vec![
            Some(vec![(1, Some("a")), (2, Some("b"))]),
            Some(vec![]),
            None,
            Some(vec![(0, None)]),
            Some(vec![(-1, Some("")), (3, Some("x"))]),
        ])],
    );
}

#[test]
fn map_utf8_to_utf8() {
    assert_compatible(
        "Map<Utf8,Utf8>",
        &[map_utf8_utf8(vec![
            Some(vec![("a", Some("x")), ("b", Some("y"))]),
            Some(vec![]),
            None,
            Some(vec![("k", None)]),
            Some(vec![("empty", Some("")), ("z", Some("zz"))]),
        ])],
    );
}

#[test]
fn map_int32_int32() {
    assert_compatible(
        "Map<Int32,Int32>",
        &[map_i32_i32(vec![
            Some(vec![(1, Some(10)), (2, Some(20))]),
            Some(vec![]),
            None,
            Some(vec![(0, None)]),
            Some(vec![(-1, Some(0)), (3, Some(-3))]),
        ])],
    );
}

#[test]
fn map_utf8_decimal128_small() {
    assert_compatible(
        "Map<Utf8,Decimal128(10,2)>",
        &[map_utf8_decimal(
            10,
            2,
            vec![
                Some(vec![("a", Some(123)), ("b", Some(-4))]),
                Some(vec![]),
                None,
                Some(vec![("k", None)]),
            ],
        )],
    );
}

#[test]
fn map_utf8_decimal128_large() {
    let wide = 10_000_000_000_000_000_000i128;
    assert_compatible(
        "Map<Utf8,Decimal128(20,2)>",
        &[map_utf8_decimal(
            20,
            2,
            vec![
                Some(vec![("a", Some(wide)), ("b", Some(-wide))]),
                Some(vec![]),
                None,
            ],
        )],
    );
}

// ---------------------------------------------------------------------------
// Nested combinations
// ---------------------------------------------------------------------------

#[test]
fn list_of_struct_non_null() {
    let mut lb = ListBuilder::new(StructBuilder::new(
        vec![
            Arc::new(Field::new("a", DataType::Int32, true)),
            Arc::new(Field::new("b", DataType::Utf8, true)),
        ],
        vec![
            Box::new(Int32Builder::new()),
            Box::new(StringBuilder::new()),
        ],
    ));
    for (a, b) in [(1, "x"), (2, "y")] {
        let sb = lb.values();
        sb.field_builder::<Int32Builder>(0).unwrap().append_value(a);
        sb.field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value(b);
        sb.append(true);
    }
    lb.append(true);
    lb.append(true); // empty list
    lb.append(false); // null list
    assert_compatible("List<Struct> non-null elements", &[Arc::new(lb.finish())]);
}

#[test]
fn struct_of_list() {
    let list = list_i32(vec![
        Some(vec![Some(1), Some(2)]),
        Some(vec![]),
        None,
        Some(vec![Some(3)]),
    ]);
    let fields: Fields = vec![Arc::new(Field::new("xs", list.data_type().clone(), true))].into();
    let array: ArrayRef = Arc::new(StructArray::new(fields, vec![list], None));
    assert_compatible("Struct<List<Int32>>", &[array]);
}

#[test]
fn list_of_dictionary_is_incompatible() {
    let values: ArrayRef = Arc::new(Int32Array::from(vec![10, 20]));
    let keys = Int8Array::from(vec![0i8, 1]);
    let dict: ArrayRef = Arc::new(DictionaryArray::<Int8Type>::try_new(keys, values).unwrap());
    let as_list = |elems: ArrayRef| -> ArrayRef {
        Arc::new(ListArray::new(
            Arc::new(Field::new("item", elems.data_type().clone(), true)),
            OffsetBuffer::new(vec![0i32, 2].into()),
            elems,
            None,
        ))
    };
    let decoded: ArrayRef = Arc::new(Int32Array::from(vec![10, 20]));
    let comet_dict = comet_kernel(&[as_list(Arc::clone(&dict))], SPARK_DEFAULT_SEED).unwrap();
    let comet_decoded = comet_kernel(&[as_list(decoded)], SPARK_DEFAULT_SEED).unwrap();
    assert_eq!(
        comet_dict, comet_decoded,
        "Comet hashes a dictionary list element as its decoded values"
    );

    let upstream = spark_xxhash64_upstream(&[as_list(dict)]).unwrap();
    assert_ne!(
        comet_dict, upstream,
        "SparkXxhash64 restarts nested dictionary hashes from seed 42"
    );
}

#[test]
fn struct_of_dictionary() {
    let values: ArrayRef = Arc::new(StringArray::from(vec!["hello", "world"]));
    let keys = Int32Array::from(vec![Some(0), Some(1), Some(0), None]);
    let dict: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::try_new(keys, values).unwrap());
    let fields: Fields = vec![Arc::new(Field::new("d", dict.data_type().clone(), true))].into();
    let array: ArrayRef = Arc::new(StructArray::new(fields, vec![dict], None));
    assert_compatible("Struct<Dictionary<Int32,Utf8>> non-null", &[array]);
}

// ---------------------------------------------------------------------------
// Seed / multi-column chaining
// ---------------------------------------------------------------------------

#[test]
fn multi_column_chain_matches_sequential() {
    let a: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), Some(2), None, Some(-1)]));
    let b: ArrayRef = Arc::new(StringArray::from(vec![
        Some("x"),
        None,
        Some("y"),
        Some("z"),
    ]));
    let c: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(1.5),
        Some(-0.0),
        Some(0.0),
        None,
    ]));
    let cols = [Arc::clone(&a), Arc::clone(&b), Arc::clone(&c)];
    assert_compatible("multi-column Int32,Utf8,Float64", &cols);

    let chained = comet_kernel(&cols, SPARK_DEFAULT_SEED).unwrap();
    let mut sequential = vec![SPARK_DEFAULT_SEED; a.len()];
    create_xxhash64_hashes(&[a], &mut sequential).unwrap();
    create_xxhash64_hashes(&[b], &mut sequential).unwrap();
    create_xxhash64_hashes(&[c], &mut sequential).unwrap();
    assert_eq!(
        chained, sequential,
        "hash(a,b,c) must fold each argument into the running seed"
    );
}

#[test]
fn custom_seed_is_honored_by_comet() {
    let array: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), Some(0), None, Some(-1)]));
    for seed in [0i64, 1, 7, 42, -1, i64::MIN] {
        let expr = comet_expr(&[Arc::clone(&array)], seed).unwrap();
        let kernel = comet_kernel(&[Arc::clone(&array)], seed as u64).unwrap();
        assert_eq!(expr, kernel, "seed={seed}");
        if seed as u64 != SPARK_DEFAULT_SEED {
            let default = comet_kernel(&[Arc::clone(&array)], SPARK_DEFAULT_SEED).unwrap();
            assert_ne!(expr, default, "custom seed {seed} must change the hash");
        }
    }
}

#[test]
fn row_dependent_starting_seeds() {
    let array: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), Some(1), Some(1)]));
    let mut hashes = vec![1u64, 2, 3];
    create_xxhash64_hashes(&[array], &mut hashes).unwrap();
    assert_ne!(hashes[0], hashes[1]);
    assert_ne!(hashes[1], hashes[2]);
}

#[test]
fn spark_xxhash64_does_not_take_a_trailing_seed_argument() {
    // A trailing seed scalar is a Comet UDF convention. Passing it to SparkXxhash64 would
    // hash the seed as another column, starting from 42.
    let array: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), Some(2)]));
    let comet = comet_expr(&[Arc::clone(&array)], 7).unwrap();
    let upstream_default = spark_xxhash64_upstream(&[array]).unwrap();
    assert_ne!(comet, upstream_default);
}

// ---------------------------------------------------------------------------
// Types Comet supports that SparkXxhash64 does not
// ---------------------------------------------------------------------------

#[test]
fn time64_nanosecond_is_comet_only() {
    let array: ArrayRef = Arc::new(Time64NanosecondArray::from(vec![
        Some(0),
        Some(1_000),
        None,
        Some(-1),
    ]));
    let comet =
        comet_kernel(&[Arc::clone(&array)], SPARK_DEFAULT_SEED).expect("Comet hashes Time64(ns)");
    let expr = comet_expr(&[Arc::clone(&array)], SPARK_DEFAULT_SEED as i64).unwrap();
    assert_eq!(expr, comet);
    let upstream = spark_xxhash64_upstream(&[array]);
    assert!(
        upstream.is_err(),
        "SparkXxhash64 is not expected to hash Time64: {upstream:?}"
    );
}

/// After routing compatible types to `SparkXxhash64`, null structs must still use the Comet
/// kernel so hidden child values do not affect the hash.
#[test]
fn null_struct_expression_matches_comet_kernel() {
    let fields: Fields = vec![Arc::new(Field::new("a", DataType::Int32, true))].into();
    let nulls = NullBuffer::from(vec![true, false]);
    let hidden: ArrayRef = Arc::new(StructArray::new(
        fields,
        vec![Arc::new(Int32Array::from(vec![Some(1), Some(999)])) as ArrayRef],
        Some(nulls),
    ));
    let kernel = comet_kernel(&[Arc::clone(&hidden)], SPARK_DEFAULT_SEED).unwrap();
    let expr = comet_expr(&[Arc::clone(&hidden)], SPARK_DEFAULT_SEED as i64).unwrap();
    assert_eq!(expr, kernel);
    assert_eq!(expr[1], SPARK_DEFAULT_SEED);
    let upstream = spark_xxhash64_upstream(&[hidden]).unwrap();
    assert_ne!(expr[1], upstream[1]);
}

#[test]
fn list_of_dictionary_expression_matches_comet_kernel() {
    let values: ArrayRef = Arc::new(Int32Array::from(vec![10, 20]));
    let keys = Int8Array::from(vec![0i8, 1]);
    let dict: ArrayRef = Arc::new(DictionaryArray::<Int8Type>::try_new(keys, values).unwrap());
    let list: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", dict.data_type().clone(), true)),
        OffsetBuffer::new(vec![0i32, 2].into()),
        dict,
        None,
    ));
    let kernel = comet_kernel(&[Arc::clone(&list)], SPARK_DEFAULT_SEED).unwrap();
    let expr = comet_expr(&[list], SPARK_DEFAULT_SEED as i64).unwrap();
    assert_eq!(expr, kernel);
}
