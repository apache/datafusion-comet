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

//! Iceberg's `truncate(width, value)` transform: `v - ((v % W) + W) % W` for integers (with
//! Java's wrapping arithmetic), the same on the unscaled value for decimals, the first `W` code
//! points of a string, and the first `W` bytes of a binary value.

use super::{apply_unary, positive_int_param, unpacked_type, unsupported_type};
use crate::utils::is_valid_decimal_precision;
use arrow::array::{Array, ArrayRef, AsArray, Decimal128Array, OffsetSizeTrait};
use arrow::compute::kernels::substring::{substring, substring_by_char};
use arrow::datatypes::{DataType, Decimal128Type, Int16Type, Int32Type, Int64Type, Int8Type};
use datafusion::common::{utils::take_function_args, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;

/// `TruncateUtil.truncateInt`. Java's `int` arithmetic wraps on overflow, which can happen both in
/// `(v % w) + w` (for widths above 2^30) and in the final subtraction (near `Integer.MIN_VALUE`).
/// `TruncateUtil.truncateByte` / `truncateShort` evaluate the same expression in `int` and then
/// narrow, so tinyint and smallint inputs go through this function and are cast afterwards.
#[inline]
fn truncate_i32(v: i32, w: i32) -> i32 {
    v.wrapping_sub((v % w).wrapping_add(w) % w)
}

/// `TruncateUtil.truncateLong`, with the width promoted to `long` as Java does.
#[inline]
fn truncate_i64(v: i64, w: i64) -> i64 {
    v.wrapping_sub((v % w).wrapping_add(w) % w)
}

/// `TruncateUtil.truncateDecimal` on the unscaled value; `BigInteger` never overflows and neither
/// does an `i128` holding a 38-digit unscaled value minus a 31-bit width.
#[inline]
fn truncate_i128(v: i128, w: i128) -> i128 {
    v - ((v % w) + w) % w
}

/// `UTF8String.substring(0, width)` counts code points, not bytes. A width that covers the whole
/// values buffer cannot truncate anything, so the input is returned as is instead of being copied.
fn truncate_string<O: OffsetSizeTrait>(array: &ArrayRef, width: i32) -> Result<ArrayRef> {
    let strings = array.as_string::<O>();
    if width as usize >= strings.value_data().len() {
        return Ok(Arc::clone(array));
    }
    Ok(Arc::new(substring_by_char(strings, 0, Some(width as u64))?))
}

/// `BinaryUtil.truncateBinaryUnsafe` keeps the first `width` bytes. The whole-buffer shortcut
/// matters here beyond avoiding a copy: Arrow's byte `substring` adds the length to each value's
/// offset without checking for overflow, which panics for a width near `i32::MAX`.
fn truncate_binary<O: OffsetSizeTrait>(array: &ArrayRef, width: i32) -> Result<ArrayRef> {
    if width as usize >= array.as_binary::<O>().value_data().len() {
        return Ok(Arc::clone(array));
    }
    Ok(substring(array.as_ref(), 0, Some(width as u64))?)
}

fn truncate_array(fn_name: &str, array: &ArrayRef, width: i32) -> Result<ArrayRef> {
    let result: ArrayRef = match array.data_type() {
        DataType::Int8 => Arc::new(
            array
                .as_primitive::<Int8Type>()
                .unary::<_, Int8Type>(|v| truncate_i32(v as i32, width) as i8),
        ),
        DataType::Int16 => Arc::new(
            array
                .as_primitive::<Int16Type>()
                .unary::<_, Int16Type>(|v| truncate_i32(v as i32, width) as i16),
        ),
        DataType::Int32 => Arc::new(
            array
                .as_primitive::<Int32Type>()
                .unary::<_, Int32Type>(|v| truncate_i32(v, width)),
        ),
        DataType::Int64 => Arc::new(
            array
                .as_primitive::<Int64Type>()
                .unary::<_, Int64Type>(|v| truncate_i64(v, width as i64)),
        ),
        DataType::Decimal128(precision, scale) => {
            // Truncating a negative value grows its magnitude by up to `width - 1` units of the
            // last digit, so the result can need one more digit than the column allows. Iceberg's
            // `TruncateDecimal.invoke` hands that oversized `Decimal` back to Spark unchanged and
            // Spark nulls it only when a row is materialized (`UnsafeRowWriter` calls
            // `changePrecision`, which fails). Nulling it here is the same answer for every path
            // that writes the value into a row -- a projection, a sort key, a shuffle key, an
            // Iceberg partition value -- and it is the only answer available to a kernel that has
            // to return a `Decimal128(precision, scale)` array. The two differ where the result
            // feeds another expression without being materialized, e.g. `truncate(w, v) IS NULL`;
            // see the Iceberg user guide.
            let truncated: Decimal128Array =
                array.as_primitive::<Decimal128Type>().unary_opt(|v| {
                    let truncated = truncate_i128(v, width as i128);
                    is_valid_decimal_precision(truncated, *precision).then_some(truncated)
                });
            Arc::new(truncated.with_precision_and_scale(*precision, *scale)?)
        }
        DataType::Utf8 => truncate_string::<i32>(array, width)?,
        DataType::LargeUtf8 => truncate_string::<i64>(array, width)?,
        DataType::Binary => truncate_binary::<i32>(array, width)?,
        DataType::LargeBinary => truncate_binary::<i64>(array, width)?,
        other => return Err(unsupported_type(fn_name, other)),
    };
    Ok(result)
}

/// `iceberg_truncate(width, value)`; see the module docs for the semantics. The result has the
/// same type as `value`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkIcebergTruncate {
    signature: Signature,
}

impl SparkIcebergTruncate {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl Default for SparkIcebergTruncate {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for SparkIcebergTruncate {
    fn name(&self) -> &str {
        "iceberg_truncate"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [_width, value] = take_function_args(self.name(), arg_types)?;
        Ok(unpacked_type(value))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [width, value] = take_function_args(self.name(), &args.args)?;
        let width = positive_int_param(self.name(), "width", width)?;
        apply_unary(value, |array| truncate_array(self.name(), array, width))
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_util::invoke;
    use super::*;
    use arrow::array::{
        BinaryArray, DictionaryArray, Int16Array, Int32Array, Int64Array, Int8Array, StringArray,
    };
    use arrow::datatypes::Int8Type;
    use datafusion::common::ScalarValue;

    fn truncate(width: i32, value: ArrayRef) -> ArrayRef {
        invoke(
            &SparkIcebergTruncate::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Int32(Some(width))),
                ColumnarValue::Array(value),
            ],
        )
        .unwrap()
    }

    /// Examples from the Iceberg spec: truncate(10, 1) = 0, truncate(10, -1) = -10,
    /// truncate(50, 10.65) = 10.50, truncate(3, "iceberg") = "ice".
    #[test]
    fn matches_iceberg_spec_examples() {
        let ints = truncate(
            10,
            Arc::new(Int32Array::from(vec![Some(1), Some(-1), None])),
        );
        assert_eq!(
            ints.as_primitive::<Int32Type>(),
            &Int32Array::from(vec![Some(0), Some(-10), None])
        );
        let longs = truncate(
            10,
            Arc::new(Int64Array::from(vec![Some(1), Some(-1), None])),
        );
        assert_eq!(
            longs.as_primitive::<Int64Type>(),
            &Int64Array::from(vec![Some(0), Some(-10), None])
        );
        let decimals = truncate(
            50,
            Arc::new(
                Decimal128Array::from(vec![Some(1065), Some(-1065), None])
                    .with_precision_and_scale(4, 2)
                    .unwrap(),
            ),
        );
        assert_eq!(decimals.data_type(), &DataType::Decimal128(4, 2));
        assert_eq!(
            decimals.as_primitive::<Decimal128Type>().values().as_ref(),
            &[1050, -1100, 0]
        );
        assert!(decimals.is_null(2));
        // -99.99 truncated to a width of 10 is -100.00, which does not fit decimal(4, 2); Spark
        // produces null for it.
        let overflow = truncate(
            10,
            Arc::new(
                Decimal128Array::from(vec![Some(-9999), Some(9999), Some(-9990)])
                    .with_precision_and_scale(4, 2)
                    .unwrap(),
            ),
        );
        assert!(overflow.is_null(0));
        assert_eq!(overflow.as_primitive::<Decimal128Type>().value(1), 9990);
        assert_eq!(overflow.as_primitive::<Decimal128Type>().value(2), -9990);
        let strings = truncate(
            3,
            Arc::new(StringArray::from(vec![
                Some("iceberg"),
                Some("ic"),
                Some(""),
                Some("日本語テキスト"),
                Some("a😀b😀c"),
                None,
            ])),
        );
        assert_eq!(
            strings.as_string::<i32>(),
            &StringArray::from(vec![
                Some("ice"),
                Some("ic"),
                Some(""),
                Some("日本語"),
                Some("a😀b"),
                None
            ])
        );
        let binary = truncate(
            3,
            Arc::new(BinaryArray::from(vec![
                Some([1u8, 2, 3, 4, 5].as_slice()),
                Some([1u8].as_slice()),
                None,
            ])),
        );
        assert_eq!(
            binary.as_binary::<i32>(),
            &BinaryArray::from(vec![
                Some([1u8, 2, 3].as_slice()),
                Some([1u8].as_slice()),
                None
            ])
        );
    }

    /// Java narrows the `int` result back to `byte` / `short` and wraps `int` / `long` overflow.
    #[test]
    fn matches_java_wrapping_arithmetic() {
        let bytes = truncate(
            1000,
            Arc::new(Int8Array::from(vec![
                Some(i8::MIN),
                Some(i8::MAX),
                Some(-1),
            ])),
        );
        assert_eq!(
            bytes.as_primitive::<Int8Type>(),
            &Int8Array::from(vec![Some(24), Some(0), Some(-1000i32 as i8)])
        );
        let shorts = truncate(
            100_000,
            Arc::new(Int16Array::from(vec![Some(i16::MIN), Some(i16::MAX)])),
        );
        assert_eq!(
            shorts.as_primitive::<Int16Type>(),
            &Int16Array::from(vec![Some(-100_000i32 as i16), Some(0)])
        );
        let ints = truncate(1000, Arc::new(Int32Array::from(vec![i32::MIN, i32::MAX])));
        assert_eq!(
            ints.as_primitive::<Int32Type>(),
            &Int32Array::from(vec![i32::MIN.wrapping_sub(352), 2_147_483_000])
        );
        let wide = truncate(i32::MAX, Arc::new(Int32Array::from(vec![i32::MAX - 1, -2])));
        // Java: (v % w) + w overflows for v = MAX - 1, then wraps back through % w.
        let w = i32::MAX;
        let expected = |v: i32| v.wrapping_sub((v % w).wrapping_add(w) % w);
        assert_eq!(
            wide.as_primitive::<Int32Type>(),
            &Int32Array::from(vec![expected(i32::MAX - 1), expected(-2)])
        );
        let longs = truncate(1000, Arc::new(Int64Array::from(vec![i64::MIN, i64::MAX])));
        assert_eq!(
            longs.as_primitive::<Int64Type>(),
            &Int64Array::from(vec![
                // i64::MIN % 1000 == -808, so the wrapped remainder is 192.
                i64::MIN.wrapping_sub(192),
                9_223_372_036_854_775_000
            ])
        );
    }

    /// A width larger than any value is a no-op for strings and binary, and must not trip
    /// Arrow's offset arithmetic (`i32::MAX` plus a non-zero offset overflows there).
    #[test]
    fn huge_width_leaves_strings_and_binary_unchanged() {
        let strings: ArrayRef = Arc::new(StringArray::from(vec![
            Some("iceberg"),
            None,
            Some("日本語"),
            Some(""),
        ]));
        let binary: ArrayRef = Arc::new(BinaryArray::from(vec![
            Some([1u8, 2, 3].as_slice()),
            None,
            Some([4u8, 5].as_slice()),
            Some([].as_slice()),
        ]));
        for width in [10, 1000, i32::MAX] {
            assert_eq!(
                truncate(width, Arc::clone(&strings)).as_ref(),
                strings.as_ref()
            );
            assert_eq!(
                truncate(width, Arc::clone(&binary)).as_ref(),
                binary.as_ref()
            );
        }
        // The same widths still truncate rows that are longer than the width.
        let long = truncate(
            5,
            Arc::new(StringArray::from(vec![Some("ab"), Some("abcdefgh"), None])),
        );
        assert_eq!(
            long.as_string::<i32>(),
            &StringArray::from(vec![Some("ab"), Some("abcde"), None])
        );
    }

    /// A dictionary is truncated once per distinct value and expanded through the keys, so the
    /// result is a plain array of the value type, as [`SparkIcebergTruncate::return_type`] says.
    #[test]
    fn dictionary_input_is_truncated_once_per_value() {
        let dict: DictionaryArray<Int8Type> =
            vec![Some("iceberg"), None, Some("ic"), Some("iceberg")]
                .into_iter()
                .collect();
        let result = truncate(3, Arc::new(dict));
        assert_eq!(result.data_type(), &DataType::Utf8);
        assert_eq!(
            result.as_string::<i32>(),
            &StringArray::from(vec![Some("ice"), None, Some("ic"), Some("ice")])
        );
        // The whole-buffer shortcut has to survive the round trip through the keys too.
        let unchanged = truncate(i32::MAX, Arc::new(dict_of(&[Some("ab"), None, Some("ab")])));
        assert_eq!(
            unchanged.as_string::<i32>(),
            &StringArray::from(vec![Some("ab"), None, Some("ab")])
        );
    }

    fn dict_of(values: &[Option<&str>]) -> DictionaryArray<Int8Type> {
        values.iter().copied().collect()
    }

    #[test]
    fn return_type_follows_value_type() {
        let udf = SparkIcebergTruncate::new();
        assert_eq!(
            udf.return_type(&[DataType::Int32, DataType::Decimal128(10, 2)])
                .unwrap(),
            DataType::Decimal128(10, 2)
        );
        assert_eq!(
            udf.return_type(&[
                DataType::Int32,
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
            ])
            .unwrap(),
            DataType::Utf8
        );
    }

    #[test]
    fn rejects_non_positive_width_and_unsupported_types() {
        let err = invoke(
            &SparkIcebergTruncate::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Int32(Some(0))),
                ColumnarValue::Array(Arc::new(Int32Array::from(vec![1]))),
            ],
        )
        .unwrap_err();
        assert!(err
            .to_string()
            .contains("width must be a positive Int32 literal"));
        let err = invoke(
            &SparkIcebergTruncate::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Int32(Some(1))),
                ColumnarValue::Array(Arc::new(arrow::array::Date32Array::from(vec![1]))),
            ],
        )
        .unwrap_err();
        assert!(err
            .to_string()
            .contains("does not support input type Date32"));
    }
}
