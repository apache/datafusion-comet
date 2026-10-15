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

//! Iceberg's `bucket(numBuckets, value)` transform:
//! `(murmur3_32(bytes(value)) & Integer.MAX_VALUE) % numBuckets`, where `bytes(value)` is the
//! encoding from Appendix B of the Iceberg spec (8-byte little-endian for integers, dates, and
//! timestamps; UTF-8 for strings; raw bytes for binary; minimal big-endian two's complement of
//! the unscaled value for decimals).

use super::{apply_unary, positive_int_param, unsupported_type};
use arrow::array::{ArrayRef, AsArray, Int32Array};
use arrow::datatypes::{
    DataType, Date32Type, Decimal128Type, Int16Type, Int32Type, Int64Type, Int8Type, TimeUnit,
    TimestampMicrosecondType,
};
use datafusion::common::{utils::take_function_args, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;

/// 32-bit MurmurHash3 (x86 variant) with seed 0, matching Guava's `Hashing.murmur3_32_fixed()`
/// that Iceberg's `BucketUtil` hashes with.
///
/// Comet's Spark-compatible murmur3 (`spark_compatible_murmur3_hash`) cannot be reused: Spark
/// mixes the trailing 1 to 3 bytes into the hash one byte at a time, whereas the reference
/// algorithm packs them into a single little-endian word first, so the two disagree on every
/// input whose length is not a multiple of four.
pub(crate) fn murmur3_32(data: &[u8]) -> i32 {
    const C1: u32 = 0xcc9e_2d51;
    const C2: u32 = 0x1b87_3593;

    #[inline]
    fn mix_k1(k1: u32) -> u32 {
        k1.wrapping_mul(C1).rotate_left(15).wrapping_mul(C2)
    }

    let mut h1: u32 = 0;
    let (chunks, tail) = data.as_chunks::<4>();
    for chunk in chunks {
        h1 ^= mix_k1(u32::from_le_bytes(*chunk));
        h1 = h1.rotate_left(13).wrapping_mul(5).wrapping_add(0xe654_6b64);
    }
    if !tail.is_empty() {
        let mut k1: u32 = 0;
        for (i, byte) in tail.iter().enumerate() {
            k1 |= (*byte as u32) << (8 * i);
        }
        h1 ^= mix_k1(k1);
    }
    h1 ^= data.len() as u32;
    h1 ^= h1 >> 16;
    h1 = h1.wrapping_mul(0x85eb_ca6b);
    h1 ^= h1 >> 13;
    h1 = h1.wrapping_mul(0xc2b2_ae35);
    h1 ^= h1 >> 16;
    h1 as i32
}

/// `BucketUtil.hash(long)`: ints, longs, dates, and timestamps all hash as 8 little-endian bytes.
#[inline]
fn hash_long(value: i64) -> i32 {
    murmur3_32(&value.to_le_bytes())
}

/// `BucketUtil.hash(BigDecimal)`: hashes `unscaledValue().toByteArray()`, the shortest big-endian
/// two's complement encoding of the unscaled value. That keeps exactly one sign bit, so the byte
/// count is one more than the bit length left after the run of leading sign bits.
#[inline]
fn hash_decimal(unscaled: i128) -> i32 {
    let sign_bits = if unscaled < 0 {
        unscaled.leading_ones()
    } else {
        unscaled.leading_zeros()
    };
    let skip = ((sign_bits - 1) / 8) as usize;
    murmur3_32(&unscaled.to_be_bytes()[skip..])
}

/// Buckets string or binary values by their raw bytes, keeping nulls.
fn bucket_bytes<'a, B: AsRef<[u8]> + ?Sized + 'a>(
    values: impl Iterator<Item = Option<&'a B>>,
    bucket: impl Fn(i32) -> i32,
) -> Int32Array {
    values
        .map(|v| v.map(|b| bucket(murmur3_32(b.as_ref()))))
        .collect()
}

fn bucket_array(fn_name: &str, array: &ArrayRef, num_buckets: i32) -> Result<ArrayRef> {
    let bucket = |hash: i32| (hash & i32::MAX) % num_buckets;
    let result: Int32Array = match array.data_type() {
        // Iceberg binds tinyint and smallint inputs to `BucketInt`, hashing them as ints.
        DataType::Int8 => array
            .as_primitive::<Int8Type>()
            .unary(|v| bucket(hash_long(v as i64))),
        DataType::Int16 => array
            .as_primitive::<Int16Type>()
            .unary(|v| bucket(hash_long(v as i64))),
        DataType::Int32 => array
            .as_primitive::<Int32Type>()
            .unary(|v| bucket(hash_long(v as i64))),
        DataType::Date32 => array
            .as_primitive::<Date32Type>()
            .unary(|v| bucket(hash_long(v as i64))),
        DataType::Int64 => array
            .as_primitive::<Int64Type>()
            .unary(|v| bucket(hash_long(v))),
        DataType::Timestamp(TimeUnit::Microsecond, _) => array
            .as_primitive::<TimestampMicrosecondType>()
            .unary(|v| bucket(hash_long(v))),
        DataType::Decimal128(_, _) => array
            .as_primitive::<Decimal128Type>()
            .unary(|v| bucket(hash_decimal(v))),
        DataType::Utf8 => bucket_bytes(array.as_string::<i32>().iter(), bucket),
        DataType::LargeUtf8 => bucket_bytes(array.as_string::<i64>().iter(), bucket),
        DataType::Binary => bucket_bytes(array.as_binary::<i32>().iter(), bucket),
        DataType::LargeBinary => bucket_bytes(array.as_binary::<i64>().iter(), bucket),
        DataType::FixedSizeBinary(_) => bucket_bytes(array.as_fixed_size_binary().iter(), bucket),
        other => return Err(unsupported_type(fn_name, other)),
    };
    Ok(Arc::new(result))
}

/// `iceberg_bucket(numBuckets, value)`; see the module docs for the semantics.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkIcebergBucket {
    signature: Signature,
}

impl SparkIcebergBucket {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl Default for SparkIcebergBucket {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for SparkIcebergBucket {
    fn name(&self) -> &str {
        "iceberg_bucket"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [num_buckets, value] = take_function_args(self.name(), &args.args)?;
        let num_buckets = positive_int_param(self.name(), "numBuckets", num_buckets)?;
        apply_unary(value, |array| bucket_array(self.name(), array, num_buckets))
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_util::invoke;
    use super::*;
    use arrow::array::{
        Array, BinaryArray, Date32Array, Decimal128Array, DictionaryArray, Int16Array, Int32Array,
        Int64Array, Int8Array, StringArray, TimestampMicrosecondArray,
    };
    use datafusion::common::ScalarValue;

    /// Hash values from Appendix B of the Iceberg table spec.
    #[test]
    fn hashes_match_iceberg_spec_vectors() {
        assert_eq!(hash_long(34), 2_017_239_379);
        assert_eq!(hash_decimal(1420), -500_754_589); // decimal 14.20
        assert_eq!(hash_long(17_486), -653_330_422); // date 2017-11-16
        assert_eq!(hash_long(81_068_000_000), -662_762_989); // time 22:31:08
        assert_eq!(hash_long(1_510_871_468_000_000), -2_047_944_441); // 2017-11-16T22:31:08
        assert_eq!(murmur3_32("iceberg".as_bytes()), 1_210_000_089);
        assert_eq!(murmur3_32(&[0x00, 0x01, 0x02, 0x03]), -188_683_207);
        assert_eq!(
            murmur3_32(&0xf79c_3e09_677c_4bbd_a479_3f34_9cb7_85e7_u128.to_be_bytes()),
            1_488_055_340
        ); // uuid f79c3e09-677c-4bbd-a479-3f349cb785e7
    }

    /// `BigInteger.toByteArray()` keeps exactly one sign byte.
    #[test]
    fn decimal_hash_uses_minimal_two_complement_bytes() {
        assert_eq!(hash_decimal(0), murmur3_32(&[0x00]));
        assert_eq!(hash_decimal(-1), murmur3_32(&[0xFF]));
        assert_eq!(hash_decimal(127), murmur3_32(&[0x7F]));
        assert_eq!(hash_decimal(128), murmur3_32(&[0x00, 0x80]));
        assert_eq!(hash_decimal(-128), murmur3_32(&[0x80]));
        assert_eq!(hash_decimal(-129), murmur3_32(&[0xFF, 0x7F]));
        assert_eq!(
            hash_decimal(i128::MAX),
            murmur3_32(&i128::MAX.to_be_bytes())
        );
        assert_eq!(
            hash_decimal(i128::MIN),
            murmur3_32(&i128::MIN.to_be_bytes())
        );
    }

    fn bucket(num_buckets: i32, value: ArrayRef) -> Int32Array {
        let result = invoke(
            &SparkIcebergBucket::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Int32(Some(num_buckets))),
                ColumnarValue::Array(value),
            ],
        )
        .unwrap();
        result.as_primitive::<Int32Type>().clone()
    }

    #[test]
    fn buckets_every_supported_type_and_keeps_nulls() {
        // bucket(100, 34) -> 79 is the example in Iceberg's function description, and every
        // integer width hashes the same 8 little-endian bytes.
        let expected_34 = Int32Array::from(vec![Some(79), None]);
        assert_eq!(
            bucket(100, Arc::new(Int8Array::from(vec![Some(34), None]))),
            expected_34
        );
        assert_eq!(
            bucket(100, Arc::new(Int16Array::from(vec![Some(34), None]))),
            expected_34
        );
        assert_eq!(
            bucket(100, Arc::new(Int32Array::from(vec![Some(34), None]))),
            expected_34
        );
        assert_eq!(
            bucket(100, Arc::new(Int64Array::from(vec![Some(34), None]))),
            expected_34
        );

        let expected = |hash: i32| (hash & i32::MAX) % 16;
        let dates = bucket(16, Arc::new(Date32Array::from(vec![Some(17_486), None])));
        assert_eq!(dates.value(0), expected(-653_330_422));
        assert!(dates.is_null(1));
        let timestamps = bucket(
            16,
            Arc::new(
                TimestampMicrosecondArray::from(vec![Some(1_510_871_468_000_000), None])
                    .with_timezone("America/Los_Angeles"),
            ),
        );
        assert_eq!(timestamps.value(0), expected(-2_047_944_441));
        let ntz = bucket(
            16,
            Arc::new(TimestampMicrosecondArray::from(vec![Some(
                1_510_871_468_000_000,
            )])),
        );
        assert_eq!(ntz.value(0), expected(-2_047_944_441));
        let decimals = bucket(
            16,
            Arc::new(
                Decimal128Array::from(vec![Some(1420), None])
                    .with_precision_and_scale(4, 2)
                    .unwrap(),
            ),
        );
        assert_eq!(decimals.value(0), expected(-500_754_589));
        let strings = bucket(
            16,
            Arc::new(StringArray::from(vec![Some("iceberg"), None, Some("")])),
        );
        assert_eq!(strings.value(0), expected(1_210_000_089));
        assert!(strings.is_null(1));
        assert_eq!(strings.value(2), expected(murmur3_32(&[])));
        let binary = bucket(
            16,
            Arc::new(BinaryArray::from(vec![
                Some([0x00u8, 0x01, 0x02, 0x03].as_slice()),
                None,
            ])),
        );
        assert_eq!(binary.value(0), expected(-188_683_207));
    }

    #[test]
    fn negative_hashes_never_produce_negative_buckets() {
        // hash_long(17_486) is negative; masking with Integer.MAX_VALUE keeps the result in range.
        let dates = bucket(7, Arc::new(Date32Array::from(vec![17_486])));
        assert_eq!(dates.value(0), (-653_330_422_i32 & i32::MAX) % 7);
        assert!(dates.value(0) >= 0);
    }

    #[test]
    fn dictionary_input_is_hashed_once_per_value() {
        let dict: DictionaryArray<Int8Type> = vec![Some("iceberg"), None, Some("iceberg")]
            .into_iter()
            .collect();
        let result = bucket(16, Arc::new(dict));
        let expected = (1_210_000_089_i32 & i32::MAX) % 16;
        assert_eq!(
            result,
            Int32Array::from(vec![Some(expected), None, Some(expected)])
        );
    }

    #[test]
    fn scalar_input_returns_scalar() {
        let result = SparkIcebergBucket::new()
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Scalar(ScalarValue::Int32(Some(100))),
                    ColumnarValue::Scalar(ScalarValue::Int32(Some(34))),
                ],
                arg_fields: vec![],
                number_rows: 1,
                return_field: Arc::new(arrow::datatypes::Field::new("b", DataType::Int32, true)),
                config_options: Arc::new(datafusion::config::ConfigOptions::default()),
            })
            .unwrap();
        match result {
            ColumnarValue::Scalar(ScalarValue::Int32(Some(79))) => {}
            other => panic!("expected scalar 79, got {other:?}"),
        }
    }

    #[test]
    fn rejects_non_positive_or_non_literal_num_buckets() {
        let value = ColumnarValue::Array(Arc::new(Int32Array::from(vec![1])));
        for bad in [
            ColumnarValue::Scalar(ScalarValue::Int32(Some(0))),
            ColumnarValue::Scalar(ScalarValue::Int32(None)),
            ColumnarValue::Array(Arc::new(Int32Array::from(vec![4]))),
        ] {
            let err = invoke(&SparkIcebergBucket::new(), vec![bad, value.clone()]).unwrap_err();
            assert!(err
                .to_string()
                .contains("numBuckets must be a positive Int32 literal"));
        }
    }

    #[test]
    fn rejects_unsupported_types() {
        let value = ColumnarValue::Array(Arc::new(arrow::array::Float64Array::from(vec![1.0])));
        let err = invoke(
            &SparkIcebergBucket::new(),
            vec![ColumnarValue::Scalar(ScalarValue::Int32(Some(4))), value],
        )
        .unwrap_err();
        assert!(err
            .to_string()
            .contains("does not support input type Float64"));
    }
}
