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

use std::sync::Arc;

use arrow::array::{Array, BinaryArray, BooleanArray, StructArray};
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::physical_plan::ColumnarValue;

use crate::SparkError;

pub fn spark_is_variant_null(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    variant_predicate(args, false)
}

pub fn spark_is_valid_variant(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    variant_predicate(args, true)
}

fn variant_predicate(args: &[ColumnarValue], validate: bool) -> Result<ColumnarValue> {
    let [arg] = args else {
        return exec_err!("Variant predicate requires one argument");
    };
    let array = arg.to_array(1)?;
    let Some(variant) = array.as_any().downcast_ref::<StructArray>() else {
        return exec_err!("Variant predicate requires struct<value:binary,metadata:binary>");
    };
    let binary = |name| {
        variant
            .column_by_name(name)
            .and_then(|a| a.as_any().downcast_ref::<BinaryArray>())
    };
    let (Some(values), Some(metadata)) = (binary("value"), binary("metadata")) else {
        return exec_err!("Variant predicate requires Binary value and metadata children");
    };
    let result = (0..variant.len())
        .map(|row| {
            if variant.is_null(row) {
                return Ok(if validate { None } else { Some(false) });
            }
            if validate {
                Ok(Some(
                    !values.is_null(row)
                        && !metadata.is_null(row)
                        && valid_variant(values.value(row), metadata.value(row)).is_some(),
                ))
            } else {
                // Spark checks only the first value byte. In particular, neither invalid
                // metadata nor a truncated non-null primitive changes this predicate.
                if values.is_null(row) {
                    return Err(SparkError::MalformedVariant.into());
                }
                values
                    .value(row)
                    .first()
                    .map(|header| Some(*header == 0))
                    .ok_or_else(|| SparkError::MalformedVariant.into())
            }
        })
        .collect::<Result<BooleanArray>>()?;
    match arg {
        ColumnarValue::Scalar(_) => Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
            &result, 0,
        )?)),
        ColumnarValue::Array(_) => Ok(ColumnarValue::Array(Arc::new(result))),
    }
}

// Mirrors Spark 4.2 VariantUtil.isValidVariant, whose contract differs from Arrow's
// canonical validator: it reads reachable values and dictionary keys, tolerates trailing
// bytes and invalid UTF-8, and does not check key order or unused offsets/metadata.
fn valid_variant(value: &[u8], metadata: &[u8]) -> Option<()> {
    if metadata.first()? & 0x0f != 1 {
        return None;
    }
    // Use an explicit stack so a deeply nested input cannot overflow the native call stack.
    let mut pending = vec![0];
    while let Some(pos) = pending.pop() {
        let header = *value.get(pos)?;
        let info = usize::from(header >> 2);
        match header & 3 {
            0 => match info {
                0..=2 => (), // null, true, false
                3 => {
                    value.get(pos + 1)?;
                }
                4 => {
                    value.get(pos + 2)?;
                }
                5 | 11 | 14 => {
                    value.get(pos + 4)?;
                } // int32, date, float
                6 | 7 | 12 | 13 => {
                    value.get(pos + 8)?;
                }
                8..=10 => {
                    let (width, precision) = [(4, 9), (8, 18), (16, 38)][info - 8];
                    if u32::from(*value.get(pos + 1)?) > precision {
                        return None;
                    }
                    let bytes = value.get(pos + 2..pos + 2 + width)?;
                    let mut signed = [if bytes[width - 1] & 0x80 != 0 {
                        0xff
                    } else {
                        0
                    }; 16];
                    signed[..width].copy_from_slice(bytes);
                    if i128::from_le_bytes(signed).unsigned_abs() >= 10_u128.pow(precision) {
                        return None;
                    }
                }
                15 | 16 => {
                    let len = unsigned(value, pos + 1, 4)?;
                    value.get(pos + 5..pos + 5 + len)?;
                }
                20 => {
                    value.get(pos + 16)?;
                } // UUID
                _ => return None,
            },
            1 => {
                value.get(pos + 1..pos + 1 + info)?;
            }
            basic => {
                let object = basic == 2;
                let size_width = if info & (if object { 16 } else { 4 }) != 0 {
                    4
                } else {
                    1
                };
                let size = unsigned(value, pos + 1, size_width)?;
                let ids = pos + 1 + size_width;
                let id_width = if object { ((info >> 2) & 3) + 1 } else { 0 };
                let offset_width = (info & 3) + 1;
                let offsets = ids + size * id_width;
                let data = offsets + (size + 1) * offset_width;
                // Spark does not touch the offset table of an empty object/array.
                for index in (0..size).rev() {
                    if object {
                        let id = unsigned(value, ids + index * id_width, id_width)?;
                        valid_key(metadata, id)?;
                    }
                    let offset = unsigned(value, offsets + index * offset_width, offset_width)?;
                    let child = data.checked_add(offset)?;
                    value.get(child)?;
                    pending.push(child);
                }
            }
        }
    }
    Some(())
}

fn unsigned(bytes: &[u8], pos: usize, width: usize) -> Option<usize> {
    let mut buffer = [0; 4];
    buffer[..width].copy_from_slice(bytes.get(pos..pos.checked_add(width)?)?);
    let result = u32::from_le_bytes(buffer);
    // Spark's readUnsigned rejects a 32-bit quantity whose sign bit is set.
    (result <= i32::MAX as u32).then_some(result as usize)
}

fn valid_key(metadata: &[u8], id: usize) -> Option<()> {
    let width = usize::from(metadata[0] >> 6) + 1;
    let size = unsigned(metadata, 1, width)?;
    if id >= size {
        return None;
    }
    // Spark computes dictionary addresses with Java int arithmetic. A large declared
    // dictionary can wrap to an in-bounds empty key even in a small metadata buffer.
    let address = |index: i32| 1_i32.wrapping_add(index.wrapping_mul(width as i32));
    let data = address((size as i32).wrapping_add(2));
    let offset = unsigned(
        metadata,
        usize::try_from(address((id as i32).wrapping_add(1))).ok()?,
        width,
    )?;
    let next = unsigned(
        metadata,
        usize::try_from(address((id as i32).wrapping_add(2))).ok()?,
        width,
    )?;
    if offset > next {
        return None;
    }
    let last = data.wrapping_add(next as i32).wrapping_sub(1);
    metadata.get(usize::try_from(last).ok()?)?;
    let start = usize::try_from(data.wrapping_add(offset as i32)).ok()?;
    metadata.get(start..start.checked_add(next - offset)?)?;
    Some(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::buffer::NullBuffer;
    use arrow::datatypes::{DataType, Field, Fields};

    #[test]
    fn variant_predicate_null_and_malformed_semantics() {
        let fields = Fields::from(vec![
            Field::new("value", DataType::Binary, true),
            Field::new("metadata", DataType::Binary, true),
        ]);
        let array = StructArray::new(
            fields,
            vec![
                Arc::new(BinaryArray::from(vec![
                    Some(&[0][..]),
                    Some(&[24][..]),
                    None,
                ])),
                Arc::new(BinaryArray::from(vec![
                    Some(&[1][..]),
                    Some(&[0][..]),
                    None,
                ])),
            ],
            Some(NullBuffer::from(vec![true, true, false])),
        );
        for (row, expected_null, expected_valid) in [
            (0, Some(true), Some(true)),
            (1, Some(false), Some(false)),
            (2, Some(false), None),
        ] {
            let arg = ColumnarValue::Scalar(ScalarValue::Struct(Arc::new(array.slice(row, 1))));
            let ColumnarValue::Scalar(actual_null) =
                spark_is_variant_null(std::slice::from_ref(&arg)).unwrap()
            else {
                panic!()
            };
            let ColumnarValue::Scalar(actual_valid) = spark_is_valid_variant(&[arg]).unwrap()
            else {
                panic!()
            };
            assert_eq!(actual_null, ScalarValue::Boolean(expected_null));
            assert_eq!(actual_valid, ScalarValue::Boolean(expected_valid));
        }
        let empty = ColumnarValue::Scalar(ScalarValue::Struct(Arc::new(StructArray::new(
            array.fields().clone(),
            vec![
                Arc::new(BinaryArray::from(vec![&[][..]])),
                Arc::new(BinaryArray::from(vec![&[1][..]])),
            ],
            None,
        ))));
        assert!(spark_is_variant_null(std::slice::from_ref(&empty))
            .unwrap_err()
            .to_string()
            .contains("MALFORMED_VARIANT"));
        let ColumnarValue::Scalar(valid) = spark_is_valid_variant(&[empty]).unwrap() else {
            panic!()
        };
        assert_eq!(valid, ScalarValue::Boolean(Some(false)));
        let array = ColumnarValue::Array(Arc::new(array));
        let ColumnarValue::Array(valid) = spark_is_valid_variant(&[array]).unwrap() else {
            panic!()
        };
        assert_eq!(
            valid.as_any().downcast_ref::<BooleanArray>().unwrap(),
            &BooleanArray::from(vec![Some(true), Some(false), None])
        );
        assert!(valid_variant(&[], &[1]).is_none());
    }

    #[test]
    fn variant_validity_matches_spark_accessors() {
        let metadata = [1, 2, 0, 1, 2, b'a', b'b'];
        let valid: &[&[u8]] = &[
            &[0],
            &[4],
            &[8],
            &[12, 1],
            &[16, 1, 0],
            &[20, 1, 0, 0, 0],
            &[24, 1, 0, 0, 0, 0, 0, 0, 0],
            &[28, 0, 0, 0, 0, 0, 0, 0, 0],
            &[32, 0, 1, 0, 0, 0],
            &[13, b'a', b'b', b'c'],
            &[64, 2, 0, 0, 0, b'a', b'b'],
            &[60, 2, 0, 0, 0, 1, 2],
            &[3, 2, 0, 1, 2, 4, 8],
            &[2, 2, 0, 1, 0, 2, 4, 12, 1, 12, 2],
            // Spark ignores unused metadata, terminal offsets, duplicate/order constraints,
            // trailing bytes, container reserved bits, and string UTF-8 validity.
            &[0, 255],
            &[5, 255],
            &[3, 0],
            &[2, 0],
            &[255, 0, 0, 0, 0],
            &[2, 2, 1, 0, 0, 0, 255, 0],
        ];
        for bytes in valid {
            assert!(valid_variant(bytes, &metadata).is_some(), "{bytes:?}");
        }
        for bytes in [vec![0], vec![4], vec![8], vec![3, 0], vec![2, 0]] {
            assert!(valid_variant(&bytes, &[1]).is_some());
        }
        for (kind, width) in [
            (36, 9),
            (40, 17),
            (44, 4),
            (48, 8),
            (52, 8),
            (56, 4),
            (80, 16),
        ] {
            let mut bytes = vec![kind];
            bytes.resize(width + 1, 0);
            assert!(valid_variant(&bytes, &metadata).is_some());
            bytes.pop();
            assert!(valid_variant(&bytes, &metadata).is_none());
        }
        let invalid: &[&[u8]] = &[
            &[],
            &[24, 0],
            &[32],
            &[36],
            &[40],
            &[9, b'x'],
            &[64, 0, 0, 0],
            &[64, 1, 0, 0, 0],
            &[68],
            &[32, 10, 0, 0, 0, 0],
            &[32, 0, 255, 255, 255, 127],
            &[3, 1, 0],
            &[19, 0, 0],
            &[3, 1, 1, 1],
            &[3, 1, 0, 2, 24, 0],
            &[2, 1, 2, 0, 1, 0],
            &[2, 1, 0, 5, 0, 12, 1],
            &[2, 1, 0, 0, 2, 24, 0],
            &[60, 255, 255, 255, 255],
        ];
        for bytes in invalid {
            assert!(valid_variant(bytes, &metadata).is_none(), "{bytes:?}");
        }
        for metadata in [&[][..], &[0], &[2], &[3]] {
            assert!(valid_variant(&[0], metadata).is_none());
        }
        for header in [0x11, 0x21, 0xf1] {
            assert!(valid_variant(&[0], &[header]).is_some());
        }
        let object = [2, 1, 0, 0, 2, 12, 1];
        for metadata in [&[1][..], &[1, 0, 0], &[1, 1, 0], &[1, 1, 2, 1, b'a', b'b']] {
            assert!(valid_variant(&object, metadata).is_none());
        }
        assert!(valid_variant(&object, &[1, 1, 0, 1, 255]).is_some());
        assert!(valid_variant(
            &[2, 1, 0, 0, 1, 0],
            &[193, 254, 255, 255, 63, 0, 0, 0, 0, 0, 0, 0, 0]
        )
        .is_some());
    }

    #[test]
    fn variant_validity_does_not_use_the_call_stack() {
        let mut value = [3, 1, 0, 0].repeat(10_000);
        value.push(0);
        assert!(valid_variant(&value, &[1]).is_some());
        value.pop();
        assert!(valid_variant(&value, &[1]).is_none());
    }
}
