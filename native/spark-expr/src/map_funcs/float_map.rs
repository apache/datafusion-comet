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

use std::{hash::Hash, sync::Arc};

use arrow::array::{
    Array, ArrayRef, AsArray, Int32Array, ListArray, MapArray, NullBufferBuilder, PrimitiveArray,
    StructArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::compute::{cast, take};
use arrow::datatypes::{ArrowPrimitiveType, DataType, Field, FieldRef, Float32Type, Float64Type};
use datafusion::common::cast::{as_list_array, as_struct_array};
use datafusion::common::config::MapKeyDedupPolicy;
use datafusion::common::{exec_err, internal_err, HashMap, Result};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion_functions::utils::make_scalar_function;
use datafusion_spark::function::map::{
    map_from_arrays::MapFromArrays, map_from_entries::MapFromEntries,
};

use crate::conversion_funcs::{java_float_string, JavaFloatString};
use crate::float_semantics::{canonicalize_nan, normalize_float};
use crate::SparkError;

/// Spark map construction for a top-level FLOAT or DOUBLE key.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkFloatMap {
    signature: Signature,
    normalize: bool,
    last_win: bool,
}

impl SparkFloatMap {
    pub fn new(normalize: bool, policy: MapKeyDedupPolicy) -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(1), TypeSignature::Any(2)],
                Volatility::Immutable,
            ),
            normalize,
            last_win: policy == MapKeyDedupPolicy::LastWin,
        }
    }

    fn build(&self, args: &[ArrayRef]) -> Result<ArrayRef> {
        let lists = args.iter().map(to_list).collect::<Result<Vec<_>>>()?;
        let input = match lists.as_slice() {
            [keys, values] => MapInput {
                key_rows: keys,
                value_rows: values,
                keys: keys.values(),
                values: values.values(),
                entry_nulls: None,
                from_arrays: true,
            },
            [rows] => {
                let entries = as_struct_array(rows.values())?;
                let [keys, values] = entries.columns() else {
                    return exec_err!("{}: expected key-value pairs", self.name());
                };
                MapInput {
                    key_rows: rows,
                    value_rows: rows,
                    keys,
                    values,
                    entry_nulls: entries.nulls(),
                    from_arrays: false,
                }
            }
            _ => return exec_err!("{}: expected one or two arguments", self.name()),
        };
        match input.keys.data_type() {
            DataType::Float32 => self.build_typed::<Float32Type, _>(input, f32::to_bits),
            DataType::Float64 => self.build_typed::<Float64Type, _>(input, f64::to_bits),
            dt => exec_err!("{}: expected floating-point keys, got {dt}", self.name()),
        }
    }

    fn build_typed<T, Bits>(
        &self,
        input: MapInput,
        bits: impl Fn(T::Native) -> Bits,
    ) -> Result<ArrayRef>
    where
        T: ArrowPrimitiveType,
        T::Native: JavaFloatString,
        Bits: Eq + Hash,
    {
        let keys = input.keys.as_primitive::<T>();
        let key_offsets = input.key_rows.value_offsets();
        let value_offsets = input.value_rows.value_offsets();
        let first_key = key_offsets[0] as usize;
        let first_value = value_offsets[0];
        let raw_keys = &keys.values()[first_key..];

        // Both outputs start as slices of the input. Materialize each independently once
        // a changed key or a non-contiguous value breaks that invariant.
        let mut output_keys = None;
        let mut value_indices = None;
        let mut len = 0;
        let mut offsets = Vec::with_capacity(key_offsets.len());
        let mut nulls = NullBufferBuilder::new(input.key_rows.len());
        let mut positions = HashMap::new();
        for row in 0..input.key_rows.len() {
            let start = len;
            offsets.push(start as i32);
            let key_range = key_offsets[row] as usize..key_offsets[row + 1] as usize;
            let value_range = value_offsets[row]..value_offsets[row + 1];
            let count = key_range.len();
            if input.key_rows.is_null(row)
                || input.value_rows.is_null(row)
                || input
                    .entry_nulls
                    .is_some_and(|nulls| nulls.slice(key_range.start, count).null_count() != 0)
            {
                nulls.append_null();
                continue;
            }
            nulls.append_non_null();
            if count != value_range.len() {
                return Err(SparkError::MapKeyValueDiffSizes.into());
            }

            positions.clear();
            for (i, value_index) in key_range.zip(value_range) {
                if keys.is_null(i) {
                    return Err(SparkError::NullMapKey.into());
                }
                let key = keys.value(i);
                let compared = if self.normalize {
                    normalize_float(key)
                } else {
                    canonicalize_nan(key)
                };
                let slot = *positions.entry(bits(compared)).or_insert(len);
                if slot == len {
                    let stored = if self.normalize && !input.from_arrays {
                        compared
                    } else {
                        key
                    };
                    if i != first_key + len || bits(stored) != bits(key) {
                        output_keys.get_or_insert_with(|| raw_keys[..len].to_vec());
                    }
                    if let Some(keys) = &mut output_keys {
                        keys.push(stored);
                    }
                    len += 1;
                } else if !self.last_win {
                    return Err(SparkError::DuplicatedMapKey {
                        key: java_float_string(key),
                    }
                    .into());
                }

                if value_index != first_value + slot as i32 {
                    let indices = value_indices.get_or_insert_with(|| {
                        (first_value..first_value + len as i32).collect::<Vec<_>>()
                    });
                    indices.resize(len, value_index);
                    indices[slot] = value_index;
                }
            }
            // ArrayBasedMapBuilder.from returns the original arrays if all keys are unique.
            // Once LAST_WIN collapses a duplicate, it builds the entire row with normalized keys.
            if input.from_arrays && self.normalize && len - start < count {
                let out = output_keys.get_or_insert_with(|| raw_keys[..len].to_vec());
                for key in &mut out[start..] {
                    *key = normalize_float(*key);
                }
            }
        }
        offsets.push(len as i32);
        let keys = match output_keys {
            Some(keys) => PrimitiveArray::<T>::new(keys.into(), None),
            None => keys.slice(first_key, len),
        };
        let values = match value_indices {
            Some(indices) => take(input.values, &Int32Array::from(indices), None)?,
            None => input.values.slice(first_value as usize, len),
        };
        let fields = vec![
            Field::new("key", keys.data_type().clone(), false),
            Field::new("value", values.data_type().clone(), true),
        ];
        let entries = StructArray::try_new(fields.into(), vec![Arc::new(keys), values], None)?;
        Ok(Arc::new(MapArray::try_new(
            Arc::new(Field::new("entries", entries.data_type().clone(), false)),
            OffsetBuffer::new(offsets.into()),
            entries,
            nulls.finish(),
            false,
        )?))
    }
}

struct MapInput<'a> {
    from_arrays: bool,
    key_rows: &'a ListArray,
    value_rows: &'a ListArray,
    keys: &'a ArrayRef,
    values: &'a ArrayRef,
    entry_nulls: Option<&'a NullBuffer>,
}

fn to_list(array: &ArrayRef) -> Result<ListArray> {
    match array.data_type() {
        DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            let list = cast(array, &DataType::List(Arc::clone(field)))?;
            Ok(as_list_array(&list)?.clone())
        }
        _ => Ok(as_list_array(array)?.clone()),
    }
}

impl ScalarUDFImpl for SparkFloatMap {
    fn name(&self) -> &str {
        "spark_float_map"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        match args.arg_fields {
            [_] => MapFromEntries::default().return_field_from_args(args),
            [_, _] => MapFromArrays::default().return_field_from_args(args),
            _ => exec_err!("{}: expected one or two arguments", self.name()),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.iter().any(
            |arg| matches!(arg, ColumnarValue::Array(array) if array.len() != args.number_rows),
        ) {
            return exec_err!("{}: expected length {}", self.name(), args.number_rows);
        }
        make_scalar_function(|args| self.build(args), vec![])(&args.args)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float32Array, Float64Array, StringArray};
    fn sliced_rows(values: ArrayRef, lengths: [usize; 4]) -> ArrayRef {
        let rows = ListArray::new(
            Arc::new(Field::new("item", values.data_type().clone(), true)),
            OffsetBuffer::from_lengths(lengths),
            values,
            None,
        );
        Arc::new(rows.slice(1, 3))
    }

    #[test]
    fn sliced_arrays_normalize_only_rows_collapsed_by_last_win() {
        let nan = f64::from_bits(0xfff8000000000001);
        let keys = Float64Array::from(vec![9.0, -0.0, nan, -0.0, nan, 2.0, f64::NAN, -0.0, nan]);
        let values = StringArray::from(vec![
            Some("a"),
            Some("b"),
            Some("c"),
            Some("drop"),
            Some("d"),
            None,
            Some("e"),
            Some("f"),
        ]);
        let args = [
            sliced_rows(Arc::new(keys), [1, 2, 4, 2]),
            sliced_rows(Arc::new(values), [0, 2, 4, 2]),
        ];
        for normalize in [true, false] {
            let result = SparkFloatMap::new(normalize, MapKeyDedupPolicy::LastWin)
                .build(&args)
                .unwrap();
            let map = result.as_map();
            assert_eq!(map.value_offsets(), &[0, 2, 5, 7]);
            let (zero, stored_nan) = if normalize {
                (0.0, f64::NAN)
            } else {
                (-0.0, nan)
            };
            let keys = map.keys().as_primitive::<Float64Type>();
            let bits: Vec<_> = keys.values().iter().map(|k| k.to_bits()).collect();
            assert_eq!(
                bits,
                [-0.0, nan, zero, stored_nan, 2.0, -0.0, nan].map(f64::to_bits)
            );
            assert_eq!(
                map.values().as_string::<i32>(),
                &StringArray::from(vec![
                    Some("a"),
                    Some("b"),
                    Some("c"),
                    None,
                    Some("d"),
                    Some("e"),
                    Some("f")
                ])
            );
        }
    }

    #[test]
    fn entries_skip_null_struct_rows_and_retain_first_nan_payload_under_boxed_equality() {
        let nan = f32::from_bits(0xffc00001);
        let keys = Float32Array::from(vec![
            None,
            Some(-0.0),
            None,
            Some(1.0),
            Some(nan),
            Some(0.0),
            Some(f32::NAN),
        ]);
        let values = Float32Array::from(vec![1.0, nan, 2.0, 3.0, 4.0, -0.0, nan]);
        let entries = StructArray::new(
            vec![
                Field::new("k", DataType::Float32, true),
                Field::new("v", DataType::Float32, true),
            ]
            .into(),
            vec![Arc::new(keys), Arc::new(values)],
            Some(NullBuffer::from(vec![
                true, true, true, false, true, true, true,
            ])),
        );
        let args = [sliced_rows(Arc::new(entries), [1, 1, 2, 3])];
        let bits = |arr: &ArrayRef| {
            arr.as_primitive::<Float32Type>()
                .values()
                .iter()
                .map(|v| v.to_bits())
                .collect::<Vec<_>>()
        };
        for normalize in [true, false] {
            let result = SparkFloatMap::new(normalize, MapKeyDedupPolicy::LastWin)
                .build(&args)
                .unwrap();
            let map = result.as_map();
            assert_eq!(map.value_offsets(), &[0, 1, 1, 3]);
            assert!(map.is_null(1));
            let stored = if normalize {
                [0.0, f32::NAN, 0.0]
            } else {
                [-0.0, nan, 0.0]
            };
            assert_eq!(bits(map.keys()), stored.map(f32::to_bits));
            assert_eq!(bits(map.values()), [nan, nan, -0.0].map(f32::to_bits));
        }
    }
}
