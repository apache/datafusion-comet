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

//! Spark's `map_from_arrays` and `map_from_entries` under `spark.sql.mapKeyDedupPolicy=EXCEPTION`,
//! see [`build_map`]. Keys are compared by their Arrow row encoding. The serde declines `LAST_WIN`
//! and collated keys, and documents floating-point keys.
//!
//! They replace DataFusion's `map` and `datafusion-spark`'s `map_from_entries`, which raise
//! DataFusion errors rather than Spark's. The latter also accepts a NULL key and misreads a sliced
//! input.

use crate::SparkError;
use arrow::array::{Array, ArrayData, ArrayRef, MapArray, StructArray, UInt32Array};
use arrow::buffer::{BooleanBuffer, NullBuffer, OffsetBuffer};
use arrow::compute::take;
use arrow::datatypes::{DataType, Field, Fields};
use arrow::row::{RowConverter, SortField};
use arrow::util::display::array_value_to_string;
use datafusion::common::cast::{as_list_array, as_struct_array};
use datafusion::common::utils::take_function_args;
use datafusion::common::{exec_err, HashSet, Result};
use datafusion::functions::utils::make_scalar_function;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;

/// Spark's `map_from_arrays(keys, values)`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkMapFromArrays {
    signature: Signature,
}

impl Default for SparkMapFromArrays {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkMapFromArrays {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkMapFromArrays {
    fn name(&self) -> &str {
        "map_from_arrays"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [keys, values] = take_function_args(self.name(), arg_types)?;
        Ok(map_type(
            list_element_type(keys)?,
            list_element_type(values)?,
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(map_from_arrays, vec![])(&args.args)
    }
}

fn map_from_arrays(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [keys, values] = take_function_args("map_from_arrays", args)?;
    let (keys, values) = (as_list_array(keys)?, as_list_array(values)?);
    build_map(
        keys.values(),
        values.values(),
        keys.value_offsets(),
        values.value_offsets(),
        NullBuffer::union(keys.nulls(), values.nulls()).as_ref(),
    )
}

/// Spark's `map_from_entries(array<struct<key, value>>)`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkMapFromEntries {
    signature: Signature,
}

impl Default for SparkMapFromEntries {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkMapFromEntries {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkMapFromEntries {
    fn name(&self) -> &str {
        "map_from_entries"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [entries] = take_function_args(self.name(), arg_types)?;
        match list_element_type(entries)? {
            DataType::Struct(fields) if fields.len() == 2 => {
                Ok(map_type(fields[0].data_type(), fields[1].data_type()))
            }
            other => {
                exec_err!("map_from_entries expects an array of key/value structs, got {other}")
            }
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(map_from_entries, vec![])(&args.args)
    }
}

fn map_from_entries(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [entries] = take_function_args("map_from_entries", args)?;
    let entries = as_list_array(entries)?;
    let pairs = as_struct_array(entries.values().as_ref())?;
    let offsets = entries.value_offsets();
    // Spark returns NULL for a row holding a NULL entry before it looks at any key.
    let nulls = match pairs.nulls().filter(|n| n.null_count() > 0) {
        Some(pair_nulls) => Some(NullBuffer::new(BooleanBuffer::collect_bool(
            entries.len(),
            |row| {
                let start = offsets[row] as usize;
                let len = (offsets[row + 1] - offsets[row]) as usize;
                entries.is_valid(row)
                    && pair_nulls
                        .buffer()
                        .count_set_bits_offset(pair_nulls.offset() + start, len)
                        == len
            },
        ))),
        None => entries.nulls().cloned(),
    };
    build_map(
        pairs.column(0),
        pairs.column(1),
        offsets,
        offsets,
        nulls.as_ref(),
    )
}

/// Builds one map per row from the flattened list children `keys` and `values`. A row NULL in
/// `nulls` is neither checked nor kept. Every other row is checked in `ArrayBasedMapBuilder`'s
/// order, so the error raised is the one Spark raises first: the key and value counts, then each
/// key for NULL (`NULL_MAP_KEY`) or a repeat (`DUPLICATED_MAP_KEY`). The offsets may start past
/// zero, as a sliced list's do.
fn build_map(
    keys: &ArrayRef,
    values: &ArrayRef,
    key_offsets: &[i32],
    value_offsets: &[i32],
    nulls: Option<&NullBuffer>,
) -> Result<ArrayRef> {
    let keys = offsets_window(keys, key_offsets);
    let values = offsets_window(values, value_offsets);
    let encoded_keys = RowConverter::new(vec![SortField::new(keys.data_type().clone())])?
        .convert_columns(&[Arc::clone(&keys)])?;
    let key_nulls = keys.logical_nulls().filter(|n| n.null_count() > 0);

    let mut map_offsets = Vec::with_capacity(key_offsets.len());
    map_offsets.push(0);
    let mut num_entries = 0;
    let mut row_keys = HashSet::new();
    for row in 0..key_offsets.len() - 1 {
        if nulls.is_none_or(|nulls| nulls.is_valid(row)) {
            let key_start = (key_offsets[row] - key_offsets[0]) as usize;
            let key_end = (key_offsets[row + 1] - key_offsets[0]) as usize;
            let num_values = value_offsets[row + 1] - value_offsets[row];
            if key_end - key_start != num_values as usize {
                return Err(SparkError::MapKeyValueDiffSizes.into());
            }
            row_keys.clear();
            for key in key_start..key_end {
                if key_nulls.as_ref().is_some_and(|nulls| nulls.is_null(key)) {
                    return Err(SparkError::NullMapKey.into());
                }
                if !row_keys.insert(encoded_keys.row(key)) {
                    let key = array_value_to_string(&keys, key)?;
                    return Err(SparkError::DuplicatedMapKey { key }.into());
                }
            }
            num_entries += num_values;
        }
        map_offsets.push(num_entries);
    }

    let num_entries = num_entries as usize;
    let keys = valid_entries(keys, key_offsets, nulls, num_entries)?;
    let values = valid_entries(values, value_offsets, nulls, num_entries)?;
    let fields = entry_fields(keys.data_type(), values.data_type());
    let entries = StructArray::try_new(fields.clone(), vec![keys, values], None)?;
    Ok(Arc::new(MapArray::try_new(
        Arc::new(Field::new("entries", DataType::Struct(fields), false)),
        OffsetBuffer::new(map_offsets.into()),
        entries,
        nulls.cloned(),
        false,
    )?))
}

/// The entries of `child` that `offsets` address.
fn offsets_window(child: &ArrayRef, offsets: &[i32]) -> ArrayRef {
    let start = offsets[0] as usize;
    child.slice(start, offsets[offsets.len() - 1] as usize - start)
}

/// The `num_entries` entries of the valid rows of an [`offsets_window`]. The window is copied
/// when it also holds entries of NULL rows, which a map must not keep, or when its C Data export
/// would carry an offset: Arrow Java's import ignores a nested child's offset, which a sliced
/// boolean child keeps (#2051).
fn valid_entries(
    window: ArrayRef,
    offsets: &[i32],
    nulls: Option<&NullBuffer>,
    num_entries: usize,
) -> Result<ArrayRef> {
    if window.len() == num_entries && !has_offset(&window.to_data()) {
        return Ok(window);
    }
    let start = offsets[0];
    let mut indices = Vec::with_capacity(num_entries);
    for (row, bounds) in offsets.windows(2).enumerate() {
        if nulls.is_none_or(|nulls| nulls.is_valid(row)) {
            indices.extend((bounds[0] - start) as u32..(bounds[1] - start) as u32);
        }
    }
    Ok(take(&window, &UInt32Array::from(indices), None)?)
}

/// Whether `data` or a child keeps a non-zero offset. Slicing other arrays moves their buffers
/// instead, so of the arrays a map child can hold only a boolean array keeps one.
fn has_offset(data: &ArrayData) -> bool {
    data.offset() != 0 || data.child_data().iter().any(has_offset)
}

fn list_element_type(data_type: &DataType) -> Result<&DataType> {
    match data_type {
        DataType::List(field) => Ok(field.data_type()),
        other => exec_err!("expected an array argument, got {other}"),
    }
}

/// Must match the type Comet's planner gives Spark's `MapType(key, value)`, as
/// `CometMapFromArrays`'s `CaseWhen` returns a NULL of that type: NULL-free keys, nullable
/// values.
fn map_type(key_type: &DataType, value_type: &DataType) -> DataType {
    let entries = Field::new(
        "entries",
        DataType::Struct(entry_fields(key_type, value_type)),
        false,
    );
    DataType::Map(Arc::new(entries), false)
}

fn entry_fields(key_type: &DataType, value_type: &DataType) -> Fields {
    Fields::from(vec![
        Field::new("key", key_type.clone(), false),
        Field::new("value", value_type.clone(), true),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{AsArray, BooleanArray, Int32Array, ListArray};
    use arrow::datatypes::Int32Type;

    /// The NULL values row spans keys, a NULL key and a repeat among them, that are neither checked
    /// nor kept. Comet's serde never passes such a row, so SQL cannot reach it. The slice leaves an
    /// offset on the boolean values that must not reach the JVM (#2051).
    #[test]
    fn null_row_entries_are_dropped_unchecked() -> Result<()> {
        let keys: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>([
            Some(vec![Some(10)]),
            Some(vec![Some(20), Some(21)]),
            Some(vec![Some(1), None, Some(1)]),
            Some(vec![Some(30)]),
        ]));
        let values: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new_list_field(DataType::Boolean, true)),
            OffsetBuffer::from_lengths([1, 2, 0, 1]),
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                None,
                Some(true),
            ])),
            Some(NullBuffer::from(vec![true, true, false, true])),
        ));
        let map = map_from_arrays(&[keys.slice(1, 3), values.slice(1, 3)])?;
        let map = map.as_map();
        assert_eq!(map.value_offsets(), &[0, 2, 2, 3]);
        assert!(map.is_valid(0) && map.is_null(1) && map.is_valid(2));
        assert_eq!(
            map.keys().as_primitive::<Int32Type>(),
            &Int32Array::from(vec![20, 21, 30])
        );
        assert_eq!(
            map.values().as_boolean(),
            &BooleanArray::from(vec![Some(false), None, Some(true)])
        );
        assert_eq!(map.values().offset(), 0);
        Ok(())
    }
}
