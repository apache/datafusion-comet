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

//! Spark-compatible `map_from_arrays`, `map_from_entries` and `str_to_map`.
//!
//! The `datafusion-spark` kernels build the `MapArray` and already follow Spark's
//! `spark.sql.mapKeyDedupPolicy`, which Comet forwards as
//! `datafusion.spark.map_key_dedup_policy`. These wrappers add the checks Spark's
//! `ArrayBasedMapBuilder` performs before inserting an entry, and restate the upstream errors
//! as the Spark error classes `SparkErrorConverter` turns back into `QueryExecutionErrors`:
//!
//! - a `NULL` key element raises `[NULL_MAP_KEY]`, ahead of any duplicate-key check, because
//!   Spark rejects the `NULL` before it reaches the dedup map;
//! - a key array and value array of different lengths raise `[MAP_KEY_VALUE_DIFF_SIZES]`;
//! - a duplicate key under `EXCEPTION` raises `[DUPLICATED_MAP_KEY]` naming the key.
//!
//! `str_to_map` builds its keys by splitting a string, so it needs only the duplicate-key
//! restatement.

use crate::SparkError;
use arrow::array::{Array, ArrayRef, AsArray, StructArray};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, FieldRef};
use datafusion::common::{exec_err, DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use datafusion_spark::function::map::map_from_arrays::MapFromArrays as DataFusionMapFromArrays;
use datafusion_spark::function::map::map_from_entries::MapFromEntries as DataFusionMapFromEntries;
use datafusion_spark::function::map::str_to_map::SparkStrToMap as DataFusionStrToMap;
use std::sync::Arc;

/// Spark-compatible `map_from_arrays(keys, values)`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkMapFromArrays {
    inner: DataFusionMapFromArrays,
}

impl Default for SparkMapFromArrays {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkMapFromArrays {
    pub fn new() -> Self {
        Self {
            inner: DataFusionMapFromArrays::new(),
        }
    }
}

impl ScalarUDFImpl for SparkMapFromArrays {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        self.inner.return_field_from_args(args)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = expand_scalars(args)?;
        match args.args.as_slice() {
            [ColumnarValue::Array(keys), ColumnarValue::Array(values)] => {
                validate_map_from_arrays(keys, values)?
            }
            other => return exec_err!("map_from_arrays expects 2 arguments, got {}", other.len()),
        }
        self.inner
            .invoke_with_args(args)
            .map_err(|error| as_spark_error(error, DuplicateKeyFormat::Bare))
    }
}

/// Spark-compatible `map_from_entries(entries)`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkMapFromEntries {
    inner: DataFusionMapFromEntries,
}

impl Default for SparkMapFromEntries {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkMapFromEntries {
    pub fn new() -> Self {
        Self {
            inner: DataFusionMapFromEntries::new(),
        }
    }
}

impl ScalarUDFImpl for SparkMapFromEntries {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        self.inner.return_field_from_args(args)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = expand_scalars(args)?;
        match args.args.as_slice() {
            [ColumnarValue::Array(entries)] => validate_map_from_entries(entries)?,
            other => return exec_err!("map_from_entries expects 1 argument, got {}", other.len()),
        }
        self.inner
            .invoke_with_args(args)
            .map_err(|error| as_spark_error(error, DuplicateKeyFormat::Bare))
    }
}

/// Spark-compatible `str_to_map(text[, pair_delim[, key_value_delim]])`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkStrToMap {
    inner: DataFusionStrToMap,
}

impl Default for SparkStrToMap {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkStrToMap {
    pub fn new() -> Self {
        Self {
            inner: DataFusionStrToMap::new(),
        }
    }
}

impl ScalarUDFImpl for SparkStrToMap {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        self.inner.return_field_from_args(args)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // Splitting a string cannot produce a NULL key, so only the duplicate-key error needs
        // restating here.
        self.inner
            .invoke_with_args(args)
            .map_err(|error| as_spark_error(error, DuplicateKeyFormat::Quoted))
    }
}

/// Materializes scalar arguments so the validation below indexes rows the same way the kernel
/// does. `make_scalar_function` inside the kernel expands them anyway, so this only moves that
/// work earlier.
fn expand_scalars(mut args: ScalarFunctionArgs) -> Result<ScalarFunctionArgs> {
    let number_rows = args.number_rows;
    for arg in args.args.iter_mut() {
        if let ColumnarValue::Scalar(scalar) = arg {
            *arg = ColumnarValue::Array(scalar.to_array_of_size(number_rows)?);
        }
    }
    Ok(args)
}

/// Rejects the inputs Spark's `MapFromArrays` rejects before building the map: a row whose key
/// and value arrays differ in length, and a `NULL` key element.
fn validate_map_from_arrays(keys: &ArrayRef, values: &ArrayRef) -> Result<()> {
    // A `NULL`-typed argument makes every row a NULL map, which never reaches the builder.
    if matches!(keys.data_type(), DataType::Null) || matches!(values.data_type(), DataType::Null) {
        return Ok(());
    }
    let (flat_keys, key_offsets) = list_values_and_offsets(keys)?;
    let (_, value_offsets) = list_values_and_offsets(values)?;
    if key_offsets.len() != value_offsets.len() {
        return exec_err!("map_from_arrays: keys and values must have the same number of rows");
    }
    let key_nulls = element_validity(&flat_keys);

    for row in 0..key_offsets.len().saturating_sub(1) {
        // `MapFromArrays` is null intolerant, so a NULL input array yields a NULL map without
        // evaluating the builder.
        if !keys.is_valid(row) || !values.is_valid(row) {
            continue;
        }
        let (start, end) = (key_offsets[row], key_offsets[row + 1]);
        if end - start != value_offsets[row + 1] - value_offsets[row] {
            return Err(SparkError::MapKeyValueDiffSizes.into());
        }
        if let Some(nulls) = &key_nulls {
            if nulls.slice(start, end - start).null_count() > 0 {
                return Err(SparkError::NullMapKey.into());
            }
        }
    }
    Ok(())
}

/// Rejects a `NULL` key element in the rows `map_from_entries` actually builds a map from. A row
/// is skipped when its entries array is NULL or holds a NULL `struct` element, since Spark
/// returns a NULL map for both without inserting any entry.
fn validate_map_from_entries(entries: &ArrayRef) -> Result<()> {
    if matches!(entries.data_type(), DataType::Null) {
        return Ok(());
    }
    let (elements, offsets) = list_values_and_offsets(entries)?;
    let Some(structs) = elements.as_any().downcast_ref::<StructArray>() else {
        return exec_err!(
            "map_from_entries: expected array<struct<key, value>>, got {:?}",
            elements.data_type()
        );
    };
    let Some(key_nulls) = element_validity(structs.column(0)) else {
        return Ok(());
    };
    let element_nulls = structs.nulls();

    for row in 0..offsets.len().saturating_sub(1) {
        if !entries.is_valid(row) {
            continue;
        }
        let (start, len) = (offsets[row], offsets[row + 1] - offsets[row]);
        if element_nulls.is_some_and(|nulls| nulls.slice(start, len).null_count() > 0) {
            continue;
        }
        if key_nulls.slice(start, len).null_count() > 0 {
            return Err(SparkError::NullMapKey.into());
        }
    }
    Ok(())
}

/// The flattened element array of a list argument together with its per-row offsets. The offsets
/// index into the returned array, which a slice of the list does not itself narrow.
fn list_values_and_offsets(array: &ArrayRef) -> Result<(ArrayRef, Vec<usize>)> {
    match array.data_type() {
        DataType::List(_) => {
            let list = array.as_list::<i32>();
            let offsets = list.offsets().iter().map(|o| *o as usize).collect();
            Ok((Arc::clone(list.values()), offsets))
        }
        DataType::LargeList(_) => {
            let list = array.as_list::<i64>();
            let offsets = list.offsets().iter().map(|o| *o as usize).collect();
            Ok((Arc::clone(list.values()), offsets))
        }
        DataType::FixedSizeList(_, size) => {
            let list = array.as_fixed_size_list();
            let size = *size as usize;
            let offsets = (0..=list.len()).map(|row| row * size).collect();
            Ok((Arc::clone(list.values()), offsets))
        }
        other => exec_err!("expected list, large_list or fixed_size_list, got {other:?}"),
    }
}

/// The per-element validity of a map key array, or `None` when no element is NULL. A `NullArray`
/// carries no null buffer even though all of its elements are NULL, so report one for it.
fn element_validity(array: &ArrayRef) -> Option<NullBuffer> {
    if matches!(array.data_type(), DataType::Null) {
        return Some(NullBuffer::new_null(array.len()));
    }
    array
        .nulls()
        .filter(|nulls| nulls.null_count() > 0)
        .cloned()
}

/// How the upstream kernel renders the offending key in its duplicate-key message.
#[derive(Clone, Copy)]
enum DuplicateKeyFormat {
    /// The map builders write the key as-is, which is what Spark's `key.toString` produces.
    Bare,
    /// `str_to_map` single-quotes it.
    Quoted,
}

/// Restates the upstream duplicate-key error as `SparkError::DuplicatedMapKey` so the JVM side
/// raises Spark's `DUPLICATED_MAP_KEY` naming the same key. Any other error is passed through.
fn as_spark_error(error: DataFusionError, key_format: DuplicateKeyFormat) -> DataFusionError {
    match duplicate_map_key(&error.to_string(), key_format) {
        Some(key) => SparkError::DuplicatedMapKey { key }.into(),
        None => error,
    }
}

/// The key named by `datafusion-spark`'s duplicate-key message. The
/// `*_reports_the_duplicate_key` tests pin the wordings this parses against the kernels
/// themselves, so an upstream rewording fails there rather than silently downgrading the error
/// to a generic execution failure.
fn duplicate_map_key(message: &str, key_format: DuplicateKeyFormat) -> Option<String> {
    let (open, close) = match key_format {
        DuplicateKeyFormat::Bare => ("[DUPLICATED_MAP_KEY] Duplicate map key ", " was found"),
        DuplicateKeyFormat::Quoted => ("[DUPLICATED_MAP_KEY] Duplicate map key '", "' was found"),
    };
    let (_, tail) = message.split_once(open)?;
    let (key, _) = tail.rsplit_once(close)?;
    Some(key.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, ListArray, MapArray, StringArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{Field, Fields};
    use datafusion::common::config::{ConfigOptions, MapKeyDedupPolicy};
    use datafusion::common::ScalarValue;

    /// `[[1, 2], [3]]`-shaped keys, with `nulls` marking whole rows NULL.
    fn int_list(values: Int32Array, offsets: &[i32], nulls: Option<NullBuffer>) -> ArrayRef {
        let field = Arc::new(Field::new("item", DataType::Int32, true));
        Arc::new(ListArray::new(
            field,
            OffsetBuffer::new(offsets.to_vec().into()),
            Arc::new(values),
            nulls,
        ))
    }

    fn string_list(values: StringArray, offsets: &[i32], nulls: Option<NullBuffer>) -> ArrayRef {
        let field = Arc::new(Field::new("item", DataType::Utf8, true));
        Arc::new(ListArray::new(
            field,
            OffsetBuffer::new(offsets.to_vec().into()),
            Arc::new(values),
            nulls,
        ))
    }

    /// `array<struct<key int, value string>>`, with `element_nulls` marking NULL entries.
    fn entry_list(
        keys: Int32Array,
        values: StringArray,
        offsets: &[i32],
        element_nulls: Option<NullBuffer>,
    ) -> ArrayRef {
        let fields = Fields::from(vec![
            Field::new("key", DataType::Int32, true),
            Field::new("value", DataType::Utf8, true),
        ]);
        let structs = StructArray::new(
            fields.clone(),
            vec![Arc::new(keys), Arc::new(values)],
            element_nulls,
        );
        let field = Arc::new(Field::new("item", DataType::Struct(fields), true));
        Arc::new(ListArray::new(
            field,
            OffsetBuffer::new(offsets.to_vec().into()),
            Arc::new(structs),
            None,
        ))
    }

    fn invoke(
        udf: &dyn ScalarUDFImpl,
        args: Vec<ArrayRef>,
        policy: MapKeyDedupPolicy,
    ) -> Result<ColumnarValue> {
        let arg_fields: Vec<FieldRef> = args
            .iter()
            .enumerate()
            .map(|(i, arg)| Arc::new(Field::new(format!("arg{i}"), arg.data_type().clone(), true)))
            .collect();
        let scalar_arguments: Vec<Option<&ScalarValue>> = vec![None; args.len()];
        let return_field = udf.return_field_from_args(ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &scalar_arguments,
        })?;
        let mut config = ConfigOptions::default();
        config.spark.map_key_dedup_policy = policy;
        let number_rows = args.first().map(|arg| arg.len()).unwrap_or(0);
        udf.invoke_with_args(ScalarFunctionArgs {
            args: args.into_iter().map(ColumnarValue::Array).collect(),
            arg_fields,
            number_rows,
            return_field,
            config_options: Arc::new(config),
        })
    }

    fn map_result(value: ColumnarValue) -> MapArray {
        match value {
            ColumnarValue::Array(array) => array.as_map().clone(),
            ColumnarValue::Scalar(scalar) => {
                scalar.to_array().expect("scalar to array").as_map().clone()
            }
        }
    }

    #[test]
    fn map_from_arrays_rejects_null_key() {
        let keys = int_list(Int32Array::from(vec![Some(1), None]), &[0, 2], None);
        let values = string_list(StringArray::from(vec![Some("a"), Some("b")]), &[0, 2], None);
        let err = invoke(
            &SparkMapFromArrays::default(),
            vec![keys, values],
            MapKeyDedupPolicy::Exception,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("[NULL_MAP_KEY]"), "{err}");
    }

    #[test]
    fn map_from_arrays_ignores_null_key_in_a_null_row() {
        // Row 0's keys array is NULL, so Spark returns a NULL map without inspecting its keys.
        let keys = int_list(
            Int32Array::from(vec![None, Some(1)]),
            &[0, 1, 2],
            Some(NullBuffer::from(vec![false, true])),
        );
        let values = string_list(
            StringArray::from(vec![Some("a"), Some("b")]),
            &[0, 1, 2],
            None,
        );
        let result = map_result(
            invoke(
                &SparkMapFromArrays::default(),
                vec![keys, values],
                MapKeyDedupPolicy::Exception,
            )
            .unwrap(),
        );
        assert!(result.is_null(0));
        assert_eq!(result.value_offsets(), &[0, 0, 1]);
    }

    #[test]
    fn map_from_arrays_rejects_key_value_length_mismatch() {
        let keys = int_list(Int32Array::from(vec![1, 2]), &[0, 2], None);
        let values = string_list(StringArray::from(vec![Some("a")]), &[0, 1], None);
        let err = invoke(
            &SparkMapFromArrays::default(),
            vec![keys, values],
            MapKeyDedupPolicy::Exception,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("[MAP_KEY_VALUE_DIFF_SIZES]"), "{err}");
    }

    /// Pins the upstream message `duplicate_map_key` parses: a wording change upstream fails here
    /// rather than silently downgrading the error to a generic execution failure.
    #[test]
    fn map_from_arrays_reports_the_duplicate_key() {
        let keys = int_list(Int32Array::from(vec![7, 7]), &[0, 2], None);
        let values = string_list(StringArray::from(vec![Some("a"), Some("b")]), &[0, 2], None);
        let err = invoke(
            &SparkMapFromArrays::default(),
            vec![keys, values],
            MapKeyDedupPolicy::Exception,
        )
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("[DUPLICATED_MAP_KEY] Cannot create map with duplicate keys: 7."),
            "{err}"
        );
    }

    /// Spark's `duplicateMapKeyFoundError` reports `key.toString`, so a string key carries no
    /// quotes. `str_to_map` quotes its key and `map_from_arrays` does not, which is why the two
    /// go through different `DuplicateKeyFormat`s.
    #[test]
    fn map_from_arrays_reports_a_string_duplicate_key_unquoted() {
        let field = Arc::new(Field::new("item", DataType::Utf8, true));
        let keys: ArrayRef = Arc::new(ListArray::new(
            field,
            OffsetBuffer::new(vec![0i32, 2].into()),
            Arc::new(StringArray::from(vec![Some("a"), Some("a")])),
            None,
        ));
        let values = string_list(StringArray::from(vec![Some("1"), Some("2")]), &[0, 2], None);
        let err = invoke(
            &SparkMapFromArrays::default(),
            vec![keys, values],
            MapKeyDedupPolicy::Exception,
        )
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("[DUPLICATED_MAP_KEY] Cannot create map with duplicate keys: a."),
            "{err}"
        );
    }

    #[test]
    fn map_from_arrays_honours_last_win() {
        let keys = int_list(Int32Array::from(vec![7, 7]), &[0, 2], None);
        let values = string_list(StringArray::from(vec![Some("a"), Some("b")]), &[0, 2], None);
        let result = map_result(
            invoke(
                &SparkMapFromArrays::default(),
                vec![keys, values],
                MapKeyDedupPolicy::LastWin,
            )
            .unwrap(),
        );
        assert_eq!(result.value_offsets(), &[0, 1]);
        let values = result.entries().column(1).as_string::<i32>().clone();
        assert_eq!(values.value(0), "b");
    }

    #[test]
    fn map_from_entries_rejects_null_key() {
        let entries = entry_list(
            Int32Array::from(vec![Some(1), None]),
            StringArray::from(vec![Some("a"), Some("b")]),
            &[0, 2],
            None,
        );
        let err = invoke(
            &SparkMapFromEntries::default(),
            vec![entries],
            MapKeyDedupPolicy::Exception,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("[NULL_MAP_KEY]"), "{err}");
    }

    #[test]
    fn map_from_entries_ignores_a_null_entry() {
        // A NULL struct element makes the whole row a NULL map, so its NULL key is never a key.
        let entries = entry_list(
            Int32Array::from(vec![None, Some(2)]),
            StringArray::from(vec![None, Some("b")]),
            &[0, 1, 2],
            Some(NullBuffer::from(vec![false, true])),
        );
        let result = map_result(
            invoke(
                &SparkMapFromEntries::default(),
                vec![entries],
                MapKeyDedupPolicy::Exception,
            )
            .unwrap(),
        );
        assert!(result.is_null(0));
        assert_eq!(result.value_offsets(), &[0, 0, 1]);
    }

    #[test]
    fn map_from_entries_honours_last_win() {
        let entries = entry_list(
            Int32Array::from(vec![7, 7]),
            StringArray::from(vec![Some("a"), Some("b")]),
            &[0, 2],
            None,
        );
        let result = map_result(
            invoke(
                &SparkMapFromEntries::default(),
                vec![entries],
                MapKeyDedupPolicy::LastWin,
            )
            .unwrap(),
        );
        assert_eq!(result.value_offsets(), &[0, 1]);
        let values = result.entries().column(1).as_string::<i32>().clone();
        assert_eq!(values.value(0), "b");
    }

    #[test]
    fn str_to_map_reports_the_duplicate_key() {
        let text: ArrayRef = Arc::new(StringArray::from(vec![Some("a:1,b:2,a:3")]));
        let err = invoke(
            &SparkStrToMap::default(),
            vec![text],
            MapKeyDedupPolicy::Exception,
        )
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("[DUPLICATED_MAP_KEY] Cannot create map with duplicate keys: a."),
            "{err}"
        );
    }

    #[test]
    fn str_to_map_honours_last_win() {
        let text: ArrayRef = Arc::new(StringArray::from(vec![Some("a:1,b:2,a:3")]));
        let result = map_result(
            invoke(
                &SparkStrToMap::default(),
                vec![text],
                MapKeyDedupPolicy::LastWin,
            )
            .unwrap(),
        );
        assert_eq!(result.value_offsets(), &[0, 2]);
    }

    #[test]
    fn duplicate_map_key_ignores_unrelated_errors() {
        assert_eq!(
            duplicate_map_key("Execution error: something else", DuplicateKeyFormat::Bare),
            None
        );
        assert_eq!(
            duplicate_map_key(
                "Execution error: something else",
                DuplicateKeyFormat::Quoted
            ),
            None
        );
    }
}
