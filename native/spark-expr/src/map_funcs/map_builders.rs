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
//! The `datafusion-spark` kernels build the `MapArray` the way Spark's `ArrayBasedMapBuilder`
//! does: row by row, checking a row's key and value lengths and then its keys in order, under the
//! duplicate-key policy in `datafusion.spark.map_key_dedup_policy`. These wrappers add the one
//! check the kernels skip, a `NULL` key, and restate the kernels' errors as the `SparkError`s the
//! JVM side turns back into Spark's own:
//!
//! - a row whose key and value arrays differ in length raises `SparkError::MapKeyValueDiffSizes`,
//!   which reaches the user as Spark's `_LEGACY_ERROR_TEMP_2128`;
//! - a `NULL` key raises `NULL_MAP_KEY` and, under `EXCEPTION`, a duplicate key raises
//!   `DUPLICATED_MAP_KEY` naming the key. Spark inserts entries one at a time, so whichever comes
//!   first in the row decides which of the two it reports.
//!
//! `str_to_map` builds its keys by splitting a string, so it needs only the duplicate-key
//! restatement.

use crate::SparkError;
use arrow::array::{Array, ArrayRef, AsArray, ListArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, FieldRef};
use datafusion::common::config::MapKeyDedupPolicy;
use datafusion::common::{exec_err, DataFusionError, HashSet, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use datafusion_spark::function::map::map_from_arrays::MapFromArrays as DataFusionMapFromArrays;
use datafusion_spark::function::map::map_from_entries::MapFromEntries as DataFusionMapFromEntries;
use datafusion_spark::function::map::str_to_map::SparkStrToMap as DataFusionStrToMap;
use std::sync::Arc;

/// Spark-compatible `map_from_arrays(keys, values)`.
#[derive(Debug, Default, PartialEq, Eq, Hash)]
pub struct SparkMapFromArrays {
    inner: DataFusionMapFromArrays,
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
        invoke_list_builder(&self.inner, args, |args, last_value_wins| match args {
            [ColumnarValue::Array(keys), ColumnarValue::Array(values)] => {
                validate_map_from_arrays(keys, values, last_value_wins)
            }
            other => exec_err!("map_from_arrays expects 2 arguments, got {}", other.len()),
        })
    }
}

/// Spark-compatible `map_from_entries(entries)`.
#[derive(Debug, Default, PartialEq, Eq, Hash)]
pub struct SparkMapFromEntries {
    inner: DataFusionMapFromEntries,
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
        invoke_list_builder(&self.inner, args, |args, last_value_wins| match args {
            [ColumnarValue::Array(entries)] => validate_map_from_entries(entries, last_value_wins),
            other => exec_err!("map_from_entries expects 1 argument, got {}", other.len()),
        })
    }
}

/// Spark-compatible `str_to_map(text[, pair_delim[, key_value_delim]])`.
#[derive(Debug, Default, PartialEq, Eq, Hash)]
pub struct SparkStrToMap {
    inner: DataFusionStrToMap,
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

/// Runs `map_from_arrays` or `map_from_entries`: `validate` sees the arguments as arrays whose
/// entries start at offset zero and rejects a `NULL` key, which the kernel would store without a
/// word, then the kernel builds the maps and its own errors are restated.
fn invoke_list_builder(
    inner: &dyn ScalarUDFImpl,
    mut args: ScalarFunctionArgs,
    validate: impl FnOnce(&[ColumnarValue], bool) -> Result<()>,
) -> Result<ColumnarValue> {
    // The kernel evaluates an all-scalar call once and returns a scalar, which DataFusion then
    // broadcasts to the batch; build that one row here too rather than `number_rows` copies.
    let all_scalar = args
        .args
        .iter()
        .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
    if all_scalar {
        args.number_rows = 1;
    }
    expand_scalars(&mut args)?;
    rebase_sliced_lists(&mut args)?;
    let last_value_wins =
        args.config_options.spark.map_key_dedup_policy == MapKeyDedupPolicy::LastWin;
    validate(&args.args, last_value_wins)?;
    let result = inner
        .invoke_with_args(args)
        .map_err(|error| as_spark_error(error, DuplicateKeyFormat::Bare))?;
    match (all_scalar, result) {
        (true, ColumnarValue::Array(array)) => Ok(ColumnarValue::Scalar(
            ScalarValue::try_from_array(&array, 0)?,
        )),
        (_, result) => Ok(result),
    }
}

/// Materializes scalar arguments at `number_rows`, so the validation indexes rows the same way
/// the kernel does.
fn expand_scalars(args: &mut ScalarFunctionArgs) -> Result<()> {
    let number_rows = args.number_rows;
    for arg in args.args.iter_mut() {
        if let ColumnarValue::Scalar(scalar) = arg {
            *arg = ColumnarValue::Array(scalar.to_array_of_size(number_rows)?);
        }
    }
    Ok(())
}

/// Rebases a list argument whose entries do not start at offset zero, which the kernels mishandle
/// (apache/datafusion#25419): they read each row's entries at its own offset but apply the mask
/// that selects the surviving keys from the start of the list's values, so a list sliced past its
/// first row pairs values with keys from earlier rows. A `LIMIT ... OFFSET` above a projection is
/// enough to produce one. A slice that only drops trailing rows needs nothing, since Arrow's
/// `filter` accepts a predicate shorter than the array it filters. Shifting the offsets and slicing
/// the values leaves the data where it is.
///
/// Delete once Comet moves to a DataFusion release carrying apache/datafusion#25431.
fn rebase_sliced_lists(args: &mut ScalarFunctionArgs) -> Result<()> {
    for arg in args.args.iter_mut() {
        let ColumnarValue::Array(array) = arg else {
            continue;
        };
        let (DataType::List(field), Some(list)) = (array.data_type(), array.as_list_opt::<i32>())
        else {
            continue;
        };
        let offsets = list.value_offsets();
        let (first, last) = (offsets[0], offsets[offsets.len() - 1]);
        if first == 0 {
            continue;
        }
        let rebased = ListArray::try_new(
            Arc::clone(field),
            OffsetBuffer::new(offsets.iter().map(|offset| offset - first).collect()),
            list.values().slice(first as usize, (last - first) as usize),
            list.nulls().cloned(),
        )?;
        *arg = ColumnarValue::Array(Arc::new(rebased));
    }
    Ok(())
}

/// Rejects a `NULL` key in a row `map_from_arrays` builds a map from, raising what Spark raises
/// for that row. The kernel already checks each row's lengths and duplicate keys in Spark's order,
/// so a batch where no such row holds a `NULL` key is left to it. A row whose keys or values array
/// is NULL gives a NULL map before Spark builds anything, so its keys do not count.
fn validate_map_from_arrays(
    keys: &ArrayRef,
    values: &ArrayRef,
    last_value_wins: bool,
) -> Result<()> {
    // A `NULL`-typed argument makes every row a NULL map, which never reaches the builder.
    if matches!(keys.data_type(), DataType::Null) || matches!(values.data_type(), DataType::Null) {
        return Ok(());
    }
    let (keys, values) = (as_list(keys)?, as_list(values)?);
    let Some(key_nulls) = key_nulls(keys.values()) else {
        return Ok(());
    };
    let (key_offsets, value_offsets) = (keys.value_offsets(), values.value_offsets());
    let row_is_built = |row: usize| keys.is_valid(row) && values.is_valid(row);
    let Some(failing_row) = first_row_with_null_key(&key_nulls, key_offsets, row_is_built) else {
        return Ok(());
    };
    // `failing_row` fails one way or another, so walk the rows up to it in Spark's order: a row's
    // lengths are checked before any of its keys.
    let mut seen = HashSet::new();
    for row in (0..=failing_row).filter(|&row| row_is_built(row)) {
        let (start, end) = (key_offsets[row] as usize, key_offsets[row + 1] as usize);
        if end - start != (value_offsets[row + 1] - value_offsets[row]) as usize {
            return Err(SparkError::MapKeyValueDiffSizes.into());
        }
        check_keys_in_order(
            keys.values(),
            start,
            end,
            &key_nulls,
            last_value_wins,
            &mut seen,
        )?;
    }
    Ok(())
}

/// Rejects a `NULL` key in a row `map_from_entries` builds a map from, raising what Spark raises
/// for that row. A row is not built when its entries array is NULL or holds a NULL `struct`
/// element: Spark returns a NULL map for both without inserting any entry.
fn validate_map_from_entries(entries: &ArrayRef, last_value_wins: bool) -> Result<()> {
    if matches!(entries.data_type(), DataType::Null) {
        return Ok(());
    }
    let entries = as_list(entries)?;
    let Some(structs) = entries.values().as_struct_opt() else {
        return exec_err!(
            "map_from_entries: expected array<struct<key, value>>, got {:?}",
            entries.values().data_type()
        );
    };
    let keys = structs.column(0);
    let Some(key_nulls) = key_nulls(keys) else {
        return Ok(());
    };
    // Only a `NULL` key inside a non-NULL entry can fail, and one AND and a popcount rule that
    // out, which covers the common batch whose only `NULL` keys sit under `NULL` entries.
    let entry_nulls = structs.nulls();
    let null_entries = entry_nulls.map_or(0, NullBuffer::null_count);
    let null_entries_or_keys =
        NullBuffer::union(Some(&key_nulls), entry_nulls).map_or(0, |nulls| nulls.null_count());
    if null_entries_or_keys == null_entries {
        return Ok(());
    }
    let offsets = entries.value_offsets();
    let row_is_built = |row: usize| {
        let (start, end) = (offsets[row] as usize, offsets[row + 1] as usize);
        entries.is_valid(row)
            && entry_nulls.is_none_or(|nulls| nulls.slice(start, end - start).null_count() == 0)
    };
    let Some(failing_row) = first_row_with_null_key(&key_nulls, offsets, row_is_built) else {
        return Ok(());
    };
    let mut seen = HashSet::new();
    for row in (0..=failing_row).filter(|&row| row_is_built(row)) {
        let (start, end) = (offsets[row] as usize, offsets[row + 1] as usize);
        check_keys_in_order(keys, start, end, &key_nulls, last_value_wins, &mut seen)?;
    }
    Ok(())
}

/// The first row that `row_is_built` accepts and whose keys include a `NULL`.
fn first_row_with_null_key(
    key_nulls: &NullBuffer,
    offsets: &[i32],
    row_is_built: impl Fn(usize) -> bool,
) -> Option<usize> {
    (0..offsets.len() - 1).find(|&row| {
        let (start, end) = (offsets[row] as usize, offsets[row + 1] as usize);
        key_nulls.slice(start, end - start).null_count() > 0 && row_is_built(row)
    })
}

/// Walks one row's keys in the order Spark's `ArrayBasedMapBuilder` inserts them, so whichever of
/// a `NULL` key and a duplicate key comes first is the one reported, as Spark reports it.
fn check_keys_in_order(
    keys: &ArrayRef,
    start: usize,
    end: usize,
    key_nulls: &NullBuffer,
    last_value_wins: bool,
    seen: &mut HashSet<ScalarValue>,
) -> Result<()> {
    seen.clear();
    for index in start..end {
        if key_nulls.is_null(index) {
            return Err(SparkError::NullMapKey.into());
        }
        // `LAST_WIN` overwrites a duplicate rather than raising, so only the `NULL` check is
        // left to do in that mode.
        if last_value_wins {
            continue;
        }
        let key = ScalarValue::try_from_array(keys, index)?.compacted();
        if let Some(duplicate) = seen.replace(key) {
            return Err(SparkError::DuplicatedMapKey {
                key: duplicate.to_string(),
            }
            .into());
        }
    }
    Ok(())
}

/// Comet hands every Spark `ArrayType` to native code as a `List`.
fn as_list(array: &ArrayRef) -> Result<&ListArray> {
    match array.as_list_opt::<i32>() {
        Some(list) => Ok(list),
        None => exec_err!("expected a list argument, got {:?}", array.data_type()),
    }
}

/// The nulls of a map key array, or `None` when no key is `NULL`. Logical nulls, so a `NullArray`
/// (which has no null buffer) and a dictionary whose values hold a `NULL` count too.
fn key_nulls(keys: &ArrayRef) -> Option<NullBuffer> {
    keys.logical_nulls().filter(|nulls| nulls.null_count() > 0)
}

/// How the upstream kernel renders the offending key in its duplicate-key message.
#[derive(Clone, Copy)]
enum DuplicateKeyFormat {
    /// The map builders write the key unquoted, as Spark does.
    Bare,
    /// `str_to_map` single-quotes it.
    Quoted,
}

/// The message the kernels' shared map builder raises for a row whose key and value arrays differ
/// in length.
const LENGTH_MISMATCH_MESSAGE: &str =
    "keys and values lists in the same row must have equal lengths";

/// Restates the upstream length-mismatch and duplicate-key errors as `SparkError`s, so the JVM
/// side raises Spark's own errors (naming the same key, for a duplicate). Any other error is
/// passed through. The `*_reports_*` tests pin the wordings parsed here against the kernels
/// themselves, so an upstream rewording fails there rather than silently downgrading the error to
/// a generic execution failure.
fn as_spark_error(error: DataFusionError, key_format: DuplicateKeyFormat) -> DataFusionError {
    let message = error.to_string();
    if message.contains(LENGTH_MISMATCH_MESSAGE) {
        return SparkError::MapKeyValueDiffSizes.into();
    }
    match duplicate_map_key(&message, key_format) {
        Some(key) => SparkError::DuplicatedMapKey { key }.into(),
        None => error,
    }
}

/// The key named by `datafusion-spark`'s duplicate-key message.
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
    use arrow::array::{Int32Array, MapArray, StringArray, StructArray};
    use arrow::datatypes::{Field, Fields, Int32Type};
    use datafusion::common::config::ConfigOptions;

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

    fn invoke_values(
        udf: &dyn ScalarUDFImpl,
        args: Vec<ColumnarValue>,
        number_rows: usize,
        policy: MapKeyDedupPolicy,
    ) -> Result<ColumnarValue> {
        let arg_fields: Vec<FieldRef> = args
            .iter()
            .enumerate()
            .map(|(i, arg)| Arc::new(Field::new(format!("arg{i}"), arg.data_type(), true)))
            .collect();
        let scalar_arguments: Vec<Option<&ScalarValue>> = vec![None; args.len()];
        let return_field = udf.return_field_from_args(ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &scalar_arguments,
        })?;
        let mut config = ConfigOptions::default();
        config.spark.map_key_dedup_policy = policy;
        udf.invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows,
            return_field,
            config_options: Arc::new(config),
        })
    }

    fn invoke(
        udf: &dyn ScalarUDFImpl,
        args: Vec<ArrayRef>,
        policy: MapKeyDedupPolicy,
    ) -> Result<ColumnarValue> {
        let number_rows = args.first().map(|arg| arg.len()).unwrap_or(0);
        let args = args.into_iter().map(ColumnarValue::Array).collect();
        invoke_values(udf, args, number_rows, policy)
    }

    fn invoke_err(
        udf: &dyn ScalarUDFImpl,
        args: Vec<ArrayRef>,
        policy: MapKeyDedupPolicy,
    ) -> String {
        invoke(udf, args, policy).unwrap_err().to_string()
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

    /// The serde guards only the keys array, so a row whose values array is NULL reaches the
    /// wrapper. Spark returns a NULL map for it without looking at its keys.
    #[test]
    fn map_from_arrays_ignores_null_key_in_a_row_with_null_values() {
        let keys = int_list(Int32Array::from(vec![None, Some(1)]), &[0, 1, 2], None);
        let values = string_list(
            StringArray::from(vec![Some("a"), Some("b")]),
            &[0, 1, 2],
            Some(NullBuffer::from(vec![false, true])),
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

    /// Pins the upstream duplicate-key message `duplicate_map_key` parses. The list builders write
    /// a string key unquoted, as Spark does; `str_to_map` quotes its key, which is why it goes
    /// through a different `DuplicateKeyFormat`.
    #[test]
    fn map_from_arrays_reports_the_duplicate_key_unquoted() {
        let keys = string_list(StringArray::from(vec![Some("a"), Some("a")]), &[0, 2], None);
        let values = int_list(Int32Array::from(vec![1, 2]), &[0, 2], None);
        let err = invoke_err(
            &SparkMapFromArrays::default(),
            vec![keys, values],
            MapKeyDedupPolicy::Exception,
        );
        assert!(
            err.contains("[DUPLICATED_MAP_KEY] Cannot create map with duplicate keys: a."),
            "{err}"
        );
    }

    /// Pins the upstream length-mismatch message `as_spark_error` recognizes.
    #[test]
    fn map_from_arrays_reports_a_length_mismatch() {
        let keys = int_list(Int32Array::from(vec![1, 2]), &[0, 2], None);
        let values = string_list(StringArray::from(vec![Some("a")]), &[0, 1], None);
        let err = invoke_err(
            &SparkMapFromArrays::default(),
            vec![keys, values],
            MapKeyDedupPolicy::Exception,
        );
        assert!(err.contains("[MAP_KEY_VALUE_DIFF_SIZES]"), "{err}");
    }

    /// Spark checks a row's lengths only when it reaches that row, so a duplicate key in an
    /// earlier row is reported ahead of a later row's length mismatch.
    #[test]
    fn map_from_arrays_reports_a_duplicate_before_a_later_length_mismatch() {
        let keys = int_list(Int32Array::from(vec![1, 1, 2]), &[0, 2, 3], None);
        let values = string_list(
            StringArray::from(vec![Some("a"), Some("b"), Some("c"), Some("d")]),
            &[0, 2, 4],
            None,
        );
        let err = invoke_err(
            &SparkMapFromArrays::default(),
            vec![keys, values],
            MapKeyDedupPolicy::Exception,
        );
        assert!(err.contains("[DUPLICATED_MAP_KEY]"), "{err}");
    }

    /// Spark checks a row's lengths before inserting any of its keys, so a length mismatch is
    /// reported ahead of a `NULL` key in the same row.
    #[test]
    fn map_from_arrays_reports_a_length_mismatch_before_a_null_key() {
        let keys = int_list(Int32Array::from(vec![None, Some(1)]), &[0, 2], None);
        let values = string_list(StringArray::from(vec![Some("a")]), &[0, 1], None);
        let err = invoke_err(
            &SparkMapFromArrays::default(),
            vec![keys, values],
            MapKeyDedupPolicy::Exception,
        );
        assert!(err.contains("[MAP_KEY_VALUE_DIFF_SIZES]"), "{err}");
    }

    /// Spark inserts entries one at a time, so a duplicate at an earlier index is reported even
    /// though a `NULL` key follows it.
    #[test]
    fn map_from_arrays_reports_a_duplicate_before_a_later_null_key() {
        let keys = int_list(
            Int32Array::from(vec![Some(1), Some(1), None]),
            &[0, 3],
            None,
        );
        let values = string_list(
            StringArray::from(vec![Some("a"), Some("b"), Some("c")]),
            &[0, 3],
            None,
        );
        let err = invoke_err(
            &SparkMapFromArrays::default(),
            vec![keys, values],
            MapKeyDedupPolicy::Exception,
        );
        assert!(err.contains("[DUPLICATED_MAP_KEY]"), "{err}");
    }

    /// The mirror case: the `NULL` comes first, so it is the one reported.
    #[test]
    fn map_from_arrays_reports_a_null_key_before_a_later_duplicate() {
        let keys = int_list(
            Int32Array::from(vec![None, Some(1), Some(1)]),
            &[0, 3],
            None,
        );
        let values = string_list(
            StringArray::from(vec![Some("a"), Some("b"), Some("c")]),
            &[0, 3],
            None,
        );
        let err = invoke_err(
            &SparkMapFromArrays::default(),
            vec![keys, values],
            MapKeyDedupPolicy::Exception,
        );
        assert!(err.contains("[NULL_MAP_KEY]"), "{err}");
    }

    /// A duplicate in an earlier row wins over a `NULL` key in a later one.
    #[test]
    fn map_from_arrays_reports_the_first_offending_row() {
        let keys = int_list(
            Int32Array::from(vec![Some(1), Some(1), None, Some(2)]),
            &[0, 2, 4],
            None,
        );
        let values = string_list(
            StringArray::from(vec![Some("a"), Some("b"), Some("c"), Some("d")]),
            &[0, 2, 4],
            None,
        );
        let err = invoke_err(
            &SparkMapFromArrays::default(),
            vec![keys, values],
            MapKeyDedupPolicy::Exception,
        );
        assert!(err.contains("[DUPLICATED_MAP_KEY]"), "{err}");
    }

    /// Under `LAST_WIN` a duplicate is not an error, so a `NULL` key is still reported.
    #[test]
    fn last_win_still_rejects_a_null_key_after_a_duplicate() {
        let keys = int_list(
            Int32Array::from(vec![Some(1), Some(1), None]),
            &[0, 3],
            None,
        );
        let values = string_list(
            StringArray::from(vec![Some("a"), Some("b"), Some("c")]),
            &[0, 3],
            None,
        );
        let err = invoke_err(
            &SparkMapFromArrays::default(),
            vec![keys, values],
            MapKeyDedupPolicy::LastWin,
        );
        assert!(err.contains("[NULL_MAP_KEY]"), "{err}");
    }

    /// An all-scalar call builds its one row once and returns a scalar, as the kernel alone does,
    /// rather than `number_rows` copies of it.
    #[test]
    fn map_from_arrays_evaluates_an_all_scalar_call_once() {
        let keys = int_list(Int32Array::from(vec![1, 2]), &[0, 2], None);
        let values = string_list(StringArray::from(vec![Some("a"), Some("b")]), &[0, 2], None);
        let scalar = |array: ArrayRef| {
            ColumnarValue::Scalar(ScalarValue::try_from_array(&array, 0).expect("scalar"))
        };
        let result = invoke_values(
            &SparkMapFromArrays::default(),
            vec![scalar(keys), scalar(values)],
            3,
            MapKeyDedupPolicy::Exception,
        )
        .unwrap();
        let ColumnarValue::Scalar(ScalarValue::Map(map)) = result else {
            panic!("expected a scalar map, got {result:?}");
        };
        assert_eq!(map.value_offsets(), &[0, 2]);
    }

    /// A `LIMIT ... OFFSET` above a projection hands the kernel a sliced list. A slice past the
    /// first row would read a preceding row's key without `rebase_sliced_lists`; a head slice
    /// needs no rebase.
    #[test]
    fn map_from_arrays_reads_the_right_row_of_a_sliced_list() {
        let keys = int_list(Int32Array::from(vec![10, 20]), &[0, 1, 2], None);
        let values = string_list(
            StringArray::from(vec![Some("100"), Some("200")]),
            &[0, 1, 2],
            None,
        );
        for (row, key, value) in [(0, 10, "100"), (1, 20, "200")] {
            let result = map_result(
                invoke(
                    &SparkMapFromArrays::default(),
                    vec![keys.slice(row, 1), values.slice(row, 1)],
                    MapKeyDedupPolicy::Exception,
                )
                .unwrap(),
            );
            assert_eq!(result.len(), 1);
            assert_eq!(
                result.keys().as_primitive::<Int32Type>().values().as_ref(),
                &[key]
            );
            assert_eq!(result.values().as_string::<i32>().value(0), value);
        }
    }

    #[test]
    fn map_from_entries_reads_the_right_row_of_a_sliced_list() {
        let entries = entry_list(
            Int32Array::from(vec![10, 20]),
            StringArray::from(vec![Some("100"), Some("200")]),
            &[0, 1, 2],
            None,
        );
        let result = map_result(
            invoke(
                &SparkMapFromEntries::default(),
                vec![entries.slice(1, 1)],
                MapKeyDedupPolicy::Exception,
            )
            .unwrap(),
        );
        assert_eq!(result.len(), 1);
        assert_eq!(
            result.keys().as_primitive::<Int32Type>().values().as_ref(),
            &[20]
        );
        assert_eq!(result.values().as_string::<i32>().value(0), "200");
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

    /// A NULL entry makes the whole row a NULL map, so a `NULL` key in another entry of the same
    /// row is never inserted either.
    #[test]
    fn map_from_entries_ignores_a_null_key_beside_a_null_entry() {
        let entries = entry_list(
            Int32Array::from(vec![None, None, Some(3)]),
            StringArray::from(vec![None, Some("b"), Some("c")]),
            &[0, 2, 3],
            Some(NullBuffer::from(vec![false, true, true])),
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
    fn map_from_entries_reports_a_duplicate_before_a_later_null_key() {
        let entries = entry_list(
            Int32Array::from(vec![Some(1), Some(1), None]),
            StringArray::from(vec![Some("a"), Some("b"), Some("c")]),
            &[0, 3],
            None,
        );
        let err = invoke_err(
            &SparkMapFromEntries::default(),
            vec![entries],
            MapKeyDedupPolicy::Exception,
        );
        assert!(err.contains("[DUPLICATED_MAP_KEY]"), "{err}");
    }

    /// Pins the quoted duplicate-key message `str_to_map` raises.
    #[test]
    fn str_to_map_reports_the_duplicate_key() {
        let text: ArrayRef = Arc::new(StringArray::from(vec![Some("a:1,b:2,a:3")]));
        let err = invoke_err(
            &SparkStrToMap::default(),
            vec![text],
            MapKeyDedupPolicy::Exception,
        );
        assert!(
            err.contains("[DUPLICATED_MAP_KEY] Cannot create map with duplicate keys: a."),
            "{err}"
        );
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
