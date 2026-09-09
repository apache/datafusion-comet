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

use arrow::array::{
    new_null_array, Array, ArrayRef, BooleanBufferBuilder, MapArray, NullBufferBuilder, Scalar,
    UInt32Array,
};
use arrow::buffer::BooleanBuffer;
use arrow::compute::kernels::cmp::eq;
use arrow::compute::take;
use arrow::datatypes::{DataType, FieldRef};
use datafusion::common::utils::take_function_args;
use datafusion::common::{exec_err, Result as DataFusionResult};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;

/// Spark's map lookup: `GetMapValue` (`m[k]`) and `element_at(<map>, k)`.
///
/// Overrides DataFusion's `map_extract` under the same name, and differs from it in two ways:
///
///   - it returns the matched **value** rather than a one-element list, so the planner does not
///     have to unwrap the list with a second `ListExtract` pass (see `planner.rs`);
///   - the lookup is vectorized. DataFusion's `general_map_extract_inner` re-slices the query key
///     and every candidate key into a fresh `ArrayRef` per comparison and compares them through
///     `dyn Array` equality, which made a constant-key lookup roughly 35x more expensive than any
///     other Comet map kernel and slower than Spark itself
///     ([#5795](https://github.com/apache/datafusion-comet/issues/5795)). Here a single Arrow
///     `eq` covers the whole batch of entries at once, the per-row work is a bit scan over the
///     resulting mask, and the values are gathered with one `take`.
///
/// Spark's own lookup returns the first entry whose key compares equal, so the mask scan stops at
/// the first match too. A missing key, a `NULL` map row, and a `NULL` lookup key all produce
/// `NULL`, matching `GetMapValueUtil.getValueEval` and `ElementAt`'s map overload.
///
/// Key types whose Spark equality this cannot reproduce (floating point, non-default collations,
/// complex keys) never reach here: `MapKeySupport` in `serde/maps.scala` declines them so the
/// expression falls back to Spark.
#[derive(Debug, Hash, Eq, PartialEq)]
pub struct SparkMapExtract {
    signature: Signature,
}

impl Default for SparkMapExtract {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkMapExtract {
    pub fn new() -> Self {
        Self {
            // `user_defined` so `coerce_types` runs and casts the lookup key to the map's key
            // type; Comet's planner applies that coercion to the argument expression.
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkMapExtract {
    fn name(&self) -> &str {
        "map_extract"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DataFusionResult<DataType> {
        let [map_type, _] = take_function_args(self.name(), arg_types)?;
        Ok(map_entry_fields(map_type)?.1.data_type().clone())
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DataFusionResult<Vec<DataType>> {
        let [map_type, _] = take_function_args(self.name(), arg_types)?;
        Ok(vec![
            map_type.clone(),
            map_entry_fields(map_type)?.0.data_type().clone(),
        ])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let [map_arg, key_arg] = take_function_args(self.name(), &args.args)?;
        spark_map_extract(map_arg, key_arg, args.number_rows)
    }
}

/// The `(key, value)` fields of a `Map`'s entry struct.
fn map_entry_fields(map_type: &DataType) -> DataFusionResult<(&FieldRef, &FieldRef)> {
    match map_type {
        DataType::Map(entries, _) => match entries.data_type() {
            DataType::Struct(fields) if fields.len() == 2 => Ok((&fields[0], &fields[1])),
            other => exec_err!("map_extract: map entries must be a two-field struct, got {other}"),
        },
        other => exec_err!("map_extract: the first argument must be a map, got {other}"),
    }
}

/// Look up `key_arg` in each row of `map_arg`, returning the matched value or `NULL`.
pub fn spark_map_extract(
    map_arg: &ColumnarValue,
    key_arg: &ColumnarValue,
    number_rows: usize,
) -> DataFusionResult<ColumnarValue> {
    let map_ref: ArrayRef = match map_arg {
        ColumnarValue::Array(array) => Arc::clone(array),
        ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(number_rows)?,
    };
    let Some(map_array) = map_ref.as_any().downcast_ref::<MapArray>() else {
        return exec_err!(
            "map_extract: the first argument must be a map, got {}",
            map_ref.data_type()
        );
    };

    let num_rows = map_array.len();
    let value_type = map_array.value_type();

    // Arrow keeps a sliced `MapArray`'s entries child intact and slices only the offsets, so the
    // offsets index the *unsliced* keys/values and the visible entries are the half-open range
    // [entries_start, entries_end). Comparing only that window keeps a native OFFSET from paying
    // for the entries it skipped.
    let offsets = map_array.offsets();
    let entries_start = offsets[0] as usize;
    let entries_end = offsets[num_rows] as usize;
    if entries_start == entries_end {
        // Every row is empty or NULL, so nothing can match.
        return Ok(ColumnarValue::Array(new_null_array(value_type, num_rows)));
    }
    let window_len = entries_end - entries_start;
    let keys = map_array.keys().slice(entries_start, window_len);

    let matched = match key_arg {
        ColumnarValue::Scalar(scalar) => {
            if scalar.is_null() {
                // Spark map keys are never NULL, so a NULL lookup key matches nothing.
                return Ok(ColumnarValue::Array(new_null_array(value_type, num_rows)));
            }
            let key = scalar.to_array_of_size(1)?;
            key_match_mask(&keys, &key, true)?
        }
        ColumnarValue::Array(key_array) => {
            if key_array.len() != num_rows {
                return exec_err!(
                    "map_extract: expected {num_rows} lookup keys, got {}",
                    key_array.len()
                );
            }
            // One vectorized compare needs a lookup key per *entry*, not per row, so gather each
            // row's key across that row's entries. Entries in a gap between two rows (offsets are
            // only required to be monotonic) keep index 0; the per-row scan below never reads
            // those positions.
            let mut gather = vec![0u32; window_len];
            for row in 0..num_rows {
                let start = offsets[row] as usize - entries_start;
                let end = offsets[row + 1] as usize - entries_start;
                gather[start..end].fill(row as u32);
            }
            let per_entry_key = take(key_array, &UInt32Array::from(gather), None)?;
            key_match_mask(&keys, &per_entry_key, false)?
        }
    };

    // Gather the first matching entry of each row. Map offsets are `i32`, so an entry index always
    // fits in `u32`.
    let mut indices = vec![0u32; num_rows];
    let mut nulls = NullBufferBuilder::new(num_rows);
    for row in 0..num_rows {
        let start = offsets[row] as usize - entries_start;
        let end = offsets[row + 1] as usize - entries_start;
        let found = (start..end).find(|&i| matched.value(i));
        if let Some(i) = found {
            indices[row] = (i + entries_start) as u32;
        }
        nulls.append(found.is_some());
    }
    let indices = UInt32Array::new(indices.into(), nulls.finish());

    Ok(ColumnarValue::Array(take(
        map_array.values(),
        &indices,
        None,
    )?))
}

/// A bit per map entry: set where the stored key equals the lookup key. `lookup` is either a
/// length-1 array broadcast over every entry (constant key) or one key per entry.
fn key_match_mask(
    keys: &ArrayRef,
    lookup: &ArrayRef,
    lookup_is_scalar: bool,
) -> DataFusionResult<BooleanBuffer> {
    // The planner casts the lookup key to the map's declared key type, so a mismatch here means
    // the runtime encoding is not the declared one (a dictionary-encoded key column, say). Reject
    // it rather than comparing incomparable encodings and reporting every row as a miss.
    if keys.data_type() != lookup.data_type() {
        return exec_err!(
            "map_extract: lookup key type {} does not match the map key type {}",
            lookup.data_type(),
            keys.data_type()
        );
    }
    let compared = if lookup_is_scalar {
        eq(keys, &Scalar::new(Arc::clone(lookup)))
    } else {
        eq(keys, lookup)
    };
    match compared {
        Ok(mask) => {
            // A NULL on either side compares as NULL, which is not a match.
            let (values, nulls) = mask.into_parts();
            Ok(match nulls {
                Some(nulls) if nulls.null_count() > 0 => &values & nulls.inner(),
                _ => values,
            })
        }
        // `eq` rejects nested key types. `MapKeySupport` declines those before they reach the
        // native lookup, but keep DataFusion's element-wise comparison as a backstop so this
        // kernel is never less capable than the one it replaces.
        Err(_) => Ok(elementwise_match_mask(keys, lookup, lookup_is_scalar)),
    }
}

fn elementwise_match_mask(
    keys: &ArrayRef,
    lookup: &ArrayRef,
    lookup_is_scalar: bool,
) -> BooleanBuffer {
    let mut builder = BooleanBufferBuilder::new(keys.len());
    for i in 0..keys.len() {
        let lookup_row = if lookup_is_scalar { 0 } else { i };
        builder.append(
            !lookup.is_null(lookup_row)
                && keys.slice(i, 1).as_ref() == lookup.slice(lookup_row, 1).as_ref(),
        );
    }
    builder.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray, StructArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{Field, Fields};
    use datafusion::common::ScalarValue;

    /// One row of a test map: `None` for a NULL map, otherwise its `(key, value)` entries.
    type MapRow<'a> = Option<Vec<(&'a str, Option<i32>)>>;

    /// `{"a": 1, "b": 2}`, `{}`, `{"c": 3, "a": 30}`, NULL, `{"b": NULL}`
    fn test_map() -> MapArray {
        map_from(vec![
            Some(vec![("a", Some(1)), ("b", Some(2))]),
            Some(vec![]),
            Some(vec![("c", Some(3)), ("a", Some(30))]),
            None,
            Some(vec![("b", None)]),
        ])
    }

    fn map_from(rows: Vec<MapRow>) -> MapArray {
        let mut keys = Vec::new();
        let mut values = Vec::new();
        let mut offsets = vec![0i32];
        let mut nulls = NullBufferBuilder::new(rows.len());
        for row in &rows {
            match row {
                Some(entries) => {
                    for (k, v) in entries {
                        keys.push(*k);
                        values.push(*v);
                    }
                    nulls.append(true);
                }
                None => nulls.append(false),
            }
            offsets.push(keys.len() as i32);
        }

        let key_field = Arc::new(Field::new("key", DataType::Utf8, false));
        let value_field = Arc::new(Field::new("value", DataType::Int32, true));
        let entries = StructArray::new(
            Fields::from(vec![Arc::clone(&key_field), Arc::clone(&value_field)]),
            vec![
                Arc::new(StringArray::from(keys)) as ArrayRef,
                Arc::new(Int32Array::from(values)) as ArrayRef,
            ],
            None,
        );
        let entries_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![key_field, value_field])),
            false,
        ));
        MapArray::try_new(
            entries_field,
            OffsetBuffer::new(offsets.into()),
            entries,
            nulls.finish(),
            false,
        )
        .unwrap()
    }

    fn extract(map: MapArray, key: ColumnarValue) -> Vec<Option<i32>> {
        let num_rows = map.len();
        let result = spark_map_extract(&ColumnarValue::Array(Arc::new(map)), &key, num_rows)
            .unwrap()
            .into_array(num_rows)
            .unwrap();
        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();
        (0..result.len())
            .map(|i| (!result.is_null(i)).then(|| result.value(i)))
            .collect()
    }

    fn key(value: &str) -> ColumnarValue {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(value.to_string())))
    }

    #[test]
    fn constant_key_hit_and_miss() {
        // A found key returns its value; an empty row, a NULL row, and a row without the key all
        // return NULL, as does a row whose stored value is NULL.
        assert_eq!(
            extract(test_map(), key("a")),
            vec![Some(1), None, Some(30), None, None]
        );
        assert_eq!(
            extract(test_map(), key("b")),
            vec![Some(2), None, None, None, None]
        );
        assert_eq!(extract(test_map(), key("zz")), vec![None; 5]);
    }

    #[test]
    fn duplicate_keys_return_the_first_match() {
        // Spark's `GetMapValueUtil` scans entries in order and stops at the first equal key, so a
        // map that kept duplicates (EXCEPTION dedup is a write-side check) resolves to the first.
        let map = map_from(vec![Some(vec![("a", Some(1)), ("a", Some(2))])]);
        assert_eq!(extract(map, key("a")), vec![Some(1)]);
    }

    #[test]
    fn null_lookup_key_matches_nothing() {
        assert_eq!(
            extract(test_map(), ColumnarValue::Scalar(ScalarValue::Utf8(None))),
            vec![None; 5]
        );
    }

    #[test]
    fn per_row_lookup_key() {
        // A different key per row, including a NULL key and a key looked up against a NULL map.
        let keys: ArrayRef = Arc::new(StringArray::from(vec![
            Some("b"),
            Some("a"),
            Some("c"),
            Some("a"),
            None,
        ]));
        assert_eq!(
            extract(test_map(), ColumnarValue::Array(keys)),
            vec![Some(2), None, Some(3), None, None]
        );
    }

    #[test]
    fn sliced_map_keeps_original_entry_offsets() {
        // Arrow slices a MapArray's offsets but not its entries, so the visible rows start part
        // way into the keys/values children. Reading the entries from index 0 would look up the
        // wrong rows.
        let map = test_map().slice(2, 3);
        assert_eq!(extract(map.clone(), key("a")), vec![Some(30), None, None]);
        assert_eq!(extract(map.clone(), key("c")), vec![Some(3), None, None]);
        assert_eq!(extract(map, key("b")), vec![None, None, None]);

        let keys: ArrayRef = Arc::new(StringArray::from(vec![Some("c"), Some("c"), Some("b")]));
        assert_eq!(
            extract(test_map().slice(2, 3), ColumnarValue::Array(keys)),
            vec![Some(3), None, None]
        );
    }

    #[test]
    fn all_rows_empty_or_null() {
        // The no-entries fast path still has to produce one NULL per row, typed as the value type.
        let map = map_from(vec![Some(vec![]), None, Some(vec![])]);
        assert_eq!(extract(map, key("a")), vec![None; 3]);
    }

    #[test]
    fn empty_input() {
        let map = map_from(vec![]);
        assert_eq!(extract(map, key("a")), Vec::<Option<i32>>::new());
    }

    #[test]
    fn non_string_keys() {
        // Integer keys go down the same vectorized compare; pin that the gathered value lines up
        // with the matching key rather than the key's position.
        let key_field = Arc::new(Field::new("key", DataType::Int32, false));
        let value_field = Arc::new(Field::new("value", DataType::Utf8, true));
        let entries = StructArray::new(
            Fields::from(vec![Arc::clone(&key_field), Arc::clone(&value_field)]),
            vec![
                Arc::new(Int32Array::from(vec![10, 20, 30])) as ArrayRef,
                Arc::new(StringArray::from(vec!["x", "y", "z"])) as ArrayRef,
            ],
            None,
        );
        let entries_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![key_field, value_field])),
            false,
        ));
        let map = MapArray::try_new(
            entries_field,
            OffsetBuffer::new(vec![0i32, 2, 3].into()),
            entries,
            None,
            false,
        )
        .unwrap();

        let result = spark_map_extract(
            &ColumnarValue::Array(Arc::new(map)),
            &ColumnarValue::Scalar(ScalarValue::Int32(Some(20))),
            2,
        )
        .unwrap()
        .into_array(2)
        .unwrap();
        let result = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result.value(0), "y");
        assert!(result.is_null(1));
    }

    #[test]
    fn scalar_map_argument() {
        // A constant map is expanded to the batch length; the lookup still runs per row.
        let map = map_from(vec![Some(vec![("a", Some(7))])]);
        let scalar = ColumnarValue::Scalar(ScalarValue::Map(Arc::new(map)));
        let result = spark_map_extract(&scalar, &key("a"), 3)
            .unwrap()
            .into_array(3)
            .unwrap();
        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(result.len(), 3);
        assert!((0..3).all(|i| result.value(i) == 7));
    }

    #[test]
    fn return_and_coerce_types() {
        let udf = SparkMapExtract::new();
        let map_type = test_map().data_type().clone();
        // The result is the map's value type, not a list of it.
        assert_eq!(
            udf.return_type(&[map_type.clone(), DataType::Utf8])
                .unwrap(),
            DataType::Int32
        );
        // A wider lookup key is narrowed to the map's key type by the planner.
        assert_eq!(
            udf.coerce_types(&[map_type, DataType::LargeUtf8]).unwrap(),
            vec![test_map().data_type().clone(), DataType::Utf8]
        );
    }

    #[test]
    fn non_map_first_argument_is_rejected() {
        let udf = SparkMapExtract::new();
        assert!(udf
            .return_type(&[DataType::Int32, DataType::Int32])
            .is_err());
        let err = spark_map_extract(
            &ColumnarValue::Array(Arc::new(Int32Array::from(vec![1]))),
            &ColumnarValue::Scalar(ScalarValue::Int32(Some(1))),
            1,
        )
        .unwrap_err();
        assert!(err.to_string().contains("must be a map"));
    }

    #[test]
    fn mismatched_key_type_is_rejected() {
        // A key the planner did not cast to the map's key type cannot be compared. Erroring keeps
        // the mismatch visible instead of reporting every row as a miss.
        let err = spark_map_extract(
            &ColumnarValue::Array(Arc::new(test_map())),
            &ColumnarValue::Scalar(ScalarValue::Int32(Some(1))),
            5,
        )
        .unwrap_err();
        assert!(err.to_string().contains("does not match the map key type"));
    }

    #[test]
    fn nested_key_falls_back_to_elementwise_comparison() {
        // `eq` refuses nested types. `MapKeySupport` keeps these on Spark, but the backstop has to
        // still find the key rather than error.
        let inner = Arc::new(Field::new("item", DataType::Int32, true));
        let key_values = arrow::array::ListArray::new(
            Arc::clone(&inner),
            OffsetBuffer::new(vec![0i32, 1, 2].into()),
            Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
            None,
        );
        let key_field = Arc::new(Field::new("key", key_values.data_type().clone(), false));
        let value_field = Arc::new(Field::new("value", DataType::Int32, true));
        let entries = StructArray::new(
            Fields::from(vec![Arc::clone(&key_field), Arc::clone(&value_field)]),
            vec![
                Arc::new(key_values) as ArrayRef,
                Arc::new(Int32Array::from(vec![11, 22])) as ArrayRef,
            ],
            None,
        );
        let entries_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![key_field, value_field])),
            false,
        ));
        let map = MapArray::try_new(
            entries_field,
            OffsetBuffer::new(vec![0i32, 2].into()),
            entries,
            None,
            false,
        )
        .unwrap();

        let lookup: ArrayRef = Arc::new(arrow::array::ListArray::new(
            inner,
            OffsetBuffer::new(vec![0i32, 1].into()),
            Arc::new(Int32Array::from(vec![2])) as ArrayRef,
            None,
        ));
        let result = spark_map_extract(
            &ColumnarValue::Array(Arc::new(map)),
            &ColumnarValue::Array(lookup),
            1,
        )
        .unwrap()
        .into_array(1)
        .unwrap();
        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(result.value(0), 22);
    }
}
