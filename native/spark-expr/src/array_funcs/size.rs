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

use arrow::array::{Array, ArrayRef, Int32Array, LargeListArray};
use arrow::buffer::NullBuffer;
use arrow::compute::kernels::length::length;
use arrow::compute::{cast_with_options, CastOptions};
use arrow::datatypes::{DataType, Field};
use datafusion::common::{exec_err, DataFusionError, Result as DataFusionResult, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;

/// Shared by the LargeList array path and `ScalarValue::LargeList` overflow guard.
const SIZE_OVERFLOW_MSG: &str = "size(): list length exceeds i32::MAX";

/// Spark size() function that returns the size of arrays or maps.
/// Returns -1 for null inputs (Spark behavior differs from standard SQL).
pub fn spark_size(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    if args.len() != 1 {
        return exec_err!("size function takes exactly one argument");
    }

    match &args[0] {
        ColumnarValue::Array(array) => {
            let result = spark_size_array(array)?;
            Ok(ColumnarValue::Array(result))
        }
        ColumnarValue::Scalar(scalar) => {
            let result = spark_size_scalar(scalar)?;
            Ok(ColumnarValue::Scalar(result))
        }
    }
}

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct SparkSizeFunc {
    signature: Signature,
}

impl Default for SparkSizeFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkSizeFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::uniform(
                1,
                vec![
                    List(Arc::new(Field::new("item", Null, true))),
                    LargeList(Arc::new(Field::new("item", Null, true))),
                    FixedSizeList(Arc::new(Field::new("item", Null, true)), -1),
                    Map(Arc::new(Field::new("entries", Null, true)), false),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkSizeFunc {
    fn name(&self) -> &str {
        "size"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Int32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        spark_size(&args.args)
    }
}

fn spark_size_array(array: &ArrayRef) -> Result<ArrayRef, DataFusionError> {
    match array.data_type() {
        // LargeList: build Int32 lengths directly from i64 offsets (avoids
        // length→Int64→cast→Int32).
        DataType::LargeList(_) => spark_size_large_list_from_offsets(array),
        // List / FixedSizeList: reuse Arrow's vectorized length kernel.
        DataType::List(_) | DataType::FixedSizeList(..) => spark_size_list_like(array),
        // Map is not supported by the length kernel; compute row lengths from
        // the entry-offset buffer directly.
        DataType::Map(_, _) => spark_size_map(array),
        _ => {
            exec_err!(
                "size function only supports arrays and maps, got: {:?}",
                array.data_type()
            )
        }
    }
}

/// Compute Spark `size()` for List / FixedSizeList via Arrow's `length` kernel,
/// then rewrite null inputs to `-1` (Spark's legacy/compatible size-of-null
/// behavior for this UDF).
fn spark_size_list_like(array: &ArrayRef) -> Result<ArrayRef, DataFusionError> {
    let lengths = length(array.as_ref())?;
    let lengths = match lengths.data_type() {
        DataType::Int32 => lengths,
        // Unsafe cast: overflow must error, not become null then get rewritten to -1.
        DataType::Int64 => cast_with_options(
            lengths.as_ref(),
            &DataType::Int32,
            &CastOptions {
                safe: false,
                ..Default::default()
            },
        )?,
        other => {
            return exec_err!("unexpected type from length kernel: {other:?}");
        }
    };

    // Fast path for the production shape: `CometSize.convert` wraps size() in a
    // `CASE WHEN isnotnull(child)` that filters null rows out before the THEN
    // branch runs, so this function only ever sees a null-free array in a real
    // Comet plan. Return the length kernel output as-is; skip the downcast and
    // `Int32Array::clone` that `rewrite_nulls_to_minus_one` would otherwise pay.
    if array.null_count() == 0 {
        return Ok(lengths);
    }

    let int_lengths = lengths
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| DataFusionError::Internal("Expected Int32Array from length".to_string()))?
        .clone();
    Ok(rewrite_nulls_to_minus_one(int_lengths))
}

/// Compute Spark `size()` for LargeList by subtracting adjacent i64 offsets into
/// Int32 lengths. Avoids Arrow's `length` kernel (which returns Int64 for
/// LargeList) and the subsequent Int32 cast allocation.
///
/// When the full offset span fits in `i32`, every per-row length does too, so the
/// hot path uses an unchecked `as i32` + `collect` (vectorization-friendly).
/// Otherwise fall back to per-row `i32::try_from` with [`SIZE_OVERFLOW_MSG`].
///
/// Null inputs become `-1`, matching `spark_size_list_like`.
fn spark_size_large_list_from_offsets(array: &ArrayRef) -> Result<ArrayRef, DataFusionError> {
    let list = array
        .as_any()
        .downcast_ref::<LargeListArray>()
        .ok_or_else(|| DataFusionError::Internal("Expected LargeListArray".to_string()))?;

    let offsets = list.offsets();
    // Offsets are monotonically non-decreasing, so no per-row length can exceed
    // the full span. If the span fits in i32, every row length does too.
    // `OffsetBuffer` always has at least one element.
    let range = *offsets.last().unwrap() - *offsets.first().unwrap();
    let values: Vec<i32> = if range > i32::MAX as i64 {
        spark_size_large_list_lengths_checked(offsets, list.nulls())?
    } else {
        offsets.windows(2).map(|w| (w[1] - w[0]) as i32).collect()
    };
    let lengths = Int32Array::new(values.into(), list.nulls().cloned());
    Ok(rewrite_nulls_to_minus_one(lengths))
}

/// Per-row checked Int32 conversion used when the overall offset span exceeds
/// `i32::MAX` (so an individual row might overflow even though most do not).
///
/// Null rows skip `try_from`: Arrow does not require a null row's offset delta to
/// be zero, and the caller rewrites those slots to `-1` anyway (same as the
/// unchecked fast path).
fn spark_size_large_list_lengths_checked(
    offsets: &arrow::buffer::OffsetBuffer<i64>,
    nulls: Option<&NullBuffer>,
) -> Result<Vec<i32>, DataFusionError> {
    let mut values = Vec::with_capacity(offsets.len() - 1);
    for (i, w) in offsets.windows(2).enumerate() {
        if nulls.is_some_and(|n| n.is_null(i)) {
            values.push(0); // overwritten to -1 by the caller
            continue;
        }
        let len = i32::try_from(w[1] - w[0])
            .map_err(|_| DataFusionError::Execution(SIZE_OVERFLOW_MSG.to_string()))?;
        values.push(len);
    }
    Ok(values)
}

/// Compute Spark `size()` for `MapArray` by differencing the entry-offset
/// buffer. `MapArray::offsets()` returns the sliced offsets, so `windows(2)`
/// stays correct on sliced inputs. Applies the same null → `-1` rewrite as the
/// list path.
fn spark_size_map(array: &ArrayRef) -> Result<ArrayRef, DataFusionError> {
    let map_array = array
        .as_any()
        .downcast_ref::<arrow::array::MapArray>()
        .ok_or_else(|| DataFusionError::Internal("Expected MapArray".to_string()))?;

    let offsets = map_array.offsets();
    let values: Vec<i32> = offsets.windows(2).map(|w| w[1] - w[0]).collect();
    let lengths = Int32Array::new(values.into(), map_array.nulls().cloned());
    Ok(rewrite_nulls_to_minus_one(lengths))
}

/// Rewrite null slots to `-1` (Spark size-of-null semantics) and drop the null
/// buffer. Returns the input untouched when there are no nulls.
///
/// Patches the values buffer in place rather than using `zip`; `zip` goes
/// through `MutableArrayData` and roughly doubled runtime on the `array_size`
/// criterion shapes. `set_indices()` on the inverted validity visits only null
/// slots (O(null_count)); `into_parts` avoids an extra values-buffer clone
/// beyond the `to_vec()` copy needed to write into the null slots.
fn rewrite_nulls_to_minus_one(lengths: Int32Array) -> ArrayRef {
    if lengths.null_count() == 0 {
        return Arc::new(lengths);
    }
    let (_, values, nulls) = lengths.into_parts();
    let Some(nulls) = nulls else {
        return Arc::new(Int32Array::new(values, None));
    };
    let mut values = values.to_vec();
    for i in (!nulls.inner()).set_indices() {
        values[i] = -1;
    }
    Arc::new(Int32Array::from(values))
}

fn spark_size_scalar(scalar: &ScalarValue) -> Result<ScalarValue, DataFusionError> {
    match scalar {
        // ScalarValue::{List,LargeList,FixedSizeList,Map} each wrap an array with
        // exactly one row; read the row's element count from the offset buffer
        // (matches the array path, avoids `value(0)` slicing).
        ScalarValue::List(array) => {
            if array.is_null(0) {
                Ok(ScalarValue::Int32(Some(-1))) // Spark behavior: return -1 for null
            } else {
                Ok(ScalarValue::Int32(Some(array.value_length(0))))
            }
        }
        ScalarValue::LargeList(array) => {
            if array.is_null(0) {
                Ok(ScalarValue::Int32(Some(-1)))
            } else {
                // Spark arrays are capped near Integer.MAX_VALUE; overflow shouldn't
                // happen in practice but must error rather than silently wrap.
                let len = i32::try_from(array.value_length(0))
                    .map_err(|_| DataFusionError::Execution(SIZE_OVERFLOW_MSG.to_string()))?;
                Ok(ScalarValue::Int32(Some(len)))
            }
        }
        ScalarValue::FixedSizeList(array) => {
            if array.is_null(0) {
                Ok(ScalarValue::Int32(Some(-1)))
            } else {
                Ok(ScalarValue::Int32(Some(array.value_length())))
            }
        }
        ScalarValue::Map(array) => {
            if array.is_null(0) {
                Ok(ScalarValue::Int32(Some(-1)))
            } else {
                Ok(ScalarValue::Int32(Some(array.value_length(0))))
            }
        }
        ScalarValue::Null => {
            Ok(ScalarValue::Int32(Some(-1))) // Spark behavior: return -1 for null
        }
        _ => {
            exec_err!(
                "size function only supports arrays and maps, got: {:?}",
                scalar
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, ListArray, NullBufferBuilder};
    use arrow::buffer::NullBuffer;
    use arrow::datatypes::{DataType, Field};
    use std::sync::Arc;

    #[test]
    fn test_spark_size_array() {
        // Create test data: [[1, 2, 3], [4, 5], null, []]
        let value_data = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let value_offsets = arrow::buffer::OffsetBuffer::new(vec![0, 3, 5, 5, 5].into());
        let field = Arc::new(Field::new("item", DataType::Int32, true));

        let mut null_buffer = NullBufferBuilder::new(4);
        null_buffer.append(true); // [1, 2, 3] - not null
        null_buffer.append(true); // [4, 5] - not null
        null_buffer.append(false); // null
        null_buffer.append(true); // [] - not null but empty

        let list_array = ListArray::try_new(
            field,
            value_offsets,
            Arc::new(value_data),
            null_buffer.finish(),
        )
        .unwrap();

        let array_ref: ArrayRef = Arc::new(list_array);
        let result = spark_size_array(&array_ref).unwrap();
        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();

        // Expected: [3, 2, -1, 0]
        assert_eq!(result.value(0), 3); // [1, 2, 3] has 3 elements
        assert_eq!(result.value(1), 2); // [4, 5] has 2 elements
        assert_eq!(result.value(2), -1); // null returns -1
        assert_eq!(result.value(3), 0); // [] has 0 elements
    }

    #[test]
    fn test_spark_size_array_no_nulls() {
        // Fast path (the shape Comet actually runs after the CASE-WHEN filter):
        // input has no nulls, so spark_size_list_like returns the length kernel
        // output directly without touching the values buffer.
        let value_data = Int32Array::from(vec![1, 2, 3, 4, 5, 6]);
        let value_offsets = arrow::buffer::OffsetBuffer::new(vec![0, 3, 5, 5, 6].into());
        let field = Arc::new(Field::new("item", DataType::Int32, true));
        let list_array =
            ListArray::try_new(field, value_offsets, Arc::new(value_data), None).unwrap();

        let array_ref: ArrayRef = Arc::new(list_array);
        let result = spark_size_array(&array_ref).unwrap();
        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();

        // Expected: [3, 2, 0, 1]; no null buffer on the output.
        assert_eq!(result.null_count(), 0);
        assert_eq!(result.value(0), 3);
        assert_eq!(result.value(1), 2);
        assert_eq!(result.value(2), 0);
        assert_eq!(result.value(3), 1);
    }

    #[test]
    fn test_spark_size_scalar() {
        // Test non-null list with 3 elements
        let values = Int32Array::from(vec![1, 2, 3]);
        let field = Arc::new(Field::new("item", DataType::Int32, true));
        let offsets = arrow::buffer::OffsetBuffer::new(vec![0, 3].into());
        let list_array = ListArray::try_new(field, offsets, Arc::new(values), None).unwrap();
        let scalar = ScalarValue::List(Arc::new(list_array));
        let result = spark_size_scalar(&scalar).unwrap();
        assert_eq!(result, ScalarValue::Int32(Some(3))); // The array [1,2,3] has 3 elements

        // Test empty list
        let empty_values = Int32Array::from(vec![] as Vec<i32>);
        let field = Arc::new(Field::new("item", DataType::Int32, true));
        let offsets = arrow::buffer::OffsetBuffer::new(vec![0, 0].into());
        let empty_list_array =
            ListArray::try_new(field, offsets, Arc::new(empty_values), None).unwrap();
        let scalar = ScalarValue::List(Arc::new(empty_list_array));
        let result = spark_size_scalar(&scalar).unwrap();
        assert_eq!(result, ScalarValue::Int32(Some(0))); // Empty array has 0 elements

        // Test null handling
        let scalar = ScalarValue::Null;
        let result = spark_size_scalar(&scalar).unwrap();
        assert_eq!(result, ScalarValue::Int32(Some(-1)));
    }

    #[test]
    fn test_spark_size_map_array() {
        use arrow::array::{Int32Array, MapArray, StringArray};

        // Create test data: [{"key1": 1, "key2": 2}, {"key3": 3}, {}, null]
        let keys = StringArray::from(vec![Some("key1"), Some("key2"), Some("key3")]);
        let values = Int32Array::from(vec![Some(1), Some(2), Some(3)]);

        // Create entry offsets: [0, 2, 3, 3, 3] representing:
        // - Map 1: entries 0-1 (2 key-value pairs)
        // - Map 2: entry 2 (1 key-value pair)
        // - Map 3: entries 3-2 (0 key-value pairs, empty map)
        // - Map 4: null (handled by null buffer)
        let entry_offsets = arrow::buffer::OffsetBuffer::new(vec![0i32, 2, 3, 3, 3].into());

        let key_field = Arc::new(Field::new("key", DataType::Utf8, false));
        let value_field = Arc::new(Field::new("value", DataType::Int32, true));

        let entries = arrow::array::StructArray::new(
            arrow::datatypes::Fields::from(vec![key_field, value_field]),
            vec![Arc::new(keys), Arc::new(values)],
            None,
        );

        // Create null buffer for the map array (fourth map is null)
        let mut null_buffer = NullBufferBuilder::new(4);
        null_buffer.append(true); // Map with 2 entries - not null
        null_buffer.append(true); // Map with 1 entry - not null
        null_buffer.append(true); // Empty map - not null
        null_buffer.append(false); // null map

        let map_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(arrow::datatypes::Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int32, true),
            ])),
            false,
        ));

        let map_array = MapArray::try_new(
            map_field,
            entry_offsets,
            entries,
            null_buffer.finish(),
            false,
        )
        .unwrap();

        let array_ref: ArrayRef = Arc::new(map_array);
        let result = spark_size_array(&array_ref).unwrap();
        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();

        // Expected: [2, 1, 0, -1]
        assert_eq!(result.value(0), 2); // Map with 2 key-value pairs
        assert_eq!(result.value(1), 1); // Map with 1 key-value pair
        assert_eq!(result.value(2), 0); // empty map has 0 pairs
        assert_eq!(result.value(3), -1); // null map returns -1
    }

    #[test]
    fn test_spark_size_map_no_nulls() {
        use arrow::array::{Int32Array, MapArray, StringArray};

        // Fast path for Map: no null buffer, so rewrite_nulls_to_minus_one returns
        // the offset-diff Int32Array as-is without touching the values buffer.
        let keys = StringArray::from(vec![Some("a"), Some("b"), Some("c"), Some("d")]);
        let values = Int32Array::from(vec![Some(1), Some(2), Some(3), Some(4)]);
        let entry_offsets = arrow::buffer::OffsetBuffer::new(vec![0i32, 2, 3, 3, 4].into());

        let key_field = Arc::new(Field::new("key", DataType::Utf8, false));
        let value_field = Arc::new(Field::new("value", DataType::Int32, true));

        let entries = arrow::array::StructArray::new(
            arrow::datatypes::Fields::from(vec![key_field, value_field]),
            vec![Arc::new(keys), Arc::new(values)],
            None,
        );

        let map_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(arrow::datatypes::Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int32, true),
            ])),
            false,
        ));

        let map_array = MapArray::try_new(map_field, entry_offsets, entries, None, false).unwrap();

        let array_ref: ArrayRef = Arc::new(map_array);
        let result = spark_size_array(&array_ref).unwrap();
        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();

        // No nulls on input; no null buffer on output.
        assert_eq!(result.null_count(), 0);
        assert_eq!(result.value(0), 2);
        assert_eq!(result.value(1), 1);
        assert_eq!(result.value(2), 0);
        assert_eq!(result.value(3), 1);
    }

    #[test]
    fn test_spark_size_sliced_map_array() {
        use arrow::array::{Int32Array, MapArray, StringArray};

        // Slicing pins that `MapArray::offsets()` returns sliced-aware offsets so
        // `windows(2)` differences the correct rows. Same trap as the sliced-List
        // test above.
        //
        // Full map: [{"a":1,"b":2}, {"c":3}, {"d":4,"e":5,"f":6}, null, {"g":7}]
        let keys = StringArray::from(vec![
            Some("a"),
            Some("b"),
            Some("c"),
            Some("d"),
            Some("e"),
            Some("f"),
            Some("g"),
        ]);
        let values = Int32Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
            Some(6),
            Some(7),
        ]);
        let entry_offsets = arrow::buffer::OffsetBuffer::new(vec![0i32, 2, 3, 6, 6, 7].into());

        let key_field = Arc::new(Field::new("key", DataType::Utf8, false));
        let value_field = Arc::new(Field::new("value", DataType::Int32, true));

        let entries = arrow::array::StructArray::new(
            arrow::datatypes::Fields::from(vec![key_field, value_field]),
            vec![Arc::new(keys), Arc::new(values)],
            None,
        );

        let mut null_buffer = NullBufferBuilder::new(5);
        null_buffer.append(true);
        null_buffer.append(true);
        null_buffer.append(true);
        null_buffer.append(false);
        null_buffer.append(true);

        let map_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(arrow::datatypes::Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int32, true),
            ])),
            false,
        ));

        let map_array = MapArray::try_new(
            map_field,
            entry_offsets,
            entries,
            null_buffer.finish(),
            false,
        )
        .unwrap();

        // Skip the first two rows: sliced view is [{d,e,f}, null, {g}].
        let sliced: ArrayRef = Arc::new(map_array.slice(2, 3));
        let result = spark_size_array(&sliced).unwrap();
        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result.value(0), 3);
        assert_eq!(result.value(1), -1);
        assert_eq!(result.value(2), 1);
    }

    #[test]
    fn test_spark_size_scalar_map() {
        use arrow::array::{Int32Array, MapArray, StringArray};

        // Test non-null map with 2 entries
        let keys = StringArray::from(vec![Some("a"), Some("b")]);
        let values = Int32Array::from(vec![Some(1), Some(2)]);
        let entry_offsets = arrow::buffer::OffsetBuffer::new(vec![0i32, 2].into());

        let key_field = Arc::new(Field::new("key", DataType::Utf8, false));
        let value_field = Arc::new(Field::new("value", DataType::Int32, true));

        let entries = arrow::array::StructArray::new(
            arrow::datatypes::Fields::from(vec![key_field, value_field]),
            vec![Arc::new(keys), Arc::new(values)],
            None,
        );

        let map_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(arrow::datatypes::Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int32, true),
            ])),
            false,
        ));

        let map_array = MapArray::try_new(map_field, entry_offsets, entries, None, false).unwrap();
        let scalar = ScalarValue::Map(Arc::new(map_array));
        let result = spark_size_scalar(&scalar).unwrap();
        assert_eq!(result, ScalarValue::Int32(Some(2))); // Map with 2 entries
    }

    #[test]
    fn test_spark_size_scalar_null_map() {
        use arrow::array::{Int32Array, MapArray, StringArray};

        // Test null map - Spark returns -1 for null map input
        let keys = StringArray::from(vec![Some("a")]);
        let values = Int32Array::from(vec![Some(1)]);
        let entry_offsets = arrow::buffer::OffsetBuffer::new(vec![0i32, 1].into());

        let key_field = Arc::new(Field::new("key", DataType::Utf8, false));
        let value_field = Arc::new(Field::new("value", DataType::Int32, true));

        let entries = arrow::array::StructArray::new(
            arrow::datatypes::Fields::from(vec![key_field, value_field]),
            vec![Arc::new(keys), Arc::new(values)],
            None,
        );

        let map_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(arrow::datatypes::Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int32, true),
            ])),
            false,
        ));

        // Mark the single map entry as null
        let mut null_buffer = NullBufferBuilder::new(1);
        null_buffer.append(false); // null map

        let map_array = MapArray::try_new(
            map_field,
            entry_offsets,
            entries,
            null_buffer.finish(),
            false,
        )
        .unwrap();
        let scalar = ScalarValue::Map(Arc::new(map_array));
        let result = spark_size_scalar(&scalar).unwrap();
        assert_eq!(result, ScalarValue::Int32(Some(-1))); // null map returns -1
    }

    #[test]
    fn test_spark_size_fixed_size_list_array() {
        use arrow::array::FixedSizeListArray;

        // Create test data: fixed-size arrays of size 3
        // [[1, 2, 3], [4, 5, 6], null]
        let values = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 0, 0, 0]); // Last 3 values are for the null entry
        let list_size = 3;

        let mut null_buffer = NullBufferBuilder::new(3);
        null_buffer.append(true); // [1, 2, 3] - not null
        null_buffer.append(true); // [4, 5, 6] - not null
        null_buffer.append(false); // null

        let list_field = Arc::new(Field::new("item", DataType::Int32, true));

        let fixed_list_array = FixedSizeListArray::new(
            list_field,
            list_size,
            Arc::new(values),
            null_buffer.finish(),
        );

        let array_ref: ArrayRef = Arc::new(fixed_list_array);
        let result = spark_size_array(&array_ref).unwrap();
        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();

        // Expected: [3, 3, -1]
        assert_eq!(result.value(0), 3); // Fixed-size list always has size 3
        assert_eq!(result.value(1), 3); // Fixed-size list always has size 3
        assert_eq!(result.value(2), -1); // null returns -1
    }

    #[test]
    fn test_spark_size_large_list_array() {
        use arrow::array::LargeListArray;

        // Create test data: [[1, 2, 3, 4], [5], null, []]
        let value_data = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let value_offsets = arrow::buffer::OffsetBuffer::new(vec![0i64, 4, 5, 5, 5].into());
        let field = Arc::new(Field::new("item", DataType::Int32, true));

        let mut null_buffer = NullBufferBuilder::new(4);
        null_buffer.append(true); // [1, 2, 3, 4] - not null
        null_buffer.append(true); // [5] - not null
        null_buffer.append(false); // null
        null_buffer.append(true); // [] - not null but empty

        let large_list_array = LargeListArray::try_new(
            field,
            value_offsets,
            Arc::new(value_data),
            null_buffer.finish(),
        )
        .unwrap();

        let array_ref: ArrayRef = Arc::new(large_list_array);
        let result = spark_size_array(&array_ref).unwrap();
        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();

        // Expected: [4, 1, -1, 0]
        assert_eq!(result.value(0), 4); // [1, 2, 3, 4] has 4 elements
        assert_eq!(result.value(1), 1); // [5] has 1 element
        assert_eq!(result.value(2), -1); // null returns -1
        assert_eq!(result.value(3), 0); // [] has 0 elements
    }

    #[test]
    fn test_spark_size_sliced_list_array() {
        // Slicing is the classic trap for buffer-level ops: the null buffer, values
        // buffer, and offsets all logically start at `array.offset()`, not 0. Pin the
        // behavior so a future edit that touches buffers directly cannot regress it.
        //
        // Full array: [[1, 2], [3], [4, 5, 6], null, [7]]
        let value_data = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7]);
        let value_offsets = arrow::buffer::OffsetBuffer::new(vec![0, 2, 3, 6, 6, 7].into());
        let field = Arc::new(Field::new("item", DataType::Int32, true));

        let mut null_buffer = NullBufferBuilder::new(5);
        null_buffer.append(true);
        null_buffer.append(true);
        null_buffer.append(true);
        null_buffer.append(false);
        null_buffer.append(true);

        let list_array = ListArray::try_new(
            field,
            value_offsets,
            Arc::new(value_data),
            null_buffer.finish(),
        )
        .unwrap();

        // Skip the first two rows: sliced view is [[4, 5, 6], null, [7]].
        let sliced: ArrayRef = Arc::new(list_array.slice(2, 3));
        let result = spark_size_array(&sliced).unwrap();
        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result.value(0), 3);
        assert_eq!(result.value(1), -1);
        assert_eq!(result.value(2), 1);
    }

    #[test]
    fn test_spark_size_sliced_large_list_array() {
        // Same sliced-offsets invariant as the List test, for the LargeList helper
        // that reads `offsets().windows(2)` directly.
        //
        // Full array: [[1, 2], [3], [4, 5, 6], null, [7]]
        use arrow::array::LargeListArray;

        let value_data = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7]);
        let value_offsets = arrow::buffer::OffsetBuffer::new(vec![0i64, 2, 3, 6, 6, 7].into());
        let field = Arc::new(Field::new("item", DataType::Int32, true));

        let mut null_buffer = NullBufferBuilder::new(5);
        null_buffer.append(true);
        null_buffer.append(true);
        null_buffer.append(true);
        null_buffer.append(false);
        null_buffer.append(true);

        let large_list_array = LargeListArray::try_new(
            field,
            value_offsets,
            Arc::new(value_data),
            null_buffer.finish(),
        )
        .unwrap();

        // Skip the first two rows: sliced view is [[4, 5, 6], null, [7]].
        let sliced: ArrayRef = Arc::new(large_list_array.slice(2, 3));
        let result = spark_size_array(&sliced).unwrap();
        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result.value(0), 3);
        assert_eq!(result.value(1), -1);
        assert_eq!(result.value(2), 1);
    }

    #[test]
    fn test_spark_size_large_list_length_overflow() {
        // A non-null row whose length is i32::MAX + 1 must error (same contract
        // as the old safe: false Int64→Int32 cast / scalar try_from). Call the
        // checked helper directly with a valid OffsetBuffer — no invalid ArrayData.
        let offsets = arrow::buffer::OffsetBuffer::new(vec![0i64, (i32::MAX as i64) + 1].into());
        let err = spark_size_large_list_lengths_checked(&offsets, None).unwrap_err();
        assert!(
            err.to_string().contains(SIZE_OVERFLOW_MSG),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_spark_size_large_list_checked_null_row_skips_overflow() {
        // Null row offset delta may exceed i32::MAX; checked path must not error
        // (matches the fast path, which rewrites null slots to -1 afterward).
        let offsets = arrow::buffer::OffsetBuffer::new(
            vec![0i64, (i32::MAX as i64) + 1, (i32::MAX as i64) + 1].into(),
        );
        let nulls = NullBuffer::from(vec![false, true]); // row 0 null, row 1 empty
        let values = spark_size_large_list_lengths_checked(&offsets, Some(&nulls)).unwrap();
        assert_eq!(values, vec![0, 0]);
        let lengths = Int32Array::new(values.into(), Some(nulls));
        let result = rewrite_nulls_to_minus_one(lengths);
        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(result.value(0), -1);
        assert_eq!(result.value(1), 0);
    }

    #[test]
    fn test_spark_size_scalar_large_list() {
        use arrow::array::LargeListArray;

        // Non-null LargeList: [10, 20, 30, 40] → 4
        let values = Int32Array::from(vec![10, 20, 30, 40]);
        let offsets = arrow::buffer::OffsetBuffer::new(vec![0i64, 4].into());
        let field = Arc::new(Field::new("item", DataType::Int32, true));
        let large_list_array =
            LargeListArray::try_new(Arc::clone(&field), offsets, Arc::new(values), None).unwrap();
        let scalar = ScalarValue::LargeList(Arc::new(large_list_array));
        assert_eq!(
            spark_size_scalar(&scalar).unwrap(),
            ScalarValue::Int32(Some(4))
        );

        // Null LargeList row → -1
        let empty_values = Int32Array::from(Vec::<i32>::new());
        let null_offsets = arrow::buffer::OffsetBuffer::new(vec![0i64, 0].into());
        let mut null_buffer = NullBufferBuilder::new(1);
        null_buffer.append(false);
        let null_large_list = LargeListArray::try_new(
            field,
            null_offsets,
            Arc::new(empty_values),
            null_buffer.finish(),
        )
        .unwrap();
        let scalar = ScalarValue::LargeList(Arc::new(null_large_list));
        assert_eq!(
            spark_size_scalar(&scalar).unwrap(),
            ScalarValue::Int32(Some(-1))
        );
    }

    #[test]
    fn test_spark_size_scalar_fixed_size_list() {
        use arrow::array::FixedSizeListArray;

        // Non-null FixedSizeList of size 4 → 4. `value_length()` on FSL takes no index,
        // unlike `value_length(i)` on List/LargeList — pin the correct API is called.
        let values = Int32Array::from(vec![10, 20, 30, 40]);
        let field = Arc::new(Field::new("item", DataType::Int32, true));
        let fsl_array = FixedSizeListArray::new(Arc::clone(&field), 4, Arc::new(values), None);
        let scalar = ScalarValue::FixedSizeList(Arc::new(fsl_array));
        assert_eq!(
            spark_size_scalar(&scalar).unwrap(),
            ScalarValue::Int32(Some(4))
        );

        // Null FSL row → -1
        let null_values = Int32Array::from(vec![0, 0, 0, 0]);
        let mut null_buffer = NullBufferBuilder::new(1);
        null_buffer.append(false);
        let null_fsl_array =
            FixedSizeListArray::new(field, 4, Arc::new(null_values), null_buffer.finish());
        let scalar = ScalarValue::FixedSizeList(Arc::new(null_fsl_array));
        assert_eq!(
            spark_size_scalar(&scalar).unwrap(),
            ScalarValue::Int32(Some(-1))
        );
    }
}
