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

//! This includes utilities for hashing and murmur3 hashing.

use arrow::array::Array;

#[macro_export]
macro_rules! hash_array {
    ($array_type: ident, $column: ident, $hashes: ident, $hash_method: ident) => {
        let array = $column
            .as_any()
            .downcast_ref::<$array_type>()
            .unwrap_or_else(|| {
                panic!(
                    "Failed to downcast column to {}. Actual data type: {:?}.",
                    stringify!($array_type),
                    $column.data_type()
                )
            });
        if array.null_count() == 0 {
            // Fast path: no nulls, use direct indexing
            for i in 0..$hashes.len() {
                $hashes[i] = $hash_method(&array.value(i), $hashes[i]);
            }
        } else {
            // Slow path: check nulls
            for i in 0..$hashes.len() {
                if !array.is_null(i) {
                    $hashes[i] = $hash_method(&array.value(i), $hashes[i]);
                }
            }
        }
    };
}

#[macro_export]
macro_rules! hash_array_boolean {
    ($array_type: ident, $column: ident, $hash_input_type: ident, $hashes: ident, $hash_method: ident) => {
        let array = $column
            .as_any()
            .downcast_ref::<$array_type>()
            .unwrap_or_else(|| {
                panic!(
                    "Failed to downcast column to {}. Actual data type: {:?}.",
                    stringify!($array_type),
                    $column.data_type()
                )
            });
        if array.null_count() == 0 {
            // Fast path: no nulls, use direct indexing
            for i in 0..$hashes.len() {
                $hashes[i] = $hash_method(
                    $hash_input_type::from(array.value(i)).to_le_bytes(),
                    $hashes[i],
                );
            }
        } else {
            // Slow path: check nulls
            for i in 0..$hashes.len() {
                if !array.is_null(i) {
                    $hashes[i] = $hash_method(
                        $hash_input_type::from(array.value(i)).to_le_bytes(),
                        $hashes[i],
                    );
                }
            }
        }
    };
}

#[macro_export]
macro_rules! hash_array_primitive {
    ($array_type: ident, $column: ident, $ty: ident, $hashes: ident, $hash_method: ident) => {
        let array = $column
            .as_any()
            .downcast_ref::<$array_type>()
            .unwrap_or_else(|| {
                panic!(
                    "Failed to downcast column to {}. Actual data type: {:?}.",
                    stringify!($array_type),
                    $column.data_type()
                )
            });
        let values = array.values();

        if array.null_count() == 0 {
            // Fast path: no nulls, use direct indexing
            for i in 0..values.len() {
                $hashes[i] = $hash_method((values[i] as $ty).to_le_bytes(), $hashes[i]);
            }
        } else {
            // Slow path: check nulls
            for i in 0..values.len() {
                if !array.is_null(i) {
                    $hashes[i] = $hash_method((values[i] as $ty).to_le_bytes(), $hashes[i]);
                }
            }
        }
    };
}

#[macro_export]
macro_rules! hash_array_primitive_float {
    ($array_type: ident, $column: ident, $ty: ident, $ty2: ident, $hashes: ident, $hash_method: ident) => {
        let array = $column
            .as_any()
            .downcast_ref::<$array_type>()
            .unwrap_or_else(|| {
                panic!(
                    "Failed to downcast column to {}. Actual data type: {:?}.",
                    stringify!($array_type),
                    $column.data_type()
                )
            });
        let values = array.values();

        if array.null_count() == 0 {
            // Fast path: no nulls, use direct indexing
            for i in 0..values.len() {
                let value = values[i];
                // Spark uses 0 as hash for -0.0, see `Murmur3Hash` expression.
                if value == 0.0 && value.is_sign_negative() {
                    $hashes[i] = $hash_method((0 as $ty2).to_le_bytes(), $hashes[i]);
                } else {
                    $hashes[i] = $hash_method((value as $ty).to_le_bytes(), $hashes[i]);
                }
            }
        } else {
            // Slow path: check nulls
            for i in 0..values.len() {
                if !array.is_null(i) {
                    let value = values[i];
                    // Spark uses 0 as hash for -0.0, see `Murmur3Hash` expression.
                    if value == 0.0 && value.is_sign_negative() {
                        $hashes[i] = $hash_method((0 as $ty2).to_le_bytes(), $hashes[i]);
                    } else {
                        $hashes[i] = $hash_method((value as $ty).to_le_bytes(), $hashes[i]);
                    }
                }
            }
        }
    };
}

#[macro_export]
macro_rules! hash_array_small_decimal {
    ($array_type:ident, $column: ident, $hashes: ident, $hash_method: ident) => {
        let array = $column
            .as_any()
            .downcast_ref::<$array_type>()
            .unwrap_or_else(|| {
                panic!(
                    "Failed to downcast column to {}. Actual data type: {:?}.",
                    stringify!($array_type),
                    $column.data_type()
                )
            });

        if array.null_count() == 0 {
            // Fast path: no nulls, use direct indexing
            for i in 0..$hashes.len() {
                $hashes[i] = $hash_method(
                    i64::try_from(array.value(i))
                        .map(|v| v.to_le_bytes())
                        .map_err(|e| DataFusionError::Execution(e.to_string()))?,
                    $hashes[i],
                );
            }
        } else {
            // Slow path: check nulls
            for i in 0..$hashes.len() {
                if !array.is_null(i) {
                    $hashes[i] = $hash_method(
                        i64::try_from(array.value(i))
                            .map(|v| v.to_le_bytes())
                            .map_err(|e| DataFusionError::Execution(e.to_string()))?,
                        $hashes[i],
                    );
                }
            }
        }
    };
}

#[macro_export]
macro_rules! hash_array_decimal {
    ($array_type:ident, $column: ident, $hashes: ident, $hash_method: ident) => {
        let array = $column
            .as_any()
            .downcast_ref::<$array_type>()
            .unwrap_or_else(|| {
                panic!(
                    "Failed to downcast column to {}. Actual data type: {:?}.",
                    stringify!($array_type),
                    $column.data_type()
                )
            });

        if array.null_count() == 0 {
            // Fast path: no nulls, use direct indexing
            for i in 0..$hashes.len() {
                $hashes[i] = $hash_method(array.value(i).to_le_bytes(), $hashes[i]);
            }
        } else {
            // Slow path: check nulls
            for i in 0..$hashes.len() {
                if !array.is_null(i) {
                    $hashes[i] = $hash_method(array.value(i).to_le_bytes(), $hashes[i]);
                }
            }
        }
    };
}

/// Hash a list array with primitive elements by directly accessing the underlying buffer.
/// This avoids the overhead of slicing and recursive calls for common cases.
/// Supports both variable-length lists (with offsets) and fixed-size lists.
#[macro_export]
macro_rules! hash_list_primitive {
    // Variable-length list variant (List/LargeList)
    (offsets: $offsets:expr, $list_array:ident, $elem_array:ident, $hashes:ident, $hash_method:ident, $value_transform:expr) => {
        if $list_array.null_count() == 0 && $elem_array.null_count() == 0 {
            for (row_idx, hash) in $hashes.iter_mut().enumerate() {
                let start = $offsets[row_idx] as usize;
                let end = $offsets[row_idx + 1] as usize;
                for elem_idx in start..end {
                    let value = $elem_array.value(elem_idx);
                    *hash = $hash_method($value_transform(value), *hash);
                }
            }
        } else {
            for (row_idx, hash) in $hashes.iter_mut().enumerate() {
                if !$list_array.is_null(row_idx) {
                    let start = $offsets[row_idx] as usize;
                    let end = $offsets[row_idx + 1] as usize;
                    for elem_idx in start..end {
                        if !$elem_array.is_null(elem_idx) {
                            let value = $elem_array.value(elem_idx);
                            *hash = $hash_method($value_transform(value), *hash);
                        }
                    }
                }
            }
        }
    };
    // Fixed-size list variant
    (fixed_size: $list_size:expr, $list_array:ident, $elem_array:ident, $hashes:ident, $hash_method:ident, $value_transform:expr) => {
        if $list_array.null_count() == 0 && $elem_array.null_count() == 0 {
            for (row_idx, hash) in $hashes.iter_mut().enumerate() {
                let start = row_idx * $list_size;
                for elem_idx in 0..$list_size {
                    let value = $elem_array.value(start + elem_idx);
                    *hash = $hash_method($value_transform(value), *hash);
                }
            }
        } else {
            for (row_idx, hash) in $hashes.iter_mut().enumerate() {
                if !$list_array.is_null(row_idx) {
                    let start = row_idx * $list_size;
                    for elem_idx in 0..$list_size {
                        if !$elem_array.is_null(start + elem_idx) {
                            let value = $elem_array.value(start + elem_idx);
                            *hash = $hash_method($value_transform(value), *hash);
                        }
                    }
                }
            }
        }
    };
}

/// Hash a list array by recursively hashing each element.
/// For each row, we hash all elements in the list.
/// Spark hashes arrays by recursively hashing each element, where each
/// element's hash is computed using the previous element's hash as the seed.
/// This creates a chain: hash(elem_n, hash(elem_n-1, ... hash(elem_0, seed)...))
/// Dispatches hash operations for List/LargeList/FixedSizeList arrays with primitive element types.
/// This macro eliminates duplication by handling the type-to-array mapping for all supported primitives.
#[macro_export]
macro_rules! hash_list_with_primitive_elements {
    // Variant for List/LargeList with offsets
    (offsets: $list_array_type:ident, $list_array:ident, $values:ident, $offsets:ident, $field:expr, $hashes_buffer:ident, $hash_method:ident, $recursive_hash_method:ident, $fallback_offset_type:ty, $col:ident) => {
        match $field.data_type() {
            DataType::Int8 => {
                let elem_array = $values.as_any().downcast_ref::<Int8Array>().unwrap();
                $crate::hash_list_primitive!(offsets: $offsets, $list_array, elem_array, $hashes_buffer, $hash_method, |v: i8| (v as i32).to_le_bytes());
            }
            DataType::Int16 => {
                let elem_array = $values.as_any().downcast_ref::<Int16Array>().unwrap();
                $crate::hash_list_primitive!(offsets: $offsets, $list_array, elem_array, $hashes_buffer, $hash_method, |v: i16| (v as i32).to_le_bytes());
            }
            DataType::Int32 => {
                let elem_array = $values.as_any().downcast_ref::<Int32Array>().unwrap();
                $crate::hash_list_primitive!(offsets: $offsets, $list_array, elem_array, $hashes_buffer, $hash_method, |v: i32| v.to_le_bytes());
            }
            DataType::Int64 => {
                let elem_array = $values.as_any().downcast_ref::<Int64Array>().unwrap();
                $crate::hash_list_primitive!(offsets: $offsets, $list_array, elem_array, $hashes_buffer, $hash_method, |v: i64| v.to_le_bytes());
            }
            DataType::Float32 => {
                let elem_array = $values.as_any().downcast_ref::<Float32Array>().unwrap();
                $crate::hash_list_primitive!(offsets: $offsets, $list_array, elem_array, $hashes_buffer, $hash_method,
                    |v: f32| if v == 0.0 && v.is_sign_negative() { (0_i32).to_le_bytes() } else { v.to_le_bytes() });
            }
            DataType::Float64 => {
                let elem_array = $values.as_any().downcast_ref::<Float64Array>().unwrap();
                $crate::hash_list_primitive!(offsets: $offsets, $list_array, elem_array, $hashes_buffer, $hash_method,
                    |v: f64| if v == 0.0 && v.is_sign_negative() { (0_i64).to_le_bytes() } else { v.to_le_bytes() });
            }
            DataType::Boolean => {
                let elem_array = $values.as_any().downcast_ref::<BooleanArray>().unwrap();
                $crate::hash_list_primitive!(offsets: $offsets, $list_array, elem_array, $hashes_buffer, $hash_method, |v: bool| (i32::from(v)).to_le_bytes());
            }
            DataType::Utf8 => {
                let elem_array = $values.as_any().downcast_ref::<StringArray>().unwrap();
                if $list_array.null_count() == 0 && elem_array.null_count() == 0 {
                    for (row_idx, hash) in $hashes_buffer.iter_mut().enumerate() {
                        let start = $offsets[row_idx] as usize;
                        let end = $offsets[row_idx + 1] as usize;
                        for elem_idx in start..end {
                            *hash = $hash_method(elem_array.value(elem_idx), *hash);
                        }
                    }
                } else {
                    for (row_idx, hash) in $hashes_buffer.iter_mut().enumerate() {
                        if !$list_array.is_null(row_idx) {
                            let start = $offsets[row_idx] as usize;
                            let end = $offsets[row_idx + 1] as usize;
                            for elem_idx in start..end {
                                if !elem_array.is_null(elem_idx) {
                                    *hash = $hash_method(elem_array.value(elem_idx), *hash);
                                }
                            }
                        }
                    }
                }
            }
            DataType::Binary => {
                let elem_array = $values.as_any().downcast_ref::<BinaryArray>().unwrap();
                if $list_array.null_count() == 0 && elem_array.null_count() == 0 {
                    for (row_idx, hash) in $hashes_buffer.iter_mut().enumerate() {
                        let start = $offsets[row_idx] as usize;
                        let end = $offsets[row_idx + 1] as usize;
                        for elem_idx in start..end {
                            *hash = $hash_method(elem_array.value(elem_idx), *hash);
                        }
                    }
                } else {
                    for (row_idx, hash) in $hashes_buffer.iter_mut().enumerate() {
                        if !$list_array.is_null(row_idx) {
                            let start = $offsets[row_idx] as usize;
                            let end = $offsets[row_idx + 1] as usize;
                            for elem_idx in start..end {
                                if !elem_array.is_null(elem_idx) {
                                    *hash = $hash_method(elem_array.value(elem_idx), *hash);
                                }
                            }
                        }
                    }
                }
            }
            DataType::Date32 => {
                let elem_array = $values.as_any().downcast_ref::<Date32Array>().unwrap();
                $crate::hash_list_primitive!(offsets: $offsets, $list_array, elem_array, $hashes_buffer, $hash_method, |v: i32| v.to_le_bytes());
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                let elem_array = $values.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
                $crate::hash_list_primitive!(offsets: $offsets, $list_array, elem_array, $hashes_buffer, $hash_method, |v: i64| v.to_le_bytes());
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                let elem_array = $values.as_any().downcast_ref::<Time64NanosecondArray>().unwrap();
                $crate::hash_list_primitive!(offsets: $offsets, $list_array, elem_array, $hashes_buffer, $hash_method, |v: i64| v.to_le_bytes());
            }
            _ => {
                // Fall back to recursive approach for complex element types
                $crate::hash_list_array!($list_array_type, $fallback_offset_type, $col, $hashes_buffer, $recursive_hash_method);
            }
        }
    };
    // Variant for FixedSizeList with fixed size
    (fixed_size: $list_array:ident, $values:ident, $list_size:ident, $field:expr, $hashes_buffer:ident, $hash_method:ident, $recursive_hash_method:ident) => {
        match $field.data_type() {
            DataType::Int8 => {
                let elem_array = $values.as_any().downcast_ref::<Int8Array>().unwrap();
                $crate::hash_list_primitive!(fixed_size: $list_size, $list_array, elem_array, $hashes_buffer, $hash_method, |v: i8| (v as i32).to_le_bytes());
            }
            DataType::Int16 => {
                let elem_array = $values.as_any().downcast_ref::<Int16Array>().unwrap();
                $crate::hash_list_primitive!(fixed_size: $list_size, $list_array, elem_array, $hashes_buffer, $hash_method, |v: i16| (v as i32).to_le_bytes());
            }
            DataType::Int32 => {
                let elem_array = $values.as_any().downcast_ref::<Int32Array>().unwrap();
                $crate::hash_list_primitive!(fixed_size: $list_size, $list_array, elem_array, $hashes_buffer, $hash_method, |v: i32| v.to_le_bytes());
            }
            DataType::Int64 => {
                let elem_array = $values.as_any().downcast_ref::<Int64Array>().unwrap();
                $crate::hash_list_primitive!(fixed_size: $list_size, $list_array, elem_array, $hashes_buffer, $hash_method, |v: i64| v.to_le_bytes());
            }
            DataType::Float32 => {
                let elem_array = $values.as_any().downcast_ref::<Float32Array>().unwrap();
                $crate::hash_list_primitive!(fixed_size: $list_size, $list_array, elem_array, $hashes_buffer, $hash_method,
                    |v: f32| if v == 0.0 && v.is_sign_negative() { (0_i32).to_le_bytes() } else { v.to_le_bytes() });
            }
            DataType::Float64 => {
                let elem_array = $values.as_any().downcast_ref::<Float64Array>().unwrap();
                $crate::hash_list_primitive!(fixed_size: $list_size, $list_array, elem_array, $hashes_buffer, $hash_method,
                    |v: f64| if v == 0.0 && v.is_sign_negative() { (0_i64).to_le_bytes() } else { v.to_le_bytes() });
            }
            DataType::Boolean => {
                let elem_array = $values.as_any().downcast_ref::<BooleanArray>().unwrap();
                $crate::hash_list_primitive!(fixed_size: $list_size, $list_array, elem_array, $hashes_buffer, $hash_method, |v: bool| (i32::from(v)).to_le_bytes());
            }
            DataType::Utf8 => {
                let elem_array = $values.as_any().downcast_ref::<StringArray>().unwrap();
                if $list_array.null_count() == 0 && elem_array.null_count() == 0 {
                    for (row_idx, hash) in $hashes_buffer.iter_mut().enumerate() {
                        let start = row_idx * $list_size;
                        for elem_idx in 0..$list_size {
                            *hash = $hash_method(elem_array.value(start + elem_idx), *hash);
                        }
                    }
                } else {
                    for (row_idx, hash) in $hashes_buffer.iter_mut().enumerate() {
                        if !$list_array.is_null(row_idx) {
                            let start = row_idx * $list_size;
                            for elem_idx in 0..$list_size {
                                if !elem_array.is_null(start + elem_idx) {
                                    *hash = $hash_method(elem_array.value(start + elem_idx), *hash);
                                }
                            }
                        }
                    }
                }
            }
            DataType::Binary => {
                let elem_array = $values.as_any().downcast_ref::<BinaryArray>().unwrap();
                if $list_array.null_count() == 0 && elem_array.null_count() == 0 {
                    for (row_idx, hash) in $hashes_buffer.iter_mut().enumerate() {
                        let start = row_idx * $list_size;
                        for elem_idx in 0..$list_size {
                            *hash = $hash_method(elem_array.value(start + elem_idx), *hash);
                        }
                    }
                } else {
                    for (row_idx, hash) in $hashes_buffer.iter_mut().enumerate() {
                        if !$list_array.is_null(row_idx) {
                            let start = row_idx * $list_size;
                            for elem_idx in 0..$list_size {
                                if !elem_array.is_null(start + elem_idx) {
                                    *hash = $hash_method(elem_array.value(start + elem_idx), *hash);
                                }
                            }
                        }
                    }
                }
            }
            DataType::Date32 => {
                let elem_array = $values.as_any().downcast_ref::<Date32Array>().unwrap();
                $crate::hash_list_primitive!(fixed_size: $list_size, $list_array, elem_array, $hashes_buffer, $hash_method, |v: i32| v.to_le_bytes());
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                let elem_array = $values.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
                $crate::hash_list_primitive!(fixed_size: $list_size, $list_array, elem_array, $hashes_buffer, $hash_method, |v: i64| v.to_le_bytes());
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                let elem_array = $values.as_any().downcast_ref::<Time64NanosecondArray>().unwrap();
                $crate::hash_list_primitive!(fixed_size: $list_size, $list_array, elem_array, $hashes_buffer, $hash_method, |v: i64| v.to_le_bytes());
            }
            _ => {
                // Fall back to recursive approach for complex element types
                if $list_array.null_count() == 0 {
                    for (row_idx, hash) in $hashes_buffer.iter_mut().enumerate() {
                        let start = row_idx * $list_size;
                        for elem_idx in 0..$list_size {
                            let elem_array = $values.slice(start + elem_idx, 1);
                            let mut single_hash = [*hash];
                            $recursive_hash_method(&[elem_array], &mut single_hash)?;
                            *hash = single_hash[0];
                        }
                    }
                } else {
                    for (row_idx, hash) in $hashes_buffer.iter_mut().enumerate() {
                        if !$list_array.is_null(row_idx) {
                            let start = row_idx * $list_size;
                            for elem_idx in 0..$list_size {
                                let elem_array = $values.slice(start + elem_idx, 1);
                                let mut single_hash = [*hash];
                                $recursive_hash_method(&[elem_array], &mut single_hash)?;
                                *hash = single_hash[0];
                            }
                        }
                    }
                }
            }
        }
    };
}

/// Whether the batched gather is used for a list whose elements are `values`.
///
/// Batching replaces a per-element slice and dispatch with one `arrow::compute::take` per element
/// position. That is a large win for a small flat struct, but `take` copies the selected payload,
/// and how much it copies is not something this kernel can predict cheaply:
///
/// - A width-based estimate is diluted by short elements. Two rows of 1024 structs where only the
///   first string is 8 MiB average out to about 16 KB per element while the gather copies 16 MiB.
/// - A sliced list keeps its child's buffers. Slicing away the one row that held ten million ints
///   leaves two cheap visible rows and a child that `take` still pre-sizes from, turning a 128-byte
///   peak into 40 MB.
/// - A dictionary child is shared rather than copied, so charging for its payload abandons batching
///   on a shape that copies nothing.
/// - A nested child recurses, and each level's gather stays live while the level below builds its
///   own, so cost accumulates down the depth.
///
/// Rather than model all of that, this admits only the shape whose cost is easy to bound -- a struct
/// of flat leaves -- and requires the child's *retained* buffers to fit a conservative limit, not an
/// average per element, so a large buffer kept alive by a slice disqualifies the gather even when
/// few rows are visible. Everything else keeps the previous per-element path.
///
/// The limit is an eligibility condition for the optimization, not a bound on the peak memory of a
/// hash call: the index array, the row mapping and Arrow's own metadata are extra.
pub fn gather_is_eligible(values: &dyn Array) -> bool {
    use arrow::datatypes::DataType;

    let DataType::Struct(fields) = values.data_type() else {
        return false;
    };
    if !fields.iter().all(|f| is_flat_leaf(f.data_type())) {
        return false;
    }
    // Retained size, not per element: a slice that hides a huge child must not qualify.
    values.get_array_memory_size() <= GATHER_ELIGIBLE_CHILD_BYTES
}

/// Leaf types whose gather cost is proportional to the rows picked, with no shared or nested
/// payload behind them. Deliberately conservative: a type absent here keeps the previous path, and
/// adding one means measuring it.
fn is_flat_leaf(data_type: &arrow::datatypes::DataType) -> bool {
    use arrow::datatypes::DataType;

    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Date32
            | DataType::Date64
            | DataType::Timestamp(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Utf8
            | DataType::Binary
    )
}

/// Ceiling on a gathered child's retained buffers for the batched path to be used.
///
/// Compared against the whole child rather than a per-pass estimate, which is what makes a large
/// buffer held alive by a slice fail the check. It is not a per-pass bound: when every row holds one
/// element, a single pass gathers nearly the whole child, so a shape just under the limit can gather
/// close to it in one pass.
pub const GATHER_ELIGIBLE_CHILD_BYTES: usize = 4 * 1024 * 1024;

#[macro_export]
macro_rules! hash_list_array {
    ($array_type:ident, $offset_type:ty, $column: ident, $hashes: ident, $recursive_hash_method: ident) => {
        let list_array = $column
            .as_any()
            .downcast_ref::<$array_type>()
            .unwrap_or_else(|| {
                panic!(
                    "Failed to downcast column to {}. Actual data type: {:?}.",
                    stringify!($array_type),
                    $column.data_type()
                )
            });

        let values = list_array.values();
        let offsets = list_array.offsets();

        // Spark chains the element hashes in order, so the elements of one row have to be hashed
        // in sequence. What does not have to happen per element is the allocation and dispatch:
        // slicing a one-element array and re-entering the hash dispatch for it costs an Arrow
        // array plus a full type match every time, and for a struct element the dispatch also
        // copies the field vector on every call.
        //
        // Instead, hash one element per row at a time in a single batched call, seeding each
        // slot with the running hash of the row it belongs to. That is exactly what the
        // per-element call did, so the result is bit-identical.
        let total_elements = offsets[$hashes.len()] as usize - offsets[0] as usize;
        if total_elements == 0 {
            // Every list is empty or null; the seeds already hold the answer.
        } else {
            // Decide before any scheduling work. Eligibility depends only on the element type and
            // the size of the child's retained buffers, not on which rows are active, so a shape
            // that keeps the previous path allocates nothing new at all: no survivor scan, no
            // sliced element view, no per-pass buffers.
            // Batching needs several rows to batch across. One row, whether the whole column is one
            // row or only one row is non-empty, has nothing to gather with, so take the per-element
            // path before allocating a sliced element view, a survivor list or any per-pass buffer.
            // Recursion makes this common rather than a corner case: a deep singleton list reaches a
            // one-row batch at every level below the first.
            let mut non_empty_rows = 0usize;
            for row_idx in 0..$hashes.len() {
                if !list_array.is_null(row_idx)
                    && offsets[row_idx + 1] > offsets[row_idx]
                {
                    non_empty_rows += 1;
                    if non_empty_rows > 1 {
                        break;
                    }
                }
            }
            if non_empty_rows <= 1
                || !$crate::hash_funcs::utils::gather_is_eligible(values.as_ref())
            {
                for row_idx in 0..$hashes.len() {
                    if list_array.is_null(row_idx) {
                        continue;
                    }
                    let start = offsets[row_idx] as usize;
                    let end = offsets[row_idx + 1] as usize;
                    for elem_idx in start..end {
                        let elem = values.slice(elem_idx, 1);
                        let mut single = [$hashes[row_idx]];
                        $recursive_hash_method(&[elem], &mut single)?;
                        $hashes[row_idx] = single[0];
                    }
                }
            } else {
            let first_offset = offsets[0] as usize;
            let elements = values.slice(first_offset, total_elements);

            // Chaining means element k of a row can only be hashed once element k-1 is known, so
            // batch by position: all the first elements together, then all the second, and so on.
            // Rows are independent, so one pass per position is enough.
            //
            // Only rows that still have an element at the current position take part, and a row
            // never becomes alive again once exhausted, so carry the surviving rows forward instead
            // of rescanning all of them each pass. Rescanning would cost rows x longest-list, which
            // for one long list among short ones is almost all wasted: 8192 rows with one list of
            // 1024 scans 8.4M slots for 9215 elements. Carrying the survivors makes the scheduling
            // work proportional to the elements actually hashed.
            //
            // Index the gather by the list's own offset width. A `LargeList` can hold more than
            // `u32::MAX` elements, so narrowing the positions to `u32` would silently wrap and
            // hash the wrong elements.
            let mut active: Vec<usize> = Vec::with_capacity($hashes.len());
            // The same pass records whether every row is non-null with the same length. When it
            // is, no row ever drops out early, so the survivor bookkeeping is pure overhead and
            // the rows can simply be walked directly.
            let mut uniform_len: Option<usize> = None;
            let mut all_same = true;
            for row_idx in 0..$hashes.len() {
                if list_array.is_null(row_idx) {
                    all_same = false;
                    continue;
                }
                let len = offsets[row_idx + 1] as usize - offsets[row_idx] as usize;
                if len > 0 {
                    active.push(row_idx);
                }
                match uniform_len {
                    None => uniform_len = Some(len),
                    Some(seen) if seen == len => {}
                    Some(_) => all_same = false,
                }
            }
            let uniform = all_same && uniform_len.unwrap_or(0) > 0;

            // Only a batch that will actually gather needs this decision, and a single row always
            // takes the direct path below, so skip the check for one row. Decided once for the
            // column, never per pass.

            // Allocated only for the batched path, after the decision above.
            let mut positions: Vec<$offset_type> = Vec::with_capacity(active.len());
            let mut rows_at_position: Vec<usize> = Vec::with_capacity(active.len());
            let mut still_active: Vec<usize> = Vec::with_capacity(active.len());
            let mut position_hashes = Vec::with_capacity(active.len());
            let mut position = 0usize;
            let uniform_passes = if uniform { uniform_len.unwrap_or(0) } else { 0 };
            while (uniform && position < uniform_passes) || (!uniform && !active.is_empty()) {
                // Batching pays only when a pass covers several rows. Once one row is left there is
                // nothing to gather across: `take` would copy that row's remaining element payloads
                // without saving a dispatch. That happens both for a batch that starts with a
                // single non-empty row and, more often, for the tail after the shorter rows finish
                // -- lengths [1, 1, 8] spend seven of eight passes on one row. Finish it by slicing,
                // the way the previous implementation did throughout.
                if active.len() == 1 {
                    let row_idx = active[0];
                    let start = offsets[row_idx] as usize;
                    let end = offsets[row_idx + 1] as usize;
                    for elem_idx in (start + position)..end {
                        let elem = values.slice(elem_idx, 1);
                        let mut single = [$hashes[row_idx]];
                        $recursive_hash_method(&[elem], &mut single)?;
                        $hashes[row_idx] = single[0];
                    }
                    // `break`, not `return`: this macro runs inside the caller's loop over
                    // columns, so returning would skip every column after this one. Leaving
                    // `active` as it is costs nothing, since nothing reads it after the loop.
                    break;
                }
                positions.clear();
                rows_at_position.clear();
                if uniform {
                    // Every row survives every pass, so skip the survivor bookkeeping.
                    for row_idx in active.iter().copied() {
                        let start = offsets[row_idx] as usize;
                        positions.push((start + position - first_offset) as $offset_type);
                        rows_at_position.push(row_idx);
                    }
                } else {
                    still_active.clear();
                    for row_idx in active.iter().copied() {
                        let start = offsets[row_idx] as usize;
                        let end = offsets[row_idx + 1] as usize;
                        positions.push((start + position - first_offset) as $offset_type);
                        rows_at_position.push(row_idx);
                        // Alive for the next pass only if it has an element beyond this one.
                        if start + position + 1 < end {
                            still_active.push(row_idx);
                        }
                    }
                    std::mem::swap(&mut active, &mut still_active);
                }
                position += 1;
                // `take` accepts any integer index type, so index by the offset width: a
                // `LargeList` can exceed `u32::MAX` elements.
                let taken = if std::mem::size_of::<$offset_type>() > 4 {
                    let indices = arrow::array::Int64Array::from_iter_values(
                        positions.iter().map(|p| *p as i64),
                    );
                    arrow::compute::take(&elements, &indices, None)?
                } else {
                    let indices = arrow::array::Int32Array::from_iter_values(
                        positions.iter().map(|p| *p as i32),
                    );
                    arrow::compute::take(&elements, &indices, None)?
                };
                // The hash width differs per algorithm (u32 for murmur3, u64 for xxhash64), so
                // let the element type come from the buffer rather than naming it here. Reused
                // across passes so the gather does not reallocate each time.
                position_hashes.clear();
                for row_idx in rows_at_position.iter() {
                    position_hashes.push($hashes[*row_idx]);
                }
                $recursive_hash_method(&[taken], &mut position_hashes)?;
                for (slot, row_idx) in rows_at_position.iter().enumerate() {
                    $hashes[*row_idx] = position_hashes[slot];
                }
            }
            }
        }
    };
}

/// Creates hash values for every row, based on the values in the
/// columns.
///
/// The number of rows to hash is determined by `hashes_buffer.len()`.
/// `hashes_buffer` should be pre-sized appropriately
///
/// `hash_method` is the hash function to use.
/// `create_dictionary_hash_method` is the function to create hashes for dictionary arrays input.
/// `recursive_hash_method` is the function to call for recursive hashing of complex types.
#[macro_export]
macro_rules! create_hashes_internal {
    ($arrays: ident, $hashes_buffer: ident, $hash_method: ident, $create_dictionary_hash_method: ident, $recursive_hash_method: ident) => {
        use arrow::datatypes::{DataType, TimeUnit};
        use arrow::array::{types::*, *};
        use datafusion_comet_common::children_with_parent_nulls;

        for (i, col) in $arrays.iter().enumerate() {
            // The dictionary fast path hashes each distinct dictionary value once and reuses that
            // result for every key, which is only valid while every row carries the same incoming
            // hash. Position in the column list is not a sufficient test: this macro also runs on
            // recursion, where a nested dictionary arrives as the only column of its call even
            // though the buffer already holds the hash accumulated for that row -- a
            // dictionary-encoded list element, for instance. So confirm the buffer is uniform,
            // which keeps the optimisation for a genuine first column (every row seeded alike,
            // whatever the seed) and unpacks otherwise. Only dictionaries need this, and the scan
            // is measurable on the hot path, so it is deferred into the dictionary arm below.
            let first_col = i == 0;
            match col.data_type() {
                DataType::Boolean => {
                    $crate::hash_array_boolean!(
                        BooleanArray,
                        col,
                        i32,
                        $hashes_buffer,
                        $hash_method
                    );
                }
                DataType::Int8 => {
                    $crate::hash_array_primitive!(
                        Int8Array,
                        col,
                        i32,
                        $hashes_buffer,
                        $hash_method
                    );
                }
                DataType::Int16 => {
                    $crate::hash_array_primitive!(
                        Int16Array,
                        col,
                        i32,
                        $hashes_buffer,
                        $hash_method
                    );
                }
                DataType::Int32 => {
                    $crate::hash_array_primitive!(
                        Int32Array,
                        col,
                        i32,
                        $hashes_buffer,
                        $hash_method
                    );
                }
                DataType::Int64 => {
                    $crate::hash_array_primitive!(
                        Int64Array,
                        col,
                        i64,
                        $hashes_buffer,
                        $hash_method
                    );
                }
                DataType::Float32 => {
                    $crate::hash_array_primitive_float!(
                        Float32Array,
                        col,
                        f32,
                        i32,
                        $hashes_buffer,
                        $hash_method
                    );
                }
                DataType::Float64 => {
                    $crate::hash_array_primitive_float!(
                        Float64Array,
                        col,
                        f64,
                        i64,
                        $hashes_buffer,
                        $hash_method
                    );
                }
                DataType::Timestamp(TimeUnit::Second, _) => {
                    $crate::hash_array_primitive!(
                        TimestampSecondArray,
                        col,
                        i64,
                        $hashes_buffer,
                        $hash_method
                    );
                }
                DataType::Timestamp(TimeUnit::Millisecond, _) => {
                    $crate::hash_array_primitive!(
                        TimestampMillisecondArray,
                        col,
                        i64,
                        $hashes_buffer,
                        $hash_method
                    );
                }
                DataType::Timestamp(TimeUnit::Microsecond, _) => {
                    $crate::hash_array_primitive!(
                        TimestampMicrosecondArray,
                        col,
                        i64,
                        $hashes_buffer,
                        $hash_method
                    );
                }
                DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                    $crate::hash_array_primitive!(
                        TimestampNanosecondArray,
                        col,
                        i64,
                        $hashes_buffer,
                        $hash_method
                    );
                }
                DataType::Date32 => {
                    $crate::hash_array_primitive!(
                        Date32Array,
                        col,
                        i32,
                        $hashes_buffer,
                        $hash_method
                    );
                }
                DataType::Date64 => {
                    $crate::hash_array_primitive!(
                        Date64Array,
                        col,
                        i64,
                        $hashes_buffer,
                        $hash_method
                    );
                }
                DataType::Time64(TimeUnit::Nanosecond) => {
                    $crate::hash_array_primitive!(
                        Time64NanosecondArray,
                        col,
                        i64,
                        $hashes_buffer,
                        $hash_method
                    );
                }
                DataType::Utf8 => {
                    $crate::hash_array!(StringArray, col, $hashes_buffer, $hash_method);
                }
                DataType::LargeUtf8 => {
                    $crate::hash_array!(LargeStringArray, col, $hashes_buffer, $hash_method);
                }
                DataType::Binary => {
                    $crate::hash_array!(BinaryArray, col, $hashes_buffer, $hash_method);
                }
                DataType::LargeBinary => {
                    $crate::hash_array!(LargeBinaryArray, col, $hashes_buffer, $hash_method);
                }
                DataType::FixedSizeBinary(_) => {
                    $crate::hash_array!(FixedSizeBinaryArray, col, $hashes_buffer, $hash_method);
                }
                // Apache Spark: if it's a small decimal, i.e. precision <= 18, turn it into long and hash it.
                // Else, turn it into bytes and hash it.
                DataType::Decimal128(precision, _) if *precision <= 18 => {
                    $crate::hash_array_small_decimal!(Decimal128Array, col, $hashes_buffer, $hash_method);
                }
                DataType::Decimal128(_, _) => {
                    $crate::hash_array_decimal!(Decimal128Array, col, $hashes_buffer, $hash_method);
                }
                DataType::Dictionary(index_type, _) => {
                    let first_col = first_col
                        && match $hashes_buffer.first() {
                            None => true,
                            Some(first) => $hashes_buffer.iter().all(|h| h == first),
                        };
                    match **index_type {
                    DataType::Int8 => {
                        $create_dictionary_hash_method::<Int8Type>(col, $hashes_buffer, first_col)?;
                    }
                    DataType::Int16 => {
                        $create_dictionary_hash_method::<Int16Type>(
                            col,
                            $hashes_buffer,
                            first_col,
                        )?;
                    }
                    DataType::Int32 => {
                        $create_dictionary_hash_method::<Int32Type>(
                            col,
                            $hashes_buffer,
                            first_col,
                        )?;
                    }
                    DataType::Int64 => {
                        $create_dictionary_hash_method::<Int64Type>(
                            col,
                            $hashes_buffer,
                            first_col,
                        )?;
                    }
                    DataType::UInt8 => {
                        $create_dictionary_hash_method::<UInt8Type>(
                            col,
                            $hashes_buffer,
                            first_col,
                        )?;
                    }
                    DataType::UInt16 => {
                        $create_dictionary_hash_method::<UInt16Type>(
                            col,
                            $hashes_buffer,
                            first_col,
                        )?;
                    }
                    DataType::UInt32 => {
                        $create_dictionary_hash_method::<UInt32Type>(
                            col,
                            $hashes_buffer,
                            first_col,
                        )?;
                    }
                    DataType::UInt64 => {
                        $create_dictionary_hash_method::<UInt64Type>(
                            col,
                            $hashes_buffer,
                            first_col,
                        )?;
                    }
                    _ => {
                        return Err(DataFusionError::Internal(format!(
                            "Unsupported dictionary type in hasher hashing: {}",
                            col.data_type(),
                        )))
                    }
                    }
                }
                DataType::List(field) => {
                    let list_array = col.as_any().downcast_ref::<ListArray>().unwrap();
                    let values = list_array.values();
                    let offsets = list_array.offsets();

                    $crate::hash_list_with_primitive_elements!(offsets: ListArray, list_array, values, offsets, field, $hashes_buffer, $hash_method, $recursive_hash_method, i32, col);
                }
                DataType::LargeList(field) => {
                    let list_array = col.as_any().downcast_ref::<LargeListArray>().unwrap();
                    let values = list_array.values();
                    let offsets = list_array.offsets();

                    $crate::hash_list_with_primitive_elements!(offsets: LargeListArray, list_array, values, offsets, field, $hashes_buffer, $hash_method, $recursive_hash_method, i64, col);
                }
                DataType::FixedSizeList(field, size) => {
                    let list_array = col.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
                    let values = list_array.values();
                    let list_size = *size as usize;

                    $crate::hash_list_with_primitive_elements!(fixed_size: list_array, values, list_size, field, $hashes_buffer, $hash_method, $recursive_hash_method);
                }
                DataType::Struct(_) => {
                    let struct_array = col.as_any().downcast_ref::<StructArray>().unwrap();
                    // Hash each field of the struct - Spark hashes all fields recursively, and a
                    // null struct hashes as the seed, so the parent's nulls have to reach the
                    // children first. See `datafusion_comet_common::struct_nulls`.
                    let columns = children_with_parent_nulls(struct_array)?;
                    if !columns.is_empty() {
                        $recursive_hash_method(&columns, $hashes_buffer)?;
                    }
                }
                DataType::Map(field, _) => {
                    let map_array = col.as_any().downcast_ref::<MapArray>().unwrap();
                    let keys = map_array.keys();
                    let values = map_array.values();
                    let offsets = map_array.offsets();

                    // Get key and value types from the struct field
                    if let DataType::Struct(fields) = field.data_type() {
                        let key_type = &fields[0].data_type();
                        let value_type = &fields[1].data_type();

                        // Specialize for common map key/value combinations
                        match (key_type, value_type) {
                            (DataType::Utf8, DataType::Int32) => {
                                let key_array = keys.as_any().downcast_ref::<StringArray>().unwrap();
                                let value_array = values.as_any().downcast_ref::<Int32Array>().unwrap();
                                if map_array.null_count() == 0 && key_array.null_count() == 0 && value_array.null_count() == 0 {
                                    for (row_idx, hash) in $hashes_buffer.iter_mut().enumerate() {
                                        let start = offsets[row_idx] as usize;
                                        let end = offsets[row_idx + 1] as usize;
                                        for entry_idx in start..end {
                                            *hash = $hash_method(key_array.value(entry_idx), *hash);
                                            *hash = $hash_method(value_array.value(entry_idx).to_le_bytes(), *hash);
                                        }
                                    }
                                } else {
                                    for (row_idx, hash) in $hashes_buffer.iter_mut().enumerate() {
                                        if !map_array.is_null(row_idx) {
                                            let start = offsets[row_idx] as usize;
                                            let end = offsets[row_idx + 1] as usize;
                                            for entry_idx in start..end {
                                                if !key_array.is_null(entry_idx) {
                                                    *hash = $hash_method(key_array.value(entry_idx), *hash);
                                                }
                                                if !value_array.is_null(entry_idx) {
                                                    *hash = $hash_method(value_array.value(entry_idx).to_le_bytes(), *hash);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            (DataType::Int32, DataType::Utf8) => {
                                let key_array = keys.as_any().downcast_ref::<Int32Array>().unwrap();
                                let value_array = values.as_any().downcast_ref::<StringArray>().unwrap();
                                if map_array.null_count() == 0 && key_array.null_count() == 0 && value_array.null_count() == 0 {
                                    for (row_idx, hash) in $hashes_buffer.iter_mut().enumerate() {
                                        let start = offsets[row_idx] as usize;
                                        let end = offsets[row_idx + 1] as usize;
                                        for entry_idx in start..end {
                                            *hash = $hash_method(key_array.value(entry_idx).to_le_bytes(), *hash);
                                            *hash = $hash_method(value_array.value(entry_idx), *hash);
                                        }
                                    }
                                } else {
                                    for (row_idx, hash) in $hashes_buffer.iter_mut().enumerate() {
                                        if !map_array.is_null(row_idx) {
                                            let start = offsets[row_idx] as usize;
                                            let end = offsets[row_idx + 1] as usize;
                                            for entry_idx in start..end {
                                                if !key_array.is_null(entry_idx) {
                                                    *hash = $hash_method(key_array.value(entry_idx).to_le_bytes(), *hash);
                                                }
                                                if !value_array.is_null(entry_idx) {
                                                    *hash = $hash_method(value_array.value(entry_idx), *hash);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            (DataType::Utf8, DataType::Utf8) => {
                                let key_array = keys.as_any().downcast_ref::<StringArray>().unwrap();
                                let value_array = values.as_any().downcast_ref::<StringArray>().unwrap();
                                if map_array.null_count() == 0 && key_array.null_count() == 0 && value_array.null_count() == 0 {
                                    for (row_idx, hash) in $hashes_buffer.iter_mut().enumerate() {
                                        let start = offsets[row_idx] as usize;
                                        let end = offsets[row_idx + 1] as usize;
                                        for entry_idx in start..end {
                                            *hash = $hash_method(key_array.value(entry_idx), *hash);
                                            *hash = $hash_method(value_array.value(entry_idx), *hash);
                                        }
                                    }
                                } else {
                                    for (row_idx, hash) in $hashes_buffer.iter_mut().enumerate() {
                                        if !map_array.is_null(row_idx) {
                                            let start = offsets[row_idx] as usize;
                                            let end = offsets[row_idx + 1] as usize;
                                            for entry_idx in start..end {
                                                if !key_array.is_null(entry_idx) {
                                                    *hash = $hash_method(key_array.value(entry_idx), *hash);
                                                }
                                                if !value_array.is_null(entry_idx) {
                                                    *hash = $hash_method(value_array.value(entry_idx), *hash);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            (DataType::Int32, DataType::Int32) => {
                                let key_array = keys.as_any().downcast_ref::<Int32Array>().unwrap();
                                let value_array = values.as_any().downcast_ref::<Int32Array>().unwrap();
                                if map_array.null_count() == 0 && key_array.null_count() == 0 && value_array.null_count() == 0 {
                                    for (row_idx, hash) in $hashes_buffer.iter_mut().enumerate() {
                                        let start = offsets[row_idx] as usize;
                                        let end = offsets[row_idx + 1] as usize;
                                        for entry_idx in start..end {
                                            *hash = $hash_method(key_array.value(entry_idx).to_le_bytes(), *hash);
                                            *hash = $hash_method(value_array.value(entry_idx).to_le_bytes(), *hash);
                                        }
                                    }
                                } else {
                                    for (row_idx, hash) in $hashes_buffer.iter_mut().enumerate() {
                                        if !map_array.is_null(row_idx) {
                                            let start = offsets[row_idx] as usize;
                                            let end = offsets[row_idx + 1] as usize;
                                            for entry_idx in start..end {
                                                if !key_array.is_null(entry_idx) {
                                                    *hash = $hash_method(key_array.value(entry_idx).to_le_bytes(), *hash);
                                                }
                                                if !value_array.is_null(entry_idx) {
                                                    *hash = $hash_method(value_array.value(entry_idx).to_le_bytes(), *hash);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            _ => {
                                // Fall back to recursive approach for other type combinations
                                if map_array.null_count() == 0 {
                                    for (row_idx, hash) in $hashes_buffer.iter_mut().enumerate() {
                                        let start = offsets[row_idx] as usize;
                                        let end = offsets[row_idx + 1] as usize;
                                        for entry_idx in start..end {
                                            let key_array = keys.slice(entry_idx, 1);
                                            let mut single_hash = [*hash];
                                            $recursive_hash_method(&[key_array], &mut single_hash)?;
                                            *hash = single_hash[0];

                                            let value_array = values.slice(entry_idx, 1);
                                            single_hash = [*hash];
                                            $recursive_hash_method(&[value_array], &mut single_hash)?;
                                            *hash = single_hash[0];
                                        }
                                    }
                                } else {
                                    for (row_idx, hash) in $hashes_buffer.iter_mut().enumerate() {
                                        if !map_array.is_null(row_idx) {
                                            let start = offsets[row_idx] as usize;
                                            let end = offsets[row_idx + 1] as usize;
                                            for entry_idx in start..end {
                                                let key_array = keys.slice(entry_idx, 1);
                                                let mut single_hash = [*hash];
                                                $recursive_hash_method(&[key_array], &mut single_hash)?;
                                                *hash = single_hash[0];

                                                let value_array = values.slice(entry_idx, 1);
                                                single_hash = [*hash];
                                                $recursive_hash_method(&[value_array], &mut single_hash)?;
                                                *hash = single_hash[0];
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    } else {
                        return Err(DataFusionError::Internal(format!(
                            "Map field type must be a struct, got: {}",
                            field.data_type()
                        )));
                    }
                }
                _ => {
                    // This is internal because we should have caught this before.
                    return Err(DataFusionError::Internal(format!(
                        "Unsupported data type in hasher: {}",
                        col.data_type()
                    )));
                }
            }
        }
    };
}

pub(crate) mod test_utils {

    #[macro_export]
    macro_rules! test_hashes_internal {
        ($hash_method: ident, $input: expr, $initial_seeds: expr, $expected: expr) => {
            let i = $input;
            let mut hashes = $initial_seeds.clone();
            $hash_method(&[i], &mut hashes).unwrap();
            assert_eq!(hashes, $expected);
        };
    }

    #[macro_export]
    macro_rules! test_hashes_with_nulls {
        ($method: ident, $t: ty, $values: ident, $expected: ident, $seed_type: ty) => {
            // copied before inserting nulls
            let mut input_with_nulls = $values.clone();
            let mut expected_with_nulls = $expected.clone();
            // test before inserting nulls
            let len = $values.len();
            let initial_seeds = vec![42 as $seed_type; len];
            let i = Arc::new(<$t>::from($values)) as ArrayRef;
            $crate::test_hashes_internal!($method, i, initial_seeds, $expected);

            // test with nulls
            let median = len / 2;
            input_with_nulls.insert(0, None);
            input_with_nulls.insert(median, None);
            expected_with_nulls.insert(0, 42 as $seed_type);
            expected_with_nulls.insert(median, 42 as $seed_type);
            let len_with_nulls = len + 2;
            let initial_seeds_with_nulls = vec![42 as $seed_type; len_with_nulls];
            let nullable_input = Arc::new(<$t>::from(input_with_nulls)) as ArrayRef;
            $crate::test_hashes_internal!(
                $method,
                nullable_input,
                initial_seeds_with_nulls,
                expected_with_nulls
            );
        };
    }
}
