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

mod array_extrema;
mod array_insert;
mod array_position;
mod array_remove;
mod array_slice;
mod arrays_overlap;
mod arrays_zip;
mod flatten;
mod get_array_struct_fields;
mod list_extract;
mod list_positions;
mod nested_comparison;
mod sequence;
mod size;
mod sort_array;

pub use array_extrema::SparkArrayExtrema;
pub use array_insert::ArrayInsert;
pub use array_position::SparkArrayPositionFunc;
pub use array_remove::SparkArrayRemove;
pub use array_slice::SparkArraySlice;
pub use arrays_overlap::SparkArraysOverlap;
pub use arrays_zip::SparkArraysZipFunc;
pub use flatten::SparkFlatten;
pub use get_array_struct_fields::GetArrayStructFields;
pub use list_extract::ListExtract;
pub use list_positions::ListPositionsExpr;
pub use nested_comparison::{spark_comparison, spark_in_list, FloatOperands};
pub use sequence::spark_sequence;
pub use size::{spark_size, SparkSizeFunc};
pub use sort_array::SparkSortArray;

use arrow::array::{ArrayRef, ListArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use std::sync::Arc;

/// A list with the field of `array` and the given row nulls, holding `values` at `offsets`.
fn with_values(
    array: &ListArray,
    offsets: OffsetBuffer<i32>,
    values: ArrayRef,
    nulls: Option<NullBuffer>,
) -> ArrayRef {
    let (field, ..) = array.clone().into_parts();
    Arc::new(ListArray::new(field, offsets, values, nulls))
}

#[cfg(test)]
mod test_util {
    use arrow::array::{ArrayRef, AsArray, ListArray};
    use arrow::datatypes::Float64Type;
    use std::sync::Arc;

    /// A list of `DOUBLE` rows.
    pub(super) fn list(rows: &[Option<Vec<Option<f64>>>]) -> ArrayRef {
        Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>(
            rows.iter().cloned(),
        ))
    }

    /// The bits of every element of a list of `DOUBLE` rows, which tell `-0.0` from `0.0` and one
    /// NaN from another.
    pub(super) fn bits(array: &ArrayRef) -> Vec<Option<Vec<Option<u64>>>> {
        array
            .as_list::<i32>()
            .iter()
            .map(|row| {
                row.map(|row| {
                    row.as_primitive::<Float64Type>()
                        .iter()
                        .map(|v| v.map(f64::to_bits))
                        .collect()
                })
            })
            .collect()
    }

    /// The bits of the first field of the struct elements in the first row of a list.
    pub(super) fn first_row_field_bits(array: &ArrayRef) -> Vec<u64> {
        let row = array.as_list::<i32>().value(0);
        let field = row.as_struct().column(0).as_primitive::<Float64Type>();
        field.values().iter().map(|v| v.to_bits()).collect()
    }
}
