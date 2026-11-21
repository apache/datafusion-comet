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

//! Element matching for Spark's array_contains, array_position and array_remove.

use std::ops::Range;
use std::sync::Arc;

use crate::float_semantics::{compare_float_array_scalar, compare_floats, spark_equality};
use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, GenericListArray, ListArray, OffsetSizeTrait,
    PrimitiveArray,
};
use arrow::buffer::{BooleanBuffer, NullBuffer};
use arrow::datatypes::{ArrowPrimitiveType, DataType, Float32Type, Float64Type};
use datafusion::common::{exec_err, utils::take_function_args, Result, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, Operator, ScalarFunctionArgs, ScalarUDFImpl};
use num::Float;

pub(super) fn invoke_match(
    udf: &impl ScalarUDFImpl,
    args: ScalarFunctionArgs,
    evaluate: impl FnOnce(&ArrayRef, &ColumnarValue) -> Result<ArrayRef>,
) -> Result<ColumnarValue> {
    let [array, needle] = take_function_args(udf.name(), &args.args)?;
    let rows = match (array, needle) {
        (_, ColumnarValue::Array(values)) => Some(values.len()),
        (ColumnarValue::Array(list), _) => Some(list.len()),
        _ => None,
    };
    let result = evaluate(&array.to_array_of_size(rows.unwrap_or(1))?, needle)?;
    match rows {
        None => Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
            &result, 0,
        )?)),
        Some(_) => Ok(ColumnarValue::Array(result)),
    }
}

/// Contains and remove can consume a bitmap for a scalar float. `IS [NOT] DISTINCT FROM`
/// applies Spark's float equality and treats null elements as distinct from the non-null needle.
pub(super) fn invoke_with_mask<M: ScalarUDFImpl + MatchOperation<i32>>(
    udf: &M,
    args: ScalarFunctionArgs,
    op: Operator,
    from_mask: impl FnOnce(&ListArray, &ArrayRef, BooleanBuffer) -> Result<ArrayRef>,
) -> Result<ColumnarValue> {
    invoke_match(udf, args, |array, needle| {
        let Some(list) = array.as_list_opt::<i32>() else {
            return exec_err!("{} takes a list, got {}", udf.name(), array.data_type());
        };
        // array_contains accepts a null scalar of any type. For array_remove, let
        // the row matcher validate the element type even when the needle is null.
        if op == Operator::IsNotDistinctFrom
            && matches!(needle, ColumnarValue::Scalar(value) if value.is_null())
        {
            return Ok(Arc::new(BooleanArray::new_null(list.len())));
        }
        if let ColumnarValue::Scalar(
            value @ (ScalarValue::Float32(Some(_)) | ScalarValue::Float64(Some(_))),
        ) = needle
        {
            let offsets = list.value_offsets();
            let elements = list.values().slice(
                offsets[0] as usize,
                (offsets[list.len()] - offsets[0]) as usize,
            );
            if let Some(mask) = compare_float_array_scalar(op, elements.as_ref(), value)? {
                return from_mask(list, &elements, mask.into_parts().0);
            }
        }
        MatchRows::new(list, needle)?.evaluate::<M>(spark_equality)
    })
}

pub(super) type EqualityBuilder =
    fn(&dyn Array, &dyn Array) -> Result<Box<dyn Fn(usize, usize) -> bool + Send + Sync>>;

/// Consume a predicate over non-null element and needle indices. The float specialization
/// lets removal copy the surviving elements directly from the typed buffer.
pub(super) trait MatchOperation<O: OffsetSizeTrait> {
    fn apply(rows: &MatchRows<'_, O>, equal: impl Fn(usize, usize) -> bool) -> Result<ArrayRef>;

    fn apply_float<T: ArrowPrimitiveType>(
        rows: &MatchRows<'_, O>,
        _values: &PrimitiveArray<T>,
        equal: impl Fn(usize, usize) -> bool,
    ) -> Result<ArrayRef> {
        Self::apply(rows, equal)
    }
}

pub(super) struct MatchRows<'a, O: OffsetSizeTrait> {
    pub array: &'a GenericListArray<O>,
    pub needles: ArrayRef,
    pub nulls: Option<NullBuffer>,
}

impl<'a, O: OffsetSizeTrait> MatchRows<'a, O> {
    pub fn new(array: &'a GenericListArray<O>, needle: &ColumnarValue) -> Result<Self> {
        let (needles, nulls) = match needle {
            ColumnarValue::Scalar(value) if value.is_null() => {
                (value.to_array()?, Some(NullBuffer::new_null(array.len())))
            }
            ColumnarValue::Scalar(value) => (value.to_array()?, array.nulls().cloned()),
            ColumnarValue::Array(values) => (
                Arc::clone(values),
                NullBuffer::union(array.nulls(), values.logical_nulls().as_ref()),
            ),
        };
        Ok(Self {
            array,
            needles,
            nulls,
        })
    }

    pub fn evaluate<M: MatchOperation<O>>(&self, equality: EqualityBuilder) -> Result<ArrayRef> {
        let elements = self.array.values();
        match (elements.data_type(), self.needles.data_type()) {
            (DataType::Float32, DataType::Float32) => self.match_floats::<M, Float32Type>(),
            (DataType::Float64, DataType::Float64) => self.match_floats::<M, Float64Type>(),
            _ => {
                let equal = equality(elements.as_ref(), self.needles.as_ref())?;
                M::apply(self, equal)
            }
        }
    }

    fn match_floats<M: MatchOperation<O>, T: ArrowPrimitiveType>(&self) -> Result<ArrayRef>
    where
        T::Native: Float,
    {
        let values = self.array.values().as_primitive::<T>();
        let needles = self.needles.as_primitive::<T>().values();
        let elements = values.values();
        M::apply_float(self, values, |i, j| {
            compare_floats(elements[i], needles[j]).is_eq()
        })
    }

    /// Each row's element range in the flattened values and its needle index.
    /// A null array or needle gives an empty range.
    pub fn iter(&self) -> impl Iterator<Item = (Range<usize>, usize)> + '_ {
        let scalar = self.needles.len() == 1;
        let nulls = self.nulls.as_ref();
        self.array
            .value_offsets()
            .windows(2)
            .enumerate()
            .map(move |(row, bounds)| {
                let range = if nulls.is_some_and(|n| n.is_null(row)) {
                    0..0
                } else {
                    bounds[0].as_usize()..bounds[1].as_usize()
                };
                (range, if scalar { 0 } else { row })
            })
    }

    /// Yield the first 1-based match (0 if absent) and whether a null element was
    /// encountered before the search ended.
    pub fn first_matches<'b>(
        &'b self,
        equal: impl Fn(usize, usize) -> bool + 'b,
    ) -> impl Iterator<Item = (i64, bool)> + 'b {
        let nulls = self.array.values().logical_nulls();
        self.iter().map(move |(range, needle)| {
            let mut has_null = false;
            for (index, i) in range.enumerate() {
                if nulls.as_ref().is_some_and(|n| n.is_null(i)) {
                    has_null = true;
                } else if equal(i, needle) {
                    return ((index + 1) as i64, has_null);
                }
            }
            (0, has_null)
        })
    }
}
