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

// Spark-compatible sequence(start, stop[, step]) for integral element types.
//
// Mirrors the code Spark's whole-stage codegen emits for `Sequence`
// (`sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/collectionOperations.scala`,
// identical from 3.4.3 through 4.1.1): the boundary check and `Sequence.sequenceLength` decide
// per row how many elements to generate, then elements are `start + step * i`. Unlike the JVM
// path, which allocates two `long[]` per row and copies every element three times, this kernel
// reserves the Arrow child buffer once for the whole batch and writes each element exactly once.
//
// Date/timestamp sequences are not handled here; the Scala serde only routes IntegralType
// sequences to this function.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, ListArray, PrimitiveArray};
use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, FieldRef, Int16Type, Int32Type, Int64Type, Int8Type,
};
use datafusion::common::cast::as_primitive_array;
use datafusion::common::{exec_err, internal_err, DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::ColumnarValue;

use crate::SparkError;

/// Spark's ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH (Integer.MAX_VALUE - 15).
const MAX_ROUNDED_ARRAY_LENGTH: i64 = (i32::MAX - 15) as i64;

pub fn spark_sequence(args: &[ColumnarValue], data_type: &DataType) -> Result<ColumnarValue> {
    let child_field = match data_type {
        DataType::List(field) => Arc::clone(field),
        other => return exec_err!("spark_sequence expects a List return type, got {other:?}"),
    };
    if args.len() != 2 && args.len() != 3 {
        return exec_err!(
            "spark_sequence expects 2 or 3 arguments, got {}",
            args.len()
        );
    }

    let all_scalar = args
        .iter()
        .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
    let num_rows = args
        .iter()
        .find_map(|arg| match arg {
            ColumnarValue::Array(array) => Some(array.len()),
            ColumnarValue::Scalar(_) => None,
        })
        .unwrap_or(1);
    for arg in args {
        if let ColumnarValue::Array(array) = arg {
            if array.len() != num_rows {
                return internal_err!(
                    "Arguments has mixed length. Expected length: {num_rows}, found length: {}",
                    array.len()
                );
            }
        }
    }

    let result = match child_field.data_type() {
        DataType::Int8 => sequence_integral::<Int8Type>(args, num_rows, child_field),
        DataType::Int16 => sequence_integral::<Int16Type>(args, num_rows, child_field),
        DataType::Int32 => sequence_integral::<Int32Type>(args, num_rows, child_field),
        DataType::Int64 => sequence_integral::<Int64Type>(args, num_rows, child_field),
        other => exec_err!("spark_sequence does not support element type {other:?}"),
    }?;

    if all_scalar {
        Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
            &result, 0,
        )?))
    } else {
        Ok(ColumnarValue::Array(result))
    }
}

trait Integral: ArrowPrimitiveType {
    fn scalar(value: &ScalarValue) -> Result<Option<Self::Native>>;
    fn from_i64(value: i64) -> Self::Native;
}
macro_rules! integral {
    ($ty:ty, $variant:ident, $native:ty) => {
        impl Integral for $ty {
            fn scalar(value: &ScalarValue) -> Result<Option<Self::Native>> {
                match value {
                    ScalarValue::$variant(value) => Ok(*value),
                    _ => exec_err!(
                        "spark_sequence expects {} arguments, got {}",
                        Self::DATA_TYPE,
                        value.data_type()
                    ),
                }
            }
            fn from_i64(value: i64) -> Self::Native {
                value as $native
            }
        }
    };
}
integral!(Int8Type, Int8, i8);
integral!(Int16Type, Int16, i16);
integral!(Int32Type, Int32, i32);
integral!(Int64Type, Int64, i64);

enum Input<'a, T: Integral> {
    Scalar(Option<T::Native>),
    Array(&'a PrimitiveArray<T>),
}
impl<'a, T: Integral> Input<'a, T>
where
    T::Native: Into<i64>,
{
    fn new(value: &'a ColumnarValue) -> Result<Self> {
        match value {
            ColumnarValue::Scalar(value) => Ok(Self::Scalar(T::scalar(value)?)),
            ColumnarValue::Array(array) => Ok(Self::Array(as_primitive_array::<T>(array)?)),
        }
    }
    #[inline(always)]
    fn value(&self, row: usize) -> i64 {
        match self {
            Self::Scalar(value) => value.unwrap().into(),
            Self::Array(array) => array.values()[row].into(),
        }
    }
    fn nulls(&self, len: usize) -> Option<NullBuffer> {
        match self {
            Self::Scalar(None) => Some(NullBuffer::new_null(len)),
            Self::Scalar(Some(_)) => None,
            Self::Array(array) => array.nulls().cloned(),
        }
    }
}

fn sequence_integral<T: Integral>(
    args: &[ColumnarValue],
    num_rows: usize,
    child_field: FieldRef,
) -> Result<ArrayRef>
where
    T::Native: Into<i64>,
{
    let start = Input::<T>::new(&args[0])?;
    let stop = Input::<T>::new(&args[1])?;
    let step = args.get(2).map(Input::<T>::new).transpose()?;
    // Reuse validity once per batch instead of checking three inputs and rebuilding a
    // bitmap row by row. Arrow unions sliced bitmaps with bulk word operations.
    let nulls = NullBuffer::union(
        start.nulls(num_rows).as_ref(),
        stop.nulls(num_rows).as_ref(),
    );
    let step_nulls = step.as_ref().and_then(|step| step.nulls(num_rows));
    let nulls = NullBuffer::union(nulls.as_ref(), step_nulls.as_ref())
        .filter(|nulls| nulls.null_count() != 0);
    if nulls
        .as_ref()
        .is_some_and(|nulls| nulls.null_count() == num_rows)
    {
        return Ok(Arc::new(ListArray::new_null(child_field, num_rows)));
    }
    let row_step = |row: usize, start: i64, stop: i64| -> i64 {
        match &step {
            Some(step) => step.value(row),
            None if start <= stop => 1,
            None => -1,
        }
    };

    // Validate every row before checking the batch total, preserving the first Spark error.
    // Retain lengths so short sequences do not repeat per-row arithmetic during generation.
    let mut lengths = Vec::with_capacity(num_rows);
    let mut total = 0usize;
    for row in 0..num_rows {
        if nulls.as_ref().is_some_and(|nulls| nulls.is_null(row)) {
            lengths.push(0);
            continue;
        }
        let s = start.value(row);
        let e = stop.value(row);
        let len = sequence_length(s, e, row_step(row, s, e))?;
        total += len;
        lengths.push(len);
    }
    // Arrow's List offsets must fit i32; Spark's per-row array limit is checked above.
    if total > i32::MAX as usize {
        return Err(DataFusionError::External(Box::new(
            SparkError::SequenceBatchTooLarge {
                total_elements: total.to_string(),
            },
        )));
    }

    let mut values: Vec<T::Native> = Vec::new();
    values.try_reserve_exact(total).map_err(|_| {
        DataFusionError::External(Box::new(SparkError::SequenceBatchTooLarge {
            total_elements: total.to_string(),
        }))
    })?;
    let slots = &mut values.spare_capacity_mut()[..total];
    let mut offsets = Vec::with_capacity(num_rows + 1);
    offsets.push(0);
    let mut written = 0;
    for (row, &len) in lengths.iter().enumerate() {
        if len != 0 {
            let s = start.value(row);
            let e = stop.value(row);
            let step = row_step(row, s, e);
            // Fixed bounds and independent arithmetic let LLVM vectorise the stores without
            // a capacity check for each element. Wrapping intermediates preserve i64 extremes.
            for (i, slot) in slots[written..written + len].iter_mut().enumerate() {
                slot.write(T::from_i64(s.wrapping_add(step.wrapping_mul(i as i64))));
            }
            written += len;
        }
        offsets.push(written as i32);
    }
    // SAFETY: the sizing pass reserved `total` slots and the loop initialised each exactly
    // once. No fallible operation exposes the partially initialised vector.
    unsafe { values.set_len(total) };
    let values = PrimitiveArray::<T>::new(ScalarBuffer::from(values), None);
    Ok(Arc::new(ListArray::try_new(
        child_field,
        OffsetBuffer::new(offsets.into()),
        Arc::new(values),
        nulls,
    )?))
}

/// Match released Spark 3.4--4.1 Sequence.sequenceLength, including its wide-arithmetic
/// fallback error when subtraction overflows even though the resulting length would fit.
#[inline(always)]
fn sequence_length(start: i64, stop: i64, step: i64) -> Result<usize> {
    if !((step > 0 && start <= stop) || (step < 0 && start >= stop) || (step == 0 && start == stop))
    {
        return Err(boundary_error(start, stop, step));
    }
    if stop == start {
        return Ok(1);
    }
    if let Some(len) = stop
        .checked_sub(start)
        .and_then(|delta| match step {
            1 => Some(delta),
            -1 => delta.checked_neg(),
            _ => delta.checked_div(step),
        })
        .and_then(|quotient| quotient.checked_add(1))
    {
        if len <= MAX_ROUNDED_ARRAY_LENGTH {
            return Ok(len as usize);
        }
        return Err(length_error(len as i128));
    }
    sequence_length_overflow(start, stop, step)
}

#[cold]
fn boundary_error(start: i64, stop: i64, step: i64) -> DataFusionError {
    DataFusionError::External(Box::new(SparkError::SequenceIllegalBoundaries {
        start: start.to_string(),
        stop: stop.to_string(),
        step: step.to_string(),
    }))
}

#[cold]
fn sequence_length_overflow(start: i64, stop: i64, step: i64) -> Result<usize> {
    // Only the rare overflow/error path needs wide division. There is no separate add-overflow
    // branch: its exact length necessarily exceeds Spark's maximum and is rejected here.
    let len = 1 + (stop as i128 - start as i128) / step as i128;
    if len > MAX_ROUNDED_ARRAY_LENGTH as i128 {
        return Err(length_error(len));
    }
    Err(DataFusionError::External(Box::new(SparkError::Internal(
        "Unreachable code reached.".to_string(),
    ))))
}

#[cold]
fn length_error(len: i128) -> DataFusionError {
    DataFusionError::External(Box::new(SparkError::CollectionSizeLimitExceeded {
        num_elements: len.to_string(),
        max_elements: MAX_ROUNDED_ARRAY_LENGTH,
        function_name: "sequence".to_string(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, Int8Array};
    use arrow::datatypes::Field;

    fn list_of(elem: DataType) -> DataType {
        DataType::List(Arc::new(Field::new_list_field(elem, false)))
    }

    fn run_i64(
        start: Vec<Option<i64>>,
        stop: Vec<Option<i64>>,
        step: Option<Vec<Option<i64>>>,
    ) -> Result<ListArray> {
        let mut args = vec![
            ColumnarValue::Array(Arc::new(Int64Array::from(start))),
            ColumnarValue::Array(Arc::new(Int64Array::from(stop))),
        ];
        if let Some(step) = step {
            args.push(ColumnarValue::Array(Arc::new(Int64Array::from(step))));
        }
        match spark_sequence(&args, &list_of(DataType::Int64))? {
            ColumnarValue::Array(arr) => Ok(as_primitive_list(&arr)),
            ColumnarValue::Scalar(_) => unreachable!("array inputs produce an array"),
        }
    }

    fn as_primitive_list(arr: &ArrayRef) -> ListArray {
        arr.as_any().downcast_ref::<ListArray>().unwrap().clone()
    }

    fn row_values(list: &ListArray, row: usize) -> Vec<i64> {
        let v = list.value(row);
        as_primitive_array::<Int64Type>(&v)
            .unwrap()
            .values()
            .to_vec()
    }

    #[test]
    fn ascending_descending_and_default_step() {
        let list = run_i64(
            vec![Some(1), Some(5), Some(3), Some(1)],
            vec![Some(5), Some(1), Some(3), Some(10)],
            Some(vec![Some(2), Some(-2), Some(0), Some(3)]),
        )
        .unwrap();
        assert_eq!(row_values(&list, 0), vec![1, 3, 5]);
        assert_eq!(row_values(&list, 1), vec![5, 3, 1]);
        assert_eq!(row_values(&list, 2), vec![3]);
        assert_eq!(row_values(&list, 3), vec![1, 4, 7, 10]);

        let list = run_i64(vec![Some(1), Some(5)], vec![Some(3), Some(2)], None).unwrap();
        assert_eq!(row_values(&list, 0), vec![1, 2, 3]);
        assert_eq!(row_values(&list, 1), vec![5, 4, 3, 2]);
    }

    #[test]
    fn null_inputs_produce_null_rows() {
        let list = run_i64(
            vec![None, Some(1), Some(1)],
            vec![Some(3), None, Some(3)],
            Some(vec![Some(1), Some(1), None]),
        )
        .unwrap();
        assert!(list.is_null(0));
        assert!(list.is_null(1));
        assert!(list.is_null(2));
    }

    #[test]
    fn illegal_boundaries() {
        let err = run_i64(vec![Some(1)], vec![Some(5)], Some(vec![Some(-1)]))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("Illegal sequence boundaries: 1 to 5 by -1"),
            "{err}"
        );
        let err = run_i64(vec![Some(1)], vec![Some(5)], Some(vec![Some(0)]))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("Illegal sequence boundaries: 1 to 5 by 0"),
            "{err}"
        );
    }

    #[test]
    fn length_limit_and_overflow_edges() {
        // Plain path: length exceeds MAX_ROUNDED_ARRAY_LENGTH.
        let err = run_i64(vec![Some(0)], vec![Some(i64::MAX - 1)], Some(vec![Some(1)]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("9223372036854775807"), "{err}");

        // Math.addExact(1, delta / step) overflow: count reported as 2^63.
        let err = run_i64(vec![Some(0)], vec![Some(i64::MAX)], Some(vec![Some(1)]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("9223372036854775808"), "{err}");

        // Long.MinValue / -1 special case: count reported as 2^63 + 1.
        let err = run_i64(vec![Some(0)], vec![Some(i64::MIN)], Some(vec![Some(-1)]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("9223372036854775809"), "{err}");

        // subtractExact overflow with a step large enough to keep the exact length small:
        // Spark reaches internalError("Unreachable code reached.").
        let err = run_i64(
            vec![Some(i64::MIN)],
            vec![Some(i64::MAX)],
            Some(vec![Some(i64::MAX)]),
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("Unreachable code reached."), "{err}");
    }

    #[test]
    fn narrow_types_and_scalar_inputs() {
        let args = vec![
            ColumnarValue::Array(Arc::new(Int8Array::from(vec![Some(1i8), Some(-3)]))),
            ColumnarValue::Array(Arc::new(Int8Array::from(vec![Some(5i8), Some(-1)]))),
        ];
        let result = spark_sequence(&args, &list_of(DataType::Int8)).unwrap();
        let ColumnarValue::Array(arr) = result else {
            unreachable!("array inputs produce an array")
        };
        let list = as_primitive_list(&arr);
        let v0 = list.value(0);
        assert_eq!(
            as_primitive_array::<Int8Type>(&v0).unwrap().values(),
            &[1, 2, 3, 4, 5]
        );
        let v1 = list.value(1);
        assert_eq!(
            as_primitive_array::<Int8Type>(&v1).unwrap().values(),
            &[-3, -2, -1]
        );

        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(3))),
        ];
        let result = spark_sequence(&args, &list_of(DataType::Int64)).unwrap();
        let ColumnarValue::Scalar(ScalarValue::List(list)) = result else {
            panic!("all-scalar inputs should produce a List scalar")
        };
        assert_eq!(row_values(&list, 0), vec![1, 2, 3]);
    }
    #[test]
    fn scalar_and_array_combinations_preserve_each_integral_type() {
        macro_rules! verify {
            ($ty:ty, $variant:ident, $native:ty) => {{
                for (start, stop, step) in [
                    (<$native>::MIN, <$native>::MIN + 3, 1),
                    (<$native>::MAX, <$native>::MAX - 3, -1),
                    (0, 0, 0),
                    (-3, 4, 2),
                    (<$native>::MIN, -1, <$native>::MAX),
                    (0, <$native>::MIN, <$native>::MIN),
                ] {
                    for mask in 0..8 {
                        let args = [start, stop, step]
                            .into_iter()
                            .enumerate()
                            .map(|(index, value)| {
                                let scalar = ScalarValue::$variant(Some(value));
                                if mask & (1 << index) == 0 {
                                    ColumnarValue::Scalar(scalar)
                                } else {
                                    ColumnarValue::Array(scalar.to_array_of_size(3).unwrap())
                                }
                            })
                            .collect::<Vec<_>>();
                        let result = spark_sequence(&args, &list_of(<$ty>::DATA_TYPE)).unwrap();
                        assert_eq!(matches!(result, ColumnarValue::Scalar(_)), mask == 0);
                        let array = result.into_array(3).unwrap();
                        let list = as_primitive_list(&array);
                        let expected_len = if start == stop {
                            1
                        } else {
                            (1 + (stop as i128 - start as i128) / step as i128) as usize
                        };
                        for row in 0..3 {
                            let values = list.value(row);
                            let values = as_primitive_array::<$ty>(&values).unwrap();
                            assert_eq!(values.len(), expected_len);
                            for (i, actual) in values.values().iter().enumerate() {
                                assert_eq!(
                                    *actual as i128,
                                    start as i128 + step as i128 * i as i128
                                );
                            }
                        }
                    }
                }
            }};
        }
        verify!(Int8Type, Int8, i8);
        verify!(Int16Type, Int16, i16);
        verify!(Int32Type, Int32, i32);
        verify!(Int64Type, Int64, i64);
    }

    #[test]
    fn sliced_null_inputs_skip_invalid_underlying_values() {
        use arrow::buffer::NullBuffer;
        // Slice at a non-byte-aligned offset. The null start hides invalid boundaries;
        // the null stop hides a length overflow; the null step hides zero-step boundaries.
        let arrays = [
            Int64Array::new(
                vec![99, 5, i64::MIN, 1, 7, 99].into(),
                Some(NullBuffer::from(vec![true, false, true, true, true, true])),
            ),
            Int64Array::new(
                vec![99, 1, i64::MAX, 5, 7, 99].into(),
                Some(NullBuffer::from(vec![true, true, false, true, true, true])),
            ),
            Int64Array::new(
                vec![99, 1, 1, 0, 0, 99].into(),
                Some(NullBuffer::from(vec![true, true, true, false, true, true])),
            ),
        ];
        let args = arrays
            .into_iter()
            .map(|array| ColumnarValue::Array(Arc::new(array.slice(1, 4))))
            .collect::<Vec<_>>();
        let result = spark_sequence(&args, &list_of(DataType::Int64))
            .unwrap()
            .into_array(4)
            .unwrap();
        let list = as_primitive_list(&result);
        assert_eq!(
            list.nulls().unwrap(),
            &NullBuffer::from(vec![false, false, false, true])
        );
        assert_eq!(list.value_offsets(), &[0, 0, 0, 0, 1]);
        assert_eq!(row_values(&list, 3), vec![7]);
        assert_eq!(list.values().null_count(), 0);

        for index in 0..3 {
            let mut args = vec![
                ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(5))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(-1))),
            ];
            args[index] = ColumnarValue::Scalar(ScalarValue::Int64(None));
            let result = spark_sequence(&args, &list_of(DataType::Int64)).unwrap();
            assert!(
                matches!(result, ColumnarValue::Scalar(ScalarValue::List(ref list)) if list.is_null(0))
            );
        }
    }

    #[test]
    fn empty_and_mismatched_arrays() {
        let empty = ColumnarValue::Array(Arc::new(Int64Array::from(Vec::<i64>::new())));
        let scalar = ColumnarValue::Scalar(ScalarValue::Int64(Some(1)));
        let result = spark_sequence(&[empty.clone(), scalar.clone()], &list_of(DataType::Int64))
            .unwrap()
            .into_array(0)
            .unwrap();
        let list = as_primitive_list(&result);
        assert!(list.is_empty());
        assert_eq!(list.value_offsets(), &[0]);
        assert!(list.values().is_empty());
        let one = ColumnarValue::Array(Arc::new(Int64Array::from(vec![1])));
        assert!(spark_sequence(&[empty, scalar, one], &list_of(DataType::Int64)).is_err());
    }

    #[test]
    fn row_errors_precede_aggregate_limit() {
        let error = run_i64(
            vec![Some(0), Some(0), Some(5)],
            vec![Some(1_073_741_823), Some(1_073_741_823), Some(1)],
            Some(vec![Some(1), Some(1), Some(1)]),
        )
        .unwrap_err()
        .to_string();
        assert!(
            error.contains("Illegal sequence boundaries: 5 to 1 by 1"),
            "{error}"
        );
        let error = run_i64(
            vec![Some(0), Some(0)],
            vec![Some(1_073_741_823), Some(1_073_741_823)],
            None,
        )
        .unwrap_err()
        .to_string();
        assert!(
            error.contains("2147483648") && error.contains("spark.comet.batchSize"),
            "{error}"
        );
    }

    #[test]
    fn checked_length_matches_exact_released_spark_arithmetic() {
        let values = [
            i64::MIN,
            i64::MIN + 1,
            -(i32::MAX as i64),
            -1,
            0,
            1,
            i32::MAX as i64,
            i64::MAX - 1,
            i64::MAX,
        ];
        for start in values {
            for stop in values {
                for step in values {
                    let result = sequence_length(start, stop, step);
                    let legal = (step > 0 && start <= stop)
                        || (step < 0 && start >= stop)
                        || (step == 0 && start == stop);
                    if !legal {
                        assert!(result
                            .unwrap_err()
                            .to_string()
                            .contains("Illegal sequence boundaries"));
                    } else if start == stop {
                        assert_eq!(result.unwrap(), 1);
                    } else {
                        let delta = stop as i128 - start as i128;
                        let len = 1 + delta / step as i128;
                        if len > MAX_ROUNDED_ARRAY_LENGTH as i128 {
                            let error = result.unwrap_err().to_string();
                            assert!(
                                error.contains(&len.to_string()),
                                "{start}, {stop}, {step}: {error}"
                            );
                        } else if delta < i64::MIN as i128 || delta > i64::MAX as i128 {
                            assert!(result
                                .unwrap_err()
                                .to_string()
                                .contains("Unreachable code reached."));
                        } else {
                            assert_eq!(result.unwrap(), len as usize);
                        }
                    }
                }
            }
        }
        assert_eq!(
            sequence_length(0, MAX_ROUNDED_ARRAY_LENGTH - 1, 1).unwrap(),
            MAX_ROUNDED_ARRAY_LENGTH as usize
        );
        assert!(sequence_length(0, MAX_ROUNDED_ARRAY_LENGTH, 1).is_err());
    }
}
