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

use arrow::array::{Array, ArrayRef, ListArray, NullBufferBuilder, PrimitiveArray};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, FieldRef, Int16Type, Int32Type, Int64Type, Int8Type,
};
use datafusion::common::cast::as_primitive_array;
use datafusion::common::{exec_err, DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::ColumnarValue;

use crate::SparkError;

/// Spark's ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH (Integer.MAX_VALUE - 15).
const MAX_ROUNDED_ARRAY_LENGTH: i128 = (i32::MAX - 15) as i128;

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
    let arrays = ColumnarValue::values_to_arrays(args)?;
    let step = arrays.get(2);

    let result = match child_field.data_type() {
        DataType::Int8 => {
            sequence_integral::<Int8Type>(&arrays[0], &arrays[1], step, child_field, |v| v as i8)
        }
        DataType::Int16 => {
            sequence_integral::<Int16Type>(&arrays[0], &arrays[1], step, child_field, |v| v as i16)
        }
        DataType::Int32 => {
            sequence_integral::<Int32Type>(&arrays[0], &arrays[1], step, child_field, |v| v as i32)
        }
        DataType::Int64 => {
            sequence_integral::<Int64Type>(&arrays[0], &arrays[1], step, child_field, |v| v)
        }
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

fn sequence_integral<T: ArrowPrimitiveType>(
    start: &ArrayRef,
    stop: &ArrayRef,
    step: Option<&ArrayRef>,
    child_field: FieldRef,
    from_i64: impl Fn(i64) -> T::Native,
) -> Result<ArrayRef>
where
    T::Native: Into<i64>,
{
    let start = as_primitive_array::<T>(start)?;
    let stop = as_primitive_array::<T>(stop)?;
    let step = step.map(|arr| as_primitive_array::<T>(arr)).transpose()?;
    let num_rows = start.len();

    let row_is_null = |row: usize| {
        start.is_null(row) || stop.is_null(row) || step.is_some_and(|arr| arr.is_null(row))
    };
    // With no explicit step, Spark uses `start <= stop ? 1 : -1` per row, so the direction
    // always matches the bounds and the boundary check below cannot fail.
    let row_step = |row: usize, start: i64, stop: i64| -> i64 {
        match step {
            Some(arr) => arr.value(row).into(),
            None => {
                if start <= stop {
                    1
                } else {
                    -1
                }
            }
        }
    };

    // First pass: compute per-row lengths so the child buffer can be reserved once for the
    // whole batch. Valid rows always produce at least one element, so length 0 marks a null row.
    let mut lengths: Vec<usize> = Vec::with_capacity(num_rows);
    let mut total: usize = 0;
    for row in 0..num_rows {
        if row_is_null(row) {
            lengths.push(0);
            continue;
        }
        let s: i64 = start.value(row).into();
        let e: i64 = stop.value(row).into();
        let len = sequence_length(s, e, row_step(row, s, e))?;
        total += len;
        lengths.push(len);
    }
    // Comet-specific ceiling: the sum of every row's length in one Arrow batch must fit in
    // the i32 offset buffer. Spark has no equivalent guard because it stores each row as its
    // own `long[]`, so the user may hit this on a query Spark itself would run. Report it via
    // a dedicated error that names `spark.comet.batchSize` as the actionable knob rather than
    // Spark's per-array size limit.
    if total > i32::MAX as usize {
        return Err(DataFusionError::External(Box::new(
            SparkError::SequenceBatchTooLarge {
                total_elements: total.to_string(),
            },
        )));
    }

    // Second pass: write elements straight into the child buffer and push offsets. The
    // batch-total check above guarantees `values.len() <= i32::MAX` at every iteration, so
    // the offset push cannot overflow. `try_reserve_exact` returns a query error on allocator
    // failure so an oversized reservation cannot abort the executor.
    let mut values: Vec<T::Native> = Vec::new();
    values.try_reserve_exact(total).map_err(|_| {
        DataFusionError::External(Box::new(SparkError::SequenceBatchTooLarge {
            total_elements: total.to_string(),
        }))
    })?;
    let mut offsets: Vec<i32> = Vec::with_capacity(num_rows + 1);
    offsets.push(0);
    let mut nulls = NullBufferBuilder::new(num_rows);
    for (row, &len) in lengths.iter().enumerate() {
        if len == 0 {
            nulls.append_null();
        } else {
            nulls.append_non_null();
            let s: i64 = start.value(row).into();
            let e: i64 = stop.value(row).into();
            let step = row_step(row, s, e);
            // Every element pushed lies between start and stop inclusive, so the widened
            // arithmetic cannot overflow; only the final unused increment may wrap.
            let mut v = s;
            for _ in 0..len {
                values.push(from_i64(v));
                v = v.wrapping_add(step);
            }
        }
        offsets.push(values.len() as i32);
    }

    let values = PrimitiveArray::<T>::new(ScalarBuffer::from(values), None);
    let list = ListArray::try_new(
        child_field,
        OffsetBuffer::new(offsets.into()),
        Arc::new(values),
        nulls.finish(),
    )?;
    Ok(Arc::new(list))
}

/// Number of elements of `sequence(start, stop, step)`, matching Spark's boundary check and
/// `Sequence.sequenceLength` (byte-identical from Spark 3.4.3 through 4.1.1), including which
/// of the three failure paths fires and the exact length value each of them reports.
fn sequence_length(start: i64, stop: i64, step: i64) -> Result<usize> {
    if !((step > 0 && start <= stop) || (step < 0 && start >= stop) || (step == 0 && start == stop))
    {
        return Err(DataFusionError::External(Box::new(
            SparkError::SequenceIllegalBoundaries {
                start: start.to_string(),
                stop: stop.to_string(),
                step: step.to_string(),
            },
        )));
    }
    if stop == start {
        return Ok(1);
    }
    // Spark computes stop - start with Math.subtractExact and special-cases
    // Long.MinValue / -1; both raise ArithmeticException, which reroutes the length through a
    // BigInt fallback. i128 arithmetic gives the same exact value on every path.
    //
    // Max delta magnitude is |i64::MAX - i64::MIN| = 2 * i64::MAX + 1, which fits in i128 but
    // overflows i64, so the `> i64::MAX` / `< i64::MIN` predicates below are both live.
    let delta = stop as i128 - start as i128;
    let overflowed = delta > i64::MAX as i128
        || delta < i64::MIN as i128
        || (delta == i64::MIN as i128 && step == -1);
    let len = 1 + delta / step as i128;
    if len > MAX_ROUNDED_ARRAY_LENGTH {
        return Err(DataFusionError::External(Box::new(
            SparkError::CollectionSizeLimitExceeded {
                num_elements: len.to_string(),
                max_elements: MAX_ROUNDED_ARRAY_LENGTH as i64,
                function_name: "sequence".to_string(),
            },
        )));
    }
    if overflowed {
        // Spark's BigInt fallback lands on `internalError("Unreachable code reached.")` when
        // the exact length is within the limit (e.g. sequence spanning more than Long.MaxValue
        // with a large step); reproduce it for error parity.
        return Err(DataFusionError::External(Box::new(SparkError::Internal(
            "Unreachable code reached.".to_string(),
        ))));
    }
    Ok(len as usize)
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
}
