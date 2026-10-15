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

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, ListArray, PrimitiveArray};
use arrow::buffer::{BooleanBuffer, Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, FieldRef, Int16Type, Int32Type, Int64Type, Int8Type,
};
use datafusion::common::cast::as_primitive_array;
use datafusion::common::{exec_err, internal_err, DataFusionError, Result, ScalarValue};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::SparkError;

/// Spark's ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH (Integer.MAX_VALUE - 15).
const MAX_ROUNDED_ARRAY_LENGTH: i64 = (i32::MAX - 15) as i64;

/// Native sequence holds a task-pool reservation only while constructing its output. Keeping
/// the consumer invocation-scoped avoids idle UDFs reducing other consumers' fair shares.
#[derive(Debug)]
pub struct SparkSequence {
    data_type: DataType,
    signature: Signature,
    pool: Arc<dyn MemoryPool>,
}

impl SparkSequence {
    pub fn new(data_type: DataType, pool: Arc<dyn MemoryPool>) -> Self {
        Self {
            data_type,
            // Admission depends on runtime pool availability. Folding to a list scalar would
            // let the caller expand it to an unreserved batch after this function returns.
            signature: Signature::variadic_any(Volatility::Volatile),
            pool,
        }
    }
}

impl PartialEq for SparkSequence {
    fn eq(&self, other: &Self) -> bool {
        self.data_type == other.data_type && Arc::ptr_eq(&self.pool, &other.pool)
    }
}
impl Eq for SparkSequence {}
impl Hash for SparkSequence {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.data_type.hash(state);
        (Arc::as_ptr(&self.pool) as *const ()).hash(state);
    }
}

impl ScalarUDFImpl for SparkSequence {
    fn name(&self) -> &str {
        "spark_sequence"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(self.data_type.clone())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        sequence(
            &args.args,
            &self.data_type,
            args.number_rows,
            Some(&self.pool),
        )
        .map(ColumnarValue::Array)
    }
}

/// Unpooled standalone kernel entry point, retained for kernel tests and benchmarks. Native
/// query plans must use SparkSequence so staged admission covers the complete requested batch.
pub fn spark_sequence(args: &[ColumnarValue], data_type: &DataType) -> Result<ColumnarValue> {
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
    let result = sequence(args, data_type, num_rows, None)?;
    if all_scalar {
        Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
            &result, 0,
        )?))
    } else {
        Ok(ColumnarValue::Array(result))
    }
}

fn sequence(
    args: &[ColumnarValue],
    data_type: &DataType,
    num_rows: usize,
    pool: Option<&Arc<dyn MemoryPool>>,
) -> Result<ArrayRef> {
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
    match child_field.data_type() {
        DataType::Int8 => sequence_integral::<Int8Type>(args, num_rows, child_field, pool),
        DataType::Int16 => sequence_integral::<Int16Type>(args, num_rows, child_field, pool),
        DataType::Int32 => sequence_integral::<Int32Type>(args, num_rows, child_field, pool),
        DataType::Int64 => sequence_integral::<Int64Type>(args, num_rows, child_field, pool),
        other => exec_err!("spark_sequence does not support element type {other:?}"),
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
    fn is_null(&self, row: usize) -> bool {
        match self {
            Self::Scalar(value) => value.is_none(),
            Self::Array(array) => array.is_null(row),
        }
    }
    fn null_count(&self, rows: usize) -> usize {
        match self {
            Self::Scalar(None) => rows,
            Self::Scalar(Some(_)) => 0,
            Self::Array(array) => array.null_count(),
        }
    }
    fn nulls(&self, len: usize) -> Option<NullBuffer> {
        match self {
            Self::Scalar(None) => Some(NullBuffer::new_null(len)),
            Self::Scalar(Some(_)) => None,
            Self::Array(array) => array
                .nulls()
                .filter(|nulls| nulls.null_count() != 0)
                .cloned(),
        }
    }
}

fn sequence_integral<T: Integral>(
    args: &[ColumnarValue],
    num_rows: usize,
    child_field: FieldRef,
    pool: Option<&Arc<dyn MemoryPool>>,
) -> Result<ArrayRef>
where
    T::Native: Into<i64>,
{
    let start = Input::<T>::new(&args[0])?;
    let stop = Input::<T>::new(&args[1])?;
    let step = args.get(2).map(Input::<T>::new).transpose()?;
    let null_counts = [
        start.null_count(num_rows),
        stop.null_count(num_rows),
        step.as_ref().map_or(0, |step| step.null_count(num_rows)),
    ];
    if num_rows == 0 || null_counts.contains(&num_rows) {
        // Metadata proves there are no values. Admit only offsets and one new null bitmap,
        // without inspecting values hidden by a null input.
        let bytes = construction_scratch(num_rows, false, usize::from(num_rows != 0))?;
        let _reservation = pool
            .map(|pool| reserve_construction(pool, bytes))
            .transpose()?;
        return null_sequence::<T>(child_field, num_rows, None);
    }
    let row_step = |row: usize, start: i64, stop: i64| -> i64 {
        match &step {
            Some(step) => step.value(row),
            None if start <= stop => 1,
            None => -1,
        }
    };
    let scalar_length = if args
        .iter()
        .all(|arg| matches!(arg, ColumnarValue::Scalar(_)))
    {
        let s = start.value(0);
        let e = stop.value(0);
        Some(sequence_length(s, e, row_step(0, s, e))?)
    } else {
        None
    };
    let scalar_total = scalar_length
        .map(|len| batch_length(len as u128 * num_rows as u128))
        .transpose()?;
    // Columns admit scratch first, then validate and cache each row length once before
    // growing this same reservation for values. Scalars know their complete size already.
    let initial = (|| {
        let bitmap_count = if null_counts.iter().any(|count| *count != 0) {
            2
        } else {
            0
        };
        let bytes = construction_scratch(num_rows, true, bitmap_count)?;
        let bytes = bytes
            .checked_add(value_bytes::<T>(scalar_total.unwrap_or(0))?)
            .ok_or_else(size_error)?;
        pool.map(|pool| reserve_construction(pool, bytes))
            .transpose()
    })();
    // Keep error-only typed parsing in the cold helper so it need not borrow hot input locals.
    let scratch_error = move |error| {
        // Only resource exhaustion needs an allocation-free validation pass to keep Spark
        // row errors and the batch element ceiling ahead of it. Propagate other pool errors,
        // such as an interrupted Spark task, immediately without inspecting more input rows.
        if scalar_length.is_none() && matches!(&error, DataFusionError::ResourcesExhausted(_)) {
            validate_before_resource_error::<T>(args, num_rows, error)
        } else {
            error
        }
    };
    let reservation = initial.map_err(scratch_error)?;
    // Reuse validity once per batch instead of checking three inputs and rebuilding a
    // bitmap row by row. Arrow unions sliced bitmaps with bulk word operations, allocating
    // one accumulator and applying the remaining masks in place.
    let input_nulls = [
        start.nulls(num_rows),
        stop.nulls(num_rows),
        step.as_ref().and_then(|step| step.nulls(num_rows)),
    ];
    let nulls = NullBuffer::union_many(input_nulls.iter().map(Option::as_ref))
        .filter(|nulls| nulls.null_count() != 0);
    if nulls
        .as_ref()
        .is_some_and(|nulls| nulls.null_count() == num_rows)
    {
        // Reuse the accumulator so another all-null bitmap cannot exceed its capacity allowance.
        return null_sequence::<T>(child_field, num_rows, nulls);
    }

    // Validate every row before checking the batch total, preserving the first Spark error.
    // Retain lengths so short sequences do not repeat per-row arithmetic during generation.
    let mut lengths = sequence_vec::<usize>(num_rows).map_err(scratch_error)?;
    let total = if let Some(len) = scalar_length {
        lengths.resize(num_rows, len);
        scalar_total.unwrap()
    } else {
        let mut total = 0u128;
        for row in 0..num_rows {
            if nulls.as_ref().is_some_and(|nulls| nulls.is_null(row)) {
                lengths.push(0);
                continue;
            }
            let s = start.value(row);
            let e = stop.value(row);
            let len = sequence_length(s, e, row_step(row, s, e))?;
            total += len as u128;
            lengths.push(len);
        }
        batch_length(total)?
    };
    if scalar_length.is_none() {
        if let Some(reservation) = &reservation {
            reservation.try_grow(value_bytes::<T>(total)?)?;
        }
    }

    let mut values = sequence_vec::<T::Native>(total)?;
    let slots = &mut values.spare_capacity_mut()[..total];
    let mut offsets = sequence_vec::<i32>(num_rows.checked_add(1).ok_or_else(size_error)?)?;
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

// Registration and reservation have the same lexical lifetime, including failed try_grow.
fn reserve_construction(pool: &Arc<dyn MemoryPool>, bytes: usize) -> Result<MemoryReservation> {
    let reservation = MemoryConsumer::new("Sequence").register(pool);
    reservation.try_grow(bytes)?;
    Ok(reservation)
}

fn size_error() -> DataFusionError {
    DataFusionError::ResourcesExhausted("Sequence construction size overflow".to_string())
}

fn bitmap_capacity(rows: usize) -> Result<usize> {
    // Round bitmap bytes to Arrow's 64-byte allocation boundary.
    Ok(rows.div_ceil(8).checked_add(63).ok_or_else(size_error)? & !63)
}

fn batch_length(total: u128) -> Result<usize> {
    if total > i32::MAX as u128 {
        return Err(DataFusionError::External(Box::new(
            SparkError::SequenceBatchTooLarge {
                total_elements: total.to_string(),
            },
        )));
    }
    Ok(total as usize)
}

fn value_bytes<T: Integral>(total: usize) -> Result<usize> {
    total
        .checked_mul(std::mem::size_of::<T::Native>())
        .ok_or_else(size_error)
}

fn construction_scratch(rows: usize, cache_lengths: bool, bitmap_count: usize) -> Result<usize> {
    let offsets = rows
        .checked_add(1)
        .and_then(|n| n.checked_mul(std::mem::size_of::<i32>()))
        .ok_or_else(size_error)?;
    let lengths = if cache_lengths {
        rows.checked_mul(std::mem::size_of::<usize>())
            .ok_or_else(size_error)?
    } else {
        0
    };
    // Bulk unions use one accumulator; two rounded bitmap sizes allow for capacity doubling.
    // The full backing allocation of a borrowed, possibly sliced input bitmap is not charged.
    let validity = bitmap_capacity(rows)?
        .checked_mul(bitmap_count)
        .ok_or_else(size_error)?;
    offsets
        .checked_add(lengths)
        .and_then(|n| n.checked_add(validity))
        .ok_or_else(size_error)
}

#[cold]
fn validate_before_resource_error<T: Integral>(
    args: &[ColumnarValue],
    rows: usize,
    resource_error: DataFusionError,
) -> DataFusionError
where
    T::Native: Into<i64>,
{
    let validation = (|| -> Result<usize> {
        let start = Input::<T>::new(&args[0])?;
        let stop = Input::<T>::new(&args[1])?;
        let step = args.get(2).map(Input::<T>::new).transpose()?;
        let mut total = 0u128;
        for row in 0..rows {
            if start.is_null(row)
                || stop.is_null(row)
                || step.as_ref().is_some_and(|step| step.is_null(row))
            {
                continue;
            }
            let s = start.value(row);
            let e = stop.value(row);
            let step = step
                .as_ref()
                .map_or_else(|| if s <= e { 1 } else { -1 }, |step| step.value(row));
            total += sequence_length(s, e, step)? as u128;
        }
        batch_length(total)
    })();
    validation.err().unwrap_or(resource_error)
}

fn sequence_vec<T>(capacity: usize) -> Result<Vec<T>> {
    let mut values = Vec::new();
    values.try_reserve_exact(capacity).map_err(|error| {
        DataFusionError::ResourcesExhausted(format!("Sequence allocation failed: {error}"))
    })?;
    Ok(values)
}

fn null_sequence<T: Integral>(
    field: FieldRef,
    rows: usize,
    existing_nulls: Option<NullBuffer>,
) -> Result<ArrayRef> {
    let count = rows.checked_add(1).ok_or_else(size_error)?;
    let mut offsets = sequence_vec::<i32>(count)?;
    offsets.resize(count, 0);
    let nulls = if existing_nulls.is_some() || rows == 0 {
        existing_nulls
    } else {
        let bytes = bitmap_capacity(rows)?;
        let mut validity = sequence_vec::<u8>(bytes)?;
        validity.resize(bytes, 0);
        Some(NullBuffer::new(BooleanBuffer::new(
            Buffer::from(validity),
            0,
            rows,
        )))
    };
    Ok(Arc::new(ListArray::try_new(
        field,
        OffsetBuffer::new(offsets.into()),
        Arc::new(PrimitiveArray::<T>::new(
            ScalarBuffer::from(Vec::<T::Native>::new()),
            None,
        )),
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

#[cfg(test)]
mod memory_tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::Field;
    use arrow::ffi::FFI_ArrowArray;
    use datafusion::common::config::ConfigOptions;
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryLimit, UnboundedMemoryPool};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Barrier;

    /// A real bounded pool with observations and an optional rendezvous after admission. The
    /// rendezvous makes concurrent construction overlap without changing admission decisions.
    #[derive(Debug)]
    struct ObservedPool {
        inner: GreedyMemoryPool,
        active: AtomicUsize,
        registrations: AtomicUsize,
        growths: AtomicUsize,
        peak: AtomicUsize,
        rendezvous: Option<Barrier>,
        external_failure: AtomicBool,
    }
    impl ObservedPool {
        fn new(limit: usize, concurrent: Option<usize>) -> Arc<Self> {
            Arc::new(Self {
                inner: GreedyMemoryPool::new(limit),
                active: AtomicUsize::new(0),
                registrations: AtomicUsize::new(0),
                growths: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
                rendezvous: concurrent.map(Barrier::new),
                external_failure: AtomicBool::new(false),
            })
        }
    }
    impl std::fmt::Display for ObservedPool {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            std::fmt::Display::fmt(&self.inner, f)
        }
    }
    impl MemoryPool for ObservedPool {
        fn name(&self) -> &str {
            "ObservedSequencePool"
        }
        fn register(&self, consumer: &MemoryConsumer) {
            self.inner.register(consumer);
            self.active.fetch_add(1, Ordering::Relaxed);
            self.registrations.fetch_add(1, Ordering::Relaxed);
        }
        fn unregister(&self, consumer: &MemoryConsumer) {
            self.inner.unregister(consumer);
            self.active.fetch_sub(1, Ordering::Relaxed);
        }
        fn grow(&self, _: &MemoryReservation, _: usize) {
            panic!("sequence must use try_grow");
        }
        fn shrink(&self, reservation: &MemoryReservation, bytes: usize) {
            self.inner.shrink(reservation, bytes);
        }
        fn try_grow(&self, reservation: &MemoryReservation, bytes: usize) -> Result<()> {
            self.growths.fetch_add(1, Ordering::Relaxed);
            if self.external_failure.swap(false, Ordering::Relaxed) {
                return Err(DataFusionError::External(Box::new(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "pool interrupted",
                ))));
            }
            let result = self.inner.try_grow(reservation, bytes);
            self.peak
                .fetch_max(self.inner.reserved(), Ordering::Relaxed);
            // Rendezvous only on initial admission, never on a later staged growth.
            if reservation.size() == 0 {
                if let Some(rendezvous) = &self.rendezvous {
                    rendezvous.wait();
                }
            }
            result
        }
        fn reserved(&self) -> usize {
            self.inner.reserved()
        }
        fn memory_limit(&self) -> MemoryLimit {
            self.inner.memory_limit()
        }
    }

    fn list_type() -> DataType {
        DataType::List(Arc::new(Field::new_list_field(DataType::Int64, false)))
    }
    fn scalars(start: Option<i64>, stop: Option<i64>, step: Option<i64>) -> Vec<ColumnarValue> {
        [start, stop, step]
            .into_iter()
            .map(|v| ColumnarValue::Scalar(ScalarValue::Int64(v)))
            .collect()
    }
    fn invoke(udf: &SparkSequence, args: Vec<ColumnarValue>, rows: usize) -> Result<ArrayRef> {
        let result = udf.invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields: vec![],
            number_rows: rows,
            return_field: Arc::new(Field::new("sequence", list_type(), true)),
            config_options: Arc::new(ConfigOptions::default()),
        })?;
        let ColumnarValue::Array(array) = result else {
            panic!("native sequence must return the full array");
        };
        assert_eq!(array.len(), rows);
        Ok(array)
    }
    fn assert_released(pool: &ObservedPool) {
        assert_eq!(pool.reserved(), 0);
        assert_eq!(pool.active.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn reserves_full_scalar_batch_and_releases_before_retained_output() {
        // Four rows of four i64 values, five offsets, and four cached lengths.
        let bytes = 4 * 4 * 8 + 5 * 4 + 4 * std::mem::size_of::<usize>();
        let pool = ObservedPool::new(bytes, None);
        let udf = SparkSequence::new(list_type(), Arc::clone(&pool) as _);
        assert_eq!(pool.registrations.load(Ordering::Relaxed), 0);
        let args = scalars(Some(1), Some(4), Some(1));
        let array = invoke(&udf, args.clone(), 4).unwrap();
        assert_eq!(pool.peak.load(Ordering::Relaxed), bytes);
        assert_released(&pool);
        let clone = Arc::clone(&array);
        let ffi = FFI_ArrowArray::new(&array.to_data());
        let next = invoke(&udf, args.clone(), 4).unwrap();
        assert_released(&pool);
        assert_eq!(array.to_data(), next.to_data());
        drop((array, clone, ffi, next));
        assert_released(&pool);
        let refused = ObservedPool::new(bytes - 1, None);
        let udf = SparkSequence::new(list_type(), Arc::clone(&refused) as _);
        assert!(matches!(
            invoke(&udf, args, 4),
            Err(DataFusionError::ResourcesExhausted(_))
        ));
        assert_released(&refused);
    }

    #[test]
    fn all_null_and_empty_outputs_are_admitted() {
        // All-null rows allocate only offsets and one bitmap rounded to Arrow's capacity.
        for (rows, bytes) in [(3, 4 * 4 + 64), (0, 4)] {
            let pool = ObservedPool::new(bytes, None);
            let udf = SparkSequence::new(list_type(), Arc::clone(&pool) as _);
            let array = invoke(&udf, scalars(None, Some(i64::MAX), Some(0)), rows).unwrap();
            assert_eq!(array.null_count(), rows);
            assert_eq!(pool.peak.load(Ordering::Relaxed), bytes);
            assert_released(&pool);
            let small = ObservedPool::new(bytes - 1, None);
            let udf = SparkSequence::new(list_type(), Arc::clone(&small) as _);
            assert!(matches!(
                invoke(&udf, scalars(None, None, None), rows),
                Err(DataFusionError::ResourcesExhausted(_))
            ));
            assert_released(&small);
        }
    }

    #[test]
    fn all_null_input_metadata_suppresses_invalid_underlying_values() {
        for null_input in 0..3 {
            let args = [10, 0, 1]
                .into_iter()
                .enumerate()
                .map(|(index, value)| {
                    // Non-null values would have illegal ascending boundaries. Slice the wholly
                    // null input too, so the shortcut relies on the logical array's metadata.
                    let validity = (index == null_input).then(|| NullBuffer::new_null(5));
                    let array = Int64Array::new(vec![value; 5].into(), validity).slice(1, 3);
                    ColumnarValue::Array(Arc::new(array))
                })
                .collect::<Vec<_>>();
            let pool = ObservedPool::new(4 * 4 + 64, None);
            let udf = SparkSequence::new(list_type(), Arc::clone(&pool) as _);
            let result = invoke(&udf, args, 3).unwrap();
            assert_eq!(result.null_count(), 3);
            assert_eq!(pool.peak.load(Ordering::Relaxed), 4 * 4 + 64);
            assert_released(&pool);
        }
    }

    #[test]
    fn sliced_validity_and_union_scratch_are_reserved() {
        let start = Int64Array::from(vec![Some(9), Some(1), None, Some(3), Some(4)]).slice(1, 4);
        let stop = Int64Array::from(vec![Some(9), Some(2), Some(3), None, Some(5)]).slice(1, 4);
        let args = vec![
            ColumnarValue::Array(Arc::new(start)),
            ColumnarValue::Array(Arc::new(stop)),
        ];
        let bytes = 4 * 8 + 5 * 4 + 4 * std::mem::size_of::<usize>() + 2 * 64;
        let pool = ObservedPool::new(bytes, None);
        let udf = SparkSequence::new(list_type(), Arc::clone(&pool) as _);
        let array = invoke(&udf, args.clone(), 4).unwrap();
        let expected = spark_sequence(&args, &list_type())
            .unwrap()
            .into_array(4)
            .unwrap();
        assert_eq!(array.to_data(), expected.to_data());
        assert_eq!(pool.peak.load(Ordering::Relaxed), bytes);
        assert_released(&pool);
    }

    #[test]
    fn sliced_validity_accumulator_capacity_and_values() {
        // Exercise byte and 64-byte bitmap boundaries, including a trailing chunk large
        // enough to double Arrow's capacity. Matching offsets need only one rounded bitmap.
        for rows in [7usize, 8, 9, 511, 512, 513, 2049, 8193] {
            for offsets in [[3usize; 3], [1, 6, 3], [8, 16, 24]] {
                let args = [1, 5, 2]
                    .into_iter()
                    .enumerate()
                    .map(|(input, value)| {
                        let offset = offsets[input];
                        let validity = NullBuffer::from(
                            (0..rows + offset)
                                .map(|row| row < offset || (row - offset) % 11 != input)
                                .collect::<Vec<_>>(),
                        );
                        let array =
                            Int64Array::new(vec![value; rows + offset].into(), Some(validity))
                                .slice(offset, rows);
                        ColumnarValue::Array(Arc::new(array))
                    })
                    .collect::<Vec<_>>();
                let valid_rows = (0..rows).filter(|row| row % 11 >= 3).count();
                let bitmap_bytes = rows.div_ceil(512) * 64;
                let bytes = (rows + 1) * 4
                    + rows * std::mem::size_of::<usize>()
                    + 2 * bitmap_bytes
                    + valid_rows * 3 * 8;
                let pool = ObservedPool::new(bytes, None);
                let udf = SparkSequence::new(list_type(), Arc::clone(&pool) as _);
                let array = invoke(&udf, args.clone(), rows).unwrap();
                assert_released(&pool);
                let lists = array.as_any().downcast_ref::<ListArray>().unwrap();
                let capacity = lists.nulls().unwrap().buffer().capacity();
                let capacity_limit = if offsets == [3; 3] {
                    (rows + 3).div_ceil(512) * 64
                } else {
                    2 * bitmap_bytes
                };
                assert!(
                    capacity <= capacity_limit,
                    "{rows} rows at offsets {offsets:?}: bitmap capacity {capacity} exceeds {capacity_limit}"
                );
                assert_eq!(lists.null_count(), rows - valid_rows);
                assert_eq!(lists.values().len(), valid_rows * 3);
                for row in 0..rows {
                    assert_eq!(lists.is_null(row), row % 11 < 3);
                    if lists.is_valid(row) {
                        let values = lists.value(row);
                        let values = values.as_any().downcast_ref::<Int64Array>().unwrap();
                        assert_eq!(values.values().as_ref(), &[1, 3, 5]);
                    }
                }
                assert_eq!(pool.peak.load(Ordering::Relaxed), bytes);
                let refused = ObservedPool::new(bytes - 1, None);
                let udf = SparkSequence::new(list_type(), Arc::clone(&refused) as _);
                assert!(matches!(
                    invoke(&udf, args, rows),
                    Err(DataFusionError::ResourcesExhausted(_))
                ));
                assert_released(&refused);
            }
        }
    }

    #[test]
    fn all_valid_bitmaps_do_not_add_construction_allocations() {
        let args = [1, 2, 1]
            .into_iter()
            .map(|value| {
                let array =
                    Int64Array::new(vec![value; 17].into(), Some(NullBuffer::new_valid(17)))
                        .slice(1, 16);
                assert!(array.nulls().is_some());
                ColumnarValue::Array(Arc::new(array))
            })
            .collect::<Vec<_>>();
        let bytes = 16 * 2 * 8 + 17 * 4 + 16 * std::mem::size_of::<usize>();
        let pool = ObservedPool::new(bytes, None);
        let udf = SparkSequence::new(list_type(), Arc::clone(&pool) as _);
        let result = invoke(&udf, args, 16).unwrap();
        assert!(result.nulls().is_none());
        assert_eq!(pool.peak.load(Ordering::Relaxed), bytes);
        assert_released(&pool);
    }

    #[test]
    fn existing_buffered_reservations_can_refuse_a_sequence_before_spilling() {
        let pool = ObservedPool::new(256, None);
        let dyn_pool = Arc::clone(&pool) as Arc<dyn MemoryPool>;
        let buffer = MemoryConsumer::new("buffering operator")
            .with_can_spill(true)
            .register(&dyn_pool);
        buffer.try_grow(128).unwrap();
        let udf = SparkSequence::new(list_type(), dyn_pool);
        let args = vec![
            ColumnarValue::Array(Arc::new(Int64Array::from(vec![1; 4]))),
            ColumnarValue::Array(Arc::new(Int64Array::from(vec![4; 4]))),
        ];
        // Scratch fits alongside the existing reservation; the later value growth does not.
        // The caller has not received this output yet, so its own try_grow/spill opportunity
        // cannot rescue a refusal in the upstream expression. No reclaim callback is implied.
        assert!(matches!(
            invoke(&udf, args.clone(), 4),
            Err(DataFusionError::ResourcesExhausted(_))
        ));
        assert_eq!(pool.growths.load(Ordering::Relaxed), 3); // buffer, scratch, values
        assert_eq!(pool.registrations.load(Ordering::Relaxed), 2);
        assert_eq!(
            pool.peak.load(Ordering::Relaxed),
            128 + 5 * 4 + 4 * std::mem::size_of::<usize>()
        );
        assert_eq!(buffer.size(), 128);
        assert_eq!(pool.reserved(), 128);
        assert_eq!(pool.active.load(Ordering::Relaxed), 1);
        drop(buffer);
        assert!(invoke(&udf, args, 4).is_ok());
        assert_released(&pool);
    }

    #[test]
    fn staged_column_refusal_releases_scratch_and_recovers() {
        let rows = 4;
        let scratch = (rows + 1) * 4 + rows * std::mem::size_of::<usize>();
        let args = vec![
            ColumnarValue::Array(Arc::new(Int64Array::from(vec![1; rows]))),
            ColumnarValue::Array(Arc::new(Int64Array::from(vec![4; rows]))),
        ];
        // Distinguish a first-stage refusal from a second-stage refusal after scratch was
        // granted. Neither leaves an idle registration or bytes behind.
        for (limit, growths, peak) in [(scratch - 1, 1, 0), (scratch + 32, 2, scratch)] {
            let pool = ObservedPool::new(limit, None);
            let udf = SparkSequence::new(list_type(), Arc::clone(&pool) as _);
            assert!(matches!(
                invoke(&udf, args.clone(), rows),
                Err(DataFusionError::ResourcesExhausted(_))
            ));
            assert_eq!(pool.growths.load(Ordering::Relaxed), growths);
            assert_eq!(pool.peak.load(Ordering::Relaxed), peak);
            assert_eq!(pool.registrations.load(Ordering::Relaxed), 1);
            assert_released(&pool);
            // Recovery uses the same UDF/pool and retains the successful output.
            let output = invoke(&udf, scalars(Some(1), Some(2), Some(1)), 1).unwrap();
            assert_eq!(output.len(), 1);
            assert_released(&pool);
        }
    }

    #[test]
    fn row_error_after_scratch_admission_releases_the_reservation() {
        let pool = ObservedPool::new(128, None);
        let udf = SparkSequence::new(list_type(), Arc::clone(&pool) as _);
        let args = vec![
            ColumnarValue::Array(Arc::new(Int64Array::from(vec![1, 2, 3]))),
            ColumnarValue::Array(Arc::new(Int64Array::from(vec![2, 3, 1]))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
        ];
        assert!(invoke(&udf, args, 3)
            .unwrap_err()
            .to_string()
            .contains("Illegal sequence boundaries"));
        assert_eq!(pool.growths.load(Ordering::Relaxed), 1);
        assert!(pool.peak.load(Ordering::Relaxed) > 0);
        assert_released(&pool);
        assert!(invoke(&udf, scalars(Some(1), Some(2), Some(1)), 1).is_ok());
        assert_released(&pool);
    }

    #[test]
    fn disjoint_null_masks_reuse_combined_validity_and_suppress_errors() {
        let args = [10, 0, 1]
            .into_iter()
            .enumerate()
            .map(|(input, value)| {
                // Every input is partially valid, but their sliced masks have no common valid
                // row. The underlying ascending bounds would be illegal without null suppression.
                let validity =
                    NullBuffer::from((0..5).map(|row| row != input + 1).collect::<Vec<_>>());
                let array = Int64Array::new(vec![value; 5].into(), Some(validity)).slice(1, 3);
                assert_eq!(array.null_count(), 1);
                ColumnarValue::Array(Arc::new(array))
            })
            .collect::<Vec<_>>();
        let bytes = 4 * 4 + 3 * std::mem::size_of::<usize>() + 2 * 64;
        let pool = ObservedPool::new(bytes, None);
        let udf = SparkSequence::new(list_type(), Arc::clone(&pool) as _);
        let result = invoke(&udf, args.clone(), 3).unwrap();
        assert_eq!(result.null_count(), 3);
        assert_eq!(
            result
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .values()
                .len(),
            0
        );
        assert_eq!(pool.growths.load(Ordering::Relaxed), 1);
        assert_eq!(pool.peak.load(Ordering::Relaxed), bytes);
        assert_released(&pool);
        // Initial refusal validates without allocating the union, respecting the same masks.
        let refused = ObservedPool::new(bytes - 1, None);
        let udf = SparkSequence::new(list_type(), Arc::clone(&refused) as _);
        assert!(matches!(
            invoke(&udf, args, 3),
            Err(DataFusionError::ResourcesExhausted(_))
        ));
        assert_released(&refused);
    }

    #[test]
    fn non_resource_pool_error_is_not_replaced_by_a_row_error() {
        let pool = ObservedPool::new(128, None);
        pool.external_failure.store(true, Ordering::Relaxed);
        let udf = SparkSequence::new(list_type(), Arc::clone(&pool) as _);
        let args = vec![
            ColumnarValue::Array(Arc::new(Int64Array::from(vec![2]))),
            ColumnarValue::Array(Arc::new(Int64Array::from(vec![1]))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
        ];
        // An exception from the pool must survive even when reading these bounds would
        // produce a Spark sequence error. Resource refusal has different precedence below.
        let error = invoke(&udf, args, 1).unwrap_err();
        let DataFusionError::External(error) = error else {
            panic!("expected the original external pool error");
        };
        let error = error.downcast_ref::<std::io::Error>().unwrap();
        assert_eq!(error.kind(), std::io::ErrorKind::Interrupted);
        assert_eq!(error.to_string(), "pool interrupted");
        assert_eq!(pool.growths.load(Ordering::Relaxed), 1);
        assert_eq!(pool.registrations.load(Ordering::Relaxed), 1);
        assert_released(&pool);
        let output = invoke(&udf, scalars(Some(1), Some(2), Some(1)), 1).unwrap();
        assert_eq!(output.len(), 1);
        assert_released(&pool);
    }

    #[test]
    fn row_errors_and_element_ceiling_precede_pool_refusal() {
        // Exercise both the cold validation after scratch refusal and the normal cached pass.
        for limit in [0, 128] {
            let pool = ObservedPool::new(limit, None);
            let udf = SparkSequence::new(list_type(), Arc::clone(&pool) as _);
            let args = vec![
                ColumnarValue::Array(Arc::new(Int64Array::from(vec![0, 0, 2]))),
                ColumnarValue::Array(Arc::new(Int64Array::from(vec![
                    MAX_ROUNDED_ARRAY_LENGTH - 1,
                    MAX_ROUNDED_ARRAY_LENGTH - 1,
                    1,
                ]))),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
            ];
            assert!(invoke(&udf, args, 3)
                .unwrap_err()
                .to_string()
                .contains("Illegal sequence boundaries"));
            let columns = vec![
                ColumnarValue::Array(Arc::new(Int64Array::from(vec![0, 0]))),
                ColumnarValue::Array(Arc::new(Int64Array::from(vec![1073741823, 1073741823]))),
            ];
            for (args, rows) in [
                (columns, 2),
                (scalars(Some(0), Some(262143), Some(1)), 8192),
            ] {
                let error = invoke(&udf, args, rows).unwrap_err().to_string();
                assert!(
                    error.contains("2147483648") && error.contains("spark.comet.batchSize"),
                    "{error}"
                );
                assert_released(&pool);
            }
            // Each column invocation registers once; the scalar ceiling is known beforehand.
            assert_eq!(pool.registrations.load(Ordering::Relaxed), 2);
        }
    }

    #[test]
    fn allocation_failure_and_unwind_release_registration_and_bytes() {
        let pool = ObservedPool::new(128, None);
        let dyn_pool = Arc::clone(&pool) as Arc<dyn MemoryPool>;
        let failure = (|| -> Result<()> {
            let _reservation = reserve_construction(&dyn_pool, 64)?;
            let _impossible = sequence_vec::<u64>(usize::MAX)?;
            Ok(())
        })();
        assert!(matches!(
            failure,
            Err(DataFusionError::ResourcesExhausted(_))
        ));
        assert_released(&pool);
        let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let reservation = reserve_construction(&dyn_pool, 64).unwrap();
            reservation.try_grow(64).unwrap();
            let mut partial = sequence_vec::<u64>(8).unwrap();
            partial.push(1);
            panic!("construction interrupted");
        }));
        assert!(unwind.is_err());
        assert_released(&pool);
        assert!(reserve_construction(&dyn_pool, 64).is_ok());
        assert_released(&pool);
    }

    #[test]
    fn concurrent_invocations_share_the_supplied_pool() {
        let bytes = 4 * 4 * 8 + 5 * 4 + 4 * std::mem::size_of::<usize>();
        let pool = ObservedPool::new(2 * bytes, Some(8));
        let udf = SparkSequence::new(list_type(), Arc::clone(&pool) as _);
        let outputs = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| scope.spawn(|| invoke(&udf, scalars(Some(1), Some(4), Some(1)), 4)))
                .collect();
            handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert_eq!(outputs.iter().filter(|output| output.is_ok()).count(), 2);
        assert_eq!(pool.peak.load(Ordering::Relaxed), 2 * bytes);
        assert_released(&pool);
    }

    #[test]
    fn unbounded_pool_keeps_the_existing_on_heap_policy() {
        let pool = Arc::new(UnboundedMemoryPool::default());
        let udf = SparkSequence::new(list_type(), Arc::clone(&pool) as _);
        let array = invoke(&udf, scalars(Some(1), Some(1024), Some(1)), 64).unwrap();
        assert_eq!(array.len(), 64);
        assert_eq!(pool.reserved(), 0);
    }
}
