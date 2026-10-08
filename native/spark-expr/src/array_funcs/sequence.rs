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

//! Spark-compatible integral sequence with whole-invocation admission.

use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, ListArray, PrimitiveArray};
use arrow::buffer::{BooleanBuffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, FieldRef, Int16Type, Int32Type, Int64Type, Int8Type,
};
use datafusion::common::cast::as_primitive_array;
use datafusion::common::{exec_err, DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_comet_jni_bridge::{check_exception, errors::CometError, JVMClasses};
use jni::objects::{Global, JObject};
use jni::signature::{Primitive, ReturnType};

use super::sequence_memory::{
    buffer_layout, default_sequence_pool, size_error, SequenceBuffer, SequenceMemoryPool,
};
use crate::SparkError;

/// Spark's ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH (Integer.MAX_VALUE - 15).
const MAX_ROUNDED_ARRAY_LENGTH: i64 = (i32::MAX - 15) as i64;
const CHECK_ROWS: usize = 4096;
const CHECK_VALUES: usize = 65_536;
type CancellationCheck = Arc<dyn Fn() -> Result<()> + Send + Sync>;

/// The result must remain an array of number_rows rows. An immutable UDF could be folded to a
/// list scalar whose subsequent broadcast allocates an unadmitted child buffer. Volatile here
/// describes admission/cancellation, even though the generated values themselves are deterministic.
pub struct SparkSequence {
    data_type: DataType,
    signature: Signature,
    pool: Arc<SequenceMemoryPool>,
    check: Option<CancellationCheck>,
}

impl SparkSequence {
    pub fn new(data_type: DataType, pool: Arc<SequenceMemoryPool>) -> Self {
        Self {
            data_type,
            signature: Signature::variadic_any(Volatility::Volatile),
            pool,
            check: None,
        }
    }

    /// Capture Spark's task lifecycle signal, already obtained on the executor task thread by
    /// createPlan. The method lookup is cached once per expression; execution never consults
    /// TaskContext.get(), which is unset on Tokio workers.
    pub fn with_task_context(
        mut self,
        context: Option<Arc<Global<JObject<'static>>>>,
    ) -> Result<Self> {
        if let Some(context) = context {
            let method = JVMClasses::with_env::<_, DataFusionError, _>(|env| {
                let class = env
                    .get_object_class(context.as_ref())
                    .map_err(CometError::from)?;
                Ok(env
                    .get_method_id(&class, jni::jni_str!("isInterrupted"), jni::jni_sig!("()Z"))
                    .map_err(CometError::from)?)
            })?;
            self.check = Some(Arc::new(move || {
                JVMClasses::with_env(|env| {
                    // SAFETY: method was resolved on this object's class with signature ()Z;
                    // the global object reference keeps the object and its class alive.
                    let value = unsafe {
                        env.call_method_unchecked(
                            context.as_ref(),
                            method,
                            ReturnType::Primitive(Primitive::Boolean),
                            &[],
                        )
                    };
                    if let Some(error) = check_exception(env)? {
                        return Err(error.into());
                    }
                    if value
                        .map_err(CometError::from)?
                        .z()
                        .map_err(CometError::from)?
                    {
                        return exec_err!(
                            "Integral sequence interrupted by Spark task cancellation"
                        );
                    }
                    Ok(())
                })
            }));
        }
        Ok(self)
    }

    pub fn evaluate(&self, args: &[ColumnarValue], number_rows: usize) -> Result<ColumnarValue> {
        sequence(
            args,
            &self.data_type,
            number_rows,
            &self.pool,
            self.check.as_ref(),
        )
    }
}

impl Debug for SparkSequence {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SparkSequence")
            .field("data_type", &self.data_type)
            .field("pool", &self.pool)
            .finish_non_exhaustive()
    }
}
impl PartialEq for SparkSequence {
    fn eq(&self, other: &Self) -> bool {
        self.data_type == other.data_type
            && Arc::ptr_eq(&self.pool, &other.pool)
            && match (&self.check, &other.check) {
                (None, None) => true,
                (Some(a), Some(b)) => Arc::ptr_eq(a, b),
                _ => false,
            }
    }
}
impl Eq for SparkSequence {}
impl Hash for SparkSequence {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.data_type.hash(state);
        Arc::as_ptr(&self.pool).hash(state);
        self.check
            .as_ref()
            .map(|check| Arc::as_ptr(check) as *const ())
            .hash(state);
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
        self.evaluate(&args.args, args.number_rows)
    }
}

/// Convenience entry point for array kernels. Scalar-only calls request one row; the physical
/// UDF uses its explicit number_rows instead. Always returns an array, never a broadcastable list
/// scalar. Scalar inputs are borrowed without allocating temporary broadcast arrays.
pub fn spark_sequence(args: &[ColumnarValue], data_type: &DataType) -> Result<ColumnarValue> {
    let rows = args
        .iter()
        .find_map(|arg| match arg {
            ColumnarValue::Array(array) => Some(array.len()),
            _ => None,
        })
        .unwrap_or(1);
    sequence(args, data_type, rows, default_sequence_pool(), None)
}

fn sequence(
    args: &[ColumnarValue],
    data_type: &DataType,
    rows: usize,
    pool: &Arc<SequenceMemoryPool>,
    check: Option<&CancellationCheck>,
) -> Result<ColumnarValue> {
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
            if array.len() != rows {
                return exec_err!(
                    "spark_sequence argument has {} rows, expected {rows}",
                    array.len()
                );
            }
        }
    }
    check_cancelled(check)?;
    let result = match child_field.data_type() {
        DataType::Int8 => sequence_integral::<Int8Type>(args, rows, child_field, pool, check),
        DataType::Int16 => sequence_integral::<Int16Type>(args, rows, child_field, pool, check),
        DataType::Int32 => sequence_integral::<Int32Type>(args, rows, child_field, pool, check),
        DataType::Int64 => sequence_integral::<Int64Type>(args, rows, child_field, pool, check),
        other => exec_err!("spark_sequence does not support element type {other:?}"),
    }?;
    Ok(ColumnarValue::Array(result))
}

fn check_cancelled(check: Option<&CancellationCheck>) -> Result<()> {
    if let Some(check) = check {
        check()?;
    }
    Ok(())
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
    fn is_null(&self, row: usize) -> bool {
        match self {
            Self::Scalar(value) => value.is_none(),
            Self::Array(array) => array.is_null(row),
        }
    }
    fn value(&self, row: usize) -> i64 {
        match self {
            Self::Scalar(value) => value.unwrap().into(),
            Self::Array(array) => array.value(row).into(),
        }
    }
    fn has_nulls(&self) -> bool {
        match self {
            Self::Scalar(value) => value.is_none(),
            Self::Array(array) => array.null_count() != 0,
        }
    }
}

#[derive(Clone, Copy, Default)]
struct Row {
    start: i64,
    step: i64,
    len: usize,
}

fn sequence_integral<T: Integral>(
    args: &[ColumnarValue],
    rows: usize,
    child_field: FieldRef,
    pool: &Arc<SequenceMemoryPool>,
    check: Option<&CancellationCheck>,
) -> Result<ArrayRef>
where
    T::Native: Into<i64>,
{
    let start = Input::<T>::new(&args[0])?;
    let stop = Input::<T>::new(&args[1])?;
    let step = args.get(2).map(Input::<T>::new).transpose()?;
    let has_nulls =
        start.has_nulls() || stop.has_nulls() || step.as_ref().is_some_and(Input::has_nulls);
    let row_bounds = |row| {
        if has_nulls
            && (start.is_null(row)
                || stop.is_null(row)
                || step.as_ref().is_some_and(|s| s.is_null(row)))
        {
            return None;
        }
        let start = start.value(row);
        let stop = stop.value(row);
        let step = step
            .as_ref()
            .map_or_else(|| if start <= stop { 1 } else { -1 }, |s| s.value(row));
        Some((start, stop, step))
    };
    let row_value = |row| -> Result<Row> {
        match row_bounds(row) {
            Some((start, stop, step)) => Ok(Row {
                start,
                step,
                len: sequence_length(start, stop, step)?,
            }),
            None => Ok(Row::default()),
        }
    };
    let scalar = args
        .iter()
        .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
    let repeated = if scalar && rows != 0 {
        Some(row_value(0)?)
    } else {
        None
    };

    // Constant-size scratch: validate every row in input order before checking either aggregate
    // limit. Recompute the cheap lengths while generating; no row-length Vec or output allocation
    // is made during sizing. u128 keeps the exact batch count even for huge scalar requests.
    let (total, nullable) = if let Some(row) = repeated {
        ((row.len as u128) * (rows as u128), row.len == 0)
    } else {
        let mut total = 0u128;
        let mut nullable = false;
        for row in 0..rows {
            if row != 0 && row % CHECK_ROWS == 0 {
                check_cancelled(check)?;
            }
            let value = row_value(row)?;
            total += value.len as u128;
            nullable |= value.len == 0;
        }
        (total, nullable)
    };
    if total > i32::MAX as u128 {
        return Err(DataFusionError::External(Box::new(
            SparkError::SequenceBatchTooLarge {
                total_elements: total.to_string(),
            },
        )));
    }
    let total = total as usize;
    let value_bytes = total
        .checked_mul(std::mem::size_of::<T::Native>())
        .ok_or_else(size_error)?;
    let offset_count = rows.checked_add(1).ok_or_else(size_error)?;
    let offset_bytes = offset_count
        .checked_mul(std::mem::size_of::<i32>())
        .ok_or_else(size_error)?;
    let null_bytes = if nullable { rows.div_ceil(8) } else { 0 };
    let value_layout = buffer_layout(value_bytes)?;
    let offset_layout = buffer_layout(offset_bytes)?;
    let null_layout = buffer_layout(null_bytes)?;
    let required = value_layout
        .size()
        .checked_add(offset_layout.size())
        .and_then(|n| n.checked_add(null_layout.size()))
        .ok_or_else(size_error)?;
    check_cancelled(check)?;
    let mut reservation = pool.reserve(required)?;
    let mut values = SequenceBuffer::new(value_layout, &mut reservation)?;
    let mut offsets = SequenceBuffer::new(offset_layout, &mut reservation)?;
    let mut validity = if nullable {
        Some(SequenceBuffer::new(null_layout, &mut reservation)?)
    } else {
        None
    };
    let validity_slots = validity
        .as_mut()
        .map(|buffer| buffer.slots::<u8>(null_bytes));
    // The bitmap is small relative to its offsets, but bound cancellation latency here too.
    if let Some(slots) = validity_slots {
        for chunk in slots.chunks_mut(CHECK_VALUES) {
            check_cancelled(check)?;
            for slot in chunk {
                slot.write(0);
            }
        }
    }
    let validity_slots = validity
        .as_mut()
        .map(|buffer| buffer.slots::<u8>(null_bytes));
    let value_slots = values.slots::<T::Native>(total);
    let offset_slots = offsets.slots::<i32>(offset_count);
    offset_slots[0].write(0);
    let mut written = 0;
    let mut until_check = CHECK_VALUES;
    let mut validity_slots = validity_slots;
    for row in 0..rows {
        if row % CHECK_ROWS == 0 {
            check_cancelled(check)?;
        }
        let value = match repeated {
            Some(value) => value,
            // The complete sizing pass already validated these immutable inputs. Avoid
            // repeating boundary/overflow checks and constructing a Result for each row.
            None => match row_bounds(row) {
                Some((start, stop, step)) => Row {
                    start,
                    step,
                    len: validated_sequence_length(start, stop, step),
                },
                None => Row::default(),
            },
        };
        if value.len != 0 {
            if let Some(slots) = &mut validity_slots {
                // SAFETY: the bitmap was initialised to zero above.
                let byte = unsafe { slots[row / 8].assume_init_mut() };
                *byte |= 1 << (row % 8);
            }
            let mut generated = 0;
            while generated < value.len {
                let count = (value.len - generated).min(until_check);
                // For repeated scalar rows, reuse the first generated row once its byte length
                // reaches Arrow's alignment. Tiny rows keep arithmetic to avoid a short memcpy.
                // Both paths write only the admitted allocation and use the same checkpoints.
                if repeated.is_some()
                    && row != 0
                    && value.len * std::mem::size_of::<T::Native>() >= arrow::alloc::ALIGNMENT
                {
                    let (prefix, output) = value_slots.split_at_mut(written);
                    output[..count].copy_from_slice(&prefix[generated..generated + count]);
                } else {
                    let first = value
                        .start
                        .wrapping_add(value.step.wrapping_mul(generated as i64));
                    // Fixed bounds and independent arithmetic let LLVM vectorise the stores;
                    // no push/capacity branches, null checks, or JNI calls occur in this loop.
                    for (i, slot) in value_slots[written..written + count].iter_mut().enumerate() {
                        slot.write(T::from_i64(
                            first.wrapping_add(value.step.wrapping_mul(i as i64)),
                        ));
                    }
                }
                written += count;
                generated += count;
                until_check -= count;
                if until_check == 0 {
                    check_cancelled(check)?;
                    until_check = CHECK_VALUES;
                }
            }
        }
        offset_slots[row + 1].write(written as i32);
    }
    check_cancelled(check)?;
    // SAFETY: every value and offset slot was written exactly once; all validity bytes were
    // initialised before setting bits. Errors and panics before here drop all partial allocations.
    let values = unsafe { values.finish(value_bytes) };
    let offsets = unsafe { offsets.finish(offset_bytes) };
    let nulls = validity.map(|buffer| {
        let buffer = unsafe { buffer.finish(null_bytes) };
        NullBuffer::new(BooleanBuffer::new(buffer, 0, rows))
    });
    let values = PrimitiveArray::<T>::new(ScalarBuffer::new(values, 0, total), None);
    Ok(Arc::new(ListArray::try_new(
        child_field,
        OffsetBuffer::new(ScalarBuffer::new(offsets, 0, offset_count)),
        Arc::new(values),
        nulls,
    )?))
}

/// Match released Spark 3.4--4.1 Sequence.sequenceLength, including its wide-arithmetic
/// fallback error when subtraction overflows even though the resulting length would fit.
#[inline]
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

/// Only valid after sequence_length succeeded for the same immutable inputs. Success proves
/// subtraction fits i64, excludes MIN / -1, and bounds the quotient and final length. A zero
/// step is valid only for equal bounds, handled before division.
#[inline]
fn validated_sequence_length(start: i64, stop: i64, step: i64) -> usize {
    if start == stop {
        return 1;
    }
    let delta = stop.wrapping_sub(start);
    let quotient = match step {
        1 => delta,
        -1 => delta.wrapping_neg(),
        _ => delta / step,
    };
    quotient as usize + 1
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
        let ColumnarValue::Array(array) = result else {
            panic!("scalar inputs must remain an admitted array")
        };
        assert_eq!(row_values(&as_primitive_list(&array), 0), vec![1, 2, 3]);
    }

    fn scalar_args(start: Option<i64>, stop: Option<i64>, step: Option<i64>) -> Vec<ColumnarValue> {
        vec![start, stop, step]
            .into_iter()
            .map(|value| ColumnarValue::Scalar(ScalarValue::Int64(value)))
            .collect()
    }

    fn evaluate(
        pool: &Arc<SequenceMemoryPool>,
        args: &[ColumnarValue],
        rows: usize,
    ) -> Result<ArrayRef> {
        let ColumnarValue::Array(array) =
            SparkSequence::new(list_of(DataType::Int64), Arc::clone(pool)).evaluate(args, rows)?
        else {
            panic!("expected array")
        };
        Ok(array)
    }

    #[test]
    fn byte_admission_includes_offsets_validity_and_rounding() {
        let args = scalar_args(Some(1), Some(3), Some(1));
        let bytes = buffer_layout(24 * 7).unwrap().size() + buffer_layout(4 * 8).unwrap().size();
        let too_small = SequenceMemoryPool::new(bytes - 1);
        assert!(matches!(
            evaluate(&too_small, &args, 7),
            Err(DataFusionError::ResourcesExhausted(_))
        ));
        assert_eq!(too_small.reserved(), 0);
        let pool = SequenceMemoryPool::new(bytes);
        let array = evaluate(&pool, &args, 7).unwrap();
        assert_eq!(array.len(), 7);
        assert_eq!(pool.reserved(), bytes);
        for row in 0..7 {
            assert_eq!(row_values(&as_primitive_list(&array), row), [1, 2, 3]);
        }
        assert!(evaluate(&pool, &args, 1).is_err());
        drop(array);
        assert_eq!(pool.reserved(), 0);
        assert!(evaluate(&pool, &args, 7).is_ok());

        let null_args = scalar_args(None, Some(5), Some(0));
        let bytes = buffer_layout(4 * 10).unwrap().size() + buffer_layout(2).unwrap().size();
        let pool = SequenceMemoryPool::new(bytes);
        let array = evaluate(&pool, &null_args, 9).unwrap();
        assert_eq!(array.null_count(), 9);
        assert_eq!(pool.reserved(), bytes);
        assert_eq!(as_primitive_list(&array).values().len(), 0);
        drop(array);
        assert_eq!(pool.reserved(), 0);
        assert!(evaluate(&SequenceMemoryPool::new(bytes - 1), &null_args, 9).is_err());
    }

    #[test]
    fn buffers_follow_clones_sliced_children_and_ffi_owners() {
        use arrow::ffi::FFI_ArrowArray;
        let pool = SequenceMemoryPool::new(4096);
        let args = scalar_args(Some(1), Some(3), Some(1));
        let array = evaluate(&pool, &args, 7).unwrap();
        let total = pool.reserved();
        let clone = Arc::clone(&array);
        let child = as_primitive_list(&array).value(3).slice(1, 1);
        let ffi = FFI_ArrowArray::new(&array.to_data());
        drop(array);
        drop(clone);
        assert_eq!(pool.reserved(), total);
        drop(ffi);
        assert_eq!(pool.reserved(), buffer_layout(7 * 3 * 8).unwrap().size());
        assert_eq!(
            as_primitive_array::<Int64Type>(&child).unwrap().values(),
            &[2]
        );
        drop(child);
        assert_eq!(pool.reserved(), 0);

        // An export of a null list owns its separate validity allocation too.
        let array = evaluate(&pool, &scalar_args(None, Some(5), Some(0)), 9).unwrap();
        let ffi = FFI_ArrowArray::new(&array.to_data());
        let total = pool.reserved();
        drop(array);
        assert_eq!(pool.reserved(), total);
        drop(ffi);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn empty_and_mismatched_inputs_allocate_no_values() {
        let pool = SequenceMemoryPool::new(1024);
        // Invalid scalar boundaries are never evaluated for an empty invocation.
        let empty = evaluate(&pool, &scalar_args(Some(1), Some(5), Some(0)), 0).unwrap();
        assert!(empty.is_empty());
        assert_eq!(pool.reserved(), buffer_layout(4).unwrap().size());
        drop(empty);
        let args = vec![
            ColumnarValue::Array(Arc::new(Int64Array::from(vec![1, 2]))),
            ColumnarValue::Array(Arc::new(Int64Array::from(vec![3]))),
        ];
        assert!(evaluate(&pool, &args, 2)
            .unwrap_err()
            .to_string()
            .contains("expected 2"));
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn row_errors_precede_batch_and_byte_limits() {
        let pool = SequenceMemoryPool::new(0);
        let args = vec![
            ColumnarValue::Array(Arc::new(Int64Array::from(vec![0, 0, 1]))),
            ColumnarValue::Array(Arc::new(Int64Array::from(vec![
                MAX_ROUNDED_ARRAY_LENGTH - 1,
                MAX_ROUNDED_ARRAY_LENGTH - 1,
                5,
            ]))),
            ColumnarValue::Array(Arc::new(Int64Array::from(vec![1, 1, -1]))),
        ];
        assert!(evaluate(&pool, &args, 3)
            .unwrap_err()
            .to_string()
            .contains("Illegal sequence boundaries"));
        let args = scalar_args(Some(0), Some(262143), Some(1));
        let error = evaluate(&pool, &args, 8192).unwrap_err().to_string();
        assert!(
            error.contains("2147483648") && error.contains("spark.comet.batchSize"),
            "{error}"
        );
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn all_integral_widths_preserve_bounds_and_direction() {
        macro_rules! verify {
            ($ty:ty, $variant:ident, $native:ty) => {{
                let data_type = list_of(<$ty>::DATA_TYPE);
                let pool = SequenceMemoryPool::new(4096);
                for (start, stop, step) in [
                    (<$native>::MIN, <$native>::MIN + 3, 1),
                    (<$native>::MAX, <$native>::MAX - 3, -1),
                    (0, 0, 0),
                    (-3, 4, 2),
                ] {
                    let args = [start, stop, step]
                        .into_iter()
                        .map(|value| ColumnarValue::Scalar(ScalarValue::$variant(Some(value))))
                        .collect::<Vec<_>>();
                    let ColumnarValue::Array(array) =
                        SparkSequence::new(data_type.clone(), Arc::clone(&pool))
                            .evaluate(&args, 5)
                            .unwrap()
                    else {
                        panic!("expected array")
                    };
                    let list = as_primitive_list(&array);
                    let expected_len =
                        sequence_length(start as i64, stop as i64, step as i64).unwrap();
                    for row in 0..5 {
                        let values = list.value(row);
                        let values = as_primitive_array::<$ty>(&values).unwrap();
                        assert_eq!(values.len(), expected_len);
                        for (i, actual) in values.values().iter().enumerate() {
                            assert_eq!(*actual as i128, start as i128 + step as i128 * i as i128);
                        }
                    }
                }
                assert_eq!(pool.reserved(), 0);
            }};
        }
        verify!(Int8Type, Int8, i8);
        verify!(Int16Type, Int16, i16);
        verify!(Int32Type, Int32, i32);
        verify!(Int64Type, Int64, i64);
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
                    if let Ok(len) = &result {
                        assert_eq!(validated_sequence_length(start, stop, step), *len);
                    }
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

    #[test]
    fn cancellation_and_unwind_return_partial_allocations() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        // The first shape interrupts arithmetic within its first row. The second interrupts
        // the copy of its second row, after exactly CHECK_VALUES generated values.
        for (stop, rows, panic) in [
            (CHECK_VALUES * 3, 1, false),
            (CHECK_VALUES * 3, 1, true),
            (CHECK_VALUES / 2 - 1, 3, false),
            (CHECK_VALUES / 2 - 1, 3, true),
        ] {
            let pool = SequenceMemoryPool::new(8 * CHECK_VALUES * 4);
            let count = Arc::new(AtomicUsize::new(0));
            let counter = Arc::clone(&count);
            let mut udf = SparkSequence::new(list_of(DataType::Int64), Arc::clone(&pool));
            udf.check = Some(Arc::new(move || {
                // Initial check, end of sizing, start of generation, then the first chunk.
                if counter.fetch_add(1, Ordering::SeqCst) == 3 {
                    if panic {
                        panic!("cancelled while filling output");
                    }
                    return exec_err!("cancelled while filling output");
                }
                Ok(())
            }));
            let args = scalar_args(Some(0), Some(stop as i64), Some(1));
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                udf.evaluate(&args, rows)
            }));
            if panic {
                assert!(result.is_err());
            } else {
                assert!(result
                    .unwrap()
                    .unwrap_err()
                    .to_string()
                    .contains("cancelled"));
            }
            assert_eq!(count.load(Ordering::SeqCst), 4);
            assert_eq!(pool.reserved(), 0);
            assert!(evaluate(&pool, &args, rows).is_ok());
        }
    }

    #[test]
    fn sizing_checks_cancellation_before_any_allocation() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let pool = SequenceMemoryPool::new(0);
        let count = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&count);
        let mut udf = SparkSequence::new(list_of(DataType::Int64), Arc::clone(&pool));
        udf.check = Some(Arc::new(move || {
            if counter.fetch_add(1, Ordering::SeqCst) == 2 {
                return exec_err!("cancelled during sizing");
            }
            Ok(())
        }));
        let rows = 3 * CHECK_ROWS;
        let args = vec![
            ColumnarValue::Array(Arc::new(Int64Array::from(vec![None; rows]))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
        ];
        assert!(udf
            .evaluate(&args, rows)
            .unwrap_err()
            .to_string()
            .contains("cancelled during sizing"));
        assert_eq!(count.load(Ordering::SeqCst), 3);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn concurrent_outputs_compete_for_one_allowance() {
        use std::sync::Barrier;
        let bytes =
            buffer_layout(16 * 10 * 8).unwrap().size() + buffer_layout(17 * 4).unwrap().size();
        let pool = SequenceMemoryPool::new(2 * bytes);
        let start = Barrier::new(8);
        let held = Barrier::new(8);
        let args = scalar_args(Some(1), Some(10), Some(1));
        let admitted = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| {
                    scope.spawn(|| {
                        start.wait();
                        let output = evaluate(&pool, &args, 16);
                        held.wait();
                        output.is_ok()
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .filter(|ok| *ok)
                .count()
        });
        assert_eq!(admitted, 2);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn repeated_scalar_rows_copy_across_cancellation_chunks() {
        macro_rules! verify {
            ($ty:ty, $variant:ident) => {{
                let pool = SequenceMemoryPool::new(1024 * 1024);
                let udf = SparkSequence::new(list_of(<$ty>::DATA_TYPE), Arc::clone(&pool));
                let args = [-64, 64, 1]
                    .into_iter()
                    .map(|value| ColumnarValue::Scalar(ScalarValue::$variant(Some(value))))
                    .collect::<Vec<_>>();
                // 129 elements exceed both 64- and 128-byte Arrow alignment even for Int8.
                // 129,000 values cross a checkpoint within a copied row, in every width.
                let ColumnarValue::Array(array) = udf.evaluate(&args, 1000).unwrap() else {
                    panic!("expected array")
                };
                let list = as_primitive_list(&array);
                for row in 0..1000 {
                    let values = list.value(row);
                    let values = as_primitive_array::<$ty>(&values).unwrap();
                    assert_eq!(values.len(), 129);
                    for (i, value) in values.values().iter().enumerate() {
                        assert_eq!(*value as i64, i as i64 - 64);
                    }
                }
                drop(list);
                drop(array);
                assert_eq!(pool.reserved(), 0);
            }};
        }
        verify!(Int8Type, Int8);
        verify!(Int16Type, Int16);
        verify!(Int32Type, Int32);
        verify!(Int64Type, Int64);
    }
}
