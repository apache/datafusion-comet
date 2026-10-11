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

use crate::{arithmetic_overflow_error, EvalMode};
use arrow::array::{
    as_primitive_array, cast::AsArray, Array, ArrayRef, ArrowNativeTypeOp, ArrowPrimitiveType,
    BooleanArray, Int64Array, PrimitiveArray,
};
use arrow::datatypes::{
    ArrowNativeType, DataType, Field, FieldRef, Int16Type, Int32Type, Int64Type, Int8Type,
};
use datafusion::common::{not_impl_err, DataFusionError, Result as DFResult, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::Volatility::Immutable;
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, EmitTo, GroupsAccumulator, ReversedUDAF, Signature,
};
use std::collections::VecDeque;
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SumInteger {
    signature: Signature,
    eval_mode: EvalMode,
}

impl SumInteger {
    pub fn try_new(data_type: DataType, eval_mode: EvalMode) -> DFResult<Self> {
        match data_type {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => Ok(Self {
                signature: Signature::user_defined(Immutable),
                eval_mode,
            }),
            _ => Err(DataFusionError::Internal(
                "Invalid data type for SumInteger".into(),
            )),
        }
    }
}

impl AggregateUDFImpl for SumInteger {
    fn name(&self) -> &str {
        "sum"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        match self.eval_mode {
            EvalMode::Legacy => Ok(Box::new(SumIntegerAccumulatorLegacy::new())),
            EvalMode::Ansi => Ok(Box::new(SumIntegerAccumulatorAnsi::new())),
            EvalMode::Try => Ok(Box::new(SumIntegerAccumulatorTry::new())),
        }
    }

    fn create_sliding_accumulator(&self, _args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        Ok(Box::new(SlidingSumIntegerAccumulator::new(self.eval_mode)))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> DFResult<Vec<FieldRef>> {
        if self.eval_mode == EvalMode::Try {
            Ok(vec![
                Arc::new(Field::new("sum", DataType::Int64, true)),
                Arc::new(Field::new("has_all_nulls", DataType::Boolean, false)),
            ])
        } else {
            Ok(vec![Arc::new(Field::new("sum", DataType::Int64, true))])
        }
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> DFResult<Box<dyn GroupsAccumulator>> {
        match self.eval_mode {
            EvalMode::Legacy => Ok(Box::new(SumIntGroupsAccumulatorLegacy::new())),
            EvalMode::Ansi => Ok(Box::new(SumIntGroupsAccumulatorAnsi::new())),
            EvalMode::Try => Ok(Box::new(SumIntGroupsAccumulatorTry::new())),
        }
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        // Checked addition depends on input order: [MAX, 1, -1] overflows,
        // while [-1, 1, MAX] does not. Reversing a frame must not change that.
        if self.eval_mode == EvalMode::Legacy {
            ReversedUDAF::Identical
        } else {
            ReversedUDAF::NotSupported
        }
    }
}

/// Spark recomputes each sliding frame in input order. Checking only its final
/// sum misses an intermediate overflow followed by cancellation. Track the min
/// and max prefix sums instead: subtracting the prefix before the frame gives
/// every partial sum Spark would visit. Monotonic queues retain just the extrema
/// candidates; each entry is pushed/popped once, for amortized O(1) work per row.
///
/// A partial sum is bounded above by the positive sum and below by the negative
/// sum of retained values, including update-before-retract overlap.
/// Only enqueue a prefix when that bound exceeds the corresponding i64 limit.
/// A skipped prefix is safe for every later frame too: moving the left boundary
/// forward only removes values from those bounds.
/// Ordinary values therefore need no queue allocation.
///
/// Worst-case queue space is still linear in the largest non-null frame,
/// including update-before-retract overlap. A suffix frame can span the entire
/// partition. Each entry occupies 32 bytes on a 64-bit host, and VecDeque retains
/// its grown capacity after retraction. size() reports this capacity, but
/// DataFusion 55.1's window operators do not account for accumulator sizes.
///
/// i128 holds the exact sum of at most usize::MAX i64 values on a 64-bit host.
/// Nulls need no entries because retraction receives the original input values.
#[derive(Debug)]
struct SlidingSumIntegerAccumulator {
    eval_mode: EvalMode,
    end: i128,
    start: i128,
    positive_sum: i128,
    added: usize,
    removed: usize,
    minima: VecDeque<(usize, i128)>,
    maxima: VecDeque<(usize, i128)>,
}

impl SlidingSumIntegerAccumulator {
    fn new(eval_mode: EvalMode) -> Self {
        Self {
            eval_mode,
            end: 0,
            start: 0,
            positive_sum: 0,
            added: 0,
            removed: 0,
            minima: VecDeque::new(),
            maxima: VecDeque::new(),
        }
    }

    fn add(&mut self, value: i64) {
        self.end += i128::from(value);
        self.positive_sum += i128::from(value.max(0));
        self.added += 1;
        while self.minima.back().is_some_and(|&(_, v)| v >= self.end) {
            self.minima.pop_back();
        }
        while self.maxima.back().is_some_and(|&(_, v)| v <= self.end) {
            self.maxima.pop_back();
        }
        // Keep the dominance pops above even for a skipped prefix. It also
        // bounds any older entries it dominates in every future frame.
        let negative_sum = self.end - self.start - self.positive_sum;
        if negative_sum < i128::from(i64::MIN) {
            self.minima.push_back((self.added, self.end));
        }
        if self.positive_sum > i128::from(i64::MAX) {
            self.maxima.push_back((self.added, self.end));
        }
    }

    fn remove(&mut self, value: i64) {
        self.start += i128::from(value);
        self.positive_sum -= i128::from(value.max(0));
        self.removed += 1;
    }

    fn visit(&mut self, values: &ArrayRef, retract: bool) -> DFResult<()> {
        fn visit<T: ArrowPrimitiveType>(
            acc: &mut SlidingSumIntegerAccumulator,
            values: &PrimitiveArray<T>,
            retract: bool,
        ) -> DFResult<()> {
            for value in values.iter().flatten() {
                let value = value.to_i64().ok_or_else(|| {
                    DataFusionError::Internal("Expected an integral SUM input".into())
                })?;
                if retract {
                    acc.remove(value);
                } else {
                    acc.add(value);
                }
            }
            Ok(())
        }
        match values.data_type() {
            DataType::Int8 => visit(self, as_primitive_array::<Int8Type>(values), retract),
            DataType::Int16 => visit(self, as_primitive_array::<Int16Type>(values), retract),
            DataType::Int32 => visit(self, as_primitive_array::<Int32Type>(values), retract),
            DataType::Int64 => visit(self, as_primitive_array::<Int64Type>(values), retract),
            other => not_impl_err!("Sliding integer SUM does not support {other}"),
        }
    }
}

impl Accumulator for SlidingSumIntegerAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        // DataFusion updates before retracting. Defer overflow checks until
        // evaluate(), when the accumulator represents the actual output frame.
        self.visit(&values[0], false)
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        self.visit(&values[0], true)?;
        while self.minima.front().is_some_and(|&(i, _)| i <= self.removed) {
            self.minima.pop_front();
        }
        while self.maxima.front().is_some_and(|&(i, _)| i <= self.removed) {
            self.maxima.pop_front();
        }
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        if self.added == self.removed {
            return Ok(ScalarValue::Int64(None));
        }
        let overflow = self
            .minima
            .front()
            .is_some_and(|&(_, v)| v - self.start < i128::from(i64::MIN))
            || self
                .maxima
                .front()
                .is_some_and(|&(_, v)| v - self.start > i128::from(i64::MAX));
        if overflow && self.eval_mode != EvalMode::Legacy {
            return match self.eval_mode {
                EvalMode::Ansi => Err(arithmetic_overflow_error("integer").into()),
                _ => Ok(ScalarValue::Int64(None)),
            };
        }
        Ok(ScalarValue::Int64(Some((self.end - self.start) as i64)))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + (self.minima.capacity() + self.maxima.capacity())
                * std::mem::size_of::<(usize, i128)>()
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        not_impl_err!("Sliding integer SUM is a window accumulator")
    }

    fn merge_batch(&mut self, _states: &[ArrayRef]) -> DFResult<()> {
        not_impl_err!("Sliding integer SUM does not merge partial aggregates")
    }
}

#[derive(Debug)]
struct SumIntegerAccumulatorLegacy {
    sum: Option<i64>,
}

impl SumIntegerAccumulatorLegacy {
    fn new() -> Self {
        Self { sum: None }
    }
}

impl Accumulator for SumIntegerAccumulatorLegacy {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        fn update_sum<T>(int_array: &PrimitiveArray<T>, mut sum: i64) -> DFResult<i64>
        where
            T: ArrowPrimitiveType,
        {
            for i in 0..int_array.len() {
                if !int_array.is_null(i) {
                    let v = int_array.value(i).to_i64().ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "Failed to convert value {:?} to i64",
                            int_array.value(i)
                        ))
                    })?;
                    sum = v.add_wrapping(sum);
                }
            }
            Ok(sum)
        }

        let values = &values[0];
        if values.len() == values.null_count() {
            return Ok(());
        }

        let running_sum = self.sum.unwrap_or(0);
        let sum = match values.data_type() {
            DataType::Int64 => update_sum(as_primitive_array::<Int64Type>(values), running_sum)?,
            DataType::Int32 => update_sum(as_primitive_array::<Int32Type>(values), running_sum)?,
            DataType::Int16 => update_sum(as_primitive_array::<Int16Type>(values), running_sum)?,
            DataType::Int8 => update_sum(as_primitive_array::<Int8Type>(values), running_sum)?,
            _ => {
                return Err(DataFusionError::Internal(format!(
                    "unsupported data type: {:?}",
                    values.data_type()
                )));
            }
        };
        self.sum = Some(sum);
        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        Ok(ScalarValue::Int64(self.sum))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Int64(self.sum)])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        // Merging partial sums is the same as summing values
        self.update_batch(states)
    }
}

#[derive(Debug)]
struct SumIntegerAccumulatorAnsi {
    sum: Option<i64>,
}

impl SumIntegerAccumulatorAnsi {
    fn new() -> Self {
        Self { sum: None }
    }
}

impl Accumulator for SumIntegerAccumulatorAnsi {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        fn update_sum<T>(int_array: &PrimitiveArray<T>, mut sum: i64) -> DFResult<i64>
        where
            T: ArrowPrimitiveType,
        {
            for i in 0..int_array.len() {
                if !int_array.is_null(i) {
                    let v = int_array.value(i).to_i64().ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "Failed to convert value {:?} to i64",
                            int_array.value(i)
                        ))
                    })?;
                    sum = v
                        .add_checked(sum)
                        .map_err(|_| DataFusionError::from(arithmetic_overflow_error("integer")))?;
                }
            }
            Ok(sum)
        }

        let values = &values[0];
        if values.len() == values.null_count() {
            return Ok(());
        }

        let running_sum = self.sum.unwrap_or(0);
        let sum = match values.data_type() {
            DataType::Int64 => update_sum(as_primitive_array::<Int64Type>(values), running_sum)?,
            DataType::Int32 => update_sum(as_primitive_array::<Int32Type>(values), running_sum)?,
            DataType::Int16 => update_sum(as_primitive_array::<Int16Type>(values), running_sum)?,
            DataType::Int8 => update_sum(as_primitive_array::<Int8Type>(values), running_sum)?,
            _ => {
                return Err(DataFusionError::Internal(format!(
                    "unsupported data type: {:?}",
                    values.data_type()
                )));
            }
        };
        self.sum = Some(sum);
        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        Ok(ScalarValue::Int64(self.sum))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Int64(self.sum)])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        // Merging partial sums is the same as summing values
        self.update_batch(states)
    }
}

#[derive(Debug)]
struct SumIntegerAccumulatorTry {
    sum: Option<i64>,
    has_all_nulls: bool,
}

impl SumIntegerAccumulatorTry {
    fn new() -> Self {
        Self {
            // Try mode starts with 0 (because if this is init to None we cant say if it is none due to all nulls or due to an overflow)
            sum: Some(0),
            has_all_nulls: true,
        }
    }

    fn overflowed(&self) -> bool {
        !self.has_all_nulls && self.sum.is_none()
    }
}

impl Accumulator for SumIntegerAccumulatorTry {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        /// Returns Ok(Some(sum)) on success, Ok(None) on overflow
        fn update_sum<T>(int_array: &PrimitiveArray<T>, mut sum: i64) -> DFResult<Option<i64>>
        where
            T: ArrowPrimitiveType,
        {
            for i in 0..int_array.len() {
                if !int_array.is_null(i) {
                    let v = int_array.value(i).to_i64().ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "Failed to convert value {:?} to i64",
                            int_array.value(i)
                        ))
                    })?;
                    match v.add_checked(sum) {
                        Ok(new_sum) => sum = new_sum,
                        Err(_) => return Ok(None),
                    }
                }
            }
            Ok(Some(sum))
        }

        // Skip if we already saw an overflow
        if self.overflowed() {
            return Ok(());
        }

        let values = &values[0];
        if values.len() == values.null_count() {
            return Ok(());
        }

        let running_sum = self.sum.unwrap_or(0);
        let sum = match values.data_type() {
            DataType::Int64 => update_sum(as_primitive_array::<Int64Type>(values), running_sum)?,
            DataType::Int32 => update_sum(as_primitive_array::<Int32Type>(values), running_sum)?,
            DataType::Int16 => update_sum(as_primitive_array::<Int16Type>(values), running_sum)?,
            DataType::Int8 => update_sum(as_primitive_array::<Int8Type>(values), running_sum)?,
            _ => {
                return Err(DataFusionError::Internal(format!(
                    "unsupported data type: {:?}",
                    values.data_type()
                )));
            }
        };
        self.sum = sum;
        self.has_all_nulls = false;
        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        if self.has_all_nulls {
            Ok(ScalarValue::Int64(None))
        } else {
            Ok(ScalarValue::Int64(self.sum))
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::Int64(self.sum),
            ScalarValue::Boolean(Some(self.has_all_nulls)),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        if states.len() != 2 {
            return Err(DataFusionError::Internal(format!(
                "Invalid state while merging batch. Expected 2 elements but found {}",
                states.len()
            )));
        }

        let that_sum_array = states[0].as_primitive::<Int64Type>();
        let that_has_all_nulls_array = states[1].as_boolean();

        for row in 0..that_sum_array.len() {
            if self.overflowed() {
                return Ok(());
            }

            let that_sum = if that_sum_array.is_null(row) {
                None
            } else {
                Some(that_sum_array.value(row))
            };
            let that_has_all_nulls = that_has_all_nulls_array.value(row);

            let that_overflowed = !that_has_all_nulls && that_sum.is_none();
            if that_overflowed {
                self.sum = None;
                self.has_all_nulls = false;
                return Ok(());
            }

            if that_has_all_nulls {
                continue;
            }

            if self.has_all_nulls {
                self.sum = that_sum;
                self.has_all_nulls = false;
                continue;
            }

            match self.sum.unwrap().add_checked(that_sum.unwrap()) {
                Ok(v) => self.sum = Some(v),
                Err(_) => {
                    self.sum = None;
                    self.has_all_nulls = false;
                }
            }
        }
        Ok(())
    }
}

struct SumIntGroupsAccumulatorLegacy {
    sums: Vec<Option<i64>>,
}

impl SumIntGroupsAccumulatorLegacy {
    fn new() -> Self {
        Self { sums: Vec::new() }
    }
}

impl GroupsAccumulator for SumIntGroupsAccumulatorLegacy {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> DFResult<()> {
        fn update_groups_sum<T>(
            int_array: &PrimitiveArray<T>,
            group_indices: &[usize],
            sums: &mut [Option<i64>],
            opt_filter: Option<&BooleanArray>,
        ) -> DFResult<()>
        where
            T: ArrowPrimitiveType,
            T::Native: ArrowNativeType,
        {
            for (i, &group_index) in group_indices.iter().enumerate() {
                if let Some(f) = opt_filter {
                    if !f.is_valid(i) || !f.value(i) {
                        continue;
                    }
                }
                if !int_array.is_null(i) {
                    let v = int_array.value(i).to_i64().ok_or_else(|| {
                        DataFusionError::Internal("Failed to convert value to i64".to_string())
                    })?;
                    sums[group_index] = Some(sums[group_index].unwrap_or(0).add_wrapping(v));
                }
            }
            Ok(())
        }

        let values = &values[0];
        self.sums.resize(total_num_groups, None);

        match values.data_type() {
            DataType::Int64 => update_groups_sum(
                as_primitive_array::<Int64Type>(values),
                group_indices,
                &mut self.sums,
                opt_filter,
            )?,
            DataType::Int32 => update_groups_sum(
                as_primitive_array::<Int32Type>(values),
                group_indices,
                &mut self.sums,
                opt_filter,
            )?,
            DataType::Int16 => update_groups_sum(
                as_primitive_array::<Int16Type>(values),
                group_indices,
                &mut self.sums,
                opt_filter,
            )?,
            DataType::Int8 => update_groups_sum(
                as_primitive_array::<Int8Type>(values),
                group_indices,
                &mut self.sums,
                opt_filter,
            )?,
            _ => {
                return Err(DataFusionError::Internal(format!(
                    "Unsupported data type for SumIntGroupsAccumulatorLegacy: {:?}",
                    values.data_type()
                )))
            }
        };
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> DFResult<ArrayRef> {
        match emit_to {
            EmitTo::All => {
                let result = Arc::new(Int64Array::from(std::mem::take(&mut self.sums))) as ArrayRef;
                Ok(result)
            }
            EmitTo::First(n) => {
                let result = Arc::new(Int64Array::from(self.sums.drain(..n).collect::<Vec<_>>()))
                    as ArrayRef;
                Ok(result)
            }
        }
    }

    fn state(&mut self, emit_to: EmitTo) -> DFResult<Vec<ArrayRef>> {
        let sums = emit_to.take_needed(&mut self.sums);
        Ok(vec![Arc::new(Int64Array::from(sums))])
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> DFResult<()> {
        if values.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "Invalid state while merging batch. Expected 1 element but found {}",
                values.len()
            )));
        }
        let that_sums = values[0].as_primitive::<Int64Type>();

        self.sums.resize(total_num_groups, None);

        for (idx, &group_index) in group_indices.iter().enumerate() {
            if that_sums.is_null(idx) {
                continue;
            }
            let that_sum = that_sums.value(idx);

            if self.sums[group_index].is_none() {
                self.sums[group_index] = Some(that_sum);
            } else {
                self.sums[group_index] =
                    Some(self.sums[group_index].unwrap().add_wrapping(that_sum));
            }
        }
        Ok(())
    }

    fn convert_to_state(
        &self,
        _values: &[ArrayRef],
        _opt_filter: Option<&BooleanArray>,
    ) -> DFResult<Vec<ArrayRef>> {
        not_impl_err!("Input batch conversion to state not implemented")
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

struct SumIntGroupsAccumulatorAnsi {
    sums: Vec<Option<i64>>,
}

impl SumIntGroupsAccumulatorAnsi {
    fn new() -> Self {
        Self { sums: Vec::new() }
    }
}

impl GroupsAccumulator for SumIntGroupsAccumulatorAnsi {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> DFResult<()> {
        fn update_groups_sum<T>(
            int_array: &PrimitiveArray<T>,
            group_indices: &[usize],
            sums: &mut [Option<i64>],
            opt_filter: Option<&BooleanArray>,
        ) -> DFResult<()>
        where
            T: ArrowPrimitiveType,
            T::Native: ArrowNativeType,
        {
            for (i, &group_index) in group_indices.iter().enumerate() {
                if let Some(f) = opt_filter {
                    if !f.is_valid(i) || !f.value(i) {
                        continue;
                    }
                }
                if !int_array.is_null(i) {
                    let v = int_array.value(i).to_i64().ok_or_else(|| {
                        DataFusionError::Internal("Failed to convert value to i64".to_string())
                    })?;
                    sums[group_index] =
                        Some(sums[group_index].unwrap_or(0).add_checked(v).map_err(|_| {
                            DataFusionError::from(arithmetic_overflow_error("integer"))
                        })?);
                }
            }
            Ok(())
        }

        let values = &values[0];
        self.sums.resize(total_num_groups, None);

        match values.data_type() {
            DataType::Int64 => update_groups_sum(
                as_primitive_array::<Int64Type>(values),
                group_indices,
                &mut self.sums,
                opt_filter,
            )?,
            DataType::Int32 => update_groups_sum(
                as_primitive_array::<Int32Type>(values),
                group_indices,
                &mut self.sums,
                opt_filter,
            )?,
            DataType::Int16 => update_groups_sum(
                as_primitive_array::<Int16Type>(values),
                group_indices,
                &mut self.sums,
                opt_filter,
            )?,
            DataType::Int8 => update_groups_sum(
                as_primitive_array::<Int8Type>(values),
                group_indices,
                &mut self.sums,
                opt_filter,
            )?,
            _ => {
                return Err(DataFusionError::Internal(format!(
                    "Unsupported data type for SumIntGroupsAccumulatorAnsi: {:?}",
                    values.data_type()
                )))
            }
        };
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> DFResult<ArrayRef> {
        match emit_to {
            EmitTo::All => {
                let result = Arc::new(Int64Array::from(std::mem::take(&mut self.sums))) as ArrayRef;
                Ok(result)
            }
            EmitTo::First(n) => {
                let result = Arc::new(Int64Array::from(self.sums.drain(..n).collect::<Vec<_>>()))
                    as ArrayRef;
                Ok(result)
            }
        }
    }

    fn state(&mut self, emit_to: EmitTo) -> DFResult<Vec<ArrayRef>> {
        let sums = emit_to.take_needed(&mut self.sums);
        Ok(vec![Arc::new(Int64Array::from(sums))])
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> DFResult<()> {
        if values.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "Invalid state while merging batch. Expected 1 element but found {}",
                values.len()
            )));
        }
        let that_sums = values[0].as_primitive::<Int64Type>();

        self.sums.resize(total_num_groups, None);

        for (idx, &group_index) in group_indices.iter().enumerate() {
            if that_sums.is_null(idx) {
                continue;
            }
            let that_sum = that_sums.value(idx);

            if self.sums[group_index].is_none() {
                self.sums[group_index] = Some(that_sum);
            } else {
                self.sums[group_index] = Some(
                    self.sums[group_index]
                        .unwrap()
                        .add_checked(that_sum)
                        .map_err(|_| DataFusionError::from(arithmetic_overflow_error("integer")))?,
                );
            }
        }
        Ok(())
    }

    fn convert_to_state(
        &self,
        _values: &[ArrayRef],
        _opt_filter: Option<&BooleanArray>,
    ) -> DFResult<Vec<ArrayRef>> {
        not_impl_err!("Input batch conversion to state not implemented")
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

struct SumIntGroupsAccumulatorTry {
    sums: Vec<Option<i64>>,
    has_all_nulls: Vec<bool>,
}

impl SumIntGroupsAccumulatorTry {
    fn new() -> Self {
        Self {
            sums: Vec::new(),
            has_all_nulls: Vec::new(),
        }
    }

    fn group_overflowed(&self, group_index: usize) -> bool {
        !self.has_all_nulls[group_index] && self.sums[group_index].is_none()
    }
}

impl GroupsAccumulator for SumIntGroupsAccumulatorTry {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> DFResult<()> {
        fn update_groups_sum<T>(
            int_array: &PrimitiveArray<T>,
            group_indices: &[usize],
            sums: &mut [Option<i64>],
            has_all_nulls: &mut [bool],
            opt_filter: Option<&BooleanArray>,
        ) -> DFResult<()>
        where
            T: ArrowPrimitiveType,
            T::Native: ArrowNativeType,
        {
            for (i, &group_index) in group_indices.iter().enumerate() {
                if let Some(f) = opt_filter {
                    if !f.is_valid(i) || !f.value(i) {
                        continue;
                    }
                }
                if !int_array.is_null(i) {
                    // Skip if this group already overflowed
                    if !has_all_nulls[group_index] && sums[group_index].is_none() {
                        continue;
                    }
                    let v = int_array.value(i).to_i64().ok_or_else(|| {
                        DataFusionError::Internal("Failed to convert value to i64".to_string())
                    })?;
                    match sums[group_index].unwrap_or(0).add_checked(v) {
                        Ok(new_sum) => sums[group_index] = Some(new_sum),
                        Err(_) => sums[group_index] = None,
                    };
                    has_all_nulls[group_index] = false;
                }
            }
            Ok(())
        }
        let values = &values[0];
        self.sums.resize(total_num_groups, Some(0));
        self.has_all_nulls.resize(total_num_groups, true);

        match values.data_type() {
            DataType::Int64 => update_groups_sum(
                as_primitive_array::<Int64Type>(values),
                group_indices,
                &mut self.sums,
                &mut self.has_all_nulls,
                opt_filter,
            )?,
            DataType::Int32 => update_groups_sum(
                as_primitive_array::<Int32Type>(values),
                group_indices,
                &mut self.sums,
                &mut self.has_all_nulls,
                opt_filter,
            )?,
            DataType::Int16 => update_groups_sum(
                as_primitive_array::<Int16Type>(values),
                group_indices,
                &mut self.sums,
                &mut self.has_all_nulls,
                opt_filter,
            )?,
            DataType::Int8 => update_groups_sum(
                as_primitive_array::<Int8Type>(values),
                group_indices,
                &mut self.sums,
                &mut self.has_all_nulls,
                opt_filter,
            )?,
            _ => {
                return Err(DataFusionError::Internal(format!(
                    "Unsupported data type for SumIntGroupsAccumulatorTry: {:?}",
                    values.data_type()
                )))
            }
        };
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> DFResult<ArrayRef> {
        match emit_to {
            EmitTo::All => {
                let result = Arc::new(Int64Array::from_iter(
                    self.sums
                        .iter()
                        .zip(self.has_all_nulls.iter())
                        .map(|(&sum, &is_null)| if is_null { None } else { sum }),
                )) as ArrayRef;
                self.sums.clear();
                self.has_all_nulls.clear();
                Ok(result)
            }
            EmitTo::First(n) => {
                let result = Arc::new(Int64Array::from_iter(
                    self.sums
                        .drain(..n)
                        .zip(self.has_all_nulls.drain(..n))
                        .map(|(sum, is_null)| if is_null { None } else { sum }),
                )) as ArrayRef;
                Ok(result)
            }
        }
    }

    fn state(&mut self, emit_to: EmitTo) -> DFResult<Vec<ArrayRef>> {
        let sums = emit_to.take_needed(&mut self.sums);
        let has_all_nulls = emit_to.take_needed(&mut self.has_all_nulls);
        Ok(vec![
            Arc::new(Int64Array::from(sums)),
            Arc::new(BooleanArray::from(has_all_nulls)),
        ])
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> DFResult<()> {
        if values.len() != 2 {
            return Err(DataFusionError::Internal(format!(
                "Invalid state while merging batch. Expected 2 elements but found {}",
                values.len()
            )));
        }
        let that_sums = values[0].as_primitive::<Int64Type>();
        let that_has_all_nulls_array = values[1].as_boolean();

        self.sums.resize(total_num_groups, Some(0));
        self.has_all_nulls.resize(total_num_groups, true);

        for (idx, &group_index) in group_indices.iter().enumerate() {
            let that_sum = if that_sums.is_null(idx) {
                None
            } else {
                Some(that_sums.value(idx))
            };
            let that_has_all_nulls = that_has_all_nulls_array.value(idx);

            let that_overflowed = !that_has_all_nulls && that_sum.is_none();
            if that_overflowed || self.group_overflowed(group_index) {
                self.sums[group_index] = None;
                self.has_all_nulls[group_index] = false;
                continue;
            }

            if that_has_all_nulls {
                continue;
            }

            if self.has_all_nulls[group_index] {
                self.sums[group_index] = that_sum;
                self.has_all_nulls[group_index] = false;
                continue;
            }

            // Both sides have non-null values
            match self.sums[group_index]
                .unwrap()
                .add_checked(that_sum.unwrap())
            {
                Ok(v) => self.sums[group_index] = Some(v),
                Err(_) => {
                    self.sums[group_index] = None;
                    self.has_all_nulls[group_index] = false;
                }
            }
        }
        Ok(())
    }

    fn convert_to_state(
        &self,
        _values: &[ArrayRef],
        _opt_filter: Option<&BooleanArray>,
    ) -> DFResult<Vec<ArrayRef>> {
        not_impl_err!("Input batch conversion to state not implemented")
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use datafusion::logical_expr::{EmitTo, GroupsAccumulator};

    #[test]
    fn sliding_sum_matches_ordered_checked_addition() {
        let choices = [
            None,
            Some(0),
            Some(1),
            Some(-1),
            Some(i64::MIN),
            Some(i64::MAX),
        ];
        // Exhaust every 5-row sequence of nulls, boundaries, and cancellation.
        for mut seed in 0..choices.len().pow(5) {
            let values: Vec<_> = (0..5)
                .map(|_| {
                    let value = choices[seed % choices.len()];
                    seed /= choices.len();
                    value
                })
                .collect();
            let array: ArrayRef = Arc::new(Int64Array::from(values.clone()));
            for width in [1, 2, 3, 5] {
                for mode in [EvalMode::Legacy, EvalMode::Try, EvalMode::Ansi] {
                    let mut acc = SlidingSumIntegerAccumulator::new(mode);
                    for end in 1..=values.len() {
                        // Deliberately update first, as DataFusion does. The
                        // transient union may overflow while both frames fit.
                        acc.update_batch(&[array.slice(end - 1, 1)]).unwrap();
                        let start = end.saturating_sub(width);
                        if end > width {
                            acc.retract_batch(&[array.slice(start - 1, 1)]).unwrap();
                        }
                        let mut expected = None;
                        let mut overflow = false;
                        for &v in values[start..end].iter().flatten() {
                            let sum = expected.unwrap_or(0_i64);
                            overflow |= sum.checked_add(v).is_none();
                            expected = Some(sum.wrapping_add(v));
                        }
                        let actual = acc.evaluate();
                        if overflow && mode == EvalMode::Ansi {
                            assert!(actual.is_err(), "{values:?}, {start}..{end}");
                        } else {
                            if overflow && mode == EvalMode::Try {
                                expected = None;
                            }
                            assert_eq!(
                                actual.unwrap(),
                                ScalarValue::Int64(expected),
                                "{values:?}, {start}..{end}, {mode:?}"
                            );
                        }
                    }
                    acc.retract_batch(&[array.slice(values.len().saturating_sub(width), width)])
                        .unwrap();
                    assert_eq!(acc.evaluate().unwrap(), ScalarValue::Int64(None));
                    assert!(acc.minima.is_empty() && acc.maxima.is_empty());
                    // Empty -> non-empty, with absolute prefix counters retained.
                    acc.update_batch(&[Arc::new(Int64Array::from(vec![7]))])
                        .unwrap();
                    assert_eq!(acc.evaluate().unwrap(), ScalarValue::Int64(Some(7)));
                }
            }
        }
    }

    #[test]
    fn sliding_sum_matches_advancing_frames() {
        // Cross the bounds by accumulating ordinary values as well as MIN/MAX.
        let half = 1_i64 << 62;
        let choices = [
            None,
            Some(half),
            Some(-half),
            Some(i64::MAX),
            Some(i64::MIN),
        ];
        for mut seed in 0..choices.len().pow(5) {
            let values: Vec<_> = (0..5)
                .map(|_| {
                    let value = choices[seed % choices.len()];
                    seed /= choices.len();
                    value
                })
                .collect();
            let array: ArrayRef = Arc::new(Int64Array::from(values.clone()));
            // Growing prefixes, shrinking suffixes, batch updates/retractions,
            // and disjoint frames.
            for frames in [
                vec![0..1, 0..2, 0..3, 0..4, 0..5],
                vec![0..5, 1..5, 2..5, 3..5, 4..5, 5..5],
                vec![0..2, 1..4, 2..5, 4..5, 5..5],
                vec![0..1, 3..4, 4..5, 5..5],
            ] {
                for mode in [EvalMode::Ansi, EvalMode::Try] {
                    let mut acc = SlidingSumIntegerAccumulator::new(mode);
                    let mut previous = 0..0;
                    for frame in &frames {
                        acc.update_batch(&[array.slice(previous.end, frame.end - previous.end)])
                            .unwrap();
                        acc.retract_batch(&[
                            array.slice(previous.start, frame.start - previous.start)
                        ])
                        .unwrap();
                        let expected = values[frame.clone()]
                            .iter()
                            .flatten()
                            .try_fold(None, |sum, &value| {
                                sum.unwrap_or(0_i64).checked_add(value).map(Some)
                            });
                        match (expected, mode) {
                            (None, EvalMode::Ansi) => assert!(acc.evaluate().is_err()),
                            _ => assert_eq!(
                                acc.evaluate().unwrap(),
                                ScalarValue::Int64(expected.flatten()),
                                "{values:?}, {frame:?}, {mode:?}"
                            ),
                        }
                        previous = frame.clone();
                    }
                }
            }
        }
    }

    #[test]
    fn sliding_sum_ordinary_suffixes_need_no_queue_allocation() {
        for value in [Some(8), Some(-8), Some(0), None] {
            let array: ArrayRef = Arc::new(Int64Array::from(vec![value; 65536]));
            let mut acc = SlidingSumIntegerAccumulator::new(EvalMode::Try);
            acc.update_batch(&[Arc::clone(&array)]).unwrap();
            for start in (0..65536).step_by(1024) {
                assert_eq!(
                    acc.evaluate().unwrap(),
                    ScalarValue::Int64(value.map(|v| v * (65536 - start) as i64))
                );
                assert_eq!(acc.size(), std::mem::size_of_val(&acc));
                acc.retract_batch(&[array.slice(start, 1024)]).unwrap();
            }
            assert_eq!(acc.evaluate().unwrap(), ScalarValue::Int64(None));
            assert_eq!(acc.size(), std::mem::size_of_val(&acc));
        }
    }

    #[test]
    fn sliding_sum_exact_bounds_need_no_queue_allocation() {
        for value in [i64::MIN, i64::MAX] {
            let array: ArrayRef = Arc::new(Int64Array::from(vec![Some(value), None, Some(0)]));
            let mut acc = SlidingSumIntegerAccumulator::new(EvalMode::Ansi);
            acc.update_batch(&[Arc::clone(&array)]).unwrap();
            assert_eq!(acc.evaluate().unwrap(), ScalarValue::Int64(Some(value)));
            assert_eq!(acc.size(), std::mem::size_of_val(&acc));
            acc.retract_batch(&[array.slice(0, 1)]).unwrap();
            assert_eq!(acc.evaluate().unwrap(), ScalarValue::Int64(Some(0)));
            assert_eq!(acc.size(), std::mem::size_of_val(&acc));
        }
    }

    #[test]
    fn sliding_sum_checks_intermediate_overflow_and_recovers() {
        for (values, expected) in [(vec![i64::MAX, 1, -1], 0), (vec![i64::MIN, -1, 1], 0)] {
            let array: ArrayRef = Arc::new(Int64Array::from(values));
            let mut acc = SlidingSumIntegerAccumulator::new(EvalMode::Try);
            acc.update_batch(&[Arc::clone(&array)]).unwrap();
            assert_eq!(acc.evaluate().unwrap(), ScalarValue::Int64(None));
            acc.retract_batch(&[array.slice(0, 1)]).unwrap();
            assert_eq!(acc.evaluate().unwrap(), ScalarValue::Int64(Some(expected)));
        }
    }

    #[test]
    fn sliding_sum_accepts_all_integral_types() {
        for datatype in [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
        ] {
            let array = arrow::compute::cast(
                &Int64Array::from(vec![Some(100), None, Some(-100), Some(7)]),
                &datatype,
            )
            .unwrap();
            let mut acc = SlidingSumIntegerAccumulator::new(EvalMode::Ansi);
            acc.update_batch(&[Arc::clone(&array)]).unwrap();
            acc.retract_batch(&[array.slice(0, 2)]).unwrap();
            assert_eq!(acc.evaluate().unwrap(), ScalarValue::Int64(Some(-93)));
        }
        for mode in [EvalMode::Ansi, EvalMode::Try] {
            let udf = SumInteger::try_new(DataType::Int64, mode).unwrap();
            assert!(matches!(udf.reverse_expr(), ReversedUDAF::NotSupported));
        }
    }

    fn run_update_batch_with_filter(
        acc: &mut dyn GroupsAccumulator,
        values: Vec<i64>,
        groups: Vec<usize>,
        filter: Vec<bool>,
        num_groups: usize,
    ) -> Vec<Option<i64>> {
        let values: ArrayRef = Arc::new(Int64Array::from(values));
        let filter = BooleanArray::from(filter);
        acc.update_batch(&[values], &groups, Some(&filter), num_groups)
            .unwrap();
        acc.evaluate(EmitTo::All)
            .unwrap()
            .as_primitive::<Int64Type>()
            .iter()
            .collect()
    }

    #[test]
    fn test_legacy_update_batch_with_filter() {
        let mut acc = SumIntGroupsAccumulatorLegacy::new();
        // values: [1, 2, 3, 4, 5], filter: [T, F, T, F, T] => sum = 1+3+5 = 9
        let result = run_update_batch_with_filter(
            &mut acc,
            vec![1, 2, 3, 4, 5],
            vec![0, 0, 0, 0, 0],
            vec![true, false, true, false, true],
            1,
        );
        assert_eq!(result, vec![Some(9)]);
    }

    #[test]
    fn test_legacy_update_batch_filter_null_treated_as_exclude() {
        let mut acc = SumIntGroupsAccumulatorLegacy::new();
        let values: ArrayRef = Arc::new(Int64Array::from(vec![10i64, 20, 30]));
        // null filter entry should be treated as exclude
        let filter = BooleanArray::from(vec![Some(true), None, Some(true)]);
        acc.update_batch(&[values], &[0, 0, 0], Some(&filter), 1)
            .unwrap();
        let result: Vec<Option<i64>> = acc
            .evaluate(EmitTo::All)
            .unwrap()
            .as_primitive::<Int64Type>()
            .iter()
            .collect();
        assert_eq!(result, vec![Some(40)]); // 10 + 30 = 40
    }

    #[test]
    fn test_ansi_update_batch_with_filter() {
        let mut acc = SumIntGroupsAccumulatorAnsi::new();
        let result = run_update_batch_with_filter(
            &mut acc,
            vec![10, 20, 30, 40],
            vec![0, 1, 0, 1],
            vec![true, true, false, true],
            2,
        );
        // group 0: 10 (30 filtered out); group 1: 20+40 = 60
        assert_eq!(result, vec![Some(10), Some(60)]);
    }

    #[test]
    fn test_try_update_batch_with_filter() {
        let mut acc = SumIntGroupsAccumulatorTry::new();
        let result = run_update_batch_with_filter(
            &mut acc,
            vec![1, 2, 3, 4, 5],
            vec![0, 0, 0, 0, 0],
            vec![true, false, true, false, true],
            1,
        );
        assert_eq!(result, vec![Some(9)]); // 1+3+5 = 9
    }

    #[test]
    fn test_no_filter_still_works() {
        let mut acc = SumIntGroupsAccumulatorLegacy::new();
        let values: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 2, 3]));
        acc.update_batch(&[values], &[0, 0, 0], None, 1).unwrap();
        let result: Vec<Option<i64>> = acc
            .evaluate(EmitTo::All)
            .unwrap()
            .as_primitive::<Int64Type>()
            .iter()
            .collect();
        assert_eq!(result, vec![Some(6)]);
    }

    /// Regression coverage for the scalar `Accumulator` path used when Comet wraps a PartialMerge
    /// expression with `MergeAsPartial`: `merge_batch` has to consume every row of the incoming
    /// state array. The previous implementation read only row 0, which silently under-counted
    /// whenever the MergeAsPartial operator handed us a state batch with more than one row.
    #[test]
    fn test_legacy_accumulator_merge_batch_multi_row() {
        let mut acc = SumIntegerAccumulatorLegacy::new();
        let states: ArrayRef = Arc::new(Int64Array::from(vec![Some(1i64), Some(2), None, Some(3)]));
        acc.merge_batch(&[states]).unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Int64(Some(6)));
    }

    #[test]
    fn test_ansi_accumulator_merge_batch_multi_row() {
        let mut acc = SumIntegerAccumulatorAnsi::new();
        let states: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(10i64),
            Some(20),
            None,
            Some(30),
        ]));
        acc.merge_batch(&[states]).unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Int64(Some(60)));
    }
}
