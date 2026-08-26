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
    builder::PrimitiveBuilder,
    cast::AsArray,
    types::{Decimal128Type, Int64Type},
    Array, ArrayRef, Decimal128Array, Int64Array, PrimitiveArray,
};
use arrow::datatypes::{DataType, Field, FieldRef};
use arrow::{array::BooleanBufferBuilder, buffer::NullBuffer, compute::sum};
use datafusion::common::{not_impl_err, Result, ScalarValue};
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, EmitTo, GroupsAccumulator, ReversedUDAF, Signature,
};
use datafusion::physical_expr::expressions::format_state_name;
use std::sync::Arc;

use crate::utils::{build_bool_state, is_valid_decimal_precision, unlikely};
use crate::{decimal_sum_overflow_error, EvalMode, SparkErrorWithContext};
use arrow::array::ArrowNativeTypeOp;
use arrow::datatypes::{
    DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE, MAX_DECIMAL128_FOR_EACH_PRECISION,
    MIN_DECIMAL128_FOR_EACH_PRECISION,
};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::Volatility::Immutable;
use num::{integer::div_ceil, Integer};
use DataType::*;

fn avg_return_type(_name: &str, data_type: &DataType) -> Result<DataType> {
    match data_type {
        Decimal128(precision, scale) => {
            // In the spark, the result type is DECIMAL(min(38,precision+4), min(38,scale+4)).
            // Ref: https://github.com/apache/spark/blob/fcf636d9eb8d645c24be3db2d599aba2d7e2955a/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/Average.scala#L66
            let new_precision = DECIMAL128_MAX_PRECISION.min(*precision + 4);
            let new_scale = DECIMAL128_MAX_SCALE.min(*scale + 4);
            Ok(Decimal128(new_precision, new_scale))
        }
        _ => not_impl_err!("Avg return type for {data_type}"),
    }
}

/// AVG aggregate expression
#[derive(Debug, Clone)]
pub struct AvgDecimal {
    signature: Signature,
    sum_data_type: DataType,
    result_data_type: DataType,
    eval_mode: EvalMode,
    expr_id: Option<u64>,
    registry: Arc<crate::QueryContextMap>,
}

// Manually implement PartialEq, Eq, and Hash excluding the registry field
impl PartialEq for AvgDecimal {
    fn eq(&self, other: &Self) -> bool {
        self.sum_data_type == other.sum_data_type
            && self.result_data_type == other.result_data_type
            && self.eval_mode == other.eval_mode
            && self.expr_id == other.expr_id
    }
}

impl Eq for AvgDecimal {}

impl std::hash::Hash for AvgDecimal {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.sum_data_type.hash(state);
        self.result_data_type.hash(state);
        self.eval_mode.hash(state);
        self.expr_id.hash(state);
    }
}

impl AvgDecimal {
    /// Create a new AVG aggregate function
    pub fn new(
        result_type: DataType,
        sum_type: DataType,
        eval_mode: EvalMode,
        expr_id: Option<u64>,
        registry: Arc<crate::QueryContextMap>,
    ) -> Self {
        Self {
            signature: Signature::user_defined(Immutable),
            result_data_type: result_type,
            sum_data_type: sum_type,
            eval_mode,
            expr_id,
            registry,
        }
    }
}

impl AggregateUDFImpl for AvgDecimal {
    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        match (&self.sum_data_type, &self.result_data_type) {
            (Decimal128(sum_precision, sum_scale), Decimal128(target_precision, target_scale)) => {
                Ok(Box::new(AvgDecimalAccumulator::new(
                    *sum_scale,
                    *sum_precision,
                    *target_precision,
                    *target_scale,
                    self.eval_mode,
                    self.expr_id,
                    Arc::clone(&self.registry),
                )))
            }
            _ => not_impl_err!(
                "AvgDecimalAccumulator for ({} --> {})",
                self.sum_data_type,
                self.result_data_type
            ),
        }
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Arc::new(Field::new(
                format_state_name(self.name(), "sum"),
                self.sum_data_type.clone(),
                true,
            )),
            Arc::new(Field::new(
                format_state_name(self.name(), "count"),
                DataType::Int64,
                true,
            )),
        ])
    }

    fn name(&self) -> &str {
        "avg"
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        ReversedUDAF::Identical
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        // instantiate specialized accumulator based for the type
        match (&self.sum_data_type, &self.result_data_type) {
            (Decimal128(sum_precision, sum_scale), Decimal128(target_precision, target_scale)) => {
                Ok(Box::new(AvgDecimalGroupsAccumulator::new(
                    &self.result_data_type,
                    &self.sum_data_type,
                    *target_precision,
                    *target_scale,
                    *sum_precision,
                    *sum_scale,
                    self.eval_mode,
                    self.expr_id,
                    Arc::clone(&self.registry),
                )))
            }
            _ => not_impl_err!(
                "AvgDecimalGroupsAccumulator for ({} --> {})",
                self.sum_data_type,
                self.result_data_type
            ),
        }
    }

    fn default_value(&self, _data_type: &DataType) -> Result<ScalarValue> {
        match &self.result_data_type {
            Decimal128(target_precision, target_scale) => {
                Ok(make_decimal128(None, *target_precision, *target_scale))
            }
            _ => not_impl_err!(
                "The result_data_type of AvgDecimal should be Decimal128 but got{}",
                self.result_data_type
            ),
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        avg_return_type(self.name(), &arg_types[0])
    }

    fn is_nullable(&self) -> bool {
        // In Spark, Sum.nullable and Average.nullable both return true irrespective of ANSI mode.
        // AvgDecimal is always nullable because overflows can cause null values.
        true
    }
}

/// An accumulator to compute the average for decimals
#[derive(Debug)]
struct AvgDecimalAccumulator {
    sum: Option<i128>,
    count: i64,
    is_empty: bool,
    is_not_null: bool,
    sum_scale: i8,
    sum_precision: u8,
    target_precision: u8,
    target_scale: i8,
    eval_mode: EvalMode,
    expr_id: Option<u64>,
    registry: Arc<crate::QueryContextMap>,
}

impl AvgDecimalAccumulator {
    pub fn new(
        sum_scale: i8,
        sum_precision: u8,
        target_precision: u8,
        target_scale: i8,
        eval_mode: EvalMode,
        expr_id: Option<u64>,
        registry: Arc<crate::QueryContextMap>,
    ) -> Self {
        Self {
            sum: None,
            count: 0,
            is_empty: true,
            is_not_null: true,
            sum_scale,
            sum_precision,
            target_precision,
            target_scale,
            eval_mode,
            expr_id,
            registry,
        }
    }

    /// Wrap a SparkError with QueryContext if expr_id is available
    fn wrap_error_with_context(
        &self,
        error: crate::SparkError,
    ) -> datafusion::common::DataFusionError {
        if let Some(expr_id) = self.expr_id {
            if let Some(query_ctx) = self.registry.get(expr_id) {
                let wrapped = SparkErrorWithContext::with_context(error, query_ctx);
                return datafusion::common::DataFusionError::External(Box::new(wrapped));
            }
        }
        datafusion::common::DataFusionError::from(error)
    }

    fn update_single(&mut self, values: &Decimal128Array, idx: usize) -> Result<()> {
        let v = unsafe { values.value_unchecked(idx) };
        let (new_sum, is_overflow) = match self.sum {
            Some(sum) => sum.overflowing_add(v),
            None => (v, false),
        };

        if is_overflow || !is_valid_decimal_precision(new_sum, self.sum_precision) {
            // Overflow: set to null. Error will be thrown during evaluate in ANSI mode.
            // This matches Spark's DecimalAddNoOverflowCheck behavior.
            self.is_not_null = false;
            return Ok(());
        }

        self.sum = Some(new_sum);

        if let Some(new_count) = self.count.checked_add(1) {
            self.count = new_count;
        } else {
            // Count overflow: set to null. Error will be thrown during evaluate in ANSI mode.
            self.is_not_null = false;
            return Ok(());
        }

        self.is_not_null = true;
        Ok(())
    }
}

fn make_decimal128(value: Option<i128>, precision: u8, scale: i8) -> ScalarValue {
    ScalarValue::Decimal128(value, precision, scale)
}

impl Accumulator for AvgDecimalAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        // Spark distinguishes an empty partial (zero sum) from an overflow (null sum).
        let sum = if !self.is_not_null {
            None
        } else if self.count == 0 {
            Some(0)
        } else {
            self.sum
        };
        Ok(vec![
            ScalarValue::Decimal128(sum, self.sum_precision, self.sum_scale),
            ScalarValue::from(self.count),
        ])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if !self.is_not_null {
            // This means there's a overflow in decimal, so we will just skip the rest
            // of the computation
            return Ok(());
        }
        let values = &values[0];
        let data = values.as_primitive::<Decimal128Type>();
        self.is_empty = self.is_empty && values.len() == values.null_count();
        if values.null_count() == 0 {
            for i in 0..data.len() {
                self.update_single(data, i)?;
            }
        } else {
            for i in 0..data.len() {
                if data.is_null(i) {
                    continue;
                }
                self.update_single(data, i)?;
            }
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let partial_sums = states[0].as_primitive::<Decimal128Type>();
        let partial_counts = states[1].as_primitive::<Int64Type>();

        // Update is_empty: if any partial state has data, we're not empty
        if self.is_empty {
            self.is_empty = partial_counts.len() == partial_counts.null_count();
        }

        // counts are summed
        self.count += sum(partial_counts).unwrap_or_default();

        // Empty partials have non-null zero buffers. A null sum is an overflow marker;
        // grouped partials also null the count. Do not let sum() skip these markers or
        // let a later empty partial revive an accumulator that has already overflowed.
        if !self.is_not_null || partial_sums.null_count() > 0 || partial_counts.null_count() > 0 {
            self.is_not_null = false;
            self.sum = None;
            return Ok(());
        }

        // sums are summed
        if let Some(x) = sum(partial_sums) {
            let v = self.sum.get_or_insert(0);
            let (result, overflowed) = v.overflowing_add(x);

            if overflowed || !is_valid_decimal_precision(result, self.sum_precision) {
                // Overflow during merge: set to null, error will be thrown during evaluate in ANSI mode
                self.is_not_null = false;
                self.sum = None;
            } else {
                *v = result;
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        // A grouped overflow can have a null count, leaving the merged count at zero.
        // Check the overflow marker before treating a zero count as empty input.
        let has_overflow =
            !self.is_not_null || (self.sum.is_none() && !self.is_empty && self.count > 0);
        if has_overflow && self.eval_mode == EvalMode::Ansi {
            let error = decimal_sum_overflow_error("avg");
            return Err(self.wrap_error_with_context(error));
        }

        // An all-empty native final now merges zero sums. It must still return NULL,
        // rather than dividing that sum by zero, in every evaluation mode.
        if self.count == 0 || has_overflow {
            return Ok(make_decimal128(
                None,
                self.target_precision,
                self.target_scale,
            ));
        }

        let scaler = 10_i128.pow(self.target_scale.saturating_sub(self.sum_scale) as u32);
        let target_min = MIN_DECIMAL128_FOR_EACH_PRECISION[self.target_precision as usize];
        let target_max = MAX_DECIMAL128_FOR_EACH_PRECISION[self.target_precision as usize];

        let result = self
            .sum
            .map(|v| avg(v, self.count as i128, target_min, target_max, scaler));

        match result {
            Some(value) => Ok(make_decimal128(
                value,
                self.target_precision,
                self.target_scale,
            )),
            _ => Ok(make_decimal128(
                None,
                self.target_precision,
                self.target_scale,
            )),
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

#[derive(Debug)]
struct AvgDecimalGroupsAccumulator {
    /// Tracks if the value is null
    is_not_null: BooleanBufferBuilder,

    /// The type of the avg return type
    return_data_type: DataType,
    target_precision: u8,
    target_scale: i8,

    /// Count per group (use i64 to make Int64Array)
    counts: Vec<i64>,

    /// Sums per group, stored as i128
    sums: Vec<i128>,

    /// The type of the sum
    sum_data_type: DataType,
    /// This is input_precision + 10 to be consistent with Spark
    sum_precision: u8,
    sum_scale: i8,

    /// Evaluation mode for error handling
    eval_mode: EvalMode,
    /// Optional expression ID for query context lookup during error creation
    expr_id: Option<u64>,
    /// Session-scoped query context registry for error reporting
    registry: Arc<crate::QueryContextMap>,
}

impl AvgDecimalGroupsAccumulator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        return_data_type: &DataType,
        sum_data_type: &DataType,
        target_precision: u8,
        target_scale: i8,
        sum_precision: u8,
        sum_scale: i8,
        eval_mode: EvalMode,
        expr_id: Option<u64>,
        registry: Arc<crate::QueryContextMap>,
    ) -> Self {
        Self {
            is_not_null: BooleanBufferBuilder::new(0),
            return_data_type: return_data_type.clone(),
            target_precision,
            target_scale,
            sum_data_type: sum_data_type.clone(),
            sum_precision,
            sum_scale,
            counts: vec![],
            sums: vec![],
            eval_mode,
            expr_id,
            registry,
        }
    }

    /// Wrap a SparkError with QueryContext if expr_id is available
    fn wrap_error_with_context(
        &self,
        error: crate::SparkError,
    ) -> datafusion::common::DataFusionError {
        if let Some(expr_id) = self.expr_id {
            if let Some(query_ctx) = self.registry.get(expr_id) {
                let wrapped = SparkErrorWithContext::with_context(error, query_ctx);
                return datafusion::common::DataFusionError::External(Box::new(wrapped));
            }
        }
        datafusion::common::DataFusionError::from(error)
    }

    #[inline]
    fn update_single(&mut self, group_index: usize, value: i128) -> Result<()> {
        let (new_sum, is_overflow) = self.sums[group_index].overflowing_add(value);
        self.counts[group_index] += 1;
        self.sums[group_index] = new_sum;

        if unlikely(is_overflow || !is_valid_decimal_precision(new_sum, self.sum_precision)) {
            // Overflow: set to null. Error will be thrown during evaluate in ANSI mode.
            // This matches Spark's DecimalAddNoOverflowCheck behavior.
            self.is_not_null.set_bit(group_index, false);
        }
        Ok(())
    }
}

fn ensure_bit_capacity(builder: &mut BooleanBufferBuilder, capacity: usize) {
    if builder.len() < capacity {
        let additional = capacity - builder.len();
        builder.append_n(additional, true);
    }
}

impl GroupsAccumulator for AvgDecimalGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&arrow::array::BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 1, "single argument to update_batch");
        let values = values[0].as_primitive::<Decimal128Type>();
        let data = values.values();

        // increment counts, update sums
        self.counts.resize(total_num_groups, 0);
        self.sums.resize(total_num_groups, 0);
        ensure_bit_capacity(&mut self.is_not_null, total_num_groups);

        let iter = group_indices.iter().zip(data.iter());
        if opt_filter.is_none() && values.null_count() == 0 {
            for (&group_index, &value) in iter {
                self.update_single(group_index, value)?;
            }
        } else {
            for (idx, (&group_index, &value)) in iter.enumerate() {
                if let Some(f) = opt_filter {
                    if !f.is_valid(idx) || !f.value(idx) {
                        continue;
                    }
                }
                if values.is_null(idx) {
                    continue;
                }
                self.update_single(group_index, value)?;
            }
        }
        Ok(())
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        _opt_filter: Option<&arrow::array::BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 2, "two arguments to merge_batch");
        // first batch is partial sums, second is counts
        let partial_sums = values[0].as_primitive::<Decimal128Type>();
        let partial_counts = values[1].as_primitive::<Int64Type>();
        // update counts with partial counts
        self.counts.resize(total_num_groups, 0);
        let iter1 = group_indices.iter().zip(partial_counts.values().iter());
        for (&group_index, &partial_count) in iter1 {
            self.counts[group_index] += partial_count;
        }

        // update sums
        self.sums.resize(total_num_groups, 0);
        // Ensure bit capacity BEFORE setting any bits
        ensure_bit_capacity(&mut self.is_not_null, total_num_groups);

        let iter2 = group_indices.iter().zip(partial_sums.values().iter());
        for (idx, (&group_index, &new_value)) in iter2.enumerate() {
            // Check if partial sum is null (indicates overflow in that partition)
            if partial_sums.is_null(idx) {
                self.is_not_null.set_bit(group_index, false);
                continue;
            }

            let sum = self.sums[group_index];
            let (new_sum, is_overflow) = sum.overflowing_add(new_value);

            if is_overflow || !is_valid_decimal_precision(new_sum, self.sum_precision) {
                if self.eval_mode == EvalMode::Ansi {
                    let error = decimal_sum_overflow_error("avg");
                    return Err(self.wrap_error_with_context(error));
                }
                self.is_not_null.set_bit(group_index, false);
            } else {
                self.sums[group_index] = new_sum;
            }
        }
        if partial_counts.null_count() != 0 {
            for (index, &group_index) in group_indices.iter().enumerate() {
                if partial_counts.is_null(index) {
                    self.is_not_null.set_bit(group_index, false);
                }
            }
        }

        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let nulls = build_bool_state(&mut self.is_not_null, &emit_to);
        let counts = emit_to.take_needed(&mut self.counts);
        let sums = emit_to.take_needed(&mut self.sums);

        let mut builder = PrimitiveBuilder::<Decimal128Type>::with_capacity(sums.len())
            .with_data_type(self.return_data_type.clone());
        let iter = sums.into_iter().zip(counts);

        let scaler = 10_i128.pow(self.target_scale.saturating_sub(self.sum_scale) as u32);
        let target_min = MIN_DECIMAL128_FOR_EACH_PRECISION[self.target_precision as usize];
        let target_max = MAX_DECIMAL128_FOR_EACH_PRECISION[self.target_precision as usize];

        for (is_not_null, (sum, count)) in nulls.into_iter().zip(iter) {
            // A null state marks overflow even if a shuffle zeroed its null count
            // payload. Empty/all-null groups keep valid zero buffers instead.
            if !is_not_null && self.eval_mode == EvalMode::Ansi {
                let error = decimal_sum_overflow_error("avg");
                return Err(self.wrap_error_with_context(error));
            }

            if !is_not_null || count == 0 {
                builder.append_null();
                continue;
            }

            match avg(sum, count as i128, target_min, target_max, scaler) {
                Some(value) => {
                    builder.append_value(value);
                }
                _ => {
                    builder.append_null();
                }
            }
        }
        let array: PrimitiveArray<Decimal128Type> = builder.finish();

        Ok(Arc::new(array))
    }

    // return arrays for sums and counts
    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let nulls = build_bool_state(&mut self.is_not_null, &emit_to);
        let nulls = Some(NullBuffer::new(nulls));

        let counts = emit_to.take_needed(&mut self.counts);
        let counts = Int64Array::new(counts.into(), nulls.clone());

        let sums = emit_to.take_needed(&mut self.sums);
        let sums =
            Decimal128Array::new(sums.into(), nulls).with_data_type(self.sum_data_type.clone());

        Ok(vec![
            Arc::new(sums) as ArrayRef,
            Arc::new(counts) as ArrayRef,
        ])
    }

    fn size(&self) -> usize {
        self.counts.capacity() * std::mem::size_of::<i64>()
            + self.sums.capacity() * std::mem::size_of::<i128>()
    }
}

/// Returns the `sum`/`count` as a i128 Decimal128 with
/// target_scale and target_precision and return None if overflows.
///
/// * sum: The total sum value stored as Decimal128 with sum_scale
/// * count: total count, stored as a i128 (*NOT* a Decimal128 value)
/// * target_min: The minimum output value possible to represent with the target precision
/// * target_max: The maximum output value possible to represent with the target precision
/// * scaler: scale factor for avg
#[inline(always)]
fn avg(sum: i128, count: i128, target_min: i128, target_max: i128, scaler: i128) -> Option<i128> {
    if let Some(value) = sum.checked_mul(scaler) {
        // `sum / count` with ROUND_HALF_UP
        let (div, rem) = value.div_rem(&count);
        let half = div_ceil(count, 2);
        let half_neg = half.neg_wrapping();
        let new_value = match value >= 0 {
            true if rem >= half => div.add_wrapping(1),
            false if rem <= half_neg => div.sub_wrapping(1),
            _ => div,
        };
        if new_value >= target_min && new_value <= target_max {
            Some(new_value)
        } else {
            None
        }
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn accumulator(mode: EvalMode) -> AvgDecimalAccumulator {
        AvgDecimalAccumulator::new(2, 17, 11, 6, mode, None, crate::create_query_context_map())
    }

    fn assert_overflow(acc: &mut AvgDecimalAccumulator) -> Result<()> {
        if acc.eval_mode == EvalMode::Ansi {
            assert!(acc.evaluate().is_err());
        } else {
            assert_eq!(
                acc.evaluate()?,
                ScalarValue::Decimal128(None, acc.target_precision, acc.target_scale)
            );
        }
        Ok(())
    }

    fn overflowed_accumulator(mode: EvalMode) -> Result<AvgDecimalAccumulator> {
        let mut acc = accumulator(mode);
        // Both partial sums fit, but their merge exceeds the sum's decimal precision.
        let sums =
            Decimal128Array::from(vec![10_i128.pow(17) - 1, 1]).with_precision_and_scale(17, 2)?;
        acc.merge_batch(&[Arc::new(sums), Arc::new(Int64Array::from(vec![1, 1]))])?;
        Ok(acc)
    }

    #[test]
    fn empty_partial_state_matches_spark() -> Result<()> {
        for mode in [EvalMode::Legacy, EvalMode::Ansi, EvalMode::Try] {
            for values in [None, Some(vec![]), Some(vec![None, None])] {
                let mut acc = accumulator(mode);
                if let Some(values) = values {
                    let values = Decimal128Array::from(values).with_precision_and_scale(7, 2)?;
                    acc.update_batch(&[Arc::new(values)])?;
                }
                assert_eq!(
                    acc.state()?,
                    vec![
                        ScalarValue::Decimal128(Some(0), 17, 2),
                        ScalarValue::Int64(Some(0))
                    ]
                );
                assert_eq!(acc.evaluate()?, ScalarValue::Decimal128(None, 11, 6));
            }
        }
        Ok(())
    }

    #[test]
    fn merge_empty_partial_states() -> Result<()> {
        for mode in [EvalMode::Legacy, EvalMode::Ansi, EvalMode::Try] {
            for nonempty in [false, true] {
                let mut final_acc = accumulator(mode);
                let values = nonempty.then(|| vec![Some(100), Some(300)]);
                for values in [None, values, Some(vec![None])] {
                    let mut partial = accumulator(mode);
                    if let Some(values) = values {
                        let values =
                            Decimal128Array::from(values).with_precision_and_scale(7, 2)?;
                        partial.update_batch(&[Arc::new(values)])?;
                    }
                    let state = partial
                        .state()?
                        .into_iter()
                        .map(|value| value.to_array_of_size(1))
                        .collect::<Result<Vec<_>>>()?;
                    final_acc.merge_batch(&state)?;
                }
                assert_eq!(
                    final_acc.evaluate()?,
                    ScalarValue::Decimal128(nonempty.then_some(2_000_000), 11, 6)
                );
            }
        }
        Ok(())
    }

    #[test]
    fn overflow_partial_state_is_not_empty() -> Result<()> {
        for mode in [EvalMode::Legacy, EvalMode::Ansi, EvalMode::Try] {
            let mut acc = overflowed_accumulator(mode)?;
            assert_eq!(
                acc.state()?,
                vec![
                    ScalarValue::Decimal128(None, 17, 2),
                    ScalarValue::Int64(Some(2))
                ]
            );
            assert_overflow(&mut acc)?;
        }
        Ok(())
    }

    #[test]
    fn update_overflow_partial_state_is_preserved() -> Result<()> {
        for mode in [EvalMode::Legacy, EvalMode::Ansi, EvalMode::Try] {
            let mut acc = AvgDecimalAccumulator::new(
                2,
                38,
                38,
                6,
                mode,
                None,
                crate::create_query_context_map(),
            );
            let values = Decimal128Array::from(vec![10_i128.pow(38) - 1; 2])
                .with_precision_and_scale(38, 2)?;
            acc.update_batch(&[Arc::new(values)])?;
            // update_single retains the last valid sum on overflow. It must not leak
            // into the partial buffer or be evaluated as a valid average.
            assert_eq!(acc.state()?[0], ScalarValue::Decimal128(None, 38, 2));
            assert_overflow(&mut acc)?;
        }
        Ok(())
    }

    #[test]
    fn empty_partial_does_not_mask_overflow() -> Result<()> {
        for mode in [EvalMode::Legacy, EvalMode::Ansi, EvalMode::Try] {
            for reverse in [false, true] {
                for batch_size in [1, 2] {
                    let mut states = [
                        accumulator(mode).state()?,
                        overflowed_accumulator(mode)?.state()?,
                    ];
                    if reverse {
                        states.reverse();
                    }
                    let mut final_acc = accumulator(mode);
                    for batch in states.chunks(batch_size) {
                        let arrays = (0..2)
                            .map(|i| {
                                ScalarValue::iter_to_array(
                                    batch.iter().map(|state| state[i].clone()),
                                )
                            })
                            .collect::<Result<Vec<_>>>()?;
                        final_acc.merge_batch(&arrays)?;
                    }
                    assert_eq!(
                        final_acc.state()?,
                        vec![
                            ScalarValue::Decimal128(None, 17, 2),
                            ScalarValue::Int64(Some(2))
                        ]
                    );
                    assert_overflow(&mut final_acc)?;
                }
            }
        }
        Ok(())
    }

    #[test]
    fn grouped_overflow_with_null_count_is_not_empty() -> Result<()> {
        for mode in [EvalMode::Legacy, EvalMode::Ansi, EvalMode::Try] {
            let mut partial = AvgDecimalGroupsAccumulator::new(
                &DataType::Decimal128(38, 6),
                &DataType::Decimal128(38, 2),
                38,
                6,
                38,
                2,
                mode,
                None,
                crate::create_query_context_map(),
            );
            let values = Decimal128Array::from(vec![10_i128.pow(38) - 1; 2])
                .with_precision_and_scale(38, 2)?;
            partial.update_batch(&[Arc::new(values)], &[0, 0], None, 1)?;
            let overflow = partial.state(EmitTo::All)?;
            // Grouped partials encode overflow by nulling both the sum and count.
            assert!(overflow[0].is_null(0));
            assert!(overflow[1].is_null(0));

            let new_acc = || {
                AvgDecimalAccumulator::new(
                    2,
                    38,
                    38,
                    6,
                    mode,
                    None,
                    crate::create_query_context_map(),
                )
            };
            for reverse in [false, true] {
                let empty = new_acc()
                    .state()?
                    .into_iter()
                    .map(|value| value.to_array_of_size(1))
                    .collect::<Result<Vec<_>>>()?;
                let mut states = [overflow.clone(), empty];
                if reverse {
                    states.reverse();
                }
                let mut final_acc = new_acc();
                for state in states {
                    final_acc.merge_batch(&state)?;
                }
                assert_overflow(&mut final_acc)?;
            }

            // A null count can leave is_empty set after merge. A subsequent update
            // must still respect the overflow marker, regardless of that empty flag.
            let mut final_acc = new_acc();
            final_acc.merge_batch(&overflow)?;
            let values = Decimal128Array::from(vec![100]).with_precision_and_scale(38, 2)?;
            final_acc.update_batch(&[Arc::new(values)])?;
            assert_overflow(&mut final_acc)?;
        }
        Ok(())
    }

    #[test]
    fn grouped_overflow_with_zeroed_null_count_is_not_empty() -> Result<()> {
        for mode in [EvalMode::Legacy, EvalMode::Ansi, EvalMode::Try] {
            let new_acc = || {
                AvgDecimalGroupsAccumulator::new(
                    &DataType::Decimal128(38, 38),
                    &DataType::Decimal128(38, 38),
                    38,
                    38,
                    38,
                    38,
                    mode,
                    None,
                    crate::create_query_context_map(),
                )
            };
            let mut partial = new_acc();
            // Group 0 has no input, group 1 has only nulls, and group 2 overflows.
            let values = Decimal128Array::from(vec![
                None,
                None,
                Some(6 * 10_i128.pow(37)),
                Some(6 * 10_i128.pow(37)),
            ])
            .with_precision_and_scale(38, 38)?;
            partial.update_batch(&[Arc::new(values)], &[1, 1, 2, 2], None, 3)?;
            let state = partial.state(EmitTo::All)?;

            // Rebuilding the logical values models a columnar shuffle's row roundtrip:
            // overflow stays null, but the null count's underlying payload becomes 0.
            let sums = state[0]
                .as_primitive::<Decimal128Type>()
                .iter()
                .collect::<Decimal128Array>()
                .with_precision_and_scale(38, 38)?;
            let counts = state[1]
                .as_primitive::<Int64Type>()
                .iter()
                .collect::<Int64Array>();
            assert!(counts.is_null(2));
            assert_eq!(counts.value(2), 0);

            let mut final_acc = new_acc();
            final_acc.merge_batch(&[Arc::new(sums), Arc::new(counts)], &[0, 1, 2], None, 3)?;
            let empty = final_acc.evaluate(EmitTo::First(2))?;
            assert_eq!(empty.len(), 2);
            assert_eq!(empty.null_count(), 2);

            let result = final_acc.evaluate(EmitTo::All);
            if mode == EvalMode::Ansi {
                let error = result.unwrap_err().to_string();
                assert!(error.contains("ARITHMETIC_OVERFLOW"), "{error}");
            } else {
                let result = result?;
                assert_eq!(result.len(), 1);
                assert!(result.is_null(0));
            }
        }
        Ok(())
    }
}
