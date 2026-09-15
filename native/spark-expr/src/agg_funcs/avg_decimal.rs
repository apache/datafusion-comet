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
            Decimal128(target_precision, target_scale) => Ok(ScalarValue::Decimal128(
                None,
                *target_precision,
                *target_scale,
            )),
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
    /// Some(0) with a zero count is empty; None is a sticky overflow marker.
    sum: Option<i128>,
    count: i64,
    sum_scale: i8,
    sum_precision: u8,
    target_precision: u8,
    target_scale: i8,
    eval_mode: EvalMode,
    expr_id: Option<u64>,
    registry: Arc<crate::QueryContextMap>,
}

impl AvgDecimalAccumulator {
    /// Create an empty sum/count buffer. Precisions and scales describe unscaled i128
    /// sums and the decimal result; the shared registry supplies optional error context.
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
            sum: Some(0),
            count: 0,
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

    /// Add one non-null, unscaled input at a valid index. Overflow nulls the sum
    /// permanently; evaluation reports the ANSI error after aggregation finishes.
    fn update_single(&mut self, values: &Decimal128Array, idx: usize) {
        let value = unsafe { values.value_unchecked(idx) };
        self.sum = self
            .sum
            .and_then(|sum| sum.checked_add(value))
            .filter(|sum| is_valid_decimal_precision(*sum, self.sum_precision));
        self.count += 1;
    }
}

impl Accumulator for AvgDecimalAccumulator {
    /// Return Spark's sum/count buffer: zero for an empty sum, null for overflow.
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::Decimal128(self.sum, self.sum_precision, self.sum_scale),
            ScalarValue::from(self.count),
        ])
    }

    /// Accumulate non-null decimal inputs in place; overflow is deferred to evaluation.
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if self.sum.is_none() {
            return Ok(());
        }
        let values = &values[0];
        let data = values.as_primitive::<Decimal128Type>();
        if values.null_count() == 0 {
            for i in 0..data.len() {
                self.update_single(data, i);
            }
        } else {
            for i in 0..data.len() {
                if data.is_null(i) {
                    continue;
                }
                self.update_single(data, i);
            }
        }
        Ok(())
    }

    /// Merge partial sum/count arrays, preserving null sums as overflow markers.
    /// Empty partials carry zero sums; grouped overflow may also carry a null count.
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let partial_sums = states[0].as_primitive::<Decimal128Type>();
        let partial_counts = states[1].as_primitive::<Int64Type>();
        self.count += sum(partial_counts).unwrap_or_default();

        if partial_sums.null_count() > 0 {
            self.sum = None;
        } else {
            self.sum = self
                .sum
                .and_then(|value| value.checked_add(sum(partial_sums).unwrap_or_default()))
                .filter(|value| is_valid_decimal_precision(*value, self.sum_precision));
        }
        Ok(())
    }

    /// Return the average without consuming state. Empty input returns null; overflow
    /// returns an ANSI error or null in other modes, even if its count was null in transit.
    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.sum.is_none() && self.eval_mode == EvalMode::Ansi {
            let error = decimal_sum_overflow_error("avg");
            return Err(self.wrap_error_with_context(error));
        }
        if self.count == 0 {
            return Ok(ScalarValue::Decimal128(
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
            .and_then(|sum| avg(sum, self.count as i128, target_min, target_max, scaler));
        Ok(ScalarValue::Decimal128(
            result,
            self.target_precision,
            self.target_scale,
        ))
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

    /// Merge parallel sum/count arrays into the indexed groups. Sum nulls also cover
    /// null counts: native state shares their validity buffer and Spark counts are valid.
    /// Keep overflow in the group state and defer ANSI errors until evaluation.
    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
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
                // Preserve overflow in intermediate/spilled state without throwing. Like
                // Spark's decimal AVG, report ANSI overflow only when evaluate emits the
                // affected group; a prefix emission may not need this group yet.
                self.is_not_null.set_bit(group_index, false);
            } else {
                self.sums[group_index] = new_sum;
            }
        }
        Ok(())
    }

    /// Consume the requested group prefix and return its averages. Empty groups return
    /// null; overflow returns null or an ANSI error after the prefix has been consumed.
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

    fn convert_to_state(
        &self,
        _values: &[ArrayRef],
        _opt_filter: Option<&arrow::array::BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        not_impl_err!("Input batch conversion to state not implemented")
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

    /// Make an empty accumulator with identical sum/result scales so test values do
    /// not need rescaling. The mode controls whether overflow evaluates to an error.
    fn accumulator(mode: EvalMode) -> AvgDecimalAccumulator {
        AvgDecimalAccumulator::new(
            38,
            38,
            38,
            38,
            mode,
            None,
            crate::create_query_context_map(),
        )
    }

    /// Empty/all-null partials emit zero buffers; merging them still evaluates to null.
    #[test]
    fn empty_partial_state_matches_spark() -> Result<()> {
        let mut partial = accumulator(EvalMode::Ansi);
        let empty = vec![
            ScalarValue::Decimal128(Some(0), 38, 38),
            ScalarValue::Int64(Some(0)),
        ];
        assert_eq!(partial.state()?, empty);
        let values = Decimal128Array::from(vec![None, None]).with_precision_and_scale(38, 38)?;
        partial.update_batch(&[Arc::new(values)])?;
        assert_eq!(partial.state()?, empty);

        let state = partial
            .state()?
            .into_iter()
            .map(|value| value.to_array_of_size(1))
            .collect::<Result<Vec<_>>>()?;
        let mut final_acc = accumulator(EvalMode::Ansi);
        final_acc.merge_batch(&state)?;
        assert_eq!(final_acc.evaluate()?, ScalarValue::Decimal128(None, 38, 38));

        let values = Decimal128Array::from(vec![100, 300]).with_precision_and_scale(38, 38)?;
        final_acc.update_batch(&[Arc::new(values)])?;
        final_acc.merge_batch(&state)?;
        assert_eq!(
            final_acc.evaluate()?,
            ScalarValue::Decimal128(Some(200), 38, 38)
        );
        Ok(())
    }

    /// A compensating value in the overflow batch cannot revive the sum. Its null
    /// marker survives a final merge followed by a valid empty partial.
    #[test]
    fn scalar_overflow_stays_null() -> Result<()> {
        for mode in [EvalMode::Ansi, EvalMode::Legacy] {
            let mut partial = accumulator(mode);
            let max = 10_i128.pow(38) - 1;
            let values =
                Decimal128Array::from(vec![max, max, -max]).with_precision_and_scale(38, 38)?;
            partial.update_batch(&[Arc::new(values)])?;
            let state = partial.state()?;
            assert_eq!(state[0], ScalarValue::Decimal128(None, 38, 38));
            let state = state
                .into_iter()
                .map(|value| value.to_array_of_size(1))
                .collect::<Result<Vec<_>>>()?;
            let mut final_acc = accumulator(mode);
            final_acc.merge_batch(&state)?;
            let empty = accumulator(mode)
                .state()?
                .into_iter()
                .map(|value| value.to_array_of_size(1))
                .collect::<Result<Vec<_>>>()?;
            final_acc.merge_batch(&empty)?;
            if mode == EvalMode::Ansi {
                let error = final_acc.evaluate().unwrap_err().to_string();
                assert!(error.contains("ARITHMETIC_OVERFLOW"), "{error}");
            } else {
                assert_eq!(final_acc.evaluate()?, ScalarValue::Decimal128(None, 38, 38));
            }
        }
        Ok(())
    }

    /// Individually valid partial sums can overflow the decimal precision on merge.
    #[test]
    fn scalar_merge_overflow() -> Result<()> {
        let mut acc = accumulator(EvalMode::Ansi);
        let sums =
            Decimal128Array::from(vec![6 * 10_i128.pow(37); 2]).with_precision_and_scale(38, 38)?;
        acc.merge_batch(&[Arc::new(sums), Arc::new(Int64Array::from(vec![1, 1]))])?;
        assert_eq!(acc.state()?[0], ScalarValue::Decimal128(None, 38, 38));
        let error = acc.evaluate().unwrap_err().to_string();
        assert!(error.contains("ARITHMETIC_OVERFLOW"), "{error}");
        Ok(())
    }

    /// Grouped merge overflow survives a shuffle that zeroes null count payloads and
    /// later updates. ANSI evaluation fails only when the affected group is emitted.
    #[test]
    fn grouped_merge_overflow_survives_roundtrip() -> Result<()> {
        for mode in [EvalMode::Ansi, EvalMode::Legacy] {
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
            let large = 6 * 10_i128.pow(37);
            // Group 0 is empty, group 1 is valid, and group 2 overflows during merge.
            let sums = Decimal128Array::from(vec![0, 100, large, large])
                .with_precision_and_scale(38, 38)?;
            partial.merge_batch(
                &[Arc::new(sums), Arc::new(Int64Array::from(vec![0, 1, 1, 1]))],
                &[0, 1, 2, 2],
                3,
            )?;
            let state = partial.state(EmitTo::All)?;
            // Rebuilding logical values models a shuffle: null payloads become zero.
            let sums = state[0]
                .as_primitive::<Decimal128Type>()
                .iter()
                .collect::<Decimal128Array>()
                .with_precision_and_scale(38, 38)?;
            let counts = state[1]
                .as_primitive::<Int64Type>()
                .iter()
                .collect::<Int64Array>();
            assert!(sums.is_null(2));
            assert!(counts.is_null(2));
            assert_eq!(counts.value(2), 0);
            let state: Vec<ArrayRef> = vec![Arc::new(sums), Arc::new(counts)];

            // A scalar final also treats a null sum/count as overflow, not empty input.
            let mut scalar = accumulator(mode);
            scalar.merge_batch(&[state[0].slice(2, 1), state[1].slice(2, 1)])?;
            assert_eq!(scalar.state()?[0], ScalarValue::Decimal128(None, 38, 38));
            if mode == EvalMode::Ansi {
                let error = scalar.evaluate().unwrap_err().to_string();
                assert!(error.contains("ARITHMETIC_OVERFLOW"), "{error}");
            } else {
                assert_eq!(scalar.evaluate()?, ScalarValue::Decimal128(None, 38, 38));
            }

            let mut final_acc = new_acc();
            final_acc.merge_batch(&state, &[0, 1, 2], 3)?;
            let prefix = final_acc.evaluate(EmitTo::First(2))?;
            assert_eq!(
                prefix
                    .as_primitive::<Decimal128Type>()
                    .iter()
                    .collect::<Vec<_>>(),
                vec![None, Some(100)]
            );
            // Prefix emission shifts the overflowing group to index 0.
            let values = Decimal128Array::from(vec![-large]).with_precision_and_scale(38, 38)?;
            final_acc.update_batch(&[Arc::new(values)], &[0], None, 1)?;
            if mode == EvalMode::Ansi {
                let error = final_acc.evaluate(EmitTo::All).unwrap_err().to_string();
                assert!(error.contains("ARITHMETIC_OVERFLOW"), "{error}");
            } else {
                let result = final_acc.evaluate(EmitTo::All)?;
                assert_eq!(result.len(), 1);
                assert!(result.is_null(0));
            }
        }
        Ok(())
    }
}
