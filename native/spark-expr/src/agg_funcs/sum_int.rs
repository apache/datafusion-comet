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
use datafusion::common::{DataFusionError, Result as DFResult, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::Volatility::Immutable;
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, EmitTo, GroupsAccumulator, ReversedUDAF, Signature,
};
use std::{any::Any, sync::Arc};

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
    fn as_any(&self) -> &dyn Any {
        self
    }

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
        ReversedUDAF::Identical
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
        if states.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "Invalid state while merging batch. Expected 1 element but found {}",
                states.len()
            )));
        }

        let that_sum_array = states[0].as_primitive::<Int64Type>();
        let that_sum = if that_sum_array.is_null(0) {
            None
        } else {
            Some(that_sum_array.value(0))
        };

        if that_sum.is_none() {
            return Ok(());
        }
        if self.sum.is_none() {
            self.sum = that_sum;
            return Ok(());
        }

        self.sum = Some(self.sum.unwrap().add_wrapping(that_sum.unwrap()));
        Ok(())
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
        if states.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "Invalid state while merging batch. Expected 1 element but found {}",
                states.len()
            )));
        }

        let that_sum_array = states[0].as_primitive::<Int64Type>();
        let that_sum = if that_sum_array.is_null(0) {
            None
        } else {
            Some(that_sum_array.value(0))
        };

        if that_sum.is_none() {
            return Ok(());
        }
        if self.sum.is_none() {
            self.sum = that_sum;
            return Ok(());
        }

        self.sum = Some(
            self.sum
                .unwrap()
                .add_checked(that_sum.unwrap())
                .map_err(|_| DataFusionError::from(arithmetic_overflow_error("integer")))?,
        );
        Ok(())
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
        let that_sum = if that_sum_array.is_null(0) {
            None
        } else {
            Some(that_sum_array.value(0))
        };
        let that_has_all_nulls = states[1].as_boolean().value(0);

        let that_overflowed = !that_has_all_nulls && that_sum.is_none();
        if that_overflowed || self.overflowed() {
            self.sum = None;
            self.has_all_nulls = false;
            return Ok(());
        }

        if that_has_all_nulls {
            return Ok(());
        }
        if self.has_all_nulls {
            self.sum = that_sum;
            self.has_all_nulls = false;
            return Ok(());
        }

        // Both sides have non-null values
        match self.sum.unwrap().add_checked(that_sum.unwrap()) {
            Ok(v) => self.sum = Some(v),
            Err(_) => {
                self.sum = None;
                self.has_all_nulls = false;
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
        ) -> DFResult<()>
        where
            T: ArrowPrimitiveType,
            T::Native: ArrowNativeType,
        {
            for (i, &group_index) in group_indices.iter().enumerate() {
                if !int_array.is_null(i) {
                    let v = int_array.value(i).to_i64().ok_or_else(|| {
                        DataFusionError::Internal("Failed to convert value to i64".to_string())
                    })?;
                    sums[group_index] = Some(sums[group_index].unwrap_or(0).add_wrapping(v));
                }
            }
            Ok(())
        }

        debug_assert!(opt_filter.is_none(), "opt_filter is not supported yet");
        let values = &values[0];
        self.sums.resize(total_num_groups, None);

        match values.data_type() {
            DataType::Int64 => update_groups_sum(
                as_primitive_array::<Int64Type>(values),
                group_indices,
                &mut self.sums,
            )?,
            DataType::Int32 => update_groups_sum(
                as_primitive_array::<Int32Type>(values),
                group_indices,
                &mut self.sums,
            )?,
            DataType::Int16 => update_groups_sum(
                as_primitive_array::<Int16Type>(values),
                group_indices,
                &mut self.sums,
            )?,
            DataType::Int8 => update_groups_sum(
                as_primitive_array::<Int8Type>(values),
                group_indices,
                &mut self.sums,
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
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> DFResult<()> {
        debug_assert!(opt_filter.is_none(), "opt_filter is not supported yet");

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
        ) -> DFResult<()>
        where
            T: ArrowPrimitiveType,
            T::Native: ArrowNativeType,
        {
            for (i, &group_index) in group_indices.iter().enumerate() {
                if !int_array.is_null(i) {
                    let v = int_array.value(i).to_i64().ok_or_else(|| {
                        DataFusionError::Internal("Failed to convert value to i64".to_string())
                    })?;
                    sums[group_index] = Some(
                        sums[group_index]
                            .unwrap_or(0)
                            .add_checked(v)
                            .map_err(|_| {
                                DataFusionError::from(arithmetic_overflow_error("integer"))
                            })?,
                    );
                }
            }
            Ok(())
        }

        debug_assert!(opt_filter.is_none(), "opt_filter is not supported yet");
        let values = &values[0];
        self.sums.resize(total_num_groups, None);

        match values.data_type() {
            DataType::Int64 => update_groups_sum(
                as_primitive_array::<Int64Type>(values),
                group_indices,
                &mut self.sums,
            )?,
            DataType::Int32 => update_groups_sum(
                as_primitive_array::<Int32Type>(values),
                group_indices,
                &mut self.sums,
            )?,
            DataType::Int16 => update_groups_sum(
                as_primitive_array::<Int16Type>(values),
                group_indices,
                &mut self.sums,
            )?,
            DataType::Int8 => update_groups_sum(
                as_primitive_array::<Int8Type>(values),
                group_indices,
                &mut self.sums,
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
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> DFResult<()> {
        debug_assert!(opt_filter.is_none(), "opt_filter is not supported yet");

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
        ) -> DFResult<()>
        where
            T: ArrowPrimitiveType,
            T::Native: ArrowNativeType,
        {
            for (i, &group_index) in group_indices.iter().enumerate() {
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

        debug_assert!(opt_filter.is_none(), "opt_filter is not supported yet");
        let values = &values[0];
        self.sums.resize(total_num_groups, Some(0));
        self.has_all_nulls.resize(total_num_groups, true);

        match values.data_type() {
            DataType::Int64 => update_groups_sum(
                as_primitive_array::<Int64Type>(values),
                group_indices,
                &mut self.sums,
                &mut self.has_all_nulls,
            )?,
            DataType::Int32 => update_groups_sum(
                as_primitive_array::<Int32Type>(values),
                group_indices,
                &mut self.sums,
                &mut self.has_all_nulls,
            )?,
            DataType::Int16 => update_groups_sum(
                as_primitive_array::<Int16Type>(values),
                group_indices,
                &mut self.sums,
                &mut self.has_all_nulls,
            )?,
            DataType::Int8 => update_groups_sum(
                as_primitive_array::<Int8Type>(values),
                group_indices,
                &mut self.sums,
                &mut self.has_all_nulls,
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
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> DFResult<()> {
        debug_assert!(opt_filter.is_none(), "opt_filter is not supported yet");

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
            match self.sums[group_index].unwrap().add_checked(that_sum.unwrap()) {
                Ok(v) => self.sums[group_index] = Some(v),
                Err(_) => {
                    self.sums[group_index] = None;
                    self.has_all_nulls[group_index] = false;
                }
            }
        }
        Ok(())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}
