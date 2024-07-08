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

use arrow::{
    array::BooleanBufferBuilder,
    buffer::{BooleanBuffer, NullBuffer},
};
use arrow_array::{
    cast::AsArray, types::Decimal128Type, Array, ArrayRef, BooleanArray, Decimal128Array,
};
use arrow_data::decimal::validate_decimal_precision;
use arrow_schema::{DataType, Field};
use datafusion::logical_expr::{Accumulator, EmitTo, GroupsAccumulator};
use datafusion_common::{Result as DFResult, ScalarValue};
use datafusion_physical_expr::{aggregate::utils::down_cast_any_ref, AggregateExpr, PhysicalExpr};
use std::{any::Any, ops::BitAnd, sync::Arc};

use crate::unlikely;

#[derive(Debug)]
pub struct SumDecimal {
    name: String,
    expr: Arc<dyn PhysicalExpr>,

    /// The data type of the SUM result
    result_type: DataType,

    /// Decimal precision and scale
    precision: u8,
    scale: i8,

    /// Whether the result is nullable
    nullable: bool,
}

impl SumDecimal {
    pub fn new(name: impl Into<String>, expr: Arc<dyn PhysicalExpr>, data_type: DataType) -> Self {
        // The `data_type` is the SUM result type passed from Spark side
        let (precision, scale) = match data_type {
            DataType::Decimal128(p, s) => (p, s),
            _ => unreachable!(),
        };
        Self {
            name: name.into(),
            expr,
            result_type: data_type,
            precision,
            scale,
            nullable: true,
        }
    }
}

impl AggregateExpr for SumDecimal {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> DFResult<Field> {
        Ok(Field::new(
            &self.name,
            self.result_type.clone(),
            self.nullable,
        ))
    }

    fn create_accumulator(&self) -> DFResult<Box<dyn Accumulator>> {
        Ok(Box::new(SumDecimalAccumulator::new(
            self.precision,
            self.scale,
        )))
    }

    fn state_fields(&self) -> DFResult<Vec<Field>> {
        let fields = vec![
            Field::new(&self.name, self.result_type.clone(), self.nullable),
            Field::new("is_empty", DataType::Boolean, false),
        ];
        Ok(fields)
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn groups_accumulator_supported(&self) -> bool {
        true
    }

    fn create_groups_accumulator(&self) -> DFResult<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(SumDecimalGroupsAccumulator::new(
            self.result_type.clone(),
            self.precision,
            self.scale,
        )))
    }
}

impl PartialEq<dyn Any> for SumDecimal {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.name == x.name
                    && self.precision == x.precision
                    && self.scale == x.scale
                    && self.nullable == x.nullable
                    && self.result_type == x.result_type
                    && self.expr.eq(&x.expr)
            })
            .unwrap_or(false)
    }
}

#[derive(Debug)]
struct SumDecimalAccumulator {
    sum: i128,
    is_empty: bool,
    is_not_null: bool,

    precision: u8,
    scale: i8,
}

impl SumDecimalAccumulator {
    fn new(precision: u8, scale: i8) -> Self {
        Self {
            sum: 0,
            is_empty: true,
            is_not_null: true,
            precision,
            scale,
        }
    }

    fn update_single(&mut self, values: &Decimal128Array, idx: usize) {
        let v = unsafe { values.value_unchecked(idx) };
        let (new_sum, is_overflow) = self.sum.overflowing_add(v);

        if is_overflow || validate_decimal_precision(new_sum, self.precision).is_err() {
            // Overflow: set buffer accumulator to null
            self.is_not_null = false;
            return;
        }

        self.sum = new_sum;
        self.is_not_null = true;
    }
}

impl Accumulator for SumDecimalAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        assert_eq!(
            values.len(),
            1,
            "Expect only one element in 'values' but found {}",
            values.len()
        );

        if !self.is_empty && !self.is_not_null {
            // This means there's a overflow in decimal, so we will just skip the rest
            // of the computation
            return Ok(());
        }

        let values = &values[0];
        let data = values.as_primitive::<Decimal128Type>();

        self.is_empty = self.is_empty && values.len() == values.null_count();

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

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        // For each group:
        //   1. if `is_empty` is true, it means either there is no value or all values for the group
        //      are null, in this case we'll return null
        //   2. if `is_empty` is false, but `null_state` is true, it means there's an overflow. In
        //      non-ANSI mode Spark returns null.
        if self.is_empty || !self.is_not_null {
            ScalarValue::new_primitive::<Decimal128Type>(
                None,
                &DataType::Decimal128(self.precision, self.scale),
            )
        } else {
            ScalarValue::try_new_decimal128(self.sum, self.precision, self.scale)
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        let sum = if self.is_not_null {
            ScalarValue::try_new_decimal128(self.sum, self.precision, self.scale)?
        } else {
            ScalarValue::new_primitive::<Decimal128Type>(
                None,
                &DataType::Decimal128(self.precision, self.scale),
            )?
        };
        Ok(vec![sum, ScalarValue::from(self.is_empty)])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        assert_eq!(
            states.len(),
            2,
            "Expect two element in 'states' but found {}",
            states.len()
        );
        assert_eq!(states[0].len(), 1);
        assert_eq!(states[1].len(), 1);

        let that_sum = states[0].as_primitive::<Decimal128Type>();
        let that_is_empty = states[1].as_any().downcast_ref::<BooleanArray>().unwrap();

        let this_overflow = !self.is_empty && !self.is_not_null;
        let that_overflow = !that_is_empty.value(0) && that_sum.is_null(0);

        self.is_not_null = !this_overflow && !that_overflow;
        self.is_empty = self.is_empty && that_is_empty.value(0);

        if self.is_not_null {
            self.sum += that_sum.value(0);
        }

        Ok(())
    }
}

struct SumDecimalGroupsAccumulator {
    // Whether aggregate buffer for a particular group is null. True indicates it is not null.
    is_not_null: BooleanBufferBuilder,
    is_empty: BooleanBufferBuilder,
    sum: Vec<i128>,
    result_type: DataType,
    precision: u8,
    scale: i8,
}

impl SumDecimalGroupsAccumulator {
    fn new(result_type: DataType, precision: u8, scale: i8) -> Self {
        Self {
            is_not_null: BooleanBufferBuilder::new(0),
            is_empty: BooleanBufferBuilder::new(0),
            sum: Vec::new(),
            result_type,
            precision,
            scale,
        }
    }

    fn is_overflow(&self, index: usize) -> bool {
        !self.is_empty.get_bit(index) && !self.is_not_null.get_bit(index)
    }

    fn update_single(&mut self, group_index: usize, value: i128) {
        if unlikely(self.is_overflow(group_index)) {
            // This means there's a overflow in decimal, so we will just skip the rest
            // of the computation
            return;
        }

        self.is_empty.set_bit(group_index, false);
        let (new_sum, is_overflow) = self.sum[group_index].overflowing_add(value);

        if is_overflow || validate_decimal_precision(new_sum, self.precision).is_err() {
            // Overflow: set buffer accumulator to null
            self.is_not_null.set_bit(group_index, false);
            return;
        }

        self.sum[group_index] = new_sum;
        self.is_not_null.set_bit(group_index, true)
    }
}

fn ensure_bit_capacity(builder: &mut BooleanBufferBuilder, capacity: usize) {
    if builder.len() < capacity {
        let additional = capacity - builder.len();
        builder.append_n(additional, true);
    }
}

/// Build a boolean buffer from the state and reset the state, based on the emit_to
/// strategy.
fn build_bool_state(state: &mut BooleanBufferBuilder, emit_to: &EmitTo) -> BooleanBuffer {
    let bool_state: BooleanBuffer = state.finish();

    match emit_to {
        EmitTo::All => bool_state,
        EmitTo::First(n) => {
            // split off the first N values in bool_state
            let first_n_bools: BooleanBuffer = bool_state.iter().take(*n).collect();
            // reset the existing seen buffer
            for seen in bool_state.iter().skip(*n) {
                state.append(seen);
            }
            first_n_bools
        }
    }
}

impl GroupsAccumulator for SumDecimalGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> DFResult<()> {
        assert!(opt_filter.is_none(), "opt_filter is not supported yet");
        assert_eq!(values.len(), 1);
        let values = values[0].as_primitive::<Decimal128Type>();
        let data = values.values();

        // Update size for the accumulate states
        self.sum.resize(total_num_groups, 0);
        ensure_bit_capacity(&mut self.is_empty, total_num_groups);
        ensure_bit_capacity(&mut self.is_not_null, total_num_groups);

        let iter = group_indices.iter().zip(data.iter());
        if values.null_count() == 0 {
            for (&group_index, &value) in iter {
                self.update_single(group_index, value);
            }
        } else {
            for (idx, (&group_index, &value)) in iter.enumerate() {
                if values.is_null(idx) {
                    continue;
                }
                self.update_single(group_index, value);
            }
        }

        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> DFResult<ArrayRef> {
        // For each group:
        //   1. if `is_empty` is true, it means either there is no value or all values for the group
        //      are null, in this case we'll return null
        //   2. if `is_empty` is false, but `null_state` is true, it means there's an overflow. In
        //      non-ANSI mode Spark returns null.
        let nulls = build_bool_state(&mut self.is_not_null, &emit_to);
        let is_empty = build_bool_state(&mut self.is_empty, &emit_to);
        let x = (!&is_empty).bitand(&nulls);

        let result = emit_to.take_needed(&mut self.sum);
        let result = Decimal128Array::new(result.into(), Some(NullBuffer::new(x)))
            .with_data_type(self.result_type.clone());

        Ok(Arc::new(result))
    }

    fn state(&mut self, emit_to: EmitTo) -> DFResult<Vec<ArrayRef>> {
        let nulls = build_bool_state(&mut self.is_not_null, &emit_to);
        let nulls = Some(NullBuffer::new(nulls));

        let sum = emit_to.take_needed(&mut self.sum);
        let sum = Decimal128Array::new(sum.into(), nulls.clone())
            .with_data_type(self.result_type.clone());

        let is_empty = build_bool_state(&mut self.is_empty, &emit_to);
        let is_empty = BooleanArray::new(is_empty, None);

        Ok(vec![
            Arc::new(sum) as ArrayRef,
            Arc::new(is_empty) as ArrayRef,
        ])
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> DFResult<()> {
        assert_eq!(
            values.len(),
            2,
            "Expected two arrays: 'sum' and 'is_empty', but found {}",
            values.len()
        );
        assert!(opt_filter.is_none(), "opt_filter is not supported yet");

        // Make sure we have enough capacity for the additional groups
        self.sum.resize(total_num_groups, 0);
        ensure_bit_capacity(&mut self.is_empty, total_num_groups);
        ensure_bit_capacity(&mut self.is_not_null, total_num_groups);

        let that_sum = &values[0];
        let that_sum = that_sum.as_primitive::<Decimal128Type>();
        let that_is_empty = &values[1];
        let that_is_empty = that_is_empty
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();

        group_indices
            .iter()
            .enumerate()
            .for_each(|(idx, &group_index)| unsafe {
                let this_overflow = self.is_overflow(group_index);
                let that_is_empty = that_is_empty.value_unchecked(idx);
                let that_overflow = !that_is_empty && that_sum.is_null(idx);
                let is_overflow = this_overflow || that_overflow;

                // This part follows the logic in Spark:
                //   `org.apache.spark.sql.catalyst.expressions.aggregate.Sum`
                self.is_not_null.set_bit(group_index, !is_overflow);
                self.is_empty.set_bit(
                    group_index,
                    self.is_empty.get_bit(group_index) && that_is_empty,
                );
                if !is_overflow {
                    // .. otherwise, the sum value for this particular index must not be null,
                    // and thus we merge both values and update this sum.
                    self.sum[group_index] += that_sum.value_unchecked(idx);
                }
            });

        Ok(())
    }

    fn size(&self) -> usize {
        self.sum.capacity() * std::mem::size_of::<i128>()
            + self.is_empty.capacity() / 8
            + self.is_not_null.capacity() / 8
    }
}
