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
    make_array, new_null_array, Array, ArrayData, ArrayRef, AsArray, BinaryBuilder,
    BinaryViewBuilder, BooleanArray, LargeBinaryBuilder, LargeStringBuilder, ListArray,
    MutableArrayData, PrimitiveArray, StringBuilder, StringViewBuilder, StructArray,
};
use arrow::compute::cast;
use arrow::datatypes::{
    DataType, Date32Type, Date64Type, Decimal128Type, Decimal256Type, Decimal32Type,
    Decimal64Type, DurationMicrosecondType, DurationMillisecondType, DurationNanosecondType,
    DurationSecondType, Field, FieldRef, Float16Type, Float32Type, Float64Type, Int16Type,
    Int32Type, Int64Type, Int8Type, Time32MillisecondType, Time32SecondType,
    Time64MicrosecondType, Time64NanosecondType, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt16Type,
    UInt32Type, UInt64Type, UInt8Type,
};
use arrow::row::{OwnedRow, RowConverter, Rows, SortField};
use datafusion::common::{internal_err, not_impl_err, Result, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, EmitTo, GroupsAccumulator, Signature, Volatility,
};
use datafusion::physical_expr::expressions::format_state_name;
use datafusion::common::scalar::{copy_array_data, partial_cmp_struct};
use std::any::Any;
use std::cmp::Ordering;
use std::mem::{size_of, size_of_val};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MaxMinByKind {
    Max,
    Min,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaxMinBy {
    name: String,
    signature: Signature,
    value_type: DataType,
    order_type: DataType,
    kind: MaxMinByKind,
}

impl std::hash::Hash for MaxMinBy {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.signature.hash(state);
        self.value_type.hash(state);
        self.order_type.hash(state);
        self.kind.hash(state);
    }
}

impl MaxMinBy {
    pub fn new(
        name: impl Into<String>,
        value_type: DataType,
        order_type: DataType,
        kind: MaxMinByKind,
    ) -> Self {
        Self {
            name: name.into(),
            signature: Signature::any(2, Volatility::Immutable),
            value_type,
            order_type,
            kind,
        }
    }
}

impl AggregateUDFImpl for MaxMinBy {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.value_type.clone())
    }

    fn default_value(&self, data_type: &DataType) -> Result<ScalarValue> {
        ScalarValue::try_from(data_type)
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(MaxMinByAccumulator::new(
            self.value_type.clone(),
            self.order_type.clone(),
            self.kind,
        )))
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        use DataType::*;
        use TimeUnit::*;

        let value_type = self.value_type.clone();
        let order_type = self.order_type.clone();
        let kind = self.kind;

        let acc: Box<dyn GroupsAccumulator> = match &order_type {
            Int8 => Box::new(PrimitiveOrderMaxMinByGroupsAccumulator::<Int8Type>::new(
                value_type, order_type, kind,
            )),
            Int16 => Box::new(PrimitiveOrderMaxMinByGroupsAccumulator::<Int16Type>::new(
                value_type, order_type, kind,
            )),
            Int32 => Box::new(PrimitiveOrderMaxMinByGroupsAccumulator::<Int32Type>::new(
                value_type, order_type, kind,
            )),
            Int64 => Box::new(PrimitiveOrderMaxMinByGroupsAccumulator::<Int64Type>::new(
                value_type, order_type, kind,
            )),
            UInt8 => Box::new(PrimitiveOrderMaxMinByGroupsAccumulator::<UInt8Type>::new(
                value_type, order_type, kind,
            )),
            UInt16 => Box::new(PrimitiveOrderMaxMinByGroupsAccumulator::<UInt16Type>::new(
                value_type, order_type, kind,
            )),
            UInt32 => Box::new(PrimitiveOrderMaxMinByGroupsAccumulator::<UInt32Type>::new(
                value_type, order_type, kind,
            )),
            UInt64 => Box::new(PrimitiveOrderMaxMinByGroupsAccumulator::<UInt64Type>::new(
                value_type, order_type, kind,
            )),
            Float16 => Box::new(PrimitiveOrderMaxMinByGroupsAccumulator::<Float16Type>::new(
                value_type, order_type, kind,
            )),
            Float32 => Box::new(PrimitiveOrderMaxMinByGroupsAccumulator::<Float32Type>::new(
                value_type, order_type, kind,
            )),
            Float64 => Box::new(PrimitiveOrderMaxMinByGroupsAccumulator::<Float64Type>::new(
                value_type, order_type, kind,
            )),
            Date32 => Box::new(PrimitiveOrderMaxMinByGroupsAccumulator::<Date32Type>::new(
                value_type, order_type, kind,
            )),
            Date64 => Box::new(PrimitiveOrderMaxMinByGroupsAccumulator::<Date64Type>::new(
                value_type, order_type, kind,
            )),
            Time32(Second) => Box::new(
                PrimitiveOrderMaxMinByGroupsAccumulator::<Time32SecondType>::new(
                    value_type, order_type, kind,
                ),
            ),
            Time32(Millisecond) => Box::new(
                PrimitiveOrderMaxMinByGroupsAccumulator::<Time32MillisecondType>::new(
                    value_type, order_type, kind,
                ),
            ),
            Time64(Microsecond) => Box::new(
                PrimitiveOrderMaxMinByGroupsAccumulator::<Time64MicrosecondType>::new(
                    value_type, order_type, kind,
                ),
            ),
            Time64(Nanosecond) => Box::new(
                PrimitiveOrderMaxMinByGroupsAccumulator::<Time64NanosecondType>::new(
                    value_type, order_type, kind,
                ),
            ),
            Timestamp(Second, _) => Box::new(
                PrimitiveOrderMaxMinByGroupsAccumulator::<TimestampSecondType>::new(
                    value_type, order_type, kind,
                ),
            ),
            Timestamp(Millisecond, _) => Box::new(
                PrimitiveOrderMaxMinByGroupsAccumulator::<TimestampMillisecondType>::new(
                    value_type, order_type, kind,
                ),
            ),
            Timestamp(Microsecond, _) => Box::new(
                PrimitiveOrderMaxMinByGroupsAccumulator::<TimestampMicrosecondType>::new(
                    value_type, order_type, kind,
                ),
            ),
            Timestamp(Nanosecond, _) => Box::new(
                PrimitiveOrderMaxMinByGroupsAccumulator::<TimestampNanosecondType>::new(
                    value_type, order_type, kind,
                ),
            ),
            Duration(Second) => Box::new(
                PrimitiveOrderMaxMinByGroupsAccumulator::<DurationSecondType>::new(
                    value_type, order_type, kind,
                ),
            ),
            Duration(Millisecond) => Box::new(
                PrimitiveOrderMaxMinByGroupsAccumulator::<DurationMillisecondType>::new(
                    value_type, order_type, kind,
                ),
            ),
            Duration(Microsecond) => Box::new(
                PrimitiveOrderMaxMinByGroupsAccumulator::<DurationMicrosecondType>::new(
                    value_type, order_type, kind,
                ),
            ),
            Duration(Nanosecond) => Box::new(
                PrimitiveOrderMaxMinByGroupsAccumulator::<DurationNanosecondType>::new(
                    value_type, order_type, kind,
                ),
            ),
            Decimal32(_, _) => Box::new(
                PrimitiveOrderMaxMinByGroupsAccumulator::<Decimal32Type>::new(
                    value_type, order_type, kind,
                ),
            ),
            Decimal64(_, _) => Box::new(
                PrimitiveOrderMaxMinByGroupsAccumulator::<Decimal64Type>::new(
                    value_type, order_type, kind,
                ),
            ),
            Decimal128(_, _) => Box::new(
                PrimitiveOrderMaxMinByGroupsAccumulator::<Decimal128Type>::new(
                    value_type, order_type, kind,
                ),
            ),
            Decimal256(_, _) => Box::new(
                PrimitiveOrderMaxMinByGroupsAccumulator::<Decimal256Type>::new(
                    value_type, order_type, kind,
                ),
            ),
            Utf8 | LargeUtf8 | Utf8View | Binary | LargeBinary | BinaryView => {
                Box::new(BytesOrderMaxMinByGroupsAccumulator::new(
                    value_type, order_type, kind,
                ))
            }
            Struct(_) => Box::new(StructOrderMaxMinByGroupsAccumulator::new(
                value_type, order_type, kind,
            )),
            _ => Box::new(RowOrderMaxMinByGroupsAccumulator::try_new(
                value_type, order_type, kind,
            )?),
        };

        Ok(acc)
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Arc::new(Field::new(
                format_state_name(&self.name, "value"),
                self.value_type.clone(),
                true,
            )),
            Arc::new(Field::new(
                format_state_name(&self.name, "ordering"),
                self.order_type.clone(),
                true,
            )),
        ])
    }
}

#[derive(Debug)]
struct MaxMinByAccumulator {
    value_type: DataType,
    order_type: DataType,
    kind: MaxMinByKind,
    best_value: Option<ScalarValue>,
    best_order: Option<ScalarValue>,
}

impl MaxMinByAccumulator {
    fn new(value_type: DataType, order_type: DataType, kind: MaxMinByKind) -> Self {
        Self {
            value_type,
            order_type,
            kind,
            best_value: None,
            best_order: None,
        }
    }

    fn typed_null(dt: &DataType) -> Result<ScalarValue> {
        ScalarValue::try_from(dt)
    }

    fn should_replace(&self, old_order: &ScalarValue, new_order: &ScalarValue) -> Result<bool> {
        match new_order.partial_cmp(old_order) {
            Some(Ordering::Greater) => Ok(matches!(self.kind, MaxMinByKind::Max)),
            Some(Ordering::Less) => Ok(matches!(self.kind, MaxMinByKind::Min)),
            Some(Ordering::Equal) => Ok(true),
            None => internal_err!(
                "max_by/min_by encountered non-orderable values: old={old_order:?}, new={new_order:?}"
            ),
        }
    }

    fn update_pair(&mut self, value: ScalarValue, order: ScalarValue) -> Result<()> {
        match (&self.best_order, order.is_null()) {
            (None, true) => Ok(()),
            (None, false) => {
                self.best_value = Some(value);
                self.best_order = Some(order);
                Ok(())
            }
            (Some(_), true) => Ok(()),
            (Some(current_order), false) => {
                if self.should_replace(current_order, &order)? {
                    self.best_value = Some(value);
                    self.best_order = Some(order);
                }
                Ok(())
            }
        }
    }
}

impl Accumulator for MaxMinByAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            self.best_value
                .clone()
                .unwrap_or(Self::typed_null(&self.value_type)?),
            self.best_order
                .clone()
                .unwrap_or(Self::typed_null(&self.order_type)?),
        ])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.len() != 2 {
            return internal_err!("max_by/min_by expects exactly 2 arguments");
        }

        for i in 0..values[0].len() {
            let value = ScalarValue::try_from_array(&values[0], i)?;
            let order = ScalarValue::try_from_array(&values[1], i)?;
            self.update_pair(value, order)?;
        }

        Ok(())
    }

    fn retract_batch(&mut self, _values: &[ArrayRef]) -> Result<()> {
        not_impl_err!("retract_batch is not implemented for max_by/min_by")
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.len() != 2 {
            return internal_err!("max_by/min_by state expects exactly 2 arrays");
        }

        for i in 0..states[0].len() {
            let value = ScalarValue::try_from_array(&states[0], i)?;
            let order = ScalarValue::try_from_array(&states[1], i)?;
            self.update_pair(value, order)?;
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(self
            .best_value
            .clone()
            .unwrap_or(Self::typed_null(&self.value_type)?))
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }
}

#[inline]
fn row_passes_filter(opt_filter: Option<&BooleanArray>, idx: usize) -> bool {
    match opt_filter {
        None => true,
        Some(filter) => filter.is_valid(idx) && filter.value(idx),
    }
}

#[inline]
fn copy_single_value(array: &ArrayRef, idx: usize) -> ArrayRef {
    let data = copy_array_data(&array.slice(idx, 1).to_data());
    make_array(data)
}

fn materialize_singleton_arrays(
    arrays: Vec<Option<ArrayRef>>,
    data_type: &DataType,
) -> Result<ArrayRef> {
    let null_array = new_null_array(data_type, 1);
    let mut all_data: Vec<ArrayData> = Vec::with_capacity(arrays.len());

    for arr in arrays {
        match arr {
            Some(arr) => all_data.push(arr.to_data()),
            None => all_data.push(null_array.to_data()),
        }
    }

    let refs: Vec<&ArrayData> = all_data.iter().collect();
    let mut copy = MutableArrayData::new(refs, true, all_data.len());
    for (i, item) in all_data.iter().enumerate() {
        copy.extend(i, 0, item.len());
    }
    Ok(make_array(copy.freeze()))
}

fn dictionary_encode_if_necessary(
    array: &ArrayRef,
    expected: &DataType,
) -> Result<ArrayRef> {
    match (expected, array.data_type()) {
        (DataType::Struct(expected_fields), _) => {
            let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();
            let arrays = expected_fields
                .iter()
                .zip(struct_array.columns())
                .map(|(expected_field, column)| {
                    dictionary_encode_if_necessary(column, expected_field.data_type())
                })
                .collect::<Result<Vec<_>>>()?;

            Ok(Arc::new(StructArray::try_new(
                expected_fields.clone(),
                arrays,
                struct_array.nulls().cloned(),
            )?))
        }
        (DataType::List(expected_field), &DataType::List(_)) => {
            let list = array.as_any().downcast_ref::<ListArray>().unwrap();

            Ok(Arc::new(ListArray::try_new(
                Arc::<arrow::datatypes::Field>::clone(expected_field),
                list.offsets().clone(),
                dictionary_encode_if_necessary(
                    list.values(),
                    expected_field.data_type(),
                )?,
                list.nulls().cloned(),
            )?))
        }
        (DataType::Dictionary(_, _), _) => Ok(cast(array.as_ref(), expected)?),
        (_, _) => Ok(Arc::<dyn Array>::clone(array)),
    }
}

/* -------------------------- Primitive order fast path -------------------------- */

#[derive(Debug)]
struct PrimitiveOrderMaxMinByGroupsAccumulator<T>
where
    T: arrow::datatypes::ArrowPrimitiveType + Send,
{
    value_type: DataType,
    order_type: DataType,
    kind: MaxMinByKind,
    best_orders: Vec<Option<T::Native>>,
    best_values: Vec<Option<ArrayRef>>,
}

impl<T> PrimitiveOrderMaxMinByGroupsAccumulator<T>
where
    T: arrow::datatypes::ArrowPrimitiveType + Send,
    T::Native: PartialOrd + Copy,
{
    fn new(value_type: DataType, order_type: DataType, kind: MaxMinByKind) -> Self {
        Self {
            value_type,
            order_type,
            kind,
            best_orders: vec![],
            best_values: vec![],
        }
    }

    #[inline]
    fn should_replace(&self, old_order: T::Native, new_order: T::Native) -> Result<bool> {
        match new_order.partial_cmp(&old_order) {
            Some(Ordering::Greater) => Ok(matches!(self.kind, MaxMinByKind::Max)),
            Some(Ordering::Less) => Ok(matches!(self.kind, MaxMinByKind::Min)),
            Some(Ordering::Equal) => Ok(true),
            None => internal_err!("max_by/min_by encountered non-orderable primitive values"),
        }
    }

    fn update_arrays(
        &mut self,
        value_arr: &ArrayRef,
        order_arr: &PrimitiveArray<T>,
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.best_orders.resize(total_num_groups, None);
        self.best_values.resize(total_num_groups, None);

        for (i, &group_idx) in group_indices.iter().enumerate() {
            if !row_passes_filter(opt_filter, i) || order_arr.is_null(i) {
                continue;
            }

            let new_order = order_arr.value(i);
            let replace = match self.best_orders[group_idx] {
                None => true,
                Some(old_order) => self.should_replace(old_order, new_order)?,
            };

            if replace {
                self.best_orders[group_idx] = Some(new_order);
                self.best_values[group_idx] = Some(copy_single_value(value_arr, i));
            }
        }

        Ok(())
    }
}

impl<T> GroupsAccumulator for PrimitiveOrderMaxMinByGroupsAccumulator<T>
where
    T: arrow::datatypes::ArrowPrimitiveType + Send,
    T::Native: PartialOrd + Copy,
{
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        let value_arr = &values[0];
        let order_arr = values[1].as_primitive::<T>();
        self.update_arrays(value_arr, order_arr, group_indices, opt_filter, total_num_groups)
    }

    fn merge_batch(
        &mut self,
        states: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        let value_arr = &states[0];
        let order_arr = states[1].as_primitive::<T>();
        self.update_arrays(value_arr, order_arr, group_indices, opt_filter, total_num_groups)
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let values = emit_to.take_needed(&mut self.best_values);
        materialize_singleton_arrays(values, &self.value_type)
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let values = emit_to.take_needed(&mut self.best_values);
        let orders = emit_to.take_needed(&mut self.best_orders);

        let order_array = PrimitiveArray::<T>::from_iter(orders).with_data_type(self.order_type.clone());

        Ok(vec![
            materialize_singleton_arrays(values, &self.value_type)?,
            Arc::new(order_array),
        ])
    }

    fn size(&self) -> usize {
        self.best_orders.capacity() * size_of::<Option<T::Native>>()
            + self.best_values.capacity() * size_of::<Option<ArrayRef>>()
    }
}

/* ---------------------------- Bytes order fast path ---------------------------- */

#[derive(Debug)]
struct BytesOrderMaxMinByGroupsAccumulator {
    value_type: DataType,
    order_type: DataType,
    kind: MaxMinByKind,
    best_orders: Vec<Option<Vec<u8>>>,
    best_values: Vec<Option<ArrayRef>>,
}

impl BytesOrderMaxMinByGroupsAccumulator {
    fn new(value_type: DataType, order_type: DataType, kind: MaxMinByKind) -> Self {
        Self {
            value_type,
            order_type,
            kind,
            best_orders: vec![],
            best_values: vec![],
        }
    }

    #[inline]
    fn should_replace(&self, old: &[u8], new: &[u8]) -> bool {
        match self.kind {
            MaxMinByKind::Max => new >= old,
            MaxMinByKind::Min => new <= old,
        }
    }

    fn maybe_update(
        &mut self,
        group_idx: usize,
        value_arr: &ArrayRef,
        new_order: &[u8],
        row_idx: usize,
    ) {
        let replace = match self.best_orders[group_idx].as_ref() {
            None => true,
            Some(old_order) => self.should_replace(old_order, new_order),
        };

        if replace {
            self.best_orders[group_idx] = Some(new_order.to_vec());
            self.best_values[group_idx] = Some(copy_single_value(value_arr, row_idx));
        }
    }

    fn build_order_array(&self, orders: Vec<Option<Vec<u8>>>) -> ArrayRef {
        match self.order_type {
            DataType::Utf8 => {
                let cap = orders.iter().map(|o| o.as_ref().map(|v| v.len()).unwrap_or(0)).sum();
                let mut builder = StringBuilder::with_capacity(orders.len(), cap);
                for order in orders {
                    match order {
                        None => builder.append_null(),
                        Some(bytes) => {
                            let s = unsafe { std::str::from_utf8_unchecked(&bytes) };
                            builder.append_value(s);
                        }
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::LargeUtf8 => {
                let cap = orders.iter().map(|o| o.as_ref().map(|v| v.len()).unwrap_or(0)).sum();
                let mut builder = LargeStringBuilder::with_capacity(orders.len(), cap);
                for order in orders {
                    match order {
                        None => builder.append_null(),
                        Some(bytes) => {
                            let s = unsafe { std::str::from_utf8_unchecked(&bytes) };
                            builder.append_value(s);
                        }
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Utf8View => {
                let mut builder = StringViewBuilder::with_capacity(orders.len());
                for order in orders {
                    match order {
                        None => builder.append_null(),
                        Some(bytes) => {
                            let s = unsafe { std::str::from_utf8_unchecked(&bytes) };
                            builder.append_value(s);
                        }
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Binary => {
                let cap = orders.iter().map(|o| o.as_ref().map(|v| v.len()).unwrap_or(0)).sum();
                let mut builder = BinaryBuilder::with_capacity(orders.len(), cap);
                for order in orders {
                    match order {
                        None => builder.append_null(),
                        Some(bytes) => builder.append_value(bytes),
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::LargeBinary => {
                let cap = orders.iter().map(|o| o.as_ref().map(|v| v.len()).unwrap_or(0)).sum();
                let mut builder = LargeBinaryBuilder::with_capacity(orders.len(), cap);
                for order in orders {
                    match order {
                        None => builder.append_null(),
                        Some(bytes) => builder.append_value(bytes),
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::BinaryView => {
                let mut builder = BinaryViewBuilder::with_capacity(orders.len());
                for order in orders {
                    match order {
                        None => builder.append_null(),
                        Some(bytes) => builder.append_value(bytes),
                    }
                }
                Arc::new(builder.finish())
            }
            _ => unreachable!("unexpected bytes order type"),
        }
    }
}

impl GroupsAccumulator for BytesOrderMaxMinByGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        let value_arr = &values[0];
        let order_arr = &values[1];

        self.best_orders.resize(total_num_groups, None);
        self.best_values.resize(total_num_groups, None);

        match &self.order_type {
            DataType::Utf8 => {
                for (i, (opt_order, &group_idx)) in
                    order_arr.as_string::<i32>().iter().zip(group_indices.iter()).enumerate()
                {
                    if !row_passes_filter(opt_filter, i) {
                        continue;
                    }
                    if let Some(order) = opt_order {
                        self.maybe_update(group_idx, value_arr, order.as_bytes(), i);
                    }
                }
            }
            DataType::LargeUtf8 => {
                for (i, (opt_order, &group_idx)) in
                    order_arr.as_string::<i64>().iter().zip(group_indices.iter()).enumerate()
                {
                    if !row_passes_filter(opt_filter, i) {
                        continue;
                    }
                    if let Some(order) = opt_order {
                        self.maybe_update(group_idx, value_arr, order.as_bytes(), i);
                    }
                }
            }
            DataType::Utf8View => {
                for (i, (opt_order, &group_idx)) in
                    order_arr.as_string_view().iter().zip(group_indices.iter()).enumerate()
                {
                    if !row_passes_filter(opt_filter, i) {
                        continue;
                    }
                    if let Some(order) = opt_order {
                        self.maybe_update(group_idx, value_arr, order.as_bytes(), i);
                    }
                }
            }
            DataType::Binary => {
                for (i, (opt_order, &group_idx)) in
                    order_arr.as_binary::<i32>().iter().zip(group_indices.iter()).enumerate()
                {
                    if !row_passes_filter(opt_filter, i) {
                        continue;
                    }
                    if let Some(order) = opt_order {
                        self.maybe_update(group_idx, value_arr, order, i);
                    }
                }
            }
            DataType::LargeBinary => {
                for (i, (opt_order, &group_idx)) in
                    order_arr.as_binary::<i64>().iter().zip(group_indices.iter()).enumerate()
                {
                    if !row_passes_filter(opt_filter, i) {
                        continue;
                    }
                    if let Some(order) = opt_order {
                        self.maybe_update(group_idx, value_arr, order, i);
                    }
                }
            }
            DataType::BinaryView => {
                for (i, (opt_order, &group_idx)) in
                    order_arr.as_binary_view().iter().zip(group_indices.iter()).enumerate()
                {
                    if !row_passes_filter(opt_filter, i) {
                        continue;
                    }
                    if let Some(order) = opt_order {
                        self.maybe_update(group_idx, value_arr, order, i);
                    }
                }
            }
            _ => unreachable!("unexpected bytes order type"),
        }

        Ok(())
    }

    fn merge_batch(
        &mut self,
        states: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.update_batch(states, group_indices, opt_filter, total_num_groups)
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let values = emit_to.take_needed(&mut self.best_values);
        materialize_singleton_arrays(values, &self.value_type)
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let values = emit_to.take_needed(&mut self.best_values);
        let orders = emit_to.take_needed(&mut self.best_orders);
        Ok(vec![
            materialize_singleton_arrays(values, &self.value_type)?,
            self.build_order_array(orders),
        ])
    }

    fn size(&self) -> usize {
        self.best_orders
            .iter()
            .map(|v| v.as_ref().map(|b| b.capacity()).unwrap_or(0))
            .sum::<usize>()
            + self.best_values.capacity() * size_of::<Option<ArrayRef>>()
    }
}

/* ---------------------------- Struct order fast path --------------------------- */

#[derive(Debug)]
struct StructOrderMaxMinByGroupsAccumulator {
    value_type: DataType,
    order_type: DataType,
    kind: MaxMinByKind,
    best_orders: Vec<Option<ArrayRef>>,
    best_values: Vec<Option<ArrayRef>>,
}

impl StructOrderMaxMinByGroupsAccumulator {
    fn new(value_type: DataType, order_type: DataType, kind: MaxMinByKind) -> Self {
        Self {
            value_type,
            order_type,
            kind,
            best_orders: vec![],
            best_values: vec![],
        }
    }

    fn should_replace(&self, old: &StructArray, new: &StructArray) -> bool {
        match partial_cmp_struct(new, old) {
            Some(Ordering::Greater) => matches!(self.kind, MaxMinByKind::Max),
            Some(Ordering::Less) => matches!(self.kind, MaxMinByKind::Min),
            Some(Ordering::Equal) => true,
            None => false,
        }
    }
}

impl GroupsAccumulator for StructOrderMaxMinByGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        let value_arr = &values[0];
        let order_arr = values[1].as_struct();

        self.best_orders.resize(total_num_groups, None);
        self.best_values.resize(total_num_groups, None);

        for (i, &group_idx) in group_indices.iter().enumerate() {
            if !row_passes_filter(opt_filter, i) || order_arr.is_null(i) {
                continue;
            }

            let new_order = order_arr.slice(i, 1);
            let replace = match &self.best_orders[group_idx] {
                None => true,
                Some(old_order) => {
                    let old_struct = old_order.as_any().downcast_ref::<StructArray>().unwrap();
                    self.should_replace(old_struct, &new_order)
                }
            };

            if replace {
                self.best_orders[group_idx] = Some(Arc::new(StructArray::from(copy_array_data(
                    &new_order.to_data(),
                ))));
                self.best_values[group_idx] = Some(copy_single_value(value_arr, i));
            }
        }

        Ok(())
    }

    fn merge_batch(
        &mut self,
        states: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.update_batch(states, group_indices, opt_filter, total_num_groups)
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let values = emit_to.take_needed(&mut self.best_values);
        materialize_singleton_arrays(values, &self.value_type)
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let values = emit_to.take_needed(&mut self.best_values);
        let orders = emit_to.take_needed(&mut self.best_orders);
        Ok(vec![
            materialize_singleton_arrays(values, &self.value_type)?,
            materialize_singleton_arrays(orders, &self.order_type)?,
        ])
    }

    fn size(&self) -> usize {
        self.best_orders.capacity() * size_of::<Option<ArrayRef>>()
            + self.best_values.capacity() * size_of::<Option<ArrayRef>>()
    }
}

/* ------------------------------- Row fallback -------------------------------- */

#[derive(Debug)]
struct RowOrderMaxMinByGroupsAccumulator {
    value_type: DataType,
    order_type: DataType,
    kind: MaxMinByKind,

    value_converter: RowConverter,
    order_converter: RowConverter,

    best_values: Vec<Option<OwnedRow>>,
    best_orders: Vec<Option<OwnedRow>>,

    null_value_row: OwnedRow,
    null_order_row: OwnedRow,
}

impl RowOrderMaxMinByGroupsAccumulator {
    fn try_new(
        value_type: DataType,
        order_type: DataType,
        kind: MaxMinByKind,
    ) -> Result<Self> {
        let value_converter = RowConverter::new(vec![SortField::new(value_type.clone())])?;
        let order_converter = RowConverter::new(vec![SortField::new(order_type.clone())])?;

        let null_value = ScalarValue::try_from(&value_type)?.to_array_of_size(1)?;
        let null_order = ScalarValue::try_from(&order_type)?.to_array_of_size(1)?;

        let null_value_rows = value_converter.convert_columns(&[null_value])?;
        let null_order_rows = order_converter.convert_columns(&[null_order])?;

        Ok(Self {
            value_type,
            order_type,
            kind,
            value_converter,
            order_converter,
            best_values: vec![],
            best_orders: vec![],
            null_value_row: null_value_rows.row(0).owned(),
            null_order_row: null_order_rows.row(0).owned(),
        })
    }

    #[inline]
    fn should_replace_rows(
        &self,
        old_order: arrow::row::Row<'_>,
        new_order: arrow::row::Row<'_>,
    ) -> bool {
        match self.kind {
            MaxMinByKind::Max => new_order >= old_order,
            MaxMinByKind::Min => new_order <= old_order,
        }
    }

    fn update_groups_from_arrays(
        &mut self,
        value_arr: &ArrayRef,
        order_arr: &ArrayRef,
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.best_values.resize(total_num_groups, None);
        self.best_orders.resize(total_num_groups, None);

        let value_rows = self.value_converter.convert_columns(&[Arc::clone(value_arr)])?;
        let order_rows = self.order_converter.convert_columns(&[Arc::clone(order_arr)])?;

        for (i, &group_idx) in group_indices.iter().enumerate() {
            if !row_passes_filter(opt_filter, i) || order_arr.is_null(i) {
                continue;
            }

            let new_order = order_rows.row(i);
            let replace = match &self.best_orders[group_idx] {
                None => true,
                Some(old_order) => self.should_replace_rows(old_order.row(), new_order),
            };

            if replace {
                self.best_orders[group_idx] = Some(new_order.owned());
                self.best_values[group_idx] = Some(value_rows.row(i).owned());
            }
        }

        Ok(())
    }

    fn materialize_owned_rows(
        converter: &RowConverter,
        rows: Vec<Option<OwnedRow>>,
        null_row: &OwnedRow,
        expected_type: &DataType,
    ) -> Result<ArrayRef> {
        let mut out_rows: Rows = converter.empty_rows(rows.len(), 0);
        for row in rows {
            match row {
                Some(r) => out_rows.push(r.row()),
                None => out_rows.push(null_row.row()),
            }
        }

        let mut arrays = converter.convert_rows(&out_rows)?;
        let array = match arrays.pop() {
            Some(arr) => arr,
            None => return internal_err!("expected exactly one output array"),
        };
        dictionary_encode_if_necessary(&array, expected_type)
    }
}

impl GroupsAccumulator for RowOrderMaxMinByGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.update_groups_from_arrays(
            &values[0],
            &values[1],
            group_indices,
            opt_filter,
            total_num_groups,
        )
    }

    fn merge_batch(
        &mut self,
        states: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.update_groups_from_arrays(
            &states[0],
            &states[1],
            group_indices,
            opt_filter,
            total_num_groups,
        )
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let values = emit_to.take_needed(&mut self.best_values);
        Self::materialize_owned_rows(
            &self.value_converter,
            values,
            &self.null_value_row,
            &self.value_type,
        )
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let values = emit_to.take_needed(&mut self.best_values);
        let orders = emit_to.take_needed(&mut self.best_orders);

        Ok(vec![
            Self::materialize_owned_rows(
                &self.value_converter,
                values,
                &self.null_value_row,
                &self.value_type,
            )?,
            Self::materialize_owned_rows(
                &self.order_converter,
                orders,
                &self.null_order_row,
                &self.order_type,
            )?,
        ])
    }

    fn size(&self) -> usize {
        self.best_values.capacity() * size_of::<Option<OwnedRow>>()
            + self.best_orders.capacity() * size_of::<Option<OwnedRow>>()
            + self.value_converter.size()
            + self.order_converter.size()
    }
}
