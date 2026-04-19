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

use std::any::Any;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::mem::size_of_val;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, BinaryBuilder, BooleanArray};
use arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion::common::{DataFusionError, Result, ScalarValue, internal_err, not_impl_err};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, EmitTo, GroupsAccumulator, Signature, Volatility,
};
use datafusion::physical_expr::expressions::format_state_name;

pub fn is_supported_mode_type(data_type: &DataType) -> bool {
    use DataType::*;
    matches!(
        data_type,
        Null
            | Boolean
            | Int8
            | Int16
            | Int32
            | Int64
            | Float32
            | Float64
            | Utf8
            | Binary
            | Timestamp(_, _)
            | Decimal128(_, _)
            | Date32
    )
}

#[derive(Debug, Clone)]
struct ModeKey(ScalarValue);

fn norm_f32_bits(v: f32) -> u32 {
    if v.is_nan() {
        f32::NAN.to_bits()
    } else {
        v.to_bits()
    }
}

fn norm_f64_bits(v: f64) -> u64 {
    if v.is_nan() {
        f64::NAN.to_bits()
    } else {
        v.to_bits()
    }
}

impl PartialEq for ModeKey {
    fn eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (ScalarValue::Float32(Some(a)), ScalarValue::Float32(Some(b))) => {
                norm_f32_bits(*a) == norm_f32_bits(*b)
            }
            (ScalarValue::Float64(Some(a)), ScalarValue::Float64(Some(b))) => {
                norm_f64_bits(*a) == norm_f64_bits(*b)
            }
            _ => self.0.eq(&other.0),
        }
    }
}

impl Eq for ModeKey {}

impl Hash for ModeKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match &self.0 {
            ScalarValue::Float32(Some(v)) => {
                1u8.hash(state);
                norm_f32_bits(*v).hash(state);
            }
            ScalarValue::Float64(Some(v)) => {
                2u8.hash(state);
                norm_f64_bits(*v).hash(state);
            }
            _ => {
                format!("{:?}", self.0).hash(state);
            }
        }
    }
}

fn typed_null(dt: &DataType) -> Result<ScalarValue> {
    ScalarValue::try_from(dt)
}

fn best_key(map: &HashMap<ModeKey, i64>) -> Option<ScalarValue> {
    map.iter().max_by_key(|(_, c)| *c).map(|(k, _)| k.0.clone())
}

fn serialize_state(map: &HashMap<ModeKey, i64>, data_type: &DataType) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(&(map.len() as u64).to_le_bytes());

    for (k, count) in map {
        serialize_scalar(&mut out, &k.0, data_type)?;
        out.extend_from_slice(&count.to_le_bytes());
    }

    Ok(out)
}

fn deserialize_and_merge(
    bytes: &[u8],
    map: &mut HashMap<ModeKey, i64>,
    data_type: &DataType,
) -> Result<()> {
    let mut off = 0usize;
    let len = read_u64(bytes, &mut off)? as usize;

    for _ in 0..len {
        let key = deserialize_scalar(bytes, &mut off, data_type)?;
        let count = read_i64(bytes, &mut off)?;
        *map.entry(ModeKey(key)).or_insert(0) += count;
    }

    Ok(())
}

fn serialize_scalar(buf: &mut Vec<u8>, value: &ScalarValue, data_type: &DataType) -> Result<()> {
    match (data_type, value) {
        (_, v) if v.is_null() => {
            buf.push(0);
            Ok(())
        }
        (DataType::Boolean, ScalarValue::Boolean(Some(v))) => {
            buf.push(1);
            buf.push(*v as u8);
            Ok(())
        }
        (DataType::Int8, ScalarValue::Int8(Some(v))) => {
            buf.push(1);
            buf.push(*v as u8);
            Ok(())
        }
        (DataType::Int16, ScalarValue::Int16(Some(v))) => {
            buf.push(1);
            buf.extend_from_slice(&v.to_le_bytes());
            Ok(())
        }
        (DataType::Int32, ScalarValue::Int32(Some(v))) => {
            buf.push(1);
            buf.extend_from_slice(&v.to_le_bytes());
            Ok(())
        }
        (DataType::Int64, ScalarValue::Int64(Some(v))) => {
            buf.push(1);
            buf.extend_from_slice(&v.to_le_bytes());
            Ok(())
        }
        (DataType::Float32, ScalarValue::Float32(Some(v))) => {
            buf.push(1);
            buf.extend_from_slice(&norm_f32_bits(*v).to_le_bytes());
            Ok(())
        }
        (DataType::Float64, ScalarValue::Float64(Some(v))) => {
            buf.push(1);
            buf.extend_from_slice(&norm_f64_bits(*v).to_le_bytes());
            Ok(())
        }
        (DataType::Utf8, ScalarValue::Utf8(Some(v))) => {
            buf.push(1);
            let b = v.as_bytes();
            buf.extend_from_slice(&(b.len() as u64).to_le_bytes());
            buf.extend_from_slice(b);
            Ok(())
        }
        (DataType::Binary, ScalarValue::Binary(Some(v))) => {
            buf.push(1);
            buf.extend_from_slice(&(v.len() as u64).to_le_bytes());
            buf.extend_from_slice(v);
            Ok(())
        }
        (DataType::Date32, ScalarValue::Date32(Some(v))) => {
            buf.push(1);
            buf.extend_from_slice(&v.to_le_bytes());
            Ok(())
        }
        (DataType::Decimal128(_, _), ScalarValue::Decimal128(Some(v), _, _)) => {
            buf.push(1);
            buf.extend_from_slice(&v.to_le_bytes());
            Ok(())
        }
        (DataType::Timestamp(TimeUnit::Second, _), ScalarValue::TimestampSecond(Some(v), _)) => {
            buf.push(1);
            buf.extend_from_slice(&v.to_le_bytes());
            Ok(())
        }
        (
            DataType::Timestamp(TimeUnit::Millisecond, _),
            ScalarValue::TimestampMillisecond(Some(v), _),
        ) => {
            buf.push(1);
            buf.extend_from_slice(&v.to_le_bytes());
            Ok(())
        }
        (
            DataType::Timestamp(TimeUnit::Microsecond, _),
            ScalarValue::TimestampMicrosecond(Some(v), _),
        ) => {
            buf.push(1);
            buf.extend_from_slice(&v.to_le_bytes());
            Ok(())
        }
        (
            DataType::Timestamp(TimeUnit::Nanosecond, _),
            ScalarValue::TimestampNanosecond(Some(v), _),
        ) => {
            buf.push(1);
            buf.extend_from_slice(&v.to_le_bytes());
            Ok(())
        }
        _ => internal_err!("unsupported mode scalar serialization"),
    }
}

fn deserialize_scalar(bytes: &[u8], off: &mut usize, data_type: &DataType) -> Result<ScalarValue> {
    let tag = read_u8(bytes, off)?;
    if tag == 0 {
        return typed_null(data_type);
    }

    match data_type {
        DataType::Boolean => Ok(ScalarValue::Boolean(Some(read_u8(bytes, off)? != 0))),
        DataType::Int8 => Ok(ScalarValue::Int8(Some(read_u8(bytes, off)? as i8))),
        DataType::Int16 => Ok(ScalarValue::Int16(Some(i16::from_le_bytes(read_exact::<2>(
            bytes, off,
        )?)))),
        DataType::Int32 => Ok(ScalarValue::Int32(Some(i32::from_le_bytes(read_exact::<4>(
            bytes, off,
        )?)))),
        DataType::Int64 => Ok(ScalarValue::Int64(Some(i64::from_le_bytes(read_exact::<8>(
            bytes, off,
        )?)))),
        DataType::Float32 => Ok(ScalarValue::Float32(Some(f32::from_bits(
            u32::from_le_bytes(read_exact::<4>(bytes, off)?),
        )))),
        DataType::Float64 => Ok(ScalarValue::Float64(Some(f64::from_bits(
            u64::from_le_bytes(read_exact::<8>(bytes, off)?),
        )))),
        DataType::Utf8 => {
            let len = read_u64(bytes, off)? as usize;
            if *off + len > bytes.len() {
                return internal_err!("corrupt state");
            }
            let s = std::str::from_utf8(&bytes[*off..*off + len])
                .map_err(|e| DataFusionError::Execution(format!("invalid utf8 in mode state: {e}")))?
                .to_string();
            *off += len;
            Ok(ScalarValue::Utf8(Some(s)))
        }
        DataType::Binary => {
            let len = read_u64(bytes, off)? as usize;
            if *off + len > bytes.len() {
                return internal_err!("corrupt state");
            }
            let v = bytes[*off..*off + len].to_vec();
            *off += len;
            Ok(ScalarValue::Binary(Some(v)))
        }
        DataType::Date32 => Ok(ScalarValue::Date32(Some(i32::from_le_bytes(read_exact::<4>(
            bytes, off,
        )?)))),
        DataType::Decimal128(p, s) => Ok(ScalarValue::Decimal128(
            Some(i128::from_le_bytes(read_exact::<16>(bytes, off)?)),
            *p,
            *s,
        )),
        DataType::Timestamp(TimeUnit::Second, tz) => Ok(ScalarValue::TimestampSecond(
            Some(i64::from_le_bytes(read_exact::<8>(bytes, off)?)),
            tz.clone(),
        )),
        DataType::Timestamp(TimeUnit::Millisecond, tz) => Ok(ScalarValue::TimestampMillisecond(
            Some(i64::from_le_bytes(read_exact::<8>(bytes, off)?)),
            tz.clone(),
        )),
        DataType::Timestamp(TimeUnit::Microsecond, tz) => Ok(ScalarValue::TimestampMicrosecond(
            Some(i64::from_le_bytes(read_exact::<8>(bytes, off)?)),
            tz.clone(),
        )),
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => Ok(ScalarValue::TimestampNanosecond(
            Some(i64::from_le_bytes(read_exact::<8>(bytes, off)?)),
            tz.clone(),
        )),
        _ => internal_err!("unsupported mode scalar deserialization"),
    }
}

fn read_exact<const N: usize>(bytes: &[u8], off: &mut usize) -> Result<[u8; N]> {
    if *off + N > bytes.len() {
        return internal_err!("corrupt state");
    }
    let mut out = [0u8; N];
    out.copy_from_slice(&bytes[*off..*off + N]);
    *off += N;
    Ok(out)
}

fn read_u8(bytes: &[u8], off: &mut usize) -> Result<u8> {
    Ok(read_exact::<1>(bytes, off)?[0])
}

fn read_u64(bytes: &[u8], off: &mut usize) -> Result<u64> {
    Ok(u64::from_le_bytes(read_exact::<8>(bytes, off)?))
}

fn read_i64(bytes: &[u8], off: &mut usize) -> Result<i64> {
    Ok(i64::from_le_bytes(read_exact::<8>(bytes, off)?))
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Mode {
    name: String,
    signature: Signature,
    data_type: DataType,
}

impl Mode {
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            signature: Signature::any(1, Volatility::Immutable),
            data_type,
        }
    }
}

impl AggregateUDFImpl for Mode {
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
        Ok(self.data_type.clone())
    }

    fn default_value(&self, data_type: &DataType) -> Result<ScalarValue> {
        typed_null(data_type)
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if !is_supported_mode_type(&self.data_type) {
            return not_impl_err!("ModeAccumulator for {}", self.data_type);
        }
        Ok(Box::new(ModeAccumulator::new(self.data_type.clone())))
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        if !is_supported_mode_type(&self.data_type) {
            return not_impl_err!("ModeGroupsAccumulator for {}", self.data_type);
        }
        Ok(Box::new(ModeGroupsAccumulator::new(self.data_type.clone())))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new(
            format_state_name(&self.name, "state"),
            DataType::Binary,
            false,
        ))])
    }
}

#[derive(Debug)]
struct ModeAccumulator {
    data_type: DataType,
    counts: HashMap<ModeKey, i64>,
}

impl ModeAccumulator {
    fn new(data_type: DataType) -> Self {
        Self {
            data_type,
            counts: HashMap::new(),
        }
    }

    fn update_one(&mut self, value: ScalarValue) {
        if value.is_null() {
            return;
        }
        *self.counts.entry(ModeKey(value)).or_insert(0) += 1;
    }
}

impl Accumulator for ModeAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Binary(Some(serialize_state(
            &self.counts,
            &self.data_type,
        )?))])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.len() != 1 {
            return internal_err!("mode expects exactly 1 argument");
        }

        for i in 0..values[0].len() {
            let value = ScalarValue::try_from_array(&values[0], i)?;
            self.update_one(value);
        }
        Ok(())
    }

    fn retract_batch(&mut self, _values: &[ArrayRef]) -> Result<()> {
        not_impl_err!("retract_batch is not implemented for mode")
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.len() != 1 {
            return internal_err!("mode state expects exactly 1 array");
        }

        let arr = states[0].as_binary::<i32>();
        for i in 0..arr.len() {
            if !arr.is_null(i) {
                deserialize_and_merge(arr.value(i), &mut self.counts, &self.data_type)?;
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        match best_key(&self.counts) {
            Some(v) => Ok(v),
            None => typed_null(&self.data_type),
        }
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }
}

#[derive(Debug)]
struct ModeGroupsAccumulator {
    data_type: DataType,
    groups: Vec<HashMap<ModeKey, i64>>,
}

impl ModeGroupsAccumulator {
    fn new(data_type: DataType) -> Self {
        Self {
            data_type,
            groups: vec![],
        }
    }

    fn row_passes_filter(opt_filter: Option<&BooleanArray>, idx: usize) -> bool {
        match opt_filter {
            None => true,
            Some(filter) => filter.is_valid(idx) && filter.value(idx),
        }
    }
}

impl GroupsAccumulator for ModeGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        if values.len() != 1 {
            return internal_err!("mode expects exactly 1 argument");
        }

        self.groups.resize_with(total_num_groups, HashMap::new);

        for (row_idx, &group_idx) in group_indices.iter().enumerate() {
            if !Self::row_passes_filter(opt_filter, row_idx) {
                continue;
            }

            let value = ScalarValue::try_from_array(&values[0], row_idx)?;
            if value.is_null() {
                continue;
            }

            *self.groups[group_idx].entry(ModeKey(value)).or_insert(0) += 1;
        }

        Ok(())
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        if values.len() != 1 {
            return internal_err!("mode state expects exactly 1 array");
        }

        self.groups.resize_with(total_num_groups, HashMap::new);
        let arr = values[0].as_binary::<i32>();

        for (row_idx, &group_idx) in group_indices.iter().enumerate() {
            if !Self::row_passes_filter(opt_filter, row_idx) {
                continue;
            }
            if arr.is_null(row_idx) {
                continue;
            }

            deserialize_and_merge(arr.value(row_idx), &mut self.groups[group_idx], &self.data_type)?;
        }

        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let groups = emit_to.take_needed(&mut self.groups);
        let scalars = groups
            .iter()
            .map(|m| match best_key(m) {
                Some(v) => Ok(v),
                None => typed_null(&self.data_type),
            })
            .collect::<Result<Vec<_>>>()?;

        ScalarValue::iter_to_array(scalars)
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let groups = emit_to.take_needed(&mut self.groups);

        let total_bytes = groups
            .iter()
            .map(|m| serialize_state(m, &self.data_type).map(|b| b.len()))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .sum();

        let mut builder = BinaryBuilder::with_capacity(groups.len(), total_bytes);

        for m in groups {
            builder.append_value(serialize_state(&m, &self.data_type)?);
        }

        Ok(vec![Arc::new(builder.finish()) as ArrayRef])
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }
}
