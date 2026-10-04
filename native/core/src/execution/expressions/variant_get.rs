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

use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
    hash::{Hash, Hasher},
    sync::Arc,
};

use arrow::{
    array::{new_null_array, timezone::Tz, Array, ArrayRef, AsArray},
    compute::interleave,
    datatypes::{i256, DataType, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use datafusion::{
    common::{exec_err, plan_err, DataFusionError, Result, ScalarValue},
    logical_expr::ColumnarValue,
    physical_expr::PhysicalExpr,
};
use datafusion_comet_common::{decode_utf8_spark_lossy, SparkError};
use datafusion_comet_proto::spark_expression::{self, variant_path_segment::Segment};
use datafusion_comet_spark_expr::{spark_cast, EvalMode, SparkCastOptions};
use parquet::variant::{Variant, VariantType};

use crate::execution::serde::to_arrow_datatype;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum PathSegment {
    Key(String),
    Index(usize),
}

#[derive(Debug, Eq)]
pub struct VariantGet {
    child: Arc<dyn PhysicalExpr>,
    path: Vec<PathSegment>,
    path_sql: String,
    target: DataType,
    target_sql: String,
    fail_on_error: bool,
    size_limit: usize,
    cast_options: SparkCastOptions,
}

impl PartialEq for VariantGet {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child)
            && self.path == other.path
            && self.path_sql == other.path_sql
            && self.target == other.target
            && self.target_sql == other.target_sql
            && self.fail_on_error == other.fail_on_error
            && self.size_limit == other.size_limit
            && self.cast_options == other.cast_options
    }
}

impl Hash for VariantGet {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.child.hash(state);
        self.path.hash(state);
        self.path_sql.hash(state);
        self.target.hash(state);
        self.target_sql.hash(state);
        self.fail_on_error.hash(state);
        self.size_limit.hash(state);
        self.cast_options.hash(state);
    }
}

impl VariantGet {
    pub fn try_new(
        child: Arc<dyn PhysicalExpr>,
        expr: &spark_expression::VariantGet,
        schema: &Schema,
    ) -> Result<Self> {
        if !child
            .return_field(schema)?
            .has_valid_extension_type::<VariantType>()
        {
            return plan_err!("variant_get requires a Variant extension field");
        }
        let target =
            to_arrow_datatype(expr.datatype.as_ref().ok_or_else(|| {
                DataFusionError::Plan("variant_get requires a target type".into())
            })?);
        if !matches!(
            target,
            DataType::Boolean
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
                | DataType::Decimal128(_, _)
                | DataType::Binary
                | DataType::Date32
                | DataType::Timestamp(TimeUnit::Microsecond, _)
        ) {
            return plan_err!("Unsupported variant_get target: {target}");
        }
        if expr.size_limit == 0 {
            return plan_err!("variant_get requires a positive Variant size limit");
        }
        let path = expr
            .path
            .iter()
            .map(|part| match &part.segment {
                Some(Segment::Key(key)) => Ok(PathSegment::Key(key.clone())),
                Some(Segment::Index(index)) => Ok(PathSegment::Index(*index as usize)),
                None => plan_err!("Missing variant_get path segment"),
            })
            .collect::<Result<Vec<_>>>()?;
        // Validate the zone during planning. Spark's analyzer has already resolved it.
        expr.timezone.parse::<Tz>()?;
        Ok(Self {
            child,
            path,
            path_sql: expr.path_sql.clone(),
            target,
            target_sql: expr.target_sql.clone(),
            fail_on_error: expr.fail_on_error,
            size_limit: expr.size_limit as usize,
            cast_options: SparkCastOptions::new_with_version(
                EvalMode::Try,
                &expr.timezone,
                true,
                true,
            ),
        })
    }

    fn extract<'v>(&self, metadata: &[u8], value: &'v [u8]) -> Result<Option<&'v [u8]>> {
        if metadata.first().is_none_or(|header| header & 15 != 1) {
            return Err(SparkError::MalformedVariant.into());
        }
        if metadata.len() > self.size_limit || value.len() > self.size_limit {
            return Err(SparkError::VariantConstructorSizeLimit.into());
        }
        let mut position = 0_i32;
        for part in &self.path {
            let header = checked_header(
                value
                    .get(checked_index(position)?..)
                    .ok_or(SparkError::MalformedVariant)?,
            )?;
            let info = header >> 2;
            let (data_start, offset) =
                match (part, header & 3) {
                    (PathSegment::Key(key), 2) => {
                        let size_bytes = if info & 16 == 0 { 1 } else { 4 };
                        let size =
                            unsigned(value, checked_index(position.wrapping_add(1))?, size_bytes)?;
                        let id_width = ((info >> 2) & 3) as usize + 1;
                        let offset_width = (info & 3) as usize + 1;
                        // Spark uses absolute positions and Java int arithmetic, including wrap.
                        // All accesses below still check the resulting index against the buffer.
                        let ids = position.wrapping_add(1 + size_bytes as i32);
                        let offsets = ids.wrapping_add((size as i32).wrapping_mul(id_width as i32));
                        let data = offsets.wrapping_add(
                            (size as i32)
                                .wrapping_add(1)
                                .wrapping_mul(offset_width as i32),
                        );
                        let key_at = |i: usize| {
                            metadata_key(
                                metadata,
                                unsigned(
                                    value,
                                    checked_index(
                                        ids.wrapping_add((i as i32).wrapping_mul(id_width as i32)),
                                    )?,
                                    id_width,
                                )?,
                            )
                        };
                        // Match Spark 4.0/4.1/4.2's small-object linear lookup and UTF-16 binary
                        // lookup. Do not decode or validate values outside the requested path.
                        let index = if size < 32 {
                            let mut found = None;
                            for i in 0..size {
                                if key_at(i)?.as_ref() == key {
                                    found = Some(i);
                                    break;
                                }
                            }
                            found
                        } else {
                            let (mut low, mut high) = (0, size);
                            let mut found = None;
                            while low < high {
                                let mid = low + (high - low - 1) / 2;
                                match key_at(mid)?.encode_utf16().cmp(key.encode_utf16()) {
                                    std::cmp::Ordering::Less => low = mid + 1,
                                    std::cmp::Ordering::Greater => high = mid,
                                    std::cmp::Ordering::Equal => {
                                        found = Some(mid);
                                        break;
                                    }
                                }
                            }
                            found
                        };
                        let Some(index) = index else {
                            return Ok(None);
                        };
                        (
                            data,
                            unsigned(
                                value,
                                checked_index(offsets.wrapping_add(
                                    (index as i32).wrapping_mul(offset_width as i32),
                                ))?,
                                offset_width,
                            )?,
                        )
                    }
                    (PathSegment::Index(index), 3) => {
                        let size_bytes = if info & 4 == 0 { 1 } else { 4 };
                        let size =
                            unsigned(value, checked_index(position.wrapping_add(1))?, size_bytes)?;
                        if *index >= size {
                            return Ok(None);
                        }
                        let offset_width = (info & 3) as usize + 1;
                        let offsets = position.wrapping_add(1 + size_bytes as i32);
                        (
                            offsets.wrapping_add(
                                (size as i32)
                                    .wrapping_add(1)
                                    .wrapping_mul(offset_width as i32),
                            ),
                            unsigned(
                                value,
                                checked_index(offsets.wrapping_add(
                                    (*index as i32).wrapping_mul(offset_width as i32),
                                ))?,
                                offset_width,
                            )?,
                        )
                    }
                    _ => return Ok(None),
                };
            position = data_start.wrapping_add(offset as i32);
        }
        let selected = value
            .get(checked_index(position)?..)
            .ok_or(SparkError::MalformedVariant)?;
        checked_header(selected)?;
        Ok(Some(selected))
    }

    fn evaluate_array(&self, input: &ArrayRef) -> Result<ArrayRef> {
        let array = input.as_struct_opt().ok_or_else(|| {
            DataFusionError::Execution("Variant input must use struct storage".into())
        })?;
        let values = array
            .column_by_name("value")
            .and_then(|a| a.as_binary_opt::<i32>());
        let metadata = array
            .column_by_name("metadata")
            .and_then(|a| a.as_binary_opt::<i32>());
        let (Some(values), Some(metadata)) = (values, metadata) else {
            return exec_err!("Variant input must contain Binary value and metadata children");
        };
        // Cast together values with the same source type. This avoids invoking Arrow's cast
        // machinery and allocating an array for each row in a heterogeneous Variant column.
        let mut groups: Vec<Vec<ScalarValue>> = vec![vec![]];
        let mut group_types = HashMap::new();
        let mut positions = Vec::with_capacity(array.len());
        let mut extracted = Vec::with_capacity(array.len());
        for row in 0..array.len() {
            let value = if array.is_null(row) {
                None
            } else {
                if values.is_null(row) || metadata.is_null(row) {
                    return Err(SparkError::MalformedVariant.into());
                }
                // Use shallow decoding: Spark validates only the selected path. Full Arrow
                // validation also rejects legacy Spark key order and empty dictionary keys.
                self.extract(metadata.value(row), values.value(row))?
            };
            let scalar = match value.as_ref() {
                None | Some(&[0, ..]) => None,
                Some(value) => self.scalar_for_cast(value)?,
            };
            if let Some(scalar) = scalar {
                let data_type = scalar.data_type();
                let group = *group_types.entry(data_type).or_insert_with(|| {
                    groups.push(vec![]);
                    groups.len() - 1
                });
                positions.push((group, groups[group].len()));
                groups[group].push(scalar);
            } else {
                positions.push((0, 0));
            }
            extracted.push(value);
        }
        let mut casted = vec![new_null_array(&self.target, 1)];
        for group in groups.into_iter().skip(1) {
            let source = ScalarValue::iter_to_array(group)?;
            let converted = spark_cast(
                ColumnarValue::Array(source),
                &self.target,
                &self.cast_options,
            )?;
            let ColumnarValue::Array(converted) = converted else {
                return exec_err!("Variant batch cast returned a scalar");
            };
            casted.push(converted);
        }
        if self.fail_on_error {
            for (row, ((group, index), value)) in positions.iter().zip(&extracted).enumerate() {
                if casted[*group].is_null(*index) && value.is_some_and(|v| v[0] != 0) {
                    // Rendering the offending value is version/JDK dependent (especially
                    // floating point). Reproduce Spark's exception only on this error path.
                    return Err(SparkError::InvalidVariantCast {
                        value: STANDARD.encode(values.value(row)),
                        metadata: STANDARD.encode(metadata.value(row)),
                        path: self.path_sql.clone(),
                        timezone: self.cast_options.timezone.clone(),
                        data_type: self.target_sql.clone(),
                    }
                    .into());
                }
            }
        }
        let arrays = casted.iter().map(|a| a.as_ref()).collect::<Vec<_>>();
        Ok(interleave(&arrays, &positions)?)
    }

    fn scalar_for_cast(&self, bytes: &[u8]) -> Result<Option<ScalarValue>> {
        let header = bytes[0];
        if matches!(header & 3, 2 | 3) {
            return Ok(None);
        }
        let mut scalar = match (header & 3, header >> 2) {
            // UUID can only be converted to STRING, which this expression does not admit.
            // Spark rejects the type without reading its payload in TRY mode.
            (0, 20) => return Ok(None),
            (0, 11) => ScalarValue::Date32(Some(i32::from_le_bytes(raw_bytes::<4>(bytes, 1)?))),
            (0, 12) => ScalarValue::TimestampMicrosecond(
                Some(i64::from_le_bytes(raw_bytes::<8>(bytes, 1)?)),
                Some("UTC".into()),
            ),
            (0, 13) => ScalarValue::TimestampMicrosecond(
                Some(i64::from_le_bytes(raw_bytes::<8>(bytes, 1)?)),
                None,
            ),
            (1, length) => ScalarValue::Utf8(Some(
                decode_utf8_spark_lossy(
                    bytes
                        .get(1..1 + length as usize)
                        .ok_or(SparkError::MalformedVariant)?,
                )
                .into_owned(),
            )),
            (0, 16) => {
                let length = unsigned(bytes, 1, 4)?;
                ScalarValue::Utf8(Some(
                    decode_utf8_spark_lossy(
                        bytes
                            .get(5..5 + length)
                            .ok_or(SparkError::MalformedVariant)?,
                    )
                    .into_owned(),
                ))
            }
            _ => {
                // Primitive values do not consult metadata. Arrow validates more of the
                // dictionary than Spark does, so give it a canonical empty dictionary.
                let value = Variant::try_new(&[1, 0, 0], bytes)
                    .map_err(|_| SparkError::MalformedVariant)?;
                to_scalar(&value)?
            }
        };
        if !can_ansi_cast(&scalar.data_type(), &self.target) {
            return Ok(None);
        }
        // Variant permits scalar casts which the generic Comet cast does not yet expose.
        // Adapt their input to a supported cast while preserving Spark's seconds semantics.
        scalar = match (&scalar, &self.target) {
            (ScalarValue::Boolean(Some(value)), DataType::Decimal128(_, _)) => {
                ScalarValue::Int64(Some(i64::from(*value)))
            }
            (ScalarValue::TimestampMicrosecond(Some(micros), Some(_)), target) => match target {
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                    ScalarValue::Int64(Some(micros.div_euclid(1_000_000)))
                }
                DataType::Float32 | DataType::Float64 => {
                    ScalarValue::Float64(Some(*micros as f64 / 1_000_000.0))
                }
                DataType::Decimal128(_, _) => ScalarValue::Decimal128(Some(*micros as i128), 19, 6),
                _ => scalar,
            },
            _ => scalar,
        };
        if let DataType::Timestamp(TimeUnit::Microsecond, Some(zone)) = &self.target {
            let micros = match scalar {
                ScalarValue::Int64(Some(value)) => Some(value.checked_mul(1_000_000)),
                ScalarValue::Decimal128(Some(value), _, scale) => {
                    let scaled = i256::from_i128(value) * i256::from_i128(1_000_000)
                        / i256::from_i128(10_i128.pow(scale as u32));
                    Some(scaled.to_i128().and_then(|v| i64::try_from(v).ok()))
                }
                _ => None,
            };
            if let Some(micros) = micros {
                let Some(micros) = micros else {
                    return Ok(None);
                };
                scalar = ScalarValue::TimestampMicrosecond(Some(micros), Some(Arc::clone(zone)));
            }
        }
        Ok(Some(scalar))
    }
}

fn to_scalar(value: &Variant<'_, '_>) -> Result<ScalarValue> {
    Ok(match value {
        Variant::Null => ScalarValue::Null,
        Variant::BooleanTrue => ScalarValue::Boolean(Some(true)),
        Variant::BooleanFalse => ScalarValue::Boolean(Some(false)),
        Variant::Int8(v) => ScalarValue::Int64(Some(*v as i64)),
        Variant::Int16(v) => ScalarValue::Int64(Some(*v as i64)),
        Variant::Int32(v) => ScalarValue::Int64(Some(*v as i64)),
        Variant::Int64(v) => ScalarValue::Int64(Some(*v)),
        Variant::Float(v) => ScalarValue::Float32(Some(*v)),
        Variant::Double(v) => ScalarValue::Float64(Some(*v)),
        Variant::String(v) => ScalarValue::Utf8(Some((*v).to_string())),
        Variant::ShortString(v) => ScalarValue::Utf8(Some(v.as_str().to_string())),
        Variant::Binary(v) => ScalarValue::Binary(Some(v.to_vec())),
        Variant::Decimal4(v) => decimal_scalar(v.integer() as i128, v.scale()),
        Variant::Decimal8(v) => decimal_scalar(v.integer() as i128, v.scale()),
        Variant::Decimal16(v) => decimal_scalar(v.integer(), v.scale()),
        // The scan rejects Variant primitive types unsupported by Spark 4.x.
        _ => return Err(SparkError::MalformedVariant.into()),
    })
}

fn decimal_scalar(mut value: i128, mut scale: u8) -> ScalarValue {
    while scale > 0 && value % 10 == 0 {
        value /= 10;
        scale -= 1;
    }
    ScalarValue::Decimal128(Some(value), 38, scale as i8)
}

/// Spark VariantGet uses Cast.canAnsiCast even when try_variant_get is requested.
fn can_ansi_cast(from: &DataType, to: &DataType) -> bool {
    use DataType::*;
    if from == to {
        return true;
    }
    let numeric = |t: &DataType| {
        matches!(
            t,
            Int64 | Int8 | Int16 | Int32 | Float32 | Float64 | Decimal128(_, _)
        )
    };
    match (from, to) {
        (
            Utf8,
            Binary
            | Boolean
            | Date32
            | Timestamp(_, _)
            | Int8
            | Int16
            | Int32
            | Int64
            | Float32
            | Float64
            | Decimal128(_, _),
        ) => true,
        (Date32 | Timestamp(_, None), Timestamp(_, Some(_))) => true,
        (Timestamp(_, Some(_)), Timestamp(_, Some(_))) => true,
        (Date32 | Timestamp(_, Some(_)), Timestamp(_, None)) => true,
        (Timestamp(_, _), Date32) => true,
        (Boolean | Timestamp(_, Some(_)), to) if numeric(to) => true,
        (from, Boolean | Timestamp(_, Some(_))) if numeric(from) => true,
        (from, to) if numeric(from) && numeric(to) => true,
        _ => false,
    }
}

impl Display for VariantGet {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}({}, {:?}, {})",
            if self.fail_on_error {
                "variant_get"
            } else {
                "try_variant_get"
            },
            self.child,
            self.path,
            self.target
        )
    }
}

impl PhysicalExpr for VariantGet {
    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
    fn data_type(&self, _: &Schema) -> Result<DataType> {
        Ok(self.target.clone())
    }
    fn nullable(&self, _: &Schema) -> Result<bool> {
        Ok(true)
    }
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let input = self.child.evaluate(batch)?.into_array(batch.num_rows())?;
        self.evaluate_array(&input).map(ColumnarValue::Array)
    }
    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.child]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let [child] = children.as_slice() else {
            return plan_err!("variant_get requires exactly one child");
        };
        Ok(Arc::new(Self {
            child: Arc::clone(child),
            path: self.path.clone(),
            path_sql: self.path_sql.clone(),
            target: self.target.clone(),
            target_sql: self.target_sql.clone(),
            fail_on_error: self.fail_on_error,
            size_limit: self.size_limit,
            cast_options: self.cast_options.clone(),
        }))
    }
}

fn raw_bytes<const N: usize>(bytes: &[u8], start: usize) -> Result<[u8; N]> {
    bytes
        .get(start..start + N)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| SparkError::MalformedVariant.into())
}

fn checked_header(bytes: &[u8]) -> Result<u8> {
    let header = *bytes.first().ok_or(SparkError::MalformedVariant)?;
    let id = header >> 2;
    if header & 3 == 0 && !(id <= 16 || id == 20) {
        return Err(SparkError::UnknownPrimitiveTypeInVariant { id }.into());
    }
    Ok(header)
}

fn unsigned(bytes: &[u8], start: usize, width: usize) -> Result<usize> {
    let source = bytes
        .get(start..start + width)
        .ok_or(SparkError::MalformedVariant)?;
    let mut value = 0u32;
    for (i, byte) in source.iter().enumerate() {
        value |= (*byte as u32) << (i * 8);
    }
    if value > i32::MAX as u32 {
        return Err(SparkError::MalformedVariant.into());
    }
    Ok(value as usize)
}

fn checked_index(index: i32) -> Result<usize> {
    usize::try_from(index).map_err(|_| SparkError::MalformedVariant.into())
}

fn metadata_key(metadata: &[u8], id: usize) -> Result<std::borrow::Cow<'_, str>> {
    let header = metadata.first().ok_or(SparkError::MalformedVariant)?;
    let width = (header >> 6) as usize + 1;
    let count = unsigned(metadata, 1, width)?;
    if id >= count {
        return Err(SparkError::MalformedVariant.into());
    }
    // Spark computes metadata addresses with Java int arithmetic. Preserve wrapping, then
    // check the resulting addresses before accessing even an empty dictionary key.
    let address = |index: i32| 1_i32.wrapping_add(index.wrapping_mul(width as i32));
    let data = address((count as i32).wrapping_add(2));
    let start = unsigned(
        metadata,
        checked_index(address((id as i32).wrapping_add(1)))?,
        width,
    )?;
    let end = unsigned(
        metadata,
        checked_index(address((id as i32).wrapping_add(2)))?,
        width,
    )?;
    if start > end {
        return Err(SparkError::MalformedVariant.into());
    }
    let last = data.wrapping_add(end as i32).wrapping_sub(1);
    metadata
        .get(checked_index(last)?)
        .ok_or(SparkError::MalformedVariant)?;
    let begin = checked_index(data.wrapping_add(start as i32))?;
    let bytes = metadata
        .get(begin..begin + end - start)
        .ok_or(SparkError::MalformedVariant)?;
    Ok(decode_utf8_spark_lossy(bytes))
}

#[cfg(test)]
mod tests;
