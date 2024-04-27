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
    any::Any,
    fmt::{Display, Formatter},
    hash::{Hash, Hasher},
    sync::Arc,
};

use crate::errors::{CometError, CometResult};
use arrow::{
    compute::{cast_with_options, take, CastOptions},
    record_batch::RecordBatch,
    util::display::FormatOptions,
};
use arrow_array::{
    types::{ArrowDictionaryKeyType, Int16Type, Int32Type, Int64Type, Int8Type},
    Array, ArrayRef, BooleanArray, DictionaryArray, GenericStringArray, OffsetSizeTrait,
    PrimitiveArray,
};
use arrow_schema::{DataType, Schema};
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::{internal_err, Result as DataFusionResult, ScalarValue};
use datafusion_physical_expr::PhysicalExpr;
use num::{traits::CheckedNeg, CheckedSub, Integer, Num};

use crate::execution::datafusion::expressions::utils::{
    array_with_timezone, down_cast_any_ref, spark_cast,
};

static TIMESTAMP_FORMAT: Option<&str> = Some("%Y-%m-%d %H:%M:%S%.f");
static CAST_OPTIONS: CastOptions = CastOptions {
    safe: true,
    format_options: FormatOptions::new()
        .with_timestamp_tz_format(TIMESTAMP_FORMAT)
        .with_timestamp_format(TIMESTAMP_FORMAT),
};

#[derive(Debug, Hash, PartialEq, Clone, Copy)]
pub enum EvalMode {
    Legacy,
    Ansi,
    Try,
}

#[derive(Debug, Hash)]
pub struct Cast {
    pub child: Arc<dyn PhysicalExpr>,
    pub data_type: DataType,
    pub eval_mode: EvalMode,

    /// When cast from/to timezone related types, we need timezone, which will be resolved with
    /// session local timezone by an analyzer in Spark.
    pub timezone: String,
}

macro_rules! cast_utf8_to_int {
    ($array:expr, $eval_mode:expr, $array_type:ty, $cast_method:ident) => {{
        let len = $array.len();
        let mut cast_array = PrimitiveArray::<$array_type>::builder(len);
        for i in 0..len {
            if $array.is_null(i) {
                cast_array.append_null()
            } else if let Some(cast_value) = $cast_method($array.value(i).trim(), $eval_mode)? {
                cast_array.append_value(cast_value);
            } else {
                cast_array.append_null()
            }
        }
        let result: CometResult<ArrayRef> = Ok(Arc::new(cast_array.finish()) as ArrayRef);
        result
    }};
}

impl Cast {
    pub fn new(
        child: Arc<dyn PhysicalExpr>,
        data_type: DataType,
        eval_mode: EvalMode,
        timezone: String,
    ) -> Self {
        Self {
            child,
            data_type,
            timezone,
            eval_mode,
        }
    }

    pub fn new_without_timezone(
        child: Arc<dyn PhysicalExpr>,
        data_type: DataType,
        eval_mode: EvalMode,
    ) -> Self {
        Self {
            child,
            data_type,
            timezone: "".to_string(),
            eval_mode,
        }
    }

    fn cast_array(&self, array: ArrayRef) -> DataFusionResult<ArrayRef> {
        let to_type = &self.data_type;
        let array = array_with_timezone(array, self.timezone.clone(), Some(to_type));
        let from_type = array.data_type();
        let cast_result = match (from_type, to_type) {
            (DataType::Utf8, DataType::Boolean) => {
                Self::spark_cast_utf8_to_boolean::<i32>(&array, self.eval_mode)?
            }
            (DataType::LargeUtf8, DataType::Boolean) => {
                Self::spark_cast_utf8_to_boolean::<i64>(&array, self.eval_mode)?
            }
            (
                DataType::Utf8,
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64,
            ) => Self::cast_string_to_int::<i32>(to_type, &array, self.eval_mode)?,
            (
                DataType::LargeUtf8,
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64,
            ) => Self::cast_string_to_int::<i64>(to_type, &array, self.eval_mode)?,
            (
                DataType::Dictionary(key_type, value_type),
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64,
            ) if key_type.as_ref() == &DataType::Int32
                && value_type.as_ref() == &DataType::Utf8 =>
            {
                // TODO: we are unpacking a dictionary-encoded array and then performing
                // the cast. We could potentially improve performance here by casting the
                // dictionary values directly without unpacking the array first, although this
                // would add more complexity to the code
                let unpacked_array = Self::unpack_dict_string_array::<Int32Type>(&array)?;
                Self::cast_string_to_int::<i32>(to_type, &unpacked_array, self.eval_mode)?
            }
            _ => {
                // when we have no Spark-specific casting we delegate to DataFusion
                cast_with_options(&array, to_type, &CAST_OPTIONS)?
            }
        };
        Ok(spark_cast(cast_result, from_type, to_type))
    }

    fn cast_string_to_int<OffsetSize: OffsetSizeTrait>(
        to_type: &DataType,
        array: &ArrayRef,
        eval_mode: EvalMode,
    ) -> CometResult<ArrayRef> {
        let string_array = array
            .as_any()
            .downcast_ref::<GenericStringArray<OffsetSize>>()
            .expect("cast_string_to_int expected a string array");

        let cast_array: ArrayRef = match to_type {
            DataType::Int8 => {
                cast_utf8_to_int!(string_array, eval_mode, Int8Type, cast_string_to_i8)?
            }
            DataType::Int16 => {
                cast_utf8_to_int!(string_array, eval_mode, Int16Type, cast_string_to_i16)?
            }
            DataType::Int32 => {
                cast_utf8_to_int!(string_array, eval_mode, Int32Type, cast_string_to_i32)?
            }
            DataType::Int64 => {
                cast_utf8_to_int!(string_array, eval_mode, Int64Type, cast_string_to_i64)?
            }
            dt => unreachable!(
                "{}",
                format!("invalid integer type {dt} in cast from string")
            ),
        };
        Ok(cast_array)
    }

    fn unpack_dict_string_array<T: ArrowDictionaryKeyType>(
        array: &ArrayRef,
    ) -> DataFusionResult<ArrayRef> {
        let dict_array = array
            .as_any()
            .downcast_ref::<DictionaryArray<T>>()
            .expect("DictionaryArray<T>");

        let unpacked_array = take(dict_array.values().as_ref(), dict_array.keys(), None)?;
        Ok(unpacked_array)
    }

    fn spark_cast_utf8_to_boolean<OffsetSize>(
        from: &dyn Array,
        eval_mode: EvalMode,
    ) -> CometResult<ArrayRef>
    where
        OffsetSize: OffsetSizeTrait,
    {
        let array = from
            .as_any()
            .downcast_ref::<GenericStringArray<OffsetSize>>()
            .unwrap();

        let output_array = array
            .iter()
            .map(|value| match value {
                Some(value) => match value.to_ascii_lowercase().trim() {
                    "t" | "true" | "y" | "yes" | "1" => Ok(Some(true)),
                    "f" | "false" | "n" | "no" | "0" => Ok(Some(false)),
                    _ if eval_mode == EvalMode::Ansi => Err(CometError::CastInvalidValue {
                        value: value.to_string(),
                        from_type: "STRING".to_string(),
                        to_type: "BOOLEAN".to_string(),
                    }),
                    _ => Ok(None),
                },
                _ => Ok(None),
            })
            .collect::<Result<BooleanArray, _>>()?;

        Ok(Arc::new(output_array))
    }
}

fn cast_string_to_i8(str: &str, eval_mode: EvalMode) -> CometResult<Option<i8>> {
    Ok(cast_string_to_int_with_range_check(
        str,
        eval_mode,
        "TINYINT",
        i8::MIN as i32,
        i8::MAX as i32,
    )?
    .map(|v| v as i8))
}

fn cast_string_to_i16(str: &str, eval_mode: EvalMode) -> CometResult<Option<i16>> {
    Ok(cast_string_to_int_with_range_check(
        str,
        eval_mode,
        "SMALLINT",
        i16::MIN as i32,
        i16::MAX as i32,
    )?
    .map(|v| v as i16))
}

fn cast_string_to_i32(str: &str, eval_mode: EvalMode) -> CometResult<Option<i32>> {
    Ok(do_cast_string_to_int::<i32>(
        str,
        eval_mode,
        "INT",
        i32::MIN,
    )?)
}

fn cast_string_to_i64(str: &str, eval_mode: EvalMode) -> CometResult<Option<i64>> {
    do_cast_string_to_int::<i64>(str, eval_mode, "BIGINT", i64::MIN)
}

fn cast_string_to_int_with_range_check(
    str: &str,
    eval_mode: EvalMode,
    type_name: &str,
    min: i32,
    max: i32,
) -> CometResult<Option<i32>> {
    match do_cast_string_to_int(str, eval_mode, type_name, i32::MIN)? {
        None => Ok(None),
        Some(v) if v >= min && v <= max => Ok(Some(v)),
        _ if eval_mode == EvalMode::Ansi => Err(invalid_value(str, "STRING", type_name)),
        _ => Ok(None),
    }
}

fn do_cast_string_to_int<
    T: Num + PartialOrd + Integer + CheckedSub + CheckedNeg + From<i32> + Copy,
>(
    str: &str,
    eval_mode: EvalMode,
    type_name: &str,
    min_value: T,
) -> CometResult<Option<T>> {
    let chars: Vec<char> = str.chars().collect();
    let mut i = 0;
    let mut end = chars.len();

    // skip leading whitespace
    while i < end && chars[i].is_whitespace() {
        i += 1;
    }

    // skip trailing whitespace
    while end > i && chars[end - 1].is_whitespace() {
        end -= 1;
    }

    // check for empty string
    if i == end {
        return none_or_err(eval_mode, type_name, str);
    }

    // skip + or -
    let negative = chars[i] == '-';
    if negative || chars[i] == '+' {
        i += 1;
        if i == end {
            return none_or_err(eval_mode, type_name, str);
        }
    }

    let mut result: T = T::zero();
    let radix = T::from(10);
    let stop_value = min_value / radix;
    while i < end {
        let b = chars[i];
        i += 1;

        if b == '.' && eval_mode == EvalMode::Legacy {
            // truncate decimal in legacy mode
            break;
        }

        let digit = if b.is_ascii_digit() {
            (b as u32) - ('0' as u32)
        } else {
            return none_or_err(eval_mode, type_name, str);
        };

        // We are going to process the new digit and accumulate the result. However, before doing
        // this, if the result is already smaller than the stopValue(Integer.MIN_VALUE / radix),
        // then result * 10 will definitely be smaller than minValue, and we can stop
        if result < stop_value {
            return none_or_err(eval_mode, type_name, str);
        }

        // Since the previous result is less than or equal to stopValue(Integer.MIN_VALUE / radix),
        // we can just use `result > 0` to check overflow. If result overflows, we should stop
        let v = result * radix;
        let digit = (digit as i32).into();
        match v.checked_sub(&digit) {
            Some(x) if x <= T::zero() => result = x,
            _ => {
                return none_or_err(eval_mode, type_name, str);
            }
        }
    }

    // This is the case when we've encountered a decimal separator. The fractional
    // part will not change the number, but we will verify that the fractional part
    // is well-formed.
    while i < end {
        let b = chars[i];
        if !b.is_ascii_digit() {
            return none_or_err(eval_mode, type_name, str);
        }
        i += 1;
    }

    if !negative {
        if let Some(x) = result.checked_neg() {
            if x < T::zero() {
                return none_or_err(eval_mode, type_name, str);
            }
            result = x;
        } else {
            return none_or_err(eval_mode, type_name, str);
        }
    }

    Ok(Some(result))
}

/// Either return Ok(None) or Err(CometError::CastInvalidValue) depending on the evaluation mode
fn none_or_err<T>(eval_mode: EvalMode, type_name: &str, str: &str) -> CometResult<Option<T>> {
    match eval_mode {
        EvalMode::Ansi => Err(invalid_value(str, "STRING", type_name)),
        _ => Ok(None),
    }
}

fn invalid_value(value: &str, from_type: &str, to_type: &str) -> CometError {
    CometError::CastInvalidValue {
        value: value.to_string(),
        from_type: from_type.to_string(),
        to_type: to_type.to_string(),
    }
}

impl Display for Cast {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Cast [data_type: {}, timezone: {}, child: {}, eval_mode: {:?}]",
            self.data_type, self.timezone, self.child, &self.eval_mode
        )
    }
}

impl PartialEq<dyn Any> for Cast {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.child.eq(&x.child)
                    && self.timezone.eq(&x.timezone)
                    && self.data_type.eq(&x.data_type)
                    && self.eval_mode.eq(&x.eval_mode)
            })
            .unwrap_or(false)
    }
}

impl PhysicalExpr for Cast {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _: &Schema) -> DataFusionResult<DataType> {
        Ok(self.data_type.clone())
    }

    fn nullable(&self, _: &Schema) -> DataFusionResult<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        let arg = self.child.evaluate(batch)?;
        match arg {
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(self.cast_array(array)?)),
            ColumnarValue::Scalar(scalar) => {
                // Note that normally CAST(scalar) should be fold in Spark JVM side. However, for
                // some cases e.g., scalar subquery, Spark will not fold it, so we need to handle it
                // here.
                let array = scalar.to_array()?;
                let scalar = ScalarValue::try_from_array(&self.cast_array(array)?, 0)?;
                Ok(ColumnarValue::Scalar(scalar))
            }
        }
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.child.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion_common::Result<Arc<dyn PhysicalExpr>> {
        match children.len() {
            1 => Ok(Arc::new(Cast::new(
                children[0].clone(),
                self.data_type.clone(),
                self.eval_mode,
                self.timezone.clone(),
            ))),
            _ => internal_err!("Cast should have exactly one child"),
        }
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.child.hash(&mut s);
        self.data_type.hash(&mut s);
        self.timezone.hash(&mut s);
        self.eval_mode.hash(&mut s);
        self.hash(&mut s);
    }
}

#[cfg(test)]
mod test {
    use super::{cast_string_to_i8, EvalMode};

    #[test]
    fn test_cast_string_as_i8() {
        // basic
        assert_eq!(
            cast_string_to_i8("127", EvalMode::Legacy).unwrap(),
            Some(127_i8)
        );
        assert_eq!(cast_string_to_i8("128", EvalMode::Legacy).unwrap(), None);
        assert!(cast_string_to_i8("128", EvalMode::Ansi).is_err());
        // decimals
        assert_eq!(
            cast_string_to_i8("0.2", EvalMode::Legacy).unwrap(),
            Some(0_i8)
        );
        assert_eq!(
            cast_string_to_i8(".", EvalMode::Legacy).unwrap(),
            Some(0_i8)
        );
        // TRY should always return null for decimals
        assert_eq!(cast_string_to_i8("0.2", EvalMode::Try).unwrap(), None);
        assert_eq!(cast_string_to_i8(".", EvalMode::Try).unwrap(), None);
        // ANSI mode should throw error on decimal
        assert!(cast_string_to_i8("0.2", EvalMode::Ansi).is_err());
        assert!(cast_string_to_i8(".", EvalMode::Ansi).is_err());
    }
}
