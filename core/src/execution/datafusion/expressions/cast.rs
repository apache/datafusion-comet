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
            ) => match to_type {
                DataType::Int8 => Self::spark_cast_utf8_to_i8::<i32>(&array, self.eval_mode)?,
                DataType::Int16 => Self::spark_cast_utf8_to_i16::<i32>(&array, self.eval_mode)?,
                DataType::Int32 => Self::spark_cast_utf8_to_i32::<i32>(&array, self.eval_mode)?,
                DataType::Int64 => Self::spark_cast_utf8_to_i64::<i32>(&array, self.eval_mode)?,
                _ => unreachable!("invalid integral type in cast from string"),
            },
            (
                DataType::Dictionary(key_type, value_type),
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64,
            ) if key_type.as_ref() == &DataType::Int32
                && value_type.as_ref() == &DataType::Utf8 =>
            {
                // TODO file follow on issue for optimizing this to avoid unpacking first
                let unpacked_array = Self::unpack_dict_string_array::<Int32Type>(&array)?;
                match to_type {
                    DataType::Int8 => {
                        Self::spark_cast_utf8_to_i8::<i32>(&unpacked_array, self.eval_mode)?
                    }
                    DataType::Int16 => {
                        Self::spark_cast_utf8_to_i16::<i32>(&unpacked_array, self.eval_mode)?
                    }
                    DataType::Int32 => {
                        Self::spark_cast_utf8_to_i32::<i32>(&unpacked_array, self.eval_mode)?
                    }
                    DataType::Int64 => {
                        Self::spark_cast_utf8_to_i64::<i32>(&unpacked_array, self.eval_mode)?
                    }
                    _ => {
                        unreachable!("invalid integral type in cast from dictionary-encoded string")
                    }
                }
            }
            _ => {
                // when we have no Spark-specific casting we delegate to DataFusion
                cast_with_options(&array, to_type, &CAST_OPTIONS)?
            }
        };
        Ok(spark_cast(cast_result, from_type, to_type))
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

    // TODO reduce code duplication

    fn spark_cast_utf8_to_i8<OffsetSize>(
        from: &dyn Array,
        eval_mode: EvalMode,
    ) -> CometResult<ArrayRef>
    where
        OffsetSize: OffsetSizeTrait,
    {
        let string_array = from
            .as_any()
            .downcast_ref::<GenericStringArray<OffsetSize>>()
            .expect("spark_cast_utf8_to_i8 expected a string array");

        // cast the dictionary values from string to int8
        let mut cast_array = PrimitiveArray::<Int8Type>::builder(string_array.len());
        for i in 0..string_array.len() {
            if string_array.is_null(i) {
                cast_array.append_null()
            } else if let Some(cast_value) =
                cast_string_to_i8(string_array.value(i).trim(), eval_mode)?
            {
                cast_array.append_value(cast_value);
            } else {
                cast_array.append_null()
            }
        }
        Ok(Arc::new(cast_array.finish()))
    }

    fn spark_cast_utf8_to_i16<OffsetSize>(
        from: &dyn Array,
        eval_mode: EvalMode,
    ) -> CometResult<ArrayRef>
    where
        OffsetSize: OffsetSizeTrait,
    {
        let string_array = from
            .as_any()
            .downcast_ref::<GenericStringArray<OffsetSize>>()
            .expect("spark_cast_utf8_to_i16 expected a string array");

        // cast the dictionary values from string to int8
        let mut cast_array = PrimitiveArray::<Int16Type>::builder(string_array.len());
        for i in 0..string_array.len() {
            if string_array.is_null(i) {
                cast_array.append_null()
            } else if let Some(cast_value) =
                cast_string_to_i16(string_array.value(i).trim(), eval_mode)?
            {
                cast_array.append_value(cast_value);
            } else {
                cast_array.append_null()
            }
        }
        Ok(Arc::new(cast_array.finish()))
    }

    fn spark_cast_utf8_to_i32<OffsetSize>(
        from: &dyn Array,
        eval_mode: EvalMode,
    ) -> CometResult<ArrayRef>
    where
        OffsetSize: OffsetSizeTrait,
    {
        let string_array = from
            .as_any()
            .downcast_ref::<GenericStringArray<OffsetSize>>()
            .expect("spark_cast_utf8_to_i32 expected a string array");

        // cast the dictionary values from string to int8
        let mut cast_array = PrimitiveArray::<Int32Type>::builder(string_array.len());
        for i in 0..string_array.len() {
            if string_array.is_null(i) {
                cast_array.append_null()
            } else if let Some(cast_value) =
                cast_string_to_i32(string_array.value(i).trim(), eval_mode)?
            {
                cast_array.append_value(cast_value);
            } else {
                cast_array.append_null()
            }
        }
        Ok(Arc::new(cast_array.finish()))
    }

    fn spark_cast_utf8_to_i64<OffsetSize>(
        from: &dyn Array,
        eval_mode: EvalMode,
    ) -> CometResult<ArrayRef>
    where
        OffsetSize: OffsetSizeTrait,
    {
        let string_array = from
            .as_any()
            .downcast_ref::<GenericStringArray<OffsetSize>>()
            .expect("spark_cast_utf8_to_i64 expected a string array");

        // cast the dictionary values from string to int8
        let mut cast_array = PrimitiveArray::<Int64Type>::builder(string_array.len());
        for i in 0..string_array.len() {
            if string_array.is_null(i) {
                cast_array.append_null()
            } else if let Some(cast_value) =
                cast_string_to_i64(string_array.value(i).trim(), eval_mode)?
            {
                cast_array.append_value(cast_value);
            } else {
                cast_array.append_null()
            }
        }
        Ok(Arc::new(cast_array.finish()))
    }
}

fn cast_string_to_i8(str: &str, eval_mode: EvalMode) -> CometResult<Option<i8>> {
    Ok(cast_string_to_integral_with_range_check(
        str,
        eval_mode,
        "TINYINT",
        i8::MIN as i32,
        i8::MAX as i32,
    )?
    .map(|v| v as i8))
}

fn cast_string_to_i16(str: &str, eval_mode: EvalMode) -> CometResult<Option<i16>> {
    Ok(cast_string_to_integral_with_range_check(
        str,
        eval_mode,
        "SMALLINT",
        i16::MIN as i32,
        i16::MAX as i32,
    )?
    .map(|v| v as i16))
}

fn cast_string_to_i32(str: &str, eval_mode: EvalMode) -> CometResult<Option<i32>> {
    let mut accum = CastStringToIntegral32::default();
    do_cast_string_to_int(&mut accum, str, eval_mode, "INT")?;
    Ok(accum.result)
}

fn cast_string_to_i64(str: &str, eval_mode: EvalMode) -> CometResult<Option<i64>> {
    let mut accum = CastStringToIntegral64::default();
    do_cast_string_to_int(&mut accum, str, eval_mode, "BIGINT")?;
    Ok(accum.result)
}

fn cast_string_to_integral_with_range_check(
    str: &str,
    eval_mode: EvalMode,
    type_name: &str,
    min: i32,
    max: i32,
) -> CometResult<Option<i32>> {
    let mut accum = CastStringToIntegral32::default();
    do_cast_string_to_int(&mut accum, str, eval_mode, type_name)?;
    match accum.result {
        None => Ok(None),
        Some(v) if v >= min && v <= max => Ok(Some(v)),
        _ if eval_mode == EvalMode::Ansi => Err(invalid_value(str, "STRING", type_name)),
        _ => Ok(None),
    }
}

trait CastStringToIntegral {
    fn accumulate(
        &mut self,
        eval_mode: EvalMode,
        type_name: &str,
        str: &str,
        digit: u32,
    ) -> CometResult<()>;

    fn reset(&mut self);

    fn finish(
        &mut self,
        eval_mode: EvalMode,
        type_name: &str,
        str: &str,
        negative: bool,
    ) -> CometResult<()>;
}
struct CastStringToIntegral32 {
    negative: bool,
    result: Option<i32>,
    radix: i32,
}

impl Default for CastStringToIntegral32 {
    fn default() -> Self {
        Self {
            negative: false,
            result: Some(0),
            radix: 10,
        }
    }
}

impl CastStringToIntegral for CastStringToIntegral32 {
    fn accumulate(
        &mut self,
        eval_mode: EvalMode,
        type_name: &str,
        str: &str,
        digit: u32,
    ) -> CometResult<()> {
        if self.result.is_some() && self.result.unwrap() < i32::MIN / self.radix {
            self.reset();
            return none_or_err(eval_mode, type_name, str);
        }
        self.result = Some(self.result.unwrap_or(0) * self.radix - digit as i32);
        if self.result.unwrap() > 0 {
            self.reset();
            return none_or_err(eval_mode, type_name, str);
        }
        Ok(())
    }
    fn reset(&mut self) {
        self.result = None;
    }

    fn finish(
        &mut self,
        eval_mode: EvalMode,
        type_name: &str,
        str: &str,
        negative: bool,
    ) -> CometResult<()> {
        if self.result.is_some() && !negative {
            self.result = Some(-self.result.unwrap());
            if self.result.unwrap() < 0 {
                return none_or_err(eval_mode, type_name, str);
            }
        }
        Ok(())
    }
}

struct CastStringToIntegral64 {
    negative: bool,
    result: Option<i64>,
    radix: i64,
}

impl Default for CastStringToIntegral64 {
    fn default() -> Self {
        Self {
            negative: false,
            result: Some(0),
            radix: 10,
        }
    }
}

impl CastStringToIntegral for CastStringToIntegral64 {
    fn accumulate(
        &mut self,
        eval_mode: EvalMode,
        type_name: &str,
        str: &str,
        digit: u32,
    ) -> CometResult<()> {
        if self.result.unwrap_or(0) < i64::MIN / self.radix {
            self.reset();
            return none_or_err(eval_mode, type_name, str);
        }
        self.result = Some(self.result.unwrap_or(0) * self.radix - digit as i64);
        if self.result.unwrap() > 0 {
            self.reset();
            return none_or_err(eval_mode, type_name, str);
        }
        Ok(())
    }

    fn reset(&mut self) {
        self.result = None;
    }

    fn finish(
        &mut self,
        eval_mode: EvalMode,
        type_name: &str,
        str: &str,
        negative: bool,
    ) -> CometResult<()> {
        if self.result.is_some() && !negative {
            self.result = Some(-self.result.unwrap());
            if self.result.unwrap() < 0 {
                return none_or_err(eval_mode, type_name, str);
            }
        }
        Ok(())
    }
}

fn do_cast_string_to_int(
    accumulator: &mut dyn CastStringToIntegral,
    str: &str,
    eval_mode: EvalMode,
    type_name: &str,
) -> CometResult<()> {
    //TODO avoid trim and parse and skip whitespace chars instead
    let str = str.trim();
    if str.is_empty() {
        accumulator.reset();
        return Ok(());
    }
    let chars: Vec<char> = str.chars().collect();
    let mut i = 0;

    // skip + or -
    let negative = chars[0] == '-';
    if negative || chars[0] == '+' {
        i += 1;
        if i == chars.len() {
            accumulator.reset();
            return Ok(());
        }
    }

    while i < chars.len() {
        let b = chars[i];
        i += 1;

        if b == '.' && eval_mode == EvalMode::Legacy {
            // truncate decimal in legacy mode
            break;
        }

        let digit = if b.is_ascii_digit() {
            (b as u32) - ('0' as u32)
        } else {
            accumulator.reset();
            return none_or_err(eval_mode, type_name, str);
        };

        accumulator.accumulate(eval_mode, type_name, str, digit)?;
    }

    // This is the case when we've encountered a decimal separator. The fractional
    // part will not change the number, but we will verify that the fractional part
    // is well-formed.
    while i < chars.len() {
        let b = chars[i];
        if !b.is_ascii_digit() {
            accumulator.reset();
            return none_or_err(eval_mode, type_name, str);
        }
        i += 1;
    }

    accumulator.finish(eval_mode, type_name, str, negative)?;

    Ok(())
}

/// Either return Ok(None) or Err(CometError::CastInvalidValue) depending on the evaluation mode
fn none_or_err(eval_mode: EvalMode, type_name: &str, str: &str) -> CometResult<()> {
    match eval_mode {
        EvalMode::Ansi => Err(invalid_value(str, "STRING", type_name)),
        _ => Ok(()),
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
