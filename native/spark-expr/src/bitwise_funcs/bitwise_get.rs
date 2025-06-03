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

use arrow::{array::*, datatypes::DataType};
use datafusion::common::{exec_err, internal_datafusion_err, Result, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use datafusion::logical_expr::{ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct SparkBitwiseGet {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for SparkBitwiseGet {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkBitwiseGet {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: vec![],
        }
    }
}

impl ScalarUDFImpl for SparkBitwiseGet {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "bit_get"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args: [ColumnarValue; 2] = args
            .args
            .try_into()
            .map_err(|_| internal_datafusion_err!("bit_get expects exactly two arguments"))?;
        spark_bit_get(&args)
    }
}

macro_rules! bit_get_scalar_position {
    ($args:expr, $array_type:ty, $pos:expr, $bit_size:expr) => {{
        if let Some(pos) = $pos {
            check_position(*pos, $bit_size as i32)?;
        }
        let args = $args
            .as_any()
            .downcast_ref::<$array_type>()
            .expect("bit_get_scalar_position failed to downcast array");

        let result: Int8Array = args
            .iter()
            .map(|x| x.and_then(|x| $pos.map(|pos| bit_get(x.into(), pos))))
            .collect();

        Ok(Arc::new(result))
    }};
}

macro_rules! bit_get_array_positions {
    ($args:expr, $array_type:ty, $positions:expr, $bit_size:expr) => {{
        let args = $args
            .as_any()
            .downcast_ref::<$array_type>()
            .expect("bit_get_array_positions failed to downcast args array");

        let positions = $positions
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("bit_get_array_positions failed to downcast positions array");

        for pos in positions.iter().flatten() {
            check_position(pos, $bit_size as i32)?
        }

        let result: Int8Array = args
            .iter()
            .zip(positions.iter())
            .map(|(i, p)| i.and_then(|i| p.map(|p| bit_get(i.into(), p))))
            .collect();

        Ok(Arc::new(result))
    }};
}

pub fn spark_bit_get(args: &[ColumnarValue; 2]) -> Result<ColumnarValue> {
    match args {
        [ColumnarValue::Array(args), ColumnarValue::Scalar(ScalarValue::Int32(pos))] => {
            let result: Result<ArrayRef> = match args.data_type() {
                DataType::Int8 => bit_get_scalar_position!(args, Int8Array, pos, i8::BITS),
                DataType::Int16 => bit_get_scalar_position!(args, Int16Array, pos, i16::BITS),
                DataType::Int32 => bit_get_scalar_position!(args, Int32Array, pos, i32::BITS),
                DataType::Int64 => bit_get_scalar_position!(args, Int64Array, pos, i64::BITS),
                _ => exec_err!(
                    "Can't be evaluated because the expression's type is {:?}, not signed int",
                    args.data_type()
                ),
            };
            result.map(ColumnarValue::Array)
        },
        [ColumnarValue::Array(args), ColumnarValue::Array(positions)] => {
            if args.len() != positions.len() {
                return exec_err!(
                    "Input arrays must have equal length. Positions array has {} elements, but arguments array has {} elements",
                    positions.len(), args.len()
                );
            }
            if !matches!(positions.data_type(), DataType::Int32) {
                return exec_err!(
                    "Invalid data type for positions array: expected `Int32`, found `{}`",
                    positions.data_type()
                );
            }
            let result: Result<ArrayRef> = match args.data_type() {
                DataType::Int8 => bit_get_array_positions!(args, Int8Array, positions, i8::BITS),
                DataType::Int16 => bit_get_array_positions!(args, Int16Array, positions, i16::BITS),
                DataType::Int32 => bit_get_array_positions!(args, Int32Array, positions, i32::BITS),
                DataType::Int64 => bit_get_array_positions!(args, Int64Array, positions, i64::BITS),
                _ => exec_err!(
                    "Can't be evaluated because the expression's type is {:?}, not signed int",
                    args.data_type()
                ),
            };
            result.map(ColumnarValue::Array)
        }
        _ => exec_err!(
            "Invalid input to function bit_get. Expected (IntegralType array, Int32Scalar) or (IntegralType array, Int32Array)"
        ),
    }
}

fn bit_get(arg: i64, pos: i32) -> i8 {
    ((arg >> pos) & 1) as i8
}

fn check_position(pos: i32, bit_size: i32) -> Result<()> {
    if pos < 0 {
        return exec_err!("Invalid bit position: {:?} is less than zero", pos);
    }
    if bit_size <= pos {
        return exec_err!(
            "Invalid bit position: {:?} exceeds the bit upper limit: {:?}",
            pos,
            bit_size
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::cast::as_int8_array;

    #[test]
    fn bitwise_get_scalar_position() -> Result<()> {
        let args = [
            ColumnarValue::Array(Arc::new(Int32Array::from(vec![
                Some(1),
                None,
                Some(1234553454),
            ]))),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(1))),
        ];

        let expected = &Int8Array::from(vec![Some(0), None, Some(1)]);

        let ColumnarValue::Array(result) = spark_bit_get(&args)? else {
            unreachable!()
        };

        let result = as_int8_array(&result).expect("failed to downcast to Int8Array");

        assert_eq!(result, expected);

        Ok(())
    }

    #[test]
    fn bitwise_get_scalar_negative_position() -> Result<()> {
        let args = [
            ColumnarValue::Array(Arc::new(Int32Array::from(vec![
                Some(1),
                None,
                Some(1234553454),
            ]))),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(-1))),
        ];

        let expected = String::from("Execution error: Invalid bit position: -1 is less than zero");
        let result = spark_bit_get(&args).err().unwrap().to_string();

        assert_eq!(result, expected);

        Ok(())
    }

    #[test]
    fn bitwise_get_scalar_overflow_position() -> Result<()> {
        let args = [
            ColumnarValue::Array(Arc::new(Int32Array::from(vec![
                Some(1),
                None,
                Some(1234553454),
            ]))),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(33))),
        ];

        let expected = String::from(
            "Execution error: Invalid bit position: 33 exceeds the bit upper limit: 32",
        );
        let result = spark_bit_get(&args).err().unwrap().to_string();

        assert_eq!(result, expected);

        Ok(())
    }

    #[test]
    fn bitwise_get_array_positions() -> Result<()> {
        let args = [
            ColumnarValue::Array(Arc::new(Int32Array::from(vec![
                Some(1),
                None,
                Some(1234553454),
            ]))),
            ColumnarValue::Array(Arc::new(Int32Array::from(vec![Some(1), None, Some(1)]))),
        ];

        let expected = &Int8Array::from(vec![Some(0), None, Some(1)]);

        let ColumnarValue::Array(result) = spark_bit_get(&args)? else {
            unreachable!()
        };

        let result = as_int8_array(&result).expect("failed to downcast to Int8Array");

        assert_eq!(result, expected);

        Ok(())
    }

    #[test]
    fn bitwise_get_array_positions_contains_negative() -> Result<()> {
        let args = [
            ColumnarValue::Array(Arc::new(Int32Array::from(vec![
                Some(1),
                None,
                Some(1234553454),
            ]))),
            ColumnarValue::Array(Arc::new(Int32Array::from(vec![Some(-1), None, Some(1)]))),
        ];

        let expected = String::from("Execution error: Invalid bit position: -1 is less than zero");
        let result = spark_bit_get(&args).err().unwrap().to_string();

        assert_eq!(result, expected);

        Ok(())
    }

    #[test]
    fn bitwise_get_array_positions_contains_overflow() -> Result<()> {
        let args = [
            ColumnarValue::Array(Arc::new(Int32Array::from(vec![
                Some(1),
                None,
                Some(1234553454),
            ]))),
            ColumnarValue::Array(Arc::new(Int32Array::from(vec![Some(33), None, Some(1)]))),
        ];

        let expected = String::from(
            "Execution error: Invalid bit position: 33 exceeds the bit upper limit: 32",
        );
        let result = spark_bit_get(&args).err().unwrap().to_string();

        assert_eq!(result, expected);

        Ok(())
    }
}
