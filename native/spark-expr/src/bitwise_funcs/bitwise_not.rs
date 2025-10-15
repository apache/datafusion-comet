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
use datafusion::common::{
    exec_err, internal_datafusion_err, internal_err, DataFusionError, Result,
};
use datafusion::logical_expr::{ColumnarValue, Volatility};
use datafusion::logical_expr::{ScalarFunctionArgs, ScalarUDFImpl, Signature};
use std::{any::Any, sync::Arc};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkBitwiseNot {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for SparkBitwiseNot {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkBitwiseNot {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: vec![],
        }
    }
}

impl ScalarUDFImpl for SparkBitwiseNot {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "bit_not"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(match arg_types[0] {
            DataType::Int8 => DataType::Int8,
            DataType::Int16 => DataType::Int16,
            DataType::Int32 => DataType::Int32,
            DataType::Int64 => DataType::Int64,
            DataType::Null => DataType::Null,
            _ => return exec_err!("{} function can only accept integral arrays", self.name()),
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args: [ColumnarValue; 1] = args
            .args
            .try_into()
            .map_err(|_| internal_datafusion_err!("bit_not expects exactly one argument"))?;
        bitwise_not(args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

macro_rules! compute_op {
    ($OPERAND:expr, $DT:ident) => {{
        let operand = $OPERAND.as_any().downcast_ref::<$DT>().ok_or_else(|| {
            DataFusionError::Execution(format!(
                "compute_op failed to downcast array to: {:?}",
                stringify!($DT)
            ))
        })?;
        let result: $DT = operand.iter().map(|x| x.map(|y| !y)).collect();
        Ok(Arc::new(result))
    }};
}

pub fn bitwise_not(args: [ColumnarValue; 1]) -> Result<ColumnarValue> {
    match args {
        [ColumnarValue::Array(array)] => {
            let result: Result<ArrayRef> = match array.data_type() {
                DataType::Int8 => compute_op!(array, Int8Array),
                DataType::Int16 => compute_op!(array, Int16Array),
                DataType::Int32 => compute_op!(array, Int32Array),
                DataType::Int64 => compute_op!(array, Int64Array),
                _ => exec_err!("bit_not can't be evaluated because the expression's type is {:?}, not signed int", array.data_type()),
            };
            result.map(ColumnarValue::Array)
        }
        [ColumnarValue::Scalar(_)] => internal_err!("shouldn't go to bitwise not scalar path"),
    }
}

#[cfg(test)]
mod tests {
    use datafusion::common::{cast::as_int32_array, Result};

    use super::*;

    #[test]
    fn bitwise_not_op() -> Result<()> {
        let int_array = Int32Array::from(vec![
            Some(1),
            Some(2),
            None,
            Some(12345),
            Some(89),
            Some(-3456),
        ]);
        let expected = &Int32Array::from(vec![
            Some(-2),
            Some(-3),
            None,
            Some(-12346),
            Some(-90),
            Some(3455),
        ]);

        let columnar_value = ColumnarValue::Array(Arc::new(int_array));

        let result = bitwise_not([columnar_value])?;
        let result = match result {
            ColumnarValue::Array(array) => array,
            _ => panic!("Expected array"),
        };
        let result = as_int32_array(&result).expect("failed to downcast to In32Array");
        assert_eq!(result, expected);

        Ok(())
    }
}
