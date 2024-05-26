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

use std::sync::Arc;
use arrow_schema::{DataType, Schema};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::expressions::{Literal, NegativeExpr};
use datafusion_common::ScalarValue;
use crate::errors::CometError;
use crate::execution::datafusion::expressions::cast::EvalMode;

pub fn create_negate_expr(expr: Arc<dyn PhysicalExpr>, input_schema: Schema, eval_mode: EvalMode) -> Result<Arc<dyn PhysicalExpr>, CometError> {
    if eval_mode == EvalMode::Ansi {
        let checked_input: Result<(), CometError> = check_invalid_inputs(expr.clone(), input_schema.clone());
        match checked_input {
            Ok(_) => {
                return Ok(Arc::new(NegativeExpr::new(expr)));
            },
            Err(e) => {
                return Err(e);
            }
        }
    }
    Ok(Arc::new(NegativeExpr::new(expr)))
}

fn check_invalid_inputs(expr: Arc<dyn PhysicalExpr>, input_schema: Schema) -> Result<(), CometError> {
    // TODO

    let data_type = expr.data_type(&input_schema)?;    

    let value = expr.as_any().downcast_ref::<Literal>().unwrap().value();
    match data_type {
        DataType::Interval(_) => {
            // TODO: implement checks for interval data type
        }
        DataType::Int8 => {
            if let ScalarValue::Int8(Some(int_value)) = value {
                if *int_value <= i8::MIN || *int_value >= i8::MAX {
                    return Err(CometError::ArithmeticOverflow{
                        from_type: "integer".to_string(),
                    });
                }
            }
        }
        DataType::Int16 => {
            if let ScalarValue::Int16(Some(int_value)) = value {
                if *int_value <= i16::MIN || *int_value >= i16::MAX {
                    return Err(CometError::ArithmeticOverflow{
                        from_type: "integer".to_string(),
                    });
                }
            }
        }
        DataType::Int32 => {
            if let ScalarValue::Int32(Some(int_value)) = value {
                if *int_value <= i32::MIN || *int_value >= i32::MAX {
                    return Err(CometError::ArithmeticOverflow{
                        from_type: "integer".to_string(),
                    });
                }
            }
        }
        DataType::Int64 => {
            if let ScalarValue::Int64(Some(int_value)) = value {
                if *int_value <= i64::MIN || *int_value >= i64::MAX {
                    return Err(CometError::ArithmeticOverflow{
                        from_type: "integer".to_string(),
                    });
                }
            }
        }
        DataType::UInt8 => {
            if let ScalarValue::UInt8(Some(uint_value)) = value {
                if *uint_value <= u8::MIN || *uint_value >= u8::MAX {
                    return Err(CometError::ArithmeticOverflow{
                        from_type: "integer".to_string(),
                    });
                }
            }
        }
        DataType::UInt16 => {
            if let ScalarValue::UInt16(Some(uint_value)) = value {
                if *uint_value <= u16::MIN || *uint_value >= u16::MAX {
                    return Err(CometError::ArithmeticOverflow{
                        from_type: "integer".to_string(),
                    });
                }
            }
        }
        DataType::UInt32 => {
            if let ScalarValue::UInt32(Some(uint_value)) = value {
                if *uint_value <= u32::MIN || *uint_value >= u32::MAX {
                    return Err(CometError::ArithmeticOverflow{
                        from_type: "integer".to_string(),
                    });
                }
            }
        }
        DataType::UInt64 => {
            if let ScalarValue::UInt64(Some(uint_value)) = value {
                if *uint_value <= u64::MIN || *uint_value >= u64::MAX {
                    return Err(CometError::ArithmeticOverflow{
                        from_type: "integer".to_string(),
                    });
                }
            }
        }
        DataType::Float32 => {
            if let ScalarValue::Float32(Some(float_value)) = value {
                if *float_value <= f32::MIN || *float_value >= f32::MAX {
                    return Err(CometError::ArithmeticOverflow{
                        from_type: "float".to_string(),
                    });
                }
            }
        }
        DataType::Float64 => {
            if let ScalarValue::Float64(Some(float_value)) = value {
                if *float_value <= f64::MIN || *float_value >= f64::MAX {
                    return Err(CometError::ArithmeticOverflow{
                        from_type: "float".to_string(),
                    });
                }
            }
        }
        _ => {
            unimplemented!("Overflow error: cannot negate value of type {:?}", value.data_type());
        }
    }
    
    Ok(())
}

// add a test
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_negate_expr() {
        let expr = Arc::new(Literal::new(1_i32.into()));
        let eval_mode = EvalMode::Ansi;
        let result = create_negate_expr(expr, Schema::empty(), eval_mode);
        assert!(result.is_ok());
        if result.is_ok() {
            let result = result.unwrap();
            assert_eq!(result.to_string(), "(- 1)");
        }

        let expr = Arc::new(Literal::new(i32::MAX.into()));
        let eval_mode = EvalMode::Ansi;
        let result = create_negate_expr(expr, Schema::empty(), eval_mode);
        assert!(result.is_err());
    }
}