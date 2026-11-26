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

//! Conversion of a serialized Spark literal into a DataFusion `ScalarValue`.

use arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use datafusion::common::scalar::ScalarStructBuilder;
use datafusion::common::utils::SingleRowListArrayBuilder;
use datafusion::common::ScalarValue;
use datafusion_comet_proto::spark_expression::{literal::Value, Literal};
use num::{BigInt, ToPrimitive};

use super::literal_to_array_ref;
use crate::execution::operators::ExecutionError::{self, GeneralError};
use crate::execution::serde::to_arrow_datatype;

/// Converts a serialized Spark literal, including a null of any supported type, to a
/// `ScalarValue`.
pub(super) fn literal_to_scalar(literal: &Literal) -> Result<ScalarValue, ExecutionError> {
    let data_type = to_arrow_datatype(literal.datatype.as_ref().unwrap());
    let scalar_value = if literal.is_null {
        match data_type {
            DataType::Boolean => ScalarValue::Boolean(None),
            DataType::Int8 => ScalarValue::Int8(None),
            DataType::Int16 => ScalarValue::Int16(None),
            DataType::Int32 => ScalarValue::Int32(None),
            DataType::Int64 => ScalarValue::Int64(None),
            DataType::Float32 => ScalarValue::Float32(None),
            DataType::Float64 => ScalarValue::Float64(None),
            DataType::Utf8 => ScalarValue::Utf8(None),
            DataType::Date32 => ScalarValue::Date32(None),
            DataType::Timestamp(TimeUnit::Microsecond, timezone) => {
                ScalarValue::TimestampMicrosecond(None, timezone)
            }
            DataType::Binary => ScalarValue::Binary(None),
            DataType::Decimal128(p, s) => ScalarValue::Decimal128(None, p, s),
            DataType::Struct(fields) => ScalarStructBuilder::new_null(fields),
            DataType::Map(f, s) => DataType::Map(f, s).try_into()?,
            DataType::List(f) => DataType::List(f).try_into()?,
            DataType::Null => ScalarValue::Null,
            DataType::Time64(TimeUnit::Nanosecond) => ScalarValue::Time64Nanosecond(None),
            DataType::Interval(IntervalUnit::YearMonth) => ScalarValue::IntervalYearMonth(None),
            DataType::Duration(TimeUnit::Microsecond) => ScalarValue::DurationMicrosecond(None),
            DataType::Interval(arrow::datatypes::IntervalUnit::MonthDayNano) => {
                ScalarValue::IntervalMonthDayNano(None)
            }
            dt => return Err(GeneralError(format!("{dt:?} is not supported in Comet"))),
        }
    } else {
        match literal.value.as_ref().unwrap() {
            Value::BoolVal(value) => ScalarValue::Boolean(Some(*value)),
            Value::ByteVal(value) => ScalarValue::Int8(Some(*value as i8)),
            Value::ShortVal(value) => ScalarValue::Int16(Some(*value as i16)),
            Value::IntVal(value) => match data_type {
                DataType::Int32 => ScalarValue::Int32(Some(*value)),
                DataType::Date32 => ScalarValue::Date32(Some(*value)),
                DataType::Interval(IntervalUnit::YearMonth) => {
                    ScalarValue::IntervalYearMonth(Some(*value))
                }
                dt => {
                    return Err(GeneralError(format!(
                        "Expected either 'Int32', 'Date32', or 'Interval(YearMonth)' for IntVal, but found {dt:?}"
                    )))
                }
            },
            Value::LongVal(value) => match data_type {
                DataType::Int64 => ScalarValue::Int64(Some(*value)),
                DataType::Timestamp(TimeUnit::Microsecond, None) => {
                    ScalarValue::TimestampMicrosecond(Some(*value), None)
                }
                DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) => {
                    ScalarValue::TimestampMicrosecond(Some(*value), Some(tz))
                }
                DataType::Duration(TimeUnit::Microsecond) => {
                    ScalarValue::DurationMicrosecond(Some(*value))
                }
                DataType::Time64(TimeUnit::Nanosecond) => {
                    ScalarValue::Time64Nanosecond(Some(*value))
                }
                dt => {
                    return Err(GeneralError(format!(
                        "Expected 'Int64', 'Timestamp', 'Duration(Microsecond)', or 'Time64(Nanosecond)' for LongVal, but found {dt:?}"
                    )))
                }
            },
            Value::FloatVal(value) => ScalarValue::Float32(Some(*value)),
            Value::DoubleVal(value) => ScalarValue::Float64(Some(*value)),
            Value::StringVal(value) => ScalarValue::Utf8(Some(value.clone())),
            Value::BytesVal(value) => ScalarValue::Binary(Some(value.clone())),
            Value::DecimalVal(value) => {
                let big_integer = BigInt::from_signed_bytes_be(value);
                let integer = big_integer.to_i128().ok_or_else(|| {
                    GeneralError(format!(
                        "Cannot parse {big_integer:?} as i128 for Decimal literal"
                    ))
                })?;

                match data_type {
                    DataType::Decimal128(p, s) => {
                        ScalarValue::Decimal128(Some(integer), p, s)
                    }
                    dt => {
                        return Err(GeneralError(format!(
                            "Decimal literal's data type should be Decimal128 but got {dt:?}"
                        )))
                    }
                }
            }
            Value::ListVal(values) => {
                if let DataType::List(_) = data_type {
                    SingleRowListArrayBuilder::new(literal_to_array_ref(data_type, values.clone())?).build_list_scalar()
                } else {
                    return Err(GeneralError(format!(
                        "Expected DataType::List but got {data_type:?}"
                    )));
                }
            }
        }
    };
    Ok(scalar_value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_comet_proto::spark_expression::data_type::DataTypeId;

    fn literal(type_id: DataTypeId, is_null: bool, value: Option<Value>) -> Literal {
        Literal {
            value,
            is_null,
            datatype: Some(datafusion_comet_proto::spark_expression::DataType {
                type_id: type_id as i32,
                type_info: None,
            }),
        }
    }

    #[test]
    fn null_literal_keeps_its_type() {
        let scalar = literal_to_scalar(&literal(DataTypeId::Int32, true, None)).unwrap();
        assert_eq!(scalar, ScalarValue::Int32(None));
    }

    #[test]
    fn int_literal_converts_to_the_declared_type() {
        let scalar =
            literal_to_scalar(&literal(DataTypeId::Date, false, Some(Value::IntVal(7)))).unwrap();
        assert_eq!(scalar, ScalarValue::Date32(Some(7)));
    }

    #[test]
    fn int_value_for_an_unexpected_type_is_rejected() {
        let err = literal_to_scalar(&literal(DataTypeId::String, false, Some(Value::IntVal(7))))
            .unwrap_err();
        assert!(err.to_string().contains("for IntVal"), "{err}");
    }
}
