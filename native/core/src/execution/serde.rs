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

//! Ser/De for expression/operators.

use super::operators::ExecutionError;
use crate::errors::ExpressionError;
use arrow::datatypes::{DataType as ArrowDataType, TimeUnit};
use arrow::datatypes::{Field, Fields};
use datafusion_comet_proto::{
    spark_config, spark_expression,
    spark_expression::data_type::{
        data_type_info::DatatypeStruct,
        DataTypeId,
        DataTypeId::{Bool, Bytes, Decimal, Double, Float, Int16, Int32, Int64, Int8, String},
    },
    spark_expression::DataType,
    spark_operator,
};
use prost::Message;
use std::{io::Cursor, sync::Arc};

impl From<prost::DecodeError> for ExpressionError {
    fn from(error: prost::DecodeError) -> ExpressionError {
        ExpressionError::Deserialize(error.to_string())
    }
}

impl From<prost::UnknownEnumValue> for ExpressionError {
    fn from(error: prost::UnknownEnumValue) -> ExpressionError {
        ExpressionError::Deserialize(error.to_string())
    }
}

impl From<prost::DecodeError> for ExecutionError {
    fn from(error: prost::DecodeError) -> ExecutionError {
        ExecutionError::DeserializeError(error.to_string())
    }
}

impl From<prost::UnknownEnumValue> for ExecutionError {
    fn from(error: prost::UnknownEnumValue) -> ExecutionError {
        ExecutionError::DeserializeError(error.to_string())
    }
}

/// Deserialize bytes to protobuf type of expression
pub fn deserialize_expr(buf: &[u8]) -> Result<spark_expression::Expr, ExpressionError> {
    match spark_expression::Expr::decode(&mut Cursor::new(buf)) {
        Ok(e) => Ok(e),
        Err(err) => Err(ExpressionError::from(err)),
    }
}

/// Deserialize bytes to protobuf type of operator
pub fn deserialize_op(buf: &[u8]) -> Result<spark_operator::Operator, ExecutionError> {
    match spark_operator::Operator::decode(&mut Cursor::new(buf)) {
        Ok(e) => Ok(e),
        Err(err) => Err(ExecutionError::from(err)),
    }
}

/// Deserialize bytes to protobuf type of data type
pub fn deserialize_config(buf: &[u8]) -> Result<spark_config::ConfigMap, ExecutionError> {
    match spark_config::ConfigMap::decode(&mut Cursor::new(buf)) {
        Ok(e) => Ok(e),
        Err(err) => Err(ExecutionError::from(err)),
    }
}

/// Deserialize bytes to protobuf type of data type
pub fn deserialize_data_type(buf: &[u8]) -> Result<spark_expression::DataType, ExecutionError> {
    match spark_expression::DataType::decode(&mut Cursor::new(buf)) {
        Ok(e) => Ok(e),
        Err(err) => Err(ExecutionError::from(err)),
    }
}

/// Converts Protobuf data type to Arrow data type.
pub fn to_arrow_datatype(dt_value: &DataType) -> ArrowDataType {
    match DataTypeId::try_from(dt_value.type_id).unwrap() {
        Bool => ArrowDataType::Boolean,
        Int8 => ArrowDataType::Int8,
        Int16 => ArrowDataType::Int16,
        Int32 => ArrowDataType::Int32,
        Int64 => ArrowDataType::Int64,
        Float => ArrowDataType::Float32,
        Double => ArrowDataType::Float64,
        String => ArrowDataType::Utf8,
        Bytes => ArrowDataType::Binary,
        Decimal => match dt_value
            .type_info
            .as_ref()
            .unwrap()
            .datatype_struct
            .as_ref()
            .unwrap()
        {
            DatatypeStruct::Decimal(info) => {
                ArrowDataType::Decimal128(info.precision as u8, info.scale as i8)
            }
            _ => unreachable!(),
        },
        DataTypeId::Timestamp => {
            ArrowDataType::Timestamp(TimeUnit::Microsecond, Some("UTC".to_string().into()))
        }
        DataTypeId::TimestampNtz => ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
        DataTypeId::Date => ArrowDataType::Date32,
        DataTypeId::Null => ArrowDataType::Null,
        DataTypeId::List => match dt_value
            .type_info
            .as_ref()
            .unwrap()
            .datatype_struct
            .as_ref()
            .unwrap()
        {
            DatatypeStruct::List(info) => {
                let field = Field::new(
                    "item",
                    to_arrow_datatype(info.element_type.as_ref().unwrap()),
                    info.contains_null,
                );
                ArrowDataType::List(Arc::new(field))
            }
            _ => unreachable!(),
        },
        DataTypeId::Map => match dt_value
            .type_info
            .as_ref()
            .unwrap()
            .datatype_struct
            .as_ref()
            .unwrap()
        {
            DatatypeStruct::Map(info) => {
                let key_field = Field::new(
                    "key",
                    to_arrow_datatype(info.key_type.as_ref().unwrap()),
                    false,
                );
                let value_field = Field::new(
                    "value",
                    to_arrow_datatype(info.value_type.as_ref().unwrap()),
                    info.value_contains_null,
                );
                let struct_field = Field::new(
                    "entries",
                    ArrowDataType::Struct(Fields::from(vec![key_field, value_field])),
                    false,
                );
                ArrowDataType::Map(Arc::new(struct_field), false)
            }
            _ => unreachable!(),
        },
        DataTypeId::Struct => match dt_value
            .type_info
            .as_ref()
            .unwrap()
            .datatype_struct
            .as_ref()
            .unwrap()
        {
            DatatypeStruct::Struct(info) => {
                let fields = info
                    .field_names
                    .iter()
                    .enumerate()
                    .map(|(idx, name)| {
                        Field::new(
                            name,
                            to_arrow_datatype(&info.field_datatypes[idx]),
                            info.field_nullable[idx],
                        )
                    })
                    .collect();
                ArrowDataType::Struct(fields)
            }
            _ => unreachable!(),
        },
    }
}
