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
use arrow::datatypes::{DataType as ArrowDataType, IntervalUnit, TimeUnit};
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
use parquet::{arrow::PARQUET_FIELD_ID_META_KEY, variant::VariantType};
use prost::Message;
use std::{io::Cursor, sync::Arc};

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
        DataTypeId::Time => ArrowDataType::Time64(TimeUnit::Nanosecond),
        // Spark's YearMonthIntervalType maps to Arrow Interval(YearMonth) (int32 months).
        DataTypeId::YearMonthInterval => ArrowDataType::Interval(IntervalUnit::YearMonth),
        // Spark's DayTimeIntervalType stores microseconds in an int64, which matches Arrow
        // Duration(Microsecond) rather than the lossy Interval(DayTime) {days, millis} layout.
        DataTypeId::DayTimeInterval => ArrowDataType::Duration(TimeUnit::Microsecond),
        // Spark's CalendarIntervalType stores months, days, and microseconds. Arrow stores the
        // same components with nanosecond precision.
        DataTypeId::CalendarInterval => ArrowDataType::Interval(IntervalUnit::MonthDayNano),
        DataTypeId::Variant => ArrowDataType::Struct(Fields::from(vec![
            Field::new("value", ArrowDataType::Binary, false),
            Field::new("metadata", ArrowDataType::Binary, false),
        ])),
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
                let field = with_parquet_field_id(
                    to_arrow_field(
                        "item",
                        info.element_type.as_ref().unwrap(),
                        info.contains_null,
                    ),
                    info.element_field_id,
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
                let key_field = with_parquet_field_id(
                    to_arrow_field("key", info.key_type.as_ref().unwrap(), false),
                    info.key_field_id,
                );
                let value_field = with_parquet_field_id(
                    to_arrow_field(
                        "value",
                        info.value_type.as_ref().unwrap(),
                        info.value_contains_null,
                    ),
                    info.value_field_id,
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
                        let field = to_arrow_field(
                            name,
                            &info.field_datatypes[idx],
                            info.field_nullable[idx],
                        );
                        // Attach Spark field metadata (currently parquet.field.id) when present.
                        // field_metadata is parallel to field_names; either empty or full length.
                        if let Some(meta) = info.field_metadata.get(idx) {
                            if !meta.metadata.is_empty() {
                                let mut metadata = meta.metadata.clone();
                                metadata.extend(field.metadata().clone());
                                return field.with_metadata(metadata);
                            }
                        }
                        field
                    })
                    .collect();
                ArrowDataType::Struct(fields)
            }
            _ => unreachable!(),
        },
    }
}

/// Converts a protobuf type to an Arrow field, preserving logical extension identity.
pub(crate) fn to_arrow_field(
    name: impl Into<std::string::String>,
    data_type: &DataType,
    nullable: bool,
) -> Field {
    let field = Field::new(name, to_arrow_datatype(data_type), nullable);
    if DataTypeId::try_from(data_type.type_id).unwrap() == DataTypeId::Variant {
        field.with_extension_type(VariantType)
    } else {
        field
    }
}

/// Attach a Parquet field ID without changing synthetic fields when Catalyst did not supply one.
fn with_parquet_field_id(field: Field, field_id: Option<i32>) -> Field {
    match field_id {
        Some(id) => {
            let mut metadata = field.metadata().clone();
            metadata.insert(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string());
            field.with_metadata(metadata)
        }
        None => field,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_comet_proto::spark_expression::data_type::{DataTypeInfo, ListInfo, MapInfo};

    fn primitive_type(type_id: DataTypeId) -> DataType {
        DataType {
            type_id: type_id as i32,
            type_info: None,
        }
    }

    fn list_type(element_type: DataType, element_field_id: Option<i32>) -> DataType {
        DataType {
            type_id: DataTypeId::List as i32,
            type_info: Some(Box::new(DataTypeInfo {
                datatype_struct: Some(DatatypeStruct::List(Box::new(ListInfo {
                    element_type: Some(Box::new(element_type)),
                    contains_null: true,
                    element_field_id,
                }))),
            })),
        }
    }

    fn map_type(key_field_id: Option<i32>, value_field_id: Option<i32>) -> DataType {
        DataType {
            type_id: DataTypeId::Map as i32,
            type_info: Some(Box::new(DataTypeInfo {
                datatype_struct: Some(DatatypeStruct::Map(Box::new(MapInfo {
                    key_type: Some(Box::new(primitive_type(DataTypeId::Int32))),
                    value_type: Some(Box::new(primitive_type(DataTypeId::String))),
                    value_contains_null: true,
                    key_field_id,
                    value_field_id,
                }))),
            })),
        }
    }

    #[test]
    fn list_element_field_id_preserves_zero_and_absence() {
        let ArrowDataType::List(field) =
            to_arrow_datatype(&list_type(primitive_type(DataTypeId::Int32), Some(0)))
        else {
            panic!("expected a list data type");
        };

        assert_eq!(field.name(), "item");
        assert!(field.is_nullable());
        assert_eq!(
            field.metadata().get(PARQUET_FIELD_ID_META_KEY),
            Some(&"0".to_string())
        );

        let ArrowDataType::List(field_without_id) =
            to_arrow_datatype(&list_type(primitive_type(DataTypeId::Int32), None))
        else {
            panic!("expected a list data type");
        };
        assert!(field_without_id.metadata().is_empty());
    }

    #[test]
    fn map_key_and_value_field_ids_preserve_nullability() {
        let ArrowDataType::Map(entries, _) = to_arrow_datatype(&map_type(Some(0), Some(23))) else {
            panic!("expected a map data type");
        };
        let ArrowDataType::Struct(fields) = entries.data_type() else {
            panic!("expected map entries to be a struct");
        };

        assert_eq!(fields[0].name(), "key");
        assert!(!fields[0].is_nullable());
        assert_eq!(
            fields[0].metadata().get(PARQUET_FIELD_ID_META_KEY),
            Some(&"0".to_string())
        );
        assert_eq!(fields[1].name(), "value");
        assert!(fields[1].is_nullable());
        assert_eq!(
            fields[1].metadata().get(PARQUET_FIELD_ID_META_KEY),
            Some(&"23".to_string())
        );

        let ArrowDataType::Map(entries_without_ids, _) = to_arrow_datatype(&map_type(None, None))
        else {
            panic!("expected a map data type");
        };
        let ArrowDataType::Struct(fields_without_ids) = entries_without_ids.data_type() else {
            panic!("expected map entries to be a struct");
        };
        assert!(fields_without_ids[0].metadata().is_empty());
        assert!(fields_without_ids[1].metadata().is_empty());
    }

    #[test]
    fn synthetic_field_ids_are_preserved_at_multiple_nesting_levels() {
        let ArrowDataType::List(element) =
            to_arrow_datatype(&list_type(map_type(Some(12), Some(13)), Some(11)))
        else {
            panic!("expected a list data type");
        };
        assert_eq!(
            element.metadata().get(PARQUET_FIELD_ID_META_KEY),
            Some(&"11".to_string())
        );

        let ArrowDataType::Map(entries, _) = element.data_type() else {
            panic!("expected a nested map data type");
        };
        let ArrowDataType::Struct(fields) = entries.data_type() else {
            panic!("expected map entries to be a struct");
        };
        assert_eq!(
            fields[0].metadata().get(PARQUET_FIELD_ID_META_KEY),
            Some(&"12".to_string())
        );
        assert_eq!(
            fields[1].metadata().get(PARQUET_FIELD_ID_META_KEY),
            Some(&"13".to_string())
        );
    }

    #[test]
    fn variant_field_preserves_storage_and_extension_identity() {
        let variant = primitive_type(DataTypeId::Variant);
        let field = to_arrow_field("v", &variant, true);

        assert_eq!(field.name(), "v");
        assert!(field.is_nullable());
        assert_eq!(field.extension_type_name(), Some("arrow.parquet.variant"));
        assert!(field.has_valid_extension_type::<VariantType>());
        let ArrowDataType::Struct(fields) = field.data_type() else {
            panic!("expected Variant to use Struct storage");
        };
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name(), "value");
        assert_eq!(fields[0].data_type(), &ArrowDataType::Binary);
        assert!(!fields[0].is_nullable());
        assert_eq!(fields[1].name(), "metadata");
        assert_eq!(fields[1].data_type(), &ArrowDataType::Binary);
        assert!(!fields[1].is_nullable());

        let ArrowDataType::List(element) = to_arrow_datatype(&list_type(variant, Some(7))) else {
            panic!("expected a list data type");
        };
        assert!(element.has_valid_extension_type::<VariantType>());
        assert_eq!(
            element.metadata().get(PARQUET_FIELD_ID_META_KEY),
            Some(&"7".to_string())
        );
    }
}
