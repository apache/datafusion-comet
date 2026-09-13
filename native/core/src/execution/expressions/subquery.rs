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

use crate::{
    errors::CometError,
    execution::utils::bytes_to_i128,
    jvm_bridge::{BinaryWrapper, JVMClasses, StringWrapper},
};
use arrow::array::{Array, ArrayRef, RecordBatch, StructArray};
use arrow::datatypes::{DataType, Schema, TimeUnit};
use arrow::ipc::reader::StreamReader;
use datafusion::common::{internal_err, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;
use jni::{
    objects::{JByteArray, JString},
    sys::{jboolean, jbyte, jint, jlong, jshort},
};
use std::{
    fmt::{Display, Formatter},
    hash::{Hash, Hasher},
    io::Cursor,
    sync::{Arc, OnceLock},
};

#[derive(Debug)]
pub struct Subquery {
    /// The ID of the execution context that owns this subquery. We use this ID to retrieve the
    /// subquery result.
    exec_context_id: i64,
    /// The ID of the subquery, we retrieve the subquery result from JVM using this ID.
    pub id: i64,
    /// The data type of the subquery result.
    pub data_type: DataType,
    // Spark materializes a scalar subquery before native execution. Cache the owned struct
    // result for this execution context so IPC serialization/decoding is not paid per batch.
    // Do not include this execution state in expression equality or hashing.
    struct_value: OnceLock<ScalarValue>,
}

impl Subquery {
    pub fn new(exec_context_id: i64, id: i64, data_type: DataType) -> Self {
        Self {
            exec_context_id,
            id,
            data_type,
            struct_value: OnceLock::new(),
        }
    }
}

impl PartialEq for Subquery {
    fn eq(&self, other: &Self) -> bool {
        self.exec_context_id == other.exec_context_id
            && self.id == other.id
            && self.data_type == other.data_type
    }
}

impl Eq for Subquery {}

impl Hash for Subquery {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.exec_context_id.hash(state);
        self.id.hash(state);
        self.data_type.hash(state);
    }
}

/// The JVM bridge emits one row with one struct column. Validate the wire shape and type before
/// creating the scalar; Arrow IPC validation also keeps malformed strings out of native arrays.
fn decode_struct_result(
    bytes: &[u8],
    data_type: &DataType,
) -> datafusion::common::Result<ScalarValue> {
    let mut reader = StreamReader::try_new(Cursor::new(bytes), None)?;
    let Some(batch) = reader.next().transpose()? else {
        return internal_err!("Scalar subquery IPC result contains no batch");
    };
    if batch.num_rows() != 1 || batch.num_columns() != 1 {
        return internal_err!("Scalar subquery IPC result must contain one row and one column");
    }
    if reader.next().transpose()?.is_some() {
        return internal_err!("Scalar subquery IPC result contains more than one batch");
    }
    let value = align_struct_metadata(batch.column(0), data_type)?;
    ScalarValue::try_from_array(&value, 0)
}

// Utils.toArrowSchema preserves field order, names, types and nullability but not Parquet field
// ID metadata. Restore only that metadata from the planned type, without permitting type casts.
fn align_struct_metadata(
    value: &ArrayRef,
    expected: &DataType,
) -> datafusion::common::Result<ArrayRef> {
    match (value.data_type(), expected) {
        (DataType::Struct(actual), DataType::Struct(fields))
            if actual.len() == fields.len()
                && actual
                    .iter()
                    .zip(fields.iter())
                    .all(|(a, b)| a.name() == b.name() && a.is_nullable() == b.is_nullable()) =>
        {
            let Some(value) = value.as_any().downcast_ref::<StructArray>() else {
                return internal_err!("Scalar subquery IPC result is not a struct array");
            };
            let children = value
                .columns()
                .iter()
                .zip(fields.iter())
                .map(|(child, field)| align_struct_metadata(child, field.data_type()))
                .collect::<datafusion::common::Result<Vec<_>>>()?;
            Ok(Arc::new(StructArray::try_new(
                fields.clone(),
                children,
                value.nulls().cloned(),
            )?))
        }
        (actual, expected) if actual == expected => Ok(Arc::clone(value)),
        (actual, expected) => {
            internal_err!("Scalar subquery IPC result has type {actual:?}, expected {expected:?}")
        }
    }
}

impl Display for Subquery {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Subquery [id: {}]", self.id)
    }
}

impl PhysicalExpr for Subquery {
    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }

    fn data_type(&self, _: &Schema) -> datafusion::common::Result<DataType> {
        Ok(self.data_type.clone())
    }

    fn nullable(&self, _: &Schema) -> datafusion::common::Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, _: &RecordBatch) -> datafusion::common::Result<ColumnarValue> {
        if let Some(value) = self.struct_value.get() {
            return Ok(ColumnarValue::Scalar(value.clone()));
        }
        let result = JVMClasses::with_env(|env| unsafe {
            let is_null = jni_static_call!(env,
                comet_exec.is_null(self.exec_context_id, self.id) -> jboolean
            )?;

            if is_null {
                return Ok(ColumnarValue::Scalar(ScalarValue::try_from(
                    &self.data_type,
                )?));
            }

            match &self.data_type {
                DataType::Struct(_) => {
                    let bytes = jni_static_call!(env,
                        comet_exec.get_struct(self.exec_context_id, self.id) -> BinaryWrapper
                    )?;
                    let bytes = JByteArray::from_raw(env, bytes.get().as_raw());
                    let bytes = env.convert_byte_array(bytes).map_err(CometError::from)?;
                    Ok(ColumnarValue::Scalar(decode_struct_result(
                        &bytes,
                        &self.data_type,
                    )?))
                }
                DataType::Boolean => {
                    let r = jni_static_call!(env,
                        comet_exec.get_bool(self.exec_context_id, self.id) -> jboolean
                    )?;
                    Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(r))))
                }
                DataType::Int8 => {
                    let r = jni_static_call!(env,
                        comet_exec.get_byte(self.exec_context_id, self.id) -> jbyte
                    )?;
                    Ok(ColumnarValue::Scalar(ScalarValue::Int8(Some(r))))
                }
                DataType::Int16 => {
                    let r = jni_static_call!(env,
                        comet_exec.get_short(self.exec_context_id, self.id) -> jshort
                    )?;
                    Ok(ColumnarValue::Scalar(ScalarValue::Int16(Some(r))))
                }
                DataType::Int32 => {
                    let r = jni_static_call!(env,
                        comet_exec.get_int(self.exec_context_id, self.id) -> jint
                    )?;
                    Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(r))))
                }
                DataType::Int64 => {
                    let r = jni_static_call!(env,
                        comet_exec.get_long(self.exec_context_id, self.id) -> jlong
                    )?;
                    Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(r))))
                }
                DataType::Float32 => {
                    let r = jni_static_call!(env,
                        comet_exec.get_float(self.exec_context_id, self.id) -> f32
                    )?;
                    Ok(ColumnarValue::Scalar(ScalarValue::Float32(Some(r))))
                }
                DataType::Float64 => {
                    let r = jni_static_call!(env,
                        comet_exec.get_double(self.exec_context_id, self.id) -> f64
                    )?;

                    Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(r))))
                }
                DataType::Decimal128(p, s) => {
                    let bytes = jni_static_call!(env,
                        comet_exec.get_decimal(self.exec_context_id, self.id) -> BinaryWrapper
                    )?;
                    let bytes = JByteArray::from_raw(env, bytes.get().as_raw());
                    let slice = env.convert_byte_array(bytes).unwrap();

                    Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                        Some(bytes_to_i128(&slice)),
                        *p,
                        *s,
                    )))
                }
                DataType::Date32 => {
                    let r = jni_static_call!(env,
                        comet_exec.get_int(self.exec_context_id, self.id) -> jint
                    )?;

                    Ok(ColumnarValue::Scalar(ScalarValue::Date32(Some(r))))
                }
                DataType::Timestamp(TimeUnit::Microsecond, timezone) => {
                    let r = jni_static_call!(env,
                        comet_exec.get_long(self.exec_context_id, self.id) -> jlong
                    )?;

                    Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                        Some(r),
                        timezone.clone(),
                    )))
                }
                DataType::Utf8 => {
                    let string = jni_static_call!(env,
                        comet_exec.get_string(self.exec_context_id, self.id) -> StringWrapper
                    )?;

                    let string = JString::from_raw(env, string.get().as_raw())
                        .try_to_string(env)
                        .unwrap();
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(string))))
                }
                DataType::Binary => {
                    let bytes = jni_static_call!(env,
                        comet_exec.get_binary(self.exec_context_id, self.id) -> BinaryWrapper
                    )?;
                    let bytes = JByteArray::from_raw(env, bytes.get().as_raw());
                    let slice = env.convert_byte_array(bytes).unwrap();

                    Ok(ColumnarValue::Scalar(ScalarValue::Binary(Some(slice))))
                }
                _ => internal_err!("Unsupported scalar subquery data type {:?}", self.data_type),
            }
        })?;
        if matches!(self.data_type, DataType::Struct(_)) {
            if let ColumnarValue::Scalar(value) = &result {
                // Concurrent first evaluations may both initialize the same immutable result.
                // Failed evaluations are never cached.
                let _ = self.struct_value.set(value.clone());
            }
        }
        Ok(result)
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::common::Result<Arc<dyn PhysicalExpr>> {
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{new_null_array, AsArray, Int32Array, StringArray},
        datatypes::Field,
        ipc::writer::StreamWriter,
    };
    use std::collections::hash_map::DefaultHasher;

    fn encode(schema: &Schema, batches: &[RecordBatch]) -> Vec<u8> {
        let mut bytes = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut bytes, schema).unwrap();
            for batch in batches {
                writer.write(batch).unwrap();
            }
            writer.finish().unwrap();
        }
        bytes
    }

    fn batch(value: ArrayRef) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            value.data_type().clone(),
            true,
        )]));
        RecordBatch::try_new(schema, vec![value]).unwrap()
    }

    fn struct_value() -> ArrayRef {
        Arc::new(StructArray::new(
            vec![
                Field::new("number", DataType::Int32, false),
                Field::new("text", DataType::Utf8, true),
            ]
            .into(),
            vec![
                Arc::new(Int32Array::from(vec![42])),
                Arc::new(StringArray::from(vec!["Comet 彗星"])),
            ],
            None,
        ))
    }

    #[test]
    fn struct_ipc_round_trip() {
        let value = struct_value();
        let batch = batch(Arc::clone(&value));
        let bytes = encode(batch.schema().as_ref(), std::slice::from_ref(&batch));
        assert_eq!(
            decode_struct_result(&bytes, value.data_type()).unwrap(),
            ScalarValue::try_from_array(&value, 0).unwrap()
        );
    }

    #[test]
    fn struct_ipc_distinguishes_null_struct_from_null_fields() {
        let fields = vec![Field::new("number", DataType::Int32, true)].into();
        let all_null_fields: ArrayRef = Arc::new(StructArray::new(
            fields,
            vec![new_null_array(&DataType::Int32, 1)],
            None,
        ));
        let null_struct = new_null_array(all_null_fields.data_type(), 1);
        for (value, expected_null) in [(all_null_fields, false), (null_struct, true)] {
            let batch = batch(Arc::clone(&value));
            let bytes = encode(batch.schema().as_ref(), std::slice::from_ref(&batch));
            let scalar = decode_struct_result(&bytes, value.data_type()).unwrap();
            assert_eq!(scalar.is_null(), expected_null);
            assert_eq!(scalar, ScalarValue::try_from_array(&value, 0).unwrap());
        }
    }

    #[test]
    fn struct_ipc_restores_nested_field_metadata() {
        let inner = struct_value();
        let outer: ArrayRef = Arc::new(StructArray::new(
            vec![Field::new("nested", inner.data_type().clone(), true)].into(),
            vec![inner],
            None,
        ));
        let with_id = |field: Field, id: &str| {
            field.with_metadata([("PARQUET:field_id".to_owned(), id.to_owned())].into())
        };
        let expected = DataType::Struct(
            vec![with_id(
                Field::new(
                    "nested",
                    DataType::Struct(
                        vec![
                            with_id(Field::new("number", DataType::Int32, false), "2"),
                            Field::new("text", DataType::Utf8, true),
                        ]
                        .into(),
                    ),
                    true,
                ),
                "1",
            )]
            .into(),
        );
        let batch = batch(Arc::clone(&outer));
        let bytes = encode(batch.schema().as_ref(), std::slice::from_ref(&batch));
        let scalar = decode_struct_result(&bytes, &expected).unwrap();
        assert_eq!(scalar.data_type(), expected);
        let ScalarValue::Struct(result) = scalar else {
            panic!("Expected struct scalar");
        };
        let nested = result
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(
            nested.column(0),
            outer.as_struct().column(0).as_struct().column(0)
        );
    }

    #[test]
    fn struct_ipc_rejects_invalid_shape_and_type() {
        let batch = batch(struct_value());
        let schema = batch.schema();
        let data_type = batch.column(0).data_type();
        assert!(decode_struct_result(b"invalid IPC", data_type).is_err());
        assert!(decode_struct_result(&encode(&schema, &[]), data_type).is_err());
        assert!(
            decode_struct_result(&encode(&schema, &[batch.clone(), batch.clone()]), data_type)
                .is_err()
        );
        assert!(decode_struct_result(&encode(&schema, &[batch.slice(0, 0)]), data_type).is_err());
        let bytes = encode(&schema, std::slice::from_ref(&batch));
        let wrong_type = DataType::Struct(
            vec![
                Field::new("number", DataType::Int64, false),
                Field::new("text", DataType::Utf8, true),
            ]
            .into(),
        );
        assert!(decode_struct_result(&bytes, &wrong_type).is_err());
    }

    #[test]
    fn struct_cache_does_not_change_expression_identity() {
        let value = ScalarValue::try_from_array(&struct_value(), 0).unwrap();
        let cached = Subquery::new(1, 2, value.data_type());
        let same = Subquery::new(1, 2, value.data_type());
        let other_context = Subquery::new(3, 2, value.data_type());
        let hash = |expr: &Subquery| {
            let mut hasher = DefaultHasher::new();
            expr.hash(&mut hasher);
            hasher.finish()
        };
        let before = hash(&cached);
        cached.struct_value.set(value.clone()).unwrap();
        assert_eq!(cached, same);
        assert_eq!(hash(&cached), before);
        assert_ne!(cached, other_context);
        // A cached result is owned by the expression and needs no live JVM registry entry.
        let input = RecordBatch::new_empty(Arc::new(Schema::empty()));
        let ColumnarValue::Scalar(result) = cached.evaluate(&input).unwrap() else {
            panic!("Expected scalar result");
        };
        assert_eq!(result, value);
    }
}
