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
    execution::utils::bytes_to_i128,
    jvm_bridge::{jni_static_call, BinaryWrapper, JVMClasses, StringWrapper},
};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Schema, TimeUnit};
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::{internal_err, ScalarValue};
use datafusion_physical_expr::PhysicalExpr;
use jni::{
    objects::JByteArray,
    sys::{jboolean, jbyte, jint, jlong, jshort},
};
use std::{
    any::Any,
    fmt::{Display, Formatter},
    hash::Hash,
    sync::Arc,
};

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct Subquery {
    /// The ID of the execution context that owns this subquery. We use this ID to retrieve the
    /// subquery result.
    exec_context_id: i64,
    /// The ID of the subquery, we retrieve the subquery result from JVM using this ID.
    pub id: i64,
    /// The data type of the subquery result.
    pub data_type: DataType,
}

impl Subquery {
    pub fn new(exec_context_id: i64, id: i64, data_type: DataType) -> Self {
        Self {
            exec_context_id,
            id,
            data_type,
        }
    }
}

impl Display for Subquery {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Subquery [id: {}]", self.id)
    }
}

impl PhysicalExpr for Subquery {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _: &Schema) -> datafusion_common::Result<DataType> {
        Ok(self.data_type.clone())
    }

    fn nullable(&self, _: &Schema) -> datafusion_common::Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, _: &RecordBatch) -> datafusion_common::Result<ColumnarValue> {
        let mut env = JVMClasses::get_env()?;

        unsafe {
            let is_null = jni_static_call!(&mut env,
                comet_exec.is_null(self.exec_context_id, self.id) -> jboolean
            )?;

            if is_null > 0 {
                return Ok(ColumnarValue::Scalar(ScalarValue::try_from(
                    &self.data_type,
                )?));
            }

            match &self.data_type {
                DataType::Boolean => {
                    let r = jni_static_call!(&mut env,
                        comet_exec.get_bool(self.exec_context_id, self.id) -> jboolean
                    )?;
                    Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(r > 0))))
                }
                DataType::Int8 => {
                    let r = jni_static_call!(&mut env,
                        comet_exec.get_byte(self.exec_context_id, self.id) -> jbyte
                    )?;
                    Ok(ColumnarValue::Scalar(ScalarValue::Int8(Some(r))))
                }
                DataType::Int16 => {
                    let r = jni_static_call!(&mut env,
                        comet_exec.get_short(self.exec_context_id, self.id) -> jshort
                    )?;
                    Ok(ColumnarValue::Scalar(ScalarValue::Int16(Some(r))))
                }
                DataType::Int32 => {
                    let r = jni_static_call!(&mut env,
                        comet_exec.get_int(self.exec_context_id, self.id) -> jint
                    )?;
                    Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(r))))
                }
                DataType::Int64 => {
                    let r = jni_static_call!(&mut env,
                        comet_exec.get_long(self.exec_context_id, self.id) -> jlong
                    )?;
                    Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(r))))
                }
                DataType::Float32 => {
                    let r = jni_static_call!(&mut env,
                        comet_exec.get_float(self.exec_context_id, self.id) -> f32
                    )?;
                    Ok(ColumnarValue::Scalar(ScalarValue::Float32(Some(r))))
                }
                DataType::Float64 => {
                    let r = jni_static_call!(&mut env,
                        comet_exec.get_double(self.exec_context_id, self.id) -> f64
                    )?;

                    Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(r))))
                }
                DataType::Decimal128(p, s) => {
                    let bytes = jni_static_call!(&mut env,
                        comet_exec.get_decimal(self.exec_context_id, self.id) -> BinaryWrapper
                    )?;
                    let bytes: &JByteArray = bytes.get().into();
                    let slice = env.convert_byte_array(bytes).unwrap();

                    Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                        Some(bytes_to_i128(&slice)),
                        *p,
                        *s,
                    )))
                }
                DataType::Date32 => {
                    let r = jni_static_call!(&mut env,
                        comet_exec.get_int(self.exec_context_id, self.id) -> jint
                    )?;

                    Ok(ColumnarValue::Scalar(ScalarValue::Date32(Some(r))))
                }
                DataType::Timestamp(TimeUnit::Microsecond, timezone) => {
                    let r = jni_static_call!(&mut env,
                        comet_exec.get_long(self.exec_context_id, self.id) -> jlong
                    )?;

                    Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                        Some(r),
                        timezone.clone(),
                    )))
                }
                DataType::Utf8 => {
                    let string = jni_static_call!(&mut env,
                        comet_exec.get_string(self.exec_context_id, self.id) -> StringWrapper
                    )?;

                    let string = env.get_string(string.get()).unwrap().into();
                    Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(string))))
                }
                DataType::Binary => {
                    let bytes = jni_static_call!(&mut env,
                        comet_exec.get_binary(self.exec_context_id, self.id) -> BinaryWrapper
                    )?;
                    let bytes: &JByteArray = bytes.get().into();
                    let slice = env.convert_byte_array(bytes).unwrap();

                    Ok(ColumnarValue::Scalar(ScalarValue::Binary(Some(slice))))
                }
                _ => internal_err!("Unsupported scalar subquery data type {:?}", self.data_type),
            }
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion_common::Result<Arc<dyn PhysicalExpr>> {
        Ok(self)
    }
}
