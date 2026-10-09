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

use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{make_array, ArrayRef};
use arrow::datatypes::{DataType, Schema};
use arrow::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use arrow::record_batch::RecordBatch;

use datafusion::common::Result as DFResult;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::expressions::Literal;
use datafusion::physical_expr::PhysicalExpr;

use datafusion_comet_common::{decode_string_arrays, zero_offsets};
use datafusion_comet_jni_bridge::errors::{CometError, ExecutionError};
use datafusion_comet_jni_bridge::JVMClasses;
use jni::objects::{Global, JObject, JValue};

/// A scalar expression that delegates evaluation to a JVM-side `CometUDF` via JNI.
/// The JVM class named by `class_name` must implement `org.apache.comet.udf.CometUDF`.
#[derive(Debug)]
pub struct JvmScalarUdfExpr {
    class_name: String,
    args: Vec<Arc<dyn PhysicalExpr>>,
    /// The length-1 array sent for each argument that is a literal. Built once here, because
    /// building it for every batch copies the value twice: `Literal::evaluate` clones it and
    /// `to_array_of_size` copies the clone. The codegen dispatcher's serialized expression is
    /// such a literal, several KB for a Scala UDF. `None` for any other argument, and for a
    /// literal whose array cannot be built, which `evaluate` then reports.
    literal_arrays: Vec<Option<ArrayRef>>,
    return_type: DataType,
    return_nullable: bool,
    /// Captured at `createPlan` time and threaded here by the planner. Passed through the
    /// JNI bridge so `CometUdfBridge.evaluate` can install it as the Tokio worker's
    /// thread-local `TaskContext`. Without this, partition-sensitive built-ins inside a UDF
    /// tree (`Rand`, `Uuid`, `MonotonicallyIncreasingID`, user code reading
    /// `TaskContext.get()`) see `null` and seed / branch incorrectly. `None` when no driving
    /// Spark task is available; the bridge then leaves whatever `TaskContext.get()` already
    /// returns in place.
    task_context: Option<Arc<Global<JObject<'static>>>>,
    /// Context `ClassLoader` of the driving Spark task thread, captured at `createPlan` time and
    /// threaded here by the planner. See `CometUdfBridge.evaluate`, which installs it for the
    /// duration of the call. `None` when no driving Spark task is available (unit tests, direct
    /// native driver runs); the bridge then installs nothing.
    class_loader: Option<Arc<Global<JObject<'static>>>>,
    /// Index of the partition this native plan computes. See `CometUDF.evaluate`.
    partition: i32,
    /// Id of this native plan. See `CometUDF.evaluate`.
    exec_context_id: i64,
}

impl JvmScalarUdfExpr {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        class_name: String,
        args: Vec<Arc<dyn PhysicalExpr>>,
        return_type: DataType,
        return_nullable: bool,
        task_context: Option<Arc<Global<JObject<'static>>>>,
        class_loader: Option<Arc<Global<JObject<'static>>>>,
        partition: i32,
        exec_context_id: i64,
    ) -> Self {
        debug_assert!(
            !class_name.is_empty(),
            "JvmScalarUdfExpr requires a non-empty class name"
        );
        let literal_arrays = args
            .iter()
            .map(|arg| {
                arg.downcast_ref::<Literal>()
                    .and_then(|literal| literal.value().to_array_of_size(1).ok())
            })
            .collect();
        Self {
            class_name,
            args,
            literal_arrays,
            return_type,
            return_nullable,
            task_context,
            class_loader,
            partition,
            exec_context_id,
        }
    }
}

impl Display for JvmScalarUdfExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "JvmScalarUdf({}", self.class_name)?;
        for a in &self.args {
            write!(f, ", {a}")?;
        }
        write!(f, ")")
    }
}

impl Hash for JvmScalarUdfExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.class_name.hash(state);
        for a in &self.args {
            a.hash(state);
        }
        self.return_type.hash(state);
        self.return_nullable.hash(state);
    }
}

impl PartialEq for JvmScalarUdfExpr {
    fn eq(&self, other: &Self) -> bool {
        self.class_name == other.class_name
            && self.return_type == other.return_type
            && self.return_nullable == other.return_nullable
            && self.args.len() == other.args.len()
            && self.args.iter().zip(&other.args).all(|(a, b)| a.eq(b))
    }
}

impl Eq for JvmScalarUdfExpr {}

impl PhysicalExpr for JvmScalarUdfExpr {
    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }

    fn data_type(&self, _input_schema: &Schema) -> DFResult<DataType> {
        Ok(self.return_type.clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> DFResult<bool> {
        Ok(self.return_nullable)
    }

    fn evaluate(&self, batch: &RecordBatch) -> DFResult<ColumnarValue> {
        // Scalar children (e.g. literal patterns) are sent as length-1 vectors rather than
        // expanded to batch-row count, so the JVM bridge does not pay an O(rows) copy for
        // values that never vary across the batch. The JVM side gets `numRows` directly via
        // the bridge so it doesn't need the scalar to carry batch length.
        let arrays: Vec<ArrayRef> = self
            .args
            .iter()
            .zip(&self.literal_arrays)
            .map(|(e, literal_array)| match literal_array {
                Some(a) => Ok(Arc::clone(a)),
                None => match e.evaluate(batch)? {
                    ColumnarValue::Array(a) => Ok(a),
                    ColumnarValue::Scalar(s) => s.to_array_of_size(1),
                },
            })
            .collect::<DFResult<_>>()?;

        // The JVM writes into the out_array/out_schema slots and reads from the in_ slots.
        // Arrow Java ignores `ArrowArray.offset` on import, so every level has to start at 0.
        let in_ffi_arrays: Vec<Box<FFI_ArrowArray>> = arrays
            .iter()
            .map(|arr| {
                let data = arr.to_data();
                let data = zero_offsets(&data).map_err(|e| CometError::Arrow { source: e })?;
                Ok(Box::new(FFI_ArrowArray::new(&data)))
            })
            .collect::<Result<_, CometError>>()?;
        let in_ffi_schemas: Vec<Box<FFI_ArrowSchema>> = arrays
            .iter()
            .map(|arr| {
                FFI_ArrowSchema::try_from(arr.data_type())
                    .map(Box::new)
                    .map_err(|e| CometError::Arrow { source: e })
            })
            .collect::<Result<_, CometError>>()?;

        let in_arr_ptrs: Vec<i64> = in_ffi_arrays
            .iter()
            .map(|b| b.as_ref() as *const FFI_ArrowArray as i64)
            .collect();
        let in_sch_ptrs: Vec<i64> = in_ffi_schemas
            .iter()
            .map(|b| b.as_ref() as *const FFI_ArrowSchema as i64)
            .collect();

        debug_assert!(!self.class_name.is_empty(), "class_name must not be empty");
        debug_assert_eq!(
            in_arr_ptrs.len(),
            in_sch_ptrs.len(),
            "input array and schema pointer counts must match"
        );

        let mut out_array = Box::new(FFI_ArrowArray::empty());
        let mut out_schema = Box::new(FFI_ArrowSchema::empty());
        let out_arr_ptr = out_array.as_mut() as *mut FFI_ArrowArray as i64;
        let out_sch_ptr = out_schema.as_mut() as *mut FFI_ArrowSchema as i64;

        let class_name = self.class_name.clone();
        let n_args = arrays.len();

        JVMClasses::with_env(|env| {
            let bridge = JVMClasses::get().comet_udf_bridge.as_ref().ok_or_else(|| {
                CometError::from(ExecutionError::GeneralError(
                    "JVM UDF bridge unavailable: org.apache.comet.udf.CometUdfBridge \
                     class was not found on the JVM classpath."
                        .to_string(),
                ))
            })?;

            let jclass_name = env
                .new_string(&class_name)
                .map_err(|e| CometError::JNI { source: e })?;

            let in_arr_java = env
                .new_long_array(n_args)
                .map_err(|e| CometError::JNI { source: e })?;
            in_arr_java
                .set_region(env, 0, &in_arr_ptrs)
                .map_err(|e| CometError::JNI { source: e })?;

            let in_sch_java = env
                .new_long_array(n_args)
                .map_err(|e| CometError::JNI { source: e })?;
            in_sch_java
                .set_region(env, 0, &in_sch_ptrs)
                .map_err(|e| CometError::JNI { source: e })?;

            // Resolve the TaskContext and ClassLoader references once before building the arg
            // array so the borrows live until `call_static_method_unchecked` returns. Absent
            // values are passed as a null object, which the bridge's null-guards skip.
            let null_obj = JObject::null();
            let task_context_ref: &JObject = match &self.task_context {
                Some(gref) => gref.as_obj(),
                None => &null_obj,
            };
            let class_loader_ref: &JObject = match &self.class_loader {
                Some(gref) => gref.as_obj(),
                None => &null_obj,
            };
            let ret = unsafe {
                env.call_static_method_unchecked(
                    &bridge.class,
                    bridge.method_evaluate,
                    bridge.method_evaluate_ret,
                    &[
                        JValue::from(&jclass_name).as_jni(),
                        JValue::Object(JObject::from(in_arr_java).as_ref()).as_jni(),
                        JValue::Object(JObject::from(in_sch_java).as_ref()).as_jni(),
                        JValue::Long(out_arr_ptr).as_jni(),
                        JValue::Long(out_sch_ptr).as_jni(),
                        JValue::Int(batch.num_rows() as i32).as_jni(),
                        JValue::Int(self.partition).as_jni(),
                        JValue::Long(self.exec_context_id).as_jni(),
                        JValue::Object(task_context_ref).as_jni(),
                        JValue::Object(class_loader_ref).as_jni(),
                    ],
                )
            };

            if let Some(exception) = datafusion_comet_jni_bridge::check_exception(env)? {
                return Err(exception);
            }

            ret.map_err(|e| CometError::JNI { source: e })?;
            Ok(())
        })?;

        // SAFETY: `*out_array` moves the FFI_ArrowArray out of the Box (the heap
        // allocation is freed by the move), and `from_ffi` wraps it in an Arc that
        // keeps the JVM-installed release callback alive until the resulting
        // ArrayData drops. `out_schema` is borrowed; its release callback runs
        // exactly once when the Box drops at end of scope.
        let result_data = unsafe { from_ffi(*out_array, &out_schema) }
            .map_err(|e| CometError::Arrow { source: e })?;
        let imported = make_array(result_data);
        let decoded =
            decode_string_arrays(&imported).map_err(|e| CometError::Arrow { source: e })?;
        Ok(ColumnarValue::Array(decoded))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.args.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> DFResult<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(JvmScalarUdfExpr::new(
            self.class_name.clone(),
            children,
            self.return_type.clone(),
            self.return_nullable,
            self.task_context.clone(),
            self.class_loader.clone(),
            self.partition,
            self.exec_context_id,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, BinaryArray};
    use datafusion::common::ScalarValue;
    use datafusion::physical_expr::expressions::Column;

    #[test]
    fn builds_literal_arguments_once() {
        let bytes = vec![7u8; 4096];
        let udf = JvmScalarUdfExpr::new(
            "org.example.Udf".to_string(),
            vec![
                Arc::new(Literal::new(ScalarValue::Binary(Some(bytes.clone())))),
                Arc::new(Column::new("a", 0)),
            ],
            DataType::Int64,
            true,
            None,
            None,
            0,
            0,
        );
        let literal = udf.literal_arrays[0]
            .as_ref()
            .expect("a literal argument's array is built up front");
        let literal = literal.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(literal.len(), 1);
        assert_eq!(literal.value(0), bytes.as_slice());
        assert!(udf.literal_arrays[1].is_none());

        // New children rebuild the arrays, so a position that no longer holds a literal does not
        // keep sending the old literal's array.
        let swapped = Arc::new(udf)
            .with_new_children(vec![
                Arc::new(Column::new("a", 0)),
                Arc::new(Literal::new(ScalarValue::Binary(Some(bytes)))),
            ])
            .unwrap();
        let swapped = swapped.downcast_ref::<JvmScalarUdfExpr>().unwrap();
        assert!(swapped.literal_arrays[0].is_none());
        assert!(swapped.literal_arrays[1].is_some());
    }
}
