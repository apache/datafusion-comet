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

use std::any::Any;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{make_array, ArrayRef};
use arrow::datatypes::{DataType, Schema};
use arrow::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use arrow::record_batch::RecordBatch;

use datafusion::common::Result as DFResult;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;

use datafusion_comet_jni_bridge::errors::{CometError, ExecutionError};
use datafusion_comet_jni_bridge::JVMClasses;
use jni::objects::{JObject, JValue};

/// A scalar expression that delegates evaluation to a JVM-side `CometUDF` via JNI.
/// The JVM class named by `class_name` must implement `org.apache.comet.udf.CometUDF`.
#[derive(Debug)]
pub struct JvmScalarUdfExpr {
    class_name: String,
    args: Vec<Arc<dyn PhysicalExpr>>,
    return_type: DataType,
}

impl JvmScalarUdfExpr {
    pub fn new(
        class_name: String,
        args: Vec<Arc<dyn PhysicalExpr>>,
        return_type: DataType,
    ) -> Self {
        Self {
            class_name,
            args,
            return_type,
        }
    }
}

impl Display for JvmScalarUdfExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "JvmScalarUdf({}, args={:?})", self.class_name, self.args)
    }
}

impl Hash for JvmScalarUdfExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.class_name.hash(state);
        for a in &self.args {
            format!("{}", a).hash(state);
        }
        self.return_type.hash(state);
    }
}

impl PartialEq for JvmScalarUdfExpr {
    fn eq(&self, other: &Self) -> bool {
        self.class_name == other.class_name
            && self.return_type == other.return_type
            && self.args.len() == other.args.len()
            && self
                .args
                .iter()
                .zip(other.args.iter())
                .all(|(a, b)| format!("{}", a) == format!("{}", b))
    }
}

impl Eq for JvmScalarUdfExpr {}

impl PhysicalExpr for JvmScalarUdfExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }

    fn data_type(&self, _input_schema: &Schema) -> DFResult<DataType> {
        Ok(self.return_type.clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> DFResult<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> DFResult<ColumnarValue> {
        // Step 1: evaluate child expressions to get Arrow arrays.
        let n = batch.num_rows();
        let arrays: Vec<ArrayRef> = self
            .args
            .iter()
            .map(|e| e.evaluate(batch).and_then(|cv| cv.into_array(n)))
            .collect::<DFResult<_>>()?;

        // Step 2: allocate FFI structs on the Rust heap and collect their raw pointers.
        // The JVM writes into the out_array/out_schema slots and reads from the in_ slots.
        let in_ffi_arrays: Vec<Box<FFI_ArrowArray>> = arrays
            .iter()
            .map(|arr| Box::new(FFI_ArrowArray::new(&arr.to_data())))
            .collect();
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

        // Allocate output FFI slots.
        let mut out_array = Box::new(FFI_ArrowArray::empty());
        let mut out_schema = Box::new(FFI_ArrowSchema::empty());
        let out_arr_ptr = out_array.as_mut() as *mut FFI_ArrowArray as i64;
        let out_sch_ptr = out_schema.as_mut() as *mut FFI_ArrowSchema as i64;

        let class_name = self.class_name.clone();
        let n_args = arrays.len();

        // Step 3: attach a JNI env for this thread and call the static bridge method.
        JVMClasses::with_env(|env| {
            let bridge = JVMClasses::get().comet_udf_bridge.as_ref().ok_or_else(|| {
                CometError::from(ExecutionError::GeneralError(
                    "JVM UDF bridge unavailable: org.apache.comet.udf.CometUdfBridge \
                     class was not found on the JVM classpath. Set \
                     spark.comet.exec.regexp.engine=rust to disable this path."
                        .to_string(),
                ))
            })?;

            // Build the JVM String for the class name.
            let jclass_name = env
                .new_string(&class_name)
                .map_err(|e| CometError::JNI { source: e })?;

            // Build the long[] arrays for input pointers.
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

            // Call CometUdfBridge.evaluate(String, long[], long[], long, long)
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
                    ],
                )
            };

            if let Some(exception) = datafusion_comet_jni_bridge::check_exception(env)? {
                return Err(exception);
            }

            ret.map_err(|e| CometError::JNI { source: e })?;
            Ok(())
        })?;

        // Step 4: import the result from the FFI slots filled by the JVM.
        // SAFETY: `*out_array` moves the FFI_ArrowArray out of the Box (the heap
        // allocation is freed by the move), and `from_ffi` wraps it in an Arc that
        // keeps the JVM-installed release callback alive until the resulting
        // ArrayData drops. `out_schema` is borrowed; its release callback runs
        // exactly once when the Box drops at end of scope.
        let result_data = unsafe { from_ffi(*out_array, &out_schema) }
            .map_err(|e| CometError::Arrow { source: e })?;
        Ok(ColumnarValue::Array(make_array(result_data)))
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
        )))
    }
}
