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

//! Define JNI APIs which can be called from Java/Scala.

use crate::execution::operators::{InputBatch, ScanExec};
use arrow::{
    array::{make_array, Array, ArrayData, ArrayRef},
    datatypes::DataType as ArrowDataType,
    ffi::{FFI_ArrowArray, FFI_ArrowSchema},
};
use arrow_array::RecordBatch;
use datafusion::{
    execution::{
        disk_manager::DiskManagerConfig,
        runtime_env::{RuntimeConfig, RuntimeEnv},
    },
    physical_plan::{display::DisplayableExecutionPlan, ExecutionPlan, SendableRecordBatchStream},
    prelude::{SessionConfig, SessionContext},
};
use datafusion_common::DataFusionError;
use futures::poll;
use jni::{
    errors::Result as JNIResult,
    objects::{JClass, JMap, JObject, JString, ReleaseMode},
    sys::{jbyteArray, jint, jlong, jlongArray},
    JNIEnv,
};
use std::{collections::HashMap, sync::Arc, task::Poll};

use super::{serde, utils::SparkArrowConvert};

use crate::{
    errors::{try_unwrap_or_throw, CometError},
    execution::{
        datafusion::planner::PhysicalPlanner, metrics::utils::update_comet_metric,
        serde::to_arrow_datatype, spark_operator::Operator,
    },
    jvm_bridge::{jni_new_global_ref, JVMClasses},
};
use futures::stream::StreamExt;
use jni::{
    objects::{AutoArray, GlobalRef},
    sys::{jbooleanArray, jobjectArray},
};
use tokio::runtime::Runtime;

use log::info;

/// Comet native execution context. Kept alive across JNI calls.
struct ExecutionContext {
    /// The id of the execution context.
    pub id: i64,
    /// The deserialized Spark plan
    pub spark_plan: Operator,
    /// The DataFusion root operator converted from the `spark_plan`
    pub root_op: Option<Arc<dyn ExecutionPlan>>,
    /// The input sources for the DataFusion plan
    pub scans: Vec<ScanExec>,
    /// The record batch stream to pull results from
    pub stream: Option<SendableRecordBatchStream>,
    /// The FFI arrays. We need to keep them alive here.
    pub ffi_arrays: Vec<(Arc<FFI_ArrowArray>, Arc<FFI_ArrowSchema>)>,
    /// Configurations for DF execution
    pub conf: HashMap<String, String>,
    /// The Tokio runtime used for async.
    pub runtime: Runtime,
    /// Native metrics
    pub metrics: Arc<GlobalRef>,
    /// DataFusion SessionContext
    pub session_ctx: Arc<SessionContext>,
    /// Whether to enable additional debugging checks & messages
    pub debug_native: bool,
}

#[no_mangle]
/// Accept serialized query plan and return the address of the native query plan.
pub extern "system" fn Java_org_apache_comet_Native_createPlan(
    env: JNIEnv,
    _class: JClass,
    id: jlong,
    config_object: JObject,
    serialized_query: jbyteArray,
    metrics_node: JObject,
) -> jlong {
    try_unwrap_or_throw(env, || {
        // Init JVM classes
        JVMClasses::init(&env);

        let bytes = env.convert_byte_array(serialized_query)?;

        // Deserialize query plan
        let spark_plan = serde::deserialize_op(bytes.as_slice())?;

        // Sets up context
        let mut configs = HashMap::new();

        let config_map = JMap::from_env(&env, config_object)?;
        config_map.iter()?.for_each(|config| {
            let key: String = env.get_string(JString::from(config.0)).unwrap().into();
            let value: String = env.get_string(JString::from(config.1)).unwrap().into();

            configs.insert(key, value);
        });

        // Whether we've enabled additional debugging on the native side
        let debug_native = configs
            .get("debug_native")
            .and_then(|x| x.parse::<bool>().ok())
            .unwrap_or(false);

        // Use multi-threaded tokio runtime to prevent blocking spawned tasks if any
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;

        let metrics = Arc::new(jni_new_global_ref!(env, metrics_node)?);

        // We need to keep the session context alive. Some session state like temporary
        // dictionaries are stored in session context. If it is dropped, the temporary
        // dictionaries will be dropped as well.
        let session = prepare_datafusion_session_context(&configs)?;

        let exec_context = Box::new(ExecutionContext {
            id,
            spark_plan,
            root_op: None,
            scans: vec![],
            stream: None,
            ffi_arrays: vec![],
            conf: configs,
            runtime,
            metrics,
            session_ctx: Arc::new(session),
            debug_native,
        });

        Ok(Box::into_raw(exec_context) as i64)
    })
}

/// Parse Comet configs and configure DataFusion session context.
fn prepare_datafusion_session_context(
    conf: &HashMap<String, String>,
) -> Result<SessionContext, CometError> {
    // Get the batch size from Comet JVM side
    let batch_size = conf
        .get("batch_size")
        .ok_or(CometError::Internal(
            "Config 'batch_size' is not specified from Comet JVM side".to_string(),
        ))?
        .parse::<usize>()?;

    let mut rt_config = RuntimeConfig::new().with_disk_manager(DiskManagerConfig::NewOs);

    // Set up memory limit if specified
    if conf.contains_key("memory_limit") {
        let memory_limit = conf.get("memory_limit").unwrap().parse::<usize>()?;

        let memory_fraction = conf
            .get("memory_fraction")
            .ok_or(CometError::Internal(
                "Config 'memory_fraction' is not specified from Comet JVM side".to_string(),
            ))?
            .parse::<f64>()?;

        rt_config = rt_config.with_memory_limit(memory_limit, memory_fraction);
    }

    // Get Datafusion configuration from Spark Execution context
    // can be configured in Comet Spark JVM using Spark --conf parameters
    // e.g: spark-shell --conf spark.datafusion.sql_parser.parse_float_as_decimal=true
    let df_config = conf
        .iter()
        .filter(|(k, _)| k.starts_with("datafusion."))
        .map(|kv| (kv.0.to_owned(), kv.1.to_owned()))
        .collect::<Vec<(String, String)>>();

    let session_config =
        SessionConfig::from_string_hash_map(std::collections::HashMap::from_iter(df_config))?
            .with_batch_size(batch_size);

    let runtime = RuntimeEnv::new(rt_config).unwrap();

    Ok(SessionContext::new_with_config_rt(
        session_config,
        Arc::new(runtime),
    ))
}

/// Prepares arrow arrays for output.
fn prepare_output(
    output: Result<RecordBatch, DataFusionError>,
    env: JNIEnv,
    exec_context: &mut ExecutionContext,
) -> Result<jlongArray, CometError> {
    let output_batch = output?;
    let results = output_batch.columns();
    let num_rows = output_batch.num_rows();

    if exec_context.debug_native {
        // Validate the output arrays.
        for array in results.iter() {
            let array_data = array.to_data();
            array_data
                .validate_full()
                .expect("Invalid output array data");
        }
    }

    let return_flag = 1;

    let long_array = env.new_long_array((results.len() * 2) as i32 + 2)?;
    env.set_long_array_region(long_array, 0, &[return_flag, num_rows as jlong])?;

    let mut arrays = vec![];

    let mut i = 0;
    while i < results.len() {
        let array_ref = results.get(i).ok_or(CometError::IndexOutOfBounds(i))?;
        let (array, schema) = array_ref.to_data().to_spark()?;

        unsafe {
            let arrow_array = Arc::from_raw(array as *const FFI_ArrowArray);
            let arrow_schema = Arc::from_raw(schema as *const FFI_ArrowSchema);
            arrays.push((arrow_array, arrow_schema));
        }

        env.set_long_array_region(long_array, (i * 2) as i32 + 2, &[array, schema])?;
        i += 1;
    }

    // Update metrics
    update_metrics(&env, exec_context)?;

    // Record the pointer to allocated Arrow Arrays
    exec_context.ffi_arrays = arrays;

    Ok(long_array)
}

#[no_mangle]
/// Accept serialized query plan and the addresses of Arrow Arrays from Spark,
/// then execute the query. Return addresses of arrow vector.
pub extern "system" fn Java_org_apache_comet_Native_executePlan(
    env: JNIEnv,
    _class: JClass,
    exec_context: jlong,
    addresses_array: jobjectArray,
    finishes: jbooleanArray,
    batch_rows: jint,
) -> jlongArray {
    try_unwrap_or_throw(env, || {
        let addresses_vec = convert_addresses_arrays(&env, addresses_array)?;
        let mut all_inputs: Vec<Vec<ArrayRef>> = Vec::with_capacity(addresses_vec.len());

        let exec_context = get_execution_context(exec_context);
        for addresses in addresses_vec.iter() {
            let mut inputs: Vec<ArrayRef> = vec![];

            let array_num = addresses.size()? as usize;
            assert_eq!(array_num % 2, 0, "Arrow Array addresses are invalid!");

            let num_arrays = array_num / 2;
            let array_elements = addresses.as_ptr();

            let mut i: usize = 0;
            while i < num_arrays {
                let array_ptr = unsafe { *(array_elements.add(i * 2)) };
                let schema_ptr = unsafe { *(array_elements.add(i * 2 + 1)) };
                let array_data = ArrayData::from_spark((array_ptr, schema_ptr))?;

                if exec_context.debug_native {
                    // Validate the array data from JVM.
                    array_data.validate_full().expect("Invalid array data");
                }

                inputs.push(make_array(array_data));
                i += 1;
            }

            all_inputs.push(inputs);
        }

        // Prepares the input batches.
        let eofs = env.get_boolean_array_elements(finishes, ReleaseMode::NoCopyBack)?;
        let eof_flags = eofs.as_ptr();

        // Whether reaching the end of input batches.
        let mut finished = true;
        let mut input_batches = all_inputs
            .into_iter()
            .enumerate()
            .map(|(idx, inputs)| unsafe {
                let eof = eof_flags.add(idx);

                if *eof == 1 {
                    InputBatch::EOF
                } else {
                    finished = false;
                    InputBatch::new(inputs, Some(batch_rows as usize))
                }
            })
            .collect::<Vec<InputBatch>>();

        // Retrieve the query
        let exec_context_id = exec_context.id;

        // Initialize the execution stream.
        // Because we don't know if input arrays are dictionary-encoded when we create
        // query plan, we need to defer stream initialization to first time execution.
        if exec_context.root_op.is_none() {
            let planner = PhysicalPlanner::new().with_exec_id(exec_context_id);
            let (scans, root_op) =
                planner.create_plan(&exec_context.spark_plan, &mut input_batches)?;

            exec_context.root_op = Some(root_op.clone());
            exec_context.scans = scans;

            if exec_context.debug_native {
                let formatted_plan_str =
                    DisplayableExecutionPlan::new(root_op.as_ref()).indent(true);
                info!("Comet native query plan:\n {formatted_plan_str:}");
            }

            let task_ctx = exec_context.session_ctx.task_ctx();
            let stream = exec_context
                .root_op
                .as_ref()
                .unwrap()
                .execute(0, task_ctx)?;
            exec_context.stream = Some(stream);
        } else {
            input_batches
                .into_iter()
                .enumerate()
                .for_each(|(idx, input_batch)| {
                    let scan = &mut exec_context.scans[idx];

                    // Set inputs at `Scan` node.
                    scan.set_input_batch(input_batch);
                });
        }

        loop {
            // Polling the stream.
            let next_item = exec_context.stream.as_mut().unwrap().next();
            let poll_output = exec_context.runtime.block_on(async { poll!(next_item) });

            match poll_output {
                Poll::Ready(Some(output)) => {
                    return prepare_output(output, env, exec_context);
                }
                Poll::Ready(None) => {
                    // Reaches EOF of output.

                    // Update metrics
                    update_metrics(&env, exec_context)?;

                    let long_array = env.new_long_array(1)?;
                    env.set_long_array_region(long_array, 0, &[-1])?;

                    return Ok(long_array);
                }
                // After reaching the end of any input, a poll pending means there are more than one
                // blocking operators, we don't need go back-forth between JVM/Native. Just
                // keeping polling.
                Poll::Pending if finished => {
                    // Update metrics
                    update_metrics(&env, exec_context)?;

                    // Output not ready yet
                    continue;
                }
                // Not reaching the end of input yet, so a poll pending means there are blocking
                // operators. Just returning to keep reading next input.
                Poll::Pending => {
                    // Update metrics
                    update_metrics(&env, exec_context)?;
                    return return_pending(env);
                }
            }
        }
    })
}

fn return_pending(env: JNIEnv) -> Result<jlongArray, CometError> {
    let long_array = env.new_long_array(1)?;
    env.set_long_array_region(long_array, 0, &[0])?;

    Ok(long_array)
}

#[no_mangle]
/// Peeks into next output if any.
pub extern "system" fn Java_org_apache_comet_Native_peekNext(
    env: JNIEnv,
    _class: JClass,
    exec_context: jlong,
) -> jlongArray {
    try_unwrap_or_throw(env, || {
        // Retrieve the query
        let exec_context = get_execution_context(exec_context);

        if exec_context.stream.is_none() {
            // Plan is not initialized yet.
            return return_pending(env);
        }

        // Polling the stream.
        let next_item = exec_context.stream.as_mut().unwrap().next();
        let poll_output = exec_context.runtime.block_on(async { poll!(next_item) });

        match poll_output {
            Poll::Ready(Some(output)) => prepare_output(output, env, exec_context),
            _ => {
                // Update metrics
                update_metrics(&env, exec_context)?;
                return_pending(env)
            }
        }
    })
}

#[no_mangle]
/// Drop the native query plan object and context object.
pub extern "system" fn Java_org_apache_comet_Native_releasePlan(
    env: JNIEnv,
    _class: JClass,
    exec_context: jlong,
) {
    try_unwrap_or_throw(env, || unsafe {
        let execution_context = get_execution_context(exec_context);
        let _: Box<ExecutionContext> = Box::from_raw(execution_context);
        Ok(())
    })
}

/// Updates the metrics of the query plan.
fn update_metrics(env: &JNIEnv, exec_context: &ExecutionContext) -> Result<(), CometError> {
    let native_query = exec_context.root_op.as_ref().unwrap();
    let metrics = exec_context.metrics.as_obj();
    update_comet_metric(env, metrics, native_query)
}

/// Converts a Java array of address arrays to a Rust vector of address arrays.
fn convert_addresses_arrays<'a>(
    env: &'a JNIEnv<'a>,
    addresses_array: jobjectArray,
) -> JNIResult<Vec<AutoArray<'a, 'a, jlong>>> {
    let array_len = env.get_array_length(addresses_array)?;
    let mut res: Vec<AutoArray<jlong>> = Vec::new();

    for i in 0..array_len {
        let array: AutoArray<jlong> = env.get_array_elements(
            env.get_object_array_element(addresses_array, i)?
                .into_inner() as jlongArray,
            ReleaseMode::NoCopyBack,
        )?;
        res.push(array);
    }

    Ok(res)
}

fn convert_datatype_arrays(
    env: &'_ JNIEnv<'_>,
    serialized_datatypes: jobjectArray,
) -> JNIResult<Vec<ArrowDataType>> {
    let array_len = env.get_array_length(serialized_datatypes)?;
    let mut res: Vec<ArrowDataType> = Vec::new();

    for i in 0..array_len {
        let array = env
            .get_object_array_element(serialized_datatypes, i)?
            .into_inner() as jbyteArray;

        let bytes = env.convert_byte_array(array)?;
        let data_type = serde::deserialize_data_type(bytes.as_slice()).unwrap();
        let arrow_dt = to_arrow_datatype(&data_type);
        res.push(arrow_dt);
    }

    Ok(res)
}

fn get_execution_context<'a>(id: i64) -> &'a mut ExecutionContext {
    unsafe {
        (id as *mut ExecutionContext)
            .as_mut()
            .expect("Comet execution context shouldn't be null!")
    }
}
