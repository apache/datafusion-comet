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

use super::{serde, utils::SparkArrowConvert, CometMemoryPool};
use arrow::datatypes::DataType as ArrowDataType;
use arrow_array::RecordBatch;
use datafusion::{
    execution::{disk_manager::DiskManagerConfig, runtime_env::RuntimeEnv},
    physical_plan::{display::DisplayableExecutionPlan, SendableRecordBatchStream},
    prelude::{SessionConfig, SessionContext},
};
use datafusion_execution::memory_pool::{
    FairSpillPool, GreedyMemoryPool, MemoryPool, TrackConsumersPool,
};
use futures::poll;
use jni::{
    errors::Result as JNIResult,
    objects::{
        JByteArray, JClass, JIntArray, JLongArray, JObject, JObjectArray, JPrimitiveArray, JString,
        ReleaseMode,
    },
    sys::{jbyteArray, jint, jlong, jlongArray},
    JNIEnv,
};
use std::time::{Duration, Instant};
use std::{collections::HashMap, sync::Arc, task::Poll};

use crate::{
    errors::{try_unwrap_or_throw, CometError, CometResult},
    execution::{
        metrics::utils::update_comet_metric, planner::PhysicalPlanner, serde::to_arrow_datatype,
        shuffle::row::process_sorted_row_partition, sort::RdxSort,
    },
    jvm_bridge::{jni_new_global_ref, JVMClasses},
};
use datafusion_comet_proto::spark_operator::Operator;
use datafusion_common::ScalarValue;
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use futures::stream::StreamExt;
use jni::objects::JByteBuffer;
use jni::sys::JNI_FALSE;
use jni::{
    objects::GlobalRef,
    sys::{jboolean, jdouble, jintArray, jobjectArray, jstring},
};
use std::num::NonZeroUsize;
use std::sync::Mutex;
use tokio::runtime::Runtime;

use crate::execution::operators::ScanExec;
use crate::execution::shuffle::{read_ipc_compressed, CompressionCodec};
use crate::execution::spark_plan::SparkPlan;
use log::info;
use once_cell::sync::{Lazy, OnceCell};

/// Comet native execution context. Kept alive across JNI calls.
struct ExecutionContext {
    /// The id of the execution context.
    pub id: i64,
    /// Task attempt id
    pub task_attempt_id: i64,
    /// The deserialized Spark plan
    pub spark_plan: Operator,
    /// The number of partitions
    pub partition_count: usize,
    /// The DataFusion root operator converted from the `spark_plan`
    pub root_op: Option<Arc<SparkPlan>>,
    /// The input sources for the DataFusion plan
    pub scans: Vec<ScanExec>,
    /// The global reference of input sources for the DataFusion plan
    pub input_sources: Vec<Arc<GlobalRef>>,
    /// The record batch stream to pull results from
    pub stream: Option<SendableRecordBatchStream>,
    /// The Tokio runtime used for async.
    pub runtime: Runtime,
    /// Native metrics
    pub metrics: Arc<GlobalRef>,
    // The interval in milliseconds to update metrics
    pub metrics_update_interval: Option<Duration>,
    // The last update time of metrics
    pub metrics_last_update_time: Instant,
    /// The time it took to create the native plan and configure the context
    pub plan_creation_time: Duration,
    /// DataFusion SessionContext
    pub session_ctx: Arc<SessionContext>,
    /// Whether to enable additional debugging checks & messages
    pub debug_native: bool,
    /// Whether to write native plans with metrics to stdout
    pub explain_native: bool,
    /// Memory pool config
    pub memory_pool_config: MemoryPoolConfig,
}

#[derive(PartialEq, Eq)]
enum MemoryPoolType {
    Unified,
    Greedy,
    FairSpill,
    GreedyTaskShared,
    FairSpillTaskShared,
    GreedyGlobal,
    FairSpillGlobal,
}

struct MemoryPoolConfig {
    pool_type: MemoryPoolType,
    pool_size: usize,
}

impl MemoryPoolConfig {
    fn new(pool_type: MemoryPoolType, pool_size: usize) -> Self {
        Self {
            pool_type,
            pool_size,
        }
    }
}

/// The per-task memory pools keyed by task attempt id.
static TASK_SHARED_MEMORY_POOLS: Lazy<Mutex<HashMap<i64, PerTaskMemoryPool>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

struct PerTaskMemoryPool {
    memory_pool: Arc<dyn MemoryPool>,
    num_plans: usize,
}

impl PerTaskMemoryPool {
    fn new(memory_pool: Arc<dyn MemoryPool>) -> Self {
        Self {
            memory_pool,
            num_plans: 0,
        }
    }
}

/// Accept serialized query plan and return the address of the native query plan.
/// # Safety
/// This function is inherently unsafe since it deals with raw pointers passed from JNI.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_comet_Native_createPlan(
    e: JNIEnv,
    _class: JClass,
    id: jlong,
    iterators: jobjectArray,
    serialized_query: jbyteArray,
    partition_count: jint,
    metrics_node: JObject,
    metrics_update_interval: jlong,
    comet_task_memory_manager_obj: JObject,
    batch_size: jint,
    use_unified_memory_manager: jboolean,
    memory_pool_type: jstring,
    memory_limit: jlong,
    memory_limit_per_task: jlong,
    task_attempt_id: jlong,
    debug_native: jboolean,
    explain_native: jboolean,
    worker_threads: jint,
    blocking_threads: jint,
) -> jlong {
    try_unwrap_or_throw(&e, |mut env| {
        // Init JVM classes
        JVMClasses::init(&mut env);

        let start = Instant::now();

        let array = unsafe { JPrimitiveArray::from_raw(serialized_query) };
        let bytes = env.convert_byte_array(array)?;

        // Deserialize query plan
        let spark_plan = serde::deserialize_op(bytes.as_slice())?;

        // Use multi-threaded tokio runtime to prevent blocking spawned tasks if any
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(worker_threads as usize)
            .max_blocking_threads(blocking_threads as usize)
            .enable_all()
            .build()?;

        let metrics = Arc::new(jni_new_global_ref!(env, metrics_node)?);

        // Get the global references of input sources
        let mut input_sources = vec![];
        let iter_array = JObjectArray::from_raw(iterators);
        let num_inputs = env.get_array_length(&iter_array)?;
        for i in 0..num_inputs {
            let input_source = env.get_object_array_element(&iter_array, i)?;
            let input_source = Arc::new(jni_new_global_ref!(env, input_source)?);
            input_sources.push(input_source);
        }
        let task_memory_manager =
            Arc::new(jni_new_global_ref!(env, comet_task_memory_manager_obj)?);

        let memory_pool_type = env.get_string(&JString::from_raw(memory_pool_type))?.into();
        let memory_pool_config = parse_memory_pool_config(
            use_unified_memory_manager != JNI_FALSE,
            memory_pool_type,
            memory_limit,
            memory_limit_per_task,
        )?;
        let memory_pool =
            create_memory_pool(&memory_pool_config, task_memory_manager, task_attempt_id);

        // We need to keep the session context alive. Some session state like temporary
        // dictionaries are stored in session context. If it is dropped, the temporary
        // dictionaries will be dropped as well.
        let session = prepare_datafusion_session_context(batch_size as usize, memory_pool)?;

        let plan_creation_time = start.elapsed();

        let metrics_update_interval = if metrics_update_interval > 0 {
            Some(Duration::from_millis(metrics_update_interval as u64))
        } else {
            None
        };

        let exec_context = Box::new(ExecutionContext {
            id,
            task_attempt_id,
            spark_plan,
            partition_count: partition_count as usize,
            root_op: None,
            scans: vec![],
            input_sources,
            stream: None,
            runtime,
            metrics,
            metrics_update_interval,
            metrics_last_update_time: Instant::now(),
            plan_creation_time,
            session_ctx: Arc::new(session),
            debug_native: debug_native == 1,
            explain_native: explain_native == 1,
            memory_pool_config,
        });

        Ok(Box::into_raw(exec_context) as i64)
    })
}

/// Configure DataFusion session context.
fn prepare_datafusion_session_context(
    batch_size: usize,
    memory_pool: Arc<dyn MemoryPool>,
) -> CometResult<SessionContext> {
    let mut rt_config = RuntimeEnvBuilder::new().with_disk_manager(DiskManagerConfig::NewOs);
    rt_config = rt_config.with_memory_pool(memory_pool);

    // Get Datafusion configuration from Spark Execution context
    // can be configured in Comet Spark JVM using Spark --conf parameters
    // e.g: spark-shell --conf spark.datafusion.sql_parser.parse_float_as_decimal=true
    let session_config = SessionConfig::new()
        .with_batch_size(batch_size)
        // DataFusion partial aggregates can emit duplicate rows so we disable the
        // skip partial aggregation feature because this is not compatible with Spark's
        // use of partial aggregates.
        .set(
            "datafusion.execution.skip_partial_aggregation_probe_ratio_threshold",
            // this is the threshold of number of groups / number of rows and the
            // maximum value is 1.0, so we set the threshold a little higher just
            // to be safe
            &ScalarValue::Float64(Some(1.1)),
        );

    #[allow(deprecated)]
    let runtime = RuntimeEnv::try_new(rt_config)?;

    let mut session_ctx = SessionContext::new_with_config_rt(session_config, Arc::new(runtime));

    datafusion_functions_nested::register_all(&mut session_ctx)?;

    Ok(session_ctx)
}

fn parse_memory_pool_config(
    use_unified_memory_manager: bool,
    memory_pool_type: String,
    memory_limit: i64,
    memory_limit_per_task: i64,
) -> CometResult<MemoryPoolConfig> {
    let memory_pool_config = if use_unified_memory_manager {
        MemoryPoolConfig::new(MemoryPoolType::Unified, 0)
    } else {
        // Use the memory pool from DF
        let pool_size = memory_limit as usize;
        let pool_size_per_task = memory_limit_per_task as usize;
        match memory_pool_type.as_str() {
            "fair_spill_task_shared" => {
                MemoryPoolConfig::new(MemoryPoolType::FairSpillTaskShared, pool_size_per_task)
            }
            "greedy_task_shared" => {
                MemoryPoolConfig::new(MemoryPoolType::GreedyTaskShared, pool_size_per_task)
            }
            "fair_spill_global" => {
                MemoryPoolConfig::new(MemoryPoolType::FairSpillGlobal, pool_size)
            }
            "greedy_global" => MemoryPoolConfig::new(MemoryPoolType::GreedyGlobal, pool_size),
            "fair_spill" => MemoryPoolConfig::new(MemoryPoolType::FairSpill, pool_size_per_task),
            "greedy" => MemoryPoolConfig::new(MemoryPoolType::Greedy, pool_size_per_task),
            _ => {
                return Err(CometError::Config(format!(
                    "Unsupported memory pool type: {}",
                    memory_pool_type
                )))
            }
        }
    };
    Ok(memory_pool_config)
}

fn create_memory_pool(
    memory_pool_config: &MemoryPoolConfig,
    comet_task_memory_manager: Arc<GlobalRef>,
    task_attempt_id: i64,
) -> Arc<dyn MemoryPool> {
    const NUM_TRACKED_CONSUMERS: usize = 10;
    match memory_pool_config.pool_type {
        MemoryPoolType::Unified => {
            // Set Comet memory pool for native
            let memory_pool = CometMemoryPool::new(comet_task_memory_manager);
            Arc::new(memory_pool)
        }
        MemoryPoolType::Greedy => Arc::new(TrackConsumersPool::new(
            GreedyMemoryPool::new(memory_pool_config.pool_size),
            NonZeroUsize::new(NUM_TRACKED_CONSUMERS).unwrap(),
        )),
        MemoryPoolType::FairSpill => Arc::new(TrackConsumersPool::new(
            FairSpillPool::new(memory_pool_config.pool_size),
            NonZeroUsize::new(NUM_TRACKED_CONSUMERS).unwrap(),
        )),
        MemoryPoolType::GreedyGlobal => {
            static GLOBAL_MEMORY_POOL_GREEDY: OnceCell<Arc<dyn MemoryPool>> = OnceCell::new();
            let memory_pool = GLOBAL_MEMORY_POOL_GREEDY.get_or_init(|| {
                Arc::new(TrackConsumersPool::new(
                    GreedyMemoryPool::new(memory_pool_config.pool_size),
                    NonZeroUsize::new(NUM_TRACKED_CONSUMERS).unwrap(),
                ))
            });
            Arc::clone(memory_pool)
        }
        MemoryPoolType::FairSpillGlobal => {
            static GLOBAL_MEMORY_POOL_FAIR: OnceCell<Arc<dyn MemoryPool>> = OnceCell::new();
            let memory_pool = GLOBAL_MEMORY_POOL_FAIR.get_or_init(|| {
                Arc::new(TrackConsumersPool::new(
                    FairSpillPool::new(memory_pool_config.pool_size),
                    NonZeroUsize::new(NUM_TRACKED_CONSUMERS).unwrap(),
                ))
            });
            Arc::clone(memory_pool)
        }
        MemoryPoolType::GreedyTaskShared | MemoryPoolType::FairSpillTaskShared => {
            let mut memory_pool_map = TASK_SHARED_MEMORY_POOLS.lock().unwrap();
            let per_task_memory_pool =
                memory_pool_map.entry(task_attempt_id).or_insert_with(|| {
                    let pool: Arc<dyn MemoryPool> =
                        if memory_pool_config.pool_type == MemoryPoolType::GreedyTaskShared {
                            Arc::new(TrackConsumersPool::new(
                                GreedyMemoryPool::new(memory_pool_config.pool_size),
                                NonZeroUsize::new(NUM_TRACKED_CONSUMERS).unwrap(),
                            ))
                        } else {
                            Arc::new(TrackConsumersPool::new(
                                FairSpillPool::new(memory_pool_config.pool_size),
                                NonZeroUsize::new(NUM_TRACKED_CONSUMERS).unwrap(),
                            ))
                        };
                    PerTaskMemoryPool::new(pool)
                });
            per_task_memory_pool.num_plans += 1;
            Arc::clone(&per_task_memory_pool.memory_pool)
        }
    }
}

/// Prepares arrow arrays for output.
fn prepare_output(
    env: &mut JNIEnv,
    array_addrs: jlongArray,
    schema_addrs: jlongArray,
    output_batch: RecordBatch,
    validate: bool,
) -> CometResult<jlong> {
    let array_address_array = unsafe { JLongArray::from_raw(array_addrs) };
    let num_cols = env.get_array_length(&array_address_array)? as usize;

    let array_addrs =
        unsafe { env.get_array_elements(&array_address_array, ReleaseMode::NoCopyBack)? };
    let array_addrs = &*array_addrs;

    let schema_address_array = unsafe { JLongArray::from_raw(schema_addrs) };
    let schema_addrs =
        unsafe { env.get_array_elements(&schema_address_array, ReleaseMode::NoCopyBack)? };
    let schema_addrs = &*schema_addrs;

    let results = output_batch.columns();
    let num_rows = output_batch.num_rows();

    if results.len() != num_cols {
        return Err(CometError::Internal(format!(
            "Output column count mismatch: expected {num_cols}, got {}",
            results.len()
        )));
    }

    if validate {
        // Validate the output arrays.
        for array in results.iter() {
            let array_data = array.to_data();
            array_data
                .validate_full()
                .expect("Invalid output array data");
        }
    }

    let mut i = 0;
    while i < results.len() {
        let array_ref = results.get(i).ok_or(CometError::IndexOutOfBounds(i))?;
        array_ref
            .to_data()
            .move_to_spark(array_addrs[i], schema_addrs[i])?;

        i += 1;
    }

    Ok(num_rows as jlong)
}

/// Pull the next input from JVM. Note that we cannot pull input batches in
/// `ScanStream.poll_next` when the execution stream is polled for output.
/// Because the input source could be another native execution stream, which
/// will be executed in another tokio blocking thread. It causes JNI throw
/// Java exception. So we pull input batches here and insert them into scan
/// operators before polling the stream,
#[inline]
fn pull_input_batches(exec_context: &mut ExecutionContext) -> Result<(), CometError> {
    exec_context.scans.iter_mut().try_for_each(|scan| {
        scan.get_next_batch()?;
        Ok::<(), CometError>(())
    })
}

/// Accept serialized query plan and the addresses of Arrow Arrays from Spark,
/// then execute the query. Return addresses of arrow vector.
/// # Safety
/// This function is inherently unsafe since it deals with raw pointers passed from JNI.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_comet_Native_executePlan(
    e: JNIEnv,
    _class: JClass,
    stage_id: jint,
    partition: jint,
    exec_context: jlong,
    array_addrs: jlongArray,
    schema_addrs: jlongArray,
) -> jlong {
    try_unwrap_or_throw(&e, |mut env| {
        // Retrieve the query
        let exec_context = get_execution_context(exec_context);

        let exec_context_id = exec_context.id;

        // Initialize the execution stream.
        // Because we don't know if input arrays are dictionary-encoded when we create
        // query plan, we need to defer stream initialization to first time execution.
        if exec_context.root_op.is_none() {
            let start = Instant::now();
            let planner = PhysicalPlanner::new(Arc::clone(&exec_context.session_ctx))
                .with_exec_id(exec_context_id);
            let (scans, root_op) = planner.create_plan(
                &exec_context.spark_plan,
                &mut exec_context.input_sources.clone(),
                exec_context.partition_count,
            )?;
            let physical_plan_time = start.elapsed();

            exec_context.plan_creation_time += physical_plan_time;
            exec_context.root_op = Some(Arc::clone(&root_op));
            exec_context.scans = scans;

            if exec_context.explain_native {
                let formatted_plan_str =
                    DisplayableExecutionPlan::new(root_op.native_plan.as_ref()).indent(true);
                info!("Comet native query plan:\n{formatted_plan_str:}");
            }

            let task_ctx = exec_context.session_ctx.task_ctx();
            let stream = exec_context
                .root_op
                .as_ref()
                .unwrap()
                .native_plan
                .execute(partition as usize, task_ctx)?;
            exec_context.stream = Some(stream);
        } else {
            // Pull input batches
            pull_input_batches(exec_context)?;
        }

        loop {
            // Polling the stream.
            let next_item = exec_context.stream.as_mut().unwrap().next();
            let poll_output = exec_context.runtime.block_on(async { poll!(next_item) });

            // update metrics at interval
            if let Some(interval) = exec_context.metrics_update_interval {
                let now = Instant::now();
                if now - exec_context.metrics_last_update_time >= interval {
                    update_metrics(&mut env, exec_context)?;
                    exec_context.metrics_last_update_time = now;
                }
            }

            match poll_output {
                Poll::Ready(Some(output)) => {
                    // prepare output for FFI transfer
                    return prepare_output(
                        &mut env,
                        array_addrs,
                        schema_addrs,
                        output?,
                        exec_context.debug_native,
                    );
                }
                Poll::Ready(None) => {
                    // Reaches EOF of output.
                    if exec_context.explain_native {
                        if let Some(plan) = &exec_context.root_op {
                            let formatted_plan_str =
                                DisplayableExecutionPlan::with_metrics(plan.native_plan.as_ref())
                                    .indent(true);
                            info!(
                                "Comet native query plan with metrics (Plan #{} Stage {} Partition {}):\
                            \n plan creation (including CometScans fetching first batches) took {:?}:\
                            \n{formatted_plan_str:}",
                                plan.plan_id, stage_id, partition, exec_context.plan_creation_time
                            );
                        }
                    }

                    return Ok(-1);
                }
                // A poll pending means there are more than one blocking operators,
                // we don't need go back-forth between JVM/Native. Just keeping polling.
                Poll::Pending => {
                    // Pull input batches
                    pull_input_batches(exec_context)?;

                    // Output not ready yet
                    continue;
                }
            }
        }
    })
}

#[no_mangle]
/// Drop the native query plan object and context object.
pub extern "system" fn Java_org_apache_comet_Native_releasePlan(
    e: JNIEnv,
    _class: JClass,
    exec_context: jlong,
) {
    try_unwrap_or_throw(&e, |mut env| unsafe {
        let execution_context = get_execution_context(exec_context);

        // Update metrics
        update_metrics(&mut env, execution_context)?;

        if execution_context.memory_pool_config.pool_type == MemoryPoolType::FairSpillTaskShared
            || execution_context.memory_pool_config.pool_type == MemoryPoolType::GreedyTaskShared
        {
            // Decrement the number of native plans using the per-task shared memory pool, and
            // remove the memory pool if the released native plan is the last native plan using it.
            let task_attempt_id = execution_context.task_attempt_id;
            let mut memory_pool_map = TASK_SHARED_MEMORY_POOLS.lock().unwrap();
            if let Some(per_task_memory_pool) = memory_pool_map.get_mut(&task_attempt_id) {
                per_task_memory_pool.num_plans -= 1;
                if per_task_memory_pool.num_plans == 0 {
                    // Drop the memory pool from the per-task memory pool map if there are no
                    // more native plans using it.
                    memory_pool_map.remove(&task_attempt_id);
                }
            }
        }
        let _: Box<ExecutionContext> = Box::from_raw(execution_context);
        Ok(())
    })
}

/// Updates the metrics of the query plan.
fn update_metrics(env: &mut JNIEnv, exec_context: &mut ExecutionContext) -> CometResult<()> {
    if exec_context.root_op.is_some() {
        let native_query = exec_context.root_op.as_ref().unwrap();
        let metrics = exec_context.metrics.as_obj();
        update_comet_metric(env, metrics, native_query)
    } else {
        Ok(())
    }
}

fn convert_datatype_arrays(
    env: &'_ mut JNIEnv<'_>,
    serialized_datatypes: jobjectArray,
) -> JNIResult<Vec<ArrowDataType>> {
    unsafe {
        let obj_array = JObjectArray::from_raw(serialized_datatypes);
        let array_len = env.get_array_length(&obj_array)?;
        let mut res: Vec<ArrowDataType> = Vec::new();

        for i in 0..array_len {
            let inner_array = env.get_object_array_element(&obj_array, i)?;
            let inner_array: JByteArray = inner_array.into();
            let bytes = env.convert_byte_array(inner_array)?;
            let data_type = serde::deserialize_data_type(bytes.as_slice()).unwrap();
            let arrow_dt = to_arrow_datatype(&data_type);
            res.push(arrow_dt);
        }

        Ok(res)
    }
}

fn get_execution_context<'a>(id: i64) -> &'a mut ExecutionContext {
    unsafe {
        (id as *mut ExecutionContext)
            .as_mut()
            .expect("Comet execution context shouldn't be null!")
    }
}

/// Used by Comet shuffle external sorter to write sorted records to disk.
/// # Safety
/// This function is inherently unsafe since it deals with raw pointers passed from JNI.
#[no_mangle]
pub unsafe extern "system" fn Java_org_apache_comet_Native_writeSortedFileNative(
    e: JNIEnv,
    _class: JClass,
    row_addresses: jlongArray,
    row_sizes: jintArray,
    serialized_datatypes: jobjectArray,
    file_path: jstring,
    prefer_dictionary_ratio: jdouble,
    batch_size: jlong,
    checksum_enabled: jboolean,
    checksum_algo: jint,
    current_checksum: jlong,
    compression_codec: jstring,
    compression_level: jint,
    enable_fast_encoding: jboolean,
) -> jlongArray {
    try_unwrap_or_throw(&e, |mut env| unsafe {
        let data_types = convert_datatype_arrays(&mut env, serialized_datatypes)?;

        let row_address_array = JLongArray::from_raw(row_addresses);
        let row_num = env.get_array_length(&row_address_array)? as usize;
        let row_addresses = env.get_array_elements(&row_address_array, ReleaseMode::NoCopyBack)?;

        let row_size_array = JIntArray::from_raw(row_sizes);
        let row_sizes = env.get_array_elements(&row_size_array, ReleaseMode::NoCopyBack)?;

        let row_addresses_ptr = row_addresses.as_ptr();
        let row_sizes_ptr = row_sizes.as_ptr();

        let output_path: String = env
            .get_string(&JString::from_raw(file_path))
            .unwrap()
            .into();

        let checksum_enabled = checksum_enabled == 1;
        let current_checksum = if current_checksum == i64::MIN {
            // Initial checksum is not available.
            None
        } else {
            Some(current_checksum as u32)
        };

        let compression_codec: String = env
            .get_string(&JString::from_raw(compression_codec))
            .unwrap()
            .into();

        let compression_codec = match compression_codec.as_str() {
            "zstd" => CompressionCodec::Zstd(compression_level),
            "lz4" => CompressionCodec::Lz4Frame,
            "snappy" => CompressionCodec::Snappy,
            _ => CompressionCodec::Lz4Frame,
        };

        let (written_bytes, checksum) = process_sorted_row_partition(
            row_num,
            batch_size as usize,
            row_addresses_ptr,
            row_sizes_ptr,
            &data_types,
            output_path,
            prefer_dictionary_ratio,
            checksum_enabled,
            checksum_algo,
            current_checksum,
            &compression_codec,
            enable_fast_encoding != JNI_FALSE,
        )?;

        let checksum = if let Some(checksum) = checksum {
            checksum as i64
        } else {
            // Spark checksums (CRC32 or Adler32) are both u32, so we use i64::MIN to indicate
            // checksum is not available.
            i64::MIN
        };

        let long_array = env.new_long_array(2)?;
        env.set_long_array_region(&long_array, 0, &[written_bytes, checksum])?;

        Ok(long_array.into_raw())
    })
}

#[no_mangle]
/// Used by Comet shuffle external sorter to sort in-memory row partition ids.
pub extern "system" fn Java_org_apache_comet_Native_sortRowPartitionsNative(
    e: JNIEnv,
    _class: JClass,
    address: jlong,
    size: jlong,
) {
    try_unwrap_or_throw(&e, |_| {
        // SAFETY: JVM unsafe memory allocation is aligned with long.
        let array = unsafe { std::slice::from_raw_parts_mut(address as *mut i64, size as usize) };
        array.rdxsort();

        Ok(())
    })
}

#[no_mangle]
/// Used by Comet native shuffle reader
/// # Safety
/// This function is inherently unsafe since it deals with raw pointers passed from JNI.
pub unsafe extern "system" fn Java_org_apache_comet_Native_decodeShuffleBlock(
    e: JNIEnv,
    _class: JClass,
    byte_buffer: JByteBuffer,
    length: jint,
    array_addrs: jlongArray,
    schema_addrs: jlongArray,
) -> jlong {
    try_unwrap_or_throw(&e, |mut env| {
        let raw_pointer = env.get_direct_buffer_address(&byte_buffer)?;
        let length = length as usize;
        let slice: &[u8] = unsafe { std::slice::from_raw_parts(raw_pointer, length) };
        let batch = read_ipc_compressed(slice)?;
        prepare_output(&mut env, array_addrs, schema_addrs, batch, false)
    })
}
