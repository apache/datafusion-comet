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

//! JNI adapter for local execution, which runs a whole admitted Comet plan as one DataFusion
//! graph inside the result task of a local Spark application. The registry holds only live
//! queries, never plans for reuse. EOF, errors and explicit close remove entries; Java task
//! completion also closes.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion_comet_jni_bridge::{jni_new_global_ref, JVMClasses};
use datafusion_comet_local::handle::{QueryHandle, QueryPoll};
use datafusion_comet_local::LocalQuery;
use jni::objects::{JByteArray, JLongArray, JObject, JObjectArray, JString, ReleaseMode};
use jni::sys::{jboolean, jint, jlong, JNI_FALSE};
use jni::{Env, EnvUnowned};
use parking_lot::Mutex;

use crate::errors::{try_unwrap_or_throw, CometError, CometResult};
use crate::execution::jni_api::{
    create_execution_context, create_memory_pool, get_runtime, prepare_output,
    prepare_plan_settings, ExecutionContext, PlanInputs, PlanJvmRefs,
};
use crate::execution::spark_config::{SparkConfig, SPARK_EXECUTOR_CORES};

struct Entry {
    query: QueryHandle,
    columns: usize,
    // Holds the plan's memory pool and session until the query is closed. Dropped after the
    // query, which cancels the graph that reserves from the pool. Only the creating thread
    // uses it; the mutex makes the entry shareable with result pulls.
    _context: Mutex<Box<ExecutionContext>>,
}

// JNI carries numeric IDs instead of raw pointers, so close racing with a result pull
// cannot free an object the pull is using. Last reader releases the removed entry.
static QUERIES: LazyLock<Mutex<HashMap<i64, Arc<Entry>>>> = LazyLock::new(Mutex::default);
static NEXT_ID: AtomicI64 = AtomicI64::new(1);
const MAX_LIVE_QUERIES: usize = 1024;

fn close(id: i64) {
    let entry = QUERIES.lock().remove(&id);
    if let Some(entry) = entry {
        entry.query.cancel();
    }
}

/// Creates and starts a local query over `partitions` partitions of a serialized Comet plan
/// without JVM inputs. Its memory pool and DataFusion session are those `Native.createPlan`
/// creates for a plan of the calling Spark task.
#[no_mangle]
pub extern "system" fn Java_org_apache_comet_local_NativeLocal_createQuery(
    e: EnvUnowned,
    _: JObject,
    serialized_query: JByteArray,
    serialized_spark_configs: JByteArray,
    partitions: jint,
    columns: jint,
    comet_task_memory_manager_obj: JObject,
    local_dirs: JObjectArray,
    batch_size: jint,
    off_heap_mode: jboolean,
    memory_pool_type: JString,
    memory_limit: jlong,
    off_heap_size: jlong,
    task_attempt_id: jlong,
    task_cpus: jlong,
    read_ahead_bytes: jlong,
) -> jlong {
    try_unwrap_or_throw(&e, |env| {
        if !(1..=MAX_PARTITIONS).contains(&partitions) || columns < 1 {
            return Err(CometError::Internal(
                "Invalid local query parameters".into(),
            ));
        }
        let start = Instant::now();
        let settings = prepare_plan_settings(&env.convert_byte_array(serialized_spark_configs)?)?;
        // Run as many partitions at once as Spark would run tasks, which also bounds how many
        // operators share the memory pool. `local[N]` resolves to N here.
        let max_running = settings.spark_config.get_usize(SPARK_EXECUTOR_CORES, 1);
        JVMClasses::init(env);
        let serialized_plan = env.convert_byte_array(serialized_query)?;
        let task_memory_manager =
            Arc::new(jni_new_global_ref!(env, comet_task_memory_manager_obj)?);
        let memory_pool_type = memory_pool_type.try_to_string(env)?;
        let mut local_dirs_vec = vec![];
        for i in 0..local_dirs.len(env)? {
            let dir = local_dirs.get_element(env, i)?;
            let dir = unsafe { JString::from_raw(&*env, dir.into_raw()) };
            local_dirs_vec.push(dir.try_to_string(env)?);
        }
        let id = next_id()?;
        let inputs = PlanInputs {
            id,
            settings,
            serialized_plan,
            partition_count: partitions as usize,
            batch_size: batch_size as usize,
            off_heap_mode: off_heap_mode != JNI_FALSE,
            memory_pool_type,
            memory_limit,
            off_heap_size,
            task_attempt_id,
            task_cpus: task_cpus as usize,
            local_dirs: local_dirs_vec,
            metrics_update_interval: 0,
            start,
            jvm: PlanJvmRefs::default(),
        };
        let context = create_execution_context(inputs, |config| {
            create_memory_pool(config, task_memory_manager, task_attempt_id)
        })?;
        let plan = local_plan(&context, partitions)?;
        if plan.schema().fields().len() != columns as usize {
            return Err(CometError::Internal(
                "Local output schema width mismatch".into(),
            ));
        }
        let query = LocalQuery::new(plan, context.session_ctx.task_ctx())
            .in_partition_order(max_running, read_ahead_bytes.max(0) as usize);
        let query = QueryHandle::start(query, &get_runtime())?;
        register(
            id,
            Entry {
                query,
                columns: columns as usize,
                _context: Mutex::new(context),
            },
        )?;
        Ok(id)
    })
}

const MAX_PARTITIONS: jint = 1024;

/// One graph for the whole query. Each partition's operators are created by the planner for
/// that partition, as a Spark task would create them, so per-partition state such as a range
/// slice or a partition-dependent expression is correct. Partition `i` of the graph is the
/// single output partition of the operators created for partition `i`.
fn local_plan(context: &ExecutionContext, partitions: jint) -> CometResult<Arc<dyn ExecutionPlan>> {
    let mut plans = Vec::with_capacity(partitions as usize);
    for partition in 0..partitions {
        let (scans, shuffle_scans, root) = context.planner(partition).create_plan(
            &context.spark_plan,
            &mut vec![],
            partitions as usize,
        )?;
        if !scans.is_empty()
            || !shuffle_scans.is_empty()
            || root.native_plan.output_partitioning().partition_count() != 1
        {
            return Err(CometError::Internal(
                "Local query plans must have no JVM input and one output partition".into(),
            ));
        }
        plans.push(Arc::clone(&root.native_plan));
    }
    Ok(UnionExec::try_new(plans)?)
}

fn next_id() -> CometResult<i64> {
    // IDs are never reused. A counter that wrapped would reuse them, so fail instead.
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    if id < 1 {
        return Err(CometError::Internal("Local query IDs exhausted".into()));
    }
    Ok(id)
}

fn register(id: i64, entry: Entry) -> CometResult<()> {
    let mut registry = QUERIES.lock();
    if registry.len() >= MAX_LIVE_QUERIES {
        return Err(CometError::Internal("Too many live local queries".into()));
    }
    registry.insert(id, Arc::new(entry));
    Ok(())
}

fn next(env: &mut Env, id: i64, arrays: JLongArray, schemas: JLongArray) -> CometResult<jlong> {
    let entry = QUERIES
        .lock()
        .get(&id)
        .cloned()
        .ok_or_else(|| CometError::Internal("Local query is closed or unknown".into()))?;
    if arrays.len(env)? != entry.columns || schemas.len(env)? != entry.columns {
        return Err(CometError::Internal(
            "Local output column count mismatch".into(),
        ));
    }
    // The caller supplies writable Arrow C structs. Reject null pointers before exporting.
    for addresses in [&arrays, &schemas] {
        let elements = unsafe { addresses.get_elements(env, ReleaseMode::NoCopyBack)? };
        if elements.contains(&0) {
            return Err(CometError::Internal("Null local output pointer".into()));
        }
    }
    match entry.query.poll(Duration::from_millis(50))? {
        QueryPoll::Pending => Ok(-2),
        QueryPoll::Finished => Ok(-1),
        QueryPoll::Batch(batch) => prepare_output(env, arrays, schemas, batch, false),
    }
}

/// -2 means pending, -1 means EOF; nonnegative values are exported row counts.
#[no_mangle]
pub extern "system" fn Java_org_apache_comet_local_NativeLocal_nextBatch(
    e: EnvUnowned,
    _: JObject,
    id: jlong,
    arrays: JLongArray,
    schemas: JLongArray,
) -> jlong {
    try_unwrap_or_throw(&e, |env| {
        let result = next(env, id, arrays, schemas);
        if result.is_err() || matches!(result, Ok(-1)) {
            close(id);
        }
        result
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_comet_local_NativeLocal_close(
    e: EnvUnowned,
    _: JObject,
    id: jlong,
) {
    try_unwrap_or_throw(&e, |_| {
        close(id);
        Ok(())
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_comet_local_NativeLocal_activeQueries(
    e: EnvUnowned,
    _: JObject,
) -> jlong {
    try_unwrap_or_throw(&e, |_| Ok(QUERIES.lock().len() as jlong))
}
