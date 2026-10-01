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

//! JNI adapter for local execution. The registry holds only live query handles, never plans
//! for reuse. EOF, errors, and explicit close remove entries; Java task completion also closes.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use datafusion::execution::TaskContext;
use datafusion::prelude::SessionConfig;
use datafusion_comet_local::handle::{QueryHandle, QueryPoll};
use datafusion_comet_local::range::range_plan;
use datafusion_comet_local::LocalQuery;
use jni::objects::{JLongArray, JObject, ReleaseMode};
use jni::sys::{jint, jlong};
use jni::{Env, EnvUnowned};
use parking_lot::Mutex;

use crate::errors::{try_unwrap_or_throw, CometError, CometResult};
use crate::execution::jni_api::{get_runtime, prepare_output};

struct Entry {
    query: QueryHandle,
    columns: usize,
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

#[no_mangle]
pub extern "system" fn Java_org_apache_comet_local_NativeLocal_createRange(
    e: EnvUnowned,
    _: JObject,
    start: jlong,
    end: jlong,
    step: jlong,
    partitions: jint,
    batch_size: jint,
    columns: jint,
) -> jlong {
    try_unwrap_or_throw(&e, |_| {
        let plan = range_plan(
            start,
            end,
            step,
            partitions as usize,
            batch_size as usize,
            columns as usize,
        )?;
        let context = Arc::new(
            TaskContext::default()
                .with_session_config(SessionConfig::new().with_batch_size(batch_size as usize)),
        );
        let query = QueryHandle::start(LocalQuery::new(plan, context), &get_runtime())?;
        let id = NEXT_ID
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .map_err(|_| CometError::Internal("Local query IDs exhausted".into()))?;
        let mut registry = QUERIES.lock();
        if registry.len() >= MAX_LIVE_QUERIES {
            return Err(CometError::Internal("Too many live local queries".into()));
        }
        registry.insert(
            id,
            Arc::new(Entry {
                query,
                columns: columns as usize,
            }),
        );
        Ok(id)
    })
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
