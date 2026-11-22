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

use arrow::datatypes::SchemaRef;
use datafusion::common::DataFusionError;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion_comet_jni_bridge::{jni_new_global_ref, JVMClasses};
use datafusion_comet_local::handle::{QueryHandle, QueryPoll};
use datafusion_comet_local::{LocalQuery, PartitionStart};
use datafusion_comet_proto::spark_operator::operator::OpStruct;
use datafusion_comet_proto::spark_operator::{NativeScan, Operator, SparkFilePartition};
use jni::objects::{JByteArray, JLongArray, JObject, JObjectArray, JString, ReleaseMode};
use jni::sys::{jboolean, jint, jlong, JNI_FALSE};
use jni::{Env, EnvUnowned};
use log::warn;
use parking_lot::Mutex;
use prost::Message;

use crate::errors::{try_unwrap_or_throw, CometError, CometResult};
use crate::execution::jni_api::{
    create_execution_context, create_memory_pool, get_runtime, prepare_output,
    prepare_plan_settings, release_plan, running_native_tasks, ExecutionContext, PlanInputs,
    PlanJvmRefs,
};
use crate::execution::planner::PhysicalPlanner;
use crate::execution::spark_config::{SparkConfig, SPARK_EXECUTOR_CORES};

struct Entry {
    query: QueryHandle,
    columns: usize,
    /// Validates exported batches, as `spark.comet.debug.enabled` does for any Comet plan.
    validate: bool,
    // Holds the plan's memory pool and session until the query is closed. Released after the
    // query, which cancels the graph that reserves from the pool. Only the creating thread
    // uses it; the mutex makes the entry shareable with result pulls.
    context: Mutex<Option<Box<ExecutionContext>>>,
}

impl Drop for Entry {
    /// Releases the context as `Native.releasePlan` does, waiting briefly for the reservations
    /// that the cancelled graph's tasks still hold, so that they are returned before the Spark
    /// task can end.
    fn drop(&mut self) {
        self.query.cancel();
        if let Some(context) = self.context.get_mut().take() {
            if let Err(error) = release_plan(context, &mut |_| Ok(())) {
                warn!("Releasing a local query failed: {error}");
            }
        }
    }
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
///
/// When the plan's leaf is a native file scan, `scan_partitions` holds each partition's
/// serialized `NativeScan` carrying that partition's files, as a Spark task injects them, and is
/// otherwise empty.
#[no_mangle]
pub extern "system" fn Java_org_apache_comet_local_NativeLocal_createQuery(
    e: EnvUnowned,
    _: JObject,
    serialized_query: JByteArray,
    scan_partitions: JObjectArray,
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
        // `local[N]` resolves to N cores here.
        let slots = task_slots(
            settings.spark_config.get_usize(SPARK_EXECUTOR_CORES, 1),
            task_cpus as usize,
        );
        JVMClasses::init(env);
        let serialized_plan = env.convert_byte_array(serialized_query)?;
        let scan_partition_count = scan_partitions.len(env)?;
        let mut scan_partitions_vec = Vec::with_capacity(scan_partition_count);
        for i in 0..scan_partition_count {
            let bytes = scan_partitions.get_element(env, i)?;
            let bytes = unsafe { JByteArray::from_raw(&*env, bytes.into_raw()) };
            scan_partitions_vec.push(env.convert_byte_array(&bytes)?);
            // A scan can have more partitions than the local frame holds, so free each one.
            env.delete_local_ref(bytes);
        }
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
        let (schema, start) = local_plan(&context, partitions as usize, scan_partitions_vec)?;
        if schema.fields().len() != columns as usize {
            return Err(CometError::Internal(
                "Local output schema width mismatch".into(),
            ));
        }
        // Run at most as many partitions at once as Spark runs tasks. Comet's pools split Spark's
        // memory equally among the tasks running native plans, this one included, so with T of
        // them the partitions each get what a task would only while at most slots / T run.
        let query = LocalQuery::from_partitions(
            schema,
            partitions as usize,
            start,
            context.session_ctx.task_ctx(),
            slots,
            read_ahead_bytes.max(0) as usize,
        )
        .with_running_limit(Arc::new(move || slots / running_native_tasks().max(1)));
        let query = QueryHandle::start(query, &get_runtime())?;
        register(
            id,
            Entry {
                query,
                columns: columns as usize,
                validate: context.debug_native,
                context: Mutex::new(Some(context)),
            },
        )?;
        Ok(id)
    })
}

const MAX_PARTITIONS: jint = 1024;

/// The tasks Spark runs at once with `cores`, as `LocalSchedulerBackend` counts them.
fn task_slots(cores: usize, task_cpus: usize) -> usize {
    (cores / task_cpus.max(1)).max(1)
}

/// One graph for the whole query, planned a partition at a time. Each partition's operators are
/// created by the planner for that partition, as a Spark task would create them, so per-partition
/// state such as a range slice or a partition-dependent expression is correct. A partition is
/// planned only when it starts, as a Spark task plans only when it runs, so the plans held at once,
/// and whatever each copies from the serialized plan, are those of the running partitions.
///
/// A file scan's partition reads the files in `scan_partitions` for that partition, which are
/// injected into a copy of the plan when it is planned, as a Spark task injects them before it
/// creates its plan. Only the files of all partitions are held for the whole query.
///
/// Partition 0 is planned here, for the output schema and so that a plan the planner rejects
/// fails the query before it starts, and that plan is the one partition 0 runs.
fn local_plan(
    context: &ExecutionContext,
    partitions: usize,
    scan_partitions: Vec<Vec<u8>>,
) -> CometResult<(SchemaRef, PartitionStart)> {
    let planner = context.planner(0);
    let spark_plan = Arc::new(context.spark_plan.clone());
    let files = Arc::new(ScanFiles::new(&spark_plan, scan_partitions, partitions)?);
    let first = plan_partition(&planner, &spark_plan, &files, 0, partitions)?;
    let schema = first.schema();
    let expected = Arc::clone(&schema);
    let first = Mutex::new(Some(first));
    let start: PartitionStart = Arc::new(move |partition, task_context| {
        let planned = if partition == 0 {
            first.lock().take()
        } else {
            None
        };
        let plan = match planned {
            Some(plan) => plan,
            None => plan_partition(&planner, &spark_plan, &files, partition, partitions)?,
        };
        if plan.schema() != expected {
            return Err(DataFusionError::Internal(format!(
                "Local query partition {partition} has a different output schema"
            )));
        }
        plan.execute(0, task_context)
    });
    Ok((schema, start))
}

/// The files each partition of a plan's native file scan reads, if it has one.
struct ScanFiles {
    /// Each partition's serialized `NativeScan`, which carries only its files.
    partitions: Vec<Vec<u8>>,
}

impl ScanFiles {
    fn new(
        spark_plan: &Operator,
        partitions: Vec<Vec<u8>>,
        partition_count: usize,
    ) -> CometResult<Self> {
        let scans = count_scans_without_files(spark_plan);
        let valid = if partitions.is_empty() {
            scans == 0
        } else {
            scans == 1 && partitions.len() == partition_count
        };
        if !valid {
            return Err(CometError::Internal(format!(
                "Local query has {scans} file scans without files and file lists for {} of its \
                 {partition_count} partitions",
                partitions.len()
            )));
        }
        Ok(Self { partitions })
    }

    /// `spark_plan` with the files of `partition` in its file scan, or `spark_plan` itself
    /// when it has no file scan.
    fn plan_for(&self, spark_plan: &Arc<Operator>, partition: usize) -> CometResult<Arc<Operator>> {
        let Some(bytes) = self.partitions.get(partition) else {
            return Ok(Arc::clone(spark_plan));
        };
        let files = NativeScan::decode(bytes.as_slice())
            .map_err(|e| CometError::Internal(format!("Invalid local scan partition: {e}")))?
            .file_partition
            .ok_or_else(|| CometError::Internal("Local scan partition has no files".into()))?;
        let mut plan = Operator::clone(spark_plan);
        inject_files(&mut plan, files);
        Ok(Arc::new(plan))
    }
}

fn count_scans_without_files(op: &Operator) -> usize {
    let own = match &op.op_struct {
        Some(OpStruct::NativeScan(scan)) if scan.file_partition.is_none() => 1,
        _ => 0,
    };
    own + op
        .children
        .iter()
        .map(count_scans_without_files)
        .sum::<usize>()
}

/// Gives the plan's one file scan without files `files`. [`ScanFiles::new`] checked that it
/// has exactly one.
fn inject_files(op: &mut Operator, files: SparkFilePartition) -> Option<SparkFilePartition> {
    if let Some(OpStruct::NativeScan(scan)) = &mut op.op_struct {
        if scan.file_partition.is_none() {
            scan.file_partition = Some(files);
            return None;
        }
    }
    let mut files = Some(files);
    for child in &mut op.children {
        files = inject_files(child, files?);
    }
    files
}

/// The operators of `partition`, whose single output partition is that partition of the graph.
fn plan_partition(
    planner: &PhysicalPlanner,
    spark_plan: &Arc<Operator>,
    files: &ScanFiles,
    partition: usize,
    partitions: usize,
) -> CometResult<Arc<dyn ExecutionPlan>> {
    let spark_plan = files.plan_for(spark_plan, partition)?;
    let (scans, shuffle_scans, root) = planner.for_partition(partition as i32).create_plan(
        &spark_plan,
        &mut vec![],
        partitions,
    )?;
    if !scans.is_empty()
        || !shuffle_scans.is_empty()
        || root.native_plan.output_partitioning().partition_count() != 1
    {
        return Err(CometError::Internal(
            "Local query plans must have no JVM input and one output partition".into(),
        ));
    }
    Ok(Arc::clone(&root.native_plan))
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
        QueryPoll::Batch(batch) => prepare_output(env, arrays, schemas, batch, entry.validate),
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
        // A failed query stays until the JVM closes it, which it does right after reading
        // `failedPartition`. A failed stream has already dropped its partitions.
        if matches!(result, Ok(-1)) {
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

/// The partition whose turn it is, which after a failure is the failed partition, or -1 once
/// the query is closed.
#[no_mangle]
pub extern "system" fn Java_org_apache_comet_local_NativeLocal_failedPartition(
    e: EnvUnowned,
    _: JObject,
    id: jlong,
) -> jint {
    try_unwrap_or_throw(&e, |_| {
        Ok(QUERIES
            .lock()
            .get(&id)
            .map_or(-1, |entry| entry.query.partition() as jint))
    })
}

#[no_mangle]
pub extern "system" fn Java_org_apache_comet_local_NativeLocal_activeQueries(
    e: EnvUnowned,
    _: JObject,
) -> jlong {
    try_unwrap_or_throw(&e, |_| Ok(QUERIES.lock().len() as jlong))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs::File;
    use std::sync::Arc;

    use arrow::array::{Array, Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::common::collect;
    use datafusion::prelude::SessionContext;
    use datafusion_comet_proto::spark_expression::{
        data_type::DataTypeId, DataType as SparkDataType,
    };
    use datafusion_comet_proto::spark_operator::operator::OpStruct;
    use datafusion_comet_proto::spark_operator::{
        NativeScan, NativeScanCommon, Operator, SparkFilePartition, SparkPartitionedFile,
        SparkStructField,
    };
    use parquet::arrow::ArrowWriter;
    use prost::Message;

    use super::{plan_partition, task_slots, ScanFiles};
    use crate::execution::planner::PhysicalPlanner;

    /// Writes `ids` to a Parquet file and returns the file as a Spark partitioned file.
    fn parquet_file(dir: &std::path::Path, name: &str, ids: &[i64]) -> SparkPartitionedFile {
        let path = dir.join(name);
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(ids.to_vec()))],
        )
        .unwrap();
        let mut writer = ArrowWriter::try_new(File::create(&path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let size = std::fs::metadata(&path).unwrap().len() as i64;
        SparkPartitionedFile {
            file_path: format!("file://{}", path.display()),
            start: 0,
            length: size,
            file_size: size,
            partition_values: vec![],
        }
    }

    fn scan() -> Operator {
        let id = SparkStructField {
            name: "id".to_string(),
            data_type: Some(SparkDataType {
                type_id: DataTypeId::Int64 as i32,
                type_info: None,
            }),
            nullable: false,
            metadata: HashMap::new(),
        };
        Operator {
            op_struct: Some(OpStruct::NativeScan(NativeScan {
                common: Some(NativeScanCommon {
                    required_schema: vec![id.clone()],
                    data_schema: vec![id],
                    projection_vector: vec![0],
                    session_timezone: "UTC".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            })),
            ..Default::default()
        }
    }

    /// A partition's serialized `NativeScan` carrying only its files, as the JVM sends it.
    fn files(partitioned_file: Vec<SparkPartitionedFile>) -> Vec<u8> {
        NativeScan {
            file_partition: Some(SparkFilePartition { partitioned_file }),
            ..Default::default()
        }
        .encode_to_vec()
    }

    #[tokio::test]
    async fn each_partition_reads_its_own_files() {
        let dir = tempfile::tempdir().unwrap();
        let groups = vec![
            vec![
                parquet_file(dir.path(), "a.parquet", &[1, 2]),
                parquet_file(dir.path(), "b.parquet", &[3]),
            ],
            vec![],
            vec![parquet_file(dir.path(), "c.parquet", &[4, 5, 6])],
        ];
        let plan = Arc::new(scan());
        let scan_files = ScanFiles::new(&plan, groups.into_iter().map(files).collect(), 3).unwrap();
        let session = Arc::new(SessionContext::new());
        let planner = PhysicalPlanner::new(Arc::clone(&session), 0);
        let mut ids = vec![];
        for partition in 0..3 {
            let native = plan_partition(&planner, &plan, &scan_files, partition, 3).unwrap();
            let batches = collect(native.execute(0, session.task_ctx()).unwrap())
                .await
                .unwrap();
            let read: Vec<i64> = batches
                .iter()
                .flat_map(|batch| {
                    let column = batch.column(0);
                    let values = column.as_any().downcast_ref::<Int64Array>().unwrap();
                    values.values().to_vec()
                })
                .collect();
            ids.push(read);
        }
        assert_eq!(ids, vec![vec![1, 2, 3], vec![], vec![4, 5, 6]]);
        // The injected files live only in each partition's copy.
        match &plan.op_struct {
            Some(OpStruct::NativeScan(scan)) => assert!(scan.file_partition.is_none()),
            _ => unreachable!(),
        }
    }

    #[test]
    fn file_lists_must_match_the_plan_and_its_partitions() {
        let plan = scan();
        let error = |result: super::CometResult<ScanFiles>| result.err().unwrap().to_string();
        // A file scan needs a file list for every partition.
        assert!(error(ScanFiles::new(&plan, vec![], 2)).contains("1 file scans"));
        assert!(error(ScanFiles::new(&plan, vec![files(vec![])], 2)).contains("for 1 of its 2"));
        // A plan without a file scan takes none.
        let range = Operator::default();
        assert!(ScanFiles::new(&range, vec![], 2).is_ok());
        assert!(error(ScanFiles::new(&range, vec![files(vec![])], 1)).contains("0 file scans"));
        // Nor does a plan whose scan already carries its files.
        let mut injected = scan();
        super::inject_files(&mut injected, SparkFilePartition::default());
        assert!(ScanFiles::new(&injected, vec![], 1).is_ok());
    }

    #[test]
    fn task_slots_divide_cores_by_task_cpus() {
        assert_eq!(task_slots(32, 16), 2);
        assert_eq!(task_slots(8, 3), 2);
        assert_eq!(task_slots(4, 1), 4);
        assert_eq!(task_slots(2, 4), 1);
        assert_eq!(task_slots(4, 0), 4);
    }
}
