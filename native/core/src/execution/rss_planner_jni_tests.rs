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

use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::task::Poll;
use std::time::{Duration, Instant};

use arrow::array::Int32Array;
use datafusion::common::DataFusionError;
use datafusion::prelude::SessionContext;
use datafusion_comet_proto::spark_expression;
use datafusion_comet_proto::spark_operator::{operator::OpStruct, Operator, Scan, ShuffleWriter};
use datafusion_comet_proto::spark_partitioning::{
    partition_writer::PartitionWriterStruct, partitioning::PartitioningStruct, PartitionWriter,
    Partitioning, RssPartitionWriter, SinglePartition,
};
use futures::{poll, StreamExt};
use jni::objects::{Global, JByteArray, JObject, JValue};
use jni::{Env, EnvUnowned, InitArgsBuilder, JNIVersion, JavaVM};

use super::{
    parse_memory_pool_config, register_rss_partition_pusher, ExecutionContext,
    Java_org_apache_comet_Native_registerRssPartitionPusher,
};
use crate::errors::{CometError, CometResult};
use crate::execution::operators::InputBatch;
use crate::execution::planner::{PhysicalPlanner, RegisteredShufflePusher};
use crate::execution::shuffle::read_ipc_compressed;

const FIRST_HANDLE: i64 = 41;
const SECOND_HANDLE: i64 = 42;
const TINY_FRAME_HANDLE: i64 = 43;
const MAX_FRAME_BYTES: i32 = 1024 * 1024;

fn rss_plan(handle: i64) -> Operator {
    Operator {
        children: vec![Operator {
            op_struct: Some(OpStruct::Scan(Scan {
                fields: vec![spark_expression::DataType {
                    type_id: 3,
                    type_info: None,
                }],
                source: "task-owned RSS pusher test".to_string(),
            })),
            ..Default::default()
        }],
        op_struct: Some(OpStruct::ShuffleWriter(ShuffleWriter {
            partitioning: Some(Partitioning {
                partitioning_struct: Some(PartitioningStruct::SinglePartition(SinglePartition {})),
            }),
            partition_writer: Some(PartitionWriter {
                partition_writer_struct: Some(PartitionWriterStruct::RssPartitionWriter(
                    RssPartitionWriter {
                        rss_partition_pusher: handle,
                    },
                )),
            }),
            write_buffer_size: 1024,
            ..Default::default()
        })),
        ..Default::default()
    }
}

fn execution_context(
    env: &mut Env,
    spark_plan: Operator,
    task_attempt_id: i64,
) -> CometResult<ExecutionContext> {
    let metrics = env.new_object(jni::jni_str!("java/lang/Object"), jni::jni_sig!("()V"), &[])?;

    Ok(ExecutionContext {
        id: task_attempt_id,
        task_attempt_id,
        spark_plan,
        partition_count: 1,
        root_op: None,
        scans: vec![],
        shuffle_scans: vec![],
        input_sources: vec![],
        stream: None,
        batch_receiver: None,
        metrics: Arc::new(env.new_global_ref(&metrics)?),
        metrics_update_interval: None,
        metrics_last_update_time: Instant::now(),
        poll_count_since_metrics_check: 0,
        plan_creation_time: Duration::ZERO,
        session_ctx: Arc::new(SessionContext::new()),
        debug_native: false,
        explain_native: false,
        memory_pool_config: parse_memory_pool_config(false, "unbounded".to_string(), 0, 0)?,
        tracing_enabled: false,
        rust_thread_id: 0,
        tracing_memory_metric_name: String::new(),
        tracing_event_name: String::new(),
        task_context: None,
        class_loader: None,
        memory_pool_registration: None,
        rss_pusher: None,
    })
}

fn assert_rejected_plan(plan: &Operator, registration: Option<RegisteredShufflePusher>) {
    let rejected = PhysicalPlanner::default()
        .with_rss_pusher(registration)
        .create_plan(plan, &mut vec![], 1);
    assert!(
        rejected.is_err(),
        "invalid task-owned RSS plan was accepted"
    );
}

fn execute_registered_plan(
    context: &ExecutionContext,
    values: &[i32],
) -> Result<(), DataFusionError> {
    let planner = PhysicalPlanner::new(Arc::clone(&context.session_ctx), 0)
        .with_rss_pusher(context.rss_pusher.clone());
    let (mut scans, _, plan) = planner
        .create_plan(&context.spark_plan, &mut vec![], 1)
        .expect("registered RSS plan should be executable");
    scans[0].set_input_batch(InputBatch::Batch(
        vec![Arc::new(Int32Array::from(values.to_vec()))],
        values.len(),
    ));

    let mut stream = plan
        .native_plan
        .execute(0, context.session_ctx.task_ctx())?;
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let mut eof_sent = false;
        loop {
            match poll!(stream.next()) {
                Poll::Ready(Some(Ok(_))) => panic!("RSS shuffle writer must not produce batches"),
                Poll::Ready(Some(Err(error))) => return Err(error),
                Poll::Ready(None) => return Ok(()),
                Poll::Pending if !eof_sent => {
                    scans[0].set_input_batch(InputBatch::EOF);
                    eof_sent = true;
                }
                Poll::Pending => tokio::task::yield_now().await,
            }
        }
    })
}

fn calls(vm: &JavaVM, fixture: &Global<JObject<'static>>) -> i32 {
    vm.attach_current_thread(|env| -> jni::errors::Result<i32> {
        env.get_field(fixture, jni::jni_str!("calls"), jni::jni_sig!("I"))?
            .i()
    })
    .unwrap()
}

#[test]
#[cfg_attr(miri, ignore)] // Miri cannot launch a JVM.
fn task_owned_rss_plan_pushes_complete_frames_to_java() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let classes = tempfile::tempdir().unwrap();
    let javac = std::env::var_os("JAVA_HOME")
        .map(|java_home| PathBuf::from(java_home).join("bin/javac"))
        .unwrap_or_else(|| PathBuf::from("javac"));
    let output =
        Command::new(javac)
            .arg("-d")
            .arg(classes.path())
            .arg(manifest.join(
                "../../spark/src/main/java/org/apache/comet/shuffle/ShufflePartitionPusher.java",
            ))
            .arg(manifest.join("../jni-bridge/tests/java/RecordingShufflePartitionPusher.java"))
            .output()
            .expect("compile the task-owned RSS Java callback fixture");
    assert!(
        output.status.success(),
        "javac: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let vm = JavaVM::new(
        InitArgsBuilder::new()
            .version(JNIVersion::V1_8)
            .option("-Xcheck:jni")
            .option(format!("-Djava.class.path={}", classes.path().display()))
            .build()
            .unwrap(),
    )
    .unwrap();

    let (first_context, second_context, tiny_context, first_fixture, second_fixture, tiny_fixture) =
        vm.attach_current_thread(|env| -> CometResult<_> {
            let first = env.new_object(
                jni::jni_str!("org/apache/comet/shuffle/RecordingShufflePartitionPusher"),
                jni::jni_sig!("()V"),
                &[],
            )?;
            let second = env.new_object(
                jni::jni_str!("org/apache/comet/shuffle/RecordingShufflePartitionPusher"),
                jni::jni_sig!("()V"),
                &[],
            )?;
            let tiny = env.new_object(
                jni::jni_str!("org/apache/comet/shuffle/RecordingShufflePartitionPusher"),
                jni::jni_sig!("()V"),
                &[],
            )?;
            let wrong_type =
                env.new_object(jni::jni_str!("java/lang/Object"), jni::jni_sig!("()V"), &[])?;

            let mut first_context = execution_context(env, rss_plan(FIRST_HANDLE), 101)?;
            for (handle, partitions, max_bytes) in [
                (0, 1, MAX_FRAME_BYTES),
                (-1, 1, MAX_FRAME_BYTES),
                (FIRST_HANDLE, 0, MAX_FRAME_BYTES),
                (FIRST_HANDLE, -1, MAX_FRAME_BYTES),
                (FIRST_HANDLE, 1, 0),
                (FIRST_HANDLE, 1, -1),
            ] {
                assert!(register_rss_partition_pusher(
                    env,
                    &mut first_context,
                    handle,
                    &first,
                    partitions,
                    max_bytes,
                )
                .is_err());
                assert!(first_context.rss_pusher.is_none());
            }
            assert!(register_rss_partition_pusher(
                env,
                &mut first_context,
                FIRST_HANDLE,
                &JObject::null(),
                1,
                MAX_FRAME_BYTES,
            )
            .is_err());
            assert!(register_rss_partition_pusher(
                env,
                &mut first_context,
                FIRST_HANDLE,
                &wrong_type,
                1,
                MAX_FRAME_BYTES,
            )
            .is_err());
            assert!(first_context.rss_pusher.is_none());

            let registration_class = env.find_class(jni::jni_str!("java/lang/Object"))?;
            let callback = env.new_local_ref(&first)?;
            // SAFETY: this is the current thread's live JNI attachment; a null context must be
            // rejected at the exported boundary without dereferencing it.
            unsafe {
                Java_org_apache_comet_Native_registerRssPartitionPusher(
                    EnvUnowned::from_raw(env.get_raw()),
                    registration_class,
                    0,
                    FIRST_HANDLE,
                    callback,
                    1,
                    MAX_FRAME_BYTES,
                );
            }
            let null_context_exception = env
                .exception_occurred()
                .expect("null execution context must raise a Java exception");
            env.exception_clear();
            let null_pointer_exception =
                env.find_class(jni::jni_str!("java/lang/NullPointerException"))?;
            assert!(env.is_instance_of(&null_context_exception, &null_pointer_exception)?);
            assert!(first_context.rss_pusher.is_none());

            let registration_class = env.find_class(jni::jni_str!("java/lang/Object"))?;
            let callback = env.new_local_ref(&first)?;
            let context_address = &mut first_context as *mut ExecutionContext as i64;
            // SAFETY: the JNI attachment, callback, and execution context all remain live for
            // the synchronous exported registration call.
            unsafe {
                Java_org_apache_comet_Native_registerRssPartitionPusher(
                    EnvUnowned::from_raw(env.get_raw()),
                    registration_class,
                    context_address,
                    FIRST_HANDLE,
                    callback,
                    1,
                    MAX_FRAME_BYTES,
                );
            }
            assert!(
                !env.exception_check(),
                "valid RSS pusher registration raised a Java exception"
            );
            assert!(register_rss_partition_pusher(
                env,
                &mut first_context,
                SECOND_HANDLE,
                &second,
                1,
                MAX_FRAME_BYTES,
            )
            .is_err());
            assert_eq!(
                first_context.rss_pusher.as_ref().unwrap().handle,
                FIRST_HANDLE
            );

            let mut second_context = execution_context(env, rss_plan(SECOND_HANDLE), 102)?;
            register_rss_partition_pusher(
                env,
                &mut second_context,
                SECOND_HANDLE,
                &second,
                1,
                MAX_FRAME_BYTES,
            )?;

            // The JNI bridge accepts a positive byte limit, but the destination writer rejects
            // anything too small to contain the mandatory 20-byte Comet frame header.
            let mut tiny_context = execution_context(env, rss_plan(TINY_FRAME_HANDLE), 103)?;
            register_rss_partition_pusher(env, &mut tiny_context, TINY_FRAME_HANDLE, &tiny, 1, 19)?;

            Ok((
                first_context,
                second_context,
                tiny_context,
                env.new_global_ref(&first)?,
                env.new_global_ref(&second)?,
                env.new_global_ref(&tiny)?,
            ))
        })
        .unwrap();

    assert_rejected_plan(&first_context.spark_plan, None);
    assert_rejected_plan(&first_context.spark_plan, second_context.rss_pusher.clone());
    assert_rejected_plan(&second_context.spark_plan, first_context.rss_pusher.clone());
    for invalid_handle in [0, -1, SECOND_HANDLE] {
        assert_rejected_plan(&rss_plan(invalid_handle), first_context.rss_pusher.clone());
    }

    let mut wrong_partition_count = first_context.rss_pusher.clone().unwrap();
    wrong_partition_count.num_partitions = 2;
    assert_rejected_plan(&first_context.spark_plan, Some(wrong_partition_count));
    assert_rejected_plan(&tiny_context.spark_plan, tiny_context.rss_pusher.clone());

    // Registration cannot alter an execution context after its physical plan is initialized.
    let (_, _, planned_root) = PhysicalPlanner::new(Arc::clone(&first_context.session_ctx), 0)
        .with_rss_pusher(first_context.rss_pusher.clone())
        .create_plan(&first_context.spark_plan, &mut vec![], 1)
        .unwrap();
    let mut late_context = vm
        .attach_current_thread(|env| execution_context(env, rss_plan(FIRST_HANDLE), 104))
        .unwrap();
    late_context.root_op = Some(planned_root);
    vm.attach_current_thread(|env| -> CometResult<()> {
        let error = register_rss_partition_pusher(
            env,
            &mut late_context,
            FIRST_HANDLE,
            first_fixture.as_obj(),
            1,
            MAX_FRAME_BYTES,
        )
        .expect_err("registration after physical planning must be rejected");
        assert!(error.to_string().contains("before execution"), "{error}");
        assert!(late_context.rss_pusher.is_none());
        Ok(())
    })
    .unwrap();

    assert_eq!(calls(&vm, &first_fixture), 0);
    assert_eq!(calls(&vm, &second_fixture), 0);
    assert_eq!(calls(&vm, &tiny_fixture), 0);

    let expected_values = [11, 29, 47];
    execute_registered_plan(&first_context, &expected_values).unwrap();
    assert_eq!(calls(&vm, &first_fixture), 1);
    assert_eq!(calls(&vm, &second_fixture), 0);
    assert_eq!(calls(&vm, &tiny_fixture), 0);

    let frame = vm
        .attach_current_thread(|env| -> jni::errors::Result<Vec<u8>> {
            assert_eq!(
                env.get_field(
                    &first_fixture,
                    jni::jni_str!("partitionId"),
                    jni::jni_sig!("I"),
                )?
                .i()?,
                0
            );
            let bytes = env
                .get_field(
                    &first_fixture,
                    jni::jni_str!("lastBytes"),
                    jni::jni_sig!("[B"),
                )?
                .l()?;
            // SAFETY: the fixture field has descriptor byte[] and into_raw transfers its local ref.
            let bytes = unsafe { JByteArray::from_raw(env, bytes.into_raw()) };
            env.convert_byte_array(&bytes)
        })
        .unwrap();
    assert!(frame.len() >= 20);
    assert_eq!(
        u64::from_le_bytes(frame[..8].try_into().unwrap()) + 8,
        frame.len() as u64
    );
    assert_eq!(u64::from_le_bytes(frame[8..16].try_into().unwrap()), 1);
    let decoded = read_ipc_compressed(&frame[16..]).unwrap();
    let decoded_values = decoded
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(decoded_values.values().as_ref(), expected_values.as_slice());

    // A Java callback failure must survive RSS writing as the original throwable, not as a
    // flattened string or a newly constructed generic native exception.
    vm.attach_current_thread(|env| -> jni::errors::Result<()> {
        env.set_field(
            &second_fixture,
            jni::jni_str!("failureMode"),
            jni::jni_sig!("I"),
            JValue::Int(1),
        )
    })
    .unwrap();
    let error = execute_registered_plan(&second_context, &expected_values).unwrap_err();
    let DataFusionError::External(source) = error else {
        panic!("Java callback failure lost its typed DataFusion wrapper: {error}");
    };
    let Some(CometError::JavaException { throwable, .. }) = source.downcast_ref::<CometError>()
    else {
        panic!("Java callback failure lost its original throwable: {source}");
    };
    vm.attach_current_thread(|env| -> jni::errors::Result<()> {
        let expected = env
            .get_field(
                &second_fixture,
                jni::jni_str!("failure"),
                jni::jni_sig!("Ljava/io/IOException;"),
            )?
            .l()?;
        assert!(env.is_same_object(&expected, throwable)?);
        Ok(())
    })
    .unwrap();
    assert_eq!(calls(&vm, &first_fixture), 1);
    assert_eq!(calls(&vm, &second_fixture), 1);
    assert_eq!(calls(&vm, &tiny_fixture), 0);
}
