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

// JNI errors retain Java exception information; match the bridge crate's lint policy.
#![allow(clippy::result_large_err)]

use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use datafusion_comet_jni_bridge::errors::{CometError, CometResult};
use datafusion_comet_jni_bridge::shuffle_partition_pusher::JavaShufflePartitionPusher;
use jni::objects::{JByteArray, JObject, JValue};
use jni::{InitArgsBuilder, JNIVersion, JavaVM};

fn complete_frame() -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1, 2]))],
    )
    .unwrap();
    let mut ipc = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut ipc, schema.as_ref()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
    let mut frame = Vec::new();
    frame.extend_from_slice(&(12 + ipc.len() as u64).to_le_bytes());
    frame.extend_from_slice(&1_u64.to_le_bytes());
    frame.extend_from_slice(b"NONE");
    frame.extend_from_slice(&ipc);
    frame
}

#[test]
#[cfg_attr(miri, ignore)] // Miri cannot launch a JVM.
fn rss_jni_callback_contract() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let classes = tempfile::tempdir().unwrap();
    let javac = std::env::var_os("JAVA_HOME")
        .map(|home| PathBuf::from(home).join("bin/javac"))
        .unwrap_or_else(|| PathBuf::from("javac"));
    let output =
        Command::new(javac)
            .arg("-d")
            .arg(classes.path())
            .arg(manifest.join(
                "../../spark/src/main/java/org/apache/comet/shuffle/ShufflePartitionPusher.java",
            ))
            .arg(manifest.join("tests/java/RecordingShufflePartitionPusher.java"))
            .output()
            .expect("run javac for the RSS callback fixture");
    assert!(
        output.status.success(),
        "javac: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let args = InitArgsBuilder::new()
        .version(JNIVersion::V1_8)
        .option("-Xcheck:jni")
        .option(format!("-Djava.class.path={}", classes.path().display()))
        .build()
        .unwrap();
    let vm = JavaVM::new(args).unwrap();
    let frame = complete_frame();
    let (pusher, fixture) = vm
        .attach_current_thread(|env| -> CometResult<_> {
            let object = env.new_object(
                jni::jni_str!("org/apache/comet/shuffle/RecordingShufflePartitionPusher"),
                jni::jni_sig!("()V"),
                &[],
            )?;
            assert!(matches!(
                JavaShufflePartitionPusher::try_new(env, &JObject::null(), 2, frame.len()),
                Err(CometError::NullPointer(_))
            ));
            for (partitions, limit) in [
                (0, frame.len()),
                (i32::MAX as usize + 1, frame.len()),
                (2, 0),
                (2, i32::MAX as usize + 1),
            ] {
                assert!(matches!(
                    JavaShufflePartitionPusher::try_new(env, &object, partitions, limit),
                    Err(CometError::Config(_))
                ));
            }
            let wrong_type =
                env.new_object(jni::jni_str!("java/lang/Object"), jni::jni_sig!("()V"), &[])?;
            assert!(matches!(
                JavaShufflePartitionPusher::try_new(env, &wrong_type, 2, frame.len()),
                Err(CometError::Config(_))
            ));
            let pusher = JavaShufflePartitionPusher::try_new(env, &object, 2, frame.len())?;
            Ok((pusher, env.new_global_ref(&object)?))
        })
        .unwrap();

    // Global references and the cached method remain valid on another attached native thread.
    let worker_pusher = pusher.clone();
    let worker_frame = frame.clone();
    std::thread::spawn(move || {
        worker_pusher.reserve_partition_data(worker_frame.len())?;
        worker_pusher.push_partition_data(1, &worker_frame)
    })
    .join()
    .unwrap()
    .unwrap();

    vm.attach_current_thread(|env| -> jni::errors::Result<()> {
        assert_eq!(
            env.get_field(&fixture, jni::jni_str!("calls"), jni::jni_sig!("I"))?
                .i()?,
            1
        );
        assert_eq!(
            env.get_field(&fixture, jni::jni_str!("partitionId"), jni::jni_sig!("I"))?
                .i()?,
            1
        );
        assert_eq!(
            env.get_field(
                &fixture,
                jni::jni_str!("reservationCalls"),
                jni::jni_sig!("I"),
            )?
            .i()?,
            1
        );
        assert!(env
            .get_field(
                &fixture,
                jni::jni_str!("reservedBeforePush"),
                jni::jni_sig!("Z"),
            )?
            .z()?);
        let bytes = env
            .get_field(&fixture, jni::jni_str!("lastBytes"), jni::jni_sig!("[B"))?
            .l()?;
        // SAFETY: the field descriptor is byte[] and into_raw transfers this local reference.
        let bytes = unsafe { JByteArray::from_raw(env, bytes.into_raw()) };
        assert_eq!(env.convert_byte_array(&bytes)?, frame);
        Ok(())
    })
    .unwrap();

    assert!(pusher.push_partition_data(2, &frame).is_err());
    assert!(pusher.reserve_partition_data(0).is_err());
    assert!(pusher
        .reserve_partition_data(i32::MAX as usize + 1)
        .is_err());
    pusher.reserve_partition_data(frame.len() + 1).unwrap();
    vm.attach_current_thread(|env| -> jni::errors::Result<()> {
        assert_eq!(
            env.get_field(&fixture, jni::jni_str!("reservedBytes"), jni::jni_sig!("I"))?
                .i()?,
            frame.len() as i32 + 1
        );
        Ok(())
    })
    .unwrap();
    pusher.release_partition_data_reservation().unwrap();
    pusher.reserve_partition_data(frame.len()).unwrap();
    pusher.release_partition_data_reservation().unwrap();
    assert!(pusher.push_partition_data(0, &[]).is_err());
    assert!(pusher
        .push_partition_data(0, &vec![0; frame.len() + 1])
        .is_err());
    vm.attach_current_thread(|env| -> jni::errors::Result<()> {
        assert_eq!(
            env.get_field(&fixture, jni::jni_str!("calls"), jni::jni_sig!("I"))?
                .i()?,
            1
        );
        Ok(())
    })
    .unwrap();

    for adjustment in [-(frame.len() as i32), -1, 1] {
        vm.attach_current_thread(|env| -> jni::errors::Result<()> {
            env.set_field(
                &fixture,
                jni::jni_str!("adjustment"),
                jni::jni_sig!("I"),
                JValue::Int(adjustment),
            )
        })
        .unwrap();
        assert!(matches!(
            pusher.push_partition_data(0, &frame),
            Err(CometError::Internal(_))
        ));
    }

    vm.attach_current_thread(|env| -> jni::errors::Result<()> {
        env.set_field(
            &fixture,
            jni::jni_str!("failureMode"),
            jni::jni_sig!("I"),
            JValue::Int(1),
        )
    })
    .unwrap();
    let error = pusher.push_partition_data(0, &frame).unwrap_err();
    let CometError::JavaException { throwable, .. } = error else {
        panic!("original Java throwable was not preserved: {error:?}");
    };
    vm.attach_current_thread(|env| -> jni::errors::Result<()> {
        let expected = env
            .get_field(
                &fixture,
                jni::jni_str!("failure"),
                jni::jni_sig!("Ljava/io/IOException;"),
            )?
            .l()?;
        assert!(env.is_same_object(&expected, &throwable)?);
        Ok(())
    })
    .unwrap();
}
