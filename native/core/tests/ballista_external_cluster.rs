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
// Distributes a Comet plan across a REAL external Ballista cluster: a separate
// `comet-scheduler` process and a separate `comet-executor`
// process, each spawned as a child of this test. This is unlike
// `ballista_distributed.rs`, which runs an *in-process* standalone cluster
// (scheduler + executor threads inside the test).
//
// The point it proves:
//   1. The two Comet-flavored binaries (which inject Comet's extension codecs,
//      unlike the stock Ballista CLIs) start, and the executor registers.
//   2. A two-stage Comet plan (`CometFragment(NativeScan) -> hash-shuffle ->
//      CometFragment(Filter over a Scan)`) submitted to the external scheduler
//      is split into two stages, shipped to the *separate* executor process,
//      reconstructed there via the codecs, and executed — returning correct
//      results across the process boundary.
//   3. Crucially, the executor is a plain Rust process with NO running JVM (only
//      `libjvm` on the loader path). The Comet fragments must execute there
//      without a "JAVA_VM not initialized" panic. A childless `NativeScan`
//      fragment reads Parquet directly and never touches `JAVA_VM`, so this
//      should hold — this test is the first proof of it in a *separate* process.
//
// Spawns child processes and binds ports, so it is `#[ignore]`. Run explicitly:
//   export DYLD_LIBRARY_PATH="$JAVA_HOME/lib/server:$DYLD_LIBRARY_PATH"
//   cargo test -p datafusion-comet --features ballista \
//       --test ballista_external_cluster -- --ignored --nocapture

#![cfg(feature = "ballista")]

use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::Arc;
use std::time::{Duration, Instant};

use datafusion::arrow::array::{Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Schema, SchemaRef};
use datafusion::parquet::arrow::ArrowWriter;
use prost::Message;

use comet::execution::ballista::execute_two_stage;
use datafusion_comet_proto::spark_expression::{
    data_type::DataTypeId, expr::ExprStruct, literal, BinaryExpr, BoundReference, DataType, Expr,
    Literal,
};
use datafusion_comet_proto::spark_operator::{
    operator::OpStruct, Filter, NativeScan, NativeScanCommon, Operator, Scan, SparkFilePartition,
    SparkPartitionedFile, SparkStructField,
};

// Non-default ports so this test does not collide with a real cluster on the
// usual 50050/50051/50052.
const SCHEDULER_PORT: u16 = 51050;
const EXECUTOR_FLIGHT_PORT: u16 = 51051;
const EXECUTOR_GRPC_PORT: u16 = 51052;

fn int32_type() -> DataType {
    DataType {
        type_id: DataTypeId::Int32 as i32,
        type_info: None,
    }
}

fn int32_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "a",
        ArrowDataType::Int32,
        true,
    )]))
}

/// Write a tiny Parquet file with a single int32 column `a` = [1..=5].
fn write_test_parquet(path: &std::path::Path) -> anyhow::Result<()> {
    let schema = int32_schema();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))],
    )?;
    let file = std::fs::File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

/// block1: a childless `NativeScan` fragment over `parquet_path` (int32 `a`).
/// This is the JVM-less leaf — it reads Parquet directly, no `JAVA_VM`.
fn build_native_scan_proto(parquet_path: &std::path::Path) -> anyhow::Result<Vec<u8>> {
    let field_a = SparkStructField {
        name: "a".to_string(),
        data_type: Some(int32_type()),
        nullable: true,
        metadata: Default::default(),
    };
    let common = NativeScanCommon {
        required_schema: vec![field_a.clone()],
        data_schema: vec![field_a],
        projection_vector: vec![0],
        session_timezone: "UTC".to_string(),
        source: "comet-external-cluster-native-scan".to_string(),
        ..Default::default()
    };
    let file_size = std::fs::metadata(parquet_path)?.len() as i64;
    let partitioned_file = SparkPartitionedFile {
        file_path: format!("file://{}", parquet_path.display()),
        start: 0,
        length: file_size,
        file_size,
        partition_values: vec![],
    };
    let native_scan = NativeScan {
        common: Some(common),
        file_partition: Some(SparkFilePartition {
            partitioned_file: vec![partitioned_file],
        }),
    };
    Ok(Operator {
        children: vec![],
        plan_id: 0,
        op_struct: Some(OpStruct::NativeScan(native_scan)),
    }
    .encode_to_vec())
}

/// block2: `Filter(col0 > 2)` over a `Scan` (#100) input leaf, fed by the shuffle
/// reader. Keeps a > 2, i.e. rows {3, 4, 5}.
fn build_filter_over_scan_proto() -> Vec<u8> {
    let scan = Scan {
        fields: vec![int32_type()],
        source: "comet-external-cluster-shuffle-scan".to_string(),
    };
    let scan_op = Operator {
        children: vec![],
        plan_id: 1,
        op_struct: Some(OpStruct::Scan(scan)),
    };

    let col0 = Expr {
        expr_struct: Some(ExprStruct::Bound(BoundReference {
            index: 0,
            datatype: Some(int32_type()),
        })),
        ..Default::default()
    };
    let lit2 = Expr {
        expr_struct: Some(ExprStruct::Literal(Literal {
            value: Some(literal::Value::IntVal(2)),
            datatype: Some(int32_type()),
            is_null: false,
        })),
        ..Default::default()
    };
    let predicate = Expr {
        expr_struct: Some(ExprStruct::Gt(Box::new(BinaryExpr {
            left: Some(Box::new(col0)),
            right: Some(Box::new(lit2)),
        }))),
        ..Default::default()
    };
    Operator {
        children: vec![scan_op],
        plan_id: 2,
        op_struct: Some(OpStruct::Filter(Filter {
            predicate: Some(predicate),
        })),
    }
    .encode_to_vec()
}

/// Kills the spawned child processes on drop, so a panicking assertion still
/// tears the external cluster down.
struct ClusterGuard {
    children: Vec<(&'static str, Child)>,
}

impl Drop for ClusterGuard {
    fn drop(&mut self) {
        for (name, child) in self.children.iter_mut() {
            let _ = child.kill();
            let _ = child.wait();
            eprintln!("[harness] stopped {name}");
        }
    }
}

/// The `libjvm` directory (`$JAVA_HOME/lib/server`) so the spawned binaries can
/// load `libjvm` (present, not a running JVM). Inherited env usually already has
/// it, but we set it explicitly to be robust to macOS DYLD stripping.
fn dyld_path() -> Option<String> {
    let java_home = std::env::var("JAVA_HOME").ok()?;
    let lib = PathBuf::from(&java_home).join("lib").join("server");
    let existing = std::env::var("DYLD_LIBRARY_PATH").unwrap_or_default();
    Some(if existing.is_empty() {
        lib.display().to_string()
    } else {
        format!("{}:{}", lib.display(), existing)
    })
}

/// Poll a TCP port until it accepts a connection (the process is listening) or
/// the deadline passes.
fn wait_for_port(port: u16, what: &str, timeout: Duration) -> anyhow::Result<()> {
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse()?;
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_ok() {
            eprintln!("[harness] {what} is listening on {port}");
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(150));
    }
    anyhow::bail!("timed out waiting for {what} on port {port}")
}

#[ignore = "spawns external scheduler + executor processes and binds ports; run explicitly"]
#[test]
fn comet_plan_on_external_cluster() -> anyhow::Result<()> {
    let scheduler_bin = env!("CARGO_BIN_EXE_comet-scheduler");
    let executor_bin = env!("CARGO_BIN_EXE_comet-executor");
    let dyld = dyld_path();

    // --- 1. Spawn the external scheduler process ---
    let mut scheduler_cmd = Command::new(scheduler_bin);
    scheduler_cmd
        .env("COMET_BALLISTA_SCHEDULER_BIND_HOST", "127.0.0.1")
        .env(
            "COMET_BALLISTA_SCHEDULER_BIND_PORT",
            SCHEDULER_PORT.to_string(),
        );
    if let Some(ref d) = dyld {
        scheduler_cmd.env("DYLD_LIBRARY_PATH", d);
    }
    let scheduler = scheduler_cmd.spawn()?;
    let mut guard = ClusterGuard {
        children: vec![("comet-scheduler", scheduler)],
    };
    wait_for_port(SCHEDULER_PORT, "scheduler", Duration::from_secs(30))?;

    // --- 2. Spawn the external executor process (separate, JVM-less) ---
    let mut executor_cmd = Command::new(executor_bin);
    executor_cmd
        .env("COMET_BALLISTA_EXECUTOR_BIND_HOST", "127.0.0.1")
        .env(
            "COMET_BALLISTA_EXECUTOR_PORT",
            EXECUTOR_FLIGHT_PORT.to_string(),
        )
        .env(
            "COMET_BALLISTA_EXECUTOR_GRPC_PORT",
            EXECUTOR_GRPC_PORT.to_string(),
        )
        .env("COMET_BALLISTA_SCHEDULER_HOST", "127.0.0.1")
        .env("COMET_BALLISTA_SCHEDULER_PORT", SCHEDULER_PORT.to_string())
        .env("COMET_BALLISTA_EXECUTOR_CONCURRENT_TASKS", "4");
    if let Some(ref d) = dyld {
        executor_cmd.env("DYLD_LIBRARY_PATH", d);
    }
    let executor = executor_cmd.spawn()?;
    guard.children.push(("comet-executor", executor));
    wait_for_port(
        EXECUTOR_FLIGHT_PORT,
        "executor flight",
        Duration::from_secs(30),
    )?;
    wait_for_port(EXECUTOR_GRPC_PORT, "executor grpc", Duration::from_secs(30))?;
    // Grace for the executor to complete registration with the scheduler.
    std::thread::sleep(Duration::from_secs(3));

    // --- 3. Build the two-stage Comet plan protos ---
    let parquet = std::env::temp_dir().join("comet_external_cluster.parquet");
    write_test_parquet(&parquet)?;
    let block1 = build_native_scan_proto(&parquet)?; // NativeScan a=[1..5]
    let block2 = build_filter_over_scan_proto(); // Filter(a > 2)

    // --- 4. Submit to the EXTERNAL scheduler (non-empty URL => remote path) ---
    let scheduler_url = format!("http://127.0.0.1:{SCHEDULER_PORT}");
    eprintln!("[harness] submitting two-stage Comet plan to {scheduler_url}");
    let (schema, batches) = execute_two_stage(
        &block1,
        &block2,
        /* num_group_keys */ 1,
        /* num_partitions */ 4,
        &scheduler_url,
    )
    .map_err(|e| anyhow::anyhow!("external submission failed: {e}"))?;

    // --- 5. Verify correctness across the process boundary ---
    let mut values: Vec<i32> = Vec::new();
    for batch in &batches {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 column");
        values.extend(col.values().iter().copied());
    }
    values.sort_unstable();
    eprintln!(
        "[harness] external cluster returned {} rows: {:?} (schema: {:?})",
        values.len(),
        values,
        schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
    );

    assert_eq!(
        values,
        vec![3, 4, 5],
        "distributed Comet plan on the external cluster must return {{3,4,5}} (a > 2)"
    );

    eprintln!(
        "PASS: a distributed Comet plan ran on a SEPARATE scheduler+executor process pair \
         (JVM-less executor) and returned correct results"
    );

    // guard drops here, tearing down both child processes.
    drop(guard);
    let _ = schema;
    Ok(())
}
