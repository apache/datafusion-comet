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

//! A Comet-flavored Ballista **executor** process.
//!
//! Identical to the stock `ballista-executor` binary except that it registers
//! Comet's extension codecs ([`CometLogicalCodec`] / [`CometPhysicalCodec`]) on
//! the [`ExecutorProcessConfig`], so a `CometFragmentExec` shipped from the
//! scheduler is reconstructed here (via Comet's planner over the proto) and run.
//!
//! This is the process that actually **executes** Comet fragments. It links
//! `libcomet` (as an rlib) but is a plain Rust process with **no running JVM** —
//! only `libjvm` is on the loader path. Comet fragments whose leaf is a
//! self-contained `NativeScan` read Parquet directly and never touch `JAVA_VM`,
//! so they run here JVM-less; this binary is the first place that is proven in a
//! *separate* process rather than an in-process test.
//!
//! Only built with `--features ballista` (see `required-features` in
//! `core/Cargo.toml`).
//!
//! Configuration (all optional, env-driven so a harness can place it):
//! - `COMET_BALLISTA_EXECUTOR_BIND_HOST`  (default `127.0.0.1`)
//! - `COMET_BALLISTA_EXECUTOR_PORT`       (flight port, default `50051`)
//! - `COMET_BALLISTA_EXECUTOR_GRPC_PORT`  (default `50052`)
//! - `COMET_BALLISTA_SCHEDULER_HOST`      (default `localhost`)
//! - `COMET_BALLISTA_SCHEDULER_PORT`      (default `50050`)
//! - `COMET_BALLISTA_EXECUTOR_CONCURRENT_TASKS` (default: available parallelism)

use std::sync::Arc;

use ballista_executor::executor_process::{start_executor_process, ExecutorProcessConfig};

use comet::execution::ballista::{CometLogicalCodec, CometPhysicalCodec};

fn env_u16(key: &str, default: u16) -> u16 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bind_host = std::env::var("COMET_BALLISTA_EXECUTOR_BIND_HOST")
        .unwrap_or_else(|_| "127.0.0.1".to_string());
    let port = env_u16("COMET_BALLISTA_EXECUTOR_PORT", 50051);
    let grpc_port = env_u16("COMET_BALLISTA_EXECUTOR_GRPC_PORT", 50052);
    let scheduler_host =
        std::env::var("COMET_BALLISTA_SCHEDULER_HOST").unwrap_or_else(|_| "localhost".to_string());
    let scheduler_port = env_u16("COMET_BALLISTA_SCHEDULER_PORT", 50050);
    let concurrent_tasks = std::env::var("COMET_BALLISTA_EXECUTOR_CONCURRENT_TASKS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        });

    // Manual runtime so the default (feature-less) build needs no tokio `macros`.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    runtime.block_on(async move {
        let config = ExecutorProcessConfig {
            bind_host,
            port,
            grpc_port,
            scheduler_host,
            scheduler_port,
            concurrent_tasks,
            // The seam: Comet codecs so the executor can rebuild Comet fragments.
            override_logical_codec: Some(Arc::new(CometLogicalCodec::default())),
            override_physical_codec: Some(Arc::new(CometPhysicalCodec::default())),
            ..Default::default()
        };

        eprintln!(
            "[comet-ballista-executor] flight :{port} grpc :{grpc_port} -> scheduler {}:{}",
            config.scheduler_host, config.scheduler_port
        );

        start_executor_process(Arc::new(config)).await?;
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}
