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

//! A Comet executor: Ballista's executor, able to run the stages of Comet queries with Comet's
//! planner, operators and expressions. It needs no JVM.
//!
//! Configured through environment variables, with Ballista's defaults otherwise:
//! - `COMET_EXECUTOR_BIND_HOST` (default `127.0.0.1`)
//! - `COMET_EXECUTOR_EXTERNAL_HOST`: the host that drivers and other executors reach this
//!   executor at (default: the address the scheduler sees it connect from)
//! - `COMET_EXECUTOR_PORT`: the Flight port that serves shuffle and result data (default `50051`)
//! - `COMET_EXECUTOR_GRPC_PORT` (default `50052`)
//! - `COMET_SCHEDULER_HOST` (default `localhost`) and `COMET_SCHEDULER_PORT` (default `50050`)
//! - `COMET_EXECUTOR_TASKS`: concurrent tasks (default: the number of CPUs)
//! - `COMET_EXECUTOR_WORK_DIR`: where shuffle and spill files go (default: a temporary directory)

use std::sync::Arc;

use ballista_executor::executor_process::{start_executor_process, ExecutorProcessConfig};
use comet_native_executors::codec::CometPhysicalCodec;
use comet_native_executors::{env_var, init_logging};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_logging();
    let mut config = ExecutorProcessConfig {
        override_physical_codec: Some(Arc::new(CometPhysicalCodec::default())),
        ..Default::default()
    };
    if let Some(host) = env_var("COMET_EXECUTOR_BIND_HOST")? {
        config.bind_host = host;
    }
    config.external_host = env_var("COMET_EXECUTOR_EXTERNAL_HOST")?;
    if let Some(port) = env_var("COMET_EXECUTOR_PORT")? {
        config.port = port;
    }
    if let Some(port) = env_var("COMET_EXECUTOR_GRPC_PORT")? {
        config.grpc_port = port;
    }
    if let Some(host) = env_var("COMET_SCHEDULER_HOST")? {
        config.scheduler_host = host;
    }
    if let Some(port) = env_var("COMET_SCHEDULER_PORT")? {
        config.scheduler_port = port;
    }
    if let Some(tasks) = env_var("COMET_EXECUTOR_TASKS")? {
        config.vcores = tasks;
    }
    config.work_dir = env_var("COMET_EXECUTOR_WORK_DIR")?;

    start_executor_process(Arc::new(config)).await?;
    Ok(())
}
