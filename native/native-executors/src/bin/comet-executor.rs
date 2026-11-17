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
//! - `COMET_GRPC_MAX_MESSAGE_SIZE`: the largest message, in bytes, the executor sends or accepts
//!   on Ballista's gRPC connections with the scheduler, such as a task with many partitions of a
//!   scan, and on its Flight service (default: Ballista's 16 MiB). Set it on the scheduler as
//!   well. The executor's reads of other executors' shuffle data keep the limit of the query's
//!   `ballista.client.grpc_max_message_size` (16 MiB by default), which this does not change.
//! - `COMET_EXECUTOR_MEMORY_POOL_SIZE`: the executor's memory budget in bytes, `0` for no limit.
//!   It is not one pool that the tasks share: each task gets its own pool of the budget divided
//!   by `COMET_EXECUTOR_TASKS`, times the task slots it takes, even when it runs alone. By
//!   default the budget is 70% of the smaller of the container's memory limit and the host's
//!   memory, or no limit when neither can be found. Memory outside the pools, such as batches in
//!   flight between stages, comes on top of it.
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
    if let Some(size) = env_var("COMET_EXECUTOR_MEMORY_POOL_SIZE")? {
        config.memory_pool_size = Some(size);
    }
    if let Some(size) = env_var("COMET_GRPC_MAX_MESSAGE_SIZE")? {
        config.grpc_max_decoding_message_size = size;
        config.grpc_max_encoding_message_size = size;
    }

    start_executor_process(Arc::new(config)).await?;
    Ok(())
}
