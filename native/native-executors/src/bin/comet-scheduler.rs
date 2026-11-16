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

//! A Comet scheduler: Ballista's scheduler, which Comet executors register with, wrapped in the
//! Arrow Flight service that Spark drivers submit queries to.
//!
//! Configured through environment variables, with Ballista's defaults otherwise:
//! - `COMET_SCHEDULER_BIND_HOST` (default `127.0.0.1`)
//! - `COMET_SCHEDULER_EXTERNAL_HOST`: the host executors reach this scheduler at, which they
//!   report task status to (default `localhost`)
//! - `COMET_SCHEDULER_PORT`: Ballista's gRPC port, for executors (default `50050`)
//! - `COMET_SCHEDULER_FLIGHT_PORT`: the Flight port, for drivers (default `50060`)
//! - `COMET_SCHEDULER_RESULT_RETENTION_SECONDS`: how long a query's results stay readable after
//!   the query ends, whether or not a driver has read them (default `300`). `0` keeps them.
//! - `COMET_GRPC_MAX_MESSAGE_SIZE`: the largest message, in bytes, of Ballista's gRPC service
//!   between the scheduler and its executors, such as a task with many partitions of a scan
//!   (default: Ballista's 16 MiB). Set it on the executors as well, for their side of the same
//!   messages. The Flight service for drivers has no limit. Shuffle reads between executors keep
//!   the limit of the query's `ballista.client.grpc_max_message_size` (16 MiB by default), which
//!   this does not change.
//! - `COMET_SCHEDULER_EVENT_LOG_DIR`: a directory to write one event log per query to, as
//!   Ballista's scheduler does (default: no event logs). The scheduler creates it if needed and
//!   doesn't start if it can't write there. A log has the query's plan and a record for each
//!   stage and task, with the task's times and the rows it wrote. The log sees a stage's Comet
//!   block as one operator with no metrics, so the plan shows a block only by its size, input
//!   rows count only the rows a stage reads from other stages (not those it reads from files),
//!   and a task's compute time covers only that reading. A task's start and end times give how
//!   long it ran.
//!   The task records are best effort: when the writer falls behind, Ballista drops them, so a
//!   log can have fewer records than the query had tasks. Logs stay in the directory until
//!   something removes them; `COMET_SCHEDULER_RESULT_RETENTION_SECONDS` applies to query
//!   results, not to logs.
//!
//! Ballista's REST API, which lists and cancels jobs without authentication, is not served.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use arrow_flight::flight_service_server::FlightServiceServer;
use ballista_scheduler::cluster::BallistaCluster;
use ballista_scheduler::config::SchedulerConfig;
use ballista_scheduler::scheduler_process::{create_scheduler, start_grpc_service};
use comet_native_executors::codec::CometPhysicalCodec;
use comet_native_executors::frontend::CometFlightService;
use comet_native_executors::{env_var, init_logging};
use datafusion_proto::protobuf::{LogicalPlanNode, PhysicalPlanNode};
use log::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_logging();
    let mut config = SchedulerConfig {
        override_physical_codec: Some(Arc::new(CometPhysicalCodec::default())),
        // Compiling the event-log writer compiles the REST API in with it, on the gRPC port.
        disable_rest_api: true,
        ..Default::default()
    };
    if let Some(host) = env_var("COMET_SCHEDULER_BIND_HOST")? {
        config.bind_host = host;
    }
    if let Some(host) = env_var("COMET_SCHEDULER_EXTERNAL_HOST")? {
        config.external_host = host;
    }
    if let Some(size) = env_var("COMET_GRPC_MAX_MESSAGE_SIZE")? {
        // The scheduler is a gRPC server for the executors and also a client of theirs, and has
        // a setting for the client. An executor has one pair of settings, for its server and for
        // its client of the scheduler.
        config.grpc_server_max_decoding_message_size = size;
        config.grpc_server_max_encoding_message_size = size;
        config.grpc_client_max_message_size = size;
    }
    if let Some(port) = env_var("COMET_SCHEDULER_PORT")? {
        config.bind_port = port;
    }
    if let Some(seconds) = env_var("COMET_SCHEDULER_RESULT_RETENTION_SECONDS")? {
        config.finished_job_data_clean_up_interval_seconds = seconds;
    }
    if let Some(dir) = env_var::<String>("COMET_SCHEDULER_EVENT_LOG_DIR")? {
        // Ballista creates the directory on a background task, and when it can't write there it
        // only logs warnings. Fail at startup instead, as for a malformed setting.
        check_writable(Path::new(&dir)).map_err(|e| {
            format!("Cannot write event logs to COMET_SCHEDULER_EVENT_LOG_DIR {dir:?}: {e}")
        })?;
        config.event_log_dir = Some(dir);
    }
    let flight_port: u16 = env_var("COMET_SCHEDULER_FLIGHT_PORT")?.unwrap_or(50060);
    let grpc_addr: SocketAddr = format!("{}:{}", config.bind_host, config.bind_port).parse()?;
    let flight_addr = SocketAddr::new(grpc_addr.ip(), flight_port);
    let config = Arc::new(config);
    config.validate()?;

    let cluster = BallistaCluster::new_from_config(&config).await?;
    let scheduler =
        create_scheduler::<LogicalPlanNode, PhysicalPlanNode>(cluster, Arc::clone(&config)).await?;

    info!("Comet scheduler: executors connect to {grpc_addr}, drivers to {flight_addr}");
    let grpc = tokio::spawn(start_grpc_service(grpc_addr, scheduler.clone()));
    let flight = tonic::transport::Server::builder()
        .add_service(
            FlightServiceServer::new(CometFlightService::new(scheduler))
                .max_decoding_message_size(usize::MAX)
                .max_encoding_message_size(usize::MAX),
        )
        .serve(flight_addr);
    tokio::select! {
        result = grpc => result??,
        result = flight => result?,
    }
    Ok(())
}

/// Creates `dir` if it doesn't exist, and checks that the scheduler can create files in it: a
/// directory that already exists, such as a volume mounted with another owner, may not allow it.
fn check_writable(dir: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    // Schedulers in containers are often PID 1, so the name also has the time, for two that share
    // a log volume.
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let probe = dir.join(format!(".comet-scheduler-{}-{nanos}", std::process::id()));
    std::fs::File::create(&probe)?;
    std::fs::remove_file(probe)
}
