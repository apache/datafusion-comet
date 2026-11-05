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
//! - `COMET_SCHEDULER_PORT`: Ballista's gRPC port, for executors (default `50050`)
//! - `COMET_SCHEDULER_FLIGHT_PORT`: the Flight port, for drivers (default `50060`)
//! - `COMET_SCHEDULER_RESULT_RETENTION_SECONDS`: how long a query's results stay readable after
//!   the query ends, whether or not a driver has read them (default `300`). `0` keeps them.

use std::net::SocketAddr;
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
        ..Default::default()
    };
    if let Some(host) = env_var("COMET_SCHEDULER_BIND_HOST")? {
        config.bind_host = host;
    }
    if let Some(port) = env_var("COMET_SCHEDULER_PORT")? {
        config.bind_port = port;
    }
    if let Some(seconds) = env_var("COMET_SCHEDULER_RESULT_RETENTION_SECONDS")? {
        config.finished_job_data_clean_up_interval_seconds = seconds;
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
