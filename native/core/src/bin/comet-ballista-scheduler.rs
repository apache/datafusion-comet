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

//! A Comet-flavored Ballista **scheduler** process.
//!
//! Identical to the stock `ballista-scheduler` binary except that it registers
//! Comet's extension codecs ([`CometLogicalCodec`] / [`CometPhysicalCodec`]) on
//! the [`SchedulerConfig`], so a submitted plan containing Comet nodes
//! (`CometFragmentExec` / `CometScanExec`) survives (de)serialization on the
//! scheduler. The stock CLI hardcodes those overrides to `None`, which is why
//! this bespoke binary exists.
//!
//! Only built with `--features ballista` (see `required-features` in
//! `core/Cargo.toml`). Runs a real, externally reachable gRPC scheduler that a
//! separate `comet-ballista-executor` process connects to.
//!
//! Configuration (all optional, env-driven so a harness can place it):
//! - `COMET_BALLISTA_SCHEDULER_BIND_HOST` (default `127.0.0.1`)
//! - `COMET_BALLISTA_SCHEDULER_BIND_PORT` (default `50050`)

use std::net::SocketAddr;
use std::sync::Arc;

use ballista_scheduler::cluster::BallistaCluster;
use ballista_scheduler::config::SchedulerConfig;
use ballista_scheduler::scheduler_process::start_server;

use comet::execution::ballista::{CometLogicalCodec, CometPhysicalCodec};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bind_host = std::env::var("COMET_BALLISTA_SCHEDULER_BIND_HOST")
        .unwrap_or_else(|_| "127.0.0.1".to_string());
    let bind_port: u16 = std::env::var("COMET_BALLISTA_SCHEDULER_BIND_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(50050);

    // Manual runtime so the default (feature-less) build needs no tokio `macros`.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    runtime.block_on(async move {
        let config = SchedulerConfig {
            bind_host: bind_host.clone(),
            bind_port,
            // The seam: Comet codecs so the scheduler can decode Comet plan nodes.
            override_logical_codec: Some(Arc::new(CometLogicalCodec::default())),
            override_physical_codec: Some(Arc::new(CometPhysicalCodec::default())),
            ..Default::default()
        };

        let addr: SocketAddr = format!("{bind_host}:{bind_port}").parse()?;
        eprintln!("[comet-ballista-scheduler] starting on {addr}");

        let cluster = BallistaCluster::new_from_config(&config).await?;
        start_server(cluster, addr, Arc::new(config)).await?;
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}
