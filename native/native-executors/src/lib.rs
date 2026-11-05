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

//! Comet Native Executors: a Comet scheduler and executors built on Apache DataFusion Ballista,
//! which run whole Spark queries planned by a Spark driver with no JVM. See
//! <https://github.com/apache/datafusion-comet/issues/7288>.
//!
//! A driver submits a query as a stage graph of Comet native blocks (a
//! [`QueryRequest`](datafusion_comet_proto::spark_native_executors::QueryRequest)) to the
//! scheduler's Arrow Flight service ([`frontend`]). The scheduler checks it, turns it into a
//! Ballista physical plan ([`assembly`]) and runs it on the executors, which rebuild each block
//! with Comet's planner. The driver then fetches the result partitions from the executors.

use std::fmt::Display;
use std::str::FromStr;

pub mod assembly;
pub mod codec;
pub mod frontend;

/// The value of the environment variable `key`, if it is set, for the binaries' settings.
pub fn env_var<T: FromStr>(key: &str) -> Result<Option<T>, String>
where
    T::Err: Display,
{
    std::env::var(key)
        .ok()
        .map(|value| {
            value
                .parse()
                .map_err(|e| format!("Invalid {key} {value:?}: {e}"))
        })
        .transpose()
}

/// Logs at `info` unless `RUST_LOG` says otherwise.
pub fn init_logging() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
}
