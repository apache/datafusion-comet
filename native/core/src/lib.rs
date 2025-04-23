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

#![allow(incomplete_features)]
#![allow(non_camel_case_types)]
#![allow(clippy::upper_case_acronyms)]
// For prost generated struct
#![allow(clippy::derive_partial_eq_without_eq)]
// The clippy throws an error if the reference clone not wrapped into `Arc::clone`
// The lint makes easier for code reader/reviewer separate references clones from more heavyweight ones
#![deny(clippy::clone_on_ref_ptr)]
extern crate core;

use jni::{
    objects::{JClass, JString},
    JNIEnv, JavaVM,
};
use log::{info, LevelFilter};
use log4rs::{
    append::console::{ConsoleAppender, Target},
    config::{load_config_file, Appender, Deserializers, Root},
    encode::pattern::PatternEncoder,
    Config,
};
use once_cell::sync::OnceCell;

#[cfg(all(not(target_env = "msvc"), feature = "jemalloc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(feature = "mimalloc")]
use mimalloc::MiMalloc;

#[cfg(feature = "snmalloc")]
use snmalloc_rs::SnMalloc;

#[cfg(feature = "tcmalloc")]
use tcmalloc::TCMalloc;

use errors::{try_unwrap_or_throw, CometError, CometResult};

#[macro_use]
mod errors;
#[macro_use]
pub mod common;
pub mod execution;
mod jvm_bridge;
pub mod parquet;

#[cfg(all(not(target_env = "msvc"), feature = "jemalloc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[cfg(feature = "snmalloc")]
#[global_allocator]
static GLOBAL: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[cfg(feature = "tcmalloc")]
#[global_allocator]
static GLOBAL: TCMalloc = TCMalloc;

const _: () = {
    let enabled_features = {
        cfg!(feature = "jemalloc") as u32
            + cfg!(feature = "mimalloc") as u32
            + cfg!(feature = "snmalloc") as u32
            + cfg!(feature = "tcmalloc") as u32
    };

    match enabled_features {
        0 | 1=> {}
        2.. => panic!("Invalid feature flags for custom allocators. Please enable at most one of [\"jemalloc\", \"mimalloc\", \"snmalloc\", \"tcmalloc\"]"),
    }
};

static JAVA_VM: OnceCell<JavaVM> = OnceCell::new();

#[no_mangle]
pub extern "system" fn Java_org_apache_comet_NativeBase_init(
    e: JNIEnv,
    _: JClass,
    log_conf_path: JString,
) {
    // Initialize the error handling to capture panic backtraces
    errors::init();

    try_unwrap_or_throw(&e, |mut env| {
        let path: String = env.get_string(&log_conf_path)?.into();

        // empty path means there is no custom log4rs config file provided, so fallback to use
        // the default configuration
        let log_config = if path.is_empty() {
            default_logger_config()
        } else {
            load_config_file(path, Deserializers::default())
                .map_err(|err| CometError::Config(err.to_string()))
        };

        let _ = log4rs::init_config(log_config?).map_err(|err| CometError::Config(err.to_string()));

        // Initialize the global Java VM
        let java_vm = env.get_java_vm()?;
        JAVA_VM.get_or_init(|| java_vm);

        let comet_version = env!("CARGO_PKG_VERSION");
        info!("Comet native library version {} initialized", comet_version);
        Ok(())
    })
}

const LOG_PATTERN: &str = "{d(%y/%m/%d %H:%M:%S)} {l} {f}: {m}{n}";

// Creates a default log4rs config, which logs to console with `INFO` level.
fn default_logger_config() -> CometResult<Config> {
    let console_append = ConsoleAppender::builder()
        .target(Target::Stderr)
        .encoder(Box::new(PatternEncoder::new(LOG_PATTERN)))
        .build();
    let appender = Appender::builder().build("console", Box::new(console_append));
    let root = Root::builder().appender("console").build(LevelFilter::Info);
    Config::builder()
        .appender(appender)
        .build(root)
        .map_err(|err| CometError::Config(err.to_string()))
}
