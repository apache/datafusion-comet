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
#![allow(clippy::result_large_err)]
// For prost generated struct
#![allow(clippy::derive_partial_eq_without_eq)]
// The clippy throws an error if the reference clone not wrapped into `Arc::clone`
// The lint makes easier for code reader/reviewer separate references clones from more heavyweight ones
#![deny(clippy::clone_on_ref_ptr)]
extern crate core;

#[macro_use]
extern crate datafusion_comet_jvm_bridge;

use jni::{
    objects::{JClass, JString},
    JNIEnv,
};
use log::info;
use log4rs::{
    append::console::{ConsoleAppender, Target},
    config::{load_config_file, Appender, Deserializers, Root},
    encode::pattern::PatternEncoder,
    Config,
};

#[cfg(all(
    not(target_env = "msvc"),
    feature = "jemalloc",
    not(feature = "mimalloc")
))]
use tikv_jemallocator::Jemalloc;

#[cfg(all(
    feature = "mimalloc",
    not(all(not(target_env = "msvc"), feature = "jemalloc"))
))]
use mimalloc::MiMalloc;

// Re-export from jvm-bridge crate for internal use
pub use datafusion_comet_jvm_bridge::errors;
pub use datafusion_comet_jvm_bridge::JAVA_VM;

/// Re-export jvm-bridge items under the `jvm_bridge` name for convenience.
pub mod jvm_bridge {
    pub use datafusion_comet_jvm_bridge::*;
}

use errors::{try_unwrap_or_throw, CometError, CometResult};

#[macro_use]
pub mod common;
pub mod execution;
pub mod parquet;

#[cfg(all(
    not(target_env = "msvc"),
    feature = "jemalloc",
    not(feature = "mimalloc")
))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[cfg(all(
    feature = "mimalloc",
    not(all(not(target_env = "msvc"), feature = "jemalloc"))
))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[no_mangle]
pub extern "system" fn Java_org_apache_comet_NativeBase_init(
    e: JNIEnv,
    _: JClass,
    log_conf_path: JString,
    log_level: JString,
) {
    // Initialize the error handling to capture panic backtraces
    errors::init();

    try_unwrap_or_throw(&e, |mut env| {
        let path: String = env.get_string(&log_conf_path)?.into();

        // empty path means there is no custom log4rs config file provided, so fallback to use
        // the default configuration
        let log_config = if path.is_empty() {
            let log_level: String = match env.get_string(&log_level) {
                Ok(level) => level.into(),
                Err(_) => "info".parse().unwrap(),
            };
            default_logger_config(&log_level)
        } else {
            load_config_file(path, Deserializers::default())
                .map_err(|err| CometError::Config(err.to_string()))
        };

        let _ = log4rs::init_config(log_config?).map_err(|err| CometError::Config(err.to_string()));

        // Initialize the global Java VM
        let java_vm = env.get_java_vm()?;
        JAVA_VM.get_or_init(|| java_vm);

        let comet_version = env!("CARGO_PKG_VERSION");
        info!("Comet native library version {comet_version} initialized");
        Ok(())
    })
}

const LOG_PATTERN: &str = "{d(%y/%m/%d %H:%M:%S)} {l} {f}: {m}{n}";

/// JNI method to check if a specific feature is enabled in the native Rust code.
/// # Arguments
/// * `feature_name` - The name of the feature to check. Supported features:
///   - "jemalloc" - tikv-jemallocator memory allocator
///   - "hdfs" - HDFS object store support
///   - "hdfs-opendal" - HDFS support via OpenDAL
/// # Returns
/// * `1` (true) if the feature is enabled
/// * `0` (false) if the feature is disabled or unknown
#[no_mangle]
pub extern "system" fn Java_org_apache_comet_NativeBase_isFeatureEnabled(
    env: JNIEnv,
    _: JClass,
    feature_name: JString,
) -> jni::sys::jboolean {
    try_unwrap_or_throw(&env, |mut env| {
        let feature: String = env.get_string(&feature_name)?.into();

        let enabled = match feature.as_str() {
            "jemalloc" => cfg!(feature = "jemalloc"),
            "hdfs" => cfg!(feature = "hdfs"),
            "hdfs-opendal" => cfg!(feature = "hdfs-opendal"),
            _ => false, // Unknown features return false
        };

        Ok(enabled as u8)
    })
}

// Creates a default log4rs config, which logs to console with log level.
fn default_logger_config(log_level: &str) -> CometResult<Config> {
    let console_append = ConsoleAppender::builder()
        .target(Target::Stderr)
        .encoder(Box::new(PatternEncoder::new(LOG_PATTERN)))
        .build();
    let appender = Appender::builder().build("console", Box::new(console_append));
    let root = Root::builder().appender("console").build(
        log_level
            .parse()
            .map_err(|err| CometError::Config(format!("{err}")))?,
    );
    Config::builder()
        .appender(appender)
        .build(root)
        .map_err(|err| CometError::Config(err.to_string()))
}
