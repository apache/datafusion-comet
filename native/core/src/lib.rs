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
extern crate datafusion_comet_jni_bridge;

use jni::{
    objects::{JClass, JString},
    EnvUnowned,
};
use log::info;
use log4rs::{
    append::console::{ConsoleAppender, Target},
    config::{load_config_file, Appender, Deserializers, Root},
    encode::pattern::PatternEncoder,
    Config,
};

// Re-export from jvm-bridge crate for internal use
pub use datafusion_comet_jni_bridge::errors;
pub use datafusion_comet_jni_bridge::JAVA_VM;

/// Re-export jvm-bridge items under the `jvm_bridge` name for convenience.
pub mod jvm_bridge {
    pub use datafusion_comet_jni_bridge::*;
}

use errors::{try_unwrap_or_throw, CometError, CometResult};

pub mod alloc_accounting;
pub mod cloud;
pub mod execution;
pub mod parquet;
// this module is for non release only. Intended for debugging/profiling purposes
#[cfg(debug_assertions)]
pub mod debug;

// Global allocator selection.
//
// `backend` names the allocator the feature set asks for: jemalloc where it builds, otherwise
// mimalloc, otherwise the system allocator. The three `backend` cfgs partition every feature
// combination, so exactly one definition exists, and each backend predicate is written once. The
// unwrapped `#[global_allocator]` lives inside the backend module that owns it, so a build without
// `alloc-accounting` is byte-for-byte the previous arrangement: no wrapper, no per-allocation work,
// and no explicit allocator at all when the selection is the system allocator.
//
// With `alloc-accounting`, the single wrapped `#[global_allocator]` below refers to
// `backend::Backend` whatever it resolved to. That is what makes the wrapper impossible to drop
// silently: a feature combination with no backend would fail to compile rather than run with the
// metric enabled and reading zero.

/// jemalloc, on targets where it builds, unless mimalloc was also requested.
#[cfg(all(
    not(target_env = "msvc"),
    feature = "jemalloc",
    not(feature = "mimalloc")
))]
mod backend {
    pub type Backend = tikv_jemallocator::Jemalloc;
    pub const BACKEND: Backend = tikv_jemallocator::Jemalloc;
    pub const NAME: &str = "jemalloc";

    #[cfg(not(feature = "alloc-accounting"))]
    #[global_allocator]
    static GLOBAL: Backend = BACKEND;
}

/// mimalloc, unless a usable jemalloc was also requested.
#[cfg(all(
    feature = "mimalloc",
    not(all(not(target_env = "msvc"), feature = "jemalloc"))
))]
mod backend {
    pub type Backend = mimalloc::MiMalloc;
    pub const BACKEND: Backend = mimalloc::MiMalloc;
    pub const NAME: &str = "mimalloc";

    #[cfg(not(feature = "alloc-accounting"))]
    #[global_allocator]
    static GLOBAL: Backend = BACKEND;
}

/// The system allocator: the complement of the two cases above. This covers neither feature, a
/// jemalloc request on MSVC, and both features together, which each backend cfg excludes in favour
/// of the other.
#[cfg(not(any(
    all(
        not(target_env = "msvc"),
        feature = "jemalloc",
        not(feature = "mimalloc")
    ),
    all(
        feature = "mimalloc",
        not(all(not(target_env = "msvc"), feature = "jemalloc"))
    )
)))]
// Without `alloc-accounting` nothing refers to this selection: the system allocator is the
// default, so no `#[global_allocator]` is installed.
#[cfg_attr(not(feature = "alloc-accounting"), allow(dead_code))]
mod backend {
    pub type Backend = std::alloc::System;
    pub const BACKEND: Backend = std::alloc::System;
    pub const NAME: &str = "system";
}

/// The name of the allocator backend this build selected: `"jemalloc"`, `"mimalloc"` or
/// `"system"`. This is the one place the selection is decided, so anything that needs to know
/// which allocator is in effect (the `alloc_overhead` benchmark's liveness check, for instance)
/// reads it from here rather than re-deriving it from the feature set.
pub use backend::NAME as ALLOCATOR_BACKEND;

#[cfg(feature = "alloc-accounting")]
#[global_allocator]
static GLOBAL: alloc_accounting::AccountingAllocator<backend::Backend> =
    alloc_accounting::AccountingAllocator::new(backend::BACKEND);

#[no_mangle]
pub extern "system" fn Java_org_apache_comet_NativeBase_init(
    e: EnvUnowned,
    _: JClass,
    log_conf_path: JString,
    log_level: JString,
) {
    // Initialize the error handling to capture panic backtraces
    errors::init();

    try_unwrap_or_throw(&e, |env| {
        let path: String = log_conf_path.try_to_string(env)?;

        // empty path means there is no custom log4rs config file provided, so fallback to use
        // the default configuration
        let log_config = if path.is_empty() {
            let log_level: String = match log_level.try_to_string(env) {
                Ok(level) => level,
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

#[no_mangle]
/// Releases the global Tokio runtime used by Comet native execution.
pub extern "system" fn Java_org_apache_comet_NativeBase_release(_e: EnvUnowned, _class: JClass) {
    execution::jni_api::release_runtime();
}

const LOG_PATTERN: &str = "{d(%y/%m/%d %H:%M:%S)} {l} {f}: {m}{n}";

/// JNI method to check if a specific feature is enabled in the native Rust code.
/// # Arguments
/// * `feature_name` - The name of the feature to check. Supported features:
///   - "jemalloc" - tikv-jemallocator memory allocator
///   - "hdfs-opendal" - HDFS support via OpenDAL
/// # Returns
/// * `1` (true) if the feature is enabled
/// * `0` (false) if the feature is disabled or unknown
#[no_mangle]
pub extern "system" fn Java_org_apache_comet_NativeBase_isFeatureEnabled(
    env: EnvUnowned,
    _: JClass,
    feature_name: JString,
) -> jni::sys::jboolean {
    try_unwrap_or_throw(&env, |env| {
        let feature: String = feature_name.try_to_string(env)?;

        let enabled = match feature.as_str() {
            "jemalloc" => cfg!(feature = "jemalloc"),
            "hdfs-opendal" => cfg!(feature = "hdfs-opendal"),
            _ => false, // Unknown features return false
        };

        Ok(enabled)
    })
}

/// JNI: can object_store build a store AND an object key for this URL?
///
/// Source of truth for the JVM planner's "can the native reader handle this filesystem?" check.
/// `prepare_object_store_with_configs` dispatches non-hdfs/non-s3 schemes to object_store's
/// `parse_url` (driven by `ObjectStoreScheme::parse`), so answering from the same parser lets the
/// planner decline early without hardcoding the supported set. (hdfs/libhdfs are handled JVM-side.)
///
/// `ObjectStoreScheme::parse` validates the PATH as well as the scheme -- it ends in
/// `Path::from_url_path` -- so a recognized scheme carrying a key object_store forbids (e.g. a
/// directory name with a newline) answers false. `CometScanRule` relies on both halves: it probes
/// a synthetic scheme-only URL for the cached scheme gate and the real URL for the path gate.
///
/// Alias schemes (e.g. `blob`) are NOT answered here: this gate has no config. Their opt-in lives
/// in the JVM `CometScanRule` via `fs.comet.s3Compliant.schemes`; natively they are rewritten to
/// `s3://` by `normalize_object_store_url`, the first step of
/// `prepare_object_store_with_configs`.
#[no_mangle]
pub extern "system" fn Java_org_apache_comet_NativeBase_isObjectStoreSchemeSupported(
    env: EnvUnowned,
    _: JClass,
    url: JString,
) -> jni::sys::jboolean {
    try_unwrap_or_throw(&env, |env| {
        let url_str: String = url.try_to_string(env)?;
        let supported = url::Url::parse(&url_str)
            .ok()
            .map(|u| object_store::ObjectStoreScheme::parse(&u).is_ok())
            .unwrap_or(false);
        Ok(supported)
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
