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
#![allow(dead_code)]
#![allow(clippy::upper_case_acronyms)]
#![allow(clippy::derive_partial_eq_without_eq)] // For prost generated struct
#![cfg_attr(feature = "nightly", feature(core_intrinsics))]
#![feature(int_roundings)]
#![feature(specialization)]

// Branch prediction hint. This is currently only available on nightly.
#[cfg(feature = "nightly")]
use core::intrinsics::{likely, unlikely};

use jni::{
    objects::{JClass, JString},
    JNIEnv, JavaVM,
};
use log::{info, LevelFilter};
use log4rs::{
    append::console::ConsoleAppender,
    config::{load_config_file, Appender, Deserializers, Root},
    encode::pattern::PatternEncoder,
    Config,
};
#[cfg(feature = "mimalloc")]
use mimalloc::MiMalloc;
use once_cell::sync::OnceCell;

pub use data_type::*;

use crate::errors::{try_unwrap_or_throw, CometError, CometResult};

#[macro_use]
mod errors;
#[macro_use]
pub mod common;
mod data_type;
pub mod execution;
mod jvm_bridge;
pub mod parquet;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

static JAVA_VM: OnceCell<JavaVM> = OnceCell::new();

#[no_mangle]
pub extern "system" fn Java_org_apache_comet_NativeBase_init(
    env: JNIEnv,
    _: JClass,
    log_conf_path: JString,
) {
    // Initialize the error handling to capture panic backtraces
    errors::init();

    try_unwrap_or_throw(env, || {
        let path: String = env.get_string(log_conf_path)?.into();

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

        info!("Comet native library initialized");
        Ok(())
    })
}

const LOG_PATTERN: &str = "{d(%y/%m/%d %H:%M:%S)} {l} {f}: {m}{n}";

// Creates a default log4rs config, which logs to console with `INFO` level.
fn default_logger_config() -> CometResult<Config> {
    let console_append = ConsoleAppender::builder()
        .encoder(Box::new(PatternEncoder::new(LOG_PATTERN)))
        .build();
    let appender = Appender::builder().build("console", Box::new(console_append));
    let root = Root::builder().appender("console").build(LevelFilter::Info);
    Config::builder()
        .appender(appender)
        .build(root)
        .map_err(|err| CometError::Config(err.to_string()))
}

// These are borrowed from hashbrown crate:
//   https://github.com/rust-lang/hashbrown/blob/master/src/raw/mod.rs

// On stable we can use #[cold] to get a equivalent effect: this attributes
// suggests that the function is unlikely to be called
#[cfg(not(feature = "nightly"))]
#[inline]
#[cold]
fn cold() {}

#[cfg(not(feature = "nightly"))]
#[inline]
fn likely(b: bool) -> bool {
    if !b {
        cold();
    }
    b
}
#[cfg(not(feature = "nightly"))]
#[inline]
fn unlikely(b: bool) -> bool {
    if b {
        cold();
    }
    b
}
