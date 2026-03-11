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

//! JVM bridge and error types for Apache DataFusion Comet.
//!
//! This crate provides the JNI/JVM interaction layer and common error types
//! used across Comet's native Rust crates.

#![allow(clippy::result_large_err)]

use jni::JavaVM;
use once_cell::sync::OnceCell;

pub mod errors;
pub mod jvm_bridge;
pub mod query_context;
pub mod spark_error;

pub use query_context::{create_query_context_map, QueryContext, QueryContextMap};
pub use spark_error::{decimal_overflow_error, SparkError, SparkErrorWithContext, SparkResult};

/// Global reference to the Java VM, initialized during native library setup.
pub static JAVA_VM: OnceCell<JavaVM> = OnceCell::new();
