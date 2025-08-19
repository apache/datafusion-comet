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

//! Operators

use std::fmt::Debug;

use jni::objects::GlobalRef;

pub use copy::*;
pub use scan::*;

mod copy;
mod expand;
pub use expand::ExpandExec;
mod scan;

/// Error returned during executing operators.
#[derive(thiserror::Error, Debug)]
pub enum ExecutionError {
    /// Simple error
    #[allow(dead_code)]
    #[error("General execution error with reason: {0}.")]
    GeneralError(String),

    /// Error when deserializing an operator.
    #[error("Fail to deserialize to native operator with reason: {0}.")]
    DeserializeError(String),

    /// Error when processing Arrow array.
    #[error("Fail to process Arrow array with reason: {0}.")]
    ArrowError(String),

    /// DataFusion error
    #[error("Error from DataFusion: {0}.")]
    DataFusionError(String),

    #[error("{class}: {msg}")]
    JavaException {
        class: String,
        msg: String,
        throwable: GlobalRef,
    },
}
