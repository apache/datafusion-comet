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

// Re-export all error types from the common crate
pub use datafusion_comet_common::{
    decimal_overflow_error, SparkError, SparkErrorWithContext, SparkResult,
};

use arrow::error::ArrowError;
use datafusion::common::DataFusionError;

/// Arrow's `try_*` kernels require closure errors to be `ArrowError`, so a `SparkError`
/// travels through `ExternalError`. Unwrap it again so JNI sees the direct `SparkError`.
pub(crate) fn unwrap_arrow_external_error(error: ArrowError) -> DataFusionError {
    match error {
        ArrowError::ExternalError(error) => DataFusionError::External(error),
        error => error.into(),
    }
}
