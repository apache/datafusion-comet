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

//! Test UDF cdylib used by Comet's host tests.
//!
//! Built as `libcomet_test_udfs.{so,dylib}`. Loaded by tests in
//! `native/core/src/execution/rust_udf/` via libloading.

#![warn(missing_docs)]

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::DataType;

use comet_udf_sdk::error::CometUdfError;
use comet_udf_sdk::types::{CometUdfSignature, Volatility};
use comet_udf_sdk::CometScalarUdf;

/// Adds 1 to each element of an `Int64` input.
///
/// Intentionally mirrors the SDK's internal `AddOne` test fixture in
/// `comet-udf-sdk` — the SDK fixture validates the trait can be invoked
/// directly (no FFI), while this one validates the full FFI path through
/// a real cdylib loaded by Comet's host tests. The duplication is the
/// point: it confirms the FFI layer doesn't change semantics.
pub struct AddOne {
    sig: CometUdfSignature,
}

impl Default for AddOne {
    fn default() -> Self {
        Self {
            sig: CometUdfSignature {
                args: vec![DataType::Int64],
                return_type: DataType::Int64,
                volatility: Volatility::Immutable,
            },
        }
    }
}

impl CometScalarUdf for AddOne {
    fn name(&self) -> &str {
        "add_one"
    }
    fn signature(&self) -> &CometUdfSignature {
        &self.sig
    }
    fn invoke(&self, args: &[ArrayRef]) -> Result<ArrayRef, CometUdfError> {
        let arr = args[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| CometUdfError::new("expected Int64Array"))?;
        let out: Int64Array = arr.iter().map(|v| v.map(|x| x + 1)).collect();
        Ok(Arc::new(out))
    }
}

comet_udf_sdk::export!(AddOne);
