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

use arrow::array::{ArrayRef, Int64Array, StructArray};
use arrow::datatypes::{DataType, Field, Fields};

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

/// Returns the value of the `a: Int32` field of an input struct.
///
/// Test fixture for descriptor encoding of complex (non-primitive) types
/// — exercises the IPC-encoded `Field` bytes path in `UdfDescriptor`.
pub struct StructFieldA {
    sig: CometUdfSignature,
}

impl Default for StructFieldA {
    fn default() -> Self {
        let inner = Fields::from(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
        ]);
        Self {
            sig: CometUdfSignature {
                args: vec![DataType::Struct(inner)],
                return_type: DataType::Int32,
                volatility: Volatility::Immutable,
            },
        }
    }
}

impl CometScalarUdf for StructFieldA {
    fn name(&self) -> &str {
        "struct_field_a"
    }
    fn signature(&self) -> &CometUdfSignature {
        &self.sig
    }
    fn invoke(&self, args: &[ArrayRef]) -> Result<ArrayRef, CometUdfError> {
        let s = args[0]
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| CometUdfError::new("expected StructArray"))?;
        Ok(s.column_by_name("a")
            .ok_or_else(|| CometUdfError::new("missing field 'a'"))?
            .clone())
    }
}

/// Returns Err on every invocation.
///
/// Test fixture for the user-error path through `invoke_impl` (rc=1 in
/// the C ABI). Verifies error messages flow through `UdfError`.
pub struct AlwaysErr {
    sig: CometUdfSignature,
}

impl Default for AlwaysErr {
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

impl CometScalarUdf for AlwaysErr {
    fn name(&self) -> &str {
        "always_err"
    }
    fn signature(&self) -> &CometUdfSignature {
        &self.sig
    }
    fn invoke(&self, _: &[ArrayRef]) -> Result<ArrayRef, CometUdfError> {
        Err(CometUdfError::new("intentional failure for tests"))
    }
}

/// Panics on every invocation.
///
/// Test fixture for the panic path through `invoke_impl` (rc=2). Verifies
/// `catch_unwind` catches the panic, formats it into `UdfError`, and the
/// host process stays alive.
pub struct AlwaysPanic {
    sig: CometUdfSignature,
}

impl Default for AlwaysPanic {
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

impl CometScalarUdf for AlwaysPanic {
    fn name(&self) -> &str {
        "always_panic"
    }
    fn signature(&self) -> &CometUdfSignature {
        &self.sig
    }
    fn invoke(&self, _: &[ArrayRef]) -> Result<ArrayRef, CometUdfError> {
        panic!("intentional panic for tests");
    }
}

/// Returns a constant single-element array, regardless of input length.
///
/// Test fixture for the host's length-mismatch handling: the host
/// expects the returned array length to match the longest input, and
/// must surface a clear error when it doesn't.
pub struct LengthOne {
    sig: CometUdfSignature,
}

impl Default for LengthOne {
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

impl CometScalarUdf for LengthOne {
    fn name(&self) -> &str {
        "length_one"
    }
    fn signature(&self) -> &CometUdfSignature {
        &self.sig
    }
    fn invoke(&self, _: &[ArrayRef]) -> Result<ArrayRef, CometUdfError> {
        Ok(Arc::new(Int64Array::from(vec![42])))
    }
}

comet_udf_sdk::export!(AddOne, StructFieldA, AlwaysErr, AlwaysPanic, LengthOne);
