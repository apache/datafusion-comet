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

//! Error types used in the SDK trait and across the C ABI.

use std::ffi::{c_char, CString};
use std::ptr;

/// Error returned by `CometScalarUdf::invoke` (trait added in Task 4).
#[derive(Debug, Clone)]
pub struct CometUdfError {
    /// Human-readable error message.
    pub message: String,
}

impl CometUdfError {
    /// Construct from any displayable message.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for CometUdfError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for CometUdfError {}

/// Error code written into [`UdfError`] when `comet_udf_invoke` fails.
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UdfErrorCode {
    /// Reserved — never written.
    None = 0,
    /// User invoke returned `Err(CometUdfError)`.
    UserError = 1,
    /// User invoke panicked. `catch_unwind` caught it.
    Panic = 2,
    /// SDK-level failure (FFI conversion, descriptor parse, etc.).
    Internal = 3,
}

/// Error struct populated by `comet_udf_invoke` on failure. Memory for
/// `message` is allocated via [`UdfError::set`] (which uses
/// `CString::into_raw`) and **must** be freed by calling
/// `comet_udf_free_error` in the same `cdylib` that produced it.
///
/// `_reserved` exists so error fields can be added in a future ABI minor
/// version without breaking layout. Always zeroed today; ignored by the
/// host.
#[repr(C)]
pub struct UdfError {
    /// Null-terminated UTF-8 string, or null if `code == None`.
    pub message: *mut c_char,
    /// One of [`UdfErrorCode`].
    pub code: u32,
    /// Reserved space; zero today.
    pub _reserved: [u64; 3],
}

impl UdfError {
    /// Build a zeroed error.
    pub fn zeroed() -> Self {
        Self {
            message: ptr::null_mut(),
            code: 0,
            _reserved: [0; 3],
        }
    }

    /// Populate this error with a code and message. Replaces any existing
    /// message (which leaks — caller must have freed it).
    pub fn set(&mut self, code: UdfErrorCode, message: &str) {
        let cstr = CString::new(message.replace('\0', "?"))
            .expect("replaced nul bytes; CString::new must succeed");
        self.message = cstr.into_raw();
        self.code = code as u32;
    }

    /// Free the message buffer if present. Safe to call when message is
    /// null. Resets `code` to `None`.
    ///
    /// # Safety
    /// Must be called from the same `cdylib` whose [`UdfError::set`] wrote
    /// the message — `CString::from_raw` requires the same allocator.
    pub unsafe fn free_in_place(&mut self) {
        if !self.message.is_null() {
            let _ = CString::from_raw(self.message);
            self.message = ptr::null_mut();
        }
        self.code = UdfErrorCode::None as u32;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn comet_udf_error_basic() {
        let e = CometUdfError::new("boom");
        assert_eq!(e.to_string(), "boom");
    }

    #[test]
    fn udf_error_set_and_free() {
        let mut err = UdfError::zeroed();
        err.set(UdfErrorCode::UserError, "oops");
        assert!(!err.message.is_null());
        assert_eq!(err.code, UdfErrorCode::UserError as u32);
        unsafe { err.free_in_place() };
        assert!(err.message.is_null());
        assert_eq!(err.code, UdfErrorCode::None as u32);
    }
}
