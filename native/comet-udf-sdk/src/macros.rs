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

//! Internal helpers used by the [`crate::export!`] macro and exercised by SDK
//! unit tests. Stable across patch versions only — do not rely on these
//! from user code.

use std::ffi::CString;
use std::panic::AssertUnwindSafe;
use std::ptr;

use arrow::array::{make_array, ArrayRef};
use arrow::ffi::{from_ffi, to_ffi, FFI_ArrowArray, FFI_ArrowSchema};

use crate::error::{UdfError, UdfErrorCode};
use crate::types::{
    field_to_ipc_bytes, ArrowTypeTag, CometUdfSignature, UdfDescriptor, Volatility,
};
use crate::CometScalarUdf;

/// Encoded name + per-arg / return descriptor bytes for a single UDF.
/// Lives in `static`s in the cdylib so descriptor pointers are stable for
/// the lifetime of the process.
pub struct EncodedSignature {
    /// UDF name as a NUL-terminated C string (used to expose `*const c_char`).
    pub name: CString,
    /// One Arrow type tag per argument (in declaration order).
    pub arg_tags: Vec<u32>,
    /// IPC bytes per argument; empty Vec when the arg is primitive.
    pub arg_field_bytes: Vec<Vec<u8>>,
    /// Return type tag.
    pub return_tag: u32,
    /// IPC bytes for the return type; empty when primitive.
    pub return_field_bytes: Vec<u8>,
    /// Stable pointer table for `UdfDescriptor::arg_field_ipc_ptrs`.
    /// Each entry is null when the corresponding `arg_field_bytes` is empty.
    pub arg_field_ipc_ptrs: Vec<*const u8>,
    /// Lengths matching `arg_field_ipc_ptrs`.
    pub arg_field_ipc_lens: Vec<u32>,
}

// `EncodedSignature` is held behind a static `OnceLock` and exposed only
// via raw pointers we control. Marking Send/Sync is sound because the
// pointers we expose either point into stable static-lived buffers (the
// `Vec`s inside this struct) or into the `Vec` allocations themselves.
unsafe impl Sync for EncodedSignature {}
unsafe impl Send for EncodedSignature {}

impl EncodedSignature {
    /// Encode a (name, signature) pair for inclusion in `UdfDescriptor`.
    pub fn from_signature(name: &str, sig: &CometUdfSignature) -> Self {
        let mut arg_tags = Vec::with_capacity(sig.args.len());
        let mut arg_field_bytes: Vec<Vec<u8>> = Vec::with_capacity(sig.args.len());
        for dt in &sig.args {
            match ArrowTypeTag::primitive(dt) {
                Some(tag) => {
                    arg_tags.push(tag as u32);
                    arg_field_bytes.push(Vec::new());
                }
                None => {
                    arg_tags.push(ArrowTypeTag::Field as u32);
                    let f = arrow::datatypes::Field::new("arg", dt.clone(), true);
                    arg_field_bytes.push(field_to_ipc_bytes(&f));
                }
            }
        }
        let arg_field_ipc_ptrs: Vec<*const u8> = arg_field_bytes
            .iter()
            .map(|v| if v.is_empty() { ptr::null() } else { v.as_ptr() })
            .collect();
        let arg_field_ipc_lens: Vec<u32> =
            arg_field_bytes.iter().map(|v| v.len() as u32).collect();
        let (return_tag, return_field_bytes) = match ArrowTypeTag::primitive(&sig.return_type) {
            Some(tag) => (tag as u32, Vec::new()),
            None => {
                let f = arrow::datatypes::Field::new("ret", sig.return_type.clone(), true);
                (ArrowTypeTag::Field as u32, field_to_ipc_bytes(&f))
            }
        };
        let name = CString::new(name).expect("UDF name must not contain NUL bytes");
        Self {
            name,
            arg_tags,
            arg_field_bytes,
            return_tag,
            return_field_bytes,
            arg_field_ipc_ptrs,
            arg_field_ipc_lens,
        }
    }

    /// Populate a zeroed [`UdfDescriptor`] from this encoded signature.
    pub fn fill_descriptor(&self, sig: &CometUdfSignature, out: &mut UdfDescriptor) {
        out.name_ptr = self.name.as_ptr();
        out.name_len = self.name.as_bytes().len() as u32;
        out.volatility = match sig.volatility {
            Volatility::Immutable => 0,
            Volatility::Stable => 1,
            Volatility::Volatile => 2,
        };
        out.n_args = self.arg_tags.len() as u32;
        out.arg_tags = self.arg_tags.as_ptr();
        out.arg_field_ipc_ptrs = self.arg_field_ipc_ptrs.as_ptr();
        out.arg_field_ipc_lens = self.arg_field_ipc_lens.as_ptr();
        out.return_tag = self.return_tag;
        out.return_field_ipc_ptr = if self.return_field_bytes.is_empty() {
            ptr::null()
        } else {
            self.return_field_bytes.as_ptr()
        };
        out.return_field_ipc_len = self.return_field_bytes.len() as u32;
    }
}

/// Implementation of `comet_udf_invoke` for one UDF.
///
/// Returns 0 on success, 1 on user error, 2 on panic.
///
/// # Safety
///
/// All pointer/len pairs must satisfy the C ABI contract documented on
/// [`crate::export!`]: the caller "moves" the input arrays into the cdylib
/// (the cdylib will replace each input slot with an empty FFI struct), and
/// the cdylib writes new arrays into the caller-provided output slots.
pub unsafe fn invoke_impl(
    udf: &dyn CometScalarUdf,
    in_arrays: *const FFI_ArrowArray,
    in_schemas: *const FFI_ArrowSchema,
    n_in: u32,
    out_array: *mut FFI_ArrowArray,
    out_schema: *mut FFI_ArrowSchema,
    err_out: *mut UdfError,
) -> i32 {
    if !err_out.is_null() {
        (*err_out).free_in_place();
    }

    let result = std::panic::catch_unwind(AssertUnwindSafe(|| -> Result<ArrayRef, String> {
        let mut inputs: Vec<ArrayRef> = Vec::with_capacity(n_in as usize);
        for i in 0..(n_in as usize) {
            // Move ownership of the FFI structs out of the caller's slots
            // and into our local storage. The caller-side memory is
            // released when the imported `ArrayData` is dropped.
            let arr_ptr = in_arrays.add(i) as *mut FFI_ArrowArray;
            let sch_ptr = in_schemas.add(i) as *mut FFI_ArrowSchema;
            let arr = std::ptr::replace(arr_ptr, FFI_ArrowArray::empty());
            let sch = std::ptr::replace(sch_ptr, FFI_ArrowSchema::empty());
            let array_data = from_ffi(arr, &sch).map_err(|e| e.to_string())?;
            inputs.push(make_array(array_data));
        }
        let out = udf.invoke(&inputs).map_err(|e| e.message)?;
        let (out_arr, out_sch) = to_ffi(&out.to_data()).map_err(|e| e.to_string())?;
        std::ptr::write(out_array, out_arr);
        std::ptr::write(out_schema, out_sch);
        Ok(out)
    }));

    match result {
        Ok(Ok(_)) => 0,
        Ok(Err(msg)) => {
            if !err_out.is_null() {
                (*err_out).set(UdfErrorCode::UserError, &msg);
            }
            1
        }
        Err(panic_payload) => {
            let msg = panic_msg(&panic_payload);
            if !err_out.is_null() {
                (*err_out).set(UdfErrorCode::Panic, &msg);
            }
            2
        }
    }
}

fn panic_msg(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "panic with non-string payload".to_string()
    }
}

/// Implementation of `comet_udf_free_error`.
///
/// # Safety
/// `err` must point to an `UdfError` whose message was written by this
/// cdylib's `comet_udf_invoke`.
pub unsafe fn free_error_impl(err: *mut UdfError) {
    if !err.is_null() {
        (*err).free_in_place();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;

    #[test]
    fn encoded_primitive_signature() {
        let sig = CometUdfSignature {
            args: vec![DataType::Int64, DataType::Int64],
            return_type: DataType::Int64,
            volatility: Volatility::Immutable,
        };
        let enc = EncodedSignature::from_signature("add", &sig);
        assert_eq!(enc.name.to_str().unwrap(), "add");
        assert_eq!(enc.arg_tags, vec![ArrowTypeTag::Int64 as u32; 2]);
        assert!(enc.return_field_bytes.is_empty());

        let mut out = UdfDescriptor::zeroed();
        enc.fill_descriptor(&sig, &mut out);
        assert_eq!(out.n_args, 2);
        assert_eq!(out.return_tag, ArrowTypeTag::Int64 as u32);
        assert_eq!(out.name_len, 3);
    }
}
