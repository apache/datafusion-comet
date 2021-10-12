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

use core::slice;
use std::ptr::NonNull;

use jni::{
    objects::{ReleaseMode, TypeArray},
    sys::{jbyte, jbyteArray, JNI_TRUE},
    JavaVM,
};

use crate::errors::{CometError, CometResult as Result};

use super::Buffer;

/// An immutable byte buffer wrapping a JNI byte array allocated on heap.
///
/// Unlike `AutoArray`, this doesn't have a lifetime and can be used across different JNI calls.
pub struct JniBuffer {
    /// A pointer for the JVM instance, used to obtain byte array elements (via
    /// `GetByteArrayElements`) and release byte array elements (via `ReleaseByteArrayElements`).
    jvm: JavaVM,
    /// The original JNI byte array that backs this buffer
    inner: jbyteArray,
    /// The raw pointer from the JNI byte array
    ptr: NonNull<i8>,
    /// Total number of bytes in the original array (i.e., `inner`).
    len: usize,
    /// Whether the JNI byte array is copied or not.
    is_copy: bool,
}

impl JniBuffer {
    pub fn try_new(jvm: JavaVM, array: jbyteArray, len: usize) -> Result<Self> {
        let env = jvm.get_env()?;
        let mut is_copy = 0xff;
        let ptr = jbyte::get(&env, array.into(), &mut is_copy)?;
        let res = Self {
            jvm,
            inner: array,
            ptr: NonNull::new(ptr)
                .ok_or_else(|| CometError::NullPointer("null byte array pointer".to_string()))?,
            len,
            is_copy: is_copy == JNI_TRUE,
        };
        Ok(res)
    }

    /// Whether the JNI byte array is copied or not, i.e., whether the JVM pinned down the original
    /// Java byte array, or made a new copy of it.
    pub fn is_copy(&self) -> bool {
        self.is_copy
    }
}

impl Buffer for JniBuffer {
    fn len(&self) -> usize {
        self.len
    }

    fn data(&self) -> &[u8] {
        self.as_ref()
    }
}

impl AsRef<[u8]> for JniBuffer {
    fn as_ref(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.ptr.as_ptr() as *mut u8 as *const u8, self.len) }
    }
}

impl Drop for JniBuffer {
    fn drop(&mut self) {
        let env = self.jvm.get_env().unwrap(); // TODO: log error here
        jbyte::release(
            &env,
            self.inner.into(),
            self.ptr,
            ReleaseMode::NoCopyBack as i32, // don't copy back since it's read-only here
        )
        .unwrap();
    }
}
