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

use std::{ops::Index, slice::SliceIndex, sync::Arc};

/// An immutable byte buffer.
pub trait Buffer {
    /// Returns the length (in bytes) of this buffer.
    fn len(&self) -> usize;

    /// Returns the byte array of this buffer, in range `[0, len)`.
    fn data(&self) -> &[u8];

    /// Returns whether this buffer is empty or not.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Buffer for Vec<u8> {
    fn len(&self) -> usize {
        self.len()
    }

    fn data(&self) -> &[u8] {
        self
    }
}

pub struct BufferRef {
    inner: Arc<dyn Buffer>,
    offset: usize,
    len: usize,
}

impl BufferRef {
    pub fn new(inner: Arc<dyn Buffer>) -> Self {
        let len = inner.len();
        Self {
            inner,
            offset: 0,
            len,
        }
    }

    /// Returns the length of this buffer.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    pub fn data(&self) -> &[u8] {
        self.inner.data()
    }

    /// Creates a new byte buffer containing elements in `[offset, offset+len)`
    #[inline]
    pub fn slice(&self, offset: usize, len: usize) -> BufferRef {
        assert!(
            self.offset + offset + len <= self.inner.len(),
            "can't create a buffer slice with offset exceeding original \
             JNI array len {}, self.offset: {}, offset: {}, len: {}",
            self.inner.len(),
            self.offset,
            offset,
            len
        );

        Self {
            inner: self.inner.clone(),
            offset: self.offset + offset,
            len,
        }
    }

    /// Creates a new byte buffer containing all elements starting from `offset` in this byte array.
    #[inline]
    pub fn start(&self, offset: usize) -> BufferRef {
        assert!(
            self.offset + offset <= self.inner.len(),
            "can't create a buffer slice with offset exceeding original \
             JNI array len {}, self.offset: {}, offset: {}",
            self.inner.len(),
            self.offset,
            offset
        );
        let len = self.inner.len() - offset - self.offset;
        self.slice(offset, len)
    }
}

impl AsRef<[u8]> for BufferRef {
    fn as_ref(&self) -> &[u8] {
        let slice = self.inner.as_ref().data();
        &slice[self.offset..self.offset + self.len]
    }
}

impl<Idx> Index<Idx> for BufferRef
where
    Idx: SliceIndex<[u8]>,
{
    type Output = Idx::Output;

    fn index(&self, index: Idx) -> &Self::Output {
        &self.as_ref()[index]
    }
}
