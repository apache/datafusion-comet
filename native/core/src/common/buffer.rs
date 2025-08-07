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

use crate::common::bit;
use crate::execution::operators::ExecutionError;
use arrow::buffer::Buffer as ArrowBuffer;
use std::{
    alloc::{handle_alloc_error, Layout},
    ptr::NonNull,
    sync::Arc,
};

/// A buffer implementation. This is very similar to Arrow's [`MutableBuffer`] implementation,
/// except that there are two modes depending on whether `owned` is true or false.
///
/// If `owned` is true, this behaves the same way as a Arrow [`MutableBuffer`], and the struct is
/// the unique owner for the memory it wraps. The holder of this buffer can read or write the
/// buffer, and the buffer itself will be released when it goes out of scope.
///
/// Also note that, in `owned` mode, the buffer is always filled with 0s, and its length is always
/// equal to its capacity. It's up to the caller to decide which part of the buffer contains valid
/// data.
///
/// If `owned` is false, this buffer is an alias to another buffer. The buffer itself becomes
/// immutable and can only be read.
#[derive(Debug)]
pub struct CometBuffer {
    data: NonNull<u8>,
    len: usize,
    capacity: usize,
    /// Whether this buffer owns the data it points to.
    owned: bool,
    /// The allocation instance for this buffer.
    allocation: Arc<CometBufferAllocation>,
}

unsafe impl Sync for CometBuffer {}
unsafe impl Send for CometBuffer {}

/// All buffers are aligned to 64 bytes.
const ALIGNMENT: usize = 64;

impl CometBuffer {
    /// Initializes a owned buffer filled with 0.
    pub fn new(capacity: usize) -> Self {
        let aligned_capacity = bit::round_upto_power_of_2(capacity, ALIGNMENT);
        unsafe {
            let layout = Layout::from_size_align_unchecked(aligned_capacity, ALIGNMENT);
            let ptr = std::alloc::alloc_zeroed(layout);
            Self {
                data: NonNull::new(ptr).unwrap_or_else(|| handle_alloc_error(layout)),
                len: aligned_capacity,
                capacity: aligned_capacity,
                owned: true,
                allocation: Arc::new(CometBufferAllocation::new()),
            }
        }
    }

    /// Returns the capacity of this buffer.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns the length (i.e., number of bytes) in this buffer.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether this buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the data stored in this buffer as a slice.
    pub fn as_slice(&self) -> &[u8] {
        self
    }

    /// Returns the data stored in this buffer as a mutable slice.
    pub fn as_slice_mut(&mut self) -> &mut [u8] {
        debug_assert!(self.owned, "cannot modify un-owned buffer");
        self
    }

    /// Returns a raw pointer to this buffer's internal memory
    /// This pointer is guaranteed to be aligned along cache-lines.
    #[inline]
    pub const fn as_ptr(&self) -> *const u8 {
        self.data.as_ptr()
    }

    /// Returns a mutable raw pointer to this buffer's internal memory
    /// This pointer is guaranteed to be aligned along cache-lines.
    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        debug_assert!(self.owned, "cannot modify un-owned buffer");
        self.data.as_ptr()
    }

    /// Returns an immutable Arrow buffer on the content of this buffer.
    ///
    /// # Safety
    ///
    /// This function is highly unsafe since it leaks the raw pointer to the memory region that the
    /// originally this buffer is tracking. Because of this, the caller of this function is
    /// expected to make sure the returned immutable [`ArrowBuffer`] will never live longer than the
    /// this buffer. Otherwise it will result to dangling pointers.
    ///
    /// In the particular case of the columnar reader, we'll guarantee the above since the reader
    /// itself is closed at the very end, after the Spark task is completed (either successfully or
    /// unsuccessfully) through task completion listener.
    ///
    /// When re-using [`MutableVector`] in Comet native operators, across multiple input batches,
    /// because of the iterator-style pattern, the content of the original mutable buffer will only
    /// be updated once upstream operators fully consumed the previous output batch. For breaking
    /// operators, they are responsible for copying content out of the buffers.
    pub unsafe fn to_arrow(&self) -> Result<ArrowBuffer, ExecutionError> {
        let ptr = NonNull::new_unchecked(self.data.as_ptr());
        self.check_reference()?;
        Ok(ArrowBuffer::from_custom_allocation(
            ptr,
            self.len,
            Arc::<CometBufferAllocation>::clone(&self.allocation),
        ))
    }

    /// Checks if this buffer is exclusively owned by Comet. If not, an error is returned.
    /// We run this check when we want to update the buffer. If the buffer is also shared by
    /// other components, e.g. one DataFusion operator stores the buffer, Comet cannot safely
    /// modify the buffer.
    pub fn check_reference(&self) -> Result<(), ExecutionError> {
        if Arc::strong_count(&self.allocation) > 1 {
            Err(ExecutionError::GeneralError(
                "Error on modifying a buffer which is not exclusively owned by Comet".to_string(),
            ))
        } else {
            Ok(())
        }
    }

    /// Resets this buffer by filling all bytes with zeros.
    pub fn reset(&mut self) {
        debug_assert!(self.owned, "cannot modify un-owned buffer");
        unsafe {
            std::ptr::write_bytes(self.as_mut_ptr(), 0, self.len);
        }
    }

    /// Resize this buffer to the `new_capacity`. For additional bytes allocated, they are filled
    /// with 0. if `new_capacity` is less than the current capacity of this buffer, this is a no-op.
    #[inline(always)]
    pub fn resize(&mut self, new_capacity: usize) -> Result<(), ExecutionError> {
        if !self.owned {
            return Err(ExecutionError::GeneralError(
                "cannot modify un-owned buffer".to_string(),
            ));
        }
        if new_capacity > isize::MAX as usize {
            return Err(ExecutionError::GeneralError(
                "capacity too large".to_string(),
            ));
        }
        if new_capacity > self.len {
            let (ptr, new_capacity) =
                unsafe { Self::reallocate(self.data, self.capacity, new_capacity) };
            let diff = new_capacity - self.len;
            self.data = ptr;
            self.capacity = new_capacity;
            // write the value
            unsafe { self.data.as_ptr().add(self.len).write_bytes(0, diff) };
            self.len = new_capacity;
        }
        Ok(())
    }

    unsafe fn reallocate(
        ptr: NonNull<u8>,
        old_capacity: usize,
        new_capacity: usize,
    ) -> (NonNull<u8>, usize) {
        let new_capacity = bit::round_upto_power_of_2(new_capacity, ALIGNMENT);
        let new_capacity = std::cmp::max(new_capacity, old_capacity * 2);
        let raw_ptr = std::alloc::realloc(
            ptr.as_ptr(),
            Layout::from_size_align_unchecked(old_capacity, ALIGNMENT),
            new_capacity,
        );
        let ptr = NonNull::new(raw_ptr).unwrap_or_else(|| {
            handle_alloc_error(Layout::from_size_align_unchecked(new_capacity, ALIGNMENT))
        });
        (ptr, new_capacity)
    }
}

impl Drop for CometBuffer {
    fn drop(&mut self) {
        if self.owned {
            unsafe {
                std::alloc::dealloc(
                    self.data.as_ptr(),
                    Layout::from_size_align_unchecked(self.capacity, ALIGNMENT),
                )
            }
        }
    }
}

impl PartialEq for CometBuffer {
    fn eq(&self, other: &CometBuffer) -> bool {
        if self.data.as_ptr() == other.data.as_ptr() {
            return true;
        }
        if self.len != other.len {
            return false;
        }
        if self.capacity != other.capacity {
            return false;
        }
        self.as_slice() == other.as_slice()
    }
}

impl std::ops::Deref for CometBuffer {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.as_ptr(), self.capacity) }
    }
}

impl std::ops::DerefMut for CometBuffer {
    fn deref_mut(&mut self) -> &mut [u8] {
        assert!(self.owned, "cannot modify un-owned buffer");
        unsafe { std::slice::from_raw_parts_mut(self.as_mut_ptr(), self.capacity) }
    }
}

#[derive(Debug)]
struct CometBufferAllocation {}

impl CometBufferAllocation {
    fn new() -> Self {
        Self {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::buffer::Buffer as ArrowBuffer;

    impl CometBuffer {
        pub fn from_ptr(
            ptr: *const u8,
            len: usize,
            capacity: usize,
        ) -> Result<Self, ExecutionError> {
            if capacity % ALIGNMENT != 0 {
                return Err(ExecutionError::GeneralError(format!(
                    "input buffer is not aligned to {ALIGNMENT} bytes"
                )));
            }
            if ptr.is_null() {
                return Err(ExecutionError::GeneralError(
                    "cannot create CometBuffer from null pointer".to_string(),
                ));
            }
            if len > capacity {
                return Err(ExecutionError::GeneralError(
                    "cannot create CometBuffer with len > capacity".to_string(),
                ));
            }
            Ok(Self {
                data: NonNull::new(ptr as *mut u8).unwrap(),
                len,
                capacity,
                owned: false,
                allocation: Arc::new(CometBufferAllocation::new()),
            })
        }

        /// Extends this buffer (must be an owned buffer) by appending bytes from `src`,
        /// starting from `offset`.
        pub fn extend_from_slice(
            &mut self,
            offset: usize,
            src: &[u8],
        ) -> Result<(), ExecutionError> {
            if !self.owned {
                return Err(ExecutionError::GeneralError(
                    "cannot modify un-owned buffer".to_string(),
                ));
            }

            // TODO this implementation does not seem complete or correct but this code is
            // only used in the unit tests in this module, so it probably doesn't matter

            // TODO this check seems incorrect
            if offset + src.len() > self.capacity() {
                return Err(ExecutionError::GeneralError(format!(
                    "buffer overflow, offset = {}, src.len = {}, capacity = {}",
                    offset,
                    src.len(),
                    self.capacity()
                )));
            }

            unsafe {
                let dst = self.data.as_ptr().add(offset);
                std::ptr::copy_nonoverlapping(src.as_ptr(), dst, src.len())
            }

            // TODO len and capacity are not updated after extending this buffer

            Ok(())
        }
    }
    #[test]
    fn test_buffer_new() {
        let buf = CometBuffer::new(63);
        assert_eq!(64, buf.capacity());
        assert_eq!(64, buf.len());
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_resize() {
        let mut buf = CometBuffer::new(1);
        assert_eq!(64, buf.capacity());
        assert_eq!(64, buf.len());

        buf.resize(100).unwrap();
        assert_eq!(128, buf.capacity());
        assert_eq!(128, buf.len());

        // resize with less capacity is no-op
        buf.resize(20).unwrap();
        assert_eq!(128, buf.capacity());
        assert_eq!(128, buf.len());
    }

    #[test]
    fn test_extend_from_slice() {
        let mut buf = CometBuffer::new(100);
        buf.extend_from_slice(0, b"hello").unwrap();
        assert_eq!(b"hello", &buf.as_slice()[0..5]);

        buf.extend_from_slice(5, b" world").unwrap();
        assert_eq!(b"hello world", &buf.as_slice()[0..11]);

        buf.reset();
        buf.extend_from_slice(0, b"hello arrow").unwrap();
        assert_eq!(b"hello arrow", &buf.as_slice()[0..11]);
    }

    #[test]
    fn test_to_arrow() {
        let mut buf = CometBuffer::new(1);
        assert_eq!(64, buf.len());
        assert_eq!(64, buf.capacity());

        let str = b"aaaa bbbb cccc dddd";
        buf.extend_from_slice(0, str.as_slice()).unwrap();

        assert_eq!(64, buf.len());
        assert_eq!(64, buf.capacity());
        assert_eq!(b"aaaa bbbb cccc dddd", &buf.as_slice()[0..str.len()]);

        unsafe {
            let immutable_buf: ArrowBuffer = buf.to_arrow().unwrap();
            assert_eq!(64, immutable_buf.len());
            assert_eq!(str, &immutable_buf.as_slice()[0..str.len()]);
        }
    }

    #[test]
    fn test_unowned() {
        let arrow_buf = ArrowBuffer::from(b"hello comet");
        let buf = CometBuffer::from_ptr(arrow_buf.as_ptr(), arrow_buf.len(), arrow_buf.capacity())
            .unwrap();

        assert_eq!(11, buf.len());
        assert_eq!(64, buf.capacity());
        assert_eq!(b"hello comet", &buf.as_slice()[0..11]);

        unsafe {
            let arrow_buf2 = buf.to_arrow().unwrap();
            assert_eq!(arrow_buf, arrow_buf2);
        }
    }
}
