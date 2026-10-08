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

//! Allocation admission for integral sequence output buffers.

use std::alloc::{alloc, dealloc, Layout};
use std::mem::MaybeUninit;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use arrow::alloc::ALIGNMENT;
use arrow::buffer::Buffer;
use datafusion::common::{DataFusionError, Result};

pub const DEFAULT_SEQUENCE_MAX_BYTES: usize = 256 * 1024 * 1024;
pub const SEQUENCE_MAX_BYTES_CONFIG: &str = "spark.comet.exec.sequence.maxBytesPerExecutor";

/// Bounds live sequence backing allocations. This allowance is separate from the spillable
/// operator memory pool: a scalar expression cannot spill, wait, or reclaim retained outputs.
#[derive(Debug)]
pub struct SequenceMemoryPool {
    used: Arc<AtomicUsize>,
    limit: usize,
}

impl SequenceMemoryPool {
    /// Share outstanding bytes across all plans, tasks, and Spark contexts in this process.
    /// Only the counter has process lifetime. Each executor context supplies its startup limit;
    /// recreating a context never forgives buffers still held by Arrow or the JVM.
    pub fn executor(limit: usize) -> Arc<Self> {
        static USED: OnceLock<Arc<AtomicUsize>> = OnceLock::new();
        Arc::new(Self {
            used: Arc::clone(USED.get_or_init(|| Arc::new(AtomicUsize::new(0)))),
            limit,
        })
    }

    /// An isolated allowance for embedded callers and tests.
    pub fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            used: Arc::new(AtomicUsize::new(0)),
            limit,
        })
    }

    pub fn reserved(&self) -> usize {
        self.used.load(Ordering::Acquire)
    }

    pub(super) fn reserve(self: &Arc<Self>, bytes: usize) -> Result<Reservation> {
        self.used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes).filter(|total| *total <= self.limit)
            })
            .map_err(|used| {
                DataFusionError::ResourcesExhausted(format!(
                    "Integral sequence requires {bytes} bytes; {used} bytes are already held \
                     against the executor allowance of {} bytes ({SEQUENCE_MAX_BYTES_CONFIG}). \
                     Lower spark.comet.batchSize or the sequence length, or increase the \
                     allowance at application startup. Retained outputs cannot spill or wait.",
                    self.limit
                ))
            })?;
        Ok(Reservation {
            pool: Arc::clone(self),
            bytes,
        })
    }
}

pub(super) fn default_sequence_pool() -> &'static Arc<SequenceMemoryPool> {
    static POOL: OnceLock<Arc<SequenceMemoryPool>> = OnceLock::new();
    POOL.get_or_init(|| SequenceMemoryPool::executor(DEFAULT_SEQUENCE_MAX_BYTES))
}

/// A whole invocation is admitted atomically, then its charge is split between its buffers.
/// Any remainder is returned on errors or unwind during partial construction.
pub(super) struct Reservation {
    pool: Arc<SequenceMemoryPool>,
    bytes: usize,
}

impl Reservation {
    fn split(&mut self, bytes: usize) -> Self {
        assert!(bytes <= self.bytes);
        self.bytes -= bytes;
        Self {
            pool: Arc::clone(&self.pool),
            bytes,
        }
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.pool.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

pub(super) fn size_error() -> DataFusionError {
    DataFusionError::ResourcesExhausted("Integral sequence buffer size overflow".to_string())
}

/// Round each backing allocation independently. The allocation uses exactly this Layout, so
/// there is no Vec growth or uncharged capacity. Allocator metadata and retained pages, as for
/// other native allocations, are not part of the live backing-buffer allowance.
pub(super) fn buffer_layout(bytes: usize) -> Result<Layout> {
    let size = bytes.checked_add(ALIGNMENT - 1).ok_or_else(size_error)? & !(ALIGNMENT - 1);
    Layout::from_size_align(size, ALIGNMENT).map_err(|_| size_error())
}

/// Owns a fixed-capacity allocation through construction and Arrow sharing. No references to a
/// task, plan, or JVM are needed to release it. Each allocation releases its own credit only
/// after deallocation, including when the last owner is a sliced child or an FFI release callback.
pub(super) struct SequenceBuffer {
    ptr: NonNull<u8>,
    layout: Layout,
    _reservation: Reservation,
}

// Construction mutates only through &mut self. After conversion to Buffer, all shared access is
// immutable, and the Arc owner cannot be recovered as a mutable SequenceBuffer.
unsafe impl Send for SequenceBuffer {}
unsafe impl Sync for SequenceBuffer {}

impl SequenceBuffer {
    pub(super) fn new(layout: Layout, reservation: &mut Reservation) -> Result<Self> {
        let charge = reservation.split(layout.size());
        let ptr = if layout.size() == 0 {
            // A zero-sized allocation must not be passed to alloc/dealloc. Keep the pointer
            // aligned for the primitive slices used during construction, including empty ones.
            NonNull::new(ALIGNMENT as *mut u8).unwrap()
        } else {
            // SAFETY: layout has nonzero size and was validated before admission.
            NonNull::new(unsafe { alloc(layout) }).ok_or_else(|| {
                DataFusionError::ResourcesExhausted(format!(
                    "Allocator refused {} bytes for integral sequence",
                    layout.size()
                ))
            })?
        };
        Ok(Self {
            ptr,
            layout,
            _reservation: charge,
        })
    }

    /// Uninitialised output space, never exposed to Arrow until the caller has filled it.
    pub(super) fn slots<T>(&mut self, len: usize) -> &mut [MaybeUninit<T>] {
        assert!(std::mem::align_of::<T>() <= self.layout.align());
        assert!(len
            .checked_mul(std::mem::size_of::<T>())
            .is_some_and(|n| n <= self.layout.size()));
        // SAFETY: the allocation is aligned and large enough, MaybeUninit permits unwritten
        // slots, and &mut self guarantees unique access throughout construction.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr().cast(), len) }
    }

    /// # Safety
    /// The first `len` bytes must have been initialised by the caller.
    pub(super) unsafe fn finish(self, len: usize) -> Buffer {
        assert!(len <= self.layout.size());
        // Initialise the alignment padding so the entire custom allocation is valid. Expose its
        // full capacity to Arrow's memory reporting, then slice to the logical length.
        unsafe {
            self.ptr
                .as_ptr()
                .add(len)
                .write_bytes(0, self.layout.size() - len)
        };
        let ptr = self.ptr;
        let capacity = self.layout.size();
        // SAFETY: all capacity bytes are initialised, and the custom owner retains exactly
        // this allocation until every Arrow buffer clone/slice/FFI owner has released it.
        unsafe { Buffer::from_custom_allocation(ptr, capacity, Arc::new(self)) }
            .slice_with_length(0, len)
    }
}

impl Drop for SequenceBuffer {
    fn drop(&mut self) {
        if self.layout.size() != 0 {
            // SAFETY: ptr was returned by alloc with this Layout and is freed exactly once.
            unsafe { dealloc(self.ptr.as_ptr(), self.layout) };
        }
        // The reservation field drops after the backing allocation has been freed.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;

    #[test]
    fn rounding_and_partial_construction_release() {
        for bytes in [0, 1, ALIGNMENT - 1, ALIGNMENT, ALIGNMENT + 1] {
            assert_eq!(
                buffer_layout(bytes).unwrap().size(),
                bytes.div_ceil(ALIGNMENT) * ALIGNMENT
            );
        }
        assert!(buffer_layout(usize::MAX).is_err());
        let pool = SequenceMemoryPool::new(2 * ALIGNMENT);
        let mut reservation = pool.reserve(2 * ALIGNMENT).unwrap();
        let buffer = SequenceBuffer::new(buffer_layout(1).unwrap(), &mut reservation).unwrap();
        assert_eq!(pool.reserved(), 2 * ALIGNMENT);
        drop(reservation);
        assert_eq!(pool.reserved(), ALIGNMENT);
        drop(buffer);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn concurrent_admission_is_atomic_and_recovers() {
        let pool = SequenceMemoryPool::new(2 * ALIGNMENT);
        let start = Barrier::new(8);
        let admitted = Barrier::new(8);
        let results = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| {
                    scope.spawn(|| {
                        start.wait();
                        let reservation = pool.reserve(ALIGNMENT);
                        admitted.wait();
                        reservation.is_ok()
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert_eq!(results.into_iter().filter(|ok| *ok).count(), 2);
        assert_eq!(pool.reserved(), 0);
        assert!(pool.reserve(2 * ALIGNMENT).is_ok());
    }

    #[test]
    fn a_new_context_keeps_existing_credit() {
        // Use an isolated shared counter so other parallel tests cannot affect this assertion.
        let old = SequenceMemoryPool::new(2 * ALIGNMENT);
        let current = Arc::new(SequenceMemoryPool {
            used: Arc::clone(&old.used),
            limit: ALIGNMENT,
        });
        let reservation = old.reserve(ALIGNMENT).unwrap();
        drop(old);
        assert!(current.reserve(1).is_err());
        drop(reservation);
        assert!(current.reserve(ALIGNMENT).is_ok());
        assert!(Arc::ptr_eq(
            &SequenceMemoryPool::executor(1).used,
            &SequenceMemoryPool::executor(2).used
        ));
    }
}
