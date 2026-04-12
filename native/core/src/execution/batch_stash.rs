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

//! Global registry for passing RecordBatch values between native execution contexts
//! via opaque u64 handles, without Arrow FFI serialization.

use arrow::record_batch::RecordBatch;
use once_cell::sync::Lazy;

/// Scan source name indicating the input uses the batch stash handle path.
/// Must match the value set in CometNativeShuffleWriter.getNativePlan().
pub(crate) const HANDLE_SCAN_SOURCE: &str = "ShuffleWriterInputHandle";
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// Counter for generating unique handles.
static NEXT_HANDLE: AtomicU64 = AtomicU64::new(1);

/// Global stash mapping handles to RecordBatch values.
static STASH: Lazy<Mutex<HashMap<u64, RecordBatch>>> = Lazy::new(|| Mutex::new(HashMap::new()));

/// Store a RecordBatch in the global stash and return a unique handle.
pub(crate) fn stash(batch: RecordBatch) -> u64 {
    let handle = NEXT_HANDLE.fetch_add(1, Ordering::Relaxed);
    STASH
        .lock()
        .expect("batch_stash lock poisoned")
        .insert(handle, batch);
    handle
}

/// Remove and return the RecordBatch associated with the given handle.
///
/// Returns `None` if the handle does not exist in the stash.
pub(crate) fn take(handle: u64) -> Option<RecordBatch> {
    STASH
        .lock()
        .expect("batch_stash lock poisoned")
        .remove(&handle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_batch(values: Vec<i32>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let array = Arc::new(Int32Array::from(values));
        RecordBatch::try_new(schema, vec![array]).unwrap()
    }

    #[test]
    fn test_stash_and_take() {
        let batch = make_batch(vec![1, 2, 3]);
        let num_rows = batch.num_rows();

        let handle = stash(batch);
        let retrieved = take(handle).expect("expected batch to be present");

        assert_eq!(retrieved.num_rows(), num_rows);
        let col = retrieved
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(col.values(), &[1, 2, 3]);
    }

    #[test]
    fn test_take_removes_entry() {
        let batch = make_batch(vec![10, 20]);
        let handle = stash(batch);

        // First take returns the batch.
        assert!(take(handle).is_some());
        // Second take finds nothing.
        assert!(take(handle).is_none());
    }

    #[test]
    fn test_take_unknown_handle() {
        // Handle 0 is never issued (counter starts at 1).
        assert!(take(0).is_none());
        // A large handle that was never issued.
        assert!(take(u64::MAX).is_none());
    }

    #[test]
    fn test_handles_are_unique() {
        let batch1 = make_batch(vec![1]);
        let batch2 = make_batch(vec![2]);
        let batch3 = make_batch(vec![3]);

        let h1 = stash(batch1);
        let h2 = stash(batch2);
        let h3 = stash(batch3);

        assert_ne!(h1, h2);
        assert_ne!(h2, h3);
        assert_ne!(h1, h3);

        // Clean up.
        take(h1);
        take(h2);
        take(h3);
    }
}
