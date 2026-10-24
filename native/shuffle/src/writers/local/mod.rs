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

pub(crate) mod local_partition_writer;
mod spill;

use datafusion::execution::memory_pool::MemoryReservation;

const FALLBACK_BUFFER_SIZE: usize = 8 * 1024;

/// Returns the capacity to allocate. A refused size falls back to a small buffer charged with
/// `grow`, so the task carries on and the reservation still matches what is held.
fn reserve_buffer(reservation: &MemoryReservation, size: usize) -> usize {
    if reservation.try_grow(size).is_ok() {
        return size;
    }
    let fallback = size.min(FALLBACK_BUFFER_SIZE);
    reservation.grow(fallback);
    fallback
}
