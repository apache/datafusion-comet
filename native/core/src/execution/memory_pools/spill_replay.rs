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

//! Lets a final hash aggregate read its spill files back past its share of the pool.
//!
//! Once DataFusion 55's `FinalHashAggregateStream` has spilled, it merges its sorted spill files
//! and replays them through an `OrderedFinalAggregateStream` that has no way to spill, so a
//! refused memory request there fails the task (#6254). The merge reserves read buffers for as
//! many spill files as fit, and those buffers belong to the same consumer, so the replay often
//! finds the consumer's share already taken. The pools record such a request as overcommit
//! instead of refusing it. The replay emits every finished group after each batch, so it holds
//! about one batch of groups, and releasing memory repays the overcommit first.
//!
//! Remove this once Comet's DataFusion has apache/datafusion#25383, which leaves the replay room
//! when the merge picks its files.

use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};

/// Whether `consumer` belongs to a `FinalHashAggregateStream`, whose spill replay can't spill.
pub(super) fn is_final_hash_aggregate(consumer: &MemoryConsumer) -> bool {
    consumer.name().starts_with("FinalHashAggregateStream[")
}

/// Whether a refused request from `reservation` should be recorded as overcommit instead.
/// `consumer_used` is what the reservation's consumer holds across all of its reservations.
///
/// A final hash aggregate grows one reservation while another of its reservations holds memory
/// only during the replay, while the merge holds its read buffers. Before that, the aggregate's
/// table is its only reservation holding memory, so a refusal makes it spill. The merge picks its
/// files while nothing else is held, so a refusal there still limits how many it opens.
pub(super) fn is_spill_replay(reservation: &MemoryReservation, consumer_used: usize) -> bool {
    consumer_used > reservation.size() && is_final_hash_aggregate(reservation.consumer())
}
