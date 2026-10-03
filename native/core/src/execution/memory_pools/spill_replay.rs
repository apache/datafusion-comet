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

//! Lets a final aggregate read its spill files back past its share of memory.
//!
//! Once one of DataFusion 55's final aggregates has spilled, it merges its sorted spill files and
//! replays them through an `OrderedFinalAggregateStream` that has no way to spill, so a refused
//! memory request there fails the task (#6254). `FinalHashAggregateStream` does this, and so does
//! `OrderedFinalAggregateStream` itself, which DataFusion uses when the input is sorted on some of
//! the grouping keys. The merge reserves read buffers for as many spill files as fit, and those
//! buffers belong to the same consumer, so the replay often finds the consumer's share already
//! taken. The replay only asks for memory once it has aggregated a batch, so like a `grow`, the
//! request is for memory that already exists. The pools record it the way they record a `grow`,
//! carrying what Spark doesn't grant as overcommit. The replay emits every finished group after
//! each batch, so it holds about one batch of groups, and releasing memory repays the overcommit
//! first.
//!
//! Remove this once Comet's DataFusion has apache/datafusion#25383, which leaves the replay room
//! when the merge picks its files. The #6254 tests in `CometAggregateSuite` fail without this on
//! DataFusion 55.1, so they show whether the replay still needs it.

use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};

/// Whether `consumer` belongs to a final aggregate whose spill replay can't spill.
pub(super) fn is_final_aggregate(consumer: &MemoryConsumer) -> bool {
    let name = consumer.name();
    name.starts_with("FinalHashAggregateStream[")
        || name.starts_with("OrderedFinalAggregateStream[")
}

/// Whether a refused request from `reservation` should be recorded instead. `consumer_used` is
/// what the reservation's consumer holds across all of its reservations.
///
/// A final aggregate grows one reservation while another of its reservations holds memory only
/// during the replay, while the merge holds its read buffers. Before that, the aggregate's table
/// is its only reservation holding memory, so a refusal stands and makes it spill. The merge picks
/// its files while nothing else is held, so a refusal there still limits how many it opens.
pub(super) fn is_spill_replay(reservation: &MemoryReservation, consumer_used: usize) -> bool {
    consumer_used > reservation.size() && is_final_aggregate(reservation.consumer())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_final_aggregates_replay_their_spill_files() {
        for name in [
            "FinalHashAggregateStream[3]",
            "OrderedFinalAggregateStream[0]",
        ] {
            assert!(is_final_aggregate(&MemoryConsumer::new(name)), "{name}");
        }
        // Partial aggregates emit early instead of spilling, and Comet never plans single ones.
        for name in [
            "PartialHashAggregateStream[0]",
            "OrderedPartialAggregateStream[0]",
            "SingleHashAggregateStream[0]",
            "ExternalSorterMerge[0]",
        ] {
            assert!(!is_final_aggregate(&MemoryConsumer::new(name)), "{name}");
        }
    }
}
