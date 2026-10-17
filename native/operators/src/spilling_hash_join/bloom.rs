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

//! Filters of the join keys of spilled buckets' build rows, for [`super::SpillingHashJoinExec`].
//!
//! A probe row of a spilled bucket is written to the bucket's probe file, to be joined once the
//! bucket's build rows are read back. If no build row of the bucket has the row's key, the row
//! matches nothing anywhere. It can then go to the join of the resident buckets instead, where no
//! build row has its key either, so that join emits it as unmatched exactly as the bucket's own
//! join would, for every join type. A Bloom filter of each spilled bucket's build keys tells most
//! such rows apart, and they never reach the disk.
//!
//! A probe row is looked up in its bucket's filter on the way to the disk, so the lookup has to
//! cost much less than writing and reading the row. Each key therefore sets four bits in one
//! 64-bit word, taken from bits of the routing hash that routing does not use at the first level,
//! and a lookup is one load and one comparison.
//!
//! How many rows a bucket ends up with is not known when it is destaged, so its filter grows the
//! way a scalable Bloom filter does: when the newest filter is full, a filter twice its size is
//! added, and a key is looked up in all of them.

use std::sync::Arc;

use datafusion::common::Result;
use datafusion::execution::memory_pool::MemoryReservation;

use super::memory::try_reserve;
use super::routing::BUCKET_BITS;

/// The bits of filter per key at a filter's capacity, for a false positive rate of about 2%.
const BITS_PER_KEY: usize = 10;
/// The capacity of a bucket's first filter is at least this many keys.
const MIN_CAPACITY: usize = 4096;

/// The bits of a routing hash that a filter may use: routing at the first level takes the top
/// `BUCKET_BITS`, so every key of one bucket has the same top bits.
const FREE_BITS: u32 = 64 - BUCKET_BITS;
// The word index takes the low 32 bits, and the four bit positions six bits each above them.
const _: () = assert!(FREE_BITS >= 32 + 4 * 6);

/// The bit positions within its word that a key sets.
fn mask(hash: u64) -> u64 {
    let bits = hash >> 32;
    (1 << (bits & 63))
        | (1 << ((bits >> 6) & 63))
        | (1 << ((bits >> 12) & 63))
        | (1 << ((bits >> 18) & 63))
}

/// A Bloom filter that sets every bit of a key in one 64-bit word.
struct WordFilter {
    words: Vec<u64>,
    keys: usize,
    capacity: usize,
}

impl WordFilter {
    fn bytes(capacity: usize) -> usize {
        Self::word_count(capacity) * size_of::<u64>()
    }

    fn word_count(capacity: usize) -> usize {
        capacity.saturating_mul(BITS_PER_KEY).div_ceil(64).max(1)
    }

    fn with_capacity(capacity: usize) -> Self {
        Self {
            words: vec![0; Self::word_count(capacity)],
            keys: 0,
            capacity,
        }
    }

    fn word(&self, hash: u64) -> usize {
        (((hash & 0xffff_ffff) * self.words.len() as u64) >> 32) as usize
    }

    fn insert(&mut self, hash: u64) {
        let word = self.word(hash);
        self.words[word] |= mask(hash);
        self.keys += 1;
    }

    fn contains(&self, hash: u64) -> bool {
        let mask = mask(hash);
        self.words[self.word(hash)] & mask == mask
    }
}

/// The filter of one spilled bucket's build keys, looked up with the routing hash of a key.
pub(super) struct KeyFilter {
    filters: Vec<WordFilter>,
    /// The memory of every key filter of the join.
    reservation: Arc<MemoryReservation>,
    /// The bytes of `filters`, reserved in `reservation`.
    bytes: usize,
}

impl KeyFilter {
    /// A filter for a bucket that holds `rows` rows, sized for twice as many, or None if the pool
    /// refuses its memory.
    pub(super) fn try_new(
        rows: usize,
        reservation: Arc<MemoryReservation>,
    ) -> Result<Option<Self>> {
        let capacity = rows.saturating_mul(2).max(MIN_CAPACITY);
        let bytes = WordFilter::bytes(capacity);
        if !try_reserve(&reservation, bytes)? {
            return Ok(None);
        }
        Ok(Some(Self {
            filters: vec![WordFilter::with_capacity(capacity)],
            reservation,
            bytes,
        }))
    }

    /// Adds the key of a routing hash. Returns false if the filter needed to grow and the pool
    /// refused, in which case the filter no longer holds every key and must be dropped.
    pub(super) fn insert(&mut self, hash: u64) -> Result<bool> {
        let newest = self.filters.last().expect("a filter");
        if newest.keys >= newest.capacity {
            let capacity = newest.capacity.saturating_mul(2);
            let bytes = WordFilter::bytes(capacity);
            if !try_reserve(&self.reservation, bytes)? {
                return Ok(false);
            }
            self.bytes += bytes;
            self.filters.push(WordFilter::with_capacity(capacity));
        }
        self.filters.last_mut().expect("a filter").insert(hash);
        Ok(true)
    }

    /// Whether a build row may have the key of a routing hash. False means that no build row of
    /// the bucket has it.
    pub(super) fn may_contain(&self, hash: u64) -> bool {
        self.filters.iter().any(|filter| filter.contains(hash))
    }
}

impl Drop for KeyFilter {
    fn drop(&mut self) {
        self.reservation.shrink(self.bytes);
    }
}

/// Rows looked up before a filter's use is judged.
const TRIAL_ROWS: usize = 65536;

/// Whether a bucket's filter is worth looking up. Every probe row of a spilled bucket is looked
/// up, but only the rows it rejects save anything, so a filter that rejects few of the first
/// rows is dropped.
#[derive(Default)]
pub(super) struct FilterUse {
    looked_up: usize,
    rejected: usize,
}

impl FilterUse {
    /// Records the lookups of one batch. Returns false once the filter has been looked up
    /// `TRIAL_ROWS` times and rejected fewer than one in eight.
    pub(super) fn record(&mut self, looked_up: usize, rejected: usize) -> bool {
        self.looked_up += looked_up;
        self.rejected += rejected;
        self.looked_up < TRIAL_ROWS || self.rejected.saturating_mul(8) >= self.looked_up
    }
}

#[cfg(test)]
mod tests {
    use datafusion::execution::memory_pool::{
        GreedyMemoryPool, MemoryConsumer, MemoryPool, UnboundedMemoryPool,
    };

    use super::*;

    fn reservation(pool: &Arc<dyn MemoryPool>) -> Arc<MemoryReservation> {
        Arc::new(MemoryConsumer::new("test").register(pool))
    }

    /// A routing hash of one bucket: the same top bits, and well-mixed others.
    fn hash(key: u64) -> u64 {
        let mut z = key.wrapping_add(0x9e37_79b9_7f4a_7c15);
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        ((z ^ (z >> 31)) >> BUCKET_BITS) | (5 << FREE_BITS)
    }

    #[test]
    fn holds_every_key_it_was_given_and_few_others() -> Result<()> {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        // Sized for 4,096 keys and given 100,000, so that it grows several times.
        let mut filter = KeyFilter::try_new(1000, reservation(&pool))?.unwrap();
        for key in 0..100_000u64 {
            assert!(filter.insert(hash(key))?);
        }
        assert!(filter.filters.len() > 1);
        assert_eq!(pool.reserved(), filter.bytes);
        assert!((0..100_000u64).all(|key| filter.may_contain(hash(key))));
        let false_positives = (100_000..200_000u64)
            .filter(|key| filter.may_contain(hash(*key)))
            .count();
        // About 2% for each filter, and the newest is not full.
        let bound = filter.filters.len() * 2_500;
        assert!(false_positives < bound, "{false_positives} false positives");
        drop(filter);
        assert_eq!(pool.reserved(), 0);
        Ok(())
    }

    #[test]
    fn a_refused_reservation_is_reported() -> Result<()> {
        let first = WordFilter::bytes(MIN_CAPACITY);
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(first + first / 2));
        let mut filter = KeyFilter::try_new(0, reservation(&pool))?.unwrap();
        // The first filter takes `MIN_CAPACITY` keys, and the one it would grow by does not fit.
        for key in 0..MIN_CAPACITY as u64 {
            assert!(filter.insert(hash(key))?);
        }
        assert!(!filter.insert(hash(MIN_CAPACITY as u64))?);
        assert!(KeyFilter::try_new(0, reservation(&pool))?.is_none());
        Ok(())
    }

    #[test]
    fn a_filter_that_rejects_little_is_dropped() {
        let mut keeps = FilterUse::default();
        assert!(keeps.record(TRIAL_ROWS / 2, TRIAL_ROWS / 4));
        assert!(keeps.record(TRIAL_ROWS / 2, TRIAL_ROWS / 4));
        let mut drops = FilterUse::default();
        assert!(drops.record(TRIAL_ROWS / 2, 0));
        assert!(!drops.record(TRIAL_ROWS / 2, TRIAL_ROWS / 16));
    }
}
