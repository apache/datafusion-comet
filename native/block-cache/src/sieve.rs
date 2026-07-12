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

//! Memory-tier shard: a block map with a SIEVE eviction queue, byte accounting, and the
//! single-flight in-flight map. Each shard is guarded by one `Mutex` at the `BlockCache`
//! level, so all operations here run under that lock.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use futures::future::{BoxFuture, Shared};

use crate::error::CacheError;
use crate::metrics::Metrics;

/// A cached block. `data` is the block's bytes (short for the final block of a file);
/// `visited` is SIEVE's per-entry reference bit, set on every hit.
#[derive(Debug)]
pub(crate) struct Block {
    pub data: Bytes,
    pub visited: AtomicBool,
}

impl Block {
    pub(crate) fn new(data: Bytes) -> Arc<Self> {
        Arc::new(Block {
            data,
            visited: AtomicBool::new(false),
        })
    }
}

/// `(file_id, block_index)` — 12 bytes, cheap to hash and shard.
pub(crate) type BlockKey = (u64, u32);

/// Result delivered to every task waiting on a single-flight fetch. Cloned per waiter,
/// hence the `Clone` bound satisfied by `Arc<Block>` and `CacheError`.
pub(crate) type FetchResult = std::result::Result<Arc<Block>, CacheError>;

/// A shared in-flight fetch future. The first misser drives the fetch; concurrent
/// missers clone and await this.
pub(crate) type InFlightFut = Shared<BoxFuture<'static, FetchResult>>;

/// Fixed per-block bookkeeping charged on top of the block's byte length: the `BlockKey`,
/// the `Arc<Block>`, the hash-map slot, and the SIEVE queue slot. Approximate; keeps the
/// budget honest for workloads with many small final blocks.
pub(crate) const BLOCK_OVERHEAD_BYTES: u64 = 64;

fn block_cost(data_len: usize) -> u64 {
    data_len as u64 + BLOCK_OVERHEAD_BYTES
}

/// One memory-tier shard.
pub(crate) struct Shard {
    /// Live blocks, keyed within this shard.
    map: HashMap<BlockKey, Arc<Block>>,
    /// SIEVE order. `front` is the head (newest inserts); `back` is the tail (oldest).
    queue: VecDeque<BlockKey>,
    /// SIEVE hand: an index into `queue`. Maintained best-effort across structural
    /// changes and clamped on use; only correctness invariant is that it lands on a
    /// live entry when eviction runs.
    hand: usize,
    /// Current accounted bytes (block data + per-block overhead).
    current_bytes: u64,
    /// Per-shard byte budget (total budget divided across shards).
    budget: u64,
    /// Single-flight map: block key -> shared fetch future.
    in_flight: HashMap<BlockKey, InFlightFut>,
}

impl Shard {
    pub(crate) fn new(budget: u64) -> Self {
        Shard {
            map: HashMap::new(),
            queue: VecDeque::new(),
            hand: 0,
            current_bytes: 0,
            budget,
            in_flight: HashMap::new(),
        }
    }

    /// Hit path: return a clone of the block if present, setting its `visited` bit.
    pub(crate) fn get(&self, key: &BlockKey) -> Option<Arc<Block>> {
        let block = self.map.get(key)?;
        block.visited.store(true, Ordering::Relaxed);
        Some(Arc::clone(block))
    }

    /// Insert a freshly fetched block, evicting in SIEVE order until back under budget.
    /// A block already present (a concurrent fill won the race) is left as-is.
    pub(crate) fn insert(&mut self, key: BlockKey, block: Arc<Block>, metrics: &Metrics) {
        if self.map.contains_key(&key) {
            return;
        }
        let cost = block_cost(block.data.len());
        self.map.insert(key, block);
        self.queue.push_front(key);
        // push_front shifted every existing index up by one; keep the hand on the same
        // logical entry.
        if self.queue.len() > 1 {
            self.hand += 1;
        }
        self.current_bytes += cost;
        self.evict_to(self.budget, metrics);
    }

    /// Change this shard's budget and evict down to it immediately.
    pub(crate) fn set_budget(&mut self, budget: u64, metrics: &Metrics) {
        self.budget = budget;
        self.evict_to(budget, metrics);
    }

    /// Evict in SIEVE order until `current_bytes <= target` (or the shard is empty).
    fn evict_to(&mut self, target: u64, metrics: &Metrics) {
        while self.current_bytes > target && !self.map.is_empty() {
            if let Some(cost) = self.evict_one() {
                self.current_bytes -= cost;
                metrics.record_eviction();
            } else {
                break;
            }
        }
    }

    /// Evict exactly one block per SIEVE: walk from the hand giving visited entries a
    /// second chance (clearing their bit) until an unvisited entry is found and removed.
    /// Returns the evicted block's accounted cost.
    fn evict_one(&mut self) -> Option<u64> {
        if self.queue.is_empty() {
            return None;
        }
        // At most two full sweeps: the first clears every set bit, the second finds an
        // unvisited victim. Guard the iteration count regardless.
        let max_steps = self.queue.len() * 2 + 1;
        for _ in 0..max_steps {
            if self.hand >= self.queue.len() {
                self.hand = self.queue.len() - 1; // wrap to the tail (oldest)
            }
            let key = self.queue[self.hand];
            let visited = self
                .map
                .get(&key)
                .map(|b| b.visited.load(Ordering::Relaxed))
                .unwrap_or(false);
            if visited {
                // Survive: clear the bit and advance toward the head (lower index).
                if let Some(b) = self.map.get(&key) {
                    b.visited.store(false, Ordering::Relaxed);
                }
                self.hand = if self.hand == 0 {
                    self.queue.len() - 1
                } else {
                    self.hand - 1
                };
            } else {
                // Evict this entry.
                let cost = self
                    .map
                    .remove(&key)
                    .map(|b| block_cost(b.data.len()))
                    .unwrap_or(0);
                self.queue.remove(self.hand);
                // The hand now refers to the element that followed the victim toward the
                // tail; leave it there (clamped on next entry). Nothing to adjust.
                if self.hand >= self.queue.len() && !self.queue.is_empty() {
                    self.hand = self.queue.len() - 1;
                }
                return Some(cost);
            }
        }
        None
    }

    /// Remove every block belonging to `file_id` (version-mismatch invalidation).
    /// Returns bytes reclaimed. In-flight fetches are left untouched — they may be
    /// fetching the new version.
    pub(crate) fn remove_file_blocks(&mut self, file_id: u64) -> u64 {
        let mut reclaimed = 0;
        let to_remove: Vec<BlockKey> = self
            .map
            .keys()
            .filter(|(fid, _)| *fid == file_id)
            .copied()
            .collect();
        if to_remove.is_empty() {
            return 0;
        }
        for key in &to_remove {
            if let Some(b) = self.map.remove(key) {
                reclaimed += block_cost(b.data.len());
            }
        }
        self.queue.retain(|k| k.0 != file_id);
        self.hand = 0;
        self.current_bytes = self.current_bytes.saturating_sub(reclaimed);
        reclaimed
    }

    // --- single-flight in-flight map ---

    pub(crate) fn in_flight_get(&self, key: &BlockKey) -> Option<InFlightFut> {
        self.in_flight.get(key).cloned()
    }

    pub(crate) fn in_flight_insert(&mut self, key: BlockKey, fut: InFlightFut) {
        self.in_flight.insert(key, fut);
    }

    pub(crate) fn in_flight_remove(&mut self, key: &BlockKey) {
        self.in_flight.remove(key);
    }
}
