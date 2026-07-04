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

use std::collections::{BTreeSet, HashMap};
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::future::FutureExt;

use crate::error::{CacheError, Result};
use crate::metrics::{Metrics, MetricsSnapshot};
use crate::sieve::{Block, BlockKey, FetchResult, InFlightFut, Shard};
use crate::version::{FileKey, FileVersion};

/// Minimum / maximum / default block size (the read quantum). Powers of two only.
pub const MIN_BLOCK_SIZE: u64 = 1 << 20; // 1 MiB
pub const MAX_BLOCK_SIZE: u64 = 16 << 20; // 16 MiB
pub const DEFAULT_BLOCK_SIZE: u64 = 4 << 20; // 4 MiB
/// Default number of memory-tier shards.
pub const DEFAULT_NUM_SHARDS: usize = 16;
/// Default cap on a single coalesced upstream fetch (4 default blocks).
pub const DEFAULT_MAX_COALESCE_BYTES: u64 = 16 << 20; // 16 MiB

/// Configuration for a [`BlockCache`].
#[derive(Clone, Debug)]
pub struct BlockCacheConfig {
    /// Block quantum in bytes. Clamped to a power of two in `[MIN_BLOCK_SIZE, MAX_BLOCK_SIZE]`.
    pub block_size: u64,
    /// Memory-tier budget in bytes, process-wide.
    pub memory_budget: u64,
    /// Number of memory-tier shards.
    pub num_shards: usize,
    /// Cap on bytes fetched in a single coalesced upstream request.
    pub max_coalesce_bytes: u64,
}

impl Default for BlockCacheConfig {
    fn default() -> Self {
        BlockCacheConfig {
            block_size: DEFAULT_BLOCK_SIZE,
            memory_budget: 512 << 20,
            num_shards: DEFAULT_NUM_SHARDS,
            max_coalesce_bytes: DEFAULT_MAX_COALESCE_BYTES,
        }
    }
}

/// Round `v` down to the largest power of two `<= v`.
fn floor_pow2(v: u64) -> u64 {
    if v == 0 {
        return 0;
    }
    1u64 << (63 - v.leading_zeros() as u64)
}

impl BlockCacheConfig {
    /// Normalize into a valid config: block size becomes a power of two within bounds,
    /// shard count is at least 1, and the coalesce cap is at least one block.
    fn normalized(mut self) -> Self {
        let clamped = self.block_size.clamp(MIN_BLOCK_SIZE, MAX_BLOCK_SIZE);
        self.block_size = floor_pow2(clamped).max(MIN_BLOCK_SIZE);
        self.num_shards = self.num_shards.max(1);
        self.max_coalesce_bytes = self.max_coalesce_bytes.max(self.block_size);
        self
    }
}

/// Fetches absolute byte ranges from the underlying storage on a cache miss.
///
/// The cache calls this exactly once per block per version regardless of how many tasks
/// concurrently miss it (single-flight). Implementations return the bytes for the
/// requested ranges plus the object version observed by the fetch, which the cache uses
/// to detect in-place overwrites.
#[async_trait]
pub trait RangeFetcher: Send + Sync {
    async fn fetch(&self, ranges: &[Range<u64>]) -> Result<(Vec<Bytes>, FileVersion)>;
}

/// Interned file identities and their captured versions.
struct FileTable {
    ids: HashMap<FileKey, u64>,
    versions: HashMap<u64, FileVersion>,
    next_id: u64,
}

/// The decision made after comparing a fetched version against the stored one.
enum VersionDecision {
    Unchanged,
    FirstSeen,
    Overwritten,
}

/// A block-aligned local data cache (memory tier) sitting behind a caller-supplied
/// [`RangeFetcher`]. Storage-API-neutral: it knows nothing about `object_store`.
pub struct BlockCache {
    block_size: u64,
    num_shards: usize,
    max_coalesce_blocks: u32,
    shards: Vec<Mutex<Shard>>,
    files: Mutex<FileTable>,
    memory_budget: AtomicU64,
    metrics: Arc<Metrics>,
}

impl BlockCache {
    /// Build a cache from `config` (normalized to valid values).
    pub fn new(config: BlockCacheConfig) -> Arc<Self> {
        let config = config.normalized();
        let per_shard_budget = config.memory_budget / config.num_shards as u64;
        let shards = (0..config.num_shards)
            .map(|_| Mutex::new(Shard::new(per_shard_budget)))
            .collect();
        let max_coalesce_blocks = (config.max_coalesce_bytes / config.block_size).max(1) as u32;
        Arc::new(BlockCache {
            block_size: config.block_size,
            num_shards: config.num_shards,
            max_coalesce_blocks,
            shards,
            files: Mutex::new(FileTable {
                ids: HashMap::new(),
                versions: HashMap::new(),
                next_id: 0,
            }),
            memory_budget: AtomicU64::new(config.memory_budget),
            metrics: Arc::new(Metrics::default()),
        })
    }

    /// The block quantum in bytes.
    pub fn block_size(&self) -> u64 {
        self.block_size
    }

    /// A snapshot of the cache counters.
    pub fn stats(&self) -> MetricsSnapshot {
        self.metrics.snapshot()
    }

    /// Serve `ranges` of `file`. Reads are quantized to blocks internally; misses go
    /// through `fetcher` exactly once per block regardless of concurrent callers. Returns
    /// one `Bytes` per input range, byte-for-byte identical to reading the store directly.
    pub async fn get_ranges(
        &self,
        file: &FileKey,
        ranges: &[Range<u64>],
        fetcher: &dyn RangeFetcher,
    ) -> Result<Vec<Bytes>> {
        if ranges.is_empty() {
            return Ok(Vec::new());
        }
        let file_id = self.intern(file);

        // Union of blocks touched by any requested range.
        let mut needed: BTreeSet<u32> = BTreeSet::new();
        for r in ranges {
            if r.start >= r.end {
                continue;
            }
            let first = (r.start / self.block_size) as u32;
            let last = ((r.end - 1) / self.block_size) as u32;
            for b in first..=last {
                needed.insert(b);
            }
        }

        // Probe the memory tier.
        let mut have: HashMap<u32, Bytes> = HashMap::with_capacity(needed.len());
        let mut missing: Vec<u32> = Vec::new();
        for &b in &needed {
            let key = (file_id, b);
            let hit = self.shard(key).lock().unwrap().get(&key);
            match hit {
                Some(block) => {
                    self.metrics.record_hit();
                    have.insert(b, block.data.clone());
                }
                None => {
                    self.metrics.record_miss();
                    missing.push(b);
                }
            }
        }

        if !missing.is_empty() {
            self.fill_missing(file_id, &missing, fetcher, &mut have)
                .await?;
        }

        // Assemble each requested range from the blocks now in `have`.
        let mut out = Vec::with_capacity(ranges.len());
        for r in ranges {
            out.push(self.assemble_range(r, &have)?);
        }
        Ok(out)
    }

    /// Fetch and cache every missing block, deduplicating concurrent fetches (single-flight)
    /// and coalescing runs of adjacent missing blocks into one upstream request.
    async fn fill_missing(
        &self,
        file_id: u64,
        missing: &[u32],
        fetcher: &dyn RangeFetcher,
        have: &mut HashMap<u32, Bytes>,
    ) -> Result<()> {
        // --- claim phase: decide which blocks we own vs. wait on ---
        let mut owned: Vec<u32> = Vec::new();
        let mut senders: HashMap<u32, tokio::sync::oneshot::Sender<FetchResult>> = HashMap::new();
        let mut waiters: Vec<(u32, InFlightFut)> = Vec::new();

        let mut blocks = missing.to_vec();
        blocks.sort_unstable();
        blocks.dedup();

        for &b in &blocks {
            let key = (file_id, b);
            let mut shard = self.shard(key).lock().unwrap();
            // Re-check: a concurrent task may have filled the block since our probe.
            if let Some(block) = shard.get(&key) {
                have.insert(b, block.data.clone());
                continue;
            }
            if let Some(fut) = shard.in_flight_get(&key) {
                waiters.push((b, fut));
                continue;
            }
            // Claim: install a shared future others can await, and own the fetch.
            let (tx, rx) = tokio::sync::oneshot::channel::<FetchResult>();
            let fut: InFlightFut = async move {
                match rx.await {
                    Ok(res) => res,
                    Err(_) => Err(CacheError::Internal(
                        "fetch owner dropped before delivering block".to_string(),
                    )),
                }
            }
            .boxed()
            .shared();
            shard.in_flight_insert(key, fut);
            drop(shard);
            owned.push(b);
            senders.insert(b, tx);
        }

        // --- fetch phase: our owned blocks, coalesced. Must happen BEFORE awaiting other
        // owners' futures so that two callers cross-owning each other's blocks cannot
        // deadlock (each fetches what it owns first, then waits). ---
        let mut first_error: Option<CacheError> = None;
        for run in coalesce_runs(&owned, self.max_coalesce_blocks) {
            let start_block = run[0];
            let end_block = *run.last().unwrap();
            let abs_start = start_block as u64 * self.block_size;
            // Over-read past EOF is fine: the store truncates a partially-out-of-bounds
            // range, yielding the short final block.
            let abs_end = (end_block as u64 + 1) * self.block_size;

            match fetcher.fetch(&[abs_start..abs_end]).await {
                Ok((bytes_vec, version)) => {
                    let full = concat_bytes(bytes_vec);
                    self.metrics.record_fetch(full.len() as u64);
                    self.reconcile_version(file_id, &version);
                    for &b in &run {
                        let off = ((b - start_block) as u64 * self.block_size) as usize;
                        let data = if off >= full.len() {
                            Bytes::new()
                        } else {
                            let end = (off + self.block_size as usize).min(full.len());
                            full.slice(off..end)
                        };
                        let block = Block::new(data);
                        self.publish_block(file_id, b, block, have, &mut senders);
                    }
                }
                Err(e) => {
                    // Fail every owned block in this run; remove the in-flight entries so
                    // the next caller retries. Errors are never cached.
                    for &b in &run {
                        let key = (file_id, b);
                        if let Some(tx) = senders.remove(&b) {
                            let _ = tx.send(Err(e.clone()));
                        }
                        self.shard(key).lock().unwrap().in_flight_remove(&key);
                    }
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
            }
        }

        // --- wait phase: blocks other tasks own ---
        for (b, fut) in waiters {
            match fut.await {
                Ok(block) => {
                    block.visited.store(true, Ordering::Relaxed);
                    have.insert(b, block.data.clone());
                }
                Err(e) => {
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
            }
        }

        match first_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Insert a fetched block into the memory tier, hand it to `have`, wake any waiters,
    /// and clear the in-flight entry.
    fn publish_block(
        &self,
        file_id: u64,
        block_index: u32,
        block: Arc<Block>,
        have: &mut HashMap<u32, Bytes>,
        senders: &mut HashMap<u32, tokio::sync::oneshot::Sender<FetchResult>>,
    ) {
        let key = (file_id, block_index);
        {
            let mut shard = self.shard(key).lock().unwrap();
            shard.insert(key, Arc::clone(&block), &self.metrics);
            shard.in_flight_remove(&key);
        }
        have.insert(block_index, block.data.clone());
        if let Some(tx) = senders.remove(&block_index) {
            // A closed receiver just means no other task waited; ignore.
            let _ = tx.send(Ok(block));
        }
    }

    /// Build one requested range's bytes from the (now cached) blocks in `have`.
    fn assemble_range(&self, r: &Range<u64>, have: &HashMap<u32, Bytes>) -> Result<Bytes> {
        if r.start >= r.end {
            return Ok(Bytes::new());
        }
        let bs = self.block_size;
        let first = (r.start / bs) as u32;
        let last = ((r.end - 1) / bs) as u32;

        if first == last {
            // Single block: zero-copy slice.
            let block = have
                .get(&first)
                .ok_or_else(|| CacheError::Internal("block missing during assembly".to_string()))?;
            let block_start = first as u64 * bs;
            let lo = (r.start - block_start) as usize;
            let hi = ((r.end - block_start).min(block.len() as u64)) as usize;
            if lo > block.len() {
                return Err(CacheError::Internal(
                    "range start beyond block during assembly".to_string(),
                ));
            }
            return Ok(block.slice(lo..hi));
        }

        let mut buf = BytesMut::with_capacity((r.end - r.start) as usize);
        for b in first..=last {
            let block = have
                .get(&b)
                .ok_or_else(|| CacheError::Internal("block missing during assembly".to_string()))?;
            let block_start = b as u64 * bs;
            let lo = r.start.max(block_start);
            let hi = r.end.min(block_start + block.len() as u64);
            if hi <= lo {
                continue;
            }
            buf.extend_from_slice(&block[(lo - block_start) as usize..(hi - block_start) as usize]);
        }
        Ok(buf.freeze())
    }

    /// Compare a freshly fetched version with the stored one; on the first overwrite
    /// detection, drop every cached block of the file and record the new version.
    fn reconcile_version(&self, file_id: u64, version: &FileVersion) {
        let decision = {
            let mut files = self.files.lock().unwrap();
            match files.versions.get(&file_id) {
                None => {
                    files.versions.insert(file_id, version.clone());
                    VersionDecision::FirstSeen
                }
                Some(existing) if existing.matches(version) => VersionDecision::Unchanged,
                Some(_) => {
                    files.versions.insert(file_id, version.clone());
                    VersionDecision::Overwritten
                }
            }
        };
        if let VersionDecision::Overwritten = decision {
            self.invalidate_blocks(file_id);
            self.metrics.record_invalidation();
        }
    }

    /// Drop every cached block of `file_id` from all shards.
    fn invalidate_blocks(&self, file_id: u64) {
        for shard in &self.shards {
            shard.lock().unwrap().remove_file_blocks(file_id);
        }
    }

    /// Drop all cached state for a file (used on `put`/`delete` at the wrapper).
    pub fn invalidate_file(&self, file: &FileKey) {
        let file_id = {
            let files = self.files.lock().unwrap();
            files.ids.get(file).copied()
        };
        if let Some(id) = file_id {
            self.invalidate_blocks(id);
            self.files.lock().unwrap().versions.remove(&id);
        }
    }

    /// Change the memory-tier budget at runtime. A reduction evicts (SIEVE order) until
    /// under the new per-shard budget before returning. Phase-1 callers never invoke this;
    /// it is the phase-2 unified-memory contract (grant/shrink) and is exercised by tests.
    pub fn set_memory_budget(&self, bytes: u64) {
        self.memory_budget.store(bytes, Ordering::Relaxed);
        let per_shard = bytes / self.num_shards as u64;
        for shard in &self.shards {
            shard.lock().unwrap().set_budget(per_shard, &self.metrics);
        }
    }

    /// The current memory-tier budget in bytes.
    pub fn memory_budget(&self) -> u64 {
        self.memory_budget.load(Ordering::Relaxed)
    }

    /// The version most recently captured for `file`, if the cache has ever fetched it.
    ///
    /// Used by the `object_store` wrapper to synthesize an `ObjectMeta` for a cache-served
    /// `get_opts` without issuing a `head` request. Returns `None` before the first fetch.
    pub fn file_version(&self, file: &FileKey) -> Option<FileVersion> {
        let files = self.files.lock().unwrap();
        let id = files.ids.get(file)?;
        files.versions.get(id).cloned()
    }

    /// Intern a file key into a compact id, assigning a new one on first sight.
    fn intern(&self, file: &FileKey) -> u64 {
        let mut files = self.files.lock().unwrap();
        if let Some(id) = files.ids.get(file) {
            return *id;
        }
        let id = files.next_id;
        files.next_id += 1;
        files.ids.insert(file.clone(), id);
        id
    }

    /// The shard owning a block key.
    fn shard(&self, key: BlockKey) -> &Mutex<Shard> {
        &self.shards[self.shard_index(key)]
    }

    fn shard_index(&self, (file_id, block_index): BlockKey) -> usize {
        // Cheap integer mix over the 12-byte key.
        let mut h = file_id.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        h ^= (block_index as u64).wrapping_mul(0xD6E8_FEB8_6659_FD93);
        h ^= h >> 29;
        (h % self.num_shards as u64) as usize
    }
}

/// Concatenate the pieces returned by a fetch into one `Bytes` (zero-copy for the common
/// single-range fetch this cache issues).
fn concat_bytes(mut pieces: Vec<Bytes>) -> Bytes {
    match pieces.len() {
        0 => Bytes::new(),
        1 => pieces.pop().unwrap(),
        _ => {
            let total: usize = pieces.iter().map(|p| p.len()).sum();
            let mut buf = BytesMut::with_capacity(total);
            for p in pieces {
                buf.extend_from_slice(&p);
            }
            buf.freeze()
        }
    }
}

/// Group sorted, deduped block indices into runs of consecutive integers, splitting any
/// run longer than `max_blocks` so no single upstream fetch exceeds the coalesce cap.
fn coalesce_runs(sorted: &[u32], max_blocks: u32) -> Vec<Vec<u32>> {
    let mut runs: Vec<Vec<u32>> = Vec::new();
    let mut cur: Vec<u32> = Vec::new();
    for &b in sorted {
        let extends = match cur.last() {
            Some(&last) => b == last + 1 && (cur.len() as u32) < max_blocks,
            None => false,
        };
        if !extends && !cur.is_empty() {
            runs.push(std::mem::take(&mut cur));
        }
        cur.push(b);
    }
    if !cur.is_empty() {
        runs.push(cur);
    }
    runs
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn floor_pow2_rounds_down() {
        assert_eq!(floor_pow2(1), 1);
        assert_eq!(floor_pow2(3), 2);
        assert_eq!(floor_pow2(5 << 20), 4 << 20);
        assert_eq!(floor_pow2(16 << 20), 16 << 20);
    }

    #[test]
    fn config_normalizes_block_size_to_pow2_in_range() {
        let c = BlockCacheConfig {
            block_size: 5 << 20,
            ..Default::default()
        }
        .normalized();
        assert_eq!(c.block_size, 4 << 20);

        let c = BlockCacheConfig {
            block_size: 100 << 20,
            ..Default::default()
        }
        .normalized();
        assert_eq!(c.block_size, MAX_BLOCK_SIZE);

        let c = BlockCacheConfig {
            block_size: 0,
            ..Default::default()
        }
        .normalized();
        assert_eq!(c.block_size, MIN_BLOCK_SIZE);
    }

    #[test]
    fn coalesce_runs_splits_gaps_and_caps() {
        // adjacent run [1,2,3] and isolated [5]
        assert_eq!(coalesce_runs(&[1, 2, 3, 5], 8), vec![vec![1, 2, 3], vec![5]]);
        // cap at 2 blocks per run
        assert_eq!(
            coalesce_runs(&[1, 2, 3, 4], 2),
            vec![vec![1, 2], vec![3, 4]]
        );
        assert!(coalesce_runs(&[], 4).is_empty());
    }
}
