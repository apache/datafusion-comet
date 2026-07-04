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

use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use datafusion_comet_block_cache::*;

/// A `RangeFetcher` backed by an in-memory byte vector. Counts fetch calls and total bytes
/// fetched, supports swapping its contents (to model an in-place overwrite), and can be
/// told to fail.
struct MockFetcher {
    data: Mutex<Vec<u8>>,
    version: Mutex<FileVersion>,
    fetch_calls: AtomicU64,
    bytes_fetched: AtomicU64,
    fail: Mutex<bool>,
}

impl MockFetcher {
    fn new(bytes: Vec<u8>, e_tag: &str) -> Arc<Self> {
        let size = bytes.len() as u64;
        Arc::new(MockFetcher {
            data: Mutex::new(bytes),
            version: Mutex::new(FileVersion {
                e_tag: Some(e_tag.to_string()),
                last_modified: Some(1),
                size,
            }),
            fetch_calls: AtomicU64::new(0),
            bytes_fetched: AtomicU64::new(0),
            fail: Mutex::new(false),
        })
    }

    fn calls(&self) -> u64 {
        self.fetch_calls.load(Ordering::SeqCst)
    }

    fn overwrite(&self, bytes: Vec<u8>, e_tag: &str) {
        let size = bytes.len() as u64;
        *self.data.lock().unwrap() = bytes;
        *self.version.lock().unwrap() = FileVersion {
            e_tag: Some(e_tag.to_string()),
            last_modified: Some(2),
            size,
        };
    }

    fn set_fail(&self, fail: bool) {
        *self.fail.lock().unwrap() = fail;
    }
}

#[async_trait]
impl RangeFetcher for MockFetcher {
    async fn fetch(&self, ranges: &[Range<u64>]) -> Result<(Vec<Bytes>, FileVersion)> {
        if *self.fail.lock().unwrap() {
            return Err(CacheError::Fetch("injected failure".to_string()));
        }
        self.fetch_calls.fetch_add(1, Ordering::SeqCst);
        let data = self.data.lock().unwrap();
        let mut out = Vec::with_capacity(ranges.len());
        for r in ranges {
            // Emulate object_store: a partially-out-of-bounds range is truncated to EOF.
            let start = (r.start as usize).min(data.len());
            let end = (r.end as usize).min(data.len());
            self.bytes_fetched
                .fetch_add((end - start) as u64, Ordering::SeqCst);
            out.push(Bytes::copy_from_slice(&data[start..end]));
        }
        Ok((out, self.version.lock().unwrap().clone()))
    }
}

fn seq(n: usize) -> Vec<u8> {
    (0..n).map(|i| (i % 251) as u8).collect()
}

fn small_cache() -> Arc<BlockCache> {
    // 1 MiB blocks, generous budget, few shards for deterministic-ish coverage.
    BlockCache::new(BlockCacheConfig {
        block_size: MIN_BLOCK_SIZE,
        memory_budget: 64 << 20,
        num_shards: 4,
        max_coalesce_bytes: DEFAULT_MAX_COALESCE_BYTES,
    })
}

#[tokio::test]
async fn serves_bytes_and_caches_across_calls() {
    let bs = MIN_BLOCK_SIZE as usize;
    let data = seq(bs * 3 + 12345); // 3 full blocks + a short final block
    let fetcher = MockFetcher::new(data.clone(), "v1");
    let cache = small_cache();
    let file = FileKey::new(7, "s3://b/f.parquet");

    // A cross-block range with unaligned start and end.
    let r: Range<u64> = 500..(bs as u64 * 2 + 900);
    let out = cache.get_ranges(&file, &[r.clone()], &*fetcher).await.unwrap();
    assert_eq!(&out[0][..], &data[r.start as usize..r.end as usize]);
    let calls_after_first = fetcher.calls();
    assert!(calls_after_first >= 1);

    // Second identical read: zero new upstream fetches.
    let out2 = cache.get_ranges(&file, &[r.clone()], &*fetcher).await.unwrap();
    assert_eq!(&out2[0][..], &data[r.start as usize..r.end as usize]);
    assert_eq!(fetcher.calls(), calls_after_first, "second read must be a full cache hit");
}

#[tokio::test]
async fn short_final_block_and_multi_range() {
    let bs = MIN_BLOCK_SIZE as usize;
    let data = seq(bs + 777); // one full block + a short second block
    let fetcher = MockFetcher::new(data.clone(), "v1");
    let cache = small_cache();
    let file = FileKey::new(0, "f");

    // Read the very end (short block), the footer bytes, and a mid-file range at once.
    let ranges = vec![
        (data.len() as u64 - 100)..data.len() as u64,
        10..50,
        (bs as u64 - 20)..(bs as u64 + 20), // straddles the block boundary
    ];
    let out = cache.get_ranges(&file, &ranges, &*fetcher).await.unwrap();
    for (o, r) in out.iter().zip(&ranges) {
        assert_eq!(&o[..], &data[r.start as usize..r.end as usize]);
    }
}

#[tokio::test]
async fn coalesces_adjacent_missing_blocks() {
    let bs = MIN_BLOCK_SIZE as usize;
    let data = seq(bs * 5);
    let fetcher = MockFetcher::new(data.clone(), "v1");
    let cache = small_cache();
    let file = FileKey::new(0, "f");

    // Blocks 0..=3 all missing and adjacent -> should coalesce into one fetch (<=16 MiB).
    let out = cache
        .get_ranges(&file, &[0..(bs as u64 * 4)], &*fetcher)
        .await
        .unwrap();
    assert_eq!(&out[0][..], &data[0..bs * 4]);
    assert_eq!(fetcher.calls(), 1, "four adjacent blocks should coalesce to one fetch");
}

#[tokio::test]
async fn does_not_refetch_cached_blocks_to_bridge_gaps() {
    let bs = MIN_BLOCK_SIZE as usize;
    let data = seq(bs * 4);
    let fetcher = MockFetcher::new(data.clone(), "v1");
    let cache = small_cache();
    let file = FileKey::new(0, "f");

    // Warm block 1 only.
    cache
        .get_ranges(&file, &[(bs as u64)..(bs as u64 + 10)], &*fetcher)
        .await
        .unwrap();
    let after_warm = fetcher.calls();

    // Now read blocks 0,1,2,3. Block 1 is cached, so 0 and 2,3 must fetch as two separate
    // runs (the cached block 1 splits them) — not one big run re-fetching block 1.
    let out = cache
        .get_ranges(&file, &[0..(bs as u64 * 4)], &*fetcher)
        .await
        .unwrap();
    assert_eq!(&out[0][..], &data[0..bs * 4]);
    assert_eq!(
        fetcher.calls() - after_warm,
        2,
        "cached block must split the fetch into two runs, not be re-fetched"
    );
}

#[tokio::test]
async fn single_flight_dedups_concurrent_missers() {
    let bs = MIN_BLOCK_SIZE as usize;
    let data = seq(bs * 2);
    let fetcher = MockFetcher::new(data.clone(), "v1");
    let cache = small_cache();
    let file = FileKey::new(0, "f");

    // Launch many concurrent readers of the same block.
    let mut handles = Vec::new();
    for _ in 0..32 {
        let c = Arc::clone(&cache);
        let f = Arc::clone(&fetcher);
        let file = file.clone();
        handles.push(tokio::spawn(async move {
            let out = c.get_ranges(&file, &[0..(bs as u64)], &*f).await.unwrap();
            assert_eq!(out[0].len(), bs);
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    assert_eq!(fetcher.calls(), 1, "concurrent missers must trigger exactly one fetch");
}

#[tokio::test]
async fn fetch_error_propagates_and_is_not_cached() {
    let bs = MIN_BLOCK_SIZE as usize;
    let data = seq(bs);
    let fetcher = MockFetcher::new(data.clone(), "v1");
    let cache = small_cache();
    let file = FileKey::new(0, "f");

    fetcher.set_fail(true);
    let err = cache.get_ranges(&file, &[0..10], &*fetcher).await;
    assert!(err.is_err(), "fetch failure must surface");

    // Error must not be cached: after recovery, the next read succeeds and fetches.
    fetcher.set_fail(false);
    let out = cache.get_ranges(&file, &[0..10], &*fetcher).await.unwrap();
    assert_eq!(&out[0][..], &data[0..10]);
}

#[tokio::test]
async fn version_mismatch_invalidates_and_serves_fresh() {
    let bs = MIN_BLOCK_SIZE as usize;
    let v1 = seq(bs * 2);
    let fetcher = MockFetcher::new(v1.clone(), "v1");
    let cache = small_cache();
    let file = FileKey::new(0, "f");

    // Warm block 0.
    let out = cache.get_ranges(&file, &[0..100], &*fetcher).await.unwrap();
    assert_eq!(&out[0][..], &v1[0..100]);

    // Overwrite the object in place with different content + new ETag.
    let v2: Vec<u8> = v1.iter().map(|b| b.wrapping_add(17)).collect();
    fetcher.overwrite(v2.clone(), "v2");

    // A miss on a DIFFERENT block piggybacks the new version and invalidates block 0.
    cache
        .get_ranges(&file, &[(bs as u64)..(bs as u64 + 10)], &*fetcher)
        .await
        .unwrap();

    // Reading block 0 again now returns the fresh (v2) bytes, not the stale cached ones.
    let out = cache.get_ranges(&file, &[0..100], &*fetcher).await.unwrap();
    assert_eq!(&out[0][..], &v2[0..100], "must serve post-overwrite bytes");
    assert!(cache.stats().invalidations >= 1);
}

#[tokio::test]
async fn sieve_keeps_reused_block_evicts_one_hit_wonder() {
    // Budget for exactly ~2 blocks in a single shard, so inserting a 3rd forces eviction.
    let bs = MIN_BLOCK_SIZE;
    let cache = BlockCache::new(BlockCacheConfig {
        block_size: bs,
        memory_budget: bs * 2 + 4096, // room for 2 blocks + overhead
        num_shards: 1,
        max_coalesce_bytes: bs, // force one fetch per block for determinism
    });
    let data = seq(bs as usize * 10);
    let fetcher = MockFetcher::new(data.clone(), "v1");
    let file = FileKey::new(0, "f");

    let read_block = |c: Arc<BlockCache>, f: Arc<MockFetcher>, idx: u64| async move {
        let start = idx * bs;
        c.get_ranges(&file_clone(), &[start..start + 8], &*f).await.unwrap();
    };
    fn file_clone() -> FileKey {
        FileKey::new(0, "f")
    }

    // Insert block 0, then re-read it so its `visited` bit is set (it is "reused").
    read_block(Arc::clone(&cache), Arc::clone(&fetcher), 0).await;
    read_block(Arc::clone(&cache), Arc::clone(&fetcher), 0).await; // hit -> visited
    // Insert block 1 (one-hit-wonder, never revisited).
    read_block(Arc::clone(&cache), Arc::clone(&fetcher), 1).await;
    // Insert block 2 -> forces one eviction. SIEVE should evict the unvisited block 1,
    // keeping the reused block 0.
    read_block(Arc::clone(&cache), Arc::clone(&fetcher), 2).await;

    let calls_before = fetcher.calls();
    // Block 0 should still be cached (no fetch); block 1 should have been evicted (fetch).
    read_block(Arc::clone(&cache), Arc::clone(&fetcher), 0).await;
    assert_eq!(fetcher.calls(), calls_before, "reused block 0 must survive eviction");
    read_block(Arc::clone(&cache), Arc::clone(&fetcher), 1).await;
    assert_eq!(fetcher.calls(), calls_before + 1, "one-hit block 1 must have been evicted");
}

#[tokio::test]
async fn set_memory_budget_shrink_evicts_then_growth_readmits() {
    let bs = MIN_BLOCK_SIZE;
    let cache = BlockCache::new(BlockCacheConfig {
        block_size: bs,
        memory_budget: bs * 8,
        num_shards: 1,
        max_coalesce_bytes: bs,
    });
    let data = seq(bs as usize * 8);
    let fetcher = MockFetcher::new(data.clone(), "v1");
    let file = FileKey::new(0, "f");

    // Fill several blocks.
    for i in 0..6u64 {
        cache
            .get_ranges(&file, &[i * bs..i * bs + 8], &*fetcher)
            .await
            .unwrap();
    }
    let filled_calls = fetcher.calls();

    // Shrink to hold ~1 block: must evict down immediately.
    cache.set_memory_budget(bs + 4096);
    assert!(cache.stats().evictions >= 1);

    // Reading an old block now misses (was evicted) -> a new fetch.
    cache.get_ranges(&file, &[0..8], &*fetcher).await.unwrap();
    assert!(fetcher.calls() > filled_calls, "shrunk cache must have evicted and re-fetched");

    // Grow back: subsequent fills are re-admitted (cache holds more again).
    cache.set_memory_budget(bs * 8);
    let before = fetcher.calls();
    cache.get_ranges(&file, &[0..8], &*fetcher).await.unwrap(); // just fetched above -> hit
    assert_eq!(fetcher.calls(), before, "recently filled block stays after growth");
}

#[tokio::test]
async fn invalidate_file_drops_blocks() {
    let bs = MIN_BLOCK_SIZE as usize;
    let data = seq(bs);
    let fetcher = MockFetcher::new(data.clone(), "v1");
    let cache = small_cache();
    let file = FileKey::new(3, "f");

    cache.get_ranges(&file, &[0..10], &*fetcher).await.unwrap();
    let before = fetcher.calls();
    cache.invalidate_file(&file);
    cache.get_ranges(&file, &[0..10], &*fetcher).await.unwrap();
    assert_eq!(fetcher.calls(), before + 1, "invalidated file must be re-fetched");
}

#[tokio::test]
async fn byte_exact_over_randomized_ranges() {
    let bs = MIN_BLOCK_SIZE as usize;
    let data = seq(bs * 4 + 4242);
    let fetcher = MockFetcher::new(data.clone(), "v1");
    let cache = small_cache();
    let file = FileKey::new(0, "f");

    // Deterministic pseudo-random ranges (no rand dep): LCG.
    let mut state: u64 = 0x1234_5678;
    let mut next = || {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        state >> 16
    };
    for _ in 0..200 {
        let a = (next() as usize) % data.len();
        let b = (next() as usize) % data.len();
        let (lo, hi) = if a <= b { (a, b) } else { (b, a) };
        let hi = (hi + 1).min(data.len());
        if lo == hi {
            continue;
        }
        let out = cache
            .get_ranges(&file, &[lo as u64..hi as u64], &*fetcher)
            .await
            .unwrap();
        assert_eq!(&out[0][..], &data[lo..hi], "byte mismatch for range {lo}..{hi}");
    }
}
