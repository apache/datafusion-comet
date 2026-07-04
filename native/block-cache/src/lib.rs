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

//! Storage-API-neutral block cache core for DataFusion Comet.
//!
//! This crate implements a block-aligned local data cache (phase 1: memory tier only) with
//! a SIEVE eviction policy, single-flight miss deduplication, coalesced miss fetches, and
//! object-version validation for immutable-file workloads.
//!
//! # Storage neutrality
//!
//! By construction this crate depends on `tokio`/`bytes`/`futures` primitives only. It must
//! never depend on `object_store`, `opendal`, or any Comet crate: the caller supplies a
//! [`RangeFetcher`] and the core never talks to storage itself. This keeps the tier
//! internals swappable and the crate extractable. The boundary is CI-enforced by the
//! `dependency_tree_is_storage_neutral` integration test.
//!
//! # Example
//!
//! ```
//! # use datafusion_comet_block_cache::*;
//! # use bytes::Bytes;
//! # use std::ops::Range;
//! # use async_trait::async_trait;
//! struct MyFetcher;
//! #[async_trait]
//! impl RangeFetcher for MyFetcher {
//!     async fn fetch(&self, ranges: &[Range<u64>]) -> Result<(Vec<Bytes>, FileVersion)> {
//!         let bytes = ranges.iter().map(|r| Bytes::from(vec![0u8; (r.end - r.start) as usize])).collect();
//!         Ok((bytes, FileVersion { size: 1024, ..Default::default() }))
//!     }
//! }
//! # async fn run() -> Result<()> {
//! let cache = BlockCache::new(BlockCacheConfig::default());
//! let file = FileKey::new(0, "s3://bucket/data.parquet");
//! let out = cache.get_ranges(&file, &[0..128], &MyFetcher).await?;
//! assert_eq!(out[0].len(), 128);
//! # Ok(()) }
//! ```

mod cache;
mod error;
mod metrics;
mod sieve;
mod version;

pub use cache::{
    BlockCache, BlockCacheConfig, RangeFetcher, DEFAULT_BLOCK_SIZE, DEFAULT_MAX_COALESCE_BYTES,
    DEFAULT_NUM_SHARDS, MAX_BLOCK_SIZE, MIN_BLOCK_SIZE,
};
pub use error::{CacheError, Result};
pub use metrics::{Metrics, MetricsSnapshot};
pub use version::{FileKey, FileVersion};
