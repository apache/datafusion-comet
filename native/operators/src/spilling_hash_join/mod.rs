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

//! A shuffled hash join that spills to disk instead of failing when its build side does not fit
//! in memory.
//!
//! [`SpillingHashJoinExec`] wraps the `HashJoinExec` that Comet plans for a shuffled hash join, and
//! never implements join semantics itself. Two rows of an equi-join can only match if their keys
//! are equal, and equal keys hash alike, so splitting both sides by a hash of the keys splits the
//! join exactly, for every join type: whether a row matched depends only on rows in its bucket.
//! Every bucket is then joined by a stock `HashJoinExec`, an "inner join", built from the one
//! Comet planned. Null keys match nothing, and a probe row with a null key is joined with the
//! buckets held in memory, where it is emitted as unmatched if the join type requires it.
//!
//! - While the build side fits, the operator runs that `HashJoinExec` on the buffered build
//!   batches and the probe input as is.
//! - When the memory pool first refuses, it routes the build rows to 16 buckets by the top bits of
//!   the hash, and destages the largest bucket to a spill file whenever memory runs out again.
//!   The buckets left in memory are joined with the probe input, while the probe rows of spilled
//!   buckets are written to the buckets' probe files.
//! - Each spilled bucket is then joined from its files, one at a time. A bucket that does not fit
//!   is split on the next bits of the hash, up to [`routing::MAX_LEVEL`] levels. A bucket that
//!   splitting cannot shrink holds the rows of one key, and is joined in chunks: see [`chunked`].
//! - An inner join whose probe rows match many build rows each switches to a sort-merge join for
//!   the rest of the probe input, and so do the spilled pairs after it: see [`fallback`].
//!
//! Memory: everything the operator holds, and everything an inner join holds, is reserved under
//! one consumer, including what `HashJoinExec` allocates without reserving. See [`memory`].
//!
//! See `docs/source/contributor-guide/spilling-hash-join-design.md` for the design.

mod bloom;
mod chunked;
mod concat;
mod exec;
mod fallback;
mod memory;
mod probe;
mod routing;
mod source;
mod spill;
mod stream;
#[cfg(test)]
mod tests;

pub use exec::{SpillingHashJoinConfig, SpillingHashJoinExec};
pub use fallback::SortMergeFallback;
