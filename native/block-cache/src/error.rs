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

use std::fmt;

/// Error type for the block cache.
///
/// `Clone` is required because a single fetch drives a shared future that is awaited
/// by every task that missed the same block (single-flight, see `cache.rs`); the
/// result — including the error — is cloned to each waiter. Fetch failures are never
/// cached: the in-flight entry is removed so the next caller retries.
#[derive(Clone, Debug)]
pub enum CacheError {
    /// The caller-supplied `RangeFetcher` failed. Carries the upstream message.
    Fetch(String),
    /// An invariant inside the cache was violated (bug-class, not expected at runtime).
    Internal(String),
}

impl fmt::Display for CacheError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CacheError::Fetch(msg) => write!(f, "block cache fetch error: {msg}"),
            CacheError::Internal(msg) => write!(f, "block cache internal error: {msg}"),
        }
    }
}

impl std::error::Error for CacheError {}

impl CacheError {
    /// Build a `Fetch` error from any displayable upstream error.
    pub fn fetch(err: impl fmt::Display) -> Self {
        CacheError::Fetch(err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, CacheError>;
