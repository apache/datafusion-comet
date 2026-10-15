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

use regex::Regex;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, PoisonError};

/// Per-expression cache for a compiled regex. The regexp scalar functions receive their
/// pattern as a per-batch scalar argument even though the serde only plans them with literal
/// patterns, so without this every batch would pay a full regex compile. One slot is enough:
/// a given expression instance sees a single pattern for the lifetime of its plan.
pub struct PatternCache {
    cached: Mutex<Option<(String, Regex)>>,
    #[cfg(test)]
    compile_count: AtomicUsize,
}

impl PatternCache {
    pub fn new() -> Self {
        Self {
            cached: Mutex::new(None),
            #[cfg(test)]
            compile_count: AtomicUsize::new(0),
        }
    }

    /// Return the compiled regex for `pattern`, compiling and caching it only when the
    /// pattern differs from the previously cached one. `Regex` clones share the compiled
    /// program, so handing out clones is cheap.
    pub fn get_or_compile(&self, pattern: &str) -> Result<Regex, regex::Error> {
        // A poisoned lock only means another thread panicked mid-update; the slot is either
        // intact or about to be refilled, so recover rather than propagate the panic.
        let mut slot = self.cached.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some((cached_pattern, regex)) = slot.as_ref() {
            if cached_pattern == pattern {
                return Ok(regex.clone());
            }
        }
        #[cfg(test)]
        self.compile_count.fetch_add(1, Ordering::Relaxed);
        let regex = Regex::new(pattern)?;
        *slot = Some((pattern.to_string(), regex.clone()));
        Ok(regex)
    }

    /// Number of times a regex was actually compiled, for asserting the cache works.
    #[cfg(test)]
    pub(crate) fn compile_count(&self) -> usize {
        self.compile_count.load(Ordering::Relaxed)
    }
}

impl Default for PatternCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compiles_once_for_repeated_pattern() {
        let cache = PatternCache::new();
        for _ in 0..5 {
            let re = cache.get_or_compile(r"(\d+)-(\d+)").unwrap();
            assert!(re.is_match("12-34"));
        }
        assert_eq!(cache.compile_count(), 1);
    }

    #[test]
    fn recompiles_when_pattern_changes() {
        let cache = PatternCache::new();
        cache.get_or_compile(r"\d+").unwrap();
        cache.get_or_compile(r"[a-z]+").unwrap();
        // Switching back replaces the single slot again.
        cache.get_or_compile(r"\d+").unwrap();
        assert_eq!(cache.compile_count(), 3);
    }

    #[test]
    fn invalid_pattern_errors_and_is_not_cached() {
        let cache = PatternCache::new();
        assert!(cache.get_or_compile(r"(unclosed").is_err());
        // A later valid pattern still works.
        let re = cache.get_or_compile(r"ok").unwrap();
        assert!(re.is_match("ok"));
    }
}
