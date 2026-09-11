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

//! Field-name folding for Spark-compatible case-insensitive Parquet reads.
//!
//! Spark's `ParquetReadSupport` resolves fields case-insensitively by grouping on
//! `name.toLowerCase(Locale.ROOT)`. Comet has to apply the same fold from three places -- the
//! top-level [`schema_adapter`](super::schema_adapter), the nested `Struct -> Struct` convert in
//! [`parquet_support`](super::parquet_support), and the plan-time projection in
//! [`parquet_exec`](super::parquet_exec). Keeping the policy in one module all three call is what
//! stops those copies from drifting apart, which is the drift that produced #5495 in the first
//! place.

use crate::jvm_bridge::{JVMClasses, StringWrapper};
use arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result as DataFusionResult};
use jni::objects::JString;
use std::collections::HashMap;
use std::sync::{OnceLock, RwLock};

/// Fold field names for case-insensitive matching under Spark's Parquet rules. In case-sensitive
/// mode names are returned unchanged. In case-insensitive mode this mirrors `ParquetReadSupport`,
/// which resolves fields by `name.toLowerCase(Locale.ROOT)`.
///
/// Pure-ASCII names are folded inline with `to_ascii_lowercase`: for an all-ASCII string this is
/// provably identical to Java's `toLowerCase(Locale.ROOT)` (`Locale.ROOT` excludes the
/// Turkish/Lithuanian rules, no ASCII codepoint lowercases to a non-ASCII one, and `Final_Sigma`
/// needs a sigma to fire), so it keeps the lock, the JVM crossing, and the fallback off the hot
/// path -- for almost every real schema every name is ASCII.
///
/// Non-ASCII names are delegated to the JVM (`CometSchemaUtils.toLowerCaseRoot`) so Comet folds
/// exactly as Spark does. Those folds are memoized process-wide (see [`fold_cache`]); the same
/// field names recur across every batch and file, so this is one JVM crossing per distinct
/// non-ASCII name for the life of the process. Outside a Comet task there is no attached JVM (e.g.
/// Rust unit tests) or the JNI call fails, so fall back to Rust's own Unicode fold (see
/// [`fold_uncached`]).
pub(crate) fn fold_names(names: &[&str], case_sensitive: bool) -> Vec<String> {
    if case_sensitive {
        return names.iter().map(|n| n.to_string()).collect();
    }

    // ASCII fast path: for an all-ASCII batch (the overwhelmingly common case) fold inline with no
    // `Option` buffer, no cache, and no JVM crossing. `to_ascii_lowercase` is provably identical to
    // Java's `toLowerCase(Locale.ROOT)` for ASCII (see the doc above).
    if names.iter().all(|n| n.is_ascii()) {
        return names.iter().map(|n| n.to_ascii_lowercase()).collect();
    }

    // Mixed batch: fold the ASCII names inline and route the non-ASCII ones through the cache/JVM.
    let mut result: Vec<Option<String>> = vec![None; names.len()];
    let mut non_ascii_positions: Vec<usize> = Vec::new();
    for (i, name) in names.iter().enumerate() {
        if name.is_ascii() {
            result[i] = Some(name.to_ascii_lowercase());
        } else {
            non_ascii_positions.push(i);
        }
    }
    // `non_ascii_positions` is non-empty here (the all-ASCII case returned above).
    fold_non_ascii(names, &non_ascii_positions, &mut result);

    result
        .into_iter()
        .map(|folded| folded.expect("every name folded"))
        .collect()
}

/// Fold the non-ASCII `names` at `positions`, writing each fold into `result`. Split out of
/// [`fold_names`] so the common all-ASCII path stays a tight loop with no cache or JVM machinery.
fn fold_non_ascii(names: &[&str], positions: &[usize], result: &mut [Option<String>]) {
    let cache = fold_cache();
    let mut miss_positions: Vec<usize> = Vec::new();
    {
        let read = cache.read().unwrap();
        for &i in positions {
            match read.get(names[i]) {
                Some(folded) => result[i] = Some(folded.clone()),
                None => miss_positions.push(i),
            }
        }
    }

    if miss_positions.is_empty() {
        return;
    }

    let miss_names: Vec<&str> = miss_positions.iter().map(|&i| names[i]).collect();
    let (folded, cacheable) = fold_uncached(&miss_names);
    if cacheable {
        let mut write = cache.write().unwrap();
        for (pos, &i) in miss_positions.iter().enumerate() {
            // Bound the process-wide cache: an executor JVM is long-lived, so stop inserting
            // once the distinct-name vocabulary grows large. Names beyond the cap still fold
            // correctly on each call, just uncached.
            if write.len() >= FOLD_CACHE_MAX_ENTRIES {
                break;
            }
            write.insert(names[i].to_string(), folded[pos].clone());
        }
    }
    for (pos, &i) in miss_positions.iter().enumerate() {
        result[i] = Some(folded[pos].clone());
    }
}

/// Fold a single field name. Convenience wrapper over [`fold_names`] for the per-column-reference
/// lookups; the schema side is always folded in bulk via [`fold_schema_names`]. Short-circuits the
/// common single-name cases (identity when case-sensitive, inline ASCII fold otherwise) so a lone
/// name never allocates a throwaway `Vec` or touches the cache; non-ASCII names use the bulk path.
pub(crate) fn fold_name(name: &str, case_sensitive: bool) -> String {
    if case_sensitive {
        return name.to_string();
    }
    if name.is_ascii() {
        return name.to_ascii_lowercase();
    }
    let mut folded = fold_names(&[name], false);
    folded
        .pop()
        .expect("fold_names returns one entry per input name")
}

/// Fold every field name in `schema`. See [`fold_names`].
pub(crate) fn fold_schema_names(schema: &SchemaRef, case_sensitive: bool) -> Vec<String> {
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    fold_names(&names, case_sensitive)
}

/// Upper bound on [`fold_cache`] entries, comfortably above any realistic distinct-field-name
/// vocabulary. Caps memory for pathological workloads (an executor reading very many wide or
/// high-cardinality schemas over its lifetime). Only ever holds non-ASCII names, since ASCII names
/// take the inline fast path in [`fold_names`].
const FOLD_CACHE_MAX_ENTRIES: usize = 100_000;

/// Process-wide memo of `original_name -> folded_name` for non-ASCII names, shared across all
/// tasks in the (long-lived) executor JVM. Insertion stops at [`FOLD_CACHE_MAX_ENTRIES`], so it
/// cannot grow without bound; names beyond the cap still fold correctly, just uncached.
fn fold_cache() -> &'static RwLock<HashMap<String, String>> {
    static CACHE: OnceLock<RwLock<HashMap<String, String>>> = OnceLock::new();
    CACHE.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Fold names that missed the cache. These are always non-ASCII (ASCII names never reach here).
/// Returns the folds and whether they may be cached: a transient JVM failure returns the fallback
/// but is NOT cached, so it cannot poison later lookups once the JVM recovers.
///
/// The fallback is Rust's `str::to_lowercase` (full Unicode), not `to_ascii_lowercase`: ASCII
/// folding leaves every non-ASCII cased letter untouched (~1367 mismatches against JDK 17),
/// whereas Rust's Unicode fold only differs from the JVM on the handful of codepoints where the
/// JDK's Unicode table version differs from Rust's (~95 against JDK 17). It also lets the
/// no-JVM path resolve names whose case mapping is stable across Unicode versions (e.g. `Ω`/`ω`,
/// `MÜNCHEN`), which is what the Rust unit tests rely on.
fn fold_uncached(names: &[&str]) -> (Vec<String>, bool) {
    if crate::JAVA_VM.get().is_some() {
        match jvm_fold_all(names) {
            Ok(folded) => return (folded, true),
            Err(e) => log::warn!(
                "JVM case-fold failed; falling back to Rust Unicode lowercasing, which can differ \
                 from Spark on codepoints where the JDK's Unicode table version differs: {e}"
            ),
        }
        (names.iter().map(|n| n.to_lowercase()).collect(), false)
    } else {
        // No attached JVM (e.g. Rust unit tests): Rust's Unicode fold is the permanent mode and is
        // cacheable.
        (names.iter().map(|n| n.to_lowercase()).collect(), true)
    }
}

/// Lower-case a batch of names via the JVM's `String.toLowerCase(Locale.ROOT)`, matching Spark's
/// `ParquetReadSupport` byte-for-byte. Folds in chunks so at most `CHUNK * 2` JNI local refs are
/// live in a single frame, keeping wide schemas within `DEFAULT_LOCAL_FRAME_CAPACITY` (32).
fn jvm_fold_all(names: &[&str]) -> DataFusionResult<Vec<String>> {
    const CHUNK: usize = 16;
    let mut folded = Vec::with_capacity(names.len());
    for chunk in names.chunks(CHUNK) {
        JVMClasses::with_env(|env| -> DataFusionResult<()> {
            // SAFETY: the JNI static call and `JString::from_raw` are sound here: the class and
            // method id are cached in `JVMClasses`, and `toLowerCaseRoot` returns a valid String
            // local ref that lives for this frame.
            unsafe {
                for name in chunk {
                    let jname = env
                        .new_string(name)
                        .map_err(|e| DataFusionError::Execution(format!("new_string: {e}")))?;
                    let lowered = jni_static_call!(
                        env,
                        comet_schema_utils.to_lower_case_root(&jname) -> StringWrapper
                    )?;
                    folded.push(
                        JString::from_raw(env, lowered.get().as_raw())
                            .try_to_string(env)
                            .map_err(|e| {
                                DataFusionError::Execution(format!("try_to_string: {e}"))
                            })?,
                    );
                }
            }
            Ok(())
        })?;
    }
    Ok(folded)
}

#[cfg(test)]
mod test {
    /// The process-wide fold cache populates on first fold of a non-ASCII name and serves repeated
    /// lookups. Runs under the Rust Unicode fallback (no JVM in `cargo test`), which exercises the
    /// cacheable path. Uses a non-ASCII name because ASCII names take the inline fast path and are
    /// intentionally never cached.
    #[test]
    fn fold_names_memoizes_case_insensitive() {
        // Unique non-ASCII name so parallel tests don't share this cache entry.
        let name = "ΩFoldMemoUnique";
        let folded = name.to_lowercase();
        let first = super::fold_names(&[name], false);
        assert_eq!(first, vec![folded.clone()]);
        assert_eq!(
            super::fold_cache()
                .read()
                .unwrap()
                .get(name)
                .map(String::as_str),
            Some(folded.as_str())
        );
        // Second call is served from the cache with the same result.
        assert_eq!(super::fold_names(&[name], false), first);
    }

    /// ASCII names take the inline fast path and must never touch the cache.
    #[test]
    fn fold_names_ascii_is_fast_path_and_uncached() {
        let name = "FoldAsciiUnique";
        assert_eq!(
            super::fold_names(&[name], false),
            vec!["foldasciiunique".to_string()]
        );
        assert!(super::fold_cache().read().unwrap().get(name).is_none());
    }

    /// Case-sensitive folding is identity and must never populate the cache.
    #[test]
    fn fold_names_case_sensitive_is_identity_and_uncached() {
        let name = "ΩFoldCaseSensitiveUnique";
        assert_eq!(super::fold_names(&[name], true), vec![name.to_string()]);
        assert!(super::fold_cache().read().unwrap().get(name).is_none());
    }
}
