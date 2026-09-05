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

//! JVM-planned Delta handler for the generic `OpStruct::ContribScan` dispatcher, feature-gated
//! behind `delta`.
//!
//! delta-spark has already done log replay, snapshot resolution, and partition pruning by the
//! time the scan reaches Comet, so the envelope carries a concrete file list (plus deletion
//! vector descriptors) and the read path reuses the exact same shared parquet scan builder as
//! `NativeScan` -- inheriting row-group stats pruning, page-index pruning, and filter pushdown.
//! Sibling of the kernel-planned handler in `delta_scan.rs`; the two claim different
//! `type_url`s within the same `ContribScan` envelope.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::execution::object_store::ObjectStoreUrl;
use object_store::path::Path;
use object_store::ObjectStore;
use url::Url;

use datafusion_comet_proto::spark_operator::{
    ContribScan, DeltaSparkScan, Operator, SparkFilePartition, SparkPartitionedFile,
};
use prost::Message;

use crate::execution::operators::ExecutionError;
use crate::execution::operators::ExecutionError::GeneralError;
use crate::execution::planner::PhysicalPlanner;
use crate::execution::planner::PlanCreationResult;
use crate::parquet::objectstore::s3_blob_fs_support::normalize_object_store_url;
use crate::parquet::parquet_support::{
    hash_object_store_configs, object_store_url_key, prepare_object_store_with_config_hash,
};

/// Type name the JVM-planned Delta contrib claims within the `ContribScan` envelope. The
/// contrib jar packs a `DeltaSparkScan` with a `type_url` of
/// `type.googleapis.com/comet.contrib.delta_spark.DeltaSparkScan`; dispatch keys on the
/// contrib-owned suffix, same convention as the kernel path's `delta_scan.rs`.
const DELTA_SPARK_SCAN_TYPE_NAME: &str = "comet.contrib.delta_spark.DeltaSparkScan";

/// Contrib entry point for the `OpStruct::ContribScan` dispatcher. Returns `Some(result)` when
/// the envelope carries a JVM-planned Delta scan, or `None` when the `type_url` belongs to some
/// other contrib.
pub(crate) fn try_plan_contrib_scan(
    planner: &PhysicalPlanner,
    spark_plan: &Operator,
    contrib: &ContribScan,
) -> Option<PlanCreationResult> {
    if !contrib.type_url.ends_with(DELTA_SPARK_SCAN_TYPE_NAME) {
        return None;
    }
    Some(
        DeltaSparkScan::decode(contrib.value.as_slice())
            .map_err(|e| {
                GeneralError(format!(
                    "Failed to decode DeltaSparkScan from contrib_scan: {e}"
                ))
            })
            .and_then(|scan| plan_delta_spark_scan(planner, spark_plan, &scan)),
    )
}

fn plan_delta_spark_scan(
    planner: &PhysicalPlanner,
    spark_plan: &Operator,
    scan: &DeltaSparkScan,
) -> PlanCreationResult {
    // Delta data files are plain parquet; the read path deliberately reuses
    // the same shared parquet scan builder as NativeScan so Delta inherits
    // row-group stats pruning, page-index pruning, and filter pushdown. Only
    // the file list arrives in Delta-specific form. Note delta_common's
    // column_mapping_mode is informational in M1: the actual field-id
    // matching switch is common.use_field_id, same as the Iceberg path.
    let common = scan
        .common
        .as_ref()
        .ok_or_else(|| GeneralError("DeltaSparkScan missing common data".into()))?;

    let delta_partition = scan
        .file_partition
        .as_ref()
        .ok_or_else(|| GeneralError("DeltaSparkScan missing file_partition".into()))?;

    let spark_partition = SparkFilePartition {
        partitioned_file: delta_partition
            .partitioned_file
            .iter()
            .map(|f| {
                f.file.clone().ok_or_else(|| {
                    GeneralError("DeltaSparkPartitionedFile missing inner file".into())
                })
            })
            .collect::<Result<Vec<_>, _>>()?,
    };

    // Defense-in-depth against a stale or bypassed JVM gate: DeltaScanSupport.declineReason
    // (multiStoreReason) already declines data files spanning multiple object-store authorities
    // at planning time, but prepare_scan_store_and_files below resolves this whole partition's
    // ObjectStoreUrl from the FIRST file only and then strips every other file down to its bare
    // object-store path -- a file that actually lives under a different authority would
    // silently read through the first file's store handle. Checked here rather than inside
    // prepare_scan_store_and_files itself, which is shared with plain NativeScan and out of
    // scope for this Delta-specific invariant.
    check_same_object_store_authority(&spark_partition.partitioned_file)?;

    let (object_store_url, mut files) =
        planner.prepare_scan_store_and_files(common, &spark_partition)?;

    // Translate deletion vectors into per-file ParquetAccessPlans so deleted
    // rows are skipped inside the reader (composing, by intersection, with
    // page-index pruning). Fetching the bitmaps and footers is async I/O;
    // create_plan runs on the JNI task thread outside the tokio context, so
    // block_on here is safe and keeps the scan a plain DataSourceExec.
    if delta_partition
        .partitioned_file
        .iter()
        .any(|f| f.dv.is_some())
    {
        let object_store_options: HashMap<String, String> = common
            .object_store_options
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let runtime_env = planner.session_ctx.runtime_env();
        // `object_store_options` is the same map for every file this partition resolves a store
        // for, so its hash is loop-invariant too: computed once here rather than once per file
        // inside `prepare_object_store_with_configs`.
        let object_store_config_hash = hash_object_store_configs(&object_store_options);

        // Resolve every object store this partition's files touch -- the data
        // files' shared authority (check_same_object_store_authority above
        // has already verified every data file in this partition resolves to
        // the same authority, so prepare_scan_store_and_files's
        // first-file-only resolution is safe here) plus any on-disk deletion
        // vector's authority, which may legitimately differ from the data
        // files' and carries its own resolved store -- before entering the
        // async DV runtime below. This MUST happen here, on the JNI thread
        // outside the tokio runtime:
        // constructing a cold S3 store issues its own internal
        // Handle::block_on calls (credential-provider / bucket-region
        // resolution), which panics when nested inside the
        // get_runtime().block_on(...) a few lines down. See
        // delta_dv::attach_access_plans's doc comment for the invariant this
        // maintains -- the async path never builds a store.
        let mut resolved_stores: HashMap<ObjectStoreUrl, Arc<dyn ObjectStore>> = HashMap::new();
        // Tracks, per resolved ObjectStoreUrl, the userinfo (and raw URL, for the error
        // message) of the first URL that resolved to it. This closure is the ONE place in this
        // scan that sees both data-file and deletion-vector URLs together, so the
        // store-identity collision check (see check_store_identity's doc comment) lives here
        // rather than as an extension of check_same_object_store_authority above, which sees
        // only data files and would hard-error the legitimate cross-bucket DV shape.
        let mut store_identities: HashMap<ObjectStoreUrl, (String, String)> = HashMap::new();
        let mut resolve_store =
            |url: String| -> Result<(Path, Arc<dyn ObjectStore>), ExecutionError> {
                let parsed_url = Url::parse(&url).map_err(|e| {
                    GeneralError(format!(
                        "Error parsing URL {}: {e}",
                        redacted_url_display(&url)
                    ))
                })?;
                let user_info = url_user_info(&parsed_url);
                // The same s3a/alias rewrite the shared resolution path applies first, so the
                // cheap key and cached-hit path below agree with what
                // prepare_object_store_with_config_hash registers under. Re-parses the already
                // parsed string only so the parse error above stays redacted.
                let parsed_url = normalize_object_store_url(&url, &object_store_options)?;

                // Cheap, I/O-free cache key: no config hashing, no global object-store-cache
                // lock, no runtime_env registration. Checked against the LOCAL `resolved_stores`
                // map below before ever paying for the expensive resolution path -- most files
                // in a partition share the same authority as an already-resolved file.
                let (url_key, _is_hdfs_scheme) =
                    object_store_url_key(&parsed_url, &object_store_options);
                let store_url = ObjectStoreUrl::parse(url_key)?;
                check_store_identity(&store_url, &user_info, &url, &mut store_identities)?;
                if let Some(store) = resolved_stores.get(&store_url) {
                    let path = Path::from_url_path(parsed_url.path())
                        .map_err(|e| GeneralError(e.to_string()))?;
                    return Ok((path, Arc::clone(store)));
                }

                // Local miss: fall through to the expensive resolution (global cache lock,
                // possible store creation, runtime_env registration). `object_store_config_hash`
                // was already computed once above, outside this closure.
                let (store_url, path) = prepare_object_store_with_config_hash(
                    Arc::clone(&runtime_env),
                    url.clone(),
                    &object_store_options,
                    object_store_config_hash,
                )?;
                let store = runtime_env.object_store(&store_url)?;
                resolved_stores.insert(store_url, Arc::clone(&store));
                Ok((path, store))
            };

        // get_partitioned_files maps 1:1 over the proto file list, so the three sources are
        // expected to be index-aligned. `.zip()` truncates silently on a length mismatch instead
        // of erroring, so check_zip_lengths asserts the invariant up front rather than trusting
        // it implicitly -- a future change to any one of the three builders that drops or adds an
        // element would otherwise corrupt file-to-DV pairing without either side noticing.
        check_zip_lengths(
            files.len(),
            spark_partition.partitioned_file.len(),
            delta_partition.partitioned_file.len(),
        )?;
        let mut dv_files: Vec<crate::execution::delta_dv::DvScanFile> =
            Vec::with_capacity(files.len());
        for ((file, spark_file), delta_file) in files
            .into_iter()
            .zip(spark_partition.partitioned_file.iter())
            .zip(delta_partition.partitioned_file.iter())
        {
            let (_, data_store) = resolve_store(spark_file.file_path.clone())?;
            let dv_store = match delta_file
                .dv
                .as_ref()
                .and_then(|dv| dv.absolute_path.clone())
            {
                Some(dv_path) => {
                    let (path, store) = resolve_store(dv_path)?;
                    Some((store, path))
                }
                None => None,
            };
            dv_files.push(crate::execution::delta_dv::DvScanFile {
                file,
                file_path: spark_file.file_path.clone(),
                dv: delta_file.dv.clone(),
                data_store,
                dv_store,
            });
        }

        files = crate::execution::jni_api::get_runtime().block_on(
            crate::execution::delta_dv::attach_access_plans(runtime_env, dv_files),
        )?;
    }

    // `true`: Delta data files may predate the table (e.g. converted or imported parquet) or
    // be written with LEGACY rebase modes, and only each file's own footer metadata can say so
    // -- resolve the datetime calendar-rebase policy per file rather than inheriting
    // NativeScan's documented no-rebase behavior (see datetime_rebase.rs). The session read
    // modes forwarded in delta_common cover files whose metadata does not decide (converted
    // non-Spark parquet); absent delta_common (defensive -- the injector always sets it)
    // degrades to empty modes, i.e. the EXCEPTION refuse-ancient posture.
    let (datetime_rebase_mode, int96_rebase_mode) = scan
        .delta_common
        .as_ref()
        .map(|c| {
            (
                c.datetime_rebase_mode_in_read.as_str(),
                c.int96_rebase_mode_in_read.as_str(),
            )
        })
        .unwrap_or(("", ""));
    let scan = planner.build_parquet_scan_plan(
        spark_plan.plan_id,
        common,
        object_store_url,
        files,
        true,
        datetime_rebase_mode,
        int96_rebase_mode,
    )?;
    Ok((vec![], vec![], scan))
}

/// (scheme, username, host, port), all normalized so equality means "same object-store
/// authority". Scheme and host are lowercased; username (the URI's userinfo -- e.g. the container
/// in `abfss://container@account/...`) is compared verbatim, since object-store identifiers built
/// from it may be case-sensitive and it is safer to draw more authority distinctions than fewer;
/// port is compared as `Option<u16>` so an explicit port never collapses into an absent one.
/// Mirrors `DeltaScanSupport.uriAuthority`'s normalization on the JVM side, which folds scheme,
/// userinfo, host, and port into one lowercased `getAuthority`-derived key -- both sides must
/// treat two URIs as the same authority in exactly the same cases so the JVM-side gate
/// (`multiStoreReason`, which declines) always fires before this native check (which errors) ever
/// would.
type ObjectStoreAuthority = (String, String, String, Option<u16>);

/// Errors unless every file in `files` shares the first file's [`ObjectStoreAuthority`]. The
/// `url` crate does NOT lowercase the host for opaque (non-"special") schemes like
/// `s3a`/`abfss`/`hdfs`, so comparing `url[BeforeHost..AfterPort]` verbatim would treat two
/// spellings of the same bucket (`s3a://Bucket-A/..` vs `s3a://bucket-a/..`) as different
/// authorities and hard-error instead of gracefully declining. See the call site's comment for
/// why this defensive check exists alongside the JVM-side gate.
fn check_same_object_store_authority(files: &[SparkPartitionedFile]) -> Result<(), ExecutionError> {
    let mut first: Option<(ObjectStoreAuthority, &str)> = None;
    for file in files {
        let url = Url::parse(&file.file_path).map_err(|e| {
            GeneralError(format!(
                "Error parsing URL {}: {e}",
                redacted_url_display(&file.file_path)
            ))
        })?;
        let authority: ObjectStoreAuthority = (
            url.scheme().to_ascii_lowercase(),
            url.username().to_string(),
            url.host_str().unwrap_or("").to_ascii_lowercase(),
            url.port(),
        );
        match &first {
            None => first = Some((authority, file.file_path.as_str())),
            Some((first_authority, first_path)) if *first_authority != authority => {
                return Err(GeneralError(format!(
                    "Native Delta scan does not support data files spanning multiple object \
                     stores (found {} and {})",
                    redacted_url_display(first_path),
                    redacted_url_display(&file.file_path)
                )));
            }
            Some(_) => {}
        }
    }
    Ok(())
}

/// Errors unless `files_len`, `spark_files_len`, and `delta_files_len` all agree. Called before
/// the three-way `.zip()` over the object-store-resolved files, the JVM-planned
/// `SparkPartitionedFile`s, and the Delta-specific per-file deletion-vector descriptors that
/// builds `dv_files` -- `Iterator::zip` stops at the shortest sequence with no error, so any
/// future change to one of the three independently-built sources that adds or drops an element
/// would otherwise silently mis-pair a data file with the wrong (or a missing) deletion vector
/// instead of failing loudly.
fn check_zip_lengths(
    files_len: usize,
    spark_files_len: usize,
    delta_files_len: usize,
) -> Result<(), ExecutionError> {
    if files_len == spark_files_len && spark_files_len == delta_files_len {
        return Ok(());
    }
    Err(GeneralError(format!(
        "Native Delta scan found mismatched file-list lengths while attaching deletion vectors \
         (resolved files: {files_len}, planned files: {spark_files_len}, deletion-vector \
         descriptors: {delta_files_len}); refusing to zip index-aligned sequences of unequal \
         length"
    )))
}

/// The userinfo component of `url`'s authority (e.g. the container in
/// `abfss://container@account.dfs.core.windows.net/...`), or the empty string when the URL
/// carries none. Never lowercased, mirroring `check_same_object_store_authority`'s own use of
/// `url.username()` above: userinfo is the ONE component `parquet_support.rs`'s `url_key` drops
/// before it becomes the [`ObjectStoreUrl`] two URLs are resolved and cached under, so it must
/// be compared verbatim, not normalized, to detect a real store-identity collision. Mirrors
/// `DeltaScanSupport.uriUserInfo` on the JVM side.
fn url_user_info(url: &Url) -> String {
    url.username().to_string()
}

/// A display form of `url` safe to embed in an error message: userinfo (e.g. the access/secret
/// key pair embedded as `s3a://AKIA...:secret@bucket/...`, or a Delta shallow-clone container
/// name) is replaced with a literal `***`, mirroring `DeltaScanSupport.redactedAuthority` on the
/// JVM side (`scheme://***@host[:port]`). Scheme and host/port are kept verbatim (not
/// lowercased) and the path is kept in full -- userinfo is the only secret-bearing component,
/// and dropping the path would make the two defense-in-depth checks that call this ([`
/// check_same_object_store_authority`] and [`check_store_identity`]) unable to name which file
/// triggered the error.
///
/// `url` need not be a valid [`Url`] -- every call site formats a `GeneralError` from a URL that
/// may originate from a foreign/bypassed proto producer, including ones a credential-bearing URL
/// can produce by FAILING to parse in the first place (e.g. `s3a://AKIA:secret@bucket:notaport/x`
/// is `Url::parse`-rejected as `InvalidPort`, but still carries userinfo), so this must be total
/// (never panic) AND must still redact on the parse-failure path -- it is exactly the credentials
/// that make a URL unusual enough to fail parsing that most need to never reach a log line.
/// The fallback below is purely textual: it looks for a `://` scheme delimiter and, within the
/// authority segment that follows (up to the next `/`, mirroring where a real URL's authority
/// ends), replaces everything up to and including the LAST `@` with `***@` -- same last-`@` split
/// as the successfully-parsed path and `DeltaScanSupport.redactedAuthority` on the JVM side. A
/// string with no `://` is treated as having no authority at all and its whole text is searched
/// for a trailing userinfo-shaped `...@host` prefix the same way. A string with neither shape
/// (no `@` anywhere before its authority ends) has no evident secret to redact and is returned
/// unchanged.
fn redacted_url_display(url: &str) -> String {
    if let Ok(parsed) = Url::parse(url) {
        if parsed.username().is_empty() && parsed.password().is_none() {
            return url.to_string();
        }
        let host_port = match (parsed.host_str(), parsed.port()) {
            (Some(host), Some(port)) => format!("{host}:{port}"),
            (Some(host), None) => host.to_string(),
            (None, _) => String::new(),
        };
        let mut redacted = format!("{}://***@{host_port}{}", parsed.scheme(), parsed.path());
        if let Some(query) = parsed.query() {
            redacted.push('?');
            redacted.push_str(query);
        }
        return redacted;
    }

    let (scheme_prefix, rest) = match url.find("://") {
        Some(scheme_end) => (&url[..scheme_end + 3], &url[scheme_end + 3..]),
        None => ("", url),
    };
    let authority_len = rest.find('/').unwrap_or(rest.len());
    match rest[..authority_len].rfind('@') {
        Some(at) => format!("{scheme_prefix}***@{}", &rest[at + 1..]),
        None => url.to_string(),
    }
}

/// Errors when `store_url` was already resolved earlier in this scan under a DIFFERENT
/// `user_info` than the one now being resolved for `url`; otherwise records `(user_info, url)`
/// for `store_url` in `seen` (first resolution wins the recorded userinfo) and returns `Ok`.
///
/// This is the free-standing half of the residual cross-container DV check, called from inside
/// the `resolve_store` closure above -- the ONE place in this scan that sees both data-file AND
/// deletion-vector URLs. `store_url` is exactly the key `prepare_object_store_with_configs`
/// resolves the object store, `ObjectStoreUrl`, and DataFusion's registry under (its
/// `url_key = scheme://{BeforeHost..AfterPort}`, dropping userinfo entirely -- see
/// `parquet_support.rs`), so two URLs agreeing on `store_url` but disagreeing on `user_info` are
/// exactly the URLs the native side would otherwise silently collapse onto one store handle.
/// That is the shape a Delta shallow clone across containers on a single storage account
/// produces: data stays in `source`, a later DELETE writes its deletion vector into `clone`,
/// and `abfss://source@account/...` / `abfss://clone@account/...` share a host (so the SAME
/// `store_url`) while their userinfo (the container) differs.
///
/// Deliberately NOT folded into `check_same_object_store_authority` above: that check only ever
/// sees DATA files and hard-errors on ANY authority mismatch, which would incorrectly reject the
/// legitimate cross-bucket DV shape (data in one S3 bucket, its DV in another) -- distinct hosts
/// mean distinct `store_url`s, so this check never even treats them as collision candidates; see
/// `dv_in_different_bucket_is_allowed` below.
fn check_store_identity(
    store_url: &ObjectStoreUrl,
    user_info: &str,
    url: &str,
    seen: &mut HashMap<ObjectStoreUrl, (String, String)>,
) -> Result<(), ExecutionError> {
    match seen.get(store_url) {
        Some((seen_user_info, seen_url)) if seen_user_info != user_info => {
            Err(GeneralError(format!(
                "Native Delta scan does not support data files and deletion vectors whose \
                 stores collide under the native store-identity key (found {} and {})",
                redacted_url_display(seen_url),
                redacted_url_display(url)
            )))
        }
        Some(_) => Ok(()),
        None => {
            seen.insert(store_url.clone(), (user_info.to_string(), url.to_string()));
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn partitioned_file(path: &str) -> SparkPartitionedFile {
        SparkPartitionedFile {
            file_path: path.to_string(),
            start: 0,
            length: 0,
            file_size: 0,
            partition_values: vec![],
        }
    }

    #[test]
    fn same_authority_files_pass() {
        let files = vec![
            partitioned_file("s3a://bucket/a/part-0.parquet"),
            partitioned_file("s3a://bucket/b/part-1.parquet"),
        ];
        assert!(check_same_object_store_authority(&files).is_ok());
    }

    #[test]
    fn same_authority_files_pass_regardless_of_host_case() {
        // The `url` crate does not lowercase hosts for opaque (non-"special") schemes like
        // s3a, so this must be normalized explicitly rather than relying on Url's own
        // formatting -- otherwise the same physical bucket recorded with mixed casing would
        // pass the JVM gate (which does lowercase) but hard-error here instead.
        let files = vec![
            partitioned_file("s3a://Bucket-A/x.parquet"),
            partitioned_file("s3a://bucket-a/y.parquet"),
        ];
        assert!(check_same_object_store_authority(&files).is_ok());
    }

    #[test]
    fn mixed_authority_files_error_names_both() {
        let files = vec![
            partitioned_file("s3a://bucket-a/part-0.parquet"),
            partitioned_file("s3a://bucket-b/part-1.parquet"),
        ];
        let err = check_same_object_store_authority(&files).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("bucket-a"),
            "expected message to name bucket-a: {msg}"
        );
        assert!(
            msg.contains("bucket-b"),
            "expected message to name bucket-b: {msg}"
        );
        assert!(
            msg.contains("multiple object stores"),
            "expected message to explain the failure: {msg}"
        );
    }

    #[test]
    fn cross_container_abfss_files_error() {
        // Same storage account, different containers: the userinfo (container) must be part of
        // the authority key, or `abfss://containerA@account/..` and
        // `abfss://containerB@account/..` would collapse into the same authority (same host,
        // same scheme) and this defense-in-depth check would silently let a cross-container scan
        // through instead of erroring.
        let files = vec![
            partitioned_file("abfss://containerA@account.dfs.core.windows.net/a/part-0.parquet"),
            partitioned_file("abfss://containerB@account.dfs.core.windows.net/b/part-1.parquet"),
        ];
        let err = check_same_object_store_authority(&files).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("multiple object stores"),
            "expected message to explain the failure: {msg}"
        );
    }

    #[test]
    fn same_container_abfss_files_pass() {
        let files = vec![
            partitioned_file("abfss://container@account.dfs.core.windows.net/a/part-0.parquet"),
            partitioned_file("abfss://container@account.dfs.core.windows.net/b/part-1.parquet"),
        ];
        assert!(check_same_object_store_authority(&files).is_ok());
    }

    #[test]
    fn distinct_underscore_host_buckets_error() {
        // `gs://my_bucket/..` has an underscore reg-name; the `url` crate (unlike Java's `URI`)
        // parses it as an opaque host without failing the whole authority, so this check must
        // still tell two distinct underscore-bearing buckets apart.
        let files = vec![
            partitioned_file("gs://my_bucket/a/part-0.parquet"),
            partitioned_file("gs://other_bucket/b/part-1.parquet"),
        ];
        let err = check_same_object_store_authority(&files).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("multiple object stores"),
            "expected message to explain the failure: {msg}"
        );
    }

    #[test]
    fn same_underscore_host_bucket_files_pass() {
        let files = vec![
            partitioned_file("gs://my_bucket/a/part-0.parquet"),
            partitioned_file("gs://my_bucket/b/part-1.parquet"),
        ];
        assert!(check_same_object_store_authority(&files).is_ok());
    }

    #[test]
    fn local_paths_pass_regardless_of_directory() {
        let files = vec![
            partitioned_file("file:///tmp/a/part-0.parquet"),
            partitioned_file("file:///tmp/b/part-1.parquet"),
        ];
        assert!(check_same_object_store_authority(&files).is_ok());
    }

    /// Builds the same `(ObjectStoreUrl, userinfo)` pair `resolve_store` computes for a URL,
    /// without touching any object-store backend: it runs the closure's own normalize-then-key
    /// steps, so these fixtures collide (or don't) under [`check_store_identity`] the same way
    /// the real closure's calls would.
    fn store_url_and_user_info(url_str: &str) -> (ObjectStoreUrl, String) {
        let configs = HashMap::new();
        let user_info = url_user_info(&Url::parse(url_str).unwrap());
        let normalized = normalize_object_store_url(url_str, &configs).unwrap();
        let (key, _) = object_store_url_key(&normalized, &configs);
        (ObjectStoreUrl::parse(key).unwrap(), user_info)
    }

    #[test]
    fn dv_in_different_container_same_account_errors() {
        // Same storage account (same host -> same ObjectStoreUrl), different containers
        // (different userinfo): the shape a Delta shallow clone across containers produces
        // when data stays in `source` but a later DELETE writes its DV into `clone`. Both
        // authorities collapse onto one native store identity, so this must decline.
        let mut seen = HashMap::new();
        let data = "abfss://source@account.dfs.core.windows.net/a/part-0.parquet";
        let dv = "abfss://clone@account.dfs.core.windows.net/_delta_log/deletion_vector_x.bin";
        let (data_store_url, data_user_info) = store_url_and_user_info(data);
        check_store_identity(&data_store_url, &data_user_info, data, &mut seen).unwrap();
        let (dv_store_url, dv_user_info) = store_url_and_user_info(dv);
        let err = check_store_identity(&dv_store_url, &dv_user_info, dv, &mut seen).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("store-identity"),
            "expected message to reference the store-identity collision: {msg}"
        );
        // The container names ARE the userinfo here, so the message must redact them rather
        // than name the raw URLs -- see redacted_url_display.
        assert!(
            !msg.contains("source@") && !msg.contains("clone@"),
            "expected message to redact the container userinfo: {msg}"
        );
        assert!(
            msg.contains("***@account.dfs.core.windows.net"),
            "expected message to show a redacted authority: {msg}"
        );
        assert!(
            msg.contains("a/part-0.parquet") && msg.contains("deletion_vector_x.bin"),
            "expected message to still name the differing paths: {msg}"
        );
    }

    #[test]
    fn dv_in_different_bucket_is_allowed() {
        // Guards the legitimate MinIO/S3 shape: data in one bucket, its DV in another.
        // Distinct hosts mean distinct ObjectStoreUrls, so these must never even look like a
        // collision to check_store_identity -- this is exactly the shape
        // check_same_object_store_authority alone would be too strict to allow if the
        // collision check were folded into it instead of resolve_store.
        let mut seen = HashMap::new();
        let data = "s3://comet-delta-a/part-0.parquet";
        let dv = "s3://comet-delta-b/_delta_log/deletion_vector_x.bin";
        let (data_store_url, data_user_info) = store_url_and_user_info(data);
        check_store_identity(&data_store_url, &data_user_info, data, &mut seen).unwrap();
        let (dv_store_url, dv_user_info) = store_url_and_user_info(dv);
        assert!(check_store_identity(&dv_store_url, &dv_user_info, dv, &mut seen).is_ok());
    }

    #[test]
    fn dv_in_same_container_passes() {
        let mut seen = HashMap::new();
        let data = "abfss://container@account.dfs.core.windows.net/a/part-0.parquet";
        let dv = "abfss://container@account.dfs.core.windows.net/_delta_log/deletion_vector_x.bin";
        let (data_store_url, data_user_info) = store_url_and_user_info(data);
        check_store_identity(&data_store_url, &data_user_info, data, &mut seen).unwrap();
        let (dv_store_url, dv_user_info) = store_url_and_user_info(dv);
        assert!(check_store_identity(&dv_store_url, &dv_user_info, dv, &mut seen).is_ok());
    }

    #[test]
    fn dv_with_local_paths_passes() {
        let mut seen = HashMap::new();
        let data = "file:///tmp/a/part-0.parquet";
        let dv = "file:///tmp/_delta_log/deletion_vector_x.bin";
        let (data_store_url, data_user_info) = store_url_and_user_info(data);
        check_store_identity(&data_store_url, &data_user_info, data, &mut seen).unwrap();
        let (dv_store_url, dv_user_info) = store_url_and_user_info(dv);
        assert!(check_store_identity(&dv_store_url, &dv_user_info, dv, &mut seen).is_ok());
    }

    #[test]
    fn redacted_url_display_leaves_plain_url_unchanged() {
        let url = "s3a://bucket/a/part-0.parquet";
        assert_eq!(redacted_url_display(url), url);
    }

    #[test]
    fn redacted_url_display_redacts_userinfo() {
        let url = "s3a://AKIAEXAMPLE:supersecret@bucket/a/part-0.parquet";
        let redacted = redacted_url_display(url);
        assert!(
            !redacted.contains("AKIAEXAMPLE") && !redacted.contains("supersecret"),
            "expected credentials to be redacted: {redacted}"
        );
        assert!(
            redacted.contains("bucket"),
            "expected host to remain visible: {redacted}"
        );
        assert_eq!(redacted, "s3a://***@bucket/a/part-0.parquet");
    }

    #[test]
    fn redacted_url_display_redacts_multi_at_password_fully() {
        // The '@' inside the password must not be mistaken for the userinfo/host delimiter --
        // the LAST '@' in the authority is the real delimiter, same as the JVM's
        // `redactedAuthority` split.
        let url = "s3a://user:p@ss@bucket/k";
        let redacted = redacted_url_display(url);
        assert!(
            !redacted.contains("user") && !redacted.contains("p@ss"),
            "expected the entire userinfo, including the embedded '@', to be redacted: {redacted}"
        );
        assert_eq!(redacted, "s3a://***@bucket/k");
    }

    #[test]
    fn redacted_url_display_is_total_for_non_url_input() {
        // Not a valid URL and has no authority-like userinfo prefix before its first '/' --
        // must return unchanged rather than panic.
        let input = "not a url at all";
        assert_eq!(redacted_url_display(input), input);

        // Not a valid URL (no scheme, so `Url::parse` rejects it as relative), but does have a
        // userinfo-shaped prefix before its first '/' -- must still redact it rather than leak
        // it verbatim.
        let input = "secret@host/path";
        let redacted = redacted_url_display(input);
        assert!(
            !redacted.contains("secret"),
            "expected the userinfo-shaped prefix to be redacted: {redacted}"
        );
        assert_eq!(redacted, "***@host/path");
    }

    #[test]
    fn redacted_url_display_redacts_credentials_from_a_scheme_prefixed_url_that_fails_to_parse() {
        // Invalid port -- `url::Url::parse` rejects this outright (InvalidPort), so this never
        // reaches the successfully-parsed branch above; it must still be caught by the fallback,
        // which must recognize the `scheme://` prefix so it doesn't stop at the FIRST '/' in
        // that prefix (a bug that would leave userinfo un-redacted for exactly this shape).
        let url = "s3a://AKIA:secret@bucket:notaport/path";
        assert!(Url::parse(url).is_err(), "fixture must fail to parse");
        let redacted = redacted_url_display(url);
        assert!(
            !redacted.contains("AKIA") && !redacted.contains("secret"),
            "expected credentials to be redacted: {redacted}"
        );
        assert_eq!(redacted, "s3a://***@bucket:notaport/path");
    }

    #[test]
    fn zip_lengths_agreeing_pass() {
        assert!(check_zip_lengths(3, 3, 3).is_ok());
        assert!(check_zip_lengths(0, 0, 0).is_ok());
    }

    #[test]
    fn zip_lengths_mismatch_names_all_three_lengths() {
        // Every producer of these three sequences (get_partitioned_files, the
        // spark_partition.partitioned_file map, and the raw delta_partition.partitioned_file
        // list) currently guarantees 1:1 length agreement on every success path -- this can't be
        // reached today through the public ContribScan entry point without a code change
        // upstream of this check. It's exercised directly here as defense-in-depth against a
        // future regression in one of those producers.
        let err = check_zip_lengths(2, 3, 3).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("resolved files: 2"), "message was: {msg}");
        assert!(msg.contains("planned files: 3"), "message was: {msg}");
        assert!(
            msg.contains("deletion-vector descriptors: 3"),
            "message was: {msg}"
        );
    }

    #[test]
    fn zip_lengths_mismatch_on_delta_files_only() {
        let err = check_zip_lengths(4, 4, 5).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("resolved files: 4"), "message was: {msg}");
        assert!(msg.contains("planned files: 4"), "message was: {msg}");
        assert!(
            msg.contains("deletion-vector descriptors: 5"),
            "message was: {msg}"
        );
    }

    #[test]
    fn parse_error_on_credential_bearing_url_redacts_the_error_message() {
        // Regression: a credential-bearing URL that FAILS `Url::parse` (bad port here) must
        // still produce an error whose message omits the secret -- this exercises the actual
        // `check_same_object_store_authority` error path, not just the helper in isolation.
        let files = vec![partitioned_file(
            "s3a://AKIA:supersecret@bucket:notaport/part-0.parquet",
        )];
        let err = check_same_object_store_authority(&files).unwrap_err();
        let msg = err.to_string();
        assert!(
            !msg.contains("AKIA") && !msg.contains("supersecret"),
            "expected the parse-error message to redact credentials: {msg}"
        );
        assert!(
            msg.contains("***@bucket"),
            "expected the parse-error message to still name the redacted host: {msg}"
        );
    }
}
