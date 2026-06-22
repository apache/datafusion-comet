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

//! Executor-side Delta deletion-vector decoder.
//!
//! Pre-PR #4366 the driver materialised every DV to a `Vec<u64>` (via
//! `DvInfo::get_row_indexes`) and shipped the expanded list to the executor in
//! the `DeltaScanTask` proto. For a 2 B-row table with a 99.9 M-row DV that's
//! a ~1 GB `long[]` retained on the driver heap for the duration of the
//! `Dataset.rdd` → `executeColumnar` flow -- the dominator that drove the
//! pushdown suite's 8 g heap requirement (MAT confirmed 51 % of heap on one
//! `CometDeltaNativeScanExec.long[]`).
//!
//! The Iceberg contrib in this same repo already had the answer:
//! `IcebergScanCommon.delete_files_pool` ships *descriptors* (path/format,
//! KB-scale) and decodes on-task in the executor. We mirror that pattern for
//! Delta: the driver now emits [`crate::proto::DeltaDvDescriptor`]
//! (storage-type / path / offset / size / cardinality) and the executor calls
//! [`read_dv_indexes`] once per partition on first poll.
//!
//! `delta_kernel::actions::deletion_vector::DeletionVectorDescriptor::read`
//! handles all three storage types ("u" UUID-relative, "p" absolute,
//! "i" inline) uniformly, so the executor reuses the exact decode path the
//! driver used to call.

use std::str::FromStr;
use url::Url;

use datafusion::common::DataFusionError;
use datafusion_comet_common::SparkError;
use delta_kernel::actions::deletion_vector::{DeletionVectorDescriptor, DeletionVectorStorageType};
use delta_kernel::Engine;

use crate::engine::{get_or_create_engine, DeltaStorageConfig};
use crate::error::{DeltaError, DeltaResult};
use crate::proto::DeltaDvDescriptor;

/// Decode a proto [`DeltaDvDescriptor`] into the sorted list of deleted row
/// indexes for one Delta data file.
///
/// Runs on the **executor task**, not the driver. `table_root_url` is the
/// trailing-slash-normalised root the driver got from
/// `DeltaScanTaskList.table_root` (kernel's `absolute_path` joins
/// `_delta_log/deletion_vectors/<uuid>` onto it; the trailing slash matters --
/// see `scan::normalize_url`).
///
/// The engine is reused from [`crate::engine::get_or_create_engine`], so the
/// per-process LRU cache (size 32) bounds tokio-thread fan-out across many
/// executor tasks against the same table root.
///
/// `config` carries the table's storage credentials -- the same `DeltaStorageConfig`
/// the data read threads from `object_store_options`. DV reads against a credentialed
/// S3/Azure store therefore use the same engine the data read does, not an empty
/// `default()` that would miss static creds. Engines are keyed by
/// `(scheme, authority, config)` in `get_or_create_engine`, so a matching config
/// reuses the data read's cached engine instead of building a credential-less second one.
pub fn read_dv_indexes(
    proto_dv: &DeltaDvDescriptor,
    table_root_url: &Url,
    config: &DeltaStorageConfig,
    dv_file_name_prefix: &str,
) -> DeltaResult<Vec<u64>> {
    let descriptor = proto_to_kernel_descriptor(proto_dv)?;
    let engine = get_or_create_engine(table_root_url, config)?;
    let storage = Engine::storage_handler(&*engine);

    // Delta's test mode prepends `dv_file_name_prefix`
    // (`spark.databricks.delta.testOnly.dvFileNamePrefix`) to every DV filename Delta WRITES:
    // `<prefix>deletion_vector_<uuid>.bin`. delta-kernel-rs has no knowledge of that JVM-only conf,
    // so for "u" (UUID-relative) storage it resolves the UN-prefixed `deletion_vector_<uuid>.bin`
    // and the read fails (the file isn't there). Splice the prefix into kernel's own resolved path
    // and read through an absolute-path ("p") descriptor instead. Only "u" descriptors get the
    // prefix -- Delta applies it only to the relative DVs it writes, never to verbatim "p"
    // (absolute) or "i" (inline) descriptors. The prefix is empty in production, so this whole
    // branch is a no-op there.
    if !dv_file_name_prefix.is_empty() && proto_dv.storage_type == "u" {
        if let Some(resolved) = descriptor.absolute_path(table_root_url)? {
            if let Some(prefixed) = splice_dv_file_name_prefix(&resolved, dv_file_name_prefix) {
                let mut abs = proto_dv.clone();
                abs.storage_type = "p".to_string();
                abs.path_or_inline_dv = prefixed;
                let abs_descriptor = proto_to_kernel_descriptor(&abs)?;
                return abs_descriptor
                    .row_indexes(storage, table_root_url)
                    .map_err(|e| DeltaError::Internal(format!("DV read failed: {e}")));
            }
        }
    }

    descriptor
        .row_indexes(storage, table_root_url)
        .map_err(|e| DeltaError::Internal(format!("DV read failed: {e}")))
}

/// Insert Delta's test-mode DV filename prefix in front of the `deletion_vector_<uuid>.bin`
/// component of `resolved` (the path delta-kernel-rs computed WITHOUT the prefix). Operates on the
/// decoded filesystem path so the literal `%` in prefixes like `test%dv%prefix-` is re-encoded
/// correctly when rebuilt into a `file://` URL. Returns `None` for non-file URLs (cloud stores
/// never carry the test prefix, which only exists under `Utils.isTesting` on local temp dirs).
fn splice_dv_file_name_prefix(resolved: &Url, prefix: &str) -> Option<String> {
    let path = resolved.to_file_path().ok()?;
    let file_name = path.file_name()?.to_str()?;
    let new_path = path.with_file_name(format!("{prefix}{file_name}"));
    Some(Url::from_file_path(&new_path).ok()?.into())
}

/// Reconstruct kernel's `DeletionVectorDescriptor` from the proto wire form.
///
/// Field-for-field mapping; the proto schema was chosen to mirror kernel's
/// struct exactly so this is a memcpy-ish transcription rather than a real
/// decode. The one shape change: `offset` is `optional uint64` on the proto
/// (signed `Option<i32>` on kernel) so we narrow back through `i32::try_from`.
fn proto_to_kernel_descriptor(p: &DeltaDvDescriptor) -> DeltaResult<DeletionVectorDescriptor> {
    let storage_type = DeletionVectorStorageType::from_str(&p.storage_type).map_err(|e| {
        DeltaError::Internal(format!("invalid DV storage_type {:?}: {e}", p.storage_type))
    })?;
    // size_in_bytes is i32 in kernel; protect against overflow rather than panic in `as`.
    let size_in_bytes = i32::try_from(p.size_in_bytes).map_err(|_| {
        DeltaError::Internal(format!(
            "DV size_in_bytes {} doesn't fit in i32",
            p.size_in_bytes
        ))
    })?;
    let offset = match p.offset {
        Some(o) => Some(
            i32::try_from(o)
                .map_err(|_| DeltaError::Internal(format!("DV offset {o} doesn't fit in i32")))?,
        ),
        None => None,
    };
    Ok(DeletionVectorDescriptor {
        storage_type,
        path_or_inline_dv: p.path_or_inline_dv.clone(),
        offset,
        size_in_bytes,
        cardinality: p.cardinality as i64,
    })
}

/// Map a [`DeltaError`] from [`read_dv_indexes`] into a `DataFusionError` that carries the
/// right structured `SparkError` so the JVM shim can attach the matching `Throwable` chain.
///
/// In particular, a kernel/object-store "file not found" failure -- whether surfaced as a
/// `delta_kernel::Error` or wrapped by the engine -- needs to surface as a
/// `SparkError::FileNotFound` rather than a plain `Internal` string so that
/// `ShimSparkErrorConverter` calls
/// `QueryExecutionErrors.readCurrentFileNotFoundError(new FileNotFoundException(path))`.
/// Without this, DeletionVectorsSuite's "Check no resource leak when DV files are missing"
/// test fails because `findIfResponsible[FileNotFoundException]` walks the cause chain and
/// doesn't find a `FileNotFoundException`.
///
/// Called from `DeltaSyntheticColumnsExec::execute` whenever that exec flags
/// (`emit_is_row_deleted`) or drops (`drop_deleted`) DV rows -- the single DV decode site
/// since that exec absorbed the former `DeltaDvFilterExec`.
pub fn map_dv_error_to_datafusion(err: DeltaError, desc: &DeltaDvDescriptor) -> DataFusionError {
    let msg = err.to_string();
    // Substring match over the lowercased Display of the error. `read_dv_indexes`
    // flattens every failure into `DeltaError::Internal("DV read failed: {kernel Display}")`,
    // so there is no typed source() chain to downcast (unlike `missing_file_tolerant.rs`,
    // which sees the typed object_store error). Only `DeletionVectorDescriptor::read` errors
    // reach here, and the only missing-file variants kernel produces are:
    //   - kernel `Error::FileNotFound(path)`  -> Display "File not found: {path}"
    //   - raw object_store NotFound (S3/Azure 404 normalised by object_store)
    //                                          -> Display "...{path} not found: ..."
    // Every other reachable error (corrupt/short bitmap, CRC mismatch, "missing data",
    // bad URL, permission denied) contains NONE of the tokens below, and log-replay errors
    // like "column/version/table not found" are produced on the driver, never on this
    // executor DV-read path. The tokens are deliberately specific ("file not found" and the
    // colon-qualified "not found:") rather than a bare "not found", so a future refactor that
    // routed a planning error through here couldn't be silently misclassified as FileNotFound.
    let lower = msg.to_ascii_lowercase();
    let is_missing = lower.contains("file not found")
        || lower.contains("no such file")   // local FS errno text
        || lower.contains("not found:"); // object_store NotFound ("...{path} not found: ...")
    if is_missing {
        let path = if desc.path_or_inline_dv.is_empty() {
            msg.clone()
        } else {
            desc.path_or_inline_dv.clone()
        };
        return DataFusionError::External(Box::new(SparkError::FileNotFound { message: path }));
    }
    DataFusionError::Execution(format!("DV decode: {msg}"))
}

/// True if the (lowercased) error text looks like a missing-file failure -- the same tokens
/// [`map_dv_error_to_datafusion`] keys on. Deliberately specific so a non-IO error isn't
/// misclassified as a missing file.
fn looks_like_missing_file(msg_lower: &str) -> bool {
    msg_lower.contains("file not found")
        || msg_lower.contains("no such file")
        || msg_lower.contains("not found:")
}

/// Map a per-file read failure from `read_file_via_kernel` into a `DataFusionError` carrying the
/// structured `SparkError` the JVM shim needs. A failure to read a Delta data file (corrupt/short
/// parquet footer, decode error, IO) must surface as `SparkError::CannotReadFile` so the shim
/// raises Spark's `cannotReadFilesError` ("Encountered error while reading file ...", a
/// `[FAILED_READ_FILE]` SparkException on Spark 4.x) -- matching vanilla Spark+Delta and the old
/// ParquetSource path (#4536). A genuinely missing file still maps to `FileNotFound`, as on the DV
/// path. Without this, a corrupt file surfaces a raw "Arrow error: EOF ..." (probe Gap 2 / SC-8810).
pub fn map_file_read_error(err: delta_kernel::Error, file_path: &str) -> DataFusionError {
    let msg = err.to_string();
    if looks_like_missing_file(&msg.to_ascii_lowercase()) {
        return DataFusionError::External(Box::new(SparkError::FileNotFound {
            message: file_path.to_string(),
        }));
    }
    DataFusionError::External(Box::new(SparkError::CannotReadFile {
        file_path: file_path.to_string(),
        message: msg,
    }))
}

/// Resolve the table-root `Url` once. Kernel requires the URL to end in `/` so
/// that `absolute_path`'s join doesn't replace the last segment -- this is the
/// same trailing-slash invariant `scan::normalize_url` enforces on the driver.
pub fn normalize_table_root(raw: &str) -> DeltaResult<Url> {
    let with_slash = if raw.ends_with('/') {
        raw.to_string()
    } else {
        format!("{raw}/")
    };
    Url::parse(&with_slash)
        .map_err(|e| DeltaError::Internal(format!("invalid table_root URL {raw:?}: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proto_to_kernel_inline_storage() {
        // Inline DV: storageType "i", path_or_inline_dv carries the base85 bytes,
        // offset is None.
        let p = DeltaDvDescriptor {
            storage_type: "i".into(),
            path_or_inline_dv: "abc".into(),
            offset: None,
            size_in_bytes: 42,
            cardinality: 7,
            inline_bytes: Vec::new(),
        };
        let k = proto_to_kernel_descriptor(&p).unwrap();
        assert!(matches!(k.storage_type, DeletionVectorStorageType::Inline));
        assert_eq!(k.path_or_inline_dv, "abc");
        assert_eq!(k.offset, None);
        assert_eq!(k.size_in_bytes, 42);
        assert_eq!(k.cardinality, 7);
    }

    #[test]
    fn proto_to_kernel_uuid_storage_with_offset() {
        let p = DeltaDvDescriptor {
            storage_type: "u".into(),
            path_or_inline_dv: "uuid-string".into(),
            offset: Some(128),
            size_in_bytes: 1024,
            cardinality: 100,
            inline_bytes: Vec::new(),
        };
        let k = proto_to_kernel_descriptor(&p).unwrap();
        assert!(matches!(
            k.storage_type,
            DeletionVectorStorageType::PersistedRelative
        ));
        assert_eq!(k.offset, Some(128));
    }

    #[test]
    fn proto_to_kernel_rejects_bad_storage_type() {
        let p = DeltaDvDescriptor {
            storage_type: "x".into(),
            path_or_inline_dv: "p".into(),
            offset: None,
            size_in_bytes: 1,
            cardinality: 1,
            inline_bytes: Vec::new(),
        };
        let err = proto_to_kernel_descriptor(&p).unwrap_err();
        assert!(format!("{err}").contains("invalid DV storage_type"));
    }

    #[test]
    fn proto_to_kernel_rejects_overflowing_size() {
        let p = DeltaDvDescriptor {
            storage_type: "i".into(),
            path_or_inline_dv: String::new(),
            offset: None,
            size_in_bytes: u64::MAX,
            cardinality: 0,
            inline_bytes: Vec::new(),
        };
        let err = proto_to_kernel_descriptor(&p).unwrap_err();
        assert!(format!("{err}").contains("size_in_bytes"));
    }

    #[test]
    fn normalize_table_root_adds_trailing_slash() {
        let u = normalize_table_root("file:///tmp/t").unwrap();
        assert_eq!(u.as_str(), "file:///tmp/t/");
    }

    #[test]
    fn normalize_table_root_preserves_trailing_slash() {
        let u = normalize_table_root("file:///tmp/t/").unwrap();
        assert_eq!(u.as_str(), "file:///tmp/t/");
    }

    // The test-mode DV prefix is spliced in front of the `deletion_vector_<uuid>.bin` filename
    // (not the directory), and the literal `%` in the prefix is re-encoded as `%25` in the URL.
    #[test]
    fn splice_dv_file_name_prefix_inserts_before_filename() {
        let resolved =
            Url::parse("file:///tmp/t/deletion_vector_8d3e34f6-e84e-45b1-a9e5-529a013299b1.bin")
                .unwrap();
        let out = splice_dv_file_name_prefix(&resolved, "test%dv%prefix-").unwrap();
        assert_eq!(
            out,
            "file:///tmp/t/test%25dv%25prefix-deletion_vector_8d3e34f6-e84e-45b1-a9e5-529a013299b1.bin"
        );
    }

    // A random-prefix DV (kernel resolves it into a subdirectory) keeps the directory; only the
    // filename gets the test prefix.
    #[test]
    fn splice_dv_file_name_prefix_keeps_parent_dir() {
        let resolved = Url::parse("file:///tmp/t/ab/deletion_vector_xyz.bin").unwrap();
        let out = splice_dv_file_name_prefix(&resolved, "p-").unwrap();
        assert_eq!(out, "file:///tmp/t/ab/p-deletion_vector_xyz.bin");
    }

    // Non-file URLs (cloud object stores, which never carry the JVM test prefix) are declined so
    // the caller falls back to kernel's own resolution.
    #[test]
    fn splice_dv_file_name_prefix_declines_non_file_url() {
        let resolved = Url::parse("s3://bucket/t/deletion_vector_xyz.bin").unwrap();
        assert!(splice_dv_file_name_prefix(&resolved, "test%dv%prefix-").is_none());
    }

    // A corrupt/truncated parquet (e.g. the kernel "EOF: footer metadata" error) must become a
    // typed CannotReadFile carrying the data-file path, so the JVM shim raises Spark's
    // cannotReadFilesError (FAILED_READ_FILE) instead of leaking a raw Arrow error (probe Gap 2).
    #[test]
    fn map_file_read_error_corrupt_maps_to_cannot_read_file() {
        let err = delta_kernel::Error::generic("EOF: footer metadata requires 8 bytes");
        match map_file_read_error(err, "file:///t/part-0.parquet") {
            DataFusionError::External(e) => match e.downcast_ref::<SparkError>() {
                Some(SparkError::CannotReadFile { file_path, message }) => {
                    assert_eq!(file_path, "file:///t/part-0.parquet");
                    assert!(
                        message.contains("footer metadata"),
                        "kept the underlying cause"
                    );
                }
                other => panic!("expected CannotReadFile, got {other:?}"),
            },
            other => panic!("expected External SparkError, got {other:?}"),
        }
    }

    // A genuinely missing data file still maps to FileNotFound, like the DV path.
    #[test]
    fn map_file_read_error_missing_maps_to_file_not_found() {
        let err = delta_kernel::Error::generic("File not found: /t/part-0.parquet");
        match map_file_read_error(err, "file:///t/part-0.parquet") {
            DataFusionError::External(e) => assert!(matches!(
                e.downcast_ref::<SparkError>(),
                Some(SparkError::FileNotFound { .. })
            )),
            other => panic!("expected External SparkError, got {other:?}"),
        }
    }
}
