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
use delta_kernel::actions::deletion_vector::{
    DeletionVectorDescriptor, DeletionVectorStorageType,
};
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
/// `DeltaStorageConfig::default()` is sufficient for `file://` and any S3/Azure
/// store whose credentials come from the ambient environment (the typical
/// executor path -- the same defaults the driver uses on the no-options code
/// path). When the engine cache later supports per-table storage config
/// plumbed from `object_store_options`, swap this for the matching config.
pub fn read_dv_indexes(
    proto_dv: &DeltaDvDescriptor,
    table_root_url: &Url,
) -> DeltaResult<Vec<u64>> {
    let descriptor = proto_to_kernel_descriptor(proto_dv)?;
    let engine = get_or_create_engine(table_root_url, &DeltaStorageConfig::default())?;
    let storage = Engine::storage_handler(&*engine);
    descriptor
        .row_indexes(storage, table_root_url)
        .map_err(|e| DeltaError::Internal(format!("DV read failed: {e}")))
}

/// Reconstruct kernel's `DeletionVectorDescriptor` from the proto wire form.
///
/// Field-for-field mapping; the proto schema was chosen to mirror kernel's
/// struct exactly so this is a memcpy-ish transcription rather than a real
/// decode. The one shape change: `offset` is `optional uint64` on the proto
/// (signed `Option<i32>` on kernel) so we narrow back through `i32::try_from`.
fn proto_to_kernel_descriptor(p: &DeltaDvDescriptor) -> DeltaResult<DeletionVectorDescriptor> {
    let storage_type = DeletionVectorStorageType::from_str(&p.storage_type).map_err(|e| {
        DeltaError::Internal(format!(
            "invalid DV storage_type {:?}: {e}",
            p.storage_type
        ))
    })?;
    // size_in_bytes is i32 in kernel; protect against overflow rather than panic in `as`.
    let size_in_bytes = i32::try_from(p.size_in_bytes).map_err(|_| {
        DeltaError::Internal(format!(
            "DV size_in_bytes {} doesn't fit in i32",
            p.size_in_bytes
        ))
    })?;
    let offset = match p.offset {
        Some(o) => Some(i32::try_from(o).map_err(|_| {
            DeltaError::Internal(format!("DV offset {o} doesn't fit in i32"))
        })?),
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
/// Called from BOTH `DeltaDvFilterExec::execute` AND
/// `DeltaSyntheticColumnsExec::execute` (when `emit_is_row_deleted`) -- keep them in lock-step
/// or the failure mode visible to the test will depend on whether synthetic columns are
/// requested. (The "resource leak" test is a `SELECT *` which routes through
/// synthetic-columns when the table emits `is_row_deleted`.)
pub fn map_dv_error_to_datafusion(err: DeltaError, desc: &DeltaDvDescriptor) -> DataFusionError {
    let msg = err.to_string();
    // Substring match over the lowercased Display of the chained error. This is the
    // pragmatic option: the underlying error types differ across kernel + the three
    // object_store backends (local FS, S3, Azure), and each wraps in its own enum
    // (`std::io::Error`, `object_store::Error::NotFound`, AWS `NoSuchKey`, Azure
    // `BlobNotFound`). A structural walk via `err.source()` chain would be cleaner but
    // would require linking the object_store backends directly here. The strings below
    // cover what we've seen surface in practice. False positives are very unlikely --
    // a non-FNF error message containing one of these tokens would be malformed.
    let lower = msg.to_ascii_lowercase();
    let is_missing = lower.contains("file not found")
        || lower.contains("no such file")
        || lower.contains("notfound")    // object_store::Error::NotFound
        || lower.contains("not found")   // generic
        || lower.contains("nosuchkey")   // AWS S3
        || lower.contains("blobnotfound"); // Azure Blob
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
}
