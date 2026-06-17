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

use crate::execution::operators::parquet_writer::{ParquetWriter, StorageWriterFactory};
use arrow::array::{Array, ArrayRef, RecordBatch, StringArray, UInt32Array};
use arrow::datatypes::{DataType, SchemaRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::runtime_env::RuntimeEnv;
use futures::future::try_join_all;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::sync::Arc;

/// Placeholder directory name used for `null`/empty partition values.
/// Mirrors Spark's `ExternalCatalogUtils.DEFAULT_PARTITION_NAME`.
const DEFAULT_PARTITION_NAME: &str = "__HIVE_DEFAULT_PARTITION__";

pub(crate) struct PartitionedWriter {
    /// Open writers keyed by their partition sub-directory (e.g. `a=1/b=2`).
    writers: HashMap<String, Box<dyn ParquetWriter>>,
    work_dir: String,
    partition_id: i32,
    task_attempt_id: Option<i32>,
    /// Schema of the data actually written to the files (partition columns removed).
    data_schema: SchemaRef,
    /// Indices (into the renamed output schema) of the partition columns.
    partition_col_indices: Vec<usize>,
    /// Names of the partition columns, in declared order.
    partition_col_names: Vec<String>,
    /// Indices (into the renamed output schema) of the data columns.
    data_col_indices: Vec<usize>,
    compression: Compression,
    runtime_env: Arc<RuntimeEnv>,
    object_store_options: HashMap<String, String>,
}

// Characters that must be escaped, in addition to all control chars (< 0x20).
const NEEDS_ESCAPE: &[char] = &[
    '"', '#', '%', '\'', '*', '/', ':', '=', '?', '\\', '\u{007F}', '{', '[', ']', '^',
];

/// Escape a string so it is safe to use as a single path component of a
/// Hive-style partition directory (`col=value`).
///
/// This mirrors Spark/Hive `ExternalCatalogUtils.escapePathName`: control
/// characters and a fixed set of special characters are percent-encoded as
/// `%XX` (upper-case hex). The result must match Spark exactly so that the
/// directory layout produced natively is readable by Spark and any catalog.
fn escape_path_name(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        if (c as u32) < 0x20 || NEEDS_ESCAPE.contains(&c) {
            // Percent-encode the byte value (chars here are always < 0x80).
            out.push_str(&format!("%{:02X}", c as u32));
        } else {
            out.push(c);
        }
    }
    out
}

impl PartitionedWriter {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        work_dir: String,
        partition_id: i32,
        task_attempt_id: Option<i32>,
        data_schema: SchemaRef,
        partition_col_indices: Vec<usize>,
        partition_col_names: Vec<String>,
        data_col_indices: Vec<usize>,
        compression: Compression,
        runtime_env: Arc<RuntimeEnv>,
        object_store_options: HashMap<String, String>,
    ) -> Result<Self> {
        // `new` only assembles the struct; it performs no IO. All writer
        // creation (including the non-partitioned single-file case) happens
        // lazily in the async path so that the storage factory's internal
        // `block_on` always runs on a blocking thread and never on a tokio
        // worker. See `writer_for` / `ensure_default_writer`.
        Ok(Self {
            writers: HashMap::new(),
            work_dir,
            partition_id,
            task_attempt_id,
            data_schema,
            partition_col_indices,
            partition_col_names,
            data_col_indices,
            compression,
            runtime_env,
            object_store_options,
        })
    }

    /// Build the absolute part-file path for a given partition sub-directory.
    fn build_path(&self, subdir: &str) -> String {
        let dir = if subdir.is_empty() {
            self.work_dir.clone()
        } else {
            format!("{}/{}", self.work_dir, subdir)
        };
        match self.task_attempt_id {
            Some(attempt_id) => format!(
                "{}/part-{:05}-{:05}.parquet",
                dir, self.partition_id, attempt_id
            ),
            None => format!("{}/part-{:05}.parquet", dir, self.partition_id),
        }
    }

    /// Synchronously construct a writer from owned arguments.
    ///
    /// The storage factory may call `block_on` internally (e.g. for S3), so this
    /// function MUST NOT run on a tokio worker thread. It is only ever invoked
    /// from inside a `spawn_blocking` closure (see `writer_for`).
    fn build_writer(
        path: String,
        schema: SchemaRef,
        compression: Compression,
        runtime_env: Arc<RuntimeEnv>,
        object_store_options: HashMap<String, String>,
    ) -> Result<Box<dyn ParquetWriter>> {
        let props = WriterProperties::builder()
            .set_compression(compression)
            .build();
        StorageWriterFactory::create(&path, schema, props, runtime_env, &object_store_options)
    }

    /// Return the writer for `subdir`, creating it on first use.
    ///
    /// All values needed by the factory are cloned into owned locals *before*
    /// the `.await`, so no borrow of `self` is held across the suspension point.
    /// This keeps the future `Send` (requiring only `PartitionedWriter: Send`)
    /// instead of forcing `Sync`/`dyn ParquetWriter: Sync`.
    async fn writer_for(&mut self, subdir: &str) -> Result<&mut Box<dyn ParquetWriter>> {
        if !self.writers.contains_key(subdir) {
            // Take everything we need by value before the await.
            let path = self.build_path(subdir);
            let schema = Arc::clone(&self.data_schema);
            let compression = self.compression;
            let runtime_env = Arc::clone(&self.runtime_env);
            let object_store_options = self.object_store_options.clone();

            // The factory may call `block_on` internally (S3), so run it on a
            // blocking thread to avoid "Cannot start a runtime from within a
            // runtime" on the tokio worker.
            let writer = tokio::task::spawn_blocking(move || {
                Self::build_writer(path, schema, compression, runtime_env, object_store_options)
            })
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!("Writer creation task failed to join: {e}"))
            })??;

            self.writers.insert(subdir.to_string(), writer);
        }
        Ok(self.writers.get_mut(subdir).unwrap())
    }

    /// Compute, for each row, the partition sub-directory string `col=val/...`.
    /// Partition columns are cast to UTF-8 to obtain their string form; `null`
    /// (or empty) values map to `__HIVE_DEFAULT_PARTITION__`.
    fn partition_subdirs(&self, batch: &RecordBatch) -> Result<Vec<String>> {
        let num_rows = batch.num_rows();
        if self.partition_col_indices.is_empty() {
            return Ok(vec![String::new(); num_rows]);
        }

        // Cast each partition column to Utf8 once for the whole batch.
        let mut casted: Vec<ArrayRef> = Vec::with_capacity(self.partition_col_indices.len());
        for &col_idx in &self.partition_col_indices {
            let arr = arrow::compute::cast(batch.column(col_idx).as_ref(), &DataType::Utf8)
                .map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to cast partition column to string: {e}"
                    ))
                })?;
            casted.push(arr);
        }
        let casted: Vec<&StringArray> = casted
            .iter()
            .map(|a| {
                a.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    DataFusionError::Internal(
                        "Partition column did not cast to StringArray".to_string(),
                    )
                })
            })
            .collect::<Result<_>>()?;

        let mut subdirs = Vec::with_capacity(num_rows);
        for row in 0..num_rows {
            let mut subdir = String::new();
            for (k, name) in self.partition_col_names.iter().enumerate() {
                if k > 0 {
                    subdir.push('/');
                }
                subdir.push_str(&escape_path_name(name));
                subdir.push('=');
                let arr = casted[k];
                if arr.is_null(row) || arr.value(row).is_empty() {
                    subdir.push_str(DEFAULT_PARTITION_NAME);
                } else {
                    subdir.push_str(&escape_path_name(arr.value(row)));
                }
            }
            subdirs.push(subdir);
        }
        Ok(subdirs)
    }

    /// Split `batch` by partition key and append each group to its writer.
    pub(crate) async fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let subdirs = self.partition_subdirs(batch)?;

        // Group row indices by their partition sub-directory.
        let mut groups: HashMap<String, Vec<u32>> = HashMap::new();
        for (row, subdir) in subdirs.into_iter().enumerate() {
            groups.entry(subdir).or_default().push(row as u32);
        }

        for (subdir, rows) in groups {
            // Project to data columns only (partition columns live in the path).
            let indices = UInt32Array::from(rows);
            let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.data_col_indices.len());
            for &col_idx in &self.data_col_indices {
                let taken = arrow::compute::take(batch.column(col_idx).as_ref(), &indices, None)
                    .map_err(|e| {
                        DataFusionError::Execution(format!("Failed to take partition rows: {e}"))
                    })?;
                columns.push(taken);
            }
            let sub_batch =
                RecordBatch::try_new(Arc::clone(&self.data_schema), columns).map_err(|e| {
                    DataFusionError::Execution(format!("Failed to build partition sub-batch: {e}"))
                })?;

            let writer = self.writer_for(&subdir).await?;
            writer
                .write_batch(&sub_batch)
                .await
                .map_err(|e| DataFusionError::Execution(format!("Failed to write batch: {e}")))?;
        }
        Ok(())
    }

    /// Close every open writer concurrently, returning the absolute paths of
    /// all part-files that were actually created (with their partition
    /// sub-directories). An empty input yields an empty Vec — no phantom files.
    pub(crate) async fn close(self) -> Result<Vec<String>> {
        // Compute paths *before* consuming `self.writers` (build_path borrows &self).
        let paths: Vec<String> = self.writers.keys().map(|s| self.build_path(s)).collect();

        let closes = self.writers.into_values().map(|w| w.close());
        try_join_all(closes)
            .await
            .map_err(|e| DataFusionError::Execution(format!("Failed to close writer: {e}")))?;

        Ok(paths)
    }
}
