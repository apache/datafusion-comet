/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.comet.delta

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow}
import org.apache.spark.sql.execution.datasources.{FileIndex, PartitionDirectory}
import org.apache.spark.sql.types.{LongType, StructField, StructType}

/**
 * Wraps a Delta `FileIndex` and augments each per-file `PartitionDirectory` with two synthetic
 * partition values drawn from the corresponding Delta `AddFile`:
 *
 *   - `baseRowIdColumnName` <- `AddFile.baseRowId`
 *   - `defaultRowCommitVersionColumnName` <- `AddFile.defaultRowCommitVersion`
 *
 * Used by Comet's row-tracking phase 3: `_row_id_` and `_row_commit_version_` values that are
 * still null in their materialised physical columns get synthesised by the outer Project as:
 *
 * row_id = coalesce(materialised_row_id, baseRowIdCol + _tmp_metadata_row_index)
 * row_commit_version = coalesce(materialised_row_commit_version, defaultRowCommitVersionCol)
 *
 * once the scan can see the per-file values as constant columns.
 *
 * The map from file basename (tail of `AddFile.path`) to `RowTrackingFileInfo` is supplied by the
 * caller (via reflection on the delegate's `matchingFiles` API) so we don't need a compile-time
 * dep on spark-delta. Each listed directory entry is split into one `PartitionDirectory` per file
 * so each file's values travel with it.
 */
class RowTrackingAugmentedFileIndex(
    delegate: FileIndex,
    infoByFileName: Map[String, DeltaReflection.RowTrackingFileInfo],
    baseRowIdColumnName: String,
    defaultRowCommitVersionColumnName: String)
    extends FileIndex {

  override def rootPaths: scala.collection.Seq[Path] = delegate.rootPaths

  override def inputFiles: Array[String] = delegate.inputFiles

  override def refresh(): Unit = delegate.refresh()

  override def sizeInBytes: Long = delegate.sizeInBytes

  /** Appends both synthetic columns as Long, nullable partition columns. */
  override def partitionSchema: StructType =
    delegate.partitionSchema
      .add(StructField(baseRowIdColumnName, LongType, nullable = true))
      .add(StructField(defaultRowCommitVersionColumnName, LongType, nullable = true))

  /**
   * Delegates listing to the underlying FileIndex, then splits each returned `PartitionDirectory`
   * into one-per-file directories, each carrying the original partition values PLUS the per-file
   * baseRowId and defaultRowCommitVersion.
   */
  override def listFiles(
      partitionFilters: scala.collection.Seq[Expression],
      dataFilters: scala.collection.Seq[Expression]): scala.collection.Seq[PartitionDirectory] = {
    val underlying = delegate.listFiles(partitionFilters, dataFilters)
    underlying.flatMap { pd =>
      pd.files.map { fileStatus =>
        val info = infoByFileName.getOrElse(
          fileStatus.getPath.getName,
          DeltaReflection.RowTrackingFileInfo(None, None))
        PartitionDirectory(augmentPartitionValues(pd.values, info), Seq(fileStatus))
      }
    }
  }

  private def augmentPartitionValues(
      original: InternalRow,
      info: DeltaReflection.RowTrackingFileInfo): InternalRow = {
    val n = original.numFields
    val values = new Array[Any](n + 2)
    var i = 0
    while (i < n) {
      values(i) = original.get(i, delegate.partitionSchema.fields(i).dataType)
      i += 1
    }
    values(n) = info.baseRowId.map(Long.box).orNull
    values(n + 1) = info.defaultRowCommitVersion.map(Long.box).orNull
    new GenericInternalRow(values)
  }
}
