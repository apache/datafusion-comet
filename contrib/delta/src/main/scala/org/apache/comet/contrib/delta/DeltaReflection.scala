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

package org.apache.comet.contrib.delta

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.{Scan => V2Scan}
import org.apache.spark.sql.execution.datasources.{FileFormat, HadoopFsRelation}
import org.apache.spark.sql.types.StructType

import org.apache.comet.serde.OperatorOuterClass

/**
 * Class-name-based probes for Delta Lake plan nodes.
 *
 * We deliberately avoid a compile-time dependency on `spark-delta` - the Scala API surface churns
 * across Delta versions (2.x / 3.x / 4.x) and we want Comet's Delta detection to keep working
 * against whichever Delta version the user has on their classpath. All detection is therefore
 * done via fully-qualified class names and standard Spark APIs (HadoopFsRelation, V2Scan), which
 * have been stable for years.
 *
 * What this object provides:
 *   - `isDeltaFileFormat(fileFormat)`: true for `DeltaParquetFileFormat` and any subclass exposed
 *     by the delta-spark package.
 *   - `isDeltaV2Scan(scan)`: true for the V2 `DeltaScan` / `DeltaTableV2`-backed scan.
 *   - `extractTableRoot(relation)`: pulls the table root URI out of a `HadoopFsRelation`. Works
 *     for both path-based reads (`format("delta").load("/tmp/t")`) and table-based reads
 *     (`spark.table("delta_tbl")`).
 */
object DeltaReflection extends Logging {

  /** Fully-qualified class names we match on. */
  object ClassNames {
    val DELTA_PARQUET_FILE_FORMAT = "org.apache.spark.sql.delta.DeltaParquetFileFormat"
    val DELTA_V2_SCAN_PACKAGE_PREFIX = "org.apache.spark.sql.delta."
    val DELTA_V2_SCAN_SIMPLE_NAME = "DeltaScan"
  }

  /**
   * Synthetic column name that Delta's `PreprocessTableWithDVs` rule injects into a scan's output
   * schema when the relation has deletion vectors in use. Value `0` means "keep the row", any
   * other value means "drop it". Used to detect DV-rewritten Delta scans.
   *
   * Stable across Delta 2.x / 3.x - defined in
   * `DeltaParquetFileFormat.IS_ROW_DELETED_COLUMN_NAME`.
   */
  val IsRowDeletedColumnName: String = "__delta_internal_is_row_deleted"

  /**
   * Delta's intermediate row-index column. Inserted by
   * `DeltaParquetFileFormat.prepareSchemaForRead` and used by Delta's row-tracking /
   * row-index logic before the public `__delta_internal_row_index` is exposed. Defined in
   * `DeltaParquetFileFormat.TMP_METADATA_ROW_INDEX_COLUMN_NAME`. Appears in plans that
   * read `_metadata.row_index` from a row-tracking-enabled table.
   */
  val TmpMetadataRowIndexColumnName: String = "_tmp_metadata_row_index"

  /**
   * Synthetic column name Delta requests on the parquet scan when it needs the per-row physical
   * position within the file (e.g. for downstream DV bitmap lookup in `useMetadataRowIndex` mode,
   * or test-only reads of the metadata column). Produced only by `DeltaParquetFileFormat`'s
   * reader; Comet's parquet reader has no equivalent synthesis.
   *
   * Stable across Delta 2.x / 3.x - defined in `DeltaParquetFileFormat.ROW_INDEX_COLUMN_NAME`.
   */
  val RowIndexColumnName: String = "__delta_internal_row_index"

  /**
   * Returns true if `fileFormat` is Delta's parquet-backed `FileFormat`. Checks the exact class
   * plus any subclass, so variants like `DeletionVectorBoundFileFormat` (some Delta versions)
   * also match.
   */
  def isDeltaFileFormat(fileFormat: FileFormat): Boolean = {
    val cls = fileFormat.getClass
    isDeltaClassName(cls.getName) || isDeltaParquetSubclass(cls)
  }

  /** Walks the class hierarchy looking for DeltaParquetFileFormat. */
  private def isDeltaParquetSubclass(cls: Class[_]): Boolean = {
    var current: Class[_] = cls
    while (current != null) {
      if (current.getName == ClassNames.DELTA_PARQUET_FILE_FORMAT) return true
      current = current.getSuperclass
    }
    false
  }

  private def isDeltaClassName(name: String): Boolean =
    name == ClassNames.DELTA_PARQUET_FILE_FORMAT ||
      (name.startsWith(ClassNames.DELTA_V2_SCAN_PACKAGE_PREFIX) &&
        name.endsWith("ParquetFileFormat"))

  /**
   * Returns true if `scan` is the V2 scan implementation Delta produces for a
   * `DeltaTableV2`-backed read. Delta ships this as `org.apache.spark.sql.delta.DeltaScan` (inner
   * case class of `DeltaScanBuilder` or similar) - the enclosing class name varies by version, so
   * we match on the simple name + package prefix rather than an exact FQN.
   */
  def isDeltaV2Scan(scan: V2Scan): Boolean = {
    val name = scan.getClass.getName
    name.startsWith(ClassNames.DELTA_V2_SCAN_PACKAGE_PREFIX) &&
    name.contains(ClassNames.DELTA_V2_SCAN_SIMPLE_NAME)
  }

  /**
   * Extract the Delta table root from a V1 `HadoopFsRelation`. For Delta tables this is always a
   * single path - Delta does not support multi-root relations.
   *
   * Returns the absolute URI as a string, with whatever scheme the relation was opened with
   * (`file://`, `s3://`, etc.).
   */
  def extractTableRoot(relation: HadoopFsRelation): Option[String] = {
    try {
      val roots = relation.location.rootPaths
      roots.headOption.map(pathToSingleEncodedUri)
    } catch {
      case e: Exception =>
        logWarning(s"Failed to extract Delta table root path: ${e.getMessage}")
        None
    }
  }

  /**
   * Convert a Hadoop `Path` to a URI string whose decoded path component matches the literal
   * on-disk filesystem path Hadoop uses when reading the file.
   *
   * Hadoop's `RawLocalFileSystem.pathToFile` treats the bytes of `path.toUri.getRawPath` -- i.e.
   * the URI's path component WITHOUT decoding -- as the literal filesystem path. So if Hadoop's
   * Path stores URI form `file:/T/spark%25dir%25prefix-uuid` (typical for Delta tests whose
   * `defaultTempDirPrefix` is the literal `spark%dir%prefix`), the actual on-disk dir name is
   * `spark%25dir%25prefix-uuid` (with `%25` literal in the filename, four chars `%`, `2`, `5`).
   *
   * To send a URI that the native side can decode back to that on-disk literal, we take the raw
   * path component verbatim and URL-encode `%` one extra time, yielding
   * `file:/T/spark%2525dir%2525prefix-uuid`. The native scan decodes once (`%2525` -> `%25`) and
   * opens at the literal `%25` filename.
   */
  def pathToSingleEncodedUri(p: org.apache.hadoop.fs.Path): String = {
    // Hadoop's `Path` keeps two forms of the same URI:
    //   - `path.toString` returns a once-decoded form for display: any `%XX`
    //     escape stored in the URI is decoded once. For Delta tests whose
    //     `defaultTempDirPrefix` is the literal `spark%dir%prefix` and whose
    //     on-disk dir Spark actually creates is `spark%25dir%25prefix-uuid`
    //     (with `%25` four-char-literal in the filename), this returns
    //     `file:/T/spark%25dir%25prefix-uuid` -- which when fed to a URL
    //     parser would single-decode to a non-existent `spark%dir%prefix-uuid`.
    //   - `path.toUri.toString` returns the FULL URI form, double-encoding the
    //     literal `%` chars (`%25` -> `%2525`). When the native side parses
    //     this and percent-decodes once, it recovers the literal on-disk
    //     filename `spark%25dir%25prefix-uuid`.
    //
    // We always want the second form for native consumption, so the raw
    // ParquetSource open path matches Hadoop's `RawLocalFileSystem`
    // interpretation (which reads the URI's raw path component verbatim as
    // the filesystem path).
    p.toUri.toString
  }

  /**
   * Extract the resolved snapshot version from Delta's `FileIndex`. Delta's file index is a
   * `TahoeLogFileIndex` / `PreparedDeltaFileIndex` which has already pinned a specific snapshot
   * by the time we see it, including when the user supplied `versionAsOf` or `timestampAsOf`.
   *
   * The toString format is stable: `Delta[version=<N>, <path>]`. We parse that rather than
   * reaching into Delta's internals because the actual field names differ across Delta versions
   * (snapshotAtAnalysis vs tahoeFileIndex.snapshot vs etc.). Regex is a single point of failure
   * that's easy to update if the format ever changes.
   *
   * Returns the version as a `Long`, or `None` if parsing fails / the file index isn't a Delta
   * one (callers should fall back to `-1` = latest).
   */
  private val DeltaFileIndexVersionRegex = """^Delta\[version=(-?\d+),""".r

  /**
   * Extract the Delta table `Metadata` action's configuration map from a `HadoopFsRelation`'s
   * `TahoeFileIndex`-derivative location via reflection. Returns `None` when the lookup fails
   * (e.g. non-Delta relation, or an index type that does not expose `metadata`).
   *
   * The configuration carries user- and system-set table properties keyed by dotted names like
   * `delta.rowTracking.materializedRowIdColumnName`. Used by the CometScanRule row-tracking
   * support to discover the physical column name into which Delta has materialised `row_id`.
   */
  /**
   * The analysis-time Delta schema from `DeltaParquetFileFormat.referenceSchema` (= the
   * captured `Metadata.schema`). Available only while the ORIGINAL Delta file format is still
   * present (e.g. in `DeltaScanRule`, before core Comet replaces it with
   * `CometParquetFileFormat`). Its fields preserve `delta.columnMapping.physicalName` /
   * `delta.columnMapping.id` metadata, so it is the correct source for resolving column-mapping
   * physical names / field-ids against the schema the query was analyzed with (rather than the
   * latest snapshot). Returns None when the file format isn't a Delta format exposing it.
   */
  def extractFileFormatReferenceSchema(relation: HadoopFsRelation): Option[StructType] =
    try {
      invokeNoArg(relation.fileFormat, "referenceSchema").collect { case s: StructType => s }
    } catch {
      case scala.util.control.NonFatal(_) => None
    }

  def extractMetadataConfiguration(relation: HadoopFsRelation): Option[Map[String, String]] = {
    try {
      val location: Any = relation.location
      // Three-shape lookup. `TahoeBatchFileIndex` exposes only a `SnapshotDescriptor`
      // (not `Snapshot`), so the `snapshot.metadata` chain misses for it; we walk
      // `deltaLog.update().metadata` for the case-3 fallback. Keeping this in sync with
      // `extractSnapshotSchema` below is critical: when CM is enabled and we miss the
      // config, the contrib doesn't detect column mapping is active and falls back to
      // logical-name reads on physically-renamed files.
      val metadataObj: Option[AnyRef] =
        findAccessor(location, Seq("metadata"))
          .orElse(findAccessor(location, Seq("snapshot")).flatMap(findAccessor(_, Seq("metadata"))))
          .orElse {
            findAccessor(location, Seq("deltaLog")).flatMap { dl =>
              invokeNoArg(dl, "update").flatMap(findAccessor(_, Seq("metadata")))
            }
          }
      metadataObj.flatMap { m =>
        findAccessor(m, Seq("configuration")).collect {
          case scalaMap: Map[_, _] => scalaMap.asInstanceOf[Map[String, String]]
          case javaMap: java.util.Map[_, _] =>
            import scala.jdk.CollectionConverters._
            javaMap.asInstanceOf[java.util.Map[String, String]].asScala.toMap
        }
      }
    } catch {
      case e: Exception =>
        logWarning(s"Failed to extract Delta metadata configuration: ${e.getMessage}")
        None
    }
  }

  /** StructField metadata key under which Delta stores the column-mapping physical name. */
  val PhysicalNameMetadataKey: String = "delta.columnMapping.physicalName"

  /** StructField metadata key under which Delta stores the column-mapping field ID (CM-id mode). */
  val FieldIdMetadataKey: String = "delta.columnMapping.id"

  /**
   * StructField metadata key under which Spark+parquet store a field ID. Matches
   * arrow-rs's `PARQUET_FIELD_ID_META_KEY`, which is what Comet's native schema_adapter
   * looks for when `use_field_id=true`. To match the file's field IDs against the Spark
   * schema we copy from Delta's `delta.columnMapping.id` to this key before serializing
   * to native.
   */
  val ParquetFieldIdMetadataKey: String = "PARQUET:field_id"

  /**
   * Row-tracking column names. Pinned here so call sites in DeltaScanRule's
   * `applyRowTrackingRewrite` and CometDeltaNativeScan's synthetic-emit detection
   * stay in sync with the native exec's `synthetic_columns.rs` `ROW_ID_COLUMN_NAME` /
   * `ROW_COMMIT_VERSION_COLUMN_NAME`.
   */
  val RowIdColumnName: String = "row_id"
  val RowCommitVersionColumnName: String = "row_commit_version"

  /**
   * Extract the Delta table's Snapshot-level schema (`Metadata.schema()` in Delta terms) via
   * reflection. Unlike the `relation.dataSchema` we get from Spark -- which has its StructField
   * metadata stripped by HadoopFsRelation construction -- the Snapshot's schema preserves the
   * `delta.columnMapping.physicalName` and `delta.columnMapping.id` metadata on every StructField
   * at every level of nesting. This is the authoritative source for building a "physical schema"
   * to hand to the native parquet reader.
   */
  def extractSnapshotSchema(relation: HadoopFsRelation): Option[StructType] = {
    try {
      val location: Any = relation.location
      // Three-shape lookup. `TahoeBatchFileIndex` (UPDATE/DELETE/MERGE post-rewrite and
      // streaming micro-batches) exposes only a `SnapshotDescriptor` -- not a `Snapshot`,
      // and the SnapshotDescriptor doesn't expose `Metadata` directly. For those, walk
      // `deltaLog.update().metadata` (case 3) to get the LATEST snapshot's metadata.
      // That's also correct for UPDATE/DELETE: those commands re-read the table at commit
      // time, so the latest snapshot's column-mapping metadata is what governs how the
      // parquet files we're about to read are interpreted. Without case 3, CM-name tables
      // that have undergone RENAME COLUMN return wrong values from streaming/UPDATE/DELETE
      // reads because the contrib falls back to `relation.dataSchema` whose StructField
      // metadata is stripped by HadoopFsRelation, so `physicalName` is invisible,
      // `column_mappings` proto stays empty, and the native parquet reader reads by
      // logical name from a physically-renamed file.
      val metadataObj: Option[AnyRef] =
        findAccessor(location, Seq("metadata"))
          .orElse(findAccessor(location, Seq("snapshot")).flatMap(findAccessor(_, Seq("metadata"))))
          .orElse {
            findAccessor(location, Seq("deltaLog")).flatMap { dl =>
              invokeNoArg(dl, "update").flatMap(findAccessor(_, Seq("metadata")))
            }
          }
      metadataObj.flatMap { m =>
        // Delta's Metadata exposes a `schema(): StructType` method that parses its stored JSON
        // schema string. The returned StructType has full metadata preserved at every level.
        val schema = invokeNoArg(m, "schema").orElse(findAccessor(m, Seq("schema")))
        schema.collect { case s: StructType => s }
      }
    } catch {
      case scala.util.control.NonFatal(e) =>
        logWarning(s"Failed to extract Delta snapshot schema: ${e.getMessage}")
        None
    }
  }

  private def invokeNoArg(obj: Any, methodName: String): Option[AnyRef] = {
    if (obj == null) return None
    try {
      val m = lookupNoArgMethod(obj.getClass, methodName)
      if (m == null) None else Option(m.invoke(obj))
    } catch {
      case scala.util.control.NonFatal(_) => None
    }
  }

  /**
   * Table property key set to "true" when row tracking is enabled (Delta's
   * `DeltaConfigs.ROW_TRACKING_ENABLED`, key `enableRowTracking`). Only then are `row_id` /
   * `row_commit_version` reserved synthetic column names; otherwise they are ordinary user
   * column names.
   */
  val EnableRowTrackingProp: String = "delta.enableRowTracking"

  /** Property key for the physical column name Delta materialises row IDs into. */
  val MaterializedRowIdColumnProp: String =
    "delta.rowTracking.materializedRowIdColumnName"

  /** Property key for the physical column name Delta materialises row-commit-versions into. */
  val MaterializedRowCommitVersionColumnProp: String =
    "delta.rowTracking.materializedRowCommitVersionColumnName"

  /**
   * Row-tracking fields extracted per file for phase-3 synthesis of `_row_id_` and
   * `_row_commit_version_` when the materialised physical columns are null.
   */
  case class RowTrackingFileInfo(baseRowId: Option[Long], defaultRowCommitVersion: Option[Long])

  /**
   * Invoke `TahoeFileIndex.matchingFiles(partitionFilters = Nil, dataFilters = Nil)` on the given
   * `location`, extract each returned `AddFile`'s `path`, `baseRowId`, and
   * `defaultRowCommitVersion`, and return the resulting map keyed by file basename.
   *
   * Used by row-tracking Phase 3: we attach each file's starting row id and default commit
   * version as per-file synthetic partition columns. Returns `Map.empty` on reflection failure.
   */
  def extractRowTrackingInfoByFileName(location: Any): Map[String, RowTrackingFileInfo] = {
    if (location == null) return Map.empty
    try {
      val addFilesAny = callMatchingFiles(location).getOrElse(return Map.empty)
      val seq = addFilesAny match {
        case s: scala.collection.Seq[_] => s
        case a: Array[_] => a.toSeq
        case _ => return Map.empty
      }
      val result = scala.collection.mutable.Map.empty[String, RowTrackingFileInfo]
      seq.foreach { addFile =>
        val path = stringMember(addFile, "path")
        val baseRowId = optionLongMember(addFile, "baseRowId")
        val defaultVer = optionLongMember(addFile, "defaultRowCommitVersion")
        path.foreach { p =>
          if (baseRowId.isDefined || defaultVer.isDefined) {
            val name = new org.apache.hadoop.fs.Path(p).getName
            result.put(name, RowTrackingFileInfo(baseRowId, defaultVer))
          }
        }
      }
      result.toMap
    } catch {
      case _: Exception => Map.empty
    }
  }

  def extractSnapshotVersion(relation: HadoopFsRelation): Option[Long] = {
    try {
      val desc = relation.location.toString
      DeltaFileIndexVersionRegex.findFirstMatchIn(desc).map(_.group(1).toLong)
    } catch {
      case _: Exception => None
    }
  }

  /**
   * Read the LATEST committed version from the relation's underlying `DeltaLog`, via reflection
   * so we keep zero compile-time dep on spark-delta. Returns `None` when the relation isn't
   * backed by a Delta log (or reflection fails).
   */
  def extractLatestSnapshotVersion(relation: HadoopFsRelation): Option[Long] = {
    try {
      val deltaLogObj = findAccessor(relation.location, Seq("deltaLog")).orNull
      if (deltaLogObj == null) return None
      // `deltaLog.update()` returns the latest Snapshot; `snapshot.version` is a Long.
      val updated = invokeNoArg(deltaLogObj, "update").orNull
      if (updated == null) return None
      longMember(updated, "version")
    } catch {
      case _: Exception => None
    }
  }

  /**
   * True when `relation` is Delta's Change Data Feed relation (`DeltaCDFRelation`, produced by a
   * `readChangeFeed` read). A CDF read is a `RowDataSourceScanExec` over this `CatalystScan`
   * relation -- a different physical-node family than the `FileSourceScanExec` / `HadoopFsRelation`
   * the rest of the rule handles -- so it must be intercepted separately to engage the native
   * kernel `TableChanges` read path.
   */
  def isCdfRelation(relation: Any): Boolean =
    relation != null && relation.getClass.getName.contains("DeltaCDFRelation")

  /**
   * Table root (filesystem path) for a `DeltaCDFRelation`, read from its `DeltaLog.dataPath`.
   * The relation exposes `snapshotWithSchemaMode.snapshot.deltaLog` (and historically `deltaLog`
   * directly); `dataPath` is a Hadoop `Path`. Returns `None` on reflection failure.
   */
  def extractCdfTableRoot(relation: Any): Option[String] = {
    try {
      findAccessor(relation, Seq("snapshotWithSchemaMode"))
        .flatMap(findAccessor(_, Seq("snapshot")))
        .flatMap(findAccessor(_, Seq("deltaLog")))
        .orElse(findAccessor(relation, Seq("deltaLog")))
        .flatMap(dl => invokeNoArg(dl, "dataPath"))
        .map(_.toString)
    } catch {
      case scala.util.control.NonFatal(_) => None
    }
  }

  /**
   * `(startingVersion, endingVersion)` for a `DeltaCDFRelation`. Delta stores these as the
   * relation's `startingVersion: Option[Long]` / `endingVersion: Option[Long]` fields. A missing
   * start defaults to 0 (kernel `TableChanges` requires a concrete start); a missing end stays
   * `None` (kernel reads to the latest committed version). Returns `None` only on reflection
   * failure.
   */
  def extractCdfVersions(relation: Any): Option[(Long, Option[Long])] = {
    def asLong(any: Any): Option[Long] = any match {
      case l: Long => Some(l)
      case i: java.lang.Long => Some(i.toLong)
      case Some(x) => asLong(x)
      case _ => None
    }
    try {
      val start = findAccessor(relation, Seq("startingVersion")).flatMap(asLong).getOrElse(0L)
      val end = findAccessor(relation, Seq("endingVersion")).flatMap(asLong)
      Some((start, end))
    } catch {
      case scala.util.control.NonFatal(_) => None
    }
  }

  /**
   * The latest version available for a `DeltaCDFRelation`, read from its pinned snapshot
   * (`snapshotWithSchemaMode.snapshot.version`). Used to clamp a requested `endingVersion` that
   * exceeds the table's latest committed version: kernel's `TableChanges` errors if `end > latest`
   * (`LogSegment end version N not the same as the specified end version M`), whereas Delta clamps
   * to the latest. Returns `None` on reflection failure (caller leaves the requested end as-is).
   */
  def extractCdfLatestVersion(relation: Any): Option[Long] = {
    try {
      findAccessor(relation, Seq("snapshotWithSchemaMode"))
        .flatMap(findAccessor(_, Seq("snapshot")))
        .flatMap(snap => longMember(snap, "version"))
    } catch {
      case scala.util.control.NonFatal(_) => None
    }
  }

  /**
   * Convert a Delta partition value string to a Catalyst-internal representation. Delta stores
   * partition values as strings in add actions; this converts them to the correct type for
   * predicate evaluation.
   */
  /**
   * Normalized view of a single Delta `AddFile` extracted from a pre-materialized FileIndex
   * (`TahoeBatchFileIndex` / `CdcAddFileIndex`). Used by the scan rule to build a
   * kernel-independent `DeltaScanTask` list for streaming micro-batch reads and
   * MERGE/UPDATE/DELETE post-join rewrites, both of which already have the exact AddFile list in
   * hand and must NOT re-run kernel log replay (which would return a different file set).
   */
  case class ExtractedAddFile(
      /** Path as stored in the AddFile action -- may be relative or absolute. */
      path: String,
      size: Long,
      /** Raw partition values as Delta stores them, keyed by logical column name. */
      partitionValues: Map[String, String],
      /** Raw `stats` JSON string, or null. */
      statsJson: String,
      /** True if this AddFile has a non-null DeletionVectorDescriptor. */
      hasDeletionVector: Boolean,
      /**
       * The raw `DeletionVectorDescriptor` object (opaque via reflection -- the concrete type is
       * `org.apache.spark.sql.delta.actions.DeletionVectorDescriptor` but we keep it as `AnyRef`
       * to preserve the no-compile-time-dep-on-spark-delta property). `null` when the AddFile has
       * no DV. Pass to `materializeDeletedRowIndexes` to convert into a `Array[Long]` of deleted
       * row indexes.
       */
      dvDescriptor: AnyRef,
      /**
       * Delta row-tracking fields. `baseRowId` is the first logical row id covered by this file;
       * `defaultRowCommitVersion` is the commit that last wrote it. Both are `None` for tables
       * that don't have the rowTracking table feature enabled (or for pre-backfill files on a
       * table where row tracking was just enabled).
       */
      baseRowId: Option[Long],
      defaultRowCommitVersion: Option[Long],
      /**
       * Modification time of the underlying parquet file as recorded on the AddFile action
       * (`AddFile.modificationTime`). Epoch milliseconds. Surfaced through Spark's
       * `_metadata.file_modification_time` column when the contrib synthesises it.
       */
      modificationTime: Option[Long] = None)

  /**
   * Is this FileIndex a pre-materialized Delta index (batch or CDC)?
   *
   * CDC reads (`CdcAddFileIndex`, `TahoeRemoveFileIndex`, `TahoeChangeFileIndex`) all derive from
   * `TahoeBatchFileIndex` (conceptually or concretely) and stash the CDC metadata
   * (`_change_type`, `_commit_version`, `_commit_timestamp`) into `AddFile.partitionValues` with
   * a matching `partitionSchema`, so the native scan can materialise them as partition columns
   * without any special CDC-specific handling.
   */
  def isBatchFileIndex(location: Any): Boolean = {
    val cls = location.getClass.getName
    cls.contains("TahoeBatchFileIndex") ||
    cls.contains("CdcAddFileIndex") ||
    cls.contains("TahoeRemoveFileIndex") ||
    cls.contains("TahoeChangeFileIndex") ||
    cls.contains("PreparedDeltaFileIndex")
  }

  /**
   * Is this FileIndex one that carries an EXACT, runtime-determined file SUBSET that kernel log
   * replay cannot reproduce from the snapshot + a data predicate?
   *
   * These are MERGE / UPDATE / DELETE post-join rewrites (`TahoeBatchFileIndex`) and streaming /
   * CDC reads (`CdcAddFileIndex`, `TahoeRemoveFileIndex`, `TahoeChangeFileIndex`): the file set is
   * the join's touched files or a per-microbatch delta, NOT the whole snapshot. Kernel enumeration
   * would return a DIFFERENT (larger) set, so these keep sourcing files from the FileIndex's
   * `addFiles` (see `buildTaskListFromAddFiles`).
   *
   * `PreparedDeltaFileIndex` (regular reads -- the vast majority) is deliberately NOT here: it's the
   * pruned active file set of a pinned snapshot, which kernel reproduces exactly via log replay +
   * the shipped data predicate. Those go through `planDeltaScan` so kernel drives enumeration,
   * partition injection, DV, and row-tracking (the per-file transform) end to end.
   */
  def isSubsetFileIndex(location: Any): Boolean = {
    val cls = location.getClass.getName
    cls.contains("TahoeBatchFileIndex") ||
    cls.contains("CdcAddFileIndex") ||
    cls.contains("TahoeRemoveFileIndex") ||
    cls.contains("TahoeChangeFileIndex")
  }

  /**
   * A subset FileIndex whose touched `AddFile`s are a SUBSET of the pinned snapshot's active files
   * (MERGE / UPDATE / DELETE post-join rewrites, and streaming appends): `TahoeBatchFileIndex`. These
   * can be served by kernel enumeration FILTERED to the touched paths (every touched file is in the
   * snapshot, so kernel has its transform). The CDC family (`CdcAddFileIndex` / `TahoeRemoveFileIndex`
   * / `TahoeChangeFileIndex`) is NOT here -- it references removed / `_change_data` files outside the
   * snapshot, so kernel enumeration can't supply their transforms; those keep the legacy path.
   */
  def isDmlRewriteFileIndex(location: Any): Boolean =
    location.getClass.getName.contains("TahoeBatchFileIndex")

  /**
   * Detect whether the FileIndex carries a non-empty `rowIndexFilters` map. Delta
   * uses this to flag CDC "delete events" / "insert events" reads where the DV
   * bitmap semantics are INVERTED relative to a normal batch read: native
   * batch reads filter OUT the rows in the bitmap, but CDC needs the rows
   * IN the bitmap (the rows that are being newly deleted / newly inserted).
   * Our native scan currently only implements the batch semantics, so this
   * method lets `DeltaScanRule` decline these CDC-special reads and fall
   * back to Spark's reader.
   *
   * Returns true when:
   *  - the FileIndex exposes a `rowIndexFilters` accessor that returns
   *    `Some(map)` with non-empty contents, OR
   *  - reflection succeeds but the value is `null` => treat as not set.
   *
   * Conservative on reflection failure -- returns false (don't decline) so a
   * Delta version drift that renames the field doesn't silently break the
   * happy path. The DV cardinality / column-count fixes are still applied.
   */
  def hasInvertedRowIndexFilters(location: Any): Boolean = {
    try {
      findAccessor(location, Seq("rowIndexFilters")) match {
        case Some(opt: Option[_]) =>
          opt match {
            case Some(m: scala.collection.Map[_, _]) => m.nonEmpty
            case Some(m: java.util.Map[_, _]) => !m.isEmpty
            case _ => false
          }
        case Some(m: scala.collection.Map[_, _]) => m.nonEmpty
        case Some(m: java.util.Map[_, _]) => !m.isEmpty
        case _ => false
      }
    } catch {
      case scala.util.control.NonFatal(_) => false
    }
  }

  /**
   * Extract the AddFile list from a `TahoeBatchFileIndex`-like FileIndex via reflection (no
   * compile-time dep on spark-delta). Returns `None` when:
   *   - the FileIndex class doesn't expose an `addFiles: Seq[AddFile]` method
   *   - reflection fails for any entry
   *   - any AddFile's stats / fields can't be read
   *
   * Callers should fall back to Spark's Delta reader when this returns `None`.
   *
   * For CDC indexes (`CdcAddFileIndex`, `TahoeRemoveFileIndex`, `TahoeChangeFileIndex`) the raw
   * `addFiles` field does NOT contain the CDC metadata columns (`_change_type`,
   * `_commit_version`, `_commit_timestamp`); those are injected inside the index's
   * `matchingFiles(partitionFilters, dataFilters)` override. We therefore prefer
   * `matchingFiles(Seq.empty, Seq.empty)` when it's available, so the returned `partitionValues`
   * maps already carry the CDC metadata.
   */
  def extractBatchAddFiles(
      location: Any,
      partitionFilters: Seq[org.apache.spark.sql.catalyst.expressions.Expression] =
        Seq.empty): Option[Seq[ExtractedAddFile]] = {
    try {
      // PreparedDeltaFileIndex carries the pre-skipped scan result. Reading the
      // cached `preparedScan.files` returns whatever the FileIndex captured at
      // construction time -- so its DV descriptors are frozen at construction.
      // Re-query the scan's prepared snapshot (`preparedScan.scannedSnapshot`)
      // via `filesForScan` to pick up the freshest DV descriptors that snapshot
      // carries, WITHOUT switching to head (which would diverge from vanilla and
      // break time travel -- see preparedSnapshotFiles). Fall back to
      // `matchingFiles(Nil, Nil)`, then `preparedScan.files`, then raw `addFiles`
      // if any of these reflection calls fail.
      val refreshedFiles: Option[AnyRef] =
        if (location.getClass.getName.contains("PreparedDeltaFileIndex")) {
          preparedSnapshotFiles(location, partitionFilters)
        } else None
      val matchingFilesLive: Option[AnyRef] =
        if (refreshedFiles.isEmpty &&
          location.getClass.getName.contains("PreparedDeltaFileIndex")) {
          callMatchingFiles(location)
        } else None
      val preparedFiles: Option[AnyRef] =
        if (refreshedFiles.isEmpty && matchingFilesLive.isEmpty &&
          location.getClass.getName.contains("PreparedDeltaFileIndex")) {
          findAccessor(location, Seq("preparedScan"))
            .flatMap(ps => findAccessor(ps, Seq("files")))
        } else None
      // Prefer matchingFiles(Seq.empty, Seq.empty) -- it returns CDC-augmented
      // AddFiles on CDC indexes and the plain list on TahoeBatchFileIndex.
      // Fall back to the raw `addFiles`/`filesList` accessors for indexes that
      // don't expose a no-arg-safe matchingFiles.
      val addFilesOpt = refreshedFiles
        .orElse(matchingFilesLive)
        .orElse(preparedFiles)
        .orElse(callMatchingFiles(location))
        .orElse(findAccessor(location, Seq("addFiles", "filesList")))
      addFilesOpt.flatMap { addFilesAny =>
        val seq = addFilesAny match {
          case s: scala.collection.Seq[_] => s
          case a: Array[_] => a.toSeq
          case _ => return None
        }
        val out = new scala.collection.mutable.ArrayBuffer[ExtractedAddFile](seq.size)
        seq.foreach { addFile =>
          val path = stringMember(addFile, "path").getOrElse(return None)
          val size = longMember(addFile, "size").getOrElse(return None)
          val rawPV = findAccessor(addFile, Seq("partitionValues")).getOrElse(return None)
          val pv: Map[String, String] = rawPV match {
            case m: Map[_, _] => m.asInstanceOf[Map[String, String]]
            case m: java.util.Map[_, _] =>
              import scala.jdk.CollectionConverters._
              m.asInstanceOf[java.util.Map[String, String]].asScala.toMap
            case _ => return None
          }
          val stats = stringMember(addFile, "stats").orNull
          val dv = findAccessor(addFile, Seq("deletionVector")).orNull
          val baseRowId = optionLongMember(addFile, "baseRowId")
          val defaultRowCommitVersion = optionLongMember(addFile, "defaultRowCommitVersion")
          val modificationTime = longMember(addFile, "modificationTime")
          out += ExtractedAddFile(
            path,
            size,
            pv,
            stats,
            hasDeletionVector = dv != null,
            dvDescriptor = dv,
            baseRowId = baseRowId,
            defaultRowCommitVersion = defaultRowCommitVersion,
            modificationTime = modificationTime)
        }
        Some(out.toSeq)
      }
    } catch {
      case e: Exception =>
        logWarning(
          s"Failed to extract AddFiles from ${location.getClass.getName}: ${e.getMessage}")
        None
    }
  }

  // Resolve `DeltaSQLConf.TEST_DV_NAME_PREFIX` (a `ConfigEntry[String]`) and `SQLConf.getConf`
  // once per JVM. `None` when the entry is absent (Delta version drift) -> empty prefix.
  private lazy val dvNamePrefixAccessor: Option[(AnyRef, java.lang.reflect.Method)] =
    try {
      val confObjCls = Class.forName("org.apache.spark.sql.delta.sources.DeltaSQLConf$")
      val module = confObjCls.getField("MODULE$").get(null)
      val entry = confObjCls.getMethod("TEST_DV_NAME_PREFIX").invoke(module)
      val configEntryCls = Class.forName("org.apache.spark.internal.config.ConfigEntry")
      val getConf =
        classOf[org.apache.spark.sql.internal.SQLConf].getMethod("getConf", configEntryCls)
      Some((entry, getConf))
    } catch {
      case _: ReflectiveOperationException => None
      case _: LinkageError => None
    }

  /**
   * Delta's test-only deletion-vector filename prefix
   * (`spark.databricks.delta.testOnly.dvFileNamePrefix`). Delta prepends it to every DV file it
   * writes (`<prefix>deletion_vector_<uuid>.bin`) when `Utils.isTesting` is true; in production it
   * is empty. delta-kernel-rs has no knowledge of this JVM-only conf and resolves the un-prefixed
   * name, so the executor splices this prefix into kernel's resolved DV path (see
   * `dv_reader::read_dv_indexes`).
   *
   * Read via the `ConfigEntry` rather than `getConfString(key, "")`: the prefix is the entry's
   * (testing-gated) DEFAULT, not a session setting, so a raw string lookup would always see empty.
   * This mirrors how Delta's own writer/reader obtain it. Empty on Delta versions lacking the
   * entry (reflection miss -> production behaviour).
   */
  def dvFileNamePrefix(sqlConf: org.apache.spark.sql.internal.SQLConf): String =
    dvNamePrefixAccessor match {
      case Some((entry, getConf)) =>
        try getConf.invoke(sqlConf, entry).asInstanceOf[String]
        catch { case scala.util.control.NonFatal(_) => "" }
      case None => ""
    }

  /**
   * Convert Delta's `DeletionVectorDescriptor` into the proto `DeltaDvDescriptor` so the
   * native executor (not the driver) reads the DV bitmap. Pre-#218 the driver called
   * [[materializeDeletedRowIndexes]] up-front and shipped the full `Vec<u64>` to native --
   * for a 99 M-row DV that's a ~1 GB `long[]` retained on the driver heap. The descriptor
   * is KB-scale and the executor decodes via `kernel::DeletionVectorDescriptor::read`,
   * matching what the Iceberg contrib already does with `IcebergScanCommon.delete_files_pool`.
   *
   * Returns `None` when:
   *   - `dvDescriptor` is null (no DV on this file -- caller skips the field)
   *   - reflection setup fails (Delta DeletionVectorDescriptor layout drifted)
   *
   * Reflection layout (stable since Delta 2.0):
   * {{{
   *   case class DeletionVectorDescriptor(
   *     storageType: String,           // "u" UUID rel, "p" absolute, "i" inline
   *     pathOrInlineDv: String,
   *     offset: Option[Int],
   *     sizeInBytes: Int,
   *     cardinality: Long,
   *     maxRowIndex: Option[Long])
   * }}}
   */
  def addFileDvToProto(
      dvDescriptor: AnyRef,
      tableRoot: String): Option[OperatorOuterClass.DeltaDvDescriptor] = {
    if (dvDescriptor == null) return None
    try {
      val cls = dvDescriptor.getClass
      // Per-DV-file no-arg case-class accessors; route through the cached lookup so we don't
      // re-resolve `getMethod` on every AddFile (5 lookups/file on DV-heavy tables).
      val storageType =
        lookupNoArgMethod(cls, "storageType").invoke(dvDescriptor).asInstanceOf[String]
      val pathOrInline =
        lookupNoArgMethod(cls, "pathOrInlineDv").invoke(dvDescriptor).asInstanceOf[String]
      val sizeInBytes =
        lookupNoArgMethod(cls, "sizeInBytes").invoke(dvDescriptor).asInstanceOf[Int]
      val cardinality =
        lookupNoArgMethod(cls, "cardinality").invoke(dvDescriptor).asInstanceOf[Long]
      val offsetOpt =
        lookupNoArgMethod(cls, "offset").invoke(dvDescriptor).asInstanceOf[Option[Int]]

      val b = OperatorOuterClass.DeltaDvDescriptor.newBuilder()
      // For "u" (UUID-relative) storage we PRE-RESOLVE the absolute path via Delta's own
      // `DeletionVectorDescriptor.absolutePath` (which honours Delta's JVM-static
      // `DELETION_VECTOR_FILE_NAME_PREFIX` from `DeltaSQLConf.TEST_DV_NAME_PREFIX` --
      // `"test%dv%prefix-"` under `Utils.isTesting`) and ship as "p" (absolute). Reason:
      // delta-kernel-rs follows the protocol literally (file name is `deletion_vector_<uuid>.bin`,
      // any optional prefix is recovered from `pathOrInlineDv.length - 20`), but Delta's test
      // fixtures bake a NON-protocol filename prefix into the on-disk name that's recoverable
      // ONLY via the JVM static. Pre-resolving here keeps the executor's kernel read working
      // against a real URL (no reconstruction) and also future-proofs against any other Delta
      // path-resolution extensions the JVM side might add.
      val (finalStorage, finalPath) = storageType match {
        case "i" =>
          // Inline DV: bytes carried in pathOrInlineDv as base85; no resolution needed.
          (storageType, if (pathOrInline == null) "" else pathOrInline)
        case "u" | "p" if tableRoot != null && tableRoot.nonEmpty =>
          // For BOTH "u" (UUID-relative) and "p" (absolute) we delegate to Delta's
          // `DeletionVectorDescriptor.absolutePath` and ship the resolved absolute URL
          // as storage-type "p". Two reasons:
          //
          //  1. For "u" tables, Delta honours its JVM-static
          //     `DELETION_VECTOR_FILE_NAME_PREFIX` (`DeltaSQLConf.TEST_DV_NAME_PREFIX`,
          //     `"test%dv%prefix-"` under `Utils.isTesting`). delta-kernel-rs doesn't
          //     know about that JVM hack and would compute the wrong filename. Doing the
          //     resolution here means the executor reads a pre-resolved absolute URL.
          //
          //  2. For "p" tables, calling `absolutePath` exercises Delta's own
          //     `new URI(pathOrInlineDv)` parse, which throws `URISyntaxException` for
          //     malformed inputs (e.g. DeletionVectorsSuite's
          //     "absolute DV path with not-encoded special characters" test) -- exactly
          //     the same exception vanilla Delta would throw at read time, so
          //     `interceptWithUnwrapping[URISyntaxException]` matches. If we left "p"
          //     paths verbatim the URI parse would fail on the executor and surface as a
          //     plain `CometNativeException`, breaking the assertion.
          //
          // `tableRoot` from the contrib is DOUBLE-URL-encoded (see
          // `materializeDeletedRowIndexes` lines 641-659 for the full note); the Hadoop FS
          // path on disk is the SINGLE-decoded form, so we URLDecode once before building
          // the Path. Without this, tables in dirs like `s p a r k %2a-uuid` (literal
          // spaces + literal `%2a`) end up double-encoded.
          val singleEncoded =
            try {
              java.net.URLDecoder.decode(
                tableRoot,
                java.nio.charset.StandardCharsets.UTF_8.name())
            } catch {
              case _: IllegalArgumentException => tableRoot
            }
          val tablePath = new org.apache.hadoop.fs.Path(singleEncoded)
          // Reflective invocation preserves this file's no-compile-time-dep on `spark-delta`
          // invariant (see header) -- a direct `asInstanceOf[DeletionVectorDescriptor]` would
          // import that class at compile time, and a Delta version that renames/relocates
          // it would surface as a NoClassDefFoundError that bypasses the
          // ReflectiveOperationException catch below. `getDeclaredMethod` is safe across
          // the supported Delta versions (3.3.2 / 4.0.0 / 4.1.0) because the
          // `absolutePath(Path): Path` signature is stable.
          val absMethod =
            lookupMethod(cls, "absolutePath", Seq(classOf[org.apache.hadoop.fs.Path]))
          val abs = absMethod.invoke(dvDescriptor, tablePath)
            .asInstanceOf[org.apache.hadoop.fs.Path]
          // Use Hadoop's URI form (which Delta itself uses for "p" descriptors via
          // `copyWithAbsolutePath` -> `SparkPath.fromPath(...).urlEncoded`).
          ("p", abs.toUri.toString)
        case _ =>
          (storageType, if (pathOrInline == null) "" else pathOrInline)
      }
      b.setStorageType(finalStorage)
      b.setPathOrInlineDv(finalPath)
      // sizeInBytes/cardinality are non-negative by construction; cast widens to u64 wire.
      b.setSizeInBytes(sizeInBytes.toLong)
      b.setCardinality(cardinality)
      offsetOpt.foreach(o => b.setOffset(o.toLong))
      Some(b.build())
    } catch {
      // `InvocationTargetException` wraps anything Delta's own code threw -- e.g.
      // `URISyntaxException` from `absolutePath` parsing a malformed "p" path. We
      // MUST re-throw the inner cause unchanged so vanilla Delta's error contract
      // is preserved (DeletionVectorsSuite "absolute DV path with not-encoded
      // special characters" expects `interceptWithUnwrapping[URISyntaxException]`
      // to match it). Swallowing here would turn the failure into a silent
      // wrong-result. NoSuchMethodException / IllegalAccessException are the
      // only ReflectiveOperationException variants that count as "reflection
      // setup failure" (Delta version drift -- field rename, class missing) and
      // are safe to log + return None for.
      case e: java.lang.reflect.InvocationTargetException
          if e.getCause != null => throw e.getCause
      case e: NoSuchMethodException =>
        logWarning(
          s"addFileDvToProto: missing method on DeletionVectorDescriptor " +
            s"(class=${dvDescriptor.getClass.getName}): ${e.getMessage}")
        None
      // The cached `lookupNoArgMethod`/`lookupMethod` return null (not throw) when a method
      // is absent, so a version-drift miss surfaces here as an NPE on `null.invoke(...)`
      // rather than NoSuchMethodException. Treat it identically: log + decline so the scan
      // falls back to vanilla Delta instead of crashing planning.
      case e: NullPointerException =>
        logWarning(
          s"addFileDvToProto: reflective method not found (cache miss) on " +
            s"DeletionVectorDescriptor (class=${dvDescriptor.getClass.getName}): ${e.getMessage}")
        None
      case e: IllegalAccessException =>
        logWarning(
          s"addFileDvToProto: illegal access on DeletionVectorDescriptor " +
            s"(class=${dvDescriptor.getClass.getName}): ${e.getMessage}")
        None
      // ClassCastException from the .asInstanceOf calls on reflected getters --
      // also Delta-version-drift territory (a field's runtime type changed).
      case e: ClassCastException =>
        logWarning(
          s"addFileDvToProto: unexpected field type on DeletionVectorDescriptor " +
            s"(class=${dvDescriptor.getClass.getName}): ${e.getMessage}")
        None
    }
  }

  /**
   * Materialize a `DeletionVectorDescriptor` into the list of deleted row indexes (0-based,
   * sorted ascending) using Delta's own `HadoopFileSystemDVStore` + `RoaringBitmapArray.toArray`.
   *
   * Returns `None` when:
   *   - `dvDescriptor` is null (no DV on this file)
   *   - the Delta classes aren't on the classpath (different Delta version layout, etc.)
   *   - the read itself fails (corrupt DV file, missing file, etc.)
   *
   * Callers that need DV semantics must fall back to Spark+Delta when this returns `None`.
   *
   * Driver-side only: don't call this on executors, since it touches the filesystem and the DV
   * store may not be initialised. The native side then plumbs the row-index array into the proto
   * task's `deleted_row_indexes` field, which `DeltaDvFilterExec` already consumes.
   *
   * @deprecated Superseded by [[addFileDvToProto]] (driver ships descriptor, executor decodes).
   *             Retained only for tests that exercise the pre-refactor path; remove once the
   *             pushdown-suite 1 g run is green.
   */
  @deprecated("Use addFileDvToProto + executor-side decode", "0.18")
  def materializeDeletedRowIndexes(
      dvDescriptor: AnyRef,
      tableRoot: String,
      hadoopConf: org.apache.hadoop.conf.Configuration): Option[Array[Long]] = {
    if (dvDescriptor == null) return None
    try {
      // scalastyle:off classforname
      val storeCls =
        Class.forName("org.apache.spark.sql.delta.storage.dv.HadoopFileSystemDVStore")
      // scalastyle:on classforname
      val store = storeCls
        .getConstructor(classOf[org.apache.hadoop.conf.Configuration])
        .newInstance(hadoopConf)
        .asInstanceOf[AnyRef]
      val readMethod = storeCls.getMethods
        .find { m =>
          m.getName == "read" &&
          m.getParameterCount == 2 &&
          m.getParameterTypes()(1) == classOf[org.apache.hadoop.fs.Path]
        }
        .getOrElse(return None)
      // `tableRoot` is the contrib's double-URL-encoded form (output of
      // `pathToSingleEncodedUri`, designed so the NATIVE side's
      // RawLocalFileSystem.pathToFile decodes once to the literal on-disk
      // path). The JVM-side `HadoopFileSystemDVStore` resolves the DV file
      // via Hadoop FS, which expects the URI raw path to ALREADY be the
      // single-encoded form (Hadoop URI's getRawPath is consumed verbatim
      // by RawLocalFileSystem.pathToFile). Decode the table root once
      // before constructing the Hadoop Path so the DV store's file
      // resolution lands on the literal on-disk file -- otherwise tables
      // in temp dirs like Delta's `s p a r k %2a-uuid` (with literal
      // spaces + `%2a`) yield "file not found" / null read and our scan
      // silently treats every row as not-deleted (or deleted, depending
      // on caller default), breaking OPTIMIZE / MERGE on DV-bearing
      // tables.
      val singleEncoded = try {
        java.net.URLDecoder.decode(tableRoot, java.nio.charset.StandardCharsets.UTF_8.name())
      } catch {
        case _: IllegalArgumentException => tableRoot
      }
      val tablePath = new org.apache.hadoop.fs.Path(singleEncoded)
      val bitmap = readMethod.invoke(store, dvDescriptor, tablePath)
      // RoaringBitmapArray.toArray returns Array[Long] of all set bits (= deleted row indexes).
      val toArrayMethod = bitmap.getClass.getMethod("toArray")
      val indexes = toArrayMethod.invoke(bitmap).asInstanceOf[Array[Long]]
      Some(indexes)
    } catch {
      case e: java.lang.reflect.InvocationTargetException =>
        // The reflective `read` (or bitmap decode) ran Delta's own DV-store logic, which
        // threw a genuine DV-read error: the .bin is missing/corrupt (FileNotFoundException)
        // or its on-disk path is malformed -- e.g. an absolute DV path with not-encoded
        // special characters yields a URISyntaxException ("Malformed escape pair"). Spark/Delta
        // surface these to the user; swallowing them here and reading WITHOUT the DV would
        // silently resurface deleted rows (a correctness violation) and also masks the
        // expected failure (DeletionVectorsSuite "resource leak" / "absolute DV path" tests).
        // Propagate as a SparkException so the query fails the same way vanilla Delta does.
        val cause = Option(e.getCause).getOrElse(e)
        throw new org.apache.spark.SparkException(
          s"Failed to read deletion vector for Delta table $tableRoot", cause)
      case scala.util.control.NonFatal(e) =>
        // Reflective *setup* failure (Delta DV-store class/method absent or a different
        // layout): we can't materialise via this path, so fall back to Spark+Delta, which
        // can read the DV itself.
        logWarning(
          s"materializeDeletedRowIndexes setup failed for table $tableRoot: ${e.getMessage}")
        None
    }
  }

  /**
   * Extract number-of-records from an AddFile's `stats` JSON. Returns `None` if stats is missing
   * / malformed. The JSON structure is stable across Delta versions: `{"numRecords": N, ...}`.
   */
  def parseNumRecords(statsJson: String): Option[Long] = {
    if (statsJson == null) return None
    val idx = statsJson.indexOf("\"numRecords\"")
    if (idx < 0) return None
    // Find the colon after the key, then the first numeric sequence.
    val colon = statsJson.indexOf(':', idx)
    if (colon < 0) return None
    var i = colon + 1
    while (i < statsJson.length && !statsJson.charAt(i).isDigit && statsJson.charAt(i) != '-') {
      i += 1
    }
    val start = i
    while (i < statsJson.length && (statsJson.charAt(i).isDigit || statsJson.charAt(i) == '-')) {
      i += 1
    }
    if (start == i) {
      None
    } else {
      try Some(statsJson.substring(start, i).toLong)
      catch { case _: NumberFormatException => None }
    }
  }

  /**
   * Invoke `FileIndex.matchingFiles(partitionFilters: Seq[Expression], dataFilters:
   * Seq[Expression]): Seq[AddFile]` with empty filter sequences via reflection.
   *
   * Returns `None` if the method is missing or the invocation throws. Comet does not have a
   * compile-time dep on spark-delta, so we reach for reflection here.
   */
  /**
   * Re-resolve the matching files for a `PreparedDeltaFileIndex` against the snapshot the scan was
   * prepared against (`preparedScan.scannedSnapshot`). Returns `Some(Seq[AddFile])` (typed as the
   * raw AnyRef from the Delta API) when the re-query succeeds, `None` when reflection fails or the
   * FileIndex isn't a `PreparedDeltaFileIndex`.
   *
   * Why re-query instead of using cached `preparedScan.files`: the cached list freezes its DV
   * descriptors at FileIndex construction; `scannedSnapshot.filesForScan(filters, false).files`
   * returns AddFiles with the freshest DV descriptors carried by that snapshot. Why
   * `scannedSnapshot` and not `deltaLog.update()` to head: the scanned snapshot is exactly what
   * vanilla Spark+Delta reads (PreparedDeltaFileIndex extends
   * TahoeFileIndexWithSnapshotDescriptor over it), so matching it keeps Comet a faithful drop-in
   * accelerator -- including for time travel, where it's pinned to the requested version.
   */
  private def preparedSnapshotFiles(
      location: Any,
      partitionFilters: Seq[org.apache.spark.sql.catalyst.expressions.Expression]): Option[AnyRef] = {
    if (location == null) return None
    try {
      // Read the files from the snapshot the scan was PREPARED against
      // (`preparedScan.scannedSnapshot`). This is the same snapshot vanilla
      // Spark+Delta reads from -- `PreparedDeltaFileIndex` extends
      // `TahoeFileIndexWithSnapshotDescriptor(... preparedScan.scannedSnapshot)`
      // -- so re-querying it keeps Comet a faithful drop-in accelerator for
      // both normal reads and time travel (versionAsOf/timestampAsOf pin the
      // scannedSnapshot to the requested version).
      //
      // We deliberately do NOT `deltaLog.update()` to head here. An earlier
      // revision did, to pick up deletion-vector descriptors written after a
      // cached FileIndex was built -- but refreshing to head makes Comet read a
      // DIFFERENT (newer) snapshot than vanilla, which (a) silently diverges
      // from vanilla on the consecutive-DELETE / DeltaLog-cache-staleness case
      // and (b) returned the LATEST version's data for a time-travel query
      // (DeltaTimeTravelSuite: versionAsOf=0 yielded head's rows). Reading
      // `scannedSnapshot` matches vanilla in every case. Re-querying via
      // `filesForScan` (rather than the cached `preparedScan.files`) still picks
      // up the freshest DV descriptors carried by that snapshot.
      val snapshot: AnyRef = findAccessor(location, Seq("preparedScan"))
        .flatMap(ps => findAccessor(ps, Seq("scannedSnapshot")))
        .orNull
      if (snapshot == null) return None
      val filesForScanMethod =
        lookupMethod(snapshot.getClass, "filesForScan", Seq(null, classOf[Boolean]))
      if (filesForScanMethod == null) return None
      // Pass through the scan's partition filters so the snapshot does its own
      // file-skipping. Without this, the refreshed list is the FULL table --
      // bypassing the partition pruning Delta already applied at planning
      // time. Breaks tests like StatsCollectionSuite "gather stats" which
      // assert `recordsScanned(df.where("id = 1")) == 1`. Cast the Scala Seq
      // through AnyRef so Java reflection accepts it as the first formal arg.
      val filtersSeq: Object = if (partitionFilters.isEmpty) {
        scala.collection.immutable.Nil
      } else {
        partitionFilters.toList
      }
      val keepNumRecords: Object = java.lang.Boolean.FALSE
      val deltaScan = filesForScanMethod.invoke(snapshot, filtersSeq, keepNumRecords)
      if (deltaScan == null) return None
      findAccessor(deltaScan, Seq("files"))
    } catch {
      case scala.util.control.NonFatal(_) => None
    }
  }

  private def callMatchingFiles(location: Any): Option[AnyRef] = {
    if (location == null) return None
    try {
      // Method.matchingFiles has two parameters of type `Seq[Expression]`; we
      // can pass Nil for both. We find the method by name + arity to keep the
      // lookup tolerant of Scala's generic-erasure bridging.
      val candidate = Option(lookupMethod(location.getClass, "matchingFiles", Seq(null, null)))
      candidate.flatMap { m =>
        val nil = scala.collection.immutable.Nil
        try Option(m.invoke(location, nil, nil))
        catch {
          case scala.util.control.NonFatal(_) => None
        }
      }
    } catch {
      case scala.util.control.NonFatal(_) => None
    }
  }

  private def findAccessor(obj: Any, names: Seq[String]): Option[AnyRef] = {
    if (obj == null) return None
    val cls = obj.getClass
    names.foreach { n =>
      val m = lookupNoArgMethod(cls, n)
      if (m != null) {
        try return Option(m.invoke(obj))
        catch { case scala.util.control.NonFatal(_) => return None }
      }
    }
    None
  }

  // Cache no-arg java.lang.reflect.Method handles by (class, name). Hot path for plan
  // walks: every CometScanRule call into Delta does many name-based lookups per file.
  // `MISSING` sentinel caches negative lookups so we don't re-scan getMethods on misses.
  private val MISSING: java.lang.reflect.Method = classOf[Object].getMethod("toString")
  private val noArgMethodCache =
    new java.util.concurrent.ConcurrentHashMap[(Class[_], String), java.lang.reflect.Method]()

  private def lookupNoArgMethod(cls: Class[_], name: String): java.lang.reflect.Method = {
    val key = (cls, name)
    val cached = noArgMethodCache.get(key)
    if (cached ne null) return if (cached eq MISSING) null else cached
    val resolved =
      try {
        val m = cls.getMethod(name)
        if (m.getParameterCount == 0) m else null
      } catch {
        case _: NoSuchMethodException => null
      }
    noArgMethodCache.putIfAbsent(key, if (resolved == null) MISSING else resolved)
    resolved
  }

  private val argMethodCache =
    new java.util.concurrent.ConcurrentHashMap[
      (Class[_], String, Seq[Class[_]]),
      java.lang.reflect.Method]()

  /**
   * Companion to `lookupNoArgMethod` for arg-typed lookups it can't express. Caches a Method
   * handle by (class, name, parameter types), matching name + parameter count + each supplied
   * param type via `getMethods` (public methods only); a `null` in `argTypes` leaves that
   * position unconstrained. Negative lookups cache the MISSING sentinel so repeat misses don't
   * re-scan the method table. Returns null on no match.
   */
  private def lookupMethod(
      cls: Class[_],
      name: String,
      argTypes: Seq[Class[_]]): java.lang.reflect.Method = {
    val key = (cls, name, argTypes)
    val cached = argMethodCache.get(key)
    if (cached ne null) return if (cached eq MISSING) null else cached
    val resolved = cls.getMethods
      .find { m =>
        m.getName == name &&
        m.getParameterCount == argTypes.length &&
        argTypes.indices.forall(i => argTypes(i) == null || m.getParameterTypes()(i) == argTypes(i))
      }
      .orNull
    argMethodCache.putIfAbsent(key, if (resolved == null) MISSING else resolved)
    resolved
  }

  private def stringMember(obj: Any, name: String): Option[String] =
    findAccessor(obj, Seq(name)).flatMap {
      case s: String => Some(s)
      case null => None
      case _ => None
    }

  private def longMember(obj: Any, name: String): Option[Long] =
    findAccessor(obj, Seq(name)).flatMap {
      case l: java.lang.Long => Some(l)
      case i: java.lang.Integer => Some(i.toLong)
      case _ => None
    }

  /**
   * Read a Scala `Option[Long]` (or `Option[java.lang.Long]`) field by name. Returns `None` for
   * both `None` and a field that contains `Some(null)`. Used for optional Delta fields like
   * `AddFile.baseRowId` that only exist when rowTracking is enabled on the table.
   */
  private def optionLongMember(obj: Any, name: String): Option[Long] =
    findAccessor(obj, Seq(name)).flatMap {
      case None => None
      case Some(l: java.lang.Long) => Some(l)
      case Some(i: java.lang.Integer) => Some(i.toLong)
      case Some(l: Long) => Some(l)
      case Some(null) | null => None
      case l: java.lang.Long => Some(l) // defensive: caller extracted value already
      case _ => None
    }

  def castPartitionString(
      str: Option[String],
      dt: org.apache.spark.sql.types.DataType,
      sessionZoneId: java.time.ZoneId = java.time.ZoneOffset.UTC): Any = {
    import org.apache.spark.sql.catalyst.util.DateTimeUtils
    import org.apache.spark.sql.types._
    import org.apache.spark.unsafe.types.UTF8String
    str match {
      case None | Some(null) => null
      case Some(s) =>
        try {
          dt match {
            case StringType => UTF8String.fromString(s)
            case IntegerType => s.toInt
            case LongType => s.toLong
            case ShortType => s.toShort
            case ByteType => s.toByte
            case FloatType => s.toFloat
            case DoubleType => s.toDouble
            case BooleanType => s.toBoolean
            case DateType =>
              DateTimeUtils
                .stringToDate(UTF8String.fromString(s))
                .getOrElse(null)
            case _: TimestampType =>
              // Delta serializes TIMESTAMP partition values in the session TZ at write time, so
              // parse them in the session TZ at read time to round-trip correctly (defaults to
              // UTC when the caller hasn't plumbed the session TZ through).
              DateTimeUtils
                .stringToTimestamp(UTF8String.fromString(s), sessionZoneId)
                .getOrElse(null)
            case _: TimestampNTZType =>
              DateTimeUtils
                .stringToTimestampWithoutTimeZone(UTF8String.fromString(s))
                .getOrElse(null)
            case d: DecimalType =>
              val dec =
                org.apache.spark.sql.types.Decimal(new java.math.BigDecimal(s))
              dec.changePrecision(d.precision, d.scale)
              dec
            case _ => UTF8String.fromString(s)
          }
        } catch {
          case _: NumberFormatException | _: IllegalArgumentException =>
            null
        }
    }
  }
}
