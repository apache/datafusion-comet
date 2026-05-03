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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.{Scan => V2Scan}
import org.apache.spark.sql.execution.datasources.{FileFormat, HadoopFsRelation}
import org.apache.spark.sql.types.StructType

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
  def extractMetadataConfiguration(relation: HadoopFsRelation): Option[Map[String, String]] = {
    try {
      val location: Any = relation.location
      // TahoeFileIndex and variants expose `metadata: Metadata`. Some share a direct field;
      // CdcAddFileIndex and similar re-expose via `snapshot.metadata`. Try both.
      val metadataObj = findAccessor(location, Seq("metadata")).orElse {
        findAccessor(location, Seq("snapshot")).flatMap(findAccessor(_, Seq("metadata")))
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
      val metadataObj = findAccessor(location, Seq("metadata")).orElse {
        findAccessor(location, Seq("snapshot")).flatMap(findAccessor(_, Seq("metadata")))
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
      val m =
        obj.getClass.getMethods.find(x => x.getName == methodName && x.getParameterCount == 0)
      m.flatMap(mm => Option(mm.invoke(obj)))
    } catch {
      case scala.util.control.NonFatal(_) => None
    }
  }

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
       * Delta row-tracking fields. `baseRowId` is the first logical row id covered by this file;
       * `defaultRowCommitVersion` is the commit that last wrote it. Both are `None` for tables
       * that don't have the rowTracking table feature enabled (or for pre-backfill files on a
       * table where row tracking was just enabled).
       */
      baseRowId: Option[Long],
      defaultRowCommitVersion: Option[Long])

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
  def extractBatchAddFiles(location: Any): Option[Seq[ExtractedAddFile]] = {
    try {
      // PreparedDeltaFileIndex carries the pre-skipped scan result -- using
      // `matchingFiles(Nil, Nil)` on it falls into Delta's "Reselecting files
      // to query" branch (different filter set) and returns the FULL snapshot
      // of files (no stats-based skipping), which breaks tests like
      // StatsCollectionSuite "gather stats" that expect file-level pruning.
      // Read `preparedScan.files` directly to honour the prepared skipping.
      val preparedFiles: Option[AnyRef] =
        if (location.getClass.getName.contains("PreparedDeltaFileIndex")) {
          findAccessor(location, Seq("preparedScan"))
            .flatMap(ps => findAccessor(ps, Seq("files")))
        } else None
      // Prefer matchingFiles(Seq.empty, Seq.empty) -- it returns CDC-augmented
      // AddFiles on CDC indexes and the plain list on TahoeBatchFileIndex.
      // Fall back to the raw `addFiles`/`filesList` accessors for indexes that
      // don't expose a no-arg-safe matchingFiles.
      val addFilesOpt = preparedFiles
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
          out += ExtractedAddFile(
            path,
            size,
            pv,
            stats,
            hasDeletionVector = dv != null,
            baseRowId = baseRowId,
            defaultRowCommitVersion = defaultRowCommitVersion)
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
  private def callMatchingFiles(location: Any): Option[AnyRef] = {
    if (location == null) return None
    try {
      // Method.matchingFiles has two parameters of type `Seq[Expression]`; we
      // can pass Nil for both. We find the method by name + arity to keep the
      // lookup tolerant of Scala's generic-erasure bridging.
      val candidate = location.getClass.getMethods.find { m =>
        m.getName == "matchingFiles" && m.getParameterCount == 2
      }
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
      try {
        val m = cls.getMethod(n)
        return Option(m.invoke(obj))
      } catch {
        case _: NoSuchMethodException => // try next
      }
    }
    None
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
