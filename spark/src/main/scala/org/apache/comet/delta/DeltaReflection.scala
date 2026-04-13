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
      roots.headOption.map(_.toUri.toString)
    } catch {
      case e: Exception =>
        logWarning(s"Failed to extract Delta table root path: ${e.getMessage}")
        None
    }
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
  def castPartitionString(str: Option[String], dt: org.apache.spark.sql.types.DataType): Any = {
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
              DateTimeUtils
                .stringToTimestamp(UTF8String.fromString(s), java.time.ZoneOffset.UTC)
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
