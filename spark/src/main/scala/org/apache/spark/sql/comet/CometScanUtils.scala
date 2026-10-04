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

package org.apache.spark.sql.comet

import java.util.concurrent.{Callable, ExecutorCompletionService}

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.parquet.HadoopReadOptions
import org.apache.parquet.column.statistics.Statistics
import org.apache.parquet.format.converter.ParquetMetadataConverter.{NO_FILTER, SKIP_ROW_GROUPS}
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.{ColumnChunkMetaData, ParquetMetadata}
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.schema.{PrimitiveType, Type}
import org.apache.parquet.schema.LogicalTypeAnnotation.{DateLogicalTypeAnnotation, TimestampLogicalTypeAnnotation, TimeUnit}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.spark.sql.catalyst.expressions.{DynamicPruningExpression, Expression, Literal}
import org.apache.spark.sql.catalyst.util.RebaseDateTime
import org.apache.spark.sql.execution.{InSubqueryExec, SubqueryAdaptiveBroadcastExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, DateType, StructType, TimestampNTZType, TimestampType}
import org.apache.spark.util.ThreadUtils

import org.apache.comet.CometConf
import org.apache.comet.serde.SupportLevel

object CometScanUtils {

  /**
   * One Parquet file of the scan's listing, as the datetime rebase check sees it: its cache
   * identity and the status its footer is read with. The length and modification time must be the
   * listing's.
   */
  case class ParquetFileInfo(path: Path, length: Long, modificationTime: Long)

  private val LegacyDateTimeKey = "org.apache.spark.legacyDateTime"
  private val LegacyInt96Key = "org.apache.spark.legacyINT96"

  /** How a top-level Parquet column stores datetime values. */
  private sealed trait DatetimeEncoding
  private case object DateDays extends DatetimeEncoding
  private case object TimestampMicros extends DatetimeEncoding
  private case object TimestampMillis extends DatetimeEncoding
  private case object TimestampInt96 extends DatetimeEncoding

  /**
   * A top-level datetime column of one file. `min` is its smallest value in all row groups, in
   * the unit of the encoding. It is None when a row group has no usable statistics.
   */
  private case class DatetimeColumn(encoding: DatetimeEncoding, min: Option[Long])

  /**
   * Datetime-relevant footer metadata of one Parquet file, independent of any read mode.
   * `columns` is None until a footer read with row groups, and empty for a file that Spark never
   * rebases.
   */
  private case class DatetimeFooterFacts(
      sparkVersion: Option[String],
      legacyKeys: Set[String],
      columns: Option[Map[String, DatetimeColumn]])

  private type FooterCacheKey = (String, Long, Long)

  // The cache bound most recently applied by applyFooterFactsCacheMaxSize.
  @volatile private var footerFactsCacheMaxSize: Int =
    CometConf.COMET_SCAN_PARQUET_CHECK_DATETIME_REBASE_MAX_CACHED_FILES.defaultValue.get

  // Bounded LRU cache of per-file footer facts, keyed by (path, length, modificationTime) so a
  // rewritten file is re-read. The cached facts are independent of the read modes and requested
  // types, so one entry answers the rebase question for any query. This keeps AQE re-planning
  // and repeated queries over the same files from re-reading footers on the driver.
  private val footerFactsCache =
    java.util.Collections.synchronizedMap(
      new java.util.LinkedHashMap[FooterCacheKey, DatetimeFooterFacts](64, 0.75f, true) {
        override def removeEldestEntry(
            eldest: java.util.Map.Entry[FooterCacheKey, DatetimeFooterFacts]): Boolean =
          size() > footerFactsCacheMaxSize
      })

  /**
   * Applies the configured bound to the cache. The cache outlives any one session, so the bound
   * in effect is the one from the session that last ran the check. A lower bound evicts the least
   * recently used entries at once, since `removeEldestEntry` only evicts one entry per insert.
   */
  private def applyFooterFactsCacheMaxSize(maxSize: Int): Unit = {
    if (maxSize != footerFactsCacheMaxSize) {
      footerFactsCache.synchronized {
        footerFactsCacheMaxSize = maxSize
        // The map is in access order, so iteration starts at the least recently used entry.
        val keys = footerFactsCache.keySet.iterator
        while (footerFactsCache.size() > maxSize && keys.hasNext) {
          keys.next()
          keys.remove()
        }
      }
    }
  }

  // Spark reads a value at or after these cutoffs as stored, in every read mode.
  private val DateCutoffDays: Long = RebaseDateTime.lastSwitchJulianDay.toLong
  private val TimestampCutoffMicros: Long = RebaseDateTime.lastSwitchJulianTs
  // Spark compares millis after it scales them to micros, so round the cutoff up.
  private val TimestampCutoffMillis: Long = Math.floorDiv(TimestampCutoffMicros + 999L, 1000L)

  /** One of the two rebase specs of Spark, with the read mode of the scan. */
  private case class RebaseSpec(name: String, mode: String, minVersion: String, legacyKey: String)

  /** A rebase spec that applies to a column, and the values that Spark rebases under it. */
  private case class RebaseCheck(spec: RebaseSpec, values: String)

  /**
   * A requested field with datetime values. Only a top-level DATE or TIMESTAMP uses statistics.
   */
  private case class RequestedColumn(
      name: String,
      dataType: DataType,
      usesStatistics: Boolean,
      checks: Seq[RebaseCheck])

  private sealed trait Verdict
  private case object NoRebase extends Verdict
  private case object NeedsStatistics extends Verdict
  private case class Fallback(reason: String) extends Verdict

  // TIMESTAMP_NTZ values are never rebased by Spark, on write or on read (Spark's
  // ParquetVectorUpdaterFactory: "TIMESTAMP_NTZ is a new data type and has no legacy files
  // that need to do rebase"). The rebase question arises for a requested NTZ column only
  // when the underlying Parquet column is a TIMESTAMP (LTZ or INT96) that may carry
  // legacy-calendar values, and Comet permits that read only when
  // COMET_ALLOW_TIMESTAMP_LTZ_AS_NTZ is true (Spark 4.x, SPARK-47447).
  private def readsTimestamps(dataType: DataType): Boolean =
    SupportLevel.containsType(dataType, classOf[TimestampType]) ||
      (CometConf.COMET_ALLOW_TIMESTAMP_LTZ_AS_NTZ &&
        SupportLevel.containsType(dataType, classOf[TimestampNTZType]))

  /** Whether a scan with this required schema reads values that Spark can rebase. */
  def readsRebasableDatetimes(requiredSchema: StructType): Boolean =
    requiredSchema.fields.exists(f =>
      SupportLevel.containsType(f.dataType, classOf[DateType]) || readsTimestamps(f.dataType))

  /**
   * The fallback reason when Spark rebases `check` values of `column` in a file, or None when
   * Spark reads them as stored.
   */
  private def rebaseReason(
      facts: DatetimeFooterFacts,
      column: String,
      check: RebaseCheck): Option[String] = {
    val spec = check.spec
    val statistics =
      s", and row-group statistics do not rule out ${check.values} in column `$column`"
    def legacyFile(detail: String): String =
      s"Native Parquet scan does not rebase datetime values in a Parquet file $detail$statistics"
    // Mirrors Spark's DataSourceUtils.datetimeRebaseSpec/int96RebaseSpec: when the file has no
    // Spark version key the configured mode decides (EXCEPTION must also fall back, because
    // Spark would raise on ancient values while Comet would not); when a version is present the
    // mode is ignored and only the version and the legacy markers matter. The `v < minVersion`
    // comparison is deliberately the same lexicographic string comparison Spark uses.
    facts.sparkVersion match {
      case None if spec.mode != "CORRECTED" =>
        Some(
          s"Native Parquet scan does not support the ${spec.mode} ${spec.name} read mode for a " +
            s"Parquet file without org.apache.spark.version$statistics. Set " +
            s"${SQLConf.PARQUET_REBASE_MODE_IN_READ.key} and " +
            s"${SQLConf.PARQUET_INT96_REBASE_MODE_IN_READ.key} to CORRECTED if the files use " +
            "the proleptic Gregorian calendar")
      case Some(v) if v < spec.minVersion => Some(legacyFile(s"written by Spark $v"))
      case Some(_) if facts.legacyKeys.contains(spec.legacyKey) =>
        Some(legacyFile(s"with ${spec.legacyKey} metadata"))
      case _ => None
    }
  }

  /** How a top-level column stores datetime values, or None for another type. */
  private def encodingOf(field: PrimitiveType): Option[DatetimeEncoding] =
    (field.getPrimitiveTypeName, field.getLogicalTypeAnnotation) match {
      case (PrimitiveTypeName.INT32, _: DateLogicalTypeAnnotation) => Some(DateDays)
      case (PrimitiveTypeName.INT64, t: TimestampLogicalTypeAnnotation)
          if t.getUnit == TimeUnit.MICROS =>
        Some(TimestampMicros)
      case (PrimitiveTypeName.INT64, t: TimestampLogicalTypeAnnotation)
          if t.getUnit == TimeUnit.MILLIS =>
        Some(TimestampMillis)
      case (PrimitiveTypeName.INT96, _) => Some(TimestampInt96)
      case _ => None
    }

  /** The smallest value of a column chunk, Long.MaxValue without values, or None if unknown. */
  private def chunkMin(chunk: ColumnChunkMetaData): Option[Long] =
    if (chunk.getValueCount == 0) {
      Some(Long.MaxValue)
    } else {
      statisticsMin(chunk.getStatistics, chunk.getValueCount)
    }

  private def statisticsMin(stats: Statistics[_], valueCount: Long): Option[Long] =
    if (stats == null) {
      None
    } else if (stats.hasNonNullValue) {
      val min: Any = stats.genericGetMin
      min match {
        case v: java.lang.Integer => Some(v.longValue)
        case v: java.lang.Long => Some(v.longValue)
        case _ => None
      }
    } else if (stats.isNumNullsSet && stats.getNumNulls == valueCount) {
      // Every value of the chunk is null.
      Some(Long.MaxValue)
    } else {
      None
    }

  /** The top-level datetime columns of a file, with their smallest value in all row groups. */
  private def datetimeColumns(footer: ParquetMetadata): Map[String, DatetimeColumn] = {
    val encodings = footer.getFileMetaData.getSchema.getFields.asScala.flatMap {
      case field: PrimitiveType if !field.isRepetition(Type.Repetition.REPEATED) =>
        encodingOf(field).map(field.getName -> _)
      case _ => None
    }
    val blocks = footer.getBlocks.asScala
    val chunks = blocks
      .flatMap(_.getColumns.asScala)
      .filter(_.getPath.size == 1)
      .groupBy(_.getPath.toArray.head)
    encodings.map { case (name, encoding) =>
      val columnChunks = chunks.get(name).map(_.toList).getOrElse(Nil)
      // INT96 has no reliable statistics. A valid file has one chunk per row group.
      val min =
        if (encoding == TimestampInt96 || columnChunks.size != blocks.size) {
          None
        } else {
          columnChunks.foldLeft(Option(Long.MaxValue)) { (acc, chunk) =>
            acc.flatMap(a => chunkMin(chunk).map(math.min(a, _)))
          }
        }
      name -> DatetimeColumn(encoding, min)
    }.toMap
  }

  /** Reads the facts of one file. `withStatistics` also reads row groups for column minimums. */
  private def readFacts(
      file: ParquetFileInfo,
      conf: Configuration,
      withStatistics: Boolean): DatetimeFooterFacts = {
    // Open the file with the status the listing already returned: HadoopInputFile.fromPath
    // would ask the file system for it again, a HEAD request per file on object stores. The
    // footer is located from the end of the file, so the length must be the listed one.
    val status = new FileStatus(file.length, false, 0, 0, file.modificationTime, file.path)
    val inputFile = HadoopInputFile.fromStatus(status, conf)
    val readOptions = HadoopReadOptions
      .builder(conf, file.path)
      .withMetadataFilter(if (withStatistics) NO_FILTER else SKIP_ROW_GROUPS)
      .build()
    val reader = ParquetFileReader.open(inputFile, readOptions)
    try {
      val footer = reader.getFooter
      val metadata = footer.getFileMetaData.getKeyValueMetaData
      val sparkVersion = Option(metadata.get("org.apache.spark.version"))
      val legacyKeys = Set(LegacyDateTimeKey, LegacyInt96Key).filter(metadata.containsKey)
      // Spark never rebases a file from Spark 3.1.0 or later without legacy keys.
      val rebasable = sparkVersion.forall(_ < "3.1.0") || legacyKeys.nonEmpty
      val columns =
        if (!withStatistics) None
        else if (rebasable) Some(datetimeColumns(footer))
        else Some(Map.empty[String, DatetimeColumn])
      DatetimeFooterFacts(sparkVersion, legacyKeys, columns)
    } finally {
      reader.close()
    }
  }

  /**
   * The reason to read `files` with Spark, or None when Spark reads their datetime values as
   * stored. With `useStatistics`, row-group minimums can prove that Spark reads a column as
   * stored.
   */
  def datetimeRebaseFallbackReason(
      files: Seq[ParquetFileInfo],
      conf: Configuration,
      datetimeMode: String,
      int96Mode: String,
      requiredSchema: StructType,
      useStatistics: Boolean): Option[String] = {
    applyFooterFactsCacheMaxSize(
      CometConf.COMET_SCAN_PARQUET_CHECK_DATETIME_REBASE_MAX_CACHED_FILES.get())

    val datetimeSpec = RebaseSpec("datetime", datetimeMode, "3.0.0", LegacyDateTimeKey)
    val int96Spec = RebaseSpec("INT96", int96Mode, "3.1.0", LegacyInt96Key)
    val dateCheck = RebaseCheck(datetimeSpec, "dates before 1582-10-15")
    val timestampCheck = RebaseCheck(datetimeSpec, "timestamps before 1900-01-01T00:00:00Z")
    val int96Check = RebaseCheck(int96Spec, "timestamps before 1900-01-01T00:00:00Z")

    val requested = requiredSchema.fields.toSeq.flatMap { field =>
      val dates =
        if (SupportLevel.containsType(field.dataType, classOf[DateType])) Seq(dateCheck) else Nil
      val timestamps =
        if (readsTimestamps(field.dataType)) Seq(timestampCheck, int96Check) else Nil
      val usesStatistics =
        useStatistics && (field.dataType == DateType || field.dataType == TimestampType)
      Some(RequestedColumn(field.name, field.dataType, usesStatistics, dates ++ timestamps))
        .filter(_.checks.nonEmpty)
    }
    if (requested.isEmpty) {
      return None
    }

    // The checks left for a top-level column once its encoding and minimum in the file are known.
    def checksFor(column: RequestedColumn, stored: Option[DatetimeColumn]): Seq[RebaseCheck] = {
      def unlessFrom(min: Option[Long], cutoff: Long, check: RebaseCheck): Seq[RebaseCheck] =
        if (min.exists(_ >= cutoff)) Nil else Seq(check)
      (column.dataType, stored) match {
        case (DateType, Some(DatetimeColumn(DateDays, min))) =>
          unlessFrom(min, DateCutoffDays, dateCheck)
        case (TimestampType, Some(DatetimeColumn(TimestampMicros, min))) =>
          unlessFrom(min, TimestampCutoffMicros, timestampCheck)
        case (TimestampType, Some(DatetimeColumn(TimestampMillis, min))) =>
          unlessFrom(min, TimestampCutoffMillis, timestampCheck)
        case (TimestampType, Some(DatetimeColumn(TimestampInt96, _))) => Seq(int96Check)
        case _ => column.checks
      }
    }

    def verdict(facts: DatetimeFooterFacts): Verdict = {
      val outcomes = requested.map { column =>
        val checks = facts.columns match {
          case Some(columns) if column.usesStatistics =>
            checksFor(column, columns.get(column.name))
          case _ => column.checks
        }
        val reason = checks.flatMap(check => rebaseReason(facts, column.name, check)).headOption
        (reason, column.usesStatistics && facts.columns.isEmpty)
      }
      outcomes
        .collectFirst { case (Some(reason), false) => Fallback(reason) }
        .getOrElse(if (outcomes.exists(_._1.isDefined)) NeedsStatistics else NoRebase)
    }

    // Answer from the cache where possible. Only misses and files that need statistics pay a read.
    var cachedReason: Option[String] = None
    val pending = new ListBuffer[(FooterCacheKey, ParquetFileInfo, Option[DatetimeFooterFacts])]
    // Spark never opens a zero-length file, and a footer read fails on one.
    files.iterator.filter(_.length > 0).foreach { file =>
      val key = (file.path.toString, file.length, file.modificationTime)
      val cached = Option(footerFactsCache.get(key))
      cached.map(verdict) match {
        case Some(NoRebase) =>
        case Some(Fallback(reason)) => cachedReason = cachedReason.orElse(Some(reason))
        case _ => pending += ((key, file, cached))
      }
    }
    if (cachedReason.isDefined || pending.isEmpty) {
      return cachedReason
    }

    // A file without a Spark version needs statistics under this datetime mode, so read them now.
    val statisticsFirst = datetimeMode != "CORRECTED" && requested.exists(_.usesStatistics)

    // The facts that decide a file, with the verdict. A read with statistics always decides.
    def resolve(
        file: ParquetFileInfo,
        cached: Option[DatetimeFooterFacts]): (DatetimeFooterFacts, Verdict) = {
      val facts = cached.getOrElse(readFacts(file, conf, statisticsFirst))
      verdict(facts) match {
        case NeedsStatistics =>
          val withStatistics = readFacts(file, conf, withStatistics = true)
          (withStatistics, verdict(withStatistics))
        case decided => (facts, decided)
      }
    }

    val parallelism = 8
    val pool = ThreadUtils.newDaemonFixedThreadPool(parallelism, "checkingParquetDatetimeRebase")
    val completion =
      new ExecutorCompletionService[(FooterCacheKey, DatetimeFooterFacts, Verdict)](pool)
    val remaining = pending.iterator
    var inFlight = 0

    // Spark's Parquet footer reader uses ThreadUtils.parmap, which submits every input eagerly.
    // Keep only `parallelism` reads in flight so finding one legacy footer stops further reads.

    def submitNext(): Unit = {
      val (key, file, cached) = remaining.next()
      completion.submit(new Callable[(FooterCacheKey, DatetimeFooterFacts, Verdict)] {
        override def call(): (FooterCacheKey, DatetimeFooterFacts, Verdict) = {
          val (facts, decided) = resolve(file, cached)
          (key, facts, decided)
        }
      })
      inFlight += 1
    }

    try {
      while (inFlight < parallelism && remaining.hasNext) {
        submitNext()
      }
      var reason: Option[String] = None
      while (reason.isEmpty && inFlight > 0) {
        val (key, facts, decided) = completion.take().get()
        footerFactsCache.put(key, facts)
        decided match {
          case NoRebase =>
          case Fallback(r) => reason = Some(r)
          case NeedsStatistics =>
            throw new IllegalStateException(s"Row-group statistics did not decide ${key._1}")
        }
        inFlight -= 1
        if (reason.isEmpty && remaining.hasNext) {
          submitNext()
        }
      }
      reason
    } finally {
      val _ = pool.shutdownNow()
    }
  }

  /**
   * Filters unused DynamicPruningExpression expressions - one which has been replaced with
   * DynamicPruningExpression(Literal.TrueLiteral) during Physical Planning
   */
  def filterUnusedDynamicPruningExpressions(predicates: Seq[Expression]): Seq[Expression] = {
    // Strip DPP expressions for canonicalization. Matches Spark's
    // FileSourceScanExec.filterUnusedDynamicPruningExpressions (TrueLiteral).
    // Also strips unconverted SAB wrappers because AQE stageCache canonicalizes
    // before our queryStageOptimizerRule converts them, so they would prevent
    // exchange reuse between otherwise-identical scans.
    predicates.filterNot {
      case DynamicPruningExpression(Literal.TrueLiteral) => true
      case DynamicPruningExpression(
            InSubqueryExec(_, _: CometSubqueryAdaptiveBroadcastExec, _, _, _, _)) =>
        true
      case DynamicPruningExpression(
            InSubqueryExec(_, _: SubqueryAdaptiveBroadcastExec, _, _, _, _)) =>
        true
      case _ => false
    }
  }
}
