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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.HadoopReadOptions
import org.apache.parquet.format.converter.ParquetMetadataConverter.SKIP_ROW_GROUPS
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.spark.sql.catalyst.expressions.{DynamicPruningExpression, Expression, Literal}
import org.apache.spark.sql.execution.{InSubqueryExec, SubqueryAdaptiveBroadcastExec}
import org.apache.spark.util.ThreadUtils

object CometScanUtils {

  /** Identity of one Parquet file for the datetime rebase check. */
  case class ParquetFileInfo(path: Path, length: Long, modificationTime: Long)

  /** Datetime-relevant footer metadata of one Parquet file, independent of any read mode. */
  private case class DatetimeFooterFacts(
      sparkVersion: Option[String],
      hasLegacyDateTime: Boolean,
      hasLegacyInt96: Boolean)

  private type FooterCacheKey = (String, Long, Long)

  private val footerFactsCacheMaxSize = 32 * 1024

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

  def requiresDatetimeRebase(
      files: Seq[ParquetFileInfo],
      conf: Configuration,
      datetimeMode: String,
      int96Mode: String,
      hasDate: Boolean,
      hasTimestamp: Boolean): Boolean = {

    // Mirrors Spark's DataSourceUtils.datetimeRebaseSpec/int96RebaseSpec: when the file has no
    // Spark version key the configured mode decides (EXCEPTION must also fall back, because
    // Spark would raise on ancient values while Comet would not); when a version is present the
    // mode is ignored and only the version and the legacy markers matter. The `v < minVersion`
    // comparison is deliberately the same lexicographic string comparison Spark uses.
    def needsRebase(facts: DatetimeFooterFacts): Boolean = {
      def modeNeedsRebase(mode: String, minVersion: String, hasLegacyKey: Boolean): Boolean =
        facts.sparkVersion.fold(mode != "CORRECTED")(v => v < minVersion || hasLegacyKey)

      (hasDate &&
        modeNeedsRebase(datetimeMode, "3.0.0", facts.hasLegacyDateTime)) ||
      (hasTimestamp &&
        (modeNeedsRebase(datetimeMode, "3.0.0", facts.hasLegacyDateTime) ||
          modeNeedsRebase(int96Mode, "3.1.0", facts.hasLegacyInt96)))
    }

    def readFacts(path: Path): DatetimeFooterFacts = {
      val inputFile = HadoopInputFile.fromPath(path, conf)
      val readOptions = HadoopReadOptions
        .builder(conf, path)
        .withMetadataFilter(SKIP_ROW_GROUPS)
        .build()
      val reader = ParquetFileReader.open(inputFile, readOptions)
      try {
        val metadata = reader.getFooter.getFileMetaData.getKeyValueMetaData
        DatetimeFooterFacts(
          Option(metadata.get("org.apache.spark.version")),
          metadata.containsKey("org.apache.spark.legacyDateTime"),
          metadata.containsKey("org.apache.spark.legacyINT96"))
      } finally {
        reader.close()
      }
    }

    // Answer from the cache where possible; only cache misses pay a footer read.
    var cachedNeedsRebase = false
    val misses = new ListBuffer[(FooterCacheKey, Path)]
    files.foreach { file =>
      val key = (file.path.toString, file.length, file.modificationTime)
      val cached = footerFactsCache.get(key)
      if (cached != null) {
        cachedNeedsRebase = cachedNeedsRebase || needsRebase(cached)
      } else {
        misses += ((key, file.path))
      }
    }
    if (cachedNeedsRebase) {
      return true
    }
    if (misses.isEmpty) {
      return false
    }

    val parallelism = 8
    val pool = ThreadUtils.newDaemonFixedThreadPool(parallelism, "checkingParquetDatetimeRebase")
    val completion = new ExecutorCompletionService[(FooterCacheKey, DatetimeFooterFacts)](pool)
    val remaining = misses.iterator
    var inFlight = 0

    // Spark's Parquet footer reader uses ThreadUtils.parmap, which submits every input eagerly.
    // Keep only `parallelism` reads in flight so finding one legacy footer stops further reads.

    def submitNext(): Unit = {
      val (key, path) = remaining.next()
      completion.submit(new Callable[(FooterCacheKey, DatetimeFooterFacts)] {
        override def call(): (FooterCacheKey, DatetimeFooterFacts) = (key, readFacts(path))
      })
      inFlight += 1
    }

    try {
      while (inFlight < parallelism && remaining.hasNext) {
        submitNext()
      }
      var requiresRebase = false
      while (!requiresRebase && inFlight > 0) {
        val (key, facts) = completion.take().get()
        footerFactsCache.put(key, facts)
        requiresRebase = needsRebase(facts)
        inFlight -= 1
        if (!requiresRebase && remaining.hasNext) {
          submitNext()
        }
      }
      requiresRebase
    } finally {
      pool.shutdownNow()
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
