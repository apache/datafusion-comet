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

  def requiresDatetimeRebase(
      paths: Seq[Path],
      conf: Configuration,
      datetimeMode: String,
      int96Mode: String,
      hasDate: Boolean,
      hasTimestamp: Boolean): Boolean = {
    val parallelism = 8
    val pool = ThreadUtils.newDaemonFixedThreadPool(parallelism, "checkingParquetDatetimeRebase")
    val completion = new ExecutorCompletionService[Boolean](pool)
    val remaining = paths.iterator
    var inFlight = 0

    // Spark's Parquet footer reader uses ThreadUtils.parmap, which submits every input eagerly.
    // Keep only `parallelism` reads in flight so finding one legacy footer stops further reads.

    def submitNext(): Unit = {
      val path = remaining.next()
      completion.submit(new Callable[Boolean] {
        override def call(): Boolean = {
          val inputFile = HadoopInputFile.fromPath(path, conf)
          val readOptions = HadoopReadOptions
            .builder(conf, path)
            .withMetadataFilter(SKIP_ROW_GROUPS)
            .build()
          val reader = ParquetFileReader.open(inputFile, readOptions)
          try {
            val metadata = reader.getFooter.getFileMetaData.getKeyValueMetaData
            val version = Option(metadata.get("org.apache.spark.version"))
            val cometCorrected =
              Option(metadata.get("org.apache.comet.datetimeRebaseMode")).contains("CORRECTED")
            def needsRebase(mode: String, minVersion: String, legacyKey: String): Boolean =
              version.fold(!cometCorrected && mode != "CORRECTED")(v =>
                v < minVersion || metadata.containsKey(legacyKey))

            (hasDate &&
              needsRebase(datetimeMode, "3.0.0", "org.apache.spark.legacyDateTime")) ||
            (hasTimestamp &&
              (needsRebase(datetimeMode, "3.0.0", "org.apache.spark.legacyDateTime") ||
                needsRebase(int96Mode, "3.1.0", "org.apache.spark.legacyINT96")))
          } finally {
            reader.close()
          }
        }
      })
      inFlight += 1
    }

    try {
      while (inFlight < parallelism && remaining.hasNext) {
        submitNext()
      }
      var requiresRebase = false
      while (!requiresRebase && inFlight > 0) {
        requiresRebase = completion.take().get()
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
