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

import scala.collection.mutable

import org.apache.spark.SparkContext
import org.apache.spark.internal.config.{MEMORY_OFFHEAP_ENABLED, MEMORY_OFFHEAP_SIZE}
import org.apache.spark.sql.TPCDSBase
import org.apache.spark.sql.catalyst.util.resourceToString
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.TestSparkSession

import org.apache.comet.{CometConf, FuzzFallback}

/**
 * Fuzz suite that randomly vetoes Comet conversions while planning every TPC-DS query. The goal
 * is to stress the Spark/Comet boundary: if the rule pipeline ever constructs an invalid plan
 * (e.g. a ColumnarToRow over a non-columnar child), Spark's own driver-side assertions will throw
 * during plan construction and the failure is reported with the seed and query for reproduction.
 *
 * This suite only triggers Comet rule application via `queryExecution.executedPlan`; queries are
 * not executed, so no TPC-DS data is required. Managed (location-less) tables are created from
 * the TPC-DS schema so that `sql(...)` resolves.
 *
 * To reproduce a specific failure, rerun with `spark.comet.fuzz.fallback.seed=<seed>`.
 *
 * Run:
 * {{{
 *   ./mvnw test -Dsuites="org.apache.spark.sql.comet.CometFuzzFallbackSuite"
 * }}}
 */
class CometFuzzFallbackSuite extends TPCDSBase {

  /** Number of random seeds to exercise per query. */
  private val seedsPerQuery: Int =
    sys.env.getOrElse("COMET_FUZZ_SEEDS", "8").toInt

  /** Probability the fuzz layer vetoes converting a shuffle to Comet. */
  private val shuffleVetoProbability: Double =
    sys.env.getOrElse("COMET_FUZZ_SHUFFLE_P", "0.5").toDouble

  /** Probability the fuzz layer vetoes converting an operator to Comet. */
  private val execVetoProbability: Double =
    sys.env.getOrElse("COMET_FUZZ_EXEC_P", "0.2").toDouble

  // SF=1 synthetic stats are fine; we never execute.
  override protected val injectStats: Boolean = false

  override protected def sparkConf = {
    val conf = super.sparkConf
    conf.set("spark.sql.extensions", "org.apache.comet.CometSparkSessionExtensions")
    conf.set(
      "spark.shuffle.manager",
      "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager")
    conf.set(MEMORY_OFFHEAP_ENABLED.key, "true")
    conf.set(MEMORY_OFFHEAP_SIZE.key, "2g")
    conf.set(CometConf.COMET_ENABLED.key, "true")
    conf.set(CometConf.COMET_EXEC_ENABLED.key, "true")
    conf.set(CometConf.COMET_NATIVE_SCAN_ENABLED.key, "true")
    conf.set(CometConf.COMET_EXEC_SHUFFLE_ENABLED.key, "true")
    conf.set(CometConf.COMET_ONHEAP_MEMORY_OVERHEAD.key, "1g")
    conf.set(SQLConf.SHUFFLE_PARTITIONS.key, "4")
    conf
  }

  override protected def createSparkSession: TestSparkSession = {
    new TestSparkSession(new SparkContext("local[2]", this.getClass.getCanonicalName, sparkConf))
  }

  private val queryConf: Map[String, String] = Map(
    CometConf.COMET_FUZZ_FALLBACK_ENABLED.key -> "true",
    CometConf.COMET_FUZZ_FALLBACK_SHUFFLE_VETO_PROBABILITY.key -> shuffleVetoProbability.toString,
    CometConf.COMET_FUZZ_FALLBACK_EXEC_VETO_PROBABILITY.key -> execVetoProbability.toString,
    // Keep the DPP fallback on so we exercise the #3879 code path alongside random vetoes.
    CometConf.COMET_DPP_FALLBACK_ENABLED.key -> "true",
    SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10MB",
    SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true")

  private def runFuzzedQuery(group: String, query: String, seed: Long): Unit = {
    val sql = resourceToString(
      s"$group/$query.sql",
      classLoader = Thread.currentThread().getContextClassLoader)

    val perQueryConf = queryConf + (CometConf.COMET_FUZZ_FALLBACK_SEED.key -> seed.toString)

    withSQLConf(perQueryConf.toSeq: _*) {
      FuzzFallback.reset()
      try {
        // Touch executedPlan to drive the Comet rule pipeline and the debug assertion.
        spark.sql(sql).queryExecution.executedPlan
      } catch {
        case t: Throwable =>
          // Walk the cause chain for the most specific message.
          var cause: Throwable = t
          while (cause.getCause != null && cause.getCause != cause) cause = cause.getCause
          val msg = Option(cause.getMessage).getOrElse(cause.toString)
          throw new AssertionError(
            s"Fuzz fallback produced a bad plan for $group/$query with seed=$seed " +
              s"(shuffleVetoP=$shuffleVetoProbability, execVetoP=$execVetoProbability):\n$msg",
            t)
      } finally {
        FuzzFallback.reset()
      }
    }
  }

  // Queries known to be heavy or flaky for planning at SF=1 without execution; skip to keep
  // the suite responsive. Extend as needed.
  private val skip: Set[String] = Set.empty

  private def seedsFor(query: String): Seq[Long] = {
    // Derive a stable per-query seed base so queries don't all share the same RNG stream.
    val base = query.hashCode.toLong
    (0 until seedsPerQuery).map(i => base + i * 0x9e3779b97f4a7c15L)
  }

  for (q <- tpcdsQueries if !skip.contains(q)) {
    test(s"fuzz fallback on planning: tpcds/$q") {
      val failures = mutable.Buffer.empty[(Long, Throwable)]
      for (seed <- seedsFor(q)) {
        try runFuzzedQuery("tpcds", q, seed)
        catch { case t: Throwable => failures += ((seed, t)) }
      }
      if (failures.nonEmpty) {
        val first = failures.head
        val summary = failures.map { case (s, _) => s"seed=$s" }.mkString(", ")
        val msg = s"Fuzz fallback produced bad plans for $q across ${failures.size} seed(s): " +
          s"$summary\nFirst failure:\n${first._2.getMessage}"
        throw new AssertionError(msg, first._2)
      }
    }
  }

  for (q <- tpcdsQueriesV2_7_0 if !skip.contains(q)) {
    test(s"fuzz fallback on planning: tpcds-v2.7.0/$q") {
      val failures = mutable.Buffer.empty[(Long, Throwable)]
      for (seed <- seedsFor(q)) {
        try runFuzzedQuery("tpcds-v2.7.0", q, seed)
        catch { case t: Throwable => failures += ((seed, t)) }
      }
      if (failures.nonEmpty) {
        val first = failures.head
        val summary = failures.map { case (s, _) => s"seed=$s" }.mkString(", ")
        val msg = s"Fuzz fallback produced bad plans for $q across ${failures.size} seed(s): " +
          s"$summary\nFirst failure:\n${first._2.getMessage}"
        throw new AssertionError(msg, first._2)
      }
    }
  }
}
