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
import org.apache.spark.sql.execution.{ColumnarToRowExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.TestSparkSession

import org.apache.comet.CometConf

/**
 * Scans every TPC-DS query plan for Comet nodes whose canonicalization either throws or produces
 * a non-columnar result. Targets issue #3949: the stack trace shows Spark's `ColumnarToRowExec`
 * constructor assertion firing during `QueryPlan.doCanonicalize` → `withNewChildren(
 * canonicalizedChildren)`. That can only happen if `cometPlan.canonicalized.supportsColumnar` is
 * false.
 *
 * For each Comet node produced by planning a TPC-DS query, this suite:
 *   1. Calls `p.canonicalized` and asserts `supportsColumnar == true` (the direct invariant). 2.
 *      Wraps the original `p` in `ColumnarToRowExec(p)` and canonicalizes the wrapper — exactly
 *      the call path the issue's stack trace takes.
 *
 * Plans are only compiled (no execution), so no TPC-DS data is required.
 */
class CometCanonicalizationTpcdsSuite extends TPCDSBase {

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

  private def collectCometPlans(root: SparkPlan): Seq[SparkPlan] = {
    def walk(p: SparkPlan): Seq[SparkPlan] = p match {
      case a: AdaptiveSparkPlanExec =>
        Seq(a.initialPlan, a.executedPlan).flatMap(walk)
      case other =>
        val self =
          if (other.isInstanceOf[CometPlan] && other.supportsColumnar) Seq(other) else Nil
        self ++ other.children.flatMap(walk)
    }
    walk(root)
  }

  case class Failure(query: String, node: SparkPlan, mode: String, cause: Throwable) {
    def pretty: String =
      s"""[$query] ${node.getClass.getSimpleName} — $mode — ${Option(cause.getMessage).getOrElse(
          cause.toString)}
         |  original node:
         |${node.treeString}
         |""".stripMargin
  }

  private def checkNode(query: String, p: SparkPlan, failures: mutable.Buffer[Failure]): Unit = {
    // Direct canonicalization.
    try {
      val c = p.canonicalized
      if (!c.supportsColumnar) {
        failures += Failure(
          query,
          p,
          "canonicalized.supportsColumnar=false",
          new IllegalStateException(
            s"canonical form: ${c.getClass.getSimpleName}\n${c.treeString}"))
      }
    } catch {
      case t: Throwable =>
        failures += Failure(query, p, "canonicalized threw", t)
    }

    // Wrapped-in-ColumnarToRow canonicalization — mirrors the #3949 stack exactly.
    try {
      val wrapper = ColumnarToRowExec(p)
      wrapper.canonicalized
    } catch {
      case t: Throwable =>
        failures += Failure(query, p, "ColumnarToRowExec(p).canonicalized threw", t)
    }
  }

  private def runQueryScan(
      group: String,
      query: String,
      failures: mutable.Buffer[Failure]): Unit = {
    val sql = resourceToString(
      s"$group/$query.sql",
      classLoader = Thread.currentThread().getContextClassLoader)
    val plan =
      try spark.sql(sql).queryExecution.executedPlan
      catch {
        case t: Throwable =>
          failures += Failure(s"$group/$query", null, "executedPlan threw", t)
          return
      }
    collectCometPlans(plan).foreach(p => checkNode(s"$group/$query", p, failures))
  }

  private val perTestConf: Map[String, String] = Map(
    CometConf.COMET_DPP_FALLBACK_ENABLED.key -> "true",
    SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10MB",
    SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true")

  test("canonicalization holds across every TPC-DS query") {
    val failures = mutable.Buffer.empty[Failure]
    withSQLConf(perTestConf.toSeq: _*) {
      for (q <- tpcdsQueries) runQueryScan("tpcds", q, failures)
      for (q <- tpcdsQueriesV2_7_0) runQueryScan("tpcds-v2.7.0", q, failures)
    }
    if (failures.nonEmpty) {
      val distinctQueries = failures.map(_.query).distinct
      val header =
        s"Canonicalization broke in ${failures.size} Comet node(s) across " +
          s"${distinctQueries.size} query/ies:\n${distinctQueries.mkString(", ")}\n"
      // Limit output size on huge failure sets.
      val body = failures.take(10).map(_.pretty).mkString("\n")
      fail(
        header + body + (if (failures.size > 10) s"\n... (${failures.size - 10} more)" else ""))
    }
  }
}
