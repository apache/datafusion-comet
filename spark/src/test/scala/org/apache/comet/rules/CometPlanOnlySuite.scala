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

package org.apache.comet.rules

import org.apache.logging.log4j.Level
import org.apache.spark.CometListenerBusUtils
import org.apache.spark.sql.{CometTestBase, SparkSession}
import org.apache.spark.sql.comet.CometPlan
import org.apache.spark.sql.execution.{FileSourceScanExec, QueryExecution, SparkPlan, SubqueryBroadcastExec}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.QueryExecutionListener

import org.apache.comet.{CometConf, CometCoverageStats}
import org.apache.comet.CometSparkSessionExtensions.isSpark40Plus

/**
 * Tests for plan-only mode: `spark.comet.explain.planOnly.enabled`.
 *
 * Two properties matter. The query must run exactly as it would with Comet off, which is checked
 * by asserting the executed plan holds no Comet operator. And the report must describe the plan
 * Comet would really have executed, which is checked by running the same query with Comet enabled
 * and comparing the report's coverage against `CometCoverageStats` for the plan that ran.
 */
class CometPlanOnlySuite extends CometTestBase {

  private val PLAN_ONLY_PREFIX = "[Comet plan-only]"

  private val reporterLogger = CometPlanOnly.getClass.getName.stripSuffix("$")

  /**
   * Runs `f` and returns the plan-only reports logged for it.
   *
   * The report is written from the listener bus, so the bus has to be drained before the appender
   * is installed - or a report for an action that ran earlier, the fixture's view creation say,
   * lands in the window - and again before the log is read, or the reports for `f`'s own actions
   * may not have been written yet.
   */
  private def capturePlanOnlyReports(f: => Unit): Seq[String] = {
    CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)
    val appender = new LogAppender("Comet plan-only reports")
    withLogAppender(appender, loggerNames = Seq(reporterLogger), level = Some(Level.WARN)) {
      f
      CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)
    }
    appender.loggingEvents
      .map(_.getMessage.getFormattedMessage)
      .filter(_.startsWith(PLAN_ONLY_PREFIX))
      .toSeq
  }

  /** The `Comet accelerated N out of M eligible operators` counts in a plan-only report. */
  private def coverageOf(report: String): (Int, Int) = {
    val pattern = """Comet accelerated (\d+) out of (\d+) eligible operators""".r
    pattern
      .findFirstMatchIn(report)
      .map(m => (m.group(1).toInt, m.group(2).toInt))
      .getOrElse(fail(s"report has no coverage summary:\n$report"))
  }

  /** The transition count in a plan-only report. */
  private def transitionsOf(report: String): Int = {
    """contains (\d+) transitions""".r
      .findFirstMatchIn(report)
      .map(_.group(1).toInt)
      .getOrElse(fail(s"report has no transition count:\n$report"))
  }

  private def planOnlyConf(aqe: Boolean, useV1: Boolean): Seq[(String, String)] = Seq(
    SQLConf.USE_V1_SOURCE_LIST.key -> (if (useV1) "parquet" else ""),
    SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
    CometConf.COMET_ENABLED.key -> "true",
    CometConf.COMET_EXEC_ENABLED.key -> "true")

  // `collect` here is `AdaptiveSparkPlanHelper.collect`, which descends into query stages; a plain
  // `SparkPlan.collect` stops at them, because a stage holds its plan outside `children`.
  private def cometOperatorsOf(plan: SparkPlan): Seq[SparkPlan] =
    collect(plan) { case p: CometPlan => p }

  for {
    useV1 <- Seq(true, false)
    aqe <- Seq(true, false)
  } {
    test(s"the query runs on Spark (${if (useV1) "V1" else "V2"} scan, AQE=$aqe)") {
      // The source list has to be set before the fixture reads the table: `withParquetTable`
      // resolves the relation through `spark.read` and registers the result as a temp view, so
      // changing it afterwards leaves a V1 relation in place and the V2 case would not be covered.
      withSQLConf(planOnlyConf(aqe, useV1): _*) {
        withParquetTable((0 until 100).map(i => (i, i % 5)), "tbl") {
          val query = "SELECT _2, count(*) FROM tbl GROUP BY _2"

          // Sanity check on the fixture: with the config off Comet does accelerate this query, so
          // the assertion below is about plan-only mode and not about an unrelated fallback. Only
          // for V1: Comet declines a plain V2 Parquet scan ("Unsupported scan: ParquetScan"), so
          // the V2 case is here to cover `CometScanRule`'s V2 branch, not to show acceleration.
          if (useV1) {
            val normal = sql(query)
            normal.collect()
            assert(cometOperatorsOf(normal.queryExecution.executedPlan).nonEmpty)
          }

          withSQLConf(CometConf.COMET_EXPLAIN_PLAN_ONLY_ENABLED.key -> "true") {
            val df = sql(query)
            val reports = capturePlanOnlyReports(df.collect())
            val executed = df.queryExecution.executedPlan
            assert(
              cometOperatorsOf(executed).isEmpty,
              s"plan-only mode left Comet operators in the executed plan:\n$executed")
            // The fixture must exercise the scan path the test name claims.
            val scans = collect(executed) {
              case p: FileSourceScanExec => p
              case p: BatchScanExec => p
            }
            if (useV1) {
              assert(
                scans.exists(_.isInstanceOf[FileSourceScanExec]),
                s"expected a V1 scan:\n$executed")
            } else {
              assert(
                scans.exists(_.isInstanceOf[BatchScanExec]),
                s"expected a V2 scan:\n$executed")
            }
            assert(reports.size == 1, s"expected one report, got:\n${reports.mkString("\n\n")}")
          }
        }
      }
    }
  }

  test("the config off leaves Comet running the query") {
    withSQLConf(
      planOnlyConf(aqe = true, useV1 = true) :+
        (CometConf.COMET_EXPLAIN_PLAN_ONLY_ENABLED.key -> "false"): _*) {
      withParquetTable((0 until 100).map(i => (i, i % 5)), "tbl") {
        val df = sql("SELECT _2, count(*) FROM tbl GROUP BY _2")
        val reports = capturePlanOnlyReports(df.collect())
        assert(cometOperatorsOf(df.queryExecution.executedPlan).nonEmpty)
        assert(reports.isEmpty, s"expected no report, got:\n${reports.mkString("\n\n")}")
      }
    }
  }

  // One report per action, whatever Spark does to the plan in between. Under AQE one query reaches
  // the conversion rules once per query stage and once per adaptive re-optimization on top of the
  // initial planning, and a query with subqueries reaches them once per subquery as well; none of
  // that is visible from a query execution listener.
  for (aqe <- Seq(true, false)) {
    test(s"one report per action for a multi-stage query with a subquery (AQE=$aqe)") {
      withSQLConf(
        planOnlyConf(aqe, useV1 = true) ++ Seq(
          // Force a shuffled join so the plan has more than one shuffle boundary.
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
          CometConf.COMET_EXPLAIN_PLAN_ONLY_ENABLED.key -> "true"): _*) {
        withParquetTable((0 until 100).map(i => (i, i % 5)), "tbl") {
          // The filter has to leave rows behind: an empty stage lets AQE replace the whole plan
          // with an empty relation, and the report would then describe that instead of the query.
          val query = "SELECT a._2, count(*) FROM tbl a JOIN tbl b ON a._1 = b._2 " +
            "WHERE a._1 >= (SELECT min(_2) FROM tbl) GROUP BY a._2 ORDER BY 1"
          val reports = capturePlanOnlyReports(sql(query).collect())
          assert(reports.size == 1, s"expected one report, got:\n${reports.mkString("\n\n")}")
          // The report describes the whole query, subquery included.
          assert(reports.head.contains("HashAggregate"), s"report:\n${reports.head}")
        }
      }
    }
  }

  test("two actions on the same query are reported twice") {
    withSQLConf(
      planOnlyConf(aqe = true, useV1 = true) :+
        (CometConf.COMET_EXPLAIN_PLAN_ONLY_ENABLED.key -> "true"): _*) {
      withParquetTable((0 until 100).map(i => (i, i % 5)), "tbl") {
        val df = sql("SELECT _2, count(*) FROM tbl GROUP BY _2")
        val reports = capturePlanOnlyReports {
          df.collect()
          df.collect()
        }
        assert(reports.size == 2, s"expected two reports, got:\n${reports.mkString("\n\n")}")
      }
    }
  }

  // Reporting is driven by `QueryExecutionListener`, which Spark fires from the Dataset action
  // path, so RDD actions are outside it. What `df.rdd` itself does differs by version: on 4.0+ it
  // runs a query of its own under its own execution id and is reported once, on 3.4/3.5 it is not
  // reported at all. Either way the RDD's own actions add nothing, which is the part that makes
  // "one report per action" the wrong description of this mode. Asserted rather than skipped so
  // the version split is pinned instead of rediscovered.
  test("RDD actions are outside the reported path") {
    withSQLConf(
      planOnlyConf(aqe = true, useV1 = true) :+
        (CometConf.COMET_EXPLAIN_PLAN_ONLY_ENABLED.key -> "true"): _*) {
      withParquetTable((0 until 100).map(i => (i, i % 5)), "tbl") {
        val reports = capturePlanOnlyReports {
          val rdd = spark.sql("SELECT _2, count(*) FROM tbl GROUP BY _2").rdd
          // Three actions on the same RDD. None of them starts a SQL execution, so between them
          // they contribute nothing beyond whatever obtaining `rdd` already did.
          rdd.count()
          rdd.count()
          rdd.collect()
        }
        if (isSpark40Plus) {
          assert(
            reports.size == 1,
            s"expected obtaining `.rdd` to be reported once, got:\n${reports.mkString("\n\n")}")
          assert(reports.head.contains("HashAggregate"), s"report:\n${reports.head}")
        } else {
          assert(
            reports.isEmpty,
            s"expected no report on Spark 3.x, got:\n${reports.mkString("\n\n")}")
        }
      }
    }
  }

  /**
   * Runs `SELECT id, count(*) FROM range(100) GROUP BY id` on a fresh session with plan-only mode
   * on, holding the listener bus until `whileParked` has run, and returns the reports.
   *
   * The ordering has to be forced or a test using this proves nothing: normally the bus drains
   * during `collect()` and the callback sees the settings still in place. A fresh session gives a
   * deterministic registration order - `CometPlanOnly` registers its listener lazily on first
   * use, so on a session that has never run a plan-only query the gating listener below is
   * registered first and therefore runs first, holding the bus until `whileParked` has returned.
   */
  private def reportsWithSettingsChangedBeforeCallback(
      whileParked: SparkSession => Unit): Seq[String] = {
    val session = spark.newSession()
    val gate = new java.util.concurrent.CountDownLatch(1)
    val gating = new QueryExecutionListener {
      override def onSuccess(name: String, qe: QueryExecution, durationNs: Long): Unit = {
        gate.await(60, java.util.concurrent.TimeUnit.SECONDS)
      }
      override def onFailure(name: String, qe: QueryExecution, e: Exception): Unit = ()
    }
    session.listenerManager.register(gating)
    try {
      CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)
      val appender = new LogAppender("Comet plan-only reports")
      withLogAppender(appender, loggerNames = Seq(reporterLogger), level = Some(Level.WARN)) {
        val conf = planOnlyConf(aqe = true, useV1 = true) :+
          (CometConf.COMET_EXPLAIN_PLAN_ONLY_ENABLED.key -> "true")
        conf.foreach { case (k, v) => session.conf.set(k, v) }
        session.sql("SELECT id, count(*) FROM range(100) GROUP BY id").collect()
        // The callback is parked in `gating.onSuccess`, so anything done here happens strictly
        // before `CometPlanOnly` gets to look at the settings.
        whileParked(session)
        gate.countDown()
        CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)
      }
      appender.loggingEvents
        .map(_.getMessage.getFormattedMessage)
        .filter(_.startsWith(PLAN_ONLY_PREFIX))
        .toSeq
    } finally {
      gate.countDown()
      session.listenerManager.unregister(gating)
    }
  }

  // The report is delivered asynchronously, so a caller can enable plan-only, run the action, and
  // restore the setting while the callback is still queued. Reading the flag back off the session
  // at that point drops the report for a query that really did run in plan-only mode. The gate
  // itself is the one setting the snapshot overrides, so restoring it must not cost the report.
  test("a report survives the setting being restored before the callback runs") {
    val reports = reportsWithSettingsChangedBeforeCallback { session =>
      session.conf.unset(CometConf.COMET_EXPLAIN_PLAN_ONLY_ENABLED.key)
    }
    assert(
      reports.size == 1,
      "expected the query planned under plan-only mode to still be reported, got:\n" +
        reports.mkString("\n\n"))
    assert(
      reports.head.contains("Comet accelerated"),
      s"expected a coverage report, got:\n${reports.head}")
  }

  // Every other setting shapes the preview rather than gating it, and the snapshot cannot be
  // injected into the conversion: `SparkPlan.conf` is the captured session's live conf, so
  // `op.conf` reads it whatever the listener does with active sessions or thread-locals. Reporting
  // anyway would describe a configuration the query never ran under, so the report is skipped and
  // says which settings moved. Compared here against the unchanged-settings control above, rather
  // than only asserting that some line was logged.
  test("a report is skipped when a conversion setting changed before the callback runs") {
    val changed = reportsWithSettingsChangedBeforeCallback { session =>
      // Not the gate: a setting the preview's conversion actually reads.
      session.conf.set(CometConf.COMET_EXEC_SORT_ENABLED.key, "false")
    }
    assert(changed.size == 1, s"expected one line, got:\n${changed.mkString("\n\n")}")
    assert(
      changed.head.contains("settings changed since it was planned"),
      s"expected the skip diagnostic, got:\n${changed.head}")
    assert(
      changed.head.contains(CometConf.COMET_EXEC_SORT_ENABLED.key),
      s"expected the diagnostic to name the changed setting, got:\n${changed.head}")
    assert(
      !changed.head.contains("Comet accelerated"),
      s"expected no coverage numbers from a configuration the query never ran under, got:\n" +
        changed.head)
  }

  // `ReusedExchangeExec` re-aliases the shared child's output, so a self-join can expose `k#16`
  // while its child produces `k#3`. Undoing the reuse without carrying those IDs across leaves
  // the parent sort referencing an attribute nothing below it produces, and Comet's binder then
  // declines the sort and the join above it - understating coverage for work Comet would have run.
  //
  // Run with projection both enabled and disabled. Restoring identity with an added `ProjectExec`
  // passed the first and failed the second: the synthetic node fails
  // `CometProjectExec.enabledConfig`, so the parent sort loses its native child and the join falls
  // back, losing a supported sort/join purely because of a bookkeeping node that a real conversion
  // of those branches would not contain. Rewriting the subtree's attribute IDs instead adds no node
  // and reads no user setting.
  Seq(true, false).foreach { projectEnabled =>
    test(
      "a reused exchange keeps its output IDs so consumers still convert " +
        s"(project.enabled=$projectEnabled)") {
      withSQLConf(
        planOnlyConf(aqe = false, useV1 = true) ++ Seq(
          CometConf.COMET_EXPLAIN_PLAN_ONLY_ENABLED.key -> "true",
          CometConf.COMET_EXEC_PROJECT_ENABLED.key -> projectEnabled.toString,
          SQLConf.EXCHANGE_REUSE_ENABLED.key -> "true",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1"): _*) {
        withParquetTable((0 until 100).map(i => (i, i % 5)), "tbl") {
          val reports = capturePlanOnlyReports {
            spark
              .sql("""SELECT a._2, b._2 FROM
                     |  (SELECT _2, count(*) c FROM tbl GROUP BY _2) a
                     |  JOIN
                     |  (SELECT _2, count(*) c FROM tbl GROUP BY _2) b
                     |  ON a._2 = b._2""".stripMargin)
              .collect()
          }
          assert(reports.size == 1, s"expected one report, got:\n${reports.mkString("\n\n")}")
          val report = reports.head
          assert(
            report.contains("CometSortMergeJoin") || report.contains("CometHashJoin"),
            s"the join over a reused exchange should convert in the preview, got:\n$report")
          assert(
            !report.contains("\nSort ") && !report.contains("+- Sort "),
            s"no Sort should be left on Spark by a lost attribute binding, got:\n$report")
        }
      }
    }
  }

  // A session runs plenty of metadata-only statements, and one 0% report each would bury the
  // reports worth reading.
  test("a metadata-only command is not reported") {
    withSQLConf(
      planOnlyConf(aqe = true, useV1 = true) :+
        (CometConf.COMET_EXPLAIN_PLAN_ONLY_ENABLED.key -> "true"): _*) {
      withParquetTable((0 until 100).map(i => (i, i % 5)), "tbl") {
        withTempView("v") {
          val reports = capturePlanOnlyReports {
            sql("CREATE OR REPLACE TEMP VIEW v AS SELECT _1 FROM tbl")
            sql("SHOW TABLES").collect()
          }
          assert(reports.isEmpty, s"expected no report, got:\n${reports.mkString("\n\n")}")
        }
      }
    }
  }

  /**
   * Asserts that the report for `query` in plan-only mode agrees with the coverage of the plan
   * Comet really executes for it.
   */
  private def assertReportMatchesRealPlan(
      query: String,
      compareTransitions: Boolean = true): Unit = {
    val df = sql(query)
    df.collect()
    val executed = CometCoverageStats.forPlan(df.queryExecution.executedPlan)
    assert(
      executed.cometOperators > 0,
      "the query must be partly accelerated for the comparison to mean anything:\n" +
        df.queryExecution.executedPlan)

    withSQLConf(CometConf.COMET_EXPLAIN_PLAN_ONLY_ENABLED.key -> "true") {
      val reports = capturePlanOnlyReports(sql(query).collect())
      assert(reports.size == 1, s"expected one report, got:\n${reports.mkString("\n\n")}")
      assert(
        coverageOf(reports.head) ==
          (executed.cometOperators, executed.cometOperators + executed.sparkOperators),
        s"report disagrees with the executed plan ($executed):\n${reports.head}")
      if (compareTransitions) {
        assert(
          transitionsOf(reports.head) == executed.transitions,
          s"report disagrees with the executed plan on transitions ($executed):\n${reports.head}")
      }
    }
  }

  for {
    aqe <- Seq(true, false)
    (shape, query) <- Seq(
      "aggregate" -> "SELECT _2, count(*), sum(_1) FROM tbl GROUP BY _2",
      "shuffled join" -> "SELECT a._2, count(*) FROM tbl a JOIN tbl b ON a._1 = b._2 GROUP BY a._2",
      "scalar subquery" -> "SELECT _1 FROM tbl WHERE _1 > (SELECT max(_2) FROM tbl)")
  } {
    test(s"report coverage matches the plan Comet executes ($shape, AQE=$aqe)") {
      withSQLConf(
        planOnlyConf(aqe, useV1 = true) :+
          (SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1"): _*) {
        withParquetTable((0 until 100).map(i => (i, i % 5)), "tbl") {
          assertReportMatchesRealPlan(query)
        }
      }
    }
  }

  // A stage Comet would have handed back to Spark for having too many transitions must be reported
  // as handed back. Reversion is forced on here, and Comet project execution off so that the
  // aggregate leaves transitions behind for the rule to count.
  for (aqe <- Seq(true, false)) {
    test(s"report accounts for post-columnar stage reversion (AQE=$aqe)") {
      withSQLConf(
        planOnlyConf(aqe, useV1 = true) ++ Seq(
          CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "true",
          CometConf.COMET_EXEC_TRANSITION_REVERT_MAX_TRANSITIONS.key -> "0",
          CometConf.COMET_EXEC_PROJECT_ENABLED.key -> "false"): _*) {
        withParquetTable((0 until 100).map(i => (i, i % 5)), "tbl") {
          val query = "SELECT _2, count(*), sum(_1) FROM tbl GROUP BY _2"

          // With reversion off Comet accelerates part of this plan, so a report that skipped the
          // post-columnar rules would not match the executed plan below.
          withSQLConf(CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "false") {
            val df = sql(query)
            df.collect()
            assert(CometCoverageStats.forPlan(df.queryExecution.executedPlan).cometOperators > 0)
          }

          // Transitions are not compared under AQE: Spark inserts them one query stage at a
          // time, so a stage that reversion handed back to Spark keeps a transition above the
          // stage boundary below it that a single pass over the flattened plan does not produce.
          // The operator counts, which are what the coverage percentage is built from, do match.
          assertReportMatchesRealPlan(query, compareTransitions = !aqe)
        }
      }
    }
  }

  /** Registers a `fact` table partitioned on the join key plus a small `dim` table. */
  private def withDppTables(f: => Unit): Unit = {
    withTempDir { dir =>
      withSQLConf(CometConf.COMET_EXEC_ENABLED.key -> "false") {
        val sess = spark
        import sess.implicits._
        (0 until 400)
          .map(i => (i, i % 10, s"f$i"))
          .toDF("fact_id", "fact_key", "fact_str")
          .write
          .partitionBy("fact_key")
          .parquet(s"${dir.getAbsolutePath}/fact")
        (0 until 10)
          .map(i => (i, i, s"d$i"))
          .toDF("dim_id", "dim_key", "dim_str")
          .write
          .parquet(s"${dir.getAbsolutePath}/dim")
      }
      spark.read.parquet(s"${dir.getAbsolutePath}/fact").createOrReplaceTempView("fact")
      spark.read.parquet(s"${dir.getAbsolutePath}/dim").createOrReplaceTempView("dim")
      withTempView("fact", "dim")(f)
    }
  }

  // A dynamic partition pruning subquery is prepared by `PlanDynamicPruningFilters`, which prepares
  // the build plan and only then wraps it in the broadcast exchange, so the stage the post-columnar
  // rules judged is the exchange's child. Reversion is forced on so that getting that boundary
  // wrong changes the number.
  test("report coverage matches the plan Comet executes (DPP subquery)") {
    withSQLConf(
      planOnlyConf(aqe = false, useV1 = true) ++ Seq(
        SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "true",
        CometConf.COMET_EXEC_TRANSITION_REVERT_ENABLED.key -> "true",
        CometConf.COMET_EXEC_TRANSITION_REVERT_MAX_TRANSITIONS.key -> "0",
        CometConf.COMET_EXEC_PROJECT_ENABLED.key -> "false"): _*) {
      withDppTables {
        val query = "SELECT f.fact_id, f.fact_str, d.dim_str FROM fact f " +
          "JOIN dim d ON f.fact_key = d.dim_key WHERE d.dim_id < 10"
        val df = sql(query)
        df.collect()
        // `exists` walks children only, and a DPP subquery hangs off the scan's expressions.
        assert(
          df.queryExecution.executedPlan.collectWithSubqueries { case p: SubqueryBroadcastExec =>
            p
          }.nonEmpty,
          s"the query must produce a DPP subquery:\n${df.queryExecution.executedPlan}")

        assertReportMatchesRealPlan(query)
      }
    }
  }

  // A query AQE re-plans wholesale once a stage materializes empty. The report describes the plan
  // AQE settled on, and there is still exactly one of them.
  test("an adaptive query that collapses to nothing is reported once") {
    withSQLConf(
      planOnlyConf(aqe = true, useV1 = true) ++ Seq(
        SQLConf.SHUFFLE_PARTITIONS.key -> "2",
        CometConf.COMET_EXPLAIN_PLAN_ONLY_ENABLED.key -> "true"): _*) {
      val query = "SELECT id % 2 AS k, count(*) AS n FROM range(20) WHERE id < 0 GROUP BY id % 2"
      val reports = capturePlanOnlyReports(spark.sql(query).collect())
      assert(reports.size == 1, s"expected one report, got:\n${reports.mkString("\n\n")}")
    }
  }
}
