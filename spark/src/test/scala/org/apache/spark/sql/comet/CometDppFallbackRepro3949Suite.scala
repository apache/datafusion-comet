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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{CometTestBase, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, PlanExpression}
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution.{FileSourceScanExec, LeafExecNode, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.{CometConf, CometExplainInfo}

/**
 * Attempts an end-to-end reproduction of issue #3949.
 *
 * #3949 reports `[INTERNAL_ERROR]` from `BroadcastExchangeExec.doCanonicalize`, ultimately caused
 * by `ColumnarToRowExec.<init>` asserting that its child `supportsColumnar`. The suspected root
 * cause is that Comet's DPP fallback (`CometShuffleExchangeExec.stageContainsDPPScan`) walks
 * `s.child.exists(...)` for a `FileSourceScanExec` with a `PlanExpression` partition filter, and
 * that walk is not stable across the two planning passes:
 *
 *   - initial planning: shuffle's child subtree includes the DPP scan -> `.exists` finds it ->
 *     fall back to Spark.
 *   - AQE stage prep: the inner stage that contained the scan has materialized and been replaced
 *     by a `ShuffleQueryStageExec` (a `LeafExecNode` whose `children == Seq.empty`). `.exists`
 *     can no longer descend into it, the DPP scan becomes invisible, the same shuffle is
 *     converted to Comet, and the plan shape changes between passes.
 *
 * This suite has two tests:
 *
 *   1. `mechanism`: synthetic. Builds a real DPP plan, observes the initial-pass decision is
 *      "fall back", then swaps the shuffle's child for an opaque `LeafExecNode` (mirroring how
 *      `ShuffleQueryStageExec` presents to `.exists`) and asserts the decision flips to
 *      "convert". Documents the mechanism without depending on AQE actually triggering it.
 *
 * 2. `endToEnd`: runs DPP-flavored queries with AQE on, sweeps a few seeds/variants, and asks
 * whether `df.collect()` ever throws or whether the final executed plan ever contains a Comet
 * shuffle whose child subtree (descending through `QueryStageExec.plan`) still contains a DPP
 * scan -- i.e. an inconsistency that the bug would produce.
 */
class CometDppFallbackRepro3949Suite extends CometTestBase {

  // ----------------------------------------------------------------------
  // Mechanism (synthetic): proves the AQE wrap flips the fallback decision.
  // ----------------------------------------------------------------------

  private def buildDppTables(dir: java.io.File, factPrefix: String): Unit = {
    val factPath = s"${dir.getAbsolutePath}/$factPrefix.parquet"
    val dimPath = s"${dir.getAbsolutePath}/${factPrefix}_dim.parquet"
    withSQLConf(CometConf.COMET_EXEC_ENABLED.key -> "false") {
      val sess = spark
      import sess.implicits._
      val oneDay = 24L * 60L * 60000L
      val now = System.currentTimeMillis()
      (0 until 400)
        .map(i => (i, new java.sql.Date(now + (i % 40) * oneDay), i.toString))
        .toDF("fact_id", "fact_date", "fact_str")
        .write
        .partitionBy("fact_date")
        .parquet(factPath)
      (0 until 40)
        .map(i => (i, new java.sql.Date(now + i * oneDay), i.toString))
        .toDF("dim_id", "dim_date", "dim_str")
        .write
        .parquet(dimPath)
    }
    spark.read.parquet(factPath).createOrReplaceTempView(s"${factPrefix}_fact")
    spark.read.parquet(dimPath).createOrReplaceTempView(s"${factPrefix}_dim")
  }

  private def unwrapAqe(plan: SparkPlan): SparkPlan = plan match {
    case a: AdaptiveSparkPlanExec => a.initialPlan
    case other => other
  }

  private def findFirstShuffle(plan: SparkPlan): Option[ShuffleExchangeExec] = {
    var found: Option[ShuffleExchangeExec] = None
    plan.foreach {
      case s: ShuffleExchangeExec if found.isEmpty => found = Some(s)
      case _ =>
    }
    found
  }

  test("mechanism: DPP fallback decision is sticky across an AQE-style child wrap") {
    withTempDir { dir =>
      buildDppTables(dir, "mech")
      withSQLConf(
        CometConf.COMET_DPP_FALLBACK_ENABLED.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.PREFER_SORTMERGEJOIN.key -> "true",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
        SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {

        val df = spark.sql(
          "select f.fact_date, count(*) c " +
            "from mech_fact f join mech_dim d on f.fact_date = d.dim_date " +
            "where d.dim_id > 35 group by f.fact_date")
        val initialPlan = unwrapAqe(df.queryExecution.executedPlan)
        val shuffle = findFirstShuffle(initialPlan).getOrElse {
          fail(s"No ShuffleExchangeExec found in initial plan:\n${initialPlan.treeString}")
        }

        val initialDecision = CometShuffleExchangeExec.columnarShuffleSupported(shuffle)

        val initialDppVisible = shuffle.child.exists {
          case scan: FileSourceScanExec =>
            scan.partitionFilters.exists(_.exists(_.isInstanceOf[PlanExpression[_]]))
          case _ => false
        }

        // Simulate AQE stage prep: wrap the shuffle's child in an opaque LeafExecNode,
        // matching how `ShuffleQueryStageExec` presents to `.exists` walks (its `children`
        // is `Seq.empty`). `withNewChildren` preserves tree-node tags, so if the fix is in
        // place the sticky CometFallback marker on `shuffle` carries over to
        // `postAqeShuffle`, and the decision short-circuits to false. Without the fix,
        // the DPP walk re-runs, fails to see the scan, and flips to true.
        val hiddenChild = OpaqueStageStub(shuffle.child.output)
        val postAqeShuffle =
          shuffle.withNewChildren(Seq(hiddenChild)).asInstanceOf[ShuffleExchangeExec]
        val postAqeDecision = CometShuffleExchangeExec.columnarShuffleSupported(postAqeShuffle)

        val postAqeDppVisible = postAqeShuffle.child.exists {
          case scan: FileSourceScanExec =>
            scan.partitionFilters.exists(_.exists(_.isInstanceOf[PlanExpression[_]]))
          case _ => false
        }

        // scalastyle:off println
        println("=== mechanism check ===")
        println(s"initialDppVisible=$initialDppVisible initialDecision=$initialDecision")
        println(s"postAqeDppVisible=$postAqeDppVisible postAqeDecision=$postAqeDecision")
        // scalastyle:on println

        assert(initialDppVisible, "initial child tree should expose DPP scan")
        assert(!postAqeDppVisible, "stage-wrapped child should hide DPP scan")
        assert(!initialDecision, s"expected fall back initially, got $initialDecision")
        assert(
          !postAqeDecision,
          s"decision must stay 'fall back' across the AQE-style wrap, got $postAqeDecision")
      }
    }
  }

  // ----------------------------------------------------------------------
  // End-to-end: actually run DPP-flavored queries and look for the bug.
  // ----------------------------------------------------------------------

  // Walk the executed plan descending into QueryStageExec.plan, and return any Comet
  // shuffle whose subtree still contains a DPP scan -- the cross-pass inconsistency the
  // bug would create.
  private def findInconsistentCometShuffles(plan: SparkPlan): Seq[SparkPlan] = {
    import org.apache.spark.sql.execution.adaptive.QueryStageExec
    def containsDppScan(p: SparkPlan): Boolean = p match {
      case scan: FileSourceScanExec =>
        scan.partitionFilters.exists(_.exists(_.isInstanceOf[PlanExpression[_]]))
      case stage: QueryStageExec => containsDppScan(stage.plan)
      case other => other.children.exists(containsDppScan)
    }
    val acc = mutable.Buffer.empty[SparkPlan]
    def walk(p: SparkPlan): Unit = {
      p match {
        case s: CometShuffleExchangeExec if containsDppScan(s) =>
          acc += s
        case _ =>
      }
      p match {
        case stage: QueryStageExec => walk(stage.plan)
        case _ => p.children.foreach(walk)
      }
    }
    walk(plan)
    acc.toSeq
  }

  // Match the executed plan: any Comet shuffle whose explain-info already says it should have
  // fallen back due to DPP. That's the smoking-gun pattern from the issue:
  //   CometColumnarExchange [COMET: Stage contains a scan with Dynamic Partition Pruning]
  private def findCometShufflesTaggedAsDppFallback(plan: SparkPlan): Seq[SparkPlan] = {
    import org.apache.spark.sql.execution.adaptive.QueryStageExec
    val acc = mutable.Buffer.empty[SparkPlan]
    def walk(p: SparkPlan): Unit = {
      p match {
        case s: CometShuffleExchangeExec =>
          val tags = s.getTagValue(CometExplainInfo.EXTENSION_INFO).getOrElse(Set.empty[String])
          if (tags.exists(_.contains("Dynamic Partition Pruning"))) acc += s
        case _ =>
      }
      p match {
        case stage: QueryStageExec => walk(stage.plan)
        case _ => p.children.foreach(walk)
      }
    }
    walk(plan)
    acc.toSeq
  }

  private def buildDppTablesShared(dir: java.io.File): Unit = {
    val factPath = s"${dir.getAbsolutePath}/e2e_fact.parquet"
    val dimPath = s"${dir.getAbsolutePath}/e2e_dim.parquet"
    val dim2Path = s"${dir.getAbsolutePath}/e2e_dim2.parquet"
    withSQLConf(CometConf.COMET_EXEC_ENABLED.key -> "false") {
      val sess = spark
      import sess.implicits._
      val oneDay = 24L * 60L * 60000L
      val now = System.currentTimeMillis()
      // larger fact so AQE actually has stages to materialize
      (0 until 4000)
        .map(i => (i, new java.sql.Date(now + (i % 80) * oneDay), i.toString, i % 10))
        .toDF("fact_id", "fact_date", "fact_str", "fact_grp")
        .write
        .partitionBy("fact_date")
        .parquet(factPath)
      (0 until 80)
        .map(i => (i, new java.sql.Date(now + i * oneDay), i.toString))
        .toDF("dim_id", "dim_date", "dim_str")
        .write
        .parquet(dimPath)
      (0 until 10)
        .map(i => (i, s"g$i"))
        .toDF("grp_id", "grp_str")
        .write
        .parquet(dim2Path)
    }
    spark.read.parquet(factPath).createOrReplaceTempView("e2e_fact")
    spark.read.parquet(dimPath).createOrReplaceTempView("e2e_dim")
    spark.read.parquet(dim2Path).createOrReplaceTempView("e2e_dim2")
  }

  // A handful of DPP-flavored queries in roughly increasing complexity. The last few mimic the
  // q14a structure (UNION ALL of multiple DPP-using subqueries with HAVING and outer aggregate)
  // because the issue specifically hit q14a / q14b / q31 / q47 / q57.
  private val queries: Seq[String] = Seq(
    // 1. Plain SMJ DPP
    """select f.fact_date, count(*) c
      |from e2e_fact f join e2e_dim d on f.fact_date = d.dim_date
      |where d.dim_id > 70
      |group by f.fact_date""".stripMargin,
    // 2. Aggregation above DPP join, then second join on the aggregate
    """select g.grp_str, sum(t.c) total from e2e_dim2 g join (
      |  select f.fact_grp, count(*) c
      |  from e2e_fact f join e2e_dim d on f.fact_date = d.dim_date
      |  where d.dim_id > 60
      |  group by f.fact_grp
      |) t on g.grp_id = t.fact_grp
      |group by g.grp_str""".stripMargin,
    // 3. UNION ALL of two DPP-using subqueries, outer aggregate -- q14a-style.
    """select channel, fact_grp, sum(c) total from (
      |  select 'a' channel, f.fact_grp, count(*) c
      |  from e2e_fact f join e2e_dim d on f.fact_date = d.dim_date
      |  where d.dim_id > 60
      |  group by f.fact_grp
      |  union all
      |  select 'b' channel, f.fact_grp, count(*) c
      |  from e2e_fact f join e2e_dim d on f.fact_date = d.dim_date
      |  where d.dim_id > 70
      |  group by f.fact_grp
      |) y group by channel, fact_grp""".stripMargin,
    // 4. UNION ALL with rollup -- pushes the bug surface higher up the plan.
    """select channel, fact_grp, sum(c) total from (
      |  select 'a' channel, f.fact_grp, count(*) c
      |  from e2e_fact f join e2e_dim d on f.fact_date = d.dim_date
      |  where d.dim_id > 60
      |  group by f.fact_grp
      |  union all
      |  select 'b' channel, f.fact_grp, count(*) c
      |  from e2e_fact f join e2e_dim d on f.fact_date = d.dim_date
      |  where d.dim_id > 70
      |  group by f.fact_grp
      |  union all
      |  select 'c' channel, f.fact_grp, count(*) c
      |  from e2e_fact f join e2e_dim d on f.fact_date = d.dim_date
      |  where d.dim_id > 50
      |  group by f.fact_grp
      |) y group by rollup (channel, fact_grp)""".stripMargin,
    // 5. q14a-style with HAVING + scalar subquery (forces broadcast-side stage materialization
    //    BEFORE outer planning -- the configuration that historically reproduced #3949).
    """with avg_sales as (
      |  select avg(c) avg_c from (
      |    select count(*) c from e2e_fact f
      |    join e2e_dim d on f.fact_date = d.dim_date
      |    where d.dim_id > 50
      |    group by f.fact_grp
      |  ) x
      |)
      |select 'store' channel, f.fact_grp, count(*) c
      |from e2e_fact f join e2e_dim d on f.fact_date = d.dim_date
      |where d.dim_id > 60
      |group by f.fact_grp
      |having count(*) > (select avg_c from avg_sales)
      |union all
      |select 'web' channel, f.fact_grp, count(*) c
      |from e2e_fact f join e2e_dim d on f.fact_date = d.dim_date
      |where d.dim_id > 70
      |group by f.fact_grp
      |having count(*) > (select avg_c from avg_sales)""".stripMargin)

  private val variants: Seq[(String, Map[String, String])] = Seq(
    "smj+aqe" -> Map(
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.PREFER_SORTMERGEJOIN.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true"),
    "bhj+aqe" -> Map(
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10MB",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true"),
    "smj+aqe+coalesce" -> Map(
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.PREFER_SORTMERGEJOIN.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      "spark.sql.adaptive.coalescePartitions.enabled" -> "true",
      "spark.sql.adaptive.coalescePartitions.minPartitionSize" -> "1b",
      "spark.sql.adaptive.coalescePartitions.initialPartitionNum" -> "16"))

  test("end-to-end: collect DPP queries under AQE; look for #3949 symptoms") {
    withTempDir { dir =>
      buildDppTablesShared(dir)

      val failures = mutable.Buffer.empty[(String, Int, String, String)]
      val suspicious = mutable.Buffer.empty[(String, Int, String)]

      for ((variantName, variantConf) <- variants; (q, idx) <- queries.zipWithIndex) {
        val conf = variantConf ++ Map(
          CometConf.COMET_DPP_FALLBACK_ENABLED.key -> "true",
          SQLConf.USE_V1_SOURCE_LIST.key -> "parquet")
        try {
          withSQLConf(conf.toSeq: _*) {
            val df = spark.sql(q)
            val rows: Array[Row] = df.collect()
            rows.length // touch

            val executedPlan = df.queryExecution.executedPlan
            val taggedFallback = findCometShufflesTaggedAsDppFallback(executedPlan)
            val inconsistent = findInconsistentCometShuffles(executedPlan)
            if (taggedFallback.nonEmpty) {
              suspicious += ((
                variantName,
                idx,
                "Comet shuffle tagged with DPP fallback reason but not fallen back " +
                  s"(${taggedFallback.size}). Plan:\n${executedPlan.treeString}"))
            }
            if (inconsistent.nonEmpty) {
              suspicious += ((
                variantName,
                idx,
                s"Comet shuffle still has DPP scan in subtree (${inconsistent.size}). " +
                  s"Plan:\n${executedPlan.treeString}"))
            }
          }
        } catch {
          case t: Throwable =>
            var c: Throwable = t
            while (c.getCause != null && c.getCause != c) c = c.getCause
            val sw = new java.io.StringWriter
            c.printStackTrace(new java.io.PrintWriter(sw))
            val detail = Option(c.getMessage).getOrElse(c.toString) + "\n" + sw.toString
            failures += ((variantName, idx, detail, c.getClass.getName))
        }
      }

      // scalastyle:off println
      println("=== end-to-end summary ===")
      println(s"failures (collect threw): ${failures.size}")
      failures.foreach { case (v, i, msg, cls) =>
        println(s"  $v/q$i: $cls")
        println(msg)
      }
      println(s"suspicious (plan-shape inconsistency): ${suspicious.size}")
      suspicious.foreach { case (v, i, note) =>
        println(s"  $v/q$i: ${note.take(800)}")
      }
      // scalastyle:on println

      // Demonstrate-the-bug assertion: if EITHER an #3949-shaped crash or a plan inconsistency
      // was observed, the bug is reproduced. The 3949 signature is an AssertionError whose
      // stack goes through ColumnarToRowExec.<init> (Columnar.scala:70) during
      // BroadcastExchangeExec.doCanonicalize.
      val anyEvidence = failures.exists { case (_, _, msg, _) =>
        msg.contains("INTERNAL_ERROR") ||
        msg.contains("supportsColumnar") ||
        msg.contains("ColumnarToRowExec") ||
        msg.contains("doCanonicalize")
      } || suspicious.nonEmpty

      val summary = new StringBuilder
      summary.append(s"#3949 reproduced: ${failures.size} collect failure(s) and ")
      summary.append(s"${suspicious.size} plan-shape inconsistencies\n")
      summary.append("---- failures ----\n")
      failures.foreach { case (v, i, msg, cls) =>
        summary.append(s"$v/q$i ($cls):\n").append(msg.take(4000)).append("\n")
      }
      summary.append("---- suspicious ----\n")
      suspicious.foreach { case (v, i, note) =>
        summary.append(s"$v/q$i: ${note.take(1500)}\n")
      }
      assert(!anyEvidence, summary.toString)
    }
  }
}

/**
 * `LeafExecNode` stub mirroring how `ShuffleQueryStageExec` presents to a `.exists` walk:
 * `children == Seq.empty`, so descent stops at the wrapper. Used by the mechanism test only.
 */
private case class OpaqueStageStub(output: Seq[Attribute]) extends LeafExecNode {
  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException("stub")
}
