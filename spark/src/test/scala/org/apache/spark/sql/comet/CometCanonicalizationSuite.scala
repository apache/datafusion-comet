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

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.execution.{ColumnarToRowExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec

/**
 * Targets issue #3949. The full stack trace shows the assertion fires from
 * `QueryPlan.doCanonicalize` → `withNewChildren(canonicalizedChildren)` →
 * `ColumnarToRowExec.copy`, meaning the canonical form of some Comet plan has `supportsColumnar
 * \== false` even though the original has it `true`. That violates the implicit contract that
 * canonicalization preserves `supportsColumnar`.
 *
 * This suite constructs every kind of CometPlan reachable from common queries, canonicalizes it,
 * and asserts that `supportsColumnar` is preserved. Any failing case is the minimal repro.
 */
class CometCanonicalizationSuite extends CometTestBase {

  private def collectCometPlans(root: SparkPlan): Seq[SparkPlan] = {
    // AdaptiveSparkPlanExec hides its children from collect; reach inside explicitly.
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

  private def planOf(query: String): SparkPlan = {
    spark.sql(query).queryExecution.executedPlan
  }

  private def checkCanonicalized(tag: String, plans: Seq[SparkPlan]): Unit = {
    assert(plans.nonEmpty, s"[$tag] produced no Comet plans with supportsColumnar=true")
    val broken = plans.flatMap { p =>
      try {
        val c = p.canonicalized
        if (!c.supportsColumnar) Some((p, c, "supportsColumnar=false after canonicalization"))
        else None
      } catch {
        case t: Throwable =>
          Some((p, null, s"canonicalization threw: ${t.getClass.getName}: ${t.getMessage}"))
      }
    }
    if (broken.nonEmpty) {
      val details = broken
        .map { case (p, c, reason) =>
          s"""node: ${p.getClass.getName}
             |  reason: $reason
             |  original:
             |${p.treeString}
             |  canonical:
             |${Option(c).map(_.treeString).getOrElse("<n/a>")}
             |""".stripMargin
        }
        .mkString("\n")
      fail(
        s"[$tag] ${broken.size} node(s) lose supportsColumnar under canonicalization:\n$details")
    }
  }

  private def plansOf(query: String): Seq[SparkPlan] = {
    collectCometPlans(planOf(query))
  }

  test("CometScanExec canonicalization preserves supportsColumnar") {
    withTempDir { dir =>
      val path = s"${dir.getAbsolutePath}/t.parquet"
      val sess = spark
      import sess.implicits._
      (0 until 10).map(i => (i, i.toString)).toDF("a", "b").write.parquet(path)
      spark.read.parquet(path).createOrReplaceTempView("t3949_scan")
      checkCanonicalized("scan", plansOf("select a, b from t3949_scan"))
    }
  }

  test("CometScanExec + filter + project canonicalization") {
    withTempDir { dir =>
      val path = s"${dir.getAbsolutePath}/t.parquet"
      val sess = spark
      import sess.implicits._
      (0 until 100).map(i => (i, i.toString, i * 2L)).toDF("a", "b", "c").write.parquet(path)
      spark.read.parquet(path).createOrReplaceTempView("t3949_fp")
      checkCanonicalized("scan+filter+project", plansOf("select a, c from t3949_fp where a > 10"))
    }
  }

  test("Aggregate plan canonicalization") {
    withTempDir { dir =>
      val path = s"${dir.getAbsolutePath}/t.parquet"
      val sess = spark
      import sess.implicits._
      (0 until 100).map(i => (i % 10, i)).toDF("k", "v").write.parquet(path)
      spark.read.parquet(path).createOrReplaceTempView("t3949_agg")
      checkCanonicalized(
        "aggregate",
        plansOf("select k, sum(v), count(*) from t3949_agg group by k"))
    }
  }

  test("Broadcast hash join canonicalization") {
    withTempDir { dir =>
      val fact = s"${dir.getAbsolutePath}/fact.parquet"
      val dim = s"${dir.getAbsolutePath}/dim.parquet"
      val sess = spark
      import sess.implicits._
      (0 until 200).map(i => (i, i % 20, i.toString)).toDF("id", "k", "v").write.parquet(fact)
      (0 until 20).map(i => (i, i.toString)).toDF("k", "d").write.parquet(dim)
      spark.read.parquet(fact).createOrReplaceTempView("t3949_f")
      spark.read.parquet(dim).createOrReplaceTempView("t3949_d")
      checkCanonicalized(
        "bhj",
        plansOf("select f.id, f.v, d.d from t3949_f f join t3949_d d on f.k = d.k"))
    }
  }

  /**
   * Directly mirrors the stack path in #3949: wrap each Comet plan in `ColumnarToRowExec` and
   * canonicalize the wrapper. `QueryPlan.doCanonicalize` does `withNewChildren(
   * canonicalizedChildren)`, which triggers `ColumnarToRowExec.copy(child = cometPlan.canonical)`
   * — the exact constructor call whose assertion fires in the issue.
   */
  private def checkWrappedCanonicalization(tag: String, plans: Seq[SparkPlan]): Unit = {
    assert(plans.nonEmpty, s"[$tag] produced no Comet plans with supportsColumnar=true")
    val broken = plans.flatMap { p =>
      try {
        // Constructing the wrapper on a non-canonicalized plan succeeds (supportsColumnar=true).
        val wrapper = ColumnarToRowExec(p)
        // Canonicalizing the wrapper reproduces the AQE path: it calls withNewChildren(
        // Seq(p.canonicalized)) which reinvokes the ColumnarToRowExec constructor.
        wrapper.canonicalized
        None
      } catch {
        case t: Throwable =>
          Some(
            (
              p,
              s"ColumnarToRowExec($tag).canonicalized threw: ${t.getClass.getName}: " +
                s"${t.getMessage}"))
      }
    }
    if (broken.nonEmpty) {
      val details = broken
        .map { case (p, reason) =>
          s"""node: ${p.getClass.getName}
             |  reason: $reason
             |  original:
             |${p.treeString}
             |""".stripMargin
        }
        .mkString("\n")
      fail(
        s"[$tag] ${broken.size} node(s) blow the ColumnarToRow assertion when canonicalized:" +
          s"\n$details")
    }
  }

  test("ColumnarToRowExec(cometPlan).canonicalized reproduces the #3949 path") {
    withTempDir { dir =>
      val fact = s"${dir.getAbsolutePath}/fact.parquet"
      val dim = s"${dir.getAbsolutePath}/dim.parquet"
      val sess = spark
      import sess.implicits._
      val oneDay = 24L * 60L * 60000L
      val now = System.currentTimeMillis()
      (0 until 400)
        .map(i => (i, new java.sql.Date(now + (i % 40) * oneDay), i.toString))
        .toDF("fact_id", "fact_date", "fact_str")
        .write
        .partitionBy("fact_date")
        .parquet(fact)
      (0 until 40)
        .map(i => (i, new java.sql.Date(now + i * oneDay), i.toString))
        .toDF("dim_id", "dim_date", "dim_str")
        .write
        .parquet(dim)
      spark.read.parquet(fact).createOrReplaceTempView("t3949_fact2")
      spark.read.parquet(dim).createOrReplaceTempView("t3949_dim2")

      val queries = Seq(
        "select a, b from t3949_fact2".replace("a, b", "fact_id, fact_str"),
        "select fact_id, count(*) from t3949_fact2 group by fact_id",
        "select * from t3949_fact2 f join t3949_dim2 d on f.fact_date = d.dim_date " +
          "where d.dim_id > 35")
      queries.zipWithIndex.foreach { case (q, i) =>
        checkWrappedCanonicalization(s"q$i", plansOf(q))
      }
    }
  }

  test("DPP-shaped plan canonicalization (mirrors #3949 setup)") {
    withTempDir { dir =>
      val fact = s"${dir.getAbsolutePath}/fact.parquet"
      val dim = s"${dir.getAbsolutePath}/dim.parquet"
      val sess = spark
      import sess.implicits._
      val oneDay = 24L * 60L * 60000L
      val now = System.currentTimeMillis()
      (0 until 400)
        .map(i => (i, new java.sql.Date(now + (i % 40) * oneDay), i.toString))
        .toDF("fact_id", "fact_date", "fact_str")
        .write
        .partitionBy("fact_date")
        .parquet(fact)
      (0 until 40)
        .map(i => (i, new java.sql.Date(now + i * oneDay), i.toString))
        .toDF("dim_id", "dim_date", "dim_str")
        .toDF("dim_id", "dim_date", "dim_str")
        .write
        .parquet(dim)
      spark.read.parquet(fact).createOrReplaceTempView("t3949_fact")
      spark.read.parquet(dim).createOrReplaceTempView("t3949_dim")
      checkCanonicalized(
        "dpp",
        plansOf(
          "select * from t3949_fact f join t3949_dim d on f.fact_date = d.dim_date " +
            "where d.dim_id > 35"))
    }
  }
}
