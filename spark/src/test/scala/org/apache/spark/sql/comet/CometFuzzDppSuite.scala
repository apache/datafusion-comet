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

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.internal.config.{MEMORY_OFFHEAP_ENABLED, MEMORY_OFFHEAP_SIZE}
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.{CometConf, FuzzFallback}

/**
 * Execution-based fuzz suite that exercises the DPP fallback code path added in #3879. Builds
 * small partitioned fact/dim tables, then for each seed:
 *   - randomly vetoes Comet shuffle/exec conversions,
 *   - actually executes a DPP-flavored query so AQE re-plans stages at runtime.
 *
 * If the rule pipeline ever produces an invalid plan, Spark's own driver-side assertions will
 * throw at plan construction and the failure is surfaced with the seed, query, and variant for
 * reproduction.
 */
class CometFuzzDppSuite extends CometTestBase {

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set(MEMORY_OFFHEAP_ENABLED.key, "true")
    conf.set(MEMORY_OFFHEAP_SIZE.key, "2g")
    conf.set(CometConf.COMET_ONHEAP_MEMORY_OVERHEAD.key, "1g")
    conf
  }

  private val seedsPerVariant: Int =
    sys.env.getOrElse("COMET_FUZZ_SEEDS", "40").toInt

  private def buildDppData(base: Path): (String, String) = {
    val factPath = s"${base.toString}/fact.parquet"
    val dimPath = s"${base.toString}/dim.parquet"
    withSQLConf(CometConf.COMET_EXEC_ENABLED.key -> "false") {
      val sess = spark
      import sess.implicits._
      val oneDay = 24L * 60L * 60000L
      val now = System.currentTimeMillis()
      val fact = (0 until 400)
        .map(i => (i, new java.sql.Date(now + (i % 40) * oneDay), i.toString))
        .toDF("fact_id", "fact_date", "fact_str")
      fact.write.partitionBy("fact_date").parquet(factPath)
      val dim = (0 until 40)
        .map(i => (i, new java.sql.Date(now + i * oneDay), i.toString))
        .toDF("dim_id", "dim_date", "dim_str")
      dim.write.parquet(dimPath)
    }
    (factPath, dimPath)
  }

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

  private val queries = Seq(
    // Classic DPP: join on partitioning column, filter on dim side.
    "select * from dpp_fact f join dpp_dim d on f.fact_date = d.dim_date where d.dim_id > 35",
    // Aggregated result with DPP.
    """select f.fact_date, count(*) c
      |from dpp_fact f join dpp_dim d on f.fact_date = d.dim_date
      |where d.dim_id > 30
      |group by f.fact_date""".stripMargin,
    // Two-stage query mixing row/columnar operators above a DPP scan.
    """select cnt, count(*) from (
      |  select f.fact_id, count(*) cnt
      |  from dpp_fact f join dpp_dim d on f.fact_date = d.dim_date
      |  where d.dim_id > 20
      |  group by f.fact_id
      |) group by cnt""".stripMargin)

  test("fuzz fallback on DPP execution") {
    withTempDir { dir =>
      val (factPath, dimPath) = buildDppData(new Path(dir.getAbsolutePath))
      spark.read.parquet(factPath).createOrReplaceTempView("dpp_fact")
      spark.read.parquet(dimPath).createOrReplaceTempView("dpp_dim")

      val failures = mutable.Buffer.empty[(String, String, Long, Throwable)]
      for ((variantName, variantConf) <- variants; (q, idx) <- queries.zipWithIndex) {
        for (i <- 0 until seedsPerVariant) {
          val seed = (variantName.hashCode.toLong * 31 + idx) * 1000003L + i
          val conf = variantConf ++ Map(
            CometConf.COMET_FUZZ_FALLBACK_ENABLED.key -> "true",
            CometConf.COMET_FUZZ_FALLBACK_SEED.key -> seed.toString,
            CometConf.COMET_FUZZ_FALLBACK_SHUFFLE_VETO_PROBABILITY.key -> "0.5",
            CometConf.COMET_FUZZ_FALLBACK_EXEC_VETO_PROBABILITY.key -> "0.3",
            CometConf.COMET_DPP_FALLBACK_ENABLED.key -> "true",
            SQLConf.USE_V1_SOURCE_LIST.key -> "parquet")
          try {
            withSQLConf(conf.toSeq: _*) {
              FuzzFallback.reset()
              try {
                val rows: Array[Row] = spark.sql(q).collect()
                // Touch rows to force materialization.
                rows.length
              } finally FuzzFallback.reset()
            }
          } catch {
            case t: Throwable => failures += ((variantName, s"q$idx", seed, t))
          }
        }
      }

      if (failures.nonEmpty) {
        val grouped = failures.groupBy { case (v, q, _, _) => (v, q) }
        val summary = grouped.toSeq
          .map { case ((v, q), fs) =>
            val seeds = fs.map(_._3).mkString(", ")
            s"  $v/$q: ${fs.size} seed(s) failed: $seeds"
          }
          .mkString("\n")
        val (fv, fq, fseed, ft) = failures.head
        var cause: Throwable = ft
        while (cause.getCause != null && cause.getCause != cause) cause = cause.getCause
        val msg = Option(cause.getMessage).getOrElse(cause.toString)
        throw new AssertionError(
          s"Fuzz fallback produced ${failures.size} failure(s) across variants:\n$summary\n" +
            s"First failure: $fv/$fq seed=$fseed\n$msg",
          ft)
      }
    }
  }
}
