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

package org.apache.spark.sql.benchmark

import java.io.File
import java.lang.management.ManagementFactory

import scala.collection.mutable

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.comet.CometNativeExec
import org.apache.spark.sql.execution.{QueryExecution, SQLExecution}
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf
import org.apache.comet.vector.NativeUtil

/**
 * Benchmark to measure what it costs to move the output batches of a native plan to the JVM, with
 * the Arrow schema of the output sent with every batch or only when it changes
 * (`spark.comet.exec.cacheOutputSchema.enabled`).
 *
 * Most cases read one column of a Parquet table and project it `n` times, so the native plan does
 * almost nothing for each of its `n` output columns beyond exporting it. The last case reads `n`
 * distinct columns, so the work of the scan grows with `n` as well. The JVM only counts the rows
 * of the batches it receives, as a JVM operator that passes batches through would, so these
 * results bound the effect on a query from above.
 *
 * To run this benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make \
 *     benchmark-org.apache.spark.sql.benchmark.CometPlanOutputBenchmark
 * }}}
 * Results will be written to "spark/benchmarks/CometPlanOutputBenchmark-**results.txt".
 */
object CometPlanOutputBenchmark extends CometBenchmarkBase {

  private val narrowRows = 8 * 1024 * 1024
  private val wideRows = 1024 * 1024
  private val wideColumns = 100

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    // Keep the plan a scan and a projection under the transition to rows.
    spark.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
    withTempPath { dir =>
      withTempTable("narrow", "wide") {
        writeTable(
          dir,
          "narrow",
          spark.sql(
            "SELECT id AS i, CAST(id AS STRING) AS s, " +
              s"named_struct('a', id, 'b', CAST(id AS STRING)) AS st FROM range($narrowRows)"))
        writeTable(
          dir,
          "wide",
          spark.sql(
            (0 until wideColumns)
              .map(c => s"id + $c AS w$c")
              .mkString("SELECT ", ", ", s" FROM range($wideRows)")))

        for {
          (column, typeName) <- Seq(
            "i" -> "bigint",
            "s" -> "string",
            "st" -> "struct<bigint,string>")
          n <- Seq(1, 10, 100)
        } {
          val query =
            (0 until n).map(c => s"$column AS c$c").mkString("SELECT ", ", ", " FROM narrow")
          runCases(s"$n $typeName output columns, 1 column read", narrowRows, query)
        }
        runCases(
          s"$wideColumns bigint output columns, $wideColumns columns read",
          wideRows,
          "SELECT * FROM wide")
      }
    }
  }

  private def writeTable(dir: File, name: String, df: DataFrame): Unit = {
    val path = new File(dir, name).getCanonicalPath
    df.write.mode("overwrite").option("compression", "snappy").parquet(path)
    spark.read.parquet(path).createOrReplaceTempView(name)
  }

  private def withComet(cacheOutputSchema: Boolean)(f: => Unit): Unit =
    withSQLConf(
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_EXEC_CACHE_OUTPUT_SCHEMA_ENABLED.key -> cacheOutputSchema.toString)(f)

  private def runCases(name: String, rows: Long, query: String): Unit = {
    val benchmark = new Benchmark(name, rows, output = output)
    // Outside the timed runs, check that both arms return every row from a native plan, and that
    // the plan sends its output schema with every batch, or once per task when it caches it.
    for (cacheOutputSchema <- Seq(false, true)) withComet(cacheOutputSchema) {
      val imports = NativeUtil.planOutputSchemaImports.sum()
      val counts = countBatches(query)
      val schemas = NativeUtil.planOutputSchemaImports.sum() - imports
      assert(counts.rows == rows, s"$query returned ${counts.rows} rows, expected $rows")
      val expected = if (cacheOutputSchema) counts.tasks else counts.batches
      assert(schemas == expected, s"$query sent its schema $schemas times, expected $expected")
      benchmark.out.println(
        s"$name, cacheOutputSchema=$cacheOutputSchema: ${counts.batches} batches in " +
          s"${counts.tasks} tasks, the schema sent with $schemas of them")
    }

    CometBenchmarkBase.onOffModes(onFirst = false).foreach { case (mode, cacheOutputSchema) =>
      benchmark.addTimerCase(s"cacheOutputSchema $mode") { timer =>
        withComet(cacheOutputSchema) {
          val (queryExecution, plan) = nativePlan(query)
          timer.startTiming()
          countBatches(queryExecution, plan)
          timer.stopTiming()
        }
      }
    }
    benchmark.run()
    reportCpuPerBatch(benchmark, name, query)
  }

  private val processCpu = ManagementFactory.getOperatingSystemMXBean
    .asInstanceOf[com.sun.management.OperatingSystemMXBean]

  /**
   * Reports each arm's process CPU time per batch, the median of runs that alternate between the
   * arms so that drift affects both alike. Planning is left out. The native plan runs on a Tokio
   * worker while the task thread imports its batches, so wall time hides what the JVM saves until
   * the JVM is the slower of the two, but a busy executor has no idle core to hide it on.
   */
  private def reportCpuPerBatch(benchmark: Benchmark, name: String, query: String): Unit = {
    val samples =
      Map(false -> mutable.ArrayBuffer.empty[Double], true -> mutable.ArrayBuffer.empty[Double])
    for (_ <- 0 until 7; cacheOutputSchema <- Seq(false, true)) {
      withComet(cacheOutputSchema) {
        val plans = Seq.fill(3)(nativePlan(query))
        val start = processCpu.getProcessCpuTime
        val batches = plans.map { case (queryExecution, plan) =>
          countBatches(queryExecution, plan).batches
        }.sum
        samples(cacheOutputSchema) += (processCpu.getProcessCpuTime - start).toDouble / batches
      }
    }
    def median(cacheOutputSchema: Boolean): Double = {
      val sorted = samples(cacheOutputSchema).sorted
      sorted(sorted.length / 2)
    }
    val (off, on) = (median(false), median(true))
    benchmark.out.println(
      f"$name: process CPU per batch ${off / 1000}%.1f us with the schema in every batch, " +
        f"${on / 1000}%.1f us with it cached, ${(off - on) / 1000}%.1f us " +
        f"(${(off - on) / off * 100}%.1f%%) less")
  }

  private case class BatchCounts(rows: Long, batches: Long, tasks: Long)

  /** The planned `query`, and the root of its native plan, below the transition to rows. */
  private def nativePlan(query: String): (QueryExecution, CometNativeExec) = {
    val queryExecution = spark.sql(query).queryExecution
    val executedPlan = queryExecution.executedPlan
    // Below any codegen stage over the transition, too.
    val plan = executedPlan.collectFirst { case native: CometNativeExec => native }.getOrElse {
      throw new IllegalStateException(s"Expected a native plan for $query, got:\n$executedPlan")
    }
    (queryExecution, plan)
  }

  private def countBatches(query: String): BatchCounts = {
    val (queryExecution, plan) = nativePlan(query)
    countBatches(queryExecution, plan)
  }

  /**
   * Counts what the JVM receives from `plan`: rows, batches, and the tasks that received any
   * batch.
   */
  private def countBatches(queryExecution: QueryExecution, plan: CometNativeExec): BatchCounts = {
    // Runs as a SQL execution so that the tasks see the session's configs.
    val perTask = SQLExecution.withNewExecutionId(queryExecution) {
      plan
        .executeColumnar()
        .mapPartitions { batches =>
          var rows = 0L
          var count = 0L
          batches.foreach { batch =>
            rows += batch.numRows()
            count += 1
          }
          Iterator((rows, count))
        }
        .collect()
    }
    BatchCounts(perTask.map(_._1).sum, perTask.map(_._2).sum, perTask.count(_._2 > 0))
  }
}
