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

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.{CometConf, CometSparkSessionExtensions}

/**
 * DPP benchmark: measures row reduction and performance improvements from Dynamic Partition
 * Pruning with native scan.
 *
 * To run this benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometDPPBenchmark
 * }}}
 * Results will be written to "spark/benchmarks/CometDPPBenchmark-**results.txt".
 */
object CometDPPBenchmark extends CometBenchmarkBase {

  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName("CometDPPBenchmark")
      .set("spark.master", "local[4]")
      .setIfMissing("spark.driver.memory", "4g")
      .setIfMissing("spark.executor.memory", "4g")
      .set(
        "spark.shuffle.manager",
        "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager")

    val sparkSession = SparkSession.builder
      .config(conf)
      .withExtensions(new CometSparkSessionExtensions)
      .getOrCreate()

    sparkSession.conf.set(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key, "true")
    sparkSession.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")
    sparkSession.conf.set(CometConf.COMET_ENABLED.key, "false")
    sparkSession.conf.set(CometConf.COMET_EXEC_ENABLED.key, "false")
    sparkSession.conf.set("parquet.enable.dictionary", "false")
    sparkSession.conf.set("spark.sql.shuffle.partitions", "4")
    sparkSession
  }

  def dppJoinBenchmark(factRows: Int, dimRows: Int, selectivity: Double): Unit = {
    val filteredDimRows = (dimRows * selectivity).toInt
    val benchmark = new Benchmark(
      s"DPP Join (fact=$factRows, dim=$dimRows, selectivity=$selectivity)",
      factRows,
      output = output)

    withTempPath { dir =>
      withTempTable("fact_table", "dim_table") {
        spark
          .sql(s"""
            SELECT id, cast(id % $dimRows as long) as dim_key, rand() as value
            FROM range($factRows)
          """)
          .write
          .mode("overwrite")
          .parquet(s"${dir.getCanonicalPath}/fact")
        spark.read.parquet(s"${dir.getCanonicalPath}/fact").createOrReplaceTempView("fact_table")

        spark
          .sql(
            s"SELECT id, 'name_' || id as name FROM range($dimRows) WHERE id < $filteredDimRows")
          .write
          .mode("overwrite")
          .parquet(s"${dir.getCanonicalPath}/dim")
        spark.read.parquet(s"${dir.getCanonicalPath}/dim").createOrReplaceTempView("dim_table")

        val query =
          "SELECT f.*, d.name FROM fact_table f JOIN dim_table d ON f.dim_key = d.id"

        benchmark.addCase("Spark (baseline)") { _ =>
          withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
            spark.sql(query).noop()
          }
        }

        benchmark.addCase("Comet (auto scan)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true") {
            spark.sql(query).noop()
          }
        }

        benchmark.addCase("Comet (native_datafusion + DPP)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_NATIVE_SCAN_IMPL.key -> "native_datafusion") {
            spark.sql(query).noop()
          }
        }

        benchmark.run()
      }
    }
  }

  def ioMetricsBenchmark(factRows: Int, dimRows: Int, selectivity: Double): Unit = {
    val filteredDimRows = (dimRows * selectivity).toInt
    val expectedRows = (factRows.toDouble * selectivity).toLong

    withTempPath { dir =>
      withTempTable("fact_io", "dim_io") {
        spark
          .sql(s"""
            SELECT id, cast(id % $dimRows as long) as dim_key, rand() as value
            FROM range($factRows)
          """)
          .repartition(10)
          .write
          .mode("overwrite")
          .parquet(s"${dir.getCanonicalPath}/fact_io")
        spark.read.parquet(s"${dir.getCanonicalPath}/fact_io").createOrReplaceTempView("fact_io")

        spark
          .sql(
            s"SELECT id, 'name_' || id as name FROM range($dimRows) WHERE id < $filteredDimRows")
          .write
          .mode("overwrite")
          .parquet(s"${dir.getCanonicalPath}/dim_io")
        spark.read.parquet(s"${dir.getCanonicalPath}/dim_io").createOrReplaceTempView("dim_io")

        val query = "SELECT f.*, d.name FROM fact_io f JOIN dim_io d ON f.dim_key = d.id"

        var sparkRows = 0L
        var cometAutoRows = 0L
        var cometNativeRows = 0L
        var sparkPlan = ""
        var cometAutoPlan = ""
        var cometNativePlan = ""

        withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
          val df = spark.sql(query)
          df.collect()
          sparkRows = getNumOutputRows(df.queryExecution.executedPlan)
          sparkPlan = df.queryExecution.executedPlan.toString()
        }

        withSQLConf(
          CometConf.COMET_ENABLED.key -> "true",
          CometConf.COMET_EXEC_ENABLED.key -> "true") {
          val df = spark.sql(query)
          df.collect()
          cometAutoRows = getNumOutputRows(df.queryExecution.executedPlan)
          cometAutoPlan = df.queryExecution.executedPlan.toString()
        }

        withSQLConf(
          CometConf.COMET_ENABLED.key -> "true",
          CometConf.COMET_EXEC_ENABLED.key -> "true",
          CometConf.COMET_NATIVE_SCAN_IMPL.key -> "native_datafusion") {
          val df = spark.sql(query)
          df.collect()
          cometNativeRows = getNumOutputRows(df.queryExecution.executedPlan)
          cometNativePlan = df.queryExecution.executedPlan.toString()
        }

        val sparkReduction = if (cometNativeRows > 0) sparkRows.toDouble / cometNativeRows else 0
        val cometAutoReduction = sparkRows.toDouble / math.max(cometAutoRows, 1)

        val report = s"""
================================================================================
DPP Row Reduction Analysis (selectivity=$selectivity)
================================================================================
Fact rows: $factRows | Dim rows: $dimRows (filtered to $filteredDimRows)
Expected with DPP: ~$expectedRows rows

Implementation                    numOutputRows       Reduction Factor
--------------------------------------------------------------------------------
Spark (baseline)                  ${f"$sparkRows%,15d"}       1.0x
Comet (auto scan)                 ${f"$cometAutoRows%,15d"}       ${f"${cometAutoReduction}%.1f"}x
Comet (native_datafusion + DPP)   ${f"$cometNativeRows%,15d"}       ${f"$sparkReduction%.0f"}x
================================================================================

Key Metrics:
- I/O Reduction: ${f"$sparkReduction%.0f"}x fewer rows scanned with DPP
- Row Reduction: ${sparkRows - cometNativeRows} fewer rows processed
- Selectivity Impact: ${f"${selectivity * 100}%.1f"}%% of data actually needed
================================================================================

### Query Plans ###

--- Spark (baseline) ---
$sparkPlan

--- Comet (auto scan) ---
$cometAutoPlan

--- Comet (native_datafusion + DPP) ---
$cometNativePlan

================================================================================
"""
        output.foreach { out =>
          val writer = new java.io.PrintWriter(out)
          writer.println(report)
          writer.flush()
        }
        println(report)
      }
    }
  }

  private def getNumOutputRows(plan: SparkPlan): Long = {
    import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
    import org.apache.spark.sql.execution.WholeStageCodegenExec
    var total = 0L
    def collect(p: SparkPlan): Unit = {
      p.metrics.get("numOutputRows").foreach(m => total += m.value)
      p match {
        case a: AdaptiveSparkPlanExec => collect(a.executedPlan)
        case w: WholeStageCodegenExec => collect(w.child)
        case _ => p.children.foreach(collect)
      }
    }
    collect(plan)
    total
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val factRows = 5 * 1024 * 1024 // 5M rows
    val dimRows = 10000

    println("\n" + "=" * 70)
    println("DYNAMIC PARTITION PRUNING (DPP) BENCHMARK")
    println("=" * 70)
    println(s"Configuration: factRows=$factRows, dimRows=$dimRows")
    println("=" * 70)

    // Run I/O metrics to show row reduction
    println("\n### I/O Metrics (Row Reduction Analysis) ###")
    for (selectivity <- List(0.01, 0.1)) {
      ioMetricsBenchmark(factRows, dimRows, selectivity)
    }

    // Run performance benchmarks
    println("\n### Performance Benchmarks ###")
    for (selectivity <- List(0.01, 0.1, 0.5)) {
      dppJoinBenchmark(factRows, dimRows, selectivity)
    }
  }
}
