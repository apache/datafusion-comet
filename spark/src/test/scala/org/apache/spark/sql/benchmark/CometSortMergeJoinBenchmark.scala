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
import org.apache.spark.sql.comet.{CometSortExec, CometSortMergeJoinExec}
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.{CometConf, CometSparkSessionExtensions}

/**
 * Benchmark to measure performance of Comet's SortMergeJoin across common Spark join shapes
 * (Inner count, Inner projected, LEFT OUTER, LEFT SEMI, RIGHT OUTER). To run:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometSortMergeJoinBenchmark
 * }}}
 * Pass `--many-map-only` to run only the small shuffle-block control.
 */
object CometSortMergeJoinBenchmark extends CometBenchmarkBase {

  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName("CometSortMergeJoinBenchmark")
      .set("spark.master", "local[5]")
      .setIfMissing("spark.driver.memory", "3g")
      .setIfMissing("spark.executor.memory", "3g")
      .set(
        "spark.shuffle.manager",
        "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager")
      .set("spark.comet.exec.onHeap.enabled", "true")

    val sparkSession = SparkSession
      .builder()
      .config(conf)
      .withExtensions(new CometSparkSessionExtensions)
      .getOrCreate()

    sparkSession.conf.set(CometConf.COMET_ENABLED.key, "false")
    sparkSession.conf.set(CometConf.COMET_EXEC_ENABLED.key, "false")
    sparkSession.conf.set(SQLConf.ANSI_ENABLED.key, "false")
    sparkSession.conf.set("spark.sql.shuffle.partitions", "2")

    sparkSession
  }

  // Force SortMergeJoin: prefer SMJ, disable broadcast.
  private val cometConfigs: Map[String, String] = Map(
    SQLConf.PREFER_SORTMERGEJOIN.key -> "true",
    SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
    SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1")

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    runManyMapBenchmark()
    if (mainArgs.contains("--many-map-only")) {
      return
    }

    val probeRows = 1024 * 1024
    val buildRows = 100000

    withTempPath { dir =>
      withTempTable("probe", "build", "build_repeated", "build_balanced", "build_skewed") {
        spark
          .range(probeRows.toLong)
          .selectExpr("id AS k", "id % 100 AS v")
          .write
          .parquet(s"${dir.getAbsolutePath}/probe")
        spark
          .range(buildRows.toLong)
          .selectExpr("id AS k", "id * 10 AS w")
          .write
          .parquet(s"${dir.getAbsolutePath}/build")
        val keyPatterns = Seq(
          "build_repeated" -> "id % 128",
          "build_balanced" -> "id % 2048",
          // Twelve of every 25 rows have a unique key. The others share seven keys.
          "build_skewed" -> "IF(id % 25 < 12, 7 + (id DIV 25) * 12 + id % 25, id % 7)")
        for ((table, key) <- keyPatterns) {
          spark
            .range(buildRows.toLong)
            .selectExpr(s"$key AS k")
            .write
            .parquet(s"${dir.getAbsolutePath}/$table")
        }
        for (table <- Seq("probe", "build") ++ keyPatterns.map(_._1)) {
          spark.read.parquet(s"${dir.getAbsolutePath}/$table").createOrReplaceTempView(table)
        }

        runBenchmark("SortMergeJoin - inner count") {
          runExpressionBenchmark(
            "inner count",
            probeRows.toLong,
            "SELECT /*+ MERGE(b) */ count(*) FROM probe p JOIN build b ON p.k = b.k",
            cometConfigs)
        }

        runBenchmark("SortMergeJoin - inner projected") {
          runExpressionBenchmark(
            "inner projected",
            probeRows.toLong,
            "SELECT /*+ MERGE(b) */ p.k, p.v, b.w FROM probe p JOIN build b ON p.k = b.k",
            cometConfigs)
        }

        runBenchmark("SortMergeJoin - left outer") {
          runExpressionBenchmark(
            "left outer",
            probeRows.toLong,
            "SELECT /*+ MERGE(b) */ count(*) FROM probe p LEFT JOIN build b ON p.k = b.k",
            cometConfigs)
        }

        runBenchmark("SortMergeJoin - left semi") {
          runExpressionBenchmark(
            "left semi",
            probeRows.toLong,
            "SELECT /*+ MERGE(b) */ count(*) FROM probe p LEFT SEMI JOIN build b ON p.k = b.k",
            cometConfigs)
        }

        runBenchmark("SortMergeJoin - right key reduction") {
          val settings = cometConfigs ++ Map(
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
            SQLConf.SHUFFLE_PARTITIONS.key -> "1",
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_MODE.key -> "native")
          withSQLConf(settings.toSeq: _*) {
            val flag = CometConf.COMET_EXEC_SORT_MERGE_JOIN_RIGHT_KEY_REDUCTION_ENABLED.key
            for ((table, distinctKeys, batchSize) <- Seq(
                ("build_repeated", 128, 1024),
                ("build_balanced", 2048, 8192),
                ("build_skewed", buildRows / 25 * 12 + 7, 1024),
                ("build", buildRows, 8192))) {
              val benchmark = new Benchmark(
                s"left semi, $distinctKeys distinct right keys in $buildRows rows",
                buildRows.toLong,
                output = output)
              val query = s"""SELECT /*+ MERGE(b) */ count(*)
                             |FROM (SELECT k FROM probe WHERE k < $buildRows) p
                             |LEFT SEMI JOIN $table b ON p.k = b.k""".stripMargin
              for (enabled <- Seq(false, true)) {
                val conf = Seq(
                  flag -> enabled.toString,
                  CometConf.COMET_BATCH_SIZE.key -> batchSize.toString)
                benchmark.addCase(s"right key reduction=$enabled") { _ =>
                  withSQLConf(conf: _*) { spark.sql(query).collect() }
                }
              }
              benchmark.run()
            }
          }
        }

        runBenchmark("SortMergeJoin - right outer") {
          runExpressionBenchmark(
            "right outer",
            probeRows.toLong,
            "SELECT /*+ MERGE(p) */ count(*) FROM probe p RIGHT JOIN build b ON p.k = b.k",
            cometConfigs)
        }
      }
    }
  }

  private def runManyMapBenchmark(): Unit = {
    val rightRows = 4L * 1024 * 1024
    val keys = 8192
    val reducers = 200
    val flag = CometConf.COMET_EXEC_SORT_MERGE_JOIN_RIGHT_KEY_REDUCTION_ENABLED.key
    withTempPath { dir =>
      withTempTable("many_map_probe", "many_map_build") {
        withSQLConf(
          CometConf.COMET_ENABLED.key -> "false",
          CometConf.COMET_EXEC_ENABLED.key -> "false",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          // Keep each small Parquet file in its own scan partition.
          "spark.sql.files.maxPartitionBytes" -> "134217728",
          "spark.sql.files.openCostInBytes" -> "134217728") {
          val probePath = s"${dir.getAbsolutePath}/probe"
          spark.range(keys.toLong).selectExpr("id AS k").write.parquet(probePath)
          spark.read.parquet(probePath).createOrReplaceTempView("many_map_probe")
          for (maps <- Seq(4, 200)) {
            val buildPath = s"${dir.getAbsolutePath}/build-$maps"
            spark
              .range(0, rightRows, 1, maps)
              .selectExpr(s"id % $keys AS k")
              .write
              .parquet(buildPath)
            spark.read.parquet(buildPath).createOrReplaceTempView("many_map_build")
            val settings = cometConfigs ++ Map(
              SQLConf.SHUFFLE_PARTITIONS.key -> reducers.toString,
              CometConf.COMET_ENABLED.key -> "true",
              CometConf.COMET_EXEC_ENABLED.key -> "true",
              CometConf.COMET_SHUFFLE_ENABLED.key -> "true",
              CometConf.COMET_SHUFFLE_MODE.key -> "native",
              CometConf.COMET_SHUFFLE_DIRECT_READ_ENABLED.key -> "true",
              CometConf.COMET_BATCH_SIZE.key -> "8192")
            withSQLConf(settings.toSeq: _*) {
              val query = """SELECT /*+ MERGE(b) */ count(*)
                            |FROM many_map_probe p LEFT SEMI JOIN many_map_build b
                            |ON p.k = b.k""".stripMargin
              val benchmark = new Benchmark(
                s"right key reduction, $maps maps, $reducers reducers",
                rightRows,
                output = output)
              for (enabled <- Seq(false, true)) {
                withSQLConf(flag -> enabled.toString) {
                  val df = spark.sql(query)
                  require(df.collect().head.getLong(0) == keys)
                  val plan = df.queryExecution.executedPlan
                  val join = plan.collectFirst { case j: CometSortMergeJoinExec => j }.get
                  require(join.nativeOp.getSortMergeJoin.getRightKeyReductionEnabled == enabled)
                  val shuffle = join.right.collectFirst { case s: CometShuffleExchangeExec =>
                    s
                  }.get
                  require(shuffle.numMappers == maps && shuffle.numPartitions == reducers)
                  val sortRows = join.right.collectFirst { case s: CometSortExec =>
                    s.longMetric("output_rows").value
                  }.get
                  if (enabled) {
                    val input = join.longMetric("right_key_reduction_input_rows").value
                    val reduced = join.longMetric("right_key_reduction_output_rows").value
                    val bypassed = join.longMetric("right_key_reduction_bypass_rows").value
                    require(input == rightRows && reduced == sortRows)
                    if (maps == 200) {
                      require(reduced == rightRows && bypassed == rightRows)
                    } else {
                      require(reduced < rightRows)
                    }
                    println(
                      s"maps=$maps reducers=$reducers input=$input " +
                        s"output=$reduced bypassed=$bypassed")
                  } else {
                    require(sortRows == rightRows)
                  }
                }
                benchmark.addCase(s"right key reduction=$enabled") { _ =>
                  withSQLConf(flag -> enabled.toString) { spark.sql(query).collect() }
                }
              }
              benchmark.run()
            }
          }
        }
      }
    }
  }
}
