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
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.{CometConf, CometSparkSessionExtensions}

/**
 * Benchmark to compare join implementations: Spark Sort Merge Join, Comet Sort Merge Join, Comet
 * Hash Join, and Comet Grace Hash Join across all join types.
 *
 * To run this benchmark:
 * {{{
 *   SPARK_GENERATE_BENCHMARK_FILES=1 make \
 *     benchmark-org.apache.spark.sql.benchmark.CometJoinBenchmark
 * }}}
 *
 * Results will be written to "spark/benchmarks/CometJoinBenchmark-**results.txt".
 */
object CometJoinBenchmark extends CometBenchmarkBase {
  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName("CometJoinBenchmark")
      .set("spark.master", "local[5]")
      .setIfMissing("spark.driver.memory", "3g")
      .setIfMissing("spark.executor.memory", "3g")
      .set("spark.executor.memoryOverhead", "10g")
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
    sparkSession.conf.set(CometConf.COMET_ONHEAP_MEMORY_OVERHEAD.key, "10g")
    sparkSession.conf.set("parquet.enable.dictionary", "false")
    sparkSession.conf.set("spark.sql.shuffle.partitions", "2")

    sparkSession
  }

  /** Base Comet exec config — shuffle mode auto, no SMJ replacement by default. */
  private val cometBaseConf = Map(
    CometConf.COMET_ENABLED.key -> "true",
    CometConf.COMET_EXEC_ENABLED.key -> "true",
    CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
    CometConf.COMET_SHUFFLE_MODE.key -> "auto",
    SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1")

  private def prepareTwoTables(dir: java.io.File, rows: Int, keyCardinality: Int): Unit = {
    import spark.implicits._
    val left = spark
      .range(rows)
      .selectExpr(
        s"id % $keyCardinality as key",
        "id as l_val1",
        "cast(id * 1.5 as double) as l_val2")
    prepareTable(dir, left)
    spark.read.parquet(dir.getCanonicalPath + "/parquetV1").createOrReplaceTempView("left_table")

    val rightDir = new java.io.File(dir, "right")
    rightDir.mkdirs()
    val right = spark
      .range(rows)
      .selectExpr(
        s"id % $keyCardinality as key",
        "id as r_val1",
        "cast(id * 2.5 as double) as r_val2")
    right.write
      .mode("overwrite")
      .option("compression", "snappy")
      .parquet(rightDir.getCanonicalPath)
    spark.read.parquet(rightDir.getCanonicalPath).createOrReplaceTempView("right_table")
  }

  private def addJoinCases(benchmark: Benchmark, query: String): Unit = {
    // 1. Spark Sort Merge Join (baseline — no Comet)
    benchmark.addCase("Spark Sort Merge Join") { _ =>
      withSQLConf(
        CometConf.COMET_ENABLED.key -> "false",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        "spark.sql.join.preferSortMergeJoin" -> "true") {
        spark.sql(query).noop()
      }
    }

    // 2. Comet Sort Merge Join (Spark plans SMJ, Comet executes it natively)
    benchmark.addCase("Comet Sort Merge Join") { _ =>
      withSQLConf(
        (cometBaseConf ++ Map(
          CometConf.COMET_REPLACE_SMJ.key -> "false",
          CometConf.COMET_EXEC_GRACE_HASH_JOIN_ENABLED.key -> "false",
          "spark.sql.join.preferSortMergeJoin" -> "true")).toSeq: _*) {
        spark.sql(query).noop()
      }
    }

    // 3. Comet Hash Join (replace SMJ with ShuffledHashJoin, Comet executes)
    benchmark.addCase("Comet Hash Join") { _ =>
      withSQLConf(
        (cometBaseConf ++ Map(
          CometConf.COMET_REPLACE_SMJ.key -> "true",
          CometConf.COMET_EXEC_GRACE_HASH_JOIN_ENABLED.key -> "false")).toSeq: _*) {
        spark.sql(query).noop()
      }
    }

    // 4. Comet Grace Hash Join (replace SMJ, use grace hash join)
    benchmark.addCase("Comet Grace Hash Join") { _ =>
      withSQLConf(
        (cometBaseConf ++ Map(
          CometConf.COMET_REPLACE_SMJ.key -> "true",
          CometConf.COMET_EXEC_GRACE_HASH_JOIN_ENABLED.key -> "true")).toSeq: _*) {
        spark.sql(query).noop()
      }
    }
  }

  private def joinBenchmark(joinType: String, rows: Int, keyCardinality: Int): Unit = {
    val joinClause = joinType match {
      case "Inner" => "JOIN"
      case "Left" => "LEFT JOIN"
      case "Right" => "RIGHT JOIN"
      case "Full" => "FULL OUTER JOIN"
      case "LeftSemi" => "LEFT SEMI JOIN"
      case "LeftAnti" => "LEFT ANTI JOIN"
    }

    val selectCols = joinType match {
      case "LeftSemi" | "LeftAnti" => "l.key, l.l_val1, l.l_val2"
      case _ => "l.key, l.l_val1, r.r_val1"
    }

    val query =
      s"SELECT $selectCols FROM left_table l $joinClause right_table r ON l.key = r.key"

    val benchmark =
      new Benchmark(
        s"$joinType Join (rows=$rows, cardinality=$keyCardinality)",
        rows,
        output = output)

    addJoinCases(benchmark, query)
    benchmark.run()
  }

  private def joinWithFilterBenchmark(rows: Int, keyCardinality: Int): Unit = {
    val query =
      "SELECT l.key, l.l_val1, r.r_val1 FROM left_table l " +
        "JOIN right_table r ON l.key = r.key WHERE l.l_val1 > r.r_val1"

    val benchmark =
      new Benchmark(
        s"Inner Join with Filter (rows=$rows, cardinality=$keyCardinality)",
        rows,
        output = output)

    addJoinCases(benchmark, query)
    benchmark.run()
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val rows = 1024 * 1024 * 2
    val keyCardinality = rows / 10 // ~10 matches per key

    withTempPath { dir =>
      prepareTwoTables(dir, rows, keyCardinality)

      runBenchmark("Join Benchmark") {
        for (joinType <- Seq("Inner", "Left", "Right", "Full", "LeftSemi", "LeftAnti")) {
          joinBenchmark(joinType, rows, keyCardinality)
        }
        joinWithFilterBenchmark(rows, keyCardinality)
      }
    }
  }
}
