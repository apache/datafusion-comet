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

import java.text.SimpleDateFormat

import scala.concurrent.duration._
import scala.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.sql.comet.execution.shuffle.{CometNativeShuffle, CometShuffleExchangeExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions
import org.apache.comet.CometSparkSessionExtensions.isSpark40Plus
import org.apache.comet.testing.{DataGenOptions, FuzzDataGenerator, SchemaGenOptions}

// spotless:off
/**
 * Benchmark to measure Comet shuffle performance. To run this benchmark:
 * `SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometShuffleBenchmark`
 * Add `-- --nested-hash-only` to run just the nested hash key cases.
 * Results will be written to "spark/benchmarks/CometShuffleBenchmark-**results.txt".
 */
// spotless:on
object CometShuffleBenchmark extends CometBenchmarkBase {

  /**
   * Types covered by the shuffle groups. A representative spread of fixed-width, variable-width,
   * and decimal encodings; the shuffle paths do not branch per numeric width, so covering every
   * integer and float size multiplies runtime without distinguishing implementations.
   */
  private val benchmarkTypes: Seq[DataType] =
    Seq(IntegerType, LongType, DoubleType, StringType, DecimalType(10, 0))

  /**
   * High partition count for the groups that measure both a low and a high fan-out. This must
   * stay above Spark's `spark.shuffle.sort.bypassMergeThreshold` (200). Below that threshold the
   * write path switches to `CometBypassMergeSortShuffleWriter`, which holds a page per partition
   * from a JVM-wide pool, so a lower value both raises the shuffle memory requirement several
   * fold and hides the JVM shuffle's degradation at high fan-out.
   */
  private val manyPartitions = 201

  /**
   * Spark's `Benchmark` defaults spend two seconds warming up and two seconds measuring every
   * case. At the row counts used here a single iteration takes tens of milliseconds, so those
   * floors, not the work, set the suite's runtime. Shorter budgets with a slightly higher
   * iteration minimum keep the comparison stable while cutting the floor by 4x.
   */
  private def microBenchmark(name: String, valuesPerIteration: Long): Benchmark =
    new Benchmark(
      name,
      valuesPerIteration,
      minNumIters = 5,
      warmupTime = 500.millis,
      minTime = 500.millis,
      output = output)

  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName("CometShuffleBenchmark")
      // Since `spark.master` always exists, overrides this value
      .set("spark.master", "local[5]")
      .setIfMissing("spark.driver.memory", "3g")
      .setIfMissing("spark.executor.memory", "3g")
      .set("spark.executor.memoryOverhead", "10g")
      .set(
        "spark.shuffle.manager",
        "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager")
      .set("spark.comet.shuffle.jvm.spillThreshold", "30000")

    val sparkSession = SparkSession
      .builder()
      .config(conf)
      .withExtensions(new CometSparkSessionExtensions)
      .getOrCreate()

    // Set default configs. Individual cases will change them if necessary.
    sparkSession.conf.set(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key, "true")
    sparkSession.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")
    sparkSession.conf.set(CometConf.COMET_ENABLED.key, "false")
    sparkSession.conf.set(CometConf.COMET_EXEC_ENABLED.key, "false")
    // TODO: support dictionary encoding in vectorized execution
    sparkSession.conf.set("parquet.enable.dictionary", "false")

    sparkSession
  }

  def shuffleArrayBenchmark(values: Int, dataType: DataType, partitionNum: Int): Unit = {
    val benchmark =
      microBenchmark(s"SQL ${dataType.sql} shuffle on array ($partitionNum Partition)", values)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(dir, spark.sql(s"SELECT CAST(1 AS ${dataType.sql}) AS c1 FROM $tbl"))

        benchmark.addCase("SQL Parquet - Spark") { _ =>
          spark
            .sql(s"SELECT ARRAY_REPEAT(CAST(1 AS ${dataType.sql}), 10) AS c1 FROM parquetV1Table")
            .repartition(partitionNum, Column("c1"))
            .noop()
        }

        benchmark.addCase("SQL Parquet - Comet (Spark Shuffle)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_ENABLED.key -> "false") {
            spark
              .sql(
                s"SELECT ARRAY_REPEAT(CAST(1 AS ${dataType.sql}), 10) AS c1 FROM parquetV1Table")
              .repartition(partitionNum, Column("c1"))
              .noop()
          }
        }

        benchmark.addCase("SQL Parquet - Comet (Comet JVM Shuffle)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_JVM_PREFER_DICTIONARY_RATIO.key -> "1.0",
            CometConf.COMET_SHUFFLE_MODE.key -> "jvm") {
            spark
              .sql(
                s"SELECT ARRAY_REPEAT(CAST(1 AS ${dataType.sql}), 10) AS c1 FROM parquetV1Table")
              .repartition(partitionNum, Column("c1"))
              .noop()
          }
        }

        benchmark.run()
      }
    }
  }

  def shuffleStructBenchmark(values: Int, dataType: DataType, partitionNum: Int): Unit = {
    val benchmark =
      microBenchmark(s"SQL ${dataType.sql} shuffle on struct ($partitionNum Partition)", values)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(dir, spark.sql(s"SELECT CAST(1 AS ${dataType.sql}) AS c1 FROM $tbl"))

        benchmark.addCase("SQL Parquet - Spark") { _ =>
          spark
            .sql(
              s"SELECT STRUCT(CAST(c1 AS ${dataType.sql})," +
                s"CAST(c1 AS ${dataType.sql}), " +
                s"CAST(c1 AS ${dataType.sql})) AS c1 FROM parquetV1Table")
            .repartition(partitionNum, Column("c1"))
            .noop()
        }

        benchmark.addCase("SQL Parquet - Comet (Spark Shuffle)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_ENABLED.key -> "false") {
            spark
              .sql(
                s"SELECT STRUCT(CAST(c1 AS ${dataType.sql})," +
                  s"CAST(c1 AS ${dataType.sql}), " +
                  s"CAST(c1 AS ${dataType.sql})) AS c1 FROM parquetV1Table")
              .repartition(partitionNum, Column("c1"))
              .noop()
          }
        }

        benchmark.addCase("SQL Parquet - Comet (Comet JVM Shuffle)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_JVM_PREFER_DICTIONARY_RATIO.key -> "1.0",
            CometConf.COMET_SHUFFLE_MODE.key -> "jvm") {
            spark
              .sql(
                s"SELECT STRUCT(CAST(c1 AS ${dataType.sql})," +
                  s"CAST(c1 AS ${dataType.sql}), " +
                  s"CAST(c1 AS ${dataType.sql})) AS c1 FROM parquetV1Table")
              .repartition(partitionNum, Column("c1"))
              .noop()
          }
        }

        benchmark.run()
      }
    }
  }

  def shuffleDictionaryBenchmark(values: Int, dataType: DataType, partitionNum: Int): Unit = {
    val benchmark =
      microBenchmark(s"SQL ${dataType.sql} Dictionary Shuffle($partitionNum Partition)", values)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        // Build the repeated value as a string and cast to the target type. ANSI mode, the
        // default on Spark 4.x, rejects a direct INT to BINARY cast.
        prepareTable(
          dir,
          spark.sql(
            s"SELECT CAST(REPEAT(CAST(1 AS STRING), 100) AS ${dataType.sql}) AS c1 FROM $tbl"))

        benchmark.addCase("SQL Parquet - Spark") { _ =>
          spark
            .sql("select c1 from parquetV1Table")
            .repartition(partitionNum, Column("c1"))
            .noop()
        }

        benchmark.addCase("SQL Parquet - Comet (Spark Shuffle)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_ENABLED.key -> "false") {
            spark
              .sql("select c1 from parquetV1Table")
              .repartition(partitionNum, Column("c1"))
              .noop()
          }
        }

        benchmark.addCase("SQL Parquet - Comet (Comet JVM Shuffle)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_JVM_PREFER_DICTIONARY_RATIO.key -> "1.0",
            CometConf.COMET_SHUFFLE_MODE.key -> "jvm") {
            spark
              .sql("select c1 from parquetV1Table")
              .repartition(partitionNum, Column("c1"))
              .noop()
          }
        }

        benchmark.addCase("SQL Parquet - Comet (Comet JVM Shuffle + Prefer Dictionary)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_JVM_PREFER_DICTIONARY_RATIO.key -> "2.0",
            CometConf.COMET_SHUFFLE_MODE.key -> "jvm") {
            spark
              .sql("select c1 from parquetV1Table")
              .repartition(partitionNum, Column("c1"))
              .noop()
          }
        }

        benchmark.addCase("SQL Parquet - Comet (Comet JVM Shuffle + Fallback to string)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_JVM_PREFER_DICTIONARY_RATIO.key -> "1000000000.0",
            CometConf.COMET_SHUFFLE_MODE.key -> "jvm") {
            spark
              .sql("select c1 from parquetV1Table")
              .repartition(partitionNum, Column("c1"))
              .noop()
          }
        }

        benchmark.run()
      }
    }
  }

  def shuffleWideBenchmark(
      values: Int,
      dataType: DataType,
      width: Int,
      partitionNum: Int): Unit = {
    val benchmark =
      microBenchmark(
        s"SQL Wide ($width cols) ${dataType.sql} Shuffle($partitionNum Partition)",
        values)

    val projection = (1 to width)
      .map(i => s"CAST(CAST(RAND(1) * 100 AS INTEGER) AS ${dataType.sql}) AS c$i")
      .mkString(", ")
    val columns = (1 to width).map(i => s"c$i").mkString(", ")

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(dir, spark.sql(s"SELECT $projection FROM $tbl"))

        benchmark.addCase("SQL Parquet - Spark") { _ =>
          spark
            .sql(s"select $columns from parquetV1Table")
            .repartition(partitionNum, Column("c1"))
            .noop()
        }

        benchmark.addCase("SQL Parquet - Comet (Spark Shuffle)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_ENABLED.key -> "false") {
            spark
              .sql(s"select $columns from parquetV1Table")
              .repartition(partitionNum, Column("c1"))
              .noop()
          }
        }

        benchmark.addCase("SQL Parquet - Comet (Comet JVM Shuffle)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_MODE.key -> "jvm") {
            spark
              .sql(s"select $columns from parquetV1Table")
              .repartition(partitionNum, Column("c1"))
              .noop()
          }
        }

        benchmark.addCase("SQL Parquet - Comet (Comet Native Shuffle)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_MODE.key -> "native") {
            spark
              .sql(s"select $columns from parquetV1Table")
              .repartition(partitionNum, Column("c1"))
              .noop()
          }
        }

        benchmark.run()
      }
    }
  }

  def shuffleRangePartitionBenchmark(
      values: Int,
      dataType: DataType,
      width: Int,
      partitionNum: Int): Unit = {
    val benchmark =
      microBenchmark(
        s"SQL Wide ($width cols) ${dataType.sql} Range Partition Shuffle($partitionNum Partition)",
        values)

    val projection = (1 to width)
      .map(i => s"CAST(CAST(RAND(1) * 100 AS INTEGER) AS ${dataType.sql}) AS c$i")
      .mkString(", ")
    val columns = (1 to width).map(i => s"c$i").mkString(", ")

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(dir, spark.sql(s"SELECT $projection FROM $tbl"))

        benchmark.addCase("SQL Parquet - Spark") { _ =>
          spark
            .sql(s"select $columns from parquetV1Table")
            .repartitionByRange(partitionNum, Column("c1"))
            .noop()
        }

        benchmark.addCase("SQL Parquet - Comet (Spark Shuffle)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_ENABLED.key -> "false") {
            spark
              .sql(s"select $columns from parquetV1Table")
              .repartitionByRange(partitionNum, Column("c1"))
              .noop()
          }
        }

        benchmark.addCase("SQL Parquet - Comet (Comet JVM Shuffle)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_MODE.key -> "jvm") {
            spark
              .sql(s"select $columns from parquetV1Table")
              .repartitionByRange(partitionNum, Column("c1"))
              .noop()
          }
        }

        benchmark.addCase("SQL Parquet - Comet (Comet Native Shuffle)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_MODE.key -> "native") {
            spark
              .sql(s"select $columns from parquetV1Table")
              .repartitionByRange(partitionNum, Column("c1"))
              .noop()
          }
        }

        benchmark.run()
      }
    }
  }

  def shuffleDeeplyNestedBenchmark(
      name: String,
      filename: String,
      numRows: Int,
      partitionNum: Int): Unit = {
    val benchmark =
      microBenchmark(s"Shuffle with nested schema ($name)", numRows)
    val df = spark.read.parquet(filename)
    withTempTable("deeplyNestedTable") {
      df.createOrReplaceTempView("deeplyNestedTable")
      val sql = "select * from deeplyNestedTable"

      benchmark.addCase("Spark") { _ =>
        spark
          .sql(sql)
          .repartition(partitionNum)
          .noop()
      }

      benchmark.addCase("Comet (Spark Shuffle)") { _ =>
        withSQLConf(
          CometConf.COMET_ENABLED.key -> "true",
          CometConf.COMET_EXEC_ENABLED.key -> "true",
          CometConf.COMET_SHUFFLE_ENABLED.key -> "false") {
          spark
            .sql(sql)
            .repartition(partitionNum)
            .noop()
        }
      }

      for (shuffle <- Seq("jvm", "native")) {
        benchmark.addCase(s"Comet ($shuffle Shuffle)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_MODE.key -> shuffle) {
            spark
              .sql(sql)
              .repartition(partitionNum)
              .noop()
          }
        }
      }

      benchmark.run()
    }
  }

  /**
   * Nested hash partitioning keys, which native shuffle admits only when
   * `spark.comet.shuffle.native.partitioning.hash.nested.enabled` is on.
   *
   * Primitive arrays use the typed element path; arrays of structs exercise recursive hashing.
   * Map cases include a singleton control and variable cardinalities in both input key orders,
   * covering normalization as well as the specialized scalar key/value hash loop.
   *
   * Compare native with Comet JVM shuffle to evaluate the default `auto` mode with nested hashing
   * disabled. The all-Spark arm also changes scan and projection execution. These are end-to-end
   * shuffle measurements, not isolated hash-kernel timings.
   */
  def shuffleNestedHashKeyBenchmark(
      name: String,
      keyExpr: String,
      values: Int,
      partitionNum: Int): Unit = {
    val benchmark =
      microBenchmark(s"Nested hash key: $name ($partitionNum Partition)", values)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        // `tbl`'s `value` spans the full Long range, so a direct cast to INT overflows under ANSI
        // mode. `pmod` keeps the key varied (a constant would hash every row alike, which would
        // not measure partitioning at all) while staying in range.
        prepareTable(dir, spark.sql(s"SELECT CAST(pmod(value, 1000000) AS INT) AS c1 FROM $tbl"))
        val query = s"SELECT $keyExpr AS k, c1 FROM parquetV1Table"

        benchmark.addCase("Spark") { _ =>
          withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
            spark.sql(query).repartition(partitionNum, Column("k")).noop()
          }
        }

        benchmark.addCase("Comet (Spark Shuffle)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_ENABLED.key -> "false") {
            spark.sql(query).repartition(partitionNum, Column("k")).noop()
          }
        }

        benchmark.addCase("Comet (JVM Shuffle)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_MODE.key -> "jvm") {
            spark.sql(query).repartition(partitionNum, Column("k")).noop()
          }
        }

        def containsMap(dataType: DataType): Boolean = dataType match {
          case _: MapType => true
          case ArrayType(elementType, _) => containsMap(elementType)
          case StructType(fields) => fields.exists(f => containsMap(f.dataType))
          case _ => false
        }

        // Spark 3.x does not normalize map partitioning keys for native hashing.
        if (containsMap(spark.sql(query).schema("k").dataType) && !isSpark40Plus) {
          val message = s"Skipping native shuffle for $name: map keys require Spark 4.0+"
          benchmark.out.println(message)
        } else {
          val nativeConfigs = Seq(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_MODE.key -> "native",
            CometConf.COMET_SHUFFLE_NATIVE_HASH_PARTITIONING_NESTED_ENABLED.key -> "true")
          // Check outside the timer: enabling the gate alone does not prove native admission.
          withSQLConf(nativeConfigs: _*) {
            val plan =
              spark.sql(query).repartition(partitionNum, Column("k")).queryExecution.executedPlan
            val nativeExchanges = collect(plan) {
              case exchange: CometShuffleExchangeExec
                  if exchange.shuffleType == CometNativeShuffle =>
                exchange
            }
            require(
              nativeExchanges.size == 1,
              s"Expected one native shuffle for $name, found ${nativeExchanges.size}:\n$plan")
            benchmark.out.println(
              s"Verified native exchange for $name ($partitionNum partitions):")
            benchmark.out.println(plan.treeString)
          }
          benchmark.addCase("Comet (Native Shuffle)") { _ =>
            withSQLConf(nativeConfigs: _*) {
              spark.sql(query).repartition(partitionNum, Column("k")).noop()
            }
          }
        }

        benchmark.run()
      }
    }
  }

  private def runNestedHashKeyBenchmarks(): Unit = {
    runBenchmarkWithTable("Nested hash partitioning key", 1024 * 1024 * 1) { v =>
      val shapes = Seq(
        "struct<int, string>" -> "named_struct('a', c1, 'b', CAST(c1 AS STRING))",
        "array<int>" -> "ARRAY_REPEAT(c1, 10)",
        "struct<array<int>, string>" ->
          "named_struct('a', ARRAY_REPEAT(c1, 10), 'b', CAST(c1 AS STRING))",
        "array<struct<int, string>>" ->
          "ARRAY_REPEAT(named_struct('a', c1, 'b', CAST(c1 AS STRING)), 10)",
        "struct<map<string, int>, int>" ->
          "named_struct('m', MAP(CAST(c1 AS STRING), c1), 'i', c1)")
      // Distinct keys, variable entry counts, and opposite input orders exercise map sorting.
      val mapShapes = for {
        maxEntries <- Seq(10, 50)
        reverse <- Seq(false, true)
      } yield {
        val indices = s"sequence(1, 2 + pmod(c1, ${maxEntries - 1}))"
        val ordered = if (reverse) s"reverse($indices)" else indices
        val map = s"map_from_arrays(transform($ordered, x -> CAST(c1 + x AS STRING)), " +
          s"transform($ordered, x -> c1 + x))"
        val order = if (reverse) "reversed" else "forward"
        s"struct<map<string, int>, int> (2-$maxEntries entries, $order)" ->
          s"named_struct('m', $map, 'i', c1)"
      }
      (shapes ++ mapShapes).foreach { case (name, keyExpr) =>
        Seq(5, manyPartitions).foreach { partitionNum =>
          shuffleNestedHashKeyBenchmark(name, keyExpr, v, partitionNum)
        }
      }
    }
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    if (mainArgs.contains("--nested-hash-only")) {
      runNestedHashKeyBenchmarks()
      return
    }

    // nested type shuffle
    val numRows = 1000
    for (maxDepth <- Seq(2, 6)) {
      val filename =
        createDeeplyNestedParquetFile(numRows, maxDepth)
      try {
        for (partitionNum <- Seq(5, manyPartitions)) {
          val name = s"maxDepth=$maxDepth, partitionNum=$partitionNum"
          shuffleDeeplyNestedBenchmark(name, filename, numRows, partitionNum)
        }
      } finally {
        new java.io.File(filename).delete()
      }
    }

    runNestedHashKeyBenchmarks()

    runBenchmarkWithTable("Shuffle on array", 1024 * 1024 * 1) { v =>
      benchmarkTypes.foreach { dataType =>
        Seq(5, manyPartitions).foreach { partitionNum =>
          shuffleArrayBenchmark(v, dataType, partitionNum)
        }
      }
    }

    runBenchmarkWithTable("Shuffle on struct", 1024 * 1024 * 1) { v =>
      benchmarkTypes.foreach { dataType =>
        Seq(5, manyPartitions).foreach { partitionNum =>
          shuffleStructBenchmark(v, dataType, partitionNum)
        }
      }
    }

    runBenchmarkWithTable("Dictionary Shuffle", 1024 * 1024 * 1) { v =>
      Seq(BinaryType, StringType).foreach { dataType =>
        Seq(5, manyPartitions).foreach { partitionNum =>
          shuffleDictionaryBenchmark(v, dataType, partitionNum)
        }
      }
    }

    runBenchmarkWithTable("Wide Shuffle (10 cols)", 1024 * 1024 * 1) { v =>
      benchmarkTypes
        .foreach { dataType =>
          shuffleWideBenchmark(v, dataType, 10, 5)
        }
    }

    runBenchmarkWithTable("Wide Shuffle (20 cols)", 1024 * 1024 * 1) { v =>
      benchmarkTypes
        .foreach { dataType =>
          shuffleWideBenchmark(v, dataType, 20, 5)
        }
    }

    runBenchmarkWithTable("Wide Shuffle (10 cols)", 1024 * 1024 * 1) { v =>
      benchmarkTypes
        .foreach { dataType =>
          shuffleWideBenchmark(v, dataType, 10, manyPartitions)
        }
    }

    runBenchmarkWithTable("Wide Shuffle (20 cols)", 1024 * 1024 * 1) { v =>
      benchmarkTypes
        .foreach { dataType =>
          shuffleWideBenchmark(v, dataType, 20, manyPartitions)
        }
    }

    runBenchmarkWithTable("Wide Range Partition Shuffle (10 cols)", 1024 * 1024 * 1) { v =>
      benchmarkTypes
        .foreach { dataType =>
          shuffleRangePartitionBenchmark(v, dataType, 10, 5)
        }
    }

    runBenchmarkWithTable("Wide Range Partition Shuffle (20 cols)", 1024 * 1024 * 1) { v =>
      benchmarkTypes
        .foreach { dataType =>
          shuffleRangePartitionBenchmark(v, dataType, 20, 5)
        }
    }

    runBenchmarkWithTable("Wide Range Partition Shuffle (10 cols)", 1024 * 1024 * 1) { v =>
      benchmarkTypes
        .foreach { dataType =>
          shuffleRangePartitionBenchmark(v, dataType, 10, manyPartitions)
        }
    }

    runBenchmarkWithTable("Wide Range Partition Shuffle (20 cols)", 1024 * 1024 * 1) { v =>
      benchmarkTypes
        .foreach { dataType =>
          shuffleRangePartitionBenchmark(v, dataType, 20, manyPartitions)
        }
    }
  }

  private def createDeeplyNestedParquetFile(numRows: Int, maxDepth: Int): String = {
    val r = new Random(42)
    val options =
      SchemaGenOptions(generateArray = true, generateStruct = true, generateMap = true)
    val schema = FuzzDataGenerator.generateNestedSchema(r, 100, maxDepth - 1, maxDepth, options)
    val tempDir = System.getProperty("java.io.tmpdir")
    val filename = s"$tempDir/CometShuffleBenchmark_${System.currentTimeMillis()}.parquet"
    withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      val dataGenOptions = DataGenOptions(
        generateNegativeZero = false,
        // override base date due to known issues with experimental scans
        baseDate =
          new SimpleDateFormat("YYYY-MM-DD hh:mm:ss").parse("2024-05-25 12:34:56").getTime)
      val df =
        FuzzDataGenerator.generateDataFrame(r, spark, schema, numRows, dataGenOptions)
      df.write.mode(SaveMode.Overwrite).parquet(filename)
    }
    filename
  }
}
