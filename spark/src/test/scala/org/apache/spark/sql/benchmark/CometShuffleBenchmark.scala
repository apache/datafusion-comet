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

import scala.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions
import org.apache.comet.testing.{DataGenOptions, FuzzDataGenerator, SchemaGenOptions}

// spotless:off
/**
 * Benchmark to measure Comet shuffle performance. To run this benchmark:
 * `SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometShuffleBenchmark`
 * Results will be written to "spark/benchmarks/CometShuffleBenchmark-**results.txt".
 */
// spotless:on
object CometShuffleBenchmark extends CometBenchmarkBase {

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
      .set("spark.comet.columnar.shuffle.async.thread.num", "7")
      .set("spark.comet.columnar.shuffle.spill.threshold", "30000")
      .set("spark.comet.memoryOverhead", "10g")

    val sparkSession = SparkSession.builder
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
      new Benchmark(
        s"SQL ${dataType.sql} shuffle on array ($partitionNum Partition)",
        values,
        output = output)

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
            CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "false") {
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
            CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_PREFER_DICTIONARY_RATIO.key -> "1.0",
            CometConf.COMET_SHUFFLE_MODE.key -> "jvm",
            CometConf.COMET_COLUMNAR_SHUFFLE_ASYNC_ENABLED.key -> "false") {
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
      new Benchmark(
        s"SQL ${dataType.sql} shuffle on struct ($partitionNum Partition)",
        values,
        output = output)

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
            CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "false") {
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
            CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_PREFER_DICTIONARY_RATIO.key -> "1.0",
            CometConf.COMET_SHUFFLE_MODE.key -> "jvm",
            CometConf.COMET_COLUMNAR_SHUFFLE_ASYNC_ENABLED.key -> "false") {
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
      new Benchmark(
        s"SQL ${dataType.sql} Dictionary Shuffle($partitionNum Partition)",
        values,
        output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(
          dir,
          spark.sql(s"SELECT REPEAT(CAST(1 AS ${dataType.sql}), 100) AS c1 FROM $tbl"))

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
            CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "false") {
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
            CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_PREFER_DICTIONARY_RATIO.key -> "1.0",
            CometConf.COMET_SHUFFLE_MODE.key -> "jvm",
            CometConf.COMET_COLUMNAR_SHUFFLE_ASYNC_ENABLED.key -> "false") {
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
            CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_PREFER_DICTIONARY_RATIO.key -> "2.0",
            CometConf.COMET_SHUFFLE_MODE.key -> "jvm",
            CometConf.COMET_COLUMNAR_SHUFFLE_ASYNC_ENABLED.key -> "false") {
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
            CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_PREFER_DICTIONARY_RATIO.key -> "1000000000.0",
            CometConf.COMET_SHUFFLE_MODE.key -> "jvm",
            CometConf.COMET_COLUMNAR_SHUFFLE_ASYNC_ENABLED.key -> "false") {
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

  def shuffleBenchmark(
      values: Int,
      dataType: DataType,
      random: Boolean,
      partitionNum: Int): Unit = {
    val randomTitle = if (random) {
      "With Random"
    } else {
      ""
    }
    val benchmark =
      new Benchmark(
        s"SQL Single ${dataType.sql} Shuffle($partitionNum Partition) $randomTitle",
        values,
        output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        if (random) {
          prepareTable(
            dir,
            spark.sql(
              s"SELECT CAST(CAST(RAND(1) * 100 AS INTEGER) AS ${dataType.sql}) AS c1 FROM $tbl"))
        } else {
          prepareTable(dir, spark.sql(s"SELECT CAST(1 AS ${dataType.sql}) AS c1 FROM $tbl"))
        }

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
            CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "false") {
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
            CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_MODE.key -> "jvm",
            CometConf.COMET_COLUMNAR_SHUFFLE_ASYNC_ENABLED.key -> "false") {
            spark
              .sql("select c1 from parquetV1Table")
              .repartition(partitionNum, Column("c1"))
              .noop()
          }
        }

        benchmark.addCase("SQL Parquet - Comet (Comet Async JVM Shuffle)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
            CometConf.COMET_SHUFFLE_MODE.key -> "jvm",
            CometConf.COMET_COLUMNAR_SHUFFLE_ASYNC_ENABLED.key -> "true") {
            spark
              .sql("select c1 from parquetV1Table")
              .repartition(partitionNum, Column("c1"))
              .noop()
          }
        }

        benchmark.addCase("SQL Parquet - Comet (Comet Shuffle)") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true") {
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
      new Benchmark(
        s"SQL Wide ($width cols) ${dataType.sql} Shuffle($partitionNum Partition)",
        values,
        output = output)

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
            CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "false") {
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
            CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
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
            CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
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
      new Benchmark(
        s"SQL Wide ($width cols) ${dataType.sql} Range Partition Shuffle($partitionNum Partition)",
        values,
        output = output)

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
            CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "false") {
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
            CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
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
            CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
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
      new Benchmark(s"Shuffle with nested schema ($name)", numRows, output = output)
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
          CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "false") {
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
            CometConf.COMET_EXEC_SHUFFLE_ENABLED.key -> "true",
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

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {

    // nested type shuffle
    val numRows = 1000
    for (maxDepth <- Seq(2, 6)) {
      val filename =
        createDeeplyNestedParquetFile(numRows, maxDepth)
      try {
        for (partitionNum <- Seq(5, 201)) {
          val name = s"maxDepth=$maxDepth, partitionNum=$partitionNum"
          shuffleDeeplyNestedBenchmark(name, filename, numRows, partitionNum)
        }
      } finally {
        new java.io.File(filename).delete()
      }
    }

    runBenchmarkWithTable("Shuffle on array", 1024 * 1024 * 1) { v =>
      Seq(
        BooleanType,
        ByteType,
        ShortType,
        IntegerType,
        LongType,
        FloatType,
        DoubleType,
        StringType,
        DecimalType(10, 0)).foreach { dataType =>
        Seq(5, 201).foreach { partitionNum =>
          shuffleArrayBenchmark(v, dataType, partitionNum)
        }
      }
    }

    runBenchmarkWithTable("Shuffle on struct", 1024 * 1024 * 100) { v =>
      Seq(
        BooleanType,
        ByteType,
        ShortType,
        IntegerType,
        LongType,
        FloatType,
        DoubleType,
        StringType,
        DecimalType(10, 0)).foreach { dataType =>
        Seq(5, 201).foreach { partitionNum =>
          shuffleStructBenchmark(v, dataType, partitionNum)
        }
      }
    }

    runBenchmarkWithTable("Dictionary Shuffle", 1024 * 1024 * 100) { v =>
      Seq(BinaryType, StringType).foreach { dataType =>
        Seq(5, 201).foreach { partitionNum =>
          shuffleDictionaryBenchmark(v, dataType, partitionNum)
        }
      }
    }

    runBenchmarkWithTable("Shuffle", 1024 * 1024 * 10) { v =>
      Seq(
        BooleanType,
        ByteType,
        ShortType,
        IntegerType,
        LongType,
        FloatType,
        DoubleType,
        StringType,
        DecimalType(10, 0))
        .foreach { dataType =>
          shuffleBenchmark(v, dataType, false, 5)
        }
    }

    runBenchmarkWithTable("Shuffle", 1024 * 1024 * 10) { v =>
      Seq(
        BooleanType,
        ByteType,
        ShortType,
        IntegerType,
        LongType,
        FloatType,
        DoubleType,
        StringType,
        DecimalType(10, 0))
        .foreach { dataType =>
          shuffleBenchmark(v, dataType, false, 201)
        }
    }

    runBenchmarkWithTable("Shuffle with random values", 1024 * 1024 * 10) { v =>
      Seq(
        BooleanType,
        ByteType,
        ShortType,
        IntegerType,
        LongType,
        FloatType,
        DoubleType,
        StringType,
        DecimalType(10, 0))
        .foreach { dataType =>
          shuffleBenchmark(v, dataType, true, 5)
        }
    }

    runBenchmarkWithTable("Shuffle with random values", 1024 * 1024 * 10) { v =>
      Seq(
        BooleanType,
        ByteType,
        ShortType,
        IntegerType,
        LongType,
        FloatType,
        DoubleType,
        StringType,
        DecimalType(10, 0))
        .foreach { dataType =>
          shuffleBenchmark(v, dataType, true, 201)
        }
    }

    runBenchmarkWithTable("Wide Shuffle (10 cols)", 1024 * 1024 * 10) { v =>
      Seq(
        BooleanType,
        ByteType,
        ShortType,
        IntegerType,
        LongType,
        FloatType,
        DoubleType,
        StringType,
        DecimalType(10, 0))
        .foreach { dataType =>
          shuffleWideBenchmark(v, dataType, 10, 5)
        }
    }

    runBenchmarkWithTable("Wide Shuffle (20 cols)", 1024 * 1024 * 10) { v =>
      Seq(
        BooleanType,
        ByteType,
        ShortType,
        IntegerType,
        LongType,
        FloatType,
        DoubleType,
        StringType,
        DecimalType(10, 0))
        .foreach { dataType =>
          shuffleWideBenchmark(v, dataType, 20, 5)
        }
    }

    runBenchmarkWithTable("Wide Shuffle (10 cols)", 1024 * 1024 * 10) { v =>
      Seq(
        BooleanType,
        ByteType,
        ShortType,
        IntegerType,
        LongType,
        FloatType,
        DoubleType,
        StringType,
        DecimalType(10, 0))
        .foreach { dataType =>
          shuffleWideBenchmark(v, dataType, 10, 201)
        }
    }

    runBenchmarkWithTable("Wide Shuffle (20 cols)", 1024 * 1024 * 10) { v =>
      Seq(
        BooleanType,
        ByteType,
        ShortType,
        IntegerType,
        LongType,
        FloatType,
        DoubleType,
        StringType,
        DecimalType(10, 0))
        .foreach { dataType =>
          shuffleWideBenchmark(v, dataType, 20, 201)
        }
    }

    runBenchmarkWithTable("Wide Range Partition Shuffle (10 cols)", 1024 * 1024 * 10) { v =>
      Seq(
        BooleanType,
        ByteType,
        ShortType,
        IntegerType,
        LongType,
        FloatType,
        DoubleType,
        StringType,
        DecimalType(10, 0))
        .foreach { dataType =>
          shuffleRangePartitionBenchmark(v, dataType, 10, 5)
        }
    }

    runBenchmarkWithTable("Wide Range Partition Shuffle (20 cols)", 1024 * 1024 * 10) { v =>
      Seq(
        BooleanType,
        ByteType,
        ShortType,
        IntegerType,
        LongType,
        FloatType,
        DoubleType,
        StringType,
        DecimalType(10, 0))
        .foreach { dataType =>
          shuffleRangePartitionBenchmark(v, dataType, 20, 5)
        }
    }

    runBenchmarkWithTable("Wide Range Partition Shuffle (10 cols)", 1024 * 1024 * 10) { v =>
      Seq(
        BooleanType,
        ByteType,
        ShortType,
        IntegerType,
        LongType,
        FloatType,
        DoubleType,
        StringType,
        DecimalType(10, 0))
        .foreach { dataType =>
          shuffleRangePartitionBenchmark(v, dataType, 10, 201)
        }
    }

    runBenchmarkWithTable("Wide Range Partition Shuffle (20 cols)", 1024 * 1024 * 10) { v =>
      Seq(
        BooleanType,
        ByteType,
        ShortType,
        IntegerType,
        LongType,
        FloatType,
        DoubleType,
        StringType,
        DecimalType(10, 0))
        .foreach { dataType =>
          shuffleRangePartitionBenchmark(v, dataType, 20, 201)
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
