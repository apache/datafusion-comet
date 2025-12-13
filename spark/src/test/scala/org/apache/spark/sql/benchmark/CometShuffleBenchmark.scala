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
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions
import org.apache.comet.testing.{DataGenOptions, FuzzDataGenerator}

import java.text.SimpleDateFormat
import java.util.concurrent.atomic.AtomicLong
import scala.util.Random

/**
 * Benchmark to measure Comet shuffle performance. To run this benchmark:
 * `SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometShuffleBenchmark`
 * Results will be written to
 * "spark/benchmarks/CometShuffleBenchmark-**results.txt".
 */
object CometShuffleBenchmark extends CometBenchmarkBase {

  private val counter = new AtomicLong()

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

        benchmark.addCase("SQL Parquet - Comet (Comet Arrow Shuffle)") { _ =>
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

        benchmark.addCase("SQL Parquet - Comet (Comet Arrow Shuffle)") { _ =>
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

        benchmark.addCase("SQL Parquet - Comet (Comet Arrow Shuffle)") { _ =>
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

        benchmark.addCase("SQL Parquet - Comet (Comet Arrow Shuffle + Prefer Dictionary)") { _ =>
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

        benchmark.addCase("SQL Parquet - Comet (Comet Arrow Shuffle + Fallback to string)") { _ =>
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

        benchmark.addCase("SQL Parquet - Comet (Comet Arrow Shuffle)") { _ =>
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

        benchmark.addCase("SQL Parquet - Comet (Comet Async Arrow Shuffle)") { _ =>
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

        benchmark.addCase("SQL Parquet - Comet (Comet Arrow Shuffle)") { _ =>
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

        benchmark.addCase("SQL Parquet - Comet (Comet Shuffle)") { _ =>
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

        benchmark.addCase("SQL Parquet - Comet (Comet Arrow Shuffle)") { _ =>
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

        benchmark.addCase("SQL Parquet - Comet (Comet Shuffle)") { _ =>
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

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
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

  private def createDeeplyNestedParquetFile(maxDepth: Int): String = {
    val tempDir = System.getProperty("java.io.tmpdir")
    val filename = s"$tempDir/CometFuzzTestSuite_${System.currentTimeMillis()}.parquet"
    val r = new Random(42)
    withSQLConf(
      CometConf.COMET_ENABLED.key -> "false") {
      val dataGenOptions = DataGenOptions(
        generateNegativeZero = false,
        // override base date due to known issues with experimental scans
        baseDate =
          new SimpleDateFormat("YYYY-MM-DD hh:mm:ss").parse("2024-05-25 12:34:56").getTime)

      val schema = StructType(Range(0, 10).map(_ => genField(r, 0, maxDepth)))

      val df = FuzzDataGenerator.generateDataFrame(new Random(42), spark, schema, 100, dataGenOptions)
      println(df.schema)
      df.write.mode(SaveMode.Overwrite).parquet(filename)
    }
    filename
  }

  private def generateFieldName() = {
    s"c_${counter.incrementAndGet()}"
  }

  private def genField(r: Random, depth: Int, maxDepth: Int): StructField = {
    val name = generateFieldName()
    r.nextInt(10) match {
      case 0 if depth < maxDepth =>
        // array
        val element = genField(r, depth+1, maxDepth)
        StructField(name, DataTypes.createArrayType(element.dataType, true))
      case 1 if depth < maxDepth =>
        // struct
        val fields = Range(0, r.nextInt(10)).map(_ => genField(r, depth+1, maxDepth)).toArray
        StructField(name, DataTypes.createStructType(fields))
      case 2 =>
        StructField(name, DataTypes.LongType)
      case 3 =>
        StructField(name, DataTypes.createDecimalType(10, 2))
      case _ =>
        StructField(name, DataTypes.StringType)
    }

  }
}
