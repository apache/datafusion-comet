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

import scala.jdk.CollectionConverters._
import scala.util.Random

import org.apache.hadoop.fs.Path
import org.apache.spark.TestUtils
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.parquet.VectorizedParquetRecordReader
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnVector

import org.apache.comet.{CometConf, WithHdfsCluster}
import org.apache.comet.CometConf.{SCAN_NATIVE_COMET, SCAN_NATIVE_DATAFUSION, SCAN_NATIVE_ICEBERG_COMPAT}
import org.apache.comet.parquet.BatchReader

/**
 * Benchmark to measure Comet read performance. To run this benchmark:
 * `SPARK_GENERATE_BENCHMARK_FILES=1 make
 * benchmark-org.apache.spark.sql.benchmark.CometReadBenchmark` Results will be written to
 * "spark/benchmarks/CometReadBenchmark-**results.txt".
 */
class CometReadBaseBenchmark extends CometBenchmarkBase {

  def numericScanBenchmark(values: Int, dataType: DataType): Unit = {
    // Benchmarks running through spark sql.
    val sqlBenchmark =
      new Benchmark(s"SQL Single ${dataType.sql} Column Scan", values, output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(dir, spark.sql(s"SELECT CAST(value as ${dataType.sql}) id FROM $tbl"))

        val query = dataType match {
          case BooleanType => "sum(cast(id as bigint))"
          case _ => "sum(id)"
        }

        sqlBenchmark.addCase("SQL Parquet - Spark") { _ =>
          spark.sql(s"select $query from parquetV1Table").noop()
        }

        sqlBenchmark.addCase("SQL Parquet - Comet") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_NATIVE_SCAN_IMPL.key -> SCAN_NATIVE_COMET) {
            spark.sql(s"select $query from parquetV1Table").noop()
          }
        }

        sqlBenchmark.addCase("SQL Parquet - Comet Native DataFusion") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_NATIVE_SCAN_IMPL.key -> SCAN_NATIVE_DATAFUSION) {
            spark.sql(s"select $query from parquetV1Table").noop()
          }
        }

        sqlBenchmark.addCase("SQL Parquet - Comet Native Iceberg Compat") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_NATIVE_SCAN_IMPL.key -> SCAN_NATIVE_ICEBERG_COMPAT) {
            spark.sql(s"select $query from parquetV1Table").noop()
          }
        }

        sqlBenchmark.run()
      }
    }
  }

  def decimalScanBenchmark(values: Int, precision: Int, scale: Int): Unit = {
    val sqlBenchmark = new Benchmark(
      s"SQL Single Decimal(precision: $precision, scale: $scale) Column Scan",
      values,
      output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(
          dir,
          spark.sql(
            s"SELECT CAST(value / 10000000.0 as DECIMAL($precision, $scale)) " +
              s"id FROM $tbl"))

        sqlBenchmark.addCase("SQL Parquet - Spark") { _ =>
          spark.sql("select sum(id) from parquetV1Table").noop()
        }

        sqlBenchmark.addCase("SQL Parquet - Comet") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_NATIVE_SCAN_IMPL.key -> SCAN_NATIVE_COMET) {
            spark.sql("select sum(id) from parquetV1Table").noop()
          }
        }

        sqlBenchmark.addCase("SQL Parquet - Comet Native DataFusion") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_NATIVE_SCAN_IMPL.key -> SCAN_NATIVE_DATAFUSION) {
            spark.sql("select sum(id) from parquetV1Table").noop()
          }
        }

        sqlBenchmark.addCase("SQL Parquet - Comet Native Iceberg Compat") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_NATIVE_SCAN_IMPL.key -> SCAN_NATIVE_ICEBERG_COMPAT) {
            spark.sql("select sum(id) from parquetV1Table").noop()
          }
        }

        sqlBenchmark.run()
      }
    }
  }

  def readerBenchmark(values: Int, dataType: DataType): Unit = {
    val sqlBenchmark =
      new Benchmark(s"Parquet reader benchmark for $dataType", values, output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(dir, spark.sql(s"SELECT CAST(value as ${dataType.sql}) id FROM $tbl"))

        val enableOffHeapColumnVector = spark.sessionState.conf.offHeapColumnVectorEnabled
        val vectorizedReaderBatchSize = CometConf.COMET_BATCH_SIZE.get(spark.sessionState.conf)

        var longSum = 0L
        var doubleSum = 0.0
        val aggregateValue: (ColumnVector, Int) => Unit = dataType match {
          case BooleanType => (col: ColumnVector, i: Int) => if (col.getBoolean(i)) longSum += 1
          case ByteType => (col: ColumnVector, i: Int) => longSum += col.getByte(i)
          case ShortType => (col: ColumnVector, i: Int) => longSum += col.getShort(i)
          case IntegerType => (col: ColumnVector, i: Int) => longSum += col.getInt(i)
          case LongType => (col: ColumnVector, i: Int) => longSum += col.getLong(i)
          case FloatType => (col: ColumnVector, i: Int) => doubleSum += col.getFloat(i)
          case DoubleType => (col: ColumnVector, i: Int) => doubleSum += col.getDouble(i)
          case StringType =>
            (col: ColumnVector, i: Int) => longSum += col.getUTF8String(i).toLongExact
        }

        val files = TestUtils.listDirectory(new File(dir, "parquetV1"))

        sqlBenchmark.addCase("ParquetReader Spark") { _ =>
          files.map(_.asInstanceOf[String]).foreach { p =>
            val reader = new VectorizedParquetRecordReader(
              enableOffHeapColumnVector,
              vectorizedReaderBatchSize)
            try {
              reader.initialize(p, ("id" :: Nil).asJava)
              val batch = reader.resultBatch()
              val column = batch.column(0)
              var totalNumRows = 0
              while (reader.nextBatch()) {
                val numRows = batch.numRows()
                var i = 0
                while (i < numRows) {
                  if (!column.isNullAt(i)) aggregateValue(column, i)
                  i += 1
                }
                totalNumRows += batch.numRows()
              }
            } finally {
              reader.close()
            }
          }
        }

        sqlBenchmark.addCase("ParquetReader Comet") { _ =>
          files.map(_.asInstanceOf[String]).foreach { p =>
            val reader = new BatchReader(p, vectorizedReaderBatchSize)
            reader.init()
            try {
              var totalNumRows = 0
              while (reader.nextBatch()) {
                val batch = reader.currentBatch()
                val column = batch.column(0)
                val numRows = batch.numRows()
                var i = 0
                while (i < numRows) {
                  if (!column.isNullAt(i)) aggregateValue(column, i)
                  i += 1
                }
                totalNumRows += batch.numRows()
              }
            } finally {
              reader.close()
            }
          }
        }

        sqlBenchmark.run()
      }
    }
  }

  def numericFilterScanBenchmark(values: Int, fractionOfZeros: Double): Unit = {
    val percentageOfZeros = fractionOfZeros * 100
    val benchmark =
      new Benchmark(s"Numeric Filter Scan ($percentageOfZeros% zeros)", values, output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table", "parquetV2Table") {
        prepareTable(
          dir,
          spark.sql(
            s"SELECT IF(RAND(1) < $fractionOfZeros, -1, value) AS c1, value AS c2 FROM " +
              s"$tbl"))

        benchmark.addCase("SQL Parquet - Spark") { _ =>
          spark.sql("select sum(c2) from parquetV1Table where c1 + 1 > 0").noop()
        }

        benchmark.addCase("SQL Parquet - Comet") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_NATIVE_SCAN_IMPL.key -> SCAN_NATIVE_COMET) {
            spark.sql("select sum(c2) from parquetV1Table where c1 + 1 > 0").noop()
          }
        }

        benchmark.addCase("SQL Parquet - Comet Native DataFusion") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_NATIVE_SCAN_IMPL.key -> SCAN_NATIVE_DATAFUSION) {
            spark.sql("select sum(c2) from parquetV1Table where c1 + 1 > 0").noop()
          }
        }

        benchmark.addCase("SQL Parquet - Comet Native Iceberg Compat") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_NATIVE_SCAN_IMPL.key -> SCAN_NATIVE_ICEBERG_COMPAT) {
            spark.sql("select sum(c2) from parquetV1Table where c1 + 1 > 0").noop()
          }
        }

        benchmark.run()
      }
    }
  }

  def stringWithDictionaryScanBenchmark(values: Int): Unit = {
    val sqlBenchmark =
      new Benchmark("String Scan with Dictionary Encoding", values, output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table", "parquetV2Table") {
        prepareTable(
          dir,
          spark.sql(s"""
                       |WITH tmp
                       |  AS (SELECT RAND() r FROM $tbl)
                       |SELECT
                       |  CASE
                       |    WHEN r < 0.2 THEN 'aaa'
                       |    WHEN r < 0.4 THEN 'bbb'
                       |    WHEN r < 0.6 THEN 'ccc'
                       |    WHEN r < 0.8 THEN 'ddd'
                       |    ELSE 'eee'
                       |  END
                       |AS id
                       |FROM tmp
                       |""".stripMargin))

        sqlBenchmark.addCase("SQL Parquet - Spark") { _ =>
          spark.sql("select sum(length(id)) from parquetV1Table").noop()
        }

        sqlBenchmark.addCase("SQL Parquet - Comet") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_NATIVE_SCAN_IMPL.key -> SCAN_NATIVE_COMET) {
            spark.sql("select sum(length(id)) from parquetV1Table").noop()
          }
        }

        sqlBenchmark.addCase("SQL Parquet - Comet Native DataFusion") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_NATIVE_SCAN_IMPL.key -> SCAN_NATIVE_DATAFUSION) {
            spark.sql("select sum(length(id)) from parquetV1Table").noop()
          }
        }

        sqlBenchmark.addCase("SQL Parquet - Comet Native Iceberg Compat") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_NATIVE_SCAN_IMPL.key -> SCAN_NATIVE_ICEBERG_COMPAT) {
            spark.sql("select sum(length(id)) from parquetV1Table").noop()
          }
        }

        sqlBenchmark.run()
      }
    }
  }

  def stringWithNullsScanBenchmark(values: Int, fractionOfNulls: Double): Unit = {
    val percentageOfNulls = fractionOfNulls * 100
    val benchmark =
      new Benchmark(s"String with Nulls Scan ($percentageOfNulls%)", values, output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(
          dir,
          spark.sql(
            s"SELECT IF(RAND(1) < $fractionOfNulls, NULL, CAST(value as STRING)) AS c1, " +
              s"IF(RAND(2) < $fractionOfNulls, NULL, CAST(value as STRING)) AS c2 FROM $tbl"))

        benchmark.addCase("SQL Parquet - Spark") { _ =>
          spark
            .sql("select sum(length(c2)) from parquetV1Table where c1 is " +
              "not NULL and c2 is not NULL")
            .noop()
        }

        benchmark.addCase("SQL Parquet - Comet") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_NATIVE_SCAN_IMPL.key -> SCAN_NATIVE_COMET) {
            spark
              .sql("select sum(length(c2)) from parquetV1Table where c1 is " +
                "not NULL and c2 is not NULL")
              .noop()
          }
        }

        benchmark.addCase("SQL Parquet - Comet Native DataFusion") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_NATIVE_SCAN_IMPL.key -> SCAN_NATIVE_DATAFUSION) {
            spark
              .sql("select sum(length(c2)) from parquetV1Table where c1 is " +
                "not NULL and c2 is not NULL")
              .noop()
          }
        }

        benchmark.addCase("SQL Parquet - Comet Native Iceberg Compat") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_NATIVE_SCAN_IMPL.key -> SCAN_NATIVE_ICEBERG_COMPAT) {
            spark
              .sql("select sum(length(c2)) from parquetV1Table where c1 is " +
                "not NULL and c2 is not NULL")
              .noop()
          }
        }

        benchmark.run()
      }
    }
  }

  def columnsBenchmark(values: Int, width: Int): Unit = {
    val benchmark =
      new Benchmark(s"Single Column Scan from $width columns", values, output = output)

    withTempPath { dir =>
      withTempTable("t1", "parquetV1Table") {
        val middle = width / 2
        val selectExpr = (1 to width).map(i => s"value as c$i")
        spark.table(tbl).selectExpr(selectExpr: _*).createOrReplaceTempView("t1")

        prepareTable(dir, spark.sql("SELECT * FROM t1"))

        benchmark.addCase("SQL Parquet - Spark") { _ =>
          spark.sql(s"SELECT sum(c$middle) FROM parquetV1Table").noop()
        }

        benchmark.addCase("SQL Parquet - Comet") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_NATIVE_SCAN_IMPL.key -> SCAN_NATIVE_COMET) {
            spark.sql(s"SELECT sum(c$middle) FROM parquetV1Table").noop()
          }
        }

        benchmark.addCase("SQL Parquet - Comet Native DataFusion") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_NATIVE_SCAN_IMPL.key -> SCAN_NATIVE_DATAFUSION) {
            spark.sql(s"SELECT sum(c$middle) FROM parquetV1Table").noop()
          }
        }

        benchmark.addCase("SQL Parquet - Comet Native Iceberg Compat") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_NATIVE_SCAN_IMPL.key -> SCAN_NATIVE_ICEBERG_COMPAT) {
            spark.sql(s"SELECT sum(c$middle) FROM parquetV1Table").noop()
          }
        }

        benchmark.run()
      }
    }
  }

  def largeStringFilterScanBenchmark(values: Int, fractionOfZeros: Double): Unit = {
    val percentageOfZeros = fractionOfZeros * 100
    val benchmark =
      new Benchmark(
        s"Large String Filter Scan ($percentageOfZeros% zeros)",
        values,
        output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(
          dir,
          spark.sql(
            s"SELECT IF(RAND(1) < $fractionOfZeros, -1, value) AS c1, " +
              s"REPEAT(CAST(value AS STRING), 100) AS c2 FROM $tbl"))

        benchmark.addCase("SQL Parquet - Spark") { _ =>
          spark.sql("SELECT * FROM parquetV1Table WHERE c1 + 1 > 0").noop()
        }

        benchmark.addCase("SQL Parquet - Comet") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_NATIVE_SCAN_IMPL.key -> SCAN_NATIVE_COMET) {
            spark.sql("SELECT * FROM parquetV1Table WHERE c1 + 1 > 0").noop()
          }
        }

        benchmark.addCase("SQL Parquet - Comet Native DataFusion") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_NATIVE_SCAN_IMPL.key -> SCAN_NATIVE_DATAFUSION) {
            spark.sql("SELECT * FROM parquetV1Table WHERE c1 + 1 > 0").noop()
          }
        }

        benchmark.addCase("SQL Parquet - Comet Native Iceberg Compat") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_NATIVE_SCAN_IMPL.key -> SCAN_NATIVE_ICEBERG_COMPAT) {
            spark.sql("SELECT * FROM parquetV1Table WHERE c1 + 1 > 0").noop()
          }
        }

        benchmark.run()
      }
    }
  }

  def sortedLgStrFilterScanBenchmark(values: Int, fractionOfZeros: Double): Unit = {
    val percentageOfZeros = fractionOfZeros * 100
    val benchmark =
      new Benchmark(
        s"Sorted Lg Str Filter Scan ($percentageOfZeros% zeros)",
        values,
        output = output)

    withTempPath { dir =>
      withTempTable("parquetV1Table", "parquetV2Table") {
        prepareTable(
          dir,
          spark.sql(
            s"SELECT IF(RAND(1) < $fractionOfZeros, -1, value) AS c1, " +
              s"REPEAT(CAST(value AS STRING), 100) AS c2 FROM $tbl ORDER BY c1, c2"))

        benchmark.addCase("SQL Parquet - Spark") { _ =>
          spark.sql("SELECT * FROM parquetV1Table WHERE c1 + 1 > 0").noop()
        }

        benchmark.addCase("SQL Parquet - Comet") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_NATIVE_SCAN_IMPL.key -> SCAN_NATIVE_COMET) {
            spark.sql("SELECT * FROM parquetV1Table WHERE c1 + 1 > 0").noop()
          }
        }

        benchmark.addCase("SQL Parquet - Comet Native DataFusion") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_NATIVE_SCAN_IMPL.key -> SCAN_NATIVE_DATAFUSION) {
            spark.sql("SELECT * FROM parquetV1Table WHERE c1 + 1 > 0").noop()
          }
        }

        benchmark.addCase("SQL Parquet - Comet Native Iceberg Compat") { _ =>
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_ENABLED.key -> "true",
            CometConf.COMET_NATIVE_SCAN_IMPL.key -> SCAN_NATIVE_ICEBERG_COMPAT) {
            spark.sql("SELECT * FROM parquetV1Table WHERE c1 + 1 > 0").noop()
          }
        }

        benchmark.run()
      }
    }
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    runBenchmarkWithTable("Parquet Reader", 1024 * 1024 * 15) { v =>
      Seq(
        BooleanType,
        ByteType,
        ShortType,
        IntegerType,
        LongType,
        FloatType,
        DoubleType,
        StringType).foreach { dataType =>
        readerBenchmark(v, dataType)
      }
    }

    runBenchmarkWithTable("SQL Single Numeric Column Scan", 1024 * 1024 * 15) { v =>
      Seq(BooleanType, ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType)
        .foreach { dataType =>
          numericScanBenchmark(v, dataType)
        }
    }

    runBenchmark("SQL Decimal Column Scan") {
      withTempTable(tbl) {
        import spark.implicits._
        spark.range(1024 * 1024 * 15).map(_ => Random.nextInt).createOrReplaceTempView(tbl)

        Seq((5, 2), (18, 4), (20, 8)).foreach { case (precision, scale) =>
          decimalScanBenchmark(1024 * 1024 * 15, precision, scale)
        }
      }
    }

    runBenchmarkWithTable("String Scan with Dictionary", 1024 * 1024 * 15) { v =>
      stringWithDictionaryScanBenchmark(v)
    }

    runBenchmarkWithTable("Numeric Filter Scan", 1024 * 1024 * 10) { v =>
      for (fractionOfZeros <- List(0.0, 0.50, 0.95)) {
        numericFilterScanBenchmark(v, fractionOfZeros)
      }
    }

    runBenchmarkWithTable("String with Nulls Scan", 1024 * 1024 * 10) { v =>
      for (fractionOfNulls <- List(0.0, 0.50, 0.95)) {
        stringWithNullsScanBenchmark(v, fractionOfNulls)
      }
    }

    runBenchmarkWithTable("Single Column Scan From Wide Columns", 1024 * 1024 * 1) { v =>
      for (columnWidth <- List(10, 50, 100)) {
        columnsBenchmark(v, columnWidth)
      }
    }

    runBenchmarkWithTable("Large String Filter Scan", 1024 * 1024) { v =>
      for (fractionOfZeros <- List(0.0, 0.50, 0.999)) {
        largeStringFilterScanBenchmark(v, fractionOfZeros)
      }
    }

    runBenchmarkWithTable("Sorted Lg Str Filter Scan", 1024 * 1024) { v =>
      for (fractionOfZeros <- List(0.0, 0.50, 0.999)) {
        sortedLgStrFilterScanBenchmark(v, fractionOfZeros)
      }
    }
  }
}

object CometReadBenchmark extends CometReadBaseBenchmark {}

object CometReadHdfsBenchmark extends CometReadBaseBenchmark with WithHdfsCluster {

  override def getSparkSession: SparkSession = {
    // start HDFS cluster and add hadoop conf
    startHdfsCluster()
    val sparkSession = super.getSparkSession
    sparkSession.sparkContext.hadoopConfiguration.addResource(getHadoopConfFile)
    sparkSession
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    try {
      super.runCometBenchmark(mainArgs)
    } finally {
      stopHdfsCluster()
    }
  }

  override def readerBenchmark(values: Int, dataType: DataType): Unit = {
    // ignore reader benchmark for HDFS
  }

  // mock local dir to hdfs
  override protected def withTempPath(f: File => Unit): Unit = {
    super.withTempPath { dir =>
      val tempHdfsPath = new Path(getTmpRootDir, dir.getName)
      getFileSystem.mkdirs(tempHdfsPath)
      try f(dir)
      finally getFileSystem.delete(tempHdfsPath, true)
    }
  }
  override protected def prepareTable(
      dir: File,
      df: DataFrame,
      partition: Option[String]): Unit = {
    val testDf = if (partition.isDefined) {
      df.write.partitionBy(partition.get)
    } else {
      df.write
    }
    val tempHdfsPath = getFileSystem.resolvePath(new Path(getTmpRootDir, dir.getName))
    val parquetV1Path = new Path(tempHdfsPath, "parquetV1")
    saveAsParquetV1Table(testDf, parquetV1Path.toString)
  }
}
