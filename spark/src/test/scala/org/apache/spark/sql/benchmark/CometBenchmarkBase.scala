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

import scala.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SparkSession}
import org.apache.spark.sql.execution.benchmark.SqlBasedBenchmark
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DecimalType

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions

trait CometBenchmarkBase extends SqlBasedBenchmark {
  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName("CometReadBenchmark")
      // Since `spark.master` always exists, overrides this value
      .set("spark.master", "local[1]")
      .setIfMissing("spark.driver.memory", "3g")
      .setIfMissing("spark.executor.memory", "3g")

    val sparkSession = SparkSession.builder
      .config(conf)
      .withExtensions(new CometSparkSessionExtensions)
      .getOrCreate()

    // Set default configs. Individual cases will change them if necessary.
    sparkSession.conf.set(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key, "true")
    sparkSession.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")
    sparkSession.conf.set(CometConf.COMET_ENABLED.key, "false")
    sparkSession.conf.set(CometConf.COMET_EXEC_ENABLED.key, "false")

    sparkSession
  }

  def runCometBenchmark(args: Array[String]): Unit

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runCometBenchmark(mainArgs)
  }

  protected val tbl = "comet_table"

  protected def withTempTable(tableNames: String*)(f: => Unit): Unit = {
    try f
    finally tableNames.foreach(spark.catalog.dropTempView)
  }

  protected def runBenchmarkWithTable(
      benchmarkName: String,
      values: Int,
      useDictionary: Boolean = false)(f: Int => Any): Unit = {
    withTempTable(tbl) {
      import spark.implicits._
      spark
        .range(values)
        .map(_ => if (useDictionary) Random.nextLong % 5 else Random.nextLong)
        .createOrReplaceTempView(tbl)
      runBenchmark(benchmarkName)(f(values))
    }
  }

  /** Runs function `f` with Comet on and off. */
  final def runWithComet(name: String, cardinality: Long)(f: => Unit): Unit = {
    val benchmark = new Benchmark(name, cardinality, output = output)

    benchmark.addCase(s"$name - Spark ") { _ =>
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        f
      }
    }

    benchmark.addCase(s"$name - Comet") { _ =>
      withSQLConf(
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ALL_OPERATOR_ENABLED.key -> "true",
        SQLConf.ANSI_ENABLED.key -> "false") {
        f
      }
    }

    benchmark.run()
  }

  protected def prepareTable(dir: File, df: DataFrame, partition: Option[String] = None): Unit = {
    val testDf = if (partition.isDefined) {
      df.write.partitionBy(partition.get)
    } else {
      df.write
    }

    saveAsParquetV1Table(testDf, dir.getCanonicalPath + "/parquetV1")
  }

  protected def saveAsParquetV1Table(df: DataFrameWriter[Row], dir: String): Unit = {
    df.mode("overwrite").option("compression", "snappy").parquet(dir)
    spark.read.parquet(dir).createOrReplaceTempView("parquetV1Table")
  }

  protected def makeDecimalDataFrame(
      values: Int,
      decimal: DecimalType,
      useDictionary: Boolean): DataFrame = {
    import spark.implicits._

    val div = if (useDictionary) 5 else values
    spark
      .range(values)
      .map(_ % div)
      .select((($"value" - 500) / 100.0) cast decimal as Symbol("dec"))
  }
}
