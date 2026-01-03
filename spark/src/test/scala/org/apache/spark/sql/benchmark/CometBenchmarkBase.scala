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
import java.nio.charset.StandardCharsets
import java.util.Base64

import scala.util.Random

import org.apache.parquet.crypto.DecryptionPropertiesFactory
import org.apache.parquet.crypto.keytools.{KeyToolkit, PropertiesDrivenCryptoFactory}
import org.apache.parquet.crypto.keytools.mocks.InMemoryKMS
import org.apache.spark.SparkConf
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SparkSession}
import org.apache.spark.sql.execution.benchmark.SqlBasedBenchmark
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, DecimalType}

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

  /**
   * Creates a table with ANSI-safe values that won't overflow in arithmetic operations. Use this
   * instead of runBenchmarkWithTable for arithmetic/aggregate benchmarks.
   */
  protected def runBenchmarkWithSafeTable(
      benchmarkName: String,
      values: Int,
      useDictionary: Boolean = false)(f: Int => Any): Unit = {
    withTempTable(tbl) {
      import spark.implicits._
      spark
        .range(values)
        .map(i => if (useDictionary) i % 5 else i % 10000)
        .createOrReplaceTempView(tbl)
      runBenchmark(benchmarkName)(f(values))
    }
  }

  /**
   * Generates ANSI-safe data for casting from Long to the specified target type. Returns a SQL
   * expression that transforms the base "value" column to be within safe ranges.
   *
   * @param targetType
   *   The target data type for casting
   * @return
   *   SQL expression to generate safe data
   */
  protected def generateAnsiSafeData(targetType: DataType): String = {
    import org.apache.spark.sql.types._

//    we generate long inputs initially and this case statement translates them into right data type so that the code doesn't fail in ANSI mode
    targetType match {
      case ByteType => "CAST((value % 128) AS BIGINT)"
      case ShortType => "CAST((value % 32768) AS BIGINT)"
      case IntegerType => "CAST((value % 2147483648) AS BIGINT)"
      case LongType => "value"
      case FloatType => "CAST((value % 1000000) AS BIGINT)"
      case DoubleType => "value"
      case _: DecimalType => "CAST((value % 100000000) AS BIGINT)"
      case StringType => "CAST(value AS STRING)"
      case BooleanType => "CAST((value % 2) AS BIGINT)"
      case DateType => "CAST((value % 18262) AS BIGINT)"
      case TimestampType => "value"
      case BinaryType => "value"
      case _ => "value"
    }
  }

  /**
   * Runs an expression benchmark with standard cases: Spark, Comet (Scan), Comet (Scan + Exec).
   * This provides a consistent benchmark structure for expression evaluation.
   *
   * @param name
   *   Benchmark name
   * @param cardinality
   *   Number of rows being processed
   * @param query
   *   SQL query to benchmark
   * @param extraCometConfigs
   *   Additional configurations to apply for Comet cases (optional)
   */
  final def runExpressionBenchmark(
      name: String,
      cardinality: Long,
      query: String,
      isAnsiMode: Boolean,
      extraCometConfigs: Map[String, String] = Map.empty): Unit = {

    val benchmark = new Benchmark(name, cardinality, output = output)

    benchmark.addCase("Spark") { _ =>
      withSQLConf(
        CometConf.COMET_ENABLED.key -> "false",
        SQLConf.ANSI_ENABLED.key -> isAnsiMode.toString) {
        runSparkCommand(spark, query, isAnsiMode)
      }
    }

    benchmark.addCase("Comet (Scan)") { _ =>
      withSQLConf(
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "false",
        SQLConf.ANSI_ENABLED.key -> isAnsiMode.toString) {
        runSparkCommand(spark, query, isAnsiMode)
      }
    }

    val cometExecConfigs = Map(
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      "spark.sql.optimizer.constantFolding.enabled" -> "false",
      SQLConf.ANSI_ENABLED.key -> isAnsiMode.toString) ++ extraCometConfigs

    benchmark.addCase("Comet (Scan + Exec)") { _ =>
      withSQLConf(cometExecConfigs.toSeq: _*) {
        runSparkCommand(spark, query, isAnsiMode)
      }
    }

    benchmark.run()
  }

  private def runSparkCommand(spark: SparkSession, query: String, isANSIMode: Boolean): Unit = {
    // With ANSI-safe data generation, queries should not throw exceptions
    spark.sql(query).noop()
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

  protected def prepareEncryptedTable(
      dir: File,
      df: DataFrame,
      partition: Option[String] = None): Unit = {
    val testDf = if (partition.isDefined) {
      df.write.partitionBy(partition.get)
    } else {
      df.write
    }

    saveAsEncryptedParquetV1Table(testDf, dir.getCanonicalPath + "/parquetV1")
  }

  protected def prepareIcebergTable(
      dir: File,
      df: DataFrame,
      tableName: String = "icebergTable",
      partition: Option[String] = None): Unit = {
    val warehouseDir = new File(dir, "iceberg-warehouse")

    // Configure Hadoop catalog (same pattern as CometIcebergNativeSuite)
    spark.conf.set("spark.sql.catalog.benchmark_cat", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.benchmark_cat.type", "hadoop")
    spark.conf.set("spark.sql.catalog.benchmark_cat.warehouse", warehouseDir.getAbsolutePath)

    val fullTableName = s"benchmark_cat.db.$tableName"

    // Drop table if exists
    spark.sql(s"DROP TABLE IF EXISTS $fullTableName")

    // Create a temp view from the DataFrame
    df.createOrReplaceTempView("temp_df_for_iceberg")

    // Create Iceberg table from temp view
    val partitionClause = partition.map(p => s"PARTITIONED BY ($p)").getOrElse("")
    spark.sql(s"""
      CREATE TABLE $fullTableName
      USING iceberg
      TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec' = 'snappy')
      $partitionClause
      AS SELECT * FROM temp_df_for_iceberg
    """)

    // Create temp view for benchmarking
    spark.table(fullTableName).createOrReplaceTempView(tableName)

    spark.catalog.dropTempView("temp_df_for_iceberg")
  }

  protected def saveAsEncryptedParquetV1Table(df: DataFrameWriter[Row], dir: String): Unit = {
    val encoder = Base64.getEncoder
    val footerKey =
      encoder.encodeToString("0123456789012345".getBytes(StandardCharsets.UTF_8))
    val key1 = encoder.encodeToString("1234567890123450".getBytes(StandardCharsets.UTF_8))
    val cryptoFactoryClass =
      "org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory"
    withSQLConf(
      DecryptionPropertiesFactory.CRYPTO_FACTORY_CLASS_PROPERTY_NAME -> cryptoFactoryClass,
      KeyToolkit.KMS_CLIENT_CLASS_PROPERTY_NAME ->
        "org.apache.parquet.crypto.keytools.mocks.InMemoryKMS",
      InMemoryKMS.KEY_LIST_PROPERTY_NAME ->
        s"footerKey: ${footerKey}, key1: ${key1}") {
      df.mode("overwrite")
        .option("compression", "snappy")
        .option(PropertiesDrivenCryptoFactory.COLUMN_KEYS_PROPERTY_NAME, "key1: id")
        .option(PropertiesDrivenCryptoFactory.FOOTER_KEY_PROPERTY_NAME, "footerKey")
        .parquet(dir)
      spark.read.parquet(dir).createOrReplaceTempView("parquetV1Table")
    }
  }

  protected def makeDecimalDataFrame(
      values: Int,
      decimal: DecimalType,
      useDictionary: Boolean): DataFrame = {
    import spark.implicits._

    // Use safe range to avoid overflow in decimal operations
    val maxValue = 10000
    val div = if (useDictionary) 5 else maxValue
    spark
      .range(values)
      .map(_ % div)
      .select((($"value" - 500) / 100.0) cast decimal as Symbol("dec"))
  }
}
