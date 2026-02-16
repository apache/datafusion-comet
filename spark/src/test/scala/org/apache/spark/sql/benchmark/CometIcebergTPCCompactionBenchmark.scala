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
import java.util.Locale

import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.spark.actions.SparkActions
import org.apache.spark.sql.comet.CometNativeCompaction

/**
 * Benchmark to measure Iceberg compaction performance using TPC-H dataset. Compares Spark default
 * compaction (SparkBinPackDataRewriter) vs Comet-accelerated compaction
 * (CometBinPackRewriteRunner with iceberg-rust ReplaceDataFilesAction).
 *
 * To run this benchmark:
 * {{{
 * // Set scale factor in GB
 * scale_factor=1
 *
 * // GenTPCHData to create the data set at /tmp/tpch/sf1_parquet
 * cd $COMET_HOME
 * make benchmark-org.apache.spark.sql.GenTPCHData -- --location /tmp --scaleFactor ${scale_factor}
 *
 * // Run the Iceberg compaction benchmark
 * SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometIcebergTPCCompactionBenchmark -- --data-location /tmp/tpch/sf${scale_factor}_parquet
 * }}}
 *
 * Results will be written to "spark/benchmarks/CometIcebergTPCCompactionBenchmark-results.txt".
 */
object CometIcebergTPCCompactionBenchmark extends CometBenchmarkBase {

  // TPC-H tables to use for compaction benchmarks
  // lineitem is the largest and most representative for compaction workloads
  val compactionTables: Seq[String] = Seq("lineitem", "orders", "customer")

  // Partitioned table benchmarks use lineitem partitioned by l_shipdate
  val partitionedBenchmarkEnabled: Boolean = true

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val benchmarkArgs = new IcebergTPCBenchmarkArguments(mainArgs)

    runBenchmark("Iceberg TPC-H Compaction Benchmark") {
      runIcebergCompactionBenchmarks(benchmarkArgs.dataLocation, benchmarkArgs.numFragments)
    }
  }

  private def runIcebergCompactionBenchmarks(dataLocation: String, numFragments: Int): Unit = {
    // Print header
    printHeader()

    // Non-partitioned table benchmarks
    compactionTables.foreach { tableName =>
      runTableCompactionBenchmark(dataLocation, tableName, numFragments)
    }

    // Partitioned table benchmark (lineitem partitioned by shipdate month)
    if (partitionedBenchmarkEnabled) {
      runPartitionedTableBenchmark(dataLocation, numFragments)
    }

    // Print footer
    printFooter()
  }

  private def printToOutput(text: String): Unit = {
    // scalastyle:off println
    println(text)
    output.foreach { os =>
      os.write(text.getBytes)
      os.write('\n')
    }
    // scalastyle:on println
  }

  private def printHeader(): Unit = {
    val sep = "-" * 90
    val colHeader =
      f"${"Table"}%-15s ${"Rows"}%10s ${"Files"}%8s ${"Spark(ms)"}%12s ${"Native(ms)"}%12s ${"Speedup"}%10s"
    printToOutput(s"\n$sep")
    printToOutput("  Iceberg Compaction Benchmark: Spark Default vs Comet Native")
    printToOutput(sep)
    printToOutput(colHeader)
    printToOutput(sep)
  }

  private def printFooter(): Unit = {
    printToOutput("-" * 90)
  }

  private def writeResult(
      tableName: String,
      rowCount: Long,
      filesBefore: Long,
      filesAfter: Long,
      sparkMs: Long,
      nativeMs: Long,
      speedup: Double): Unit = {
    val speedupStr = if (speedup > 0) f"$speedup%.2fx" else "N/A"
    val sparkStr = if (sparkMs > 0) sparkMs.toString else "N/A"
    val line =
      f"$tableName%-15s ${rowCount / 1000}%9dK $filesBefore%3d->$filesAfter%-3d $sparkStr%12s $nativeMs%12d $speedupStr%10s"
    printToOutput(line)
  }

  /**
   * Run compaction benchmark for partitioned Iceberg table (lineitem by shipdate month).
   */
  private def runPartitionedTableBenchmark(dataLocation: String, numFragments: Int): Unit = {
    val tableFilePath = resolveTablePath(dataLocation, "lineitem")

    withIcebergWarehouse { (warehouseDir, catalog) =>
      val icebergTableName = s"$catalog.db.lineitem_partitioned"

      // Create fragmented partitioned table
      createFragmentedPartitionedTable(icebergTableName, tableFilePath, numFragments)
      val rowCount = spark.sql(s"SELECT COUNT(*) FROM $icebergTableName").first().getLong(0)
      val fileCount = spark.sql(s"SELECT * FROM $icebergTableName.files").count()

      // Measure native compaction on partitioned table (single run)
      val nativeStart = System.nanoTime()
      val nativeTable = Spark3Util.loadIcebergTable(spark, icebergTableName)
      new CometNativeCompaction(spark).rewriteDataFiles(nativeTable)
      val nativeTimeMs = (System.nanoTime() - nativeStart) / 1000000
      val nativeFilesAfter = spark.sql(s"SELECT * FROM $icebergTableName.files").count()

      // Write result
      writeResult("lineitem_part", rowCount, fileCount, nativeFilesAfter, 0, nativeTimeMs, 0)

      spark.sql(s"DROP TABLE IF EXISTS $icebergTableName")
    }
  }

  /**
   * Create fragmented partitioned Iceberg table from TPC-H lineitem.
   */
  private def createFragmentedPartitionedTable(
      icebergTable: String,
      sourceParquetPath: String,
      numFragments: Int): Unit = {

    val sourceDF = spark.read.parquet(sourceParquetPath)
    val totalRows = sourceDF.count()
    val rowsPerFragment = totalRows / numFragments

    // Create partitioned Iceberg table by l_shipmode
    spark.sql(s"""
      CREATE TABLE $icebergTable (
        l_orderkey BIGINT, l_partkey BIGINT, l_suppkey BIGINT, l_linenumber INT,
        l_quantity DECIMAL(15,2), l_extendedprice DECIMAL(15,2), l_discount DECIMAL(15,2),
        l_tax DECIMAL(15,2), l_returnflag STRING, l_linestatus STRING, l_shipdate DATE,
        l_commitdate DATE, l_receiptdate DATE, l_shipinstruct STRING, l_shipmode STRING,
        l_comment STRING
      ) USING iceberg PARTITIONED BY (l_shipmode)
    """)

    val schema = sourceDF.schema.fieldNames.mkString(", ")
    for (i <- 0 until numFragments) {
      val offset = i * rowsPerFragment
      spark.sql(s"""
        INSERT INTO $icebergTable
        SELECT $schema FROM parquet.`$sourceParquetPath`
        LIMIT $rowsPerFragment OFFSET $offset
      """)
    }
  }

  /**
   * Run compaction benchmark for a specific TPC-H table.
   */
  private def runTableCompactionBenchmark(
      dataLocation: String,
      tableName: String,
      numFragments: Int): Unit = {

    val tableFilePath = resolveTablePath(dataLocation, tableName)

    withIcebergWarehouse { (warehouseDir, catalog) =>
      val icebergTableName = s"$catalog.db.${tableName}_iceberg"

      // Create fragmented table once to measure metadata
      createFragmentedIcebergTable(icebergTableName, tableFilePath, numFragments)
      val rowCount = spark.sql(s"SELECT COUNT(*) FROM $icebergTableName").first().getLong(0)
      val fileCount = spark.sql(s"SELECT * FROM $icebergTableName.files").count()

      // Measure Spark compaction (single run - compaction is destructive)
      val sparkStart = System.nanoTime()
      val sparkTable = Spark3Util.loadIcebergTable(spark, icebergTableName)
      SparkActions.get(spark).rewriteDataFiles(sparkTable).binPack().execute()
      val sparkTimeMs = (System.nanoTime() - sparkStart) / 1000000
      val sparkFilesAfter = spark.sql(s"SELECT * FROM $icebergTableName.files").count()

      // Re-create fragmented table for native benchmark
      spark.sql(s"DROP TABLE IF EXISTS $icebergTableName")
      createFragmentedIcebergTable(icebergTableName, tableFilePath, numFragments)

      // Measure native compaction (single run)
      val nativeStart = System.nanoTime()
      val nativeTable = Spark3Util.loadIcebergTable(spark, icebergTableName)
      new CometNativeCompaction(spark).rewriteDataFiles(nativeTable)
      val nativeTimeMs = (System.nanoTime() - nativeStart) / 1000000
      val nativeFilesAfter = spark.sql(s"SELECT * FROM $icebergTableName.files").count()

      // Calculate speedup
      val speedup = if (nativeTimeMs > 0) sparkTimeMs.toDouble / nativeTimeMs.toDouble else 0.0

      // Write result
      writeResult(
        tableName,
        rowCount,
        fileCount,
        sparkFilesAfter,
        sparkTimeMs,
        nativeTimeMs,
        speedup)

      spark.sql(s"DROP TABLE IF EXISTS $icebergTableName")
    }
  }

  /**
   * Create a fragmented Iceberg table by importing TPC-H Parquet data in multiple batches.
   */
  private def createFragmentedIcebergTable(
      icebergTable: String,
      sourceParquetPath: String,
      numFragments: Int): Unit = {

    // Read the source Parquet data
    val sourceDF = spark.read.parquet(sourceParquetPath)
    val totalRows = sourceDF.count()
    val rowsPerFragment = totalRows / numFragments

    // Create the Iceberg table
    sourceDF.limit(0).writeTo(icebergTable).using("iceberg").create()

    // Insert data in fragments to create multiple small files
    val schema = sourceDF.schema.fieldNames.mkString(", ")

    for (i <- 0 until numFragments) {
      val offset = i * rowsPerFragment
      spark.sql(s"""
        INSERT INTO $icebergTable
        SELECT $schema FROM parquet.`$sourceParquetPath`
        LIMIT $rowsPerFragment OFFSET $offset
      """)
    }
  }

  /**
   * Resolve the path to a TPC-H table, supporting both with and without .parquet extension.
   */
  private def resolveTablePath(dataLocation: String, tableName: String): String = {
    val pathDefault = s"$dataLocation/$tableName"
    val pathAlt = s"$dataLocation/$tableName.parquet"

    if (new File(pathDefault).exists()) {
      pathDefault
    } else if (new File(pathAlt).exists()) {
      pathAlt
    } else {
      throw new java.io.FileNotFoundException(
        s"TPC-H table $tableName not found at $pathDefault or $pathAlt")
    }
  }

  /**
   * Helper to use Iceberg warehouse with catalog configuration.
   */
  private def withIcebergWarehouse(f: (File, String) => Unit): Unit = {
    withTempPath { dir =>
      val warehouseDir = new File(dir, "iceberg-warehouse")
      val catalogName = "tpc_iceberg_cat"

      spark.conf.set(s"spark.sql.catalog.$catalogName", "org.apache.iceberg.spark.SparkCatalog")
      spark.conf.set(s"spark.sql.catalog.$catalogName.type", "hadoop")
      spark.conf.set(s"spark.sql.catalog.$catalogName.warehouse", warehouseDir.getAbsolutePath)

      spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $catalogName.db")

      try {
        f(warehouseDir, catalogName)
      } finally {
        spark.conf.unset(s"spark.sql.catalog.$catalogName")
        spark.conf.unset(s"spark.sql.catalog.$catalogName.type")
        spark.conf.unset(s"spark.sql.catalog.$catalogName.warehouse")
      }
    }
  }
}

/**
 * Command line arguments for Iceberg TPC compaction benchmark.
 */
class IcebergTPCBenchmarkArguments(val args: Array[String]) {
  var dataLocation: String = null
  var numFragments: Int = 20

  parseArgs(args.toList)
  validateArguments()

  private def optionMatch(optionName: String, s: String): Boolean = {
    optionName == s.toLowerCase(Locale.ROOT)
  }

  private def parseArgs(inputArgs: List[String]): Unit = {
    var args = inputArgs

    while (args.nonEmpty) {
      args match {
        case optName :: value :: tail if optionMatch("--data-location", optName) =>
          dataLocation = value
          args = tail

        case optName :: value :: tail if optionMatch("--num-fragments", optName) =>
          numFragments = value.toInt
          args = tail

        case _ =>
          // scalastyle:off println
          System.err.println("Unknown/unsupported param " + args)
          // scalastyle:on println
          printUsageAndExit(1)
      }
    }
  }

  private def printUsageAndExit(exitCode: Int): Unit = {
    // scalastyle:off
    System.err.println("""
      |Usage: spark-submit --class <this class> <spark sql test jar> [Options]
      |Options:
      |  --data-location      Path to TPC-H Parquet data (required)
      |  --num-fragments      Number of fragments to create for compaction (default: 20)
      |
      |------------------------------------------------------------------------------------------------------------------
      |This benchmark measures Iceberg compaction performance using TPC-H data.
      |
      |To generate TPC-H data:
      |  make benchmark-org.apache.spark.sql.GenTPCHData -- --location /tmp --scaleFactor 1
      |
      |Then run the benchmark:
      |  SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.sql.benchmark.CometIcebergTPCCompactionBenchmark -- --data-location /tmp/tpch/sf1_parquet
      """.stripMargin)
    // scalastyle:on
    System.exit(exitCode)
  }

  private def validateArguments(): Unit = {
    if (dataLocation == null) {
      // scalastyle:off println
      System.err.println("Must specify --data-location")
      // scalastyle:on println
      printUsageAndExit(-1)
    }
  }
}
