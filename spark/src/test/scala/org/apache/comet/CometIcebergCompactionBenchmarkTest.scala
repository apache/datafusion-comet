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

package org.apache.comet

import java.io.File
import java.nio.file.Files

import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.spark.actions.SparkActions
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.CometNativeCompaction

/**
 * Simple benchmark test for Iceberg compaction comparing Spark default vs Native compaction. Run
 * with: mvn test -pl spark -Dsuites=org.apache.comet.CometIcebergCompactionBenchmarkTest
 */
class CometIcebergCompactionBenchmarkTest extends CometTestBase {

  private val dataLocation = "/tmp/tpch/sf1_parquet"

  private def icebergAvailable: Boolean = {
    try {
      Class.forName("org.apache.iceberg.catalog.Catalog")
      true
    } catch {
      case _: ClassNotFoundException => false
    }
  }

  private def tpcDataAvailable: Boolean = {
    new File(s"$dataLocation/lineitem").exists()
  }

  private def withTempIcebergDir(f: File => Unit): Unit = {
    val dir = Files.createTempDirectory("comet-benchmark").toFile
    try {
      f(dir)
    } finally {
      def deleteRecursively(file: File): Unit = {
        if (file.isDirectory) file.listFiles().foreach(deleteRecursively)
        file.delete()
      }
      deleteRecursively(dir)
    }
  }

  private def icebergCatalogConf(warehouseDir: File): Map[String, String] = Map(
    "spark.sql.catalog.bench_cat" -> "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.bench_cat.type" -> "hadoop",
    "spark.sql.catalog.bench_cat.warehouse" -> warehouseDir.getAbsolutePath,
    CometConf.COMET_ENABLED.key -> "true",
    CometConf.COMET_EXEC_ENABLED.key -> "true",
    CometConf.COMET_ICEBERG_COMPACTION_ENABLED.key -> "true")

  // scalastyle:off parameter.number
  private def runTableBenchmark(
      sourceTable: String,
      schema: String,
      numFragments: Int,
      rowsPerFragment: Int): (Long, Long, Double) = {

    val tableName = s"bench_cat.db.${sourceTable}_bench"

    // Create fragmented Iceberg table
    spark.sql(s"CREATE TABLE $tableName ($schema) USING iceberg")

    // Insert fragments from TPC-H source
    val cols = schema.split(",").map(_.trim.split(" ")(0)).mkString(", ")
    for (i <- 0 until numFragments) {
      spark.sql(s"""
        INSERT INTO $tableName
        SELECT $cols FROM parquet.`$dataLocation/$sourceTable`
        LIMIT $rowsPerFragment OFFSET ${i * rowsPerFragment}
      """)
    }

    // Benchmark 1: Spark default compaction
    val sparkStart = System.nanoTime()
    val sparkTable = Spark3Util.loadIcebergTable(spark, tableName)
    SparkActions.get(spark).rewriteDataFiles(sparkTable).binPack().execute()
    val sparkDuration = (System.nanoTime() - sparkStart) / 1000000

    // Re-create for native benchmark
    spark.sql(s"DROP TABLE $tableName")
    spark.sql(s"CREATE TABLE $tableName ($schema) USING iceberg")
    for (i <- 0 until numFragments) {
      spark.sql(s"""
        INSERT INTO $tableName
        SELECT $cols FROM parquet.`$dataLocation/$sourceTable`
        LIMIT $rowsPerFragment OFFSET ${i * rowsPerFragment}
      """)
    }

    // Benchmark 2: Native compaction
    val nativeStart = System.nanoTime()
    val nativeTable = Spark3Util.loadIcebergTable(spark, tableName)
    CometNativeCompaction(spark).rewriteDataFiles(nativeTable)
    val nativeDuration = (System.nanoTime() - nativeStart) / 1000000

    spark.sql(s"DROP TABLE $tableName")

    val speedup = if (nativeDuration > 0) sparkDuration.toDouble / nativeDuration else 0
    (sparkDuration, nativeDuration, speedup)
  }

  test("TPC-H compaction benchmark: lineitem, orders, customer") {
    assume(icebergAvailable, "Iceberg not available")
    assume(tpcDataAvailable, s"TPC-H data not found at $dataLocation")
    assume(CometNativeCompaction.isAvailable, "Native compaction not available")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(icebergCatalogConf(warehouseDir).toSeq: _*) {
        val numFragments = 10
        val rowsPerFragment = 5000

        // scalastyle:off println
        println("\n" + "=" * 60)
        println("  TPC-H ICEBERG COMPACTION BENCHMARK")
        println("  Spark Default vs Native (Comet) Compaction")
        println("=" * 60)
        println(f"${"Table"}%-15s ${"Spark(ms)"}%12s ${"Native(ms)"}%12s ${"Speedup"}%10s")
        println("-" * 60)

        // Lineitem benchmark
        val lineitemSchema =
          """l_orderkey BIGINT, l_partkey BIGINT, l_suppkey BIGINT, l_linenumber INT,
             l_quantity DOUBLE, l_extendedprice DOUBLE, l_discount DOUBLE, l_tax DOUBLE,
             l_returnflag STRING, l_linestatus STRING"""
        val (lSpark, lNative, lSpeedup) =
          runTableBenchmark("lineitem", lineitemSchema, numFragments, rowsPerFragment)
        println(f"${"lineitem"}%-15s $lSpark%12d $lNative%12d ${lSpeedup}%9.2fx")

        // Orders benchmark
        val ordersSchema =
          """o_orderkey BIGINT, o_custkey BIGINT, o_orderstatus STRING, o_totalprice DOUBLE,
             o_orderdate DATE, o_orderpriority STRING, o_clerk STRING, o_shippriority INT,
             o_comment STRING"""
        val (oSpark, oNative, oSpeedup) =
          runTableBenchmark("orders", ordersSchema, numFragments, rowsPerFragment)
        println(f"${"orders"}%-15s $oSpark%12d $oNative%12d ${oSpeedup}%9.2fx")

        // Customer benchmark
        val customerSchema =
          """c_custkey BIGINT, c_name STRING, c_address STRING, c_nationkey BIGINT,
             c_phone STRING, c_acctbal DOUBLE, c_mktsegment STRING, c_comment STRING"""
        val (cSpark, cNative, cSpeedup) =
          runTableBenchmark("customer", customerSchema, numFragments, rowsPerFragment)
        println(f"${"customer"}%-15s $cSpark%12d $cNative%12d ${cSpeedup}%9.2fx")

        println("-" * 60)
        val avgSpeedup = (lSpeedup + oSpeedup + cSpeedup) / 3
        println(
          f"${"AVERAGE"}%-15s ${lSpark + oSpark + cSpark}%12d ${lNative + oNative + cNative}%12d ${avgSpeedup}%9.2fx")
        println("=" * 60 + "\n")
        // scalastyle:on println
      }
    }
  }

  test("benchmark: Spark vs Native compaction on lineitem (SF0.01 subset)") {
    assume(icebergAvailable, "Iceberg not available")
    assume(tpcDataAvailable, s"TPC-H data not found at $dataLocation")
    assume(CometNativeCompaction.isAvailable, "Native compaction not available")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(icebergCatalogConf(warehouseDir).toSeq: _*) {
        val tableName = "bench_cat.db.lineitem_bench"
        val numFragments = 10
        val rowsPerFragment = 1000

        // Create fragmented Iceberg table
        spark.sql(s"""
          CREATE TABLE $tableName (
            l_orderkey BIGINT, l_partkey BIGINT, l_suppkey BIGINT, l_linenumber INT,
            l_quantity DOUBLE, l_extendedprice DOUBLE, l_discount DOUBLE,
            l_tax DOUBLE, l_returnflag STRING, l_linestatus STRING
          ) USING iceberg
        """)

        // Insert fragments from TPC-H lineitem
        for (i <- 0 until numFragments) {
          spark.sql(s"""
            INSERT INTO $tableName
            SELECT l_orderkey, l_partkey, l_suppkey, l_linenumber,
                   l_quantity, l_extendedprice, l_discount, l_tax,
                   l_returnflag, l_linestatus
            FROM parquet.`$dataLocation/lineitem`
            LIMIT $rowsPerFragment OFFSET ${i * rowsPerFragment}
          """)
        }

        val filesBefore = spark.sql(s"SELECT * FROM $tableName.files").count()
        val rowCount = spark.sql(s"SELECT COUNT(*) FROM $tableName").first().getLong(0)

        // scalastyle:off println
        println("\n========== COMPACTION BENCHMARK ==========")
        println(s"Table: $tableName")
        println(s"Files before: $filesBefore, Rows: $rowCount")
        println("=" * 45)

        // Benchmark 1: Spark default compaction
        val sparkStart = System.nanoTime()
        val sparkTable = Spark3Util.loadIcebergTable(spark, tableName)
        SparkActions.get(spark).rewriteDataFiles(sparkTable).binPack().execute()
        val sparkDuration = (System.nanoTime() - sparkStart) / 1000000

        spark.sql(s"REFRESH TABLE $tableName")
        val filesAfterSpark = spark.sql(s"SELECT * FROM $tableName.files").count()
        println(s"Spark compaction: ${sparkDuration}ms ($filesBefore -> $filesAfterSpark files)")

        // Re-create fragmented table for native benchmark
        spark.sql(s"DROP TABLE $tableName")
        spark.sql(s"""
          CREATE TABLE $tableName (
            l_orderkey BIGINT, l_partkey BIGINT, l_suppkey BIGINT, l_linenumber INT,
            l_quantity DOUBLE, l_extendedprice DOUBLE, l_discount DOUBLE,
            l_tax DOUBLE, l_returnflag STRING, l_linestatus STRING
          ) USING iceberg
        """)

        for (i <- 0 until numFragments) {
          spark.sql(s"""
            INSERT INTO $tableName
            SELECT l_orderkey, l_partkey, l_suppkey, l_linenumber,
                   l_quantity, l_extendedprice, l_discount, l_tax,
                   l_returnflag, l_linestatus
            FROM parquet.`$dataLocation/lineitem`
            LIMIT $rowsPerFragment OFFSET ${i * rowsPerFragment}
          """)
        }

        // Benchmark 2: Native compaction
        val nativeStart = System.nanoTime()
        val nativeTable = Spark3Util.loadIcebergTable(spark, tableName)
        CometNativeCompaction(spark).rewriteDataFiles(nativeTable)
        val nativeDuration = (System.nanoTime() - nativeStart) / 1000000

        spark.sql(s"REFRESH TABLE $tableName")
        val filesAfterNative = spark.sql(s"SELECT * FROM $tableName.files").count()
        println(
          s"Native compaction: ${nativeDuration}ms ($filesBefore -> $filesAfterNative files)")

        val speedup = if (nativeDuration > 0) sparkDuration.toDouble / nativeDuration else 0
        println(s"Speedup: ${f"$speedup%.2f"}x")
        println("=" * 45 + "\n")
        // scalastyle:on println

        spark.sql(s"DROP TABLE $tableName")
      }
    }
  }
}
