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

import java.nio.file.Files

import org.apache.spark.SparkConf
import org.apache.spark.sql.CometTestBase

/**
 * Quick benchmark comparing vanilla Spark+Delta vs Comet+Delta-kernel.
 *
 * Run with: export SPARK_LOCAL_IP=127.0.0.1 && ./mvnw -Pspark-3.5 -pl spark -am test \
 * -Dsuites=org.apache.comet.CometDeltaBenchmarkTest -Dmaven.gitcommitid.skip
 */
class CometDeltaBenchmarkTest extends CometTestBase {

  private def deltaSparkAvailable: Boolean =
    try {
      Class.forName("org.apache.spark.sql.delta.DeltaParquetFileFormat")
      true
    } catch {
      case _: ClassNotFoundException => false
    }

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    conf.set("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    conf.set("spark.databricks.delta.testOnly.dataFileNamePrefix", "")
    conf.set("spark.databricks.delta.testOnly.dvFileNamePrefix", "")
    conf
  }

  test("benchmark: SUM aggregation - vanilla vs Comet native Delta") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath")

    val tempDir = Files.createTempDirectory("comet-delta-bench").toFile
    try {
      val tablePath = new java.io.File(tempDir, "bench").getAbsolutePath
      val numRows = 5 * 1000 * 1000 // 5M rows
      val numFiles = 4

      // scalastyle:off println
      println(s"\n=== Comet Delta Benchmark: $numRows rows, $numFiles files ===\n")
      // scalastyle:on println

      // Generate data
      val ss = spark
      import ss.implicits._
      val df =
        (0 until numRows).map(i => (i.toLong, i * 1.5, s"name_$i")).toDF("id", "score", "name")
      df.repartition(numFiles).write.format("delta").save(tablePath)

      val warmupIters = 2
      val benchIters = 5

      // Vanilla Spark+Delta
      val vanillaTimes = new scala.collection.mutable.ArrayBuffer[Long]()
      withSQLConf(
        CometConf.COMET_ENABLED.key -> "false",
        CometConf.COMET_EXEC_ENABLED.key -> "false") {
        for (i <- 0 until (warmupIters + benchIters)) {
          val start = System.nanoTime()
          spark.sql(s"SELECT SUM(id), SUM(score) FROM delta.`$tablePath`").collect()
          val elapsed = (System.nanoTime() - start) / 1000000
          if (i >= warmupIters) vanillaTimes += elapsed
        }
      }

      // Comet native Delta
      val cometTimes = new scala.collection.mutable.ArrayBuffer[Long]()
      withSQLConf(
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_DELTA_NATIVE_ENABLED.key -> "true") {
        for (i <- 0 until (warmupIters + benchIters)) {
          val start = System.nanoTime()
          spark.sql(s"SELECT SUM(id), SUM(score) FROM delta.`$tablePath`").collect()
          val elapsed = (System.nanoTime() - start) / 1000000
          if (i >= warmupIters) cometTimes += elapsed
        }
      }

      val vanillaAvg = vanillaTimes.sum.toDouble / vanillaTimes.size
      val cometAvg = cometTimes.sum.toDouble / cometTimes.size
      val speedup = vanillaAvg / cometAvg

      // scalastyle:off println
      println(f"\n=== Results (${benchIters} iterations, ${warmupIters} warmup) ===")
      println(
        f"  Vanilla Spark+Delta: ${vanillaAvg}%.0f ms avg (${vanillaTimes.mkString(", ")} ms)")
      println(f"  Comet Native Delta:  ${cometAvg}%.0f ms avg (${cometTimes.mkString(", ")} ms)")
      println(f"  Speedup: ${speedup}%.2fx")
      println()
      // scalastyle:on println

      // Don't assert on speedup - just report numbers.
      // On debug builds the native path may actually be slower due to no LTO.
    } finally {
      def deleteRecursively(file: java.io.File): Unit = {
        if (file.isDirectory) { Option(file.listFiles()).foreach(_.foreach(deleteRecursively)) }
        file.delete()
      }
      deleteRecursively(tempDir)
    }
  }

  test("benchmark: filter scan - vanilla vs Comet native Delta") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath")

    val tempDir = Files.createTempDirectory("comet-delta-bench-filter").toFile
    try {
      val tablePath = new java.io.File(tempDir, "bench").getAbsolutePath
      val numRows = 2 * 1000 * 1000
      val numFiles = 4

      // scalastyle:off println
      println(s"\n=== Comet Delta Filter Benchmark: $numRows rows, $numFiles files ===\n")
      // scalastyle:on println

      val ss = spark
      import ss.implicits._
      val df =
        (0 until numRows).map(i => (i.toLong, i * 1.5, s"name_$i")).toDF("id", "score", "name")
      df.repartition(numFiles).write.format("delta").save(tablePath)

      val warmupIters = 2
      val benchIters = 5
      val query = s"SELECT COUNT(*), SUM(score) FROM delta.`$tablePath` WHERE id > ${numRows / 2}"

      val vanillaTimes = new scala.collection.mutable.ArrayBuffer[Long]()
      withSQLConf(
        CometConf.COMET_ENABLED.key -> "false",
        CometConf.COMET_EXEC_ENABLED.key -> "false") {
        for (i <- 0 until (warmupIters + benchIters)) {
          val start = System.nanoTime()
          spark.sql(query).collect()
          val elapsed = (System.nanoTime() - start) / 1000000
          if (i >= warmupIters) vanillaTimes += elapsed
        }
      }

      val cometTimes = new scala.collection.mutable.ArrayBuffer[Long]()
      withSQLConf(
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_DELTA_NATIVE_ENABLED.key -> "true") {
        for (i <- 0 until (warmupIters + benchIters)) {
          val start = System.nanoTime()
          spark.sql(query).collect()
          val elapsed = (System.nanoTime() - start) / 1000000
          if (i >= warmupIters) cometTimes += elapsed
        }
      }

      val vanillaAvg = vanillaTimes.sum.toDouble / vanillaTimes.size
      val cometAvg = cometTimes.sum.toDouble / cometTimes.size
      val speedup = vanillaAvg / cometAvg

      // scalastyle:off println
      println(f"\n=== Filter Results (${benchIters} iterations, ${warmupIters} warmup) ===")
      println(
        f"  Vanilla Spark+Delta: ${vanillaAvg}%.0f ms avg (${vanillaTimes.mkString(", ")} ms)")
      println(f"  Comet Native Delta:  ${cometAvg}%.0f ms avg (${cometTimes.mkString(", ")} ms)")
      println(f"  Speedup: ${speedup}%.2fx")
      println()
      // scalastyle:on println
    } finally {
      def deleteRecursively(file: java.io.File): Unit = {
        if (file.isDirectory) { Option(file.listFiles()).foreach(_.foreach(deleteRecursively)) }
        file.delete()
      }
      deleteRecursively(tempDir)
    }
  }
}
