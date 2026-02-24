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
import java.nio.file.Files

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.comet.CometIcebergNativeScanExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec

import org.apache.comet.CometConf
import org.apache.comet.serde.operator.CometIcebergNativeScan

/**
 * Benchmark for Iceberg FileScanTask serialization performance.
 *
 * This benchmark specifically measures the serializePartitions() method which performs the heavy
 * reflection work of converting Iceberg Java objects to protobuf.
 *
 * Use this to validate performance improvements from reflection caching optimizations (see GitHub
 * issue #3456).
 *
 * To run this benchmark:
 * {{{
 * SPARK_GENERATE_BENCHMARK_FILES=1 make \
 *   benchmark-org.apache.spark.sql.benchmark.CometIcebergSerializationBenchmark
 * }}}
 *
 * Results will be written to "spark/benchmarks/CometIcebergSerializationBenchmark-*results.txt".
 */
object CometIcebergSerializationBenchmark extends CometBenchmarkBase {

  private def icebergAvailable: Boolean = {
    try {
      Class.forName("org.apache.iceberg.catalog.Catalog")
      true
    } catch {
      case _: ClassNotFoundException => false
    }
  }

  private def withTempIcebergDir(f: File => Unit): Unit = {
    val dir = Files.createTempDirectory("comet-iceberg-serde-bench").toFile
    try {
      f(dir)
    } finally {
      def deleteRecursively(file: File): Unit = {
        if (file.isDirectory) {
          Option(file.listFiles()).foreach(_.foreach(deleteRecursively))
        }
        file.delete()
      }
      deleteRecursively(dir)
    }
  }

  private def extractIcebergNativeScanExec(
      plan: SparkPlan): Option[CometIcebergNativeScanExec] = {
    val unwrapped = plan match {
      case aqe: AdaptiveSparkPlanExec => aqe.executedPlan
      case other => other
    }

    def find(p: SparkPlan): Option[CometIcebergNativeScanExec] = {
      p match {
        case scan: CometIcebergNativeScanExec => Some(scan)
        case _ => p.children.flatMap(find).headOption
      }
    }
    find(unwrapped)
  }

  private def createPartitionedIcebergTable(
      warehouseDir: File,
      numPartitions: Int,
      tableName: String = "serde_bench_table"): Unit = {
    spark.conf.set("spark.sql.catalog.bench_cat", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.bench_cat.type", "hadoop")
    spark.conf.set("spark.sql.catalog.bench_cat.warehouse", warehouseDir.getAbsolutePath)

    val fullTableName = s"bench_cat.db.$tableName"

    spark.sql(s"DROP TABLE IF EXISTS $fullTableName")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS bench_cat.db")

    spark.sql(s"""
      CREATE TABLE $fullTableName (
        id BIGINT,
        name STRING,
        value DOUBLE,
        partition_col INT
      ) USING iceberg
      PARTITIONED BY (partition_col)
      TBLPROPERTIES (
        'format-version'='2',
        'write.parquet.compression-codec' = 'snappy'
      )
    """)

    // scalastyle:off println
    println(s"Creating Iceberg table with $numPartitions partitions...")
    // scalastyle:on println

    val batchSize = 1000
    var partitionsCreated = 0

    while (partitionsCreated < numPartitions) {
      val batchEnd = math.min(partitionsCreated + batchSize, numPartitions)
      val partitionRange = partitionsCreated until batchEnd

      import spark.implicits._
      val df = partitionRange
        .map { p =>
          (p.toLong, s"name_$p", p * 1.5, p)
        }
        .toDF("id", "name", "value", "partition_col")

      df.writeTo(fullTableName).append()
      partitionsCreated = batchEnd

      if (partitionsCreated % 5000 == 0 || partitionsCreated == numPartitions) {
        // scalastyle:off println
        println(s"  Created $partitionsCreated / $numPartitions partitions")
        // scalastyle:on println
      }
    }
  }

  /**
   * Benchmarks the serializePartitions() method which does the heavy reflection work.
   *
   * This is the core method that converts Iceberg FileScanTask Java objects to protobuf. The
   * optimizations from PR #3298 target this code path.
   */
  def serializePartitionsBenchmark(numPartitions: Int): Unit = {
    if (!icebergAvailable) {
      // scalastyle:off println
      println("Iceberg not available in classpath, skipping benchmark")
      // scalastyle:on println
      return
    }

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.bench_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.bench_cat.type" -> "hadoop",
        "spark.sql.catalog.bench_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        createPartitionedIcebergTable(warehouseDir, numPartitions)
        val fullTableName = "bench_cat.db.serde_bench_table"

        val df = spark.sql(s"SELECT * FROM $fullTableName")
        val plan = df.queryExecution.executedPlan

        val nativeScanOpt = extractIcebergNativeScanExec(plan)

        nativeScanOpt match {
          case Some(nativeScan) =>
            val metadata = nativeScan.nativeIcebergScanMetadata
            val originalPlan = nativeScan.originalPlan
            val output = nativeScan.output

            // scalastyle:off println
            println(s"Found ${metadata.tasks.size()} FileScanTasks")
            println(s"Output columns: ${output.map(_.name).mkString(", ")}")
            // scalastyle:on println

            val benchmark = new Benchmark(
              s"serializePartitions ($numPartitions partitions, ${metadata.tasks.size()} tasks)",
              numPartitions,
              output = this.output)

            benchmark.addCase("serializePartitions()") { _ =>
              CometIcebergNativeScan.serializePartitions(originalPlan, output, metadata)
            }

            // Measure serialized size
            val (commonBytes, perPartitionBytes) =
              CometIcebergNativeScan.serializePartitions(originalPlan, output, metadata)

            val totalBytes = commonBytes.length + perPartitionBytes.map(_.length).sum
            val commonKB = commonBytes.length / 1024.0
            val perPartKB = perPartitionBytes.map(_.length).sum / 1024.0
            val totalKB = totalBytes / 1024.0

            // scalastyle:off println
            println(
              f"Serialized size: common=$commonKB%.1f KB, " +
                f"per-partition=$perPartKB%.1f KB, total=$totalKB%.1f KB")
            println(f"Average per partition: ${perPartKB / numPartitions * 1024}%.1f bytes")
            // scalastyle:on println

            benchmark.run()

          case None =>
            // scalastyle:off println
            println("WARNING: Could not find CometIcebergNativeScanExec in query plan")
            println(s"Plan:\n$plan")
          // scalastyle:on println
        }

        spark.sql(s"DROP TABLE IF EXISTS $fullTableName")
      }
    }
  }

  /**
   * Micro-benchmark for reflection operations to isolate their cost.
   */
  def reflectionMicroBenchmark(): Unit = {
    val numOps = 100000
    val benchmark = new Benchmark("Reflection micro-benchmark", numOps, output = output)

    benchmark.addCase("Class.forName() - uncached") { _ =>
      Class.forName("org.apache.iceberg.ContentScanTask")
    }

    val cachedClass = Class.forName("org.apache.iceberg.ContentScanTask")
    benchmark.addCase("Class lookup - cached") { _ =>
      cachedClass.hashCode()
    }

    benchmark.addCase("getMethod() - uncached") { _ =>
      cachedClass.getMethod("file")
    }

    val cachedMethod = cachedClass.getMethod("file")
    benchmark.addCase("Method lookup - cached") { _ =>
      cachedMethod.hashCode()
    }

    benchmark.run()
  }

  override def runCometBenchmark(args: Array[String]): Unit = {
    val numPartitions = if (args.nonEmpty) args(0).toInt else 10000

    // First show the cost of reflection operations
    runBenchmark("Reflection Micro-benchmark") {
      reflectionMicroBenchmark()
    }

    // Then benchmark the full serialization path
    runBenchmark("Iceberg serializePartitions Benchmark") {
      serializePartitionsBenchmark(numPartitions)
    }
  }
}
