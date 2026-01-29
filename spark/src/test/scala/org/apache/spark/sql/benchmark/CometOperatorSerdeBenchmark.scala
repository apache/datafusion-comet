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
import org.apache.spark.sql.comet.{CometBatchScanExec, CometIcebergNativeScanExec}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec

import org.apache.comet.CometConf
import org.apache.comet.serde.OperatorOuterClass
import org.apache.comet.serde.operator.CometIcebergNativeScan

/**
 * Benchmark for operator serialization/deserialization roundtrip performance.
 *
 * This benchmark measures the time to serialize Iceberg FileScanTask objects to protobuf,
 * starting from actual Iceberg Java objects rather than pre-constructed protobuf messages.
 *
 * To run this benchmark:
 * {{{
 * SPARK_GENERATE_BENCHMARK_FILES=1 make \
 *   benchmark-org.apache.spark.sql.benchmark.CometOperatorSerdeBenchmark
 * }}}
 *
 * Results will be written to "spark/benchmarks/CometOperatorSerdeBenchmark-**results.txt".
 */
object CometOperatorSerdeBenchmark extends CometBenchmarkBase {

  // Check if Iceberg is available in classpath
  private def icebergAvailable: Boolean = {
    try {
      Class.forName("org.apache.iceberg.catalog.Catalog")
      true
    } catch {
      case _: ClassNotFoundException => false
    }
  }

  // Helper to create temp directory for Iceberg warehouse
  private def withTempIcebergDir(f: File => Unit): Unit = {
    val dir = Files.createTempDirectory("comet-serde-benchmark").toFile
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

  /**
   * Extracts CometIcebergNativeScanExec from a query plan, unwrapping AQE if present.
   */
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

  /**
   * Reconstructs a CometBatchScanExec from CometIcebergNativeScanExec for benchmarking the
   * conversion process.
   */
  private def reconstructBatchScanExec(
      nativeScan: CometIcebergNativeScanExec): CometBatchScanExec = {
    CometBatchScanExec(
      wrapped = nativeScan.originalPlan,
      runtimeFilters = Seq.empty,
      nativeIcebergScanMetadata = Some(nativeScan.nativeIcebergScanMetadata))
  }

  /**
   * Creates an Iceberg table with the specified number of partitions. Each partition contains one
   * data file.
   */
  private def createPartitionedIcebergTable(
      warehouseDir: File,
      numPartitions: Int,
      tableName: String = "serde_bench_table"): Unit = {
    // Configure Hadoop catalog
    spark.conf.set("spark.sql.catalog.bench_cat", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.bench_cat.type", "hadoop")
    spark.conf.set("spark.sql.catalog.bench_cat.warehouse", warehouseDir.getAbsolutePath)

    val fullTableName = s"bench_cat.db.$tableName"

    // Drop table if exists
    spark.sql(s"DROP TABLE IF EXISTS $fullTableName")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS bench_cat.db")

    // Create partitioned Iceberg table
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

    // Insert data to create the specified number of partitions
    // Use a range to create unique partition values
    // scalastyle:off println
    println(s"Creating Iceberg table with $numPartitions partitions...")
    // scalastyle:on println

    // Insert in batches to avoid memory issues
    val batchSize = 1000
    var partitionsCreated = 0

    while (partitionsCreated < numPartitions) {
      val batchEnd = math.min(partitionsCreated + batchSize, numPartitions)
      val partitionRange = partitionsCreated until batchEnd

      // Create DataFrame with partition data
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
   * Benchmarks the serialization of IcebergScan operator from FileScanTask objects.
   */
  def icebergScanSerdeBenchmark(numPartitions: Int): Unit = {
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

        // Create the partitioned table
        createPartitionedIcebergTable(warehouseDir, numPartitions)

        val fullTableName = "bench_cat.db.serde_bench_table"

        // Plan a query to get the CometIcebergNativeScanExec with FileScanTasks
        val df = spark.sql(s"SELECT * FROM $fullTableName")
        val plan = df.queryExecution.executedPlan

        val nativeScanOpt = extractIcebergNativeScanExec(plan)

        nativeScanOpt match {
          case Some(nativeScan) =>
            // Get metadata and tasks
            val metadata = nativeScan.nativeIcebergScanMetadata
            val tasks = metadata.tasks
            // scalastyle:off println
            println(s"Found ${tasks.size()} FileScanTasks")
            // scalastyle:on println

            // Reconstruct CometBatchScanExec for conversion benchmarking
            val scanExec = reconstructBatchScanExec(nativeScan)

            // Benchmark the serialization
            val iterations = 100
            val benchmark = new Benchmark(
              s"IcebergScan serde ($numPartitions partitions, ${tasks.size()} tasks)",
              iterations,
              output = output)

            // Benchmark: Convert FileScanTasks to protobuf (the convert() method)
            benchmark.addCase("FileScanTask -> Protobuf (convert)") { _ =>
              var i = 0
              while (i < iterations) {
                val builder = OperatorOuterClass.Operator.newBuilder()
                CometIcebergNativeScan.convert(scanExec, builder)
                i += 1
              }
            }

            // Benchmark: Full roundtrip - convert to protobuf and serialize to bytes
            benchmark.addCase("FileScanTask -> Protobuf -> bytes") { _ =>
              var i = 0
              while (i < iterations) {
                val builder = OperatorOuterClass.Operator.newBuilder()
                val operatorOpt = CometIcebergNativeScan.convert(scanExec, builder)
                operatorOpt.foreach(_.toByteArray)
                i += 1
              }
            }

            // Get serialized bytes for deserialization benchmark
            val builder = OperatorOuterClass.Operator.newBuilder()
            val operatorOpt = CometIcebergNativeScan.convert(scanExec, builder)

            operatorOpt match {
              case Some(operator) =>
                val serializedBytes = operator.toByteArray
                val sizeKB = serializedBytes.length / 1024.0
                val sizeMB = sizeKB / 1024.0

                // scalastyle:off println
                println(
                  s"Serialized IcebergScan size: ${f"$sizeKB%.1f"} KB (${f"$sizeMB%.2f"} MB)")
                // scalastyle:on println

                // Benchmark: Deserialize from bytes
                benchmark.addCase("bytes -> Protobuf (parseFrom)") { _ =>
                  var i = 0
                  while (i < iterations) {
                    OperatorOuterClass.Operator.parseFrom(serializedBytes)
                    i += 1
                  }
                }

                // Benchmark: Full roundtrip including deserialization
                benchmark.addCase("Full roundtrip (convert + serialize + deserialize)") { _ =>
                  var i = 0
                  while (i < iterations) {
                    val b = OperatorOuterClass.Operator.newBuilder()
                    val op = CometIcebergNativeScan.convert(scanExec, b)
                    op.foreach { o =>
                      val bytes = o.toByteArray
                      OperatorOuterClass.Operator.parseFrom(bytes)
                    }
                    i += 1
                  }
                }

              case None =>
                // scalastyle:off println
                println("WARNING: convert() returned None, cannot benchmark serialization")
              // scalastyle:on println
            }

            benchmark.run()

          case None =>
            // scalastyle:off println
            println("WARNING: Could not find CometIcebergNativeScanExec in query plan")
            println(s"Plan:\n$plan")
          // scalastyle:on println
        }

        // Cleanup
        spark.sql(s"DROP TABLE IF EXISTS $fullTableName")
      }
    }
  }

  override def runCometBenchmark(args: Array[String]): Unit = {
    val numPartitions = if (args.nonEmpty) args(0).toInt else 30000

    runBenchmark("IcebergScan Operator Serde Benchmark") {
      icebergScanSerdeBenchmark(numPartitions)
    }
  }
}
