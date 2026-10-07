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

package org.apache.comet.csv

import java.net.URI
import java.nio.charset.StandardCharsets

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.comet.CometCsvNativeScanExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.{input_file_name, spark_partition_id}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

import org.apache.comet.{CometConf, CometS3TestBase}

/** Native CSV scans over files in two MinIO buckets. Manual: needs Docker for MinIO. */
class CsvReadFromS3Suite extends CometS3TestBase with AdaptiveSparkPlanHelper {

  override protected val testBucketName = "csv-bucket-a"
  private val secondBucketName = "csv-bucket-b"

  private val csvNative =
    Seq(CometConf.COMET_CSV_V2_NATIVE_ENABLED.key -> "true", SQLConf.USE_V1_SOURCE_LIST.key -> "")

  // Spark packs every file of the scan into one partition.
  private val onePartition = Seq(
    SQLConf.FILES_MIN_PARTITION_NUM.key -> "1",
    SQLConf.FILES_OPEN_COST_IN_BYTES.key -> "1",
    SQLConf.FILES_MAX_PARTITION_BYTES.key -> (128L * 1024 * 1024).toString)

  // Spark gives every file its own partition.
  private val separatePartitions = Seq(
    SQLConf.FILES_MIN_PARTITION_NUM.key -> "1",
    SQLConf.FILES_OPEN_COST_IN_BYTES.key -> (128L * 1024 * 1024).toString,
    SQLConf.FILES_MAX_PARTITION_BYTES.key -> (128L * 1024 * 1024).toString)

  private val schema = new StructType().add("id", IntegerType).add("bucket", StringType)

  private def bucketOf(path: String): String = new URI(path).getAuthority

  private def csvBytes(from: Int, to: Int, tag: String): Array[Byte] =
    (from to to).map(i => s"$i,$tag\n").mkString.getBytes(StandardCharsets.UTF_8)

  /** Puts one file per entry of `names` under each prefix, ids 1 to 3, 4 to 6, and so on. */
  private def putTwoBucketFiles(prefixA: String, prefixB: String, names: Seq[String]): Unit = {
    createBucketIfNotExists(secondBucketName)
    names.zipWithIndex.foreach { case (name, i) =>
      putObject(testBucketName, s"$prefixA/$name", csvBytes(i * 3 + 1, i * 3 + 3, "A"))
      putObject(secondBucketName, s"$prefixB/$name", csvBytes(i * 3 + 1, i * 3 + 3, "B"))
    }
  }

  private def readCsv(paths: String*): DataFrame =
    spark.read.option("header", "false").schema(schema).csv(paths: _*)

  /**
   * Reads `paths` with Comet off and on, and checks that Comet returns Spark's rows through a
   * native CSV scan whose partitions each read from one bucket. `sparkMixesBuckets` states
   * whether Spark's own layout puts both buckets in one partition.
   */
  private def assertMultiBucketRead(paths: Seq[String], sparkMixesBuckets: Boolean): Unit = {
    var expected: Seq[Row] = Nil
    var sparkLayout: Seq[Set[String]] = Nil
    withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      expected = readCsv(paths: _*).collect().toSeq
      sparkLayout = readCsv(paths: _*)
        .select(spark_partition_id(), input_file_name())
        .distinct()
        .collect()
        .groupBy(_.getInt(0))
        .values
        .map(_.map(row => bucketOf(row.getString(1))).toSet)
        .toSeq
    }
    assert(
      sparkLayout.exists(_.size > 1) == sparkMixesBuckets,
      s"Spark's partitions read buckets $sparkLayout")

    val df = readCsv(paths: _*)
    val rows = df.collect().toSeq
    val plan = df.queryExecution.executedPlan
    val scans = collect(plan) { case scan: CometCsvNativeScanExec => scan }
    assert(scans.size == 1, s"expected one native CSV scan:\n$plan")
    val partitions = scans.head.nativeOp.getCsvScan.getFilePartitionsList.asScala.toSeq
    val cometLayout =
      partitions.map(_.getPartitionedFileList.asScala.map(f => bucketOf(f.getFilePath)).toSet)
    assert(cometLayout.forall(_.size == 1), s"Comet's partitions read buckets $cometLayout")
    assert(scans.head.outputPartitioning.numPartitions == partitions.size)
    if (sparkMixesBuckets) {
      assert(partitions.size > sparkLayout.size, "Comet splits Spark's mixed partitions")
    }
    assert(rows.map(_.toString).sorted == expected.map(_.toString).sorted)
  }

  private def a(path: String): String = s"s3a://$testBucketName/$path"
  private def b(path: String): String = s"s3a://$secondBucketName/$path"

  test("native CSV scan over two buckets holding the same keys in one Spark partition") {
    putTwoBucketFiles("csv-same", "csv-same", Seq("f1.csv", "f2.csv"))
    withSQLConf(csvNative ++ onePartition: _*) {
      assertMultiBucketRead(Seq(a("csv-same"), b("csv-same")), sparkMixesBuckets = true)
    }
  }

  test("native CSV scan over two buckets with distinct keys in one Spark partition") {
    putTwoBucketFiles("csv-a", "csv-b", Seq("f1.csv", "f2.csv"))
    withSQLConf(csvNative ++ onePartition: _*) {
      assertMultiBucketRead(Seq(a("csv-a"), b("csv-b")), sparkMixesBuckets = true)
    }
  }

  test("native CSV scan over two buckets in one Spark partition, without adaptive execution") {
    putTwoBucketFiles("csv-same", "csv-same", Seq("f1.csv", "f2.csv"))
    withSQLConf(
      csvNative ++ onePartition :+ (SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false"): _*) {
      assertMultiBucketRead(Seq(a("csv-same"), b("csv-same")), sparkMixesBuckets = true)
    }
  }

  test("native CSV scan over two buckets holding the same keys in separate partitions") {
    putTwoBucketFiles("csv1-same", "csv1-same", Seq("f.csv"))
    withSQLConf(csvNative ++ separatePartitions: _*) {
      assertMultiBucketRead(Seq(a("csv1-same"), b("csv1-same")), sparkMixesBuckets = false)
    }
  }

  test("native CSV scan over two buckets with distinct keys in separate partitions") {
    putTwoBucketFiles("csv1-a", "csv1-b", Seq("f.csv"))
    withSQLConf(csvNative ++ separatePartitions: _*) {
      assertMultiBucketRead(Seq(a("csv1-a"), b("csv1-b")), sparkMixesBuckets = false)
    }
  }

  test("native CSV scan over two buckets with the second bucket listed first") {
    putTwoBucketFiles("csv1-a", "csv1-b", Seq("f.csv"))
    withSQLConf(csvNative ++ separatePartitions: _*) {
      assertMultiBucketRead(Seq(b("csv1-b"), a("csv1-a")), sparkMixesBuckets = false)
    }
  }
}
