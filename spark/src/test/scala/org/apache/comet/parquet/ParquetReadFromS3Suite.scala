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

package org.apache.comet.parquet

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.comet.{CometNativeScanExec, CometScanExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.{col, expr, max, sum}

import org.apache.comet.CometS3TestBase

class ParquetReadFromS3Suite extends CometS3TestBase with AdaptiveSparkPlanHelper {

  override protected val testBucketName = "test-bucket"

  private def writeTestParquetFile(filePath: String): Unit = {
    val df = spark.range(0, 1000)
    df.write.format("parquet").mode(SaveMode.Overwrite).save(filePath)
  }

  private def writePartitionedParquetFile(filePath: String): Unit = {
    val df = spark.range(0, 1000).withColumn("val", expr("concat('val#', id % 10)"))
    df.write.format("parquet").partitionBy("val").mode(SaveMode.Overwrite).save(filePath)
  }

  private def assertCometScan(df: DataFrame): Unit = {
    val scans = collect(df.queryExecution.executedPlan) {
      case p: CometScanExec => p
      case p: CometNativeScanExec => p
    }
    assert(scans.size == 1)
  }

  test("read parquet file from MinIO") {
    val testFilePath = s"s3a://$testBucketName/data/test-file.parquet"
    writeTestParquetFile(testFilePath)

    val df = spark.read.format("parquet").load(testFilePath).agg(sum(col("id")))
    assertCometScan(df)
    assert(df.first().getLong(0) == 499500)
  }

  test("read partitioned parquet file from MinIO") {
    val testFilePath = s"s3a://$testBucketName/data/test-partitioned-file.parquet"
    writePartitionedParquetFile(testFilePath)

    val df = spark.read.format("parquet").load(testFilePath).agg(sum(col("id")), max(col("val")))
    val firstRow = df.first()
    assert(firstRow.getLong(0) == 499500)
    assert(firstRow.getString(1) == "val#9")
  }

  test("read parquet file from MinIO with URL escape sequences in path") {
    // Path with '%23' and '%20' which are URL escape sequences for '#' and ' '
    val testFilePath = s"s3a://$testBucketName/data/Brand%2321/test%20file.parquet"
    writeTestParquetFile(testFilePath)

    val df = spark.read.format("parquet").load(testFilePath).agg(sum(col("id")))
    assertCometScan(df)
    assert(df.first().getLong(0) == 499500)
  }
}
