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

import java.net.URI

import scala.util.Try

import org.testcontainers.containers.MinIOContainer
import org.testcontainers.utility.DockerImageName

import org.apache.spark.SparkConf
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.comet.CometNativeScanExec
import org.apache.spark.sql.comet.CometScanExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.{col, sum}

import org.apache.comet.CometConf.SCAN_NATIVE_ICEBERG_COMPAT

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.CreateBucketRequest
import software.amazon.awssdk.services.s3.model.HeadBucketRequest

class ParquetReadFromS3Suite extends CometTestBase with AdaptiveSparkPlanHelper {

  private var minioContainer: MinIOContainer = _
  private val userName = "minio-test-user"
  private val password = "minio-test-password"
  private val testBucketName = "test-bucket"

  override def beforeAll(): Unit = {
    // Start MinIO container
    minioContainer = new MinIOContainer(DockerImageName.parse("minio/minio:latest"))
      .withUserName(userName)
      .withPassword(password)
    minioContainer.start()
    createBucketIfNotExists(testBucketName)

    // Initialize Spark session
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    if (minioContainer != null) {
      minioContainer.stop()
    }
  }

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set("spark.hadoop.fs.s3a.access.key", userName)
    conf.set("spark.hadoop.fs.s3a.secret.key", password)
    conf.set("spark.hadoop.fs.s3a.endpoint", minioContainer.getS3URL)
    conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
  }

  private def createBucketIfNotExists(bucketName: String): Unit = {
    val credentials = AwsBasicCredentials.create(userName, password)
    val s3Client = S3Client
      .builder()
      .endpointOverride(URI.create(minioContainer.getS3URL))
      .credentialsProvider(StaticCredentialsProvider.create(credentials))
      .forcePathStyle(true)
      .build()
    try {
      val bucketExists = Try {
        s3Client.headBucket(HeadBucketRequest.builder().bucket(bucketName).build())
        true
      }.getOrElse(false)

      if (!bucketExists) {
        val request = CreateBucketRequest.builder().bucket(bucketName).build()
        s3Client.createBucket(request)
      }
    } finally {
      s3Client.close()
    }
  }

  private def writeTestParquetFile(filePath: String): Unit = {
    val df = spark.range(0, 1000)
    df.write.format("parquet").mode(SaveMode.Overwrite).save(filePath)
  }

  // native_iceberg_compat mode does not have comprehensive S3 support, so we don't run tests
  // under this mode.
  if (sys.env.getOrElse("COMET_PARQUET_SCAN_IMPL", "") != SCAN_NATIVE_ICEBERG_COMPAT) {
    test("read parquet file from MinIO") {
      val testFilePath = s"s3a://$testBucketName/data/test-file.parquet"
      writeTestParquetFile(testFilePath)

      val df = spark.read.format("parquet").load(testFilePath).agg(sum(col("id")))
      val scans = collect(df.queryExecution.executedPlan) {
        case p: CometScanExec =>
          p
        case p: CometNativeScanExec =>
          p
      }
      assert(scans.size == 1)

      assert(df.first().getLong(0) == 499500)
    }
  }
}
