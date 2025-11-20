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

import java.net.URI

import scala.util.Try

import org.testcontainers.containers.MinIOContainer
import org.testcontainers.utility.DockerImageName

import org.apache.spark.SparkConf
import org.apache.spark.sql.CometTestBase

import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{CreateBucketRequest, HeadBucketRequest}

trait CometS3TestBase extends CometTestBase {

  protected var minioContainer: MinIOContainer = _
  protected val userName = "minio-test-user"
  protected val password = "minio-test-password"

  protected def testBucketName: String

  override def beforeAll(): Unit = {
    minioContainer = new MinIOContainer(DockerImageName.parse("minio/minio:latest"))
      .withUserName(userName)
      .withPassword(password)
    minioContainer.start()
    createBucketIfNotExists(testBucketName)

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

  protected def createBucketIfNotExists(bucketName: String): Unit = {
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
}
