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

package org.apache.comet.cloud.s3

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import scala.util.Try

import org.apache.logging.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.{col, sum}

import org.apache.comet.CometS3TestBase

/**
 * End-to-end MinIO tests for Hadoop's `ProfileAWSCredentialsProvider` on the native Parquet path
 * with no `comet.credential.provider.class` configured. The native store picks
 * [[HadoopS3ACredentialProviderAdapter]] on its own, so Hadoop reads the profile.
 *
 * The credentials file holds wrong keys under `[default]` and the MinIO keys under the named
 * profile, so a read that ignored the profile name fails. Spark's own S3A writes and lists
 * through the same profile.
 */
class HadoopProfileCredentialsProviderSuite extends CometS3TestBase with AdaptiveSparkPlanHelper {

  override protected val testBucketName = "hadoop-profile-bucket"
  private val explicitClassBucket = "hadoop-profile-explicit-bucket"

  private val profileProvider = "org.apache.hadoop.fs.s3a.auth.ProfileAWSCredentialsProvider"
  private val profileName = "comet-test"

  private var credentialsFile: Path = _

  /**
   * Written before the base class starts MinIO and Spark, so a failure here aborts only this
   * suite and leaks nothing. The test JVM's java.io.tmpdir may not exist yet, so it is created
   * first; `Files.createTempFile` gives the file owner-only permissions.
   */
  private def writeCredentialsFile(): Path = {
    val dir = Files.createDirectories(Paths.get(System.getProperty("java.io.tmpdir")))
    val file = Files.createTempFile(dir, "comet-profile-credentials", ".ini")
    val content =
      s"""[default]
         |aws_access_key_id = WRONG-DEFAULT-ACCESS-KEY
         |aws_secret_access_key = WRONG-DEFAULT-SECRET-KEY
         |[$profileName]
         |aws_access_key_id = $userName
         |aws_secret_access_key = $password
         |""".stripMargin
    Files.write(file, content.getBytes(StandardCharsets.UTF_8))
    file
  }

  private def assumeProfileProviderAvailable(): Unit =
    assume(Try(Class.forName(profileProvider)).isSuccess, "needs hadoop-aws 3.4.2 or later")

  private def setProfile(conf: SparkConf, bucket: String): Unit = {
    conf.set(s"spark.hadoop.fs.s3a.bucket.$bucket.aws.credentials.provider", profileProvider)
    conf.set(
      s"spark.hadoop.fs.s3a.bucket.$bucket.auth.profile.file",
      credentialsFile.toAbsolutePath.toString)
    conf.set(s"spark.hadoop.fs.s3a.bucket.$bucket.auth.profile.name", profileName)
  }

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    setProfile(conf, testBucketName)
    setProfile(conf, explicitClassBucket)
    conf.set(
      s"spark.hadoop.fs.s3a.bucket.$explicitClassBucket.comet.credential.provider.class",
      classOf[MinioCometS3CredentialProvider].getName)
    conf
  }

  override def beforeAll(): Unit = {
    credentialsFile = writeCredentialsFile()
    super.beforeAll()
    MinioCometS3CredentialProvider.installCredentials(userName, password)
  }

  override def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      if (credentialsFile != null) {
        Files.deleteIfExists(credentialsFile)
      }
    }
  }

  private def writeAndSum(bucket: String, rowCount: Long): Unit = {
    val path = s"s3a://$bucket/data/profile.parquet"
    spark.range(0, rowCount).write.format("parquet").mode(SaveMode.Overwrite).save(path)
    val df = spark.read.format("parquet").load(path).agg(sum(col("id")))
    val plan = df.queryExecution.executedPlan
    assert(cometScans(plan).nonEmpty, s"Expected a Comet Parquet scan in plan:\n$plan")
    assert(df.first().getLong(0) == (0L until rowCount).sum)
  }

  test("native Parquet read resolves Hadoop's profile provider through the S3A adapter") {
    assumeProfileProviderAvailable()
    val appender = new LogAppender("Comet S3 credential provider instantiation")
    withLogAppender(
      appender,
      Seq(classOf[CometS3CredentialDispatcher].getName),
      Some(Level.INFO)) {
      writeAndSum(testBucketName, 1000L)
    }
    val messages = appender.loggingEvents.map(_.getMessage.getFormattedMessage)
    val instantiated = "Instantiated CometS3CredentialProvider " +
      classOf[HadoopS3ACredentialProviderAdapter].getName
    assert(
      messages.exists(_.contains(instantiated)),
      s"Expected the native read to use the adapter, got:\n${messages.mkString("\n")}")
  }

  test("an explicit Comet provider class wins over the implicit adapter") {
    assumeProfileProviderAvailable()
    createBucketIfNotExists(explicitClassBucket)
    MinioCometS3CredentialProvider.resetCounters()
    writeAndSum(explicitClassBucket, 500L)
    assert(
      MinioCometS3CredentialProvider.callCount() > 0,
      "Expected the configured provider class to serve the native read")
  }
}
