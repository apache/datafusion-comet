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

import scala.collection.mutable
import scala.util.Try

import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.{col, sum}

import org.apache.comet.CometS3TestBase

/**
 * End-to-end MinIO tests for [[HadoopS3ACredentialProviderAdapter]] on the native Parquet path,
 * using a delegate the native Rust list deliberately rejects. A successful read proves the
 * adapter routed credential resolution through Hadoop S3A rather than the native reader failing
 * with `Unsupported credential provider`.
 *
 * The second case pins the forwarding specifically: it hands the real keys only as relation
 * options (which reach the native scan's forwarded map but not the `SparkConf` seed that
 * `AdapterSupport.toConfiguration` reads) while the seed carries wrong per-bucket keys, so the
 * read succeeds only if the static keys actually crossed JNI and overlaid the seed.
 */
class HadoopS3ACredentialProviderAdapterBridgeSuite
    extends CometS3TestBase
    with AdaptiveSparkPlanHelper {

  override protected val testBucketName = "hadoop-adapter-bucket"
  private val staticKeyBucket = "hadoop-adapter-static-bucket"

  // The AWS default-chain FQCN must match what the active Hadoop-aws line's provider factory
  // accepts, not merely which SDK jar is on the test classpath: the v2 SDK is present on the
  // Spark 3.x test classpath too (Iceberg's S3 test deps), but Hadoop 3.3.4's factory only accepts
  // the v1 interface. CredentialProviderListFactory exists only in Hadoop 3.4+ (the v2 line), so
  // its presence is the reliable per-profile signal. Neither class is in Comet's native list.
  private val defaultChainClass: String =
    if (Try(
        Class.forName("org.apache.hadoop.fs.s3a.auth.CredentialProviderListFactory")).isSuccess) {
      "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider"
    } else {
      "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
    }

  private val savedProps = mutable.Map[String, String]()
  private def setProp(key: String, value: String): Unit = {
    savedProps(key) = System.getProperty(key)
    System.setProperty(key, value)
  }
  private def restoreProps(): Unit = {
    savedProps.foreach {
      case (key, null) => System.clearProperty(key)
      case (key, value) => System.setProperty(key, value)
    }
    savedProps.clear()
  }

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set(
      "spark.hadoop.fs.s3a.comet.credential.provider.class",
      classOf[HadoopS3ACredentialProviderAdapter].getName)
    // Disable the S3A FileSystem cache so the static-key test's Spark write builds a fresh
    // FileSystem from the relation options rather than reusing one cached from the poisoned seed.
    conf.set("spark.hadoop.fs.s3a.impl.disable.cache", "true")
    // Default bucket: delegate to the AWS default chain (credentials come from JVM system
    // properties, set within the test that uses it).
    conf.set(
      s"spark.hadoop.fs.s3a.bucket.$testBucketName.aws.credentials.provider",
      defaultChainClass)
    // Static-key bucket: SimpleAWSCredentialsProvider with deliberately WRONG per-bucket keys in
    // the seed (SparkConf spark.hadoop.*). The real keys are supplied as relation options in the
    // test, which reach only the forwarded map, so the read resolves correctly only if forwarding
    // overlaid the seed.
    conf.set(
      s"spark.hadoop.fs.s3a.bucket.$staticKeyBucket.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    conf.set(s"spark.hadoop.fs.s3a.bucket.$staticKeyBucket.access.key", "WRONG-SEED-ACCESS-KEY")
    conf.set(s"spark.hadoop.fs.s3a.bucket.$staticKeyBucket.secret.key", "WRONG-SEED-SECRET-KEY")
    conf
  }

  test(
    "native Parquet read via HadoopS3ACredentialProviderAdapter (AWS default chain delegate)") {
    // Both the v1 (aws.secretKey) and v2 (aws.secretAccessKey) secret property names are set so the
    // default chain resolves regardless of which SDK is on the classpath.
    setProp("aws.accessKeyId", userName)
    setProp("aws.secretKey", password)
    setProp("aws.secretAccessKey", password)
    try {
      val path = s"s3a://$testBucketName/data/adapter.parquet"
      val rowCount = 1000L
      spark.range(0, rowCount).write.format("parquet").mode(SaveMode.Overwrite).save(path)
      val expectedSum = (0L until rowCount).sum

      val df = spark.read.format("parquet").load(path).agg(sum(col("id")))
      val plan = df.queryExecution.executedPlan
      assert(cometScans(plan).nonEmpty, s"Expected a Comet Parquet scan in plan:\n$plan")
      // Success is only reachable if the adapter resolved credentials; otherwise the native reader
      // throws "Unsupported credential provider: $defaultChainClass".
      assert(df.first().getLong(0) == expectedSum)
    } finally {
      restoreProps()
    }
  }

  test("native Parquet read forwards static keys end to end through the adapter") {
    createBucketIfNotExists(staticKeyBucket)
    // Real keys only as relation options: these reach the native scan's forwarded map (and the
    // Spark write's job conf) but not the SparkConf seed, which holds wrong per-bucket keys. If
    // forwarding dropped the static keys the adapter would resolve the wrong seed keys and 403.
    val credOptions = Map(
      s"fs.s3a.bucket.$staticKeyBucket.access.key" -> userName,
      s"fs.s3a.bucket.$staticKeyBucket.secret.key" -> password)
    val path = s"s3a://$staticKeyBucket/data/static.parquet"
    val rowCount = 500L
    spark
      .range(0, rowCount)
      .write
      .options(credOptions)
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save(path)
    val expectedSum = (0L until rowCount).sum

    val df = spark.read.options(credOptions).format("parquet").load(path).agg(sum(col("id")))
    val plan = df.queryExecution.executedPlan
    assert(cometScans(plan).nonEmpty, s"Expected a Comet Parquet scan in plan:\n$plan")
    assert(df.first().getLong(0) == expectedSum)
  }
}
