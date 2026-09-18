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
 * End-to-end MinIO test for [[HadoopS3ACredentialProviderAdapter]] on the native Parquet path.
 *
 * The delegate is the AWS default credential chain -- a provider class Comet's native Rust list
 * deliberately does NOT recognize. Without the adapter, the native reader fails with `Unsupported
 * credential provider`; a successful read here proves the adapter routed credential resolution
 * through Hadoop S3A instead. This is the regression from the spec's failure report.
 *
 * Credentials are supplied via JVM system properties (the AWS default chain reads them) rather
 * than `fs.s3a.access.key` / `secret.key`, because Comet does not forward those secrets to the
 * SPI.
 */
class HadoopS3ACredentialProviderAdapterBridgeSuite
    extends CometS3TestBase
    with AdaptiveSparkPlanHelper {

  override protected val testBucketName = "hadoop-adapter-bucket"

  // The AWS default-chain FQCN for whichever SDK the active Spark/Hadoop line ships (v2 on Spark
  // 4.x, v1 on 3.x). Neither is in Comet's native provider list.
  private val defaultChainClass: String =
    if (Try(
        Class.forName(
          "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider")).isSuccess) {
      "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider"
    } else {
      "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
    }

  private val savedProps = mutable.Map[String, String]()
  private def setProp(key: String, value: String): Unit = {
    savedProps(key) = System.getProperty(key)
    System.setProperty(key, value)
  }

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set(
      "spark.hadoop.fs.s3a.comet.credential.provider.class",
      classOf[HadoopS3ACredentialProviderAdapter].getName)
    conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", defaultChainClass)
  }

  override def beforeAll(): Unit = {
    // Both the v1 (aws.secretKey) and v2 (aws.secretAccessKey) secret property names are set so the
    // default chain resolves regardless of which SDK is on the classpath.
    setProp("aws.accessKeyId", userName)
    setProp("aws.secretKey", password)
    setProp("aws.secretAccessKey", password)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    savedProps.foreach {
      case (key, null) => System.clearProperty(key)
      case (key, value) => System.setProperty(key, value)
    }
  }

  test(
    "native Parquet read via HadoopS3ACredentialProviderAdapter (AWS default chain delegate)") {
    val path = s"s3a://$testBucketName/data/adapter.parquet"
    val rowCount = 1000L
    spark.range(0, rowCount).write.format("parquet").mode(SaveMode.Overwrite).save(path)
    val expectedSum = (0L until rowCount).sum

    val df = spark.read.format("parquet").load(path).agg(sum(col("id")))
    val plan = df.queryExecution.executedPlan
    assert(cometScans(plan).nonEmpty, s"Expected a Comet Parquet scan in plan:\n$plan")
    // Success is the assertion: it is only reachable if the adapter resolved credentials. Without
    // it, the native reader would throw "Unsupported credential provider: $defaultChainClass".
    assert(df.first().getLong(0) == expectedSum)
  }
}
