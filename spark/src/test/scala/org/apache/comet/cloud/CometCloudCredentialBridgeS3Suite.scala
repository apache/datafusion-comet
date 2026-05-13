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

package org.apache.comet.cloud

import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.comet.{CometIcebergNativeScanExec, CometNativeScanExec, CometScanExec}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.{col, sum}

import org.apache.comet.{CometConf, CometS3TestBase}

/**
 * End-to-end test that exercises [[CometCloudCredentialDispatcher]] and the Rust JNI bridge
 * against a real S3 server (Minio). The test [[MinioCometCredentialProvider]] is registered via
 * `META-INF/services` and returns the harness's Minio credentials; success here proves Comet's
 * native scan paths actually invoked the SPI rather than falling back to the default AWS chain.
 *
 * Note: because [[CometCloudCredentialDispatcher.PROVIDER]] is a `static final` initialized once
 * per JVM, registering this test SPI affects every test in the same JVM. Other Minio suites (e.g.
 * ParquetReadFromS3Suite, IcebergReadFromS3Suite) continue to pass because the test provider
 * returns the same Minio credentials they would have used through the default chain.
 */
class CometCloudCredentialBridgeS3Suite extends CometS3TestBase with AdaptiveSparkPlanHelper {

  override protected val testBucketName = "bridge-test-bucket"

  private def icebergAvailable: Boolean = {
    try {
      Class.forName("org.apache.iceberg.catalog.Catalog")
      true
    } catch {
      case _: ClassNotFoundException => false
    }
  }

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set("spark.sql.catalog.s3_catalog", "org.apache.iceberg.spark.SparkCatalog")
    conf.set("spark.sql.catalog.s3_catalog.type", "hadoop")
    conf.set("spark.sql.catalog.s3_catalog.warehouse", s"s3a://$testBucketName/warehouse")
    // Comet's native Iceberg reader uses iceberg-rust + opendal which requires explicit S3 config
    // when a custom credential loader is wired in (opendal skips its default region-detection
    // path in that case). The Hadoop catalog above doesn't propagate these, so we set them
    // directly on the catalog config.
    conf.set("spark.sql.catalog.s3_catalog.s3.endpoint", minioContainer.getS3URL)
    conf.set("spark.sql.catalog.s3_catalog.s3.region", "us-east-1")
    conf.set("spark.sql.catalog.s3_catalog.s3.path-style-access", "true")
    conf.set(CometConf.COMET_ICEBERG_NATIVE_ENABLED.key, "true")
    conf
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    MinioCometCredentialProvider.installCredentials(userName, password)
  }

  private def assertHasCometParquetScan(plan: SparkPlan): Unit = {
    val scans = collect(plan) {
      case p: CometScanExec => p
      case p: CometNativeScanExec => p
    }
    assert(scans.nonEmpty, s"Expected at least one Comet Parquet scan in plan:\n$plan")
  }

  private def assertHasCometIcebergScan(plan: SparkPlan): Unit = {
    val scans = collect(plan) { case p: CometIcebergNativeScanExec => p }
    assert(scans.nonEmpty, s"Expected at least one CometIcebergNativeScanExec in plan:\n$plan")
  }

  test("Parquet read on S3 routes credentials through CometCloudCredentialProvider") {
    val testFilePath = s"s3a://$testBucketName/data/bridge-parquet.parquet"
    spark.range(0, 1000).write.format("parquet").mode(SaveMode.Overwrite).save(testFilePath)

    MinioCometCredentialProvider.resetCounters()
    val df = spark.read.format("parquet").load(testFilePath).agg(sum(col("id")))
    assertHasCometParquetScan(df.queryExecution.executedPlan)
    assert(df.first().getLong(0) == 499500)

    assert(
      MinioCometCredentialProvider.callCount() > 0,
      "Bridge was not invoked during Comet Parquet read")
    assert(
      MinioCometCredentialProvider.lastBucket() == testBucketName,
      s"Bridge received unexpected bucket: ${MinioCometCredentialProvider.lastBucket()}")
  }

  test("Iceberg read on S3 routes credentials through CometCloudCredentialProvider") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    spark.sql("""
      CREATE TABLE s3_catalog.db.bridge_iceberg (
        id INT,
        name STRING
      ) USING iceberg
    """)

    spark.sql("""
      INSERT INTO s3_catalog.db.bridge_iceberg
      VALUES (1, 'a'), (2, 'b'), (3, 'c')
    """)

    MinioCometCredentialProvider.resetCounters()
    val df = spark.sql("SELECT * FROM s3_catalog.db.bridge_iceberg ORDER BY id")
    assertHasCometIcebergScan(df.queryExecution.executedPlan)
    assert(df.count() == 3)

    assert(
      MinioCometCredentialProvider.callCount() > 0,
      "Bridge was not invoked during Comet Iceberg read")

    spark.sql("DROP TABLE s3_catalog.db.bridge_iceberg")
  }
}
