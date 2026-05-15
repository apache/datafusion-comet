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

import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.comet.{CometIcebergNativeScanExec, CometNativeScanExec, CometScanExec}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.{col, sum}

import org.apache.comet.{CometConf, CometS3TestBase}

/**
 * End-to-end test that exercises [[CometS3CredentialDispatcher]] and the Rust JNI bridge against
 * a real S3 server (Minio). Asserts the test SPI was actually invoked rather than the default AWS
 * credential chain.
 */
class CometS3CredentialBridgeSuite extends CometS3TestBase with AdaptiveSparkPlanHelper {

  override protected val testBucketName = "bridge-test-bucket"

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    val providerClassName = classOf[MinioCometS3CredentialProvider].getName
    // Activate the bridge for the Parquet (object_store) path via the Hadoop S3A namespace.
    conf.set("spark.hadoop.fs.s3a.comet.credential.provider.class", providerClassName)
    // Activate the bridge for the Iceberg (opendal) path via the per-catalog s3 namespace.
    conf.set("spark.sql.catalog.s3_catalog", "org.apache.iceberg.spark.SparkCatalog")
    conf.set("spark.sql.catalog.s3_catalog.type", "hadoop")
    conf.set("spark.sql.catalog.s3_catalog.warehouse", s"s3a://$testBucketName/warehouse")
    conf.set("spark.sql.catalog.s3_catalog.s3.comet.credential.provider.class", providerClassName)
    applyS3CatalogProps(conf, "s3_catalog")
    conf.set(CometConf.COMET_ICEBERG_NATIVE_ENABLED.key, "true")
    conf
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    MinioCometS3CredentialProvider.installCredentials(userName, password)
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

  test("Parquet read on S3 routes credentials through CometS3CredentialProvider") {
    val testFilePath = s"s3a://$testBucketName/data/bridge-parquet.parquet"
    val rowCount = 1000L
    spark.range(0, rowCount).write.format("parquet").mode(SaveMode.Overwrite).save(testFilePath)
    val expectedSum = (0L until rowCount).sum

    MinioCometS3CredentialProvider.resetCounters()
    val df = spark.read.format("parquet").load(testFilePath).agg(sum(col("id")))
    assertHasCometParquetScan(df.queryExecution.executedPlan)
    assert(df.first().getLong(0) == expectedSum)

    assert(
      MinioCometS3CredentialProvider.callCount() > 0,
      "Bridge was not invoked during Comet Parquet read")
    assert(
      MinioCometS3CredentialProvider.lastBucket() == testBucketName,
      s"Bridge received unexpected bucket: ${MinioCometS3CredentialProvider.lastBucket()}")
  }

  test("Iceberg read on S3 routes credentials through CometS3CredentialProvider") {
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

    MinioCometS3CredentialProvider.resetCounters()
    val df = spark.sql("SELECT * FROM s3_catalog.db.bridge_iceberg ORDER BY id")
    assertHasCometIcebergScan(df.queryExecution.executedPlan)
    assert(df.count() == 3)

    assert(
      MinioCometS3CredentialProvider.callCount() > 0,
      "Bridge was not invoked during Comet Iceberg read")

    spark.sql("DROP TABLE s3_catalog.db.bridge_iceberg")
  }
}
