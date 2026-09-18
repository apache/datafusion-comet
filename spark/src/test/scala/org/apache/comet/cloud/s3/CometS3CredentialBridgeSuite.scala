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
import org.apache.spark.sql.comet.CometIcebergNativeScanExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.{col, sum}

import org.apache.comet.{CometConf, CometS3TestBase}
import org.apache.comet.iceberg.RESTCatalogHelper

/**
 * End-to-end test that exercises [[CometS3CredentialDispatcher]] and the Rust JNI bridge against
 * a real S3 server (Minio). Asserts the test SPI was actually invoked rather than the default AWS
 * credential chain.
 */
class CometS3CredentialBridgeSuite
    extends CometS3TestBase
    with AdaptiveSparkPlanHelper
    with RESTCatalogHelper {

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

  private def assertHasCometParquetScan(plan: SparkPlan): Unit =
    assert(cometScans(plan).nonEmpty, s"Expected at least one Comet Parquet scan in plan:\n$plan")

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

  test("REST catalog vended creds reach CometS3CredentialProvider.initialize via JNI") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    // Mix storage-prefixed creds (which the old filter would have allowed through) with a
    // non-storage-prefixed sentinel key. The sentinel only reaches initialize(Map) if Comet
    // forwards the unfiltered FileIO property bag end-to-end.
    val vendedCreds = Map(
      "s3.access-key-id" -> userName,
      "s3.secret-access-key" -> password,
      "s3.endpoint" -> minioContainer.getS3URL,
      "s3.region" -> "us-east-1",
      "s3.path-style-access" -> "true",
      "comet.test-tenant-id" -> "tenant-A")
    val warehouse = s"s3a://$testBucketName/warehouse-rest-spi"
    val providerClassName = classOf[MinioCometS3CredentialProvider].getName

    withRESTCatalog(vendedCredentials = vendedCreds, warehouseLocation = Some(warehouse)) {
      (restUri, _, _) =>
        withSQLConf(
          "spark.sql.catalog.rest_spi" -> "org.apache.iceberg.spark.SparkCatalog",
          "spark.sql.catalog.rest_spi.catalog-impl" -> "org.apache.iceberg.rest.RESTCatalog",
          "spark.sql.catalog.rest_spi.uri" -> restUri,
          "spark.sql.catalog.rest_spi.warehouse" -> warehouse,
          "spark.sql.catalog.rest_spi.s3.comet.credential.provider.class" -> providerClassName) {

          spark.sql("CREATE NAMESPACE rest_spi.db")
          spark.sql("CREATE TABLE rest_spi.db.tbl (id INT) USING iceberg")
          spark.sql("INSERT INTO rest_spi.db.tbl VALUES (1), (2), (3)")

          MinioCometS3CredentialProvider.resetCounters()
          val df = spark.sql("SELECT * FROM rest_spi.db.tbl ORDER BY id")
          assertHasCometIcebergScan(df.queryExecution.executedPlan)
          assert(df.count() == 3)

          assert(
            MinioCometS3CredentialProvider.callCount() > 0,
            "Bridge was not invoked during REST catalog read")

          val captured = MinioCometS3CredentialProvider.lastInitProperties()
          assert(captured != null, "initialize(Map) was never called")
          assert(
            captured.containsKey("comet.test-tenant-id"),
            s"Unfiltered FileIO key did not reach initialize(Map). Captured keys: ${captured
                .keySet()}")
          assert(captured.get("comet.test-tenant-id") == "tenant-A")

          spark.sql("DROP TABLE rest_spi.db.tbl")
          spark.sql("DROP NAMESPACE rest_spi.db")
        }
    }
  }

  test(
    "two catalogs sharing one provider FQCN get isolated CometS3CredentialProvider instances") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    val providerClassName = classOf[MinioCometS3CredentialProvider].getName
    val s3Endpoint = minioContainer.getS3URL

    withSQLConf(
      "spark.sql.catalog.iso_a" -> "org.apache.iceberg.spark.SparkCatalog",
      "spark.sql.catalog.iso_a.type" -> "hadoop",
      "spark.sql.catalog.iso_a.warehouse" -> s"s3a://$testBucketName/iso-warehouse-a",
      "spark.sql.catalog.iso_a.s3.comet.credential.provider.class" -> providerClassName,
      "spark.sql.catalog.iso_a.s3.endpoint" -> s3Endpoint,
      "spark.sql.catalog.iso_a.s3.region" -> "us-east-1",
      "spark.sql.catalog.iso_a.s3.path-style-access" -> "true",
      s"spark.sql.catalog.iso_a.${MinioCometS3CredentialProvider.TEST_INSTANCE_TAG}" -> "tag-A",
      "spark.sql.catalog.iso_b" -> "org.apache.iceberg.spark.SparkCatalog",
      "spark.sql.catalog.iso_b.type" -> "hadoop",
      "spark.sql.catalog.iso_b.warehouse" -> s"s3a://$testBucketName/iso-warehouse-b",
      "spark.sql.catalog.iso_b.s3.comet.credential.provider.class" -> providerClassName,
      "spark.sql.catalog.iso_b.s3.endpoint" -> s3Endpoint,
      "spark.sql.catalog.iso_b.s3.region" -> "us-east-1",
      "spark.sql.catalog.iso_b.s3.path-style-access" -> "true",
      s"spark.sql.catalog.iso_b.${MinioCometS3CredentialProvider.TEST_INSTANCE_TAG}" -> "tag-B") {

      MinioCometS3CredentialProvider.resetCounters()

      spark.sql("CREATE TABLE iso_a.db.t (id INT) USING iceberg")
      spark.sql("INSERT INTO iso_a.db.t VALUES (1), (2)")
      spark.sql("CREATE TABLE iso_b.db.t (id INT) USING iceberg")
      spark.sql("INSERT INTO iso_b.db.t VALUES (10), (20), (30)")

      val dfA = spark.sql("SELECT * FROM iso_a.db.t ORDER BY id")
      assertHasCometIcebergScan(dfA.queryExecution.executedPlan)
      assert(dfA.count() == 2)

      val dfB = spark.sql("SELECT * FROM iso_b.db.t ORDER BY id")
      assertHasCometIcebergScan(dfB.queryExecution.executedPlan)
      assert(dfB.count() == 3)

      // Two distinct dispatchKeys (catalog names "iso_a" and "iso_b") must yield two separate
      // initialize(Map) calls even though the provider FQCN is shared.
      assert(
        MinioCometS3CredentialProvider.initCount() >= 2,
        "Expected at least 2 initialize() calls across two catalogs, got " +
          s"${MinioCometS3CredentialProvider.initCount()}")

      val initA = MinioCometS3CredentialProvider.initPropertiesForTag("tag-A")
      val initB = MinioCometS3CredentialProvider.initPropertiesForTag("tag-B")
      assert(initA != null, "initialize(Map) for catalog iso_a never landed in INIT_BY_TAG")
      assert(initB != null, "initialize(Map) for catalog iso_b never landed in INIT_BY_TAG")
      assert(
        initA.get(MinioCometS3CredentialProvider.TEST_INSTANCE_TAG) == "tag-A",
        s"iso_a init map tagged wrong: ${initA.get(MinioCometS3CredentialProvider.TEST_INSTANCE_TAG)}")
      assert(
        initB.get(MinioCometS3CredentialProvider.TEST_INSTANCE_TAG) == "tag-B",
        s"iso_b init map tagged wrong: ${initB.get(MinioCometS3CredentialProvider.TEST_INSTANCE_TAG)}")

      spark.sql("DROP TABLE iso_a.db.t")
      spark.sql("DROP TABLE iso_b.db.t")
    }
  }

  // ---------------------------------------------------------------------
  // Scope-aware object_store cache scenarios
  //
  // The Rust unit tests in native/core/src/parquet/objectstore/retry.rs cover the retry-on-403
  // wrapper end-to-end (pass-through, rebuild-then-retry, second-403 propagation, idempotency,
  // error surface, 401-not-retried, Send+Sync, Arc<Mutex> composition). Minio does not enforce
  // per-prefix denial out of the box, so the full "overreport-then-403-recover" path is proven
  // in Rust rather than here. The two IT scenarios below exercise the Java/JNI + native-cache
  // plumbing: the scope hint reaches Rust and the cache preserves scope granularity.
  // ---------------------------------------------------------------------

  test("scoped provider: two reads inside the same scope share one object_store entry") {
    val scopePrefix = s"s3a://$testBucketName/scope-share/"
    val fileA = scopePrefix + "a.parquet"
    val fileB = scopePrefix + "b.parquet"

    spark.range(0, 100).write.format("parquet").mode(SaveMode.Overwrite).save(fileA)
    spark.range(100, 200).write.format("parquet").mode(SaveMode.Overwrite).save(fileB)

    // Both files fall under the same advisory scope prefix. The native cache should key on the
    // (bucket, prefix-set) tuple and reuse the same object_store entry for both reads.
    MinioCometS3CredentialProvider.resetCounters()
    MinioCometS3CredentialProvider.installPolicyLocations(
      java.util.List.of(scopePrefix))

    val dfA = spark.read.format("parquet").load(fileA).agg(sum(col("id")))
    assertHasCometParquetScan(dfA.queryExecution.executedPlan)
    assert(dfA.first().getLong(0) == (0L until 100L).sum)

    val callsAfterFirst = MinioCometS3CredentialProvider.callCount()
    val policyCallsAfterFirst = MinioCometS3CredentialProvider.policyCallCount()

    val dfB = spark.read.format("parquet").load(fileB).agg(sum(col("id")))
    assertHasCometParquetScan(dfB.queryExecution.executedPlan)
    assert(dfB.first().getLong(0) == (100L until 200L).sum)

    // The scope-hint fetch fires once per cache miss. Two reads under the same scope should not
    // provoke a second scope-hint fetch on the same bucket key — the cache lookup covers the
    // second path via the existing ScopeEntry.
    assert(
      MinioCometS3CredentialProvider.policyCallCount() == policyCallsAfterFirst,
      s"Scope hint refetched on second read within same scope: " +
        s"$policyCallsAfterFirst -> ${MinioCometS3CredentialProvider.policyCallCount()}")
    assert(
      MinioCometS3CredentialProvider.callCount() > callsAfterFirst,
      "Credential provider was not consulted for the second read — cache entry seems missing")
  }

  test("scoped provider: reads under disjoint scopes get separate object_store entries") {
    val scopeA = s"s3a://$testBucketName/scope-fork-a/"
    val scopeB = s"s3a://$testBucketName/scope-fork-b/"
    val fileA = scopeA + "file.parquet"
    val fileB = scopeB + "file.parquet"

    spark.range(0, 50).write.format("parquet").mode(SaveMode.Overwrite).save(fileA)
    spark.range(50, 130).write.format("parquet").mode(SaveMode.Overwrite).save(fileB)

    MinioCometS3CredentialProvider.resetCounters()

    // First read: install a hint that only covers scope-fork-a/. Native cache seeds a ScopeEntry
    // for that prefix under the shared (bucket, config_hash, backend) key.
    MinioCometS3CredentialProvider.installPolicyLocations(java.util.List.of(scopeA))
    val dfA = spark.read.format("parquet").load(fileA).agg(sum(col("id")))
    assertHasCometParquetScan(dfA.queryExecution.executedPlan)
    assert(dfA.first().getLong(0) == (0L until 50L).sum)

    val policyCallsAfterA = MinioCometS3CredentialProvider.policyCallCount()

    // Second read: swap the hint to only cover scope-fork-b/. The path does not match the first
    // ScopeEntry (scope-fork-a/), so find_matching_scope returns None and the cache issues a fresh
    // bridge lookup — driving a second scope-hint fetch and a distinct object_store entry.
    MinioCometS3CredentialProvider.installPolicyLocations(java.util.List.of(scopeB))
    val dfB = spark.read.format("parquet").load(fileB).agg(sum(col("id")))
    assertHasCometParquetScan(dfB.queryExecution.executedPlan)
    assert(dfB.first().getLong(0) == (50L until 130L).sum)

    assert(
      MinioCometS3CredentialProvider.policyCallCount() > policyCallsAfterA,
      s"Second read under a disjoint scope did not trigger a fresh scope-hint fetch " +
        s"(policy calls: $policyCallsAfterA -> ${MinioCometS3CredentialProvider.policyCallCount()})")
  }
}
