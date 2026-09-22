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

package org.apache.comet.serde.operator

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.apache.iceberg.expressions.Expressions
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.types.{ArrayType, IntegerType, MapType, StringType, StructType}

/**
 * Unit tests for [[CometIcebergNativeScan.hadoopToIcebergS3Properties]]. The pinned iceberg-rust
 * S3 parser reads ONLY global `s3.*` keys (never `s3.bucket.*`), so the function drops per-bucket
 * keys and promotes just the TARGET bucket's keys to global `s3.*`. Pure-function assertions, so
 * a lightweight `AnyFunSuite` (no Spark session) suffices.
 */
class CometIcebergNativeScanSuite extends AnyFunSuite with Matchers {

  test("complex type null residuals are not serialized") {
    // Container predicates stay in the post-scan filter, not the native residual pool.
    for (dataType <- Seq(
        ArrayType(IntegerType),
        MapType(StringType, IntegerType),
        new StructType().add("value", IntegerType));
      predicate <- Seq(Expressions.isNull("value"), Expressions.notNull("value"))) {
      withClue(s"$dataType: $predicate") {
        CometIcebergNativeScan
          .icebergExprToProto(predicate, Seq(AttributeReference("value", dataType)()), Set.empty)
          .isEmpty shouldBe true
      }
    }
    CometIcebergNativeScan
      .icebergExprToProto(
        Expressions.notNull("value"),
        Seq(AttributeReference("value", IntegerType)()),
        Set.empty)
      .nonEmpty shouldBe true
  }

  private def translate(
      props: Map[String, String],
      targetBucket: Option[String]): Map[String, String] =
    CometIcebergNativeScan.hadoopToIcebergS3Properties(props, targetBucket)

  /** Every `fs.s3a.*` suffix the translator maps, paired with its global iceberg `s3.*` key. */
  private val suffixToIcebergKey = Seq(
    "access.key" -> "s3.access-key-id",
    "secret.key" -> "s3.secret-access-key",
    "session.token" -> "s3.session-token",
    "endpoint" -> "s3.endpoint",
    "path.style.access" -> "s3.path-style-access",
    "endpoint.region" -> "s3.region")

  test("full fs.s3a.* suffix mapping to global s3.* keys") {
    // Mappings are per-key independent (no cross-key interaction), so this table-driven case also
    // covers the "several global fs.s3a.* keys at once" scenario.
    suffixToIcebergKey.foreach { case (hadoopSuffix, icebergKey) =>
      val out = translate(Map(s"fs.s3a.$hadoopSuffix" -> "v"), None)
      out should contain(icebergKey -> "v")
      // The Hadoop key itself is never passed through untranslated.
      out.keys should not contain s"fs.s3a.$hadoopSuffix"
    }
  }

  test("target bucket per-bucket keys are promoted to global s3.*") {
    val props = suffixToIcebergKey.map { case (suffix, _) =>
      s"fs.s3a.bucket.target.$suffix" -> s"value-$suffix"
    }.toMap

    val out = translate(props, Some("target"))

    suffixToIcebergKey.foreach { case (suffix, icebergKey) =>
      out(icebergKey) shouldBe s"value-$suffix"
    }
    // Per-bucket keys are never emitted in s3.bucket.* form (the pinned parser ignores those).
    out.keys.foreach(k => k should not startWith "s3.bucket.")
  }

  test("target bucket keys coexist with a different non-target bucket") {
    val props = Map(
      "fs.s3a.bucket.target.endpoint" -> "https://target.example.com",
      "fs.s3a.bucket.target.access.key" -> "AKIA-target",
      "fs.s3a.bucket.other.endpoint" -> "https://other.example.com",
      "fs.s3a.bucket.other.access.key" -> "AKIA-other")

    val out = translate(props, Some("target"))

    out("s3.endpoint") shouldBe "https://target.example.com"
    out("s3.access-key-id") shouldBe "AKIA-target"
    // The non-target bucket contributes nothing: not promoted, not leaked into the global keys.
    out.values.toSet should not contain "https://other.example.com"
    out.values.toSet should not contain "AKIA-other"
    out.keys.size shouldBe 2
  }

  test("target bucket per-bucket value overrides a conflicting global value") {
    // targetBucketGlobals is merged last, so the per-bucket endpoint wins over the global one.
    val props = Map(
      "fs.s3a.endpoint" -> "https://global.example.com",
      "fs.s3a.access.key" -> "AKIA-global",
      "fs.s3a.bucket.target.endpoint" -> "https://target.example.com")

    val out = translate(props, Some("target"))

    out("s3.endpoint") shouldBe "https://target.example.com"
    // A global key with no per-bucket override survives.
    out("s3.access-key-id") shouldBe "AKIA-global"
  }

  test("dotted target bucket names survive (prefix match, not split)") {
    val props = Map(
      "fs.s3a.bucket.my.bucket.name.endpoint" -> "https://dotted.example.com",
      "fs.s3a.bucket.my.bucket.name.access.key" -> "AKIA-dotted",
      "fs.s3a.bucket.my.bucket.name.secret.key" -> "secret-dotted")

    val out = translate(props, Some("my.bucket.name"))

    out("s3.endpoint") shouldBe "https://dotted.example.com"
    out("s3.access-key-id") shouldBe "AKIA-dotted"
    out("s3.secret-access-key") shouldBe "secret-dotted"
  }

  test("keys already in iceberg s3.* form pass through; unrelated keys are ignored") {
    val props = Map(
      "s3.endpoint" -> "https://passthrough.example.com",
      "s3.access-key-id" -> "AKIA-passthrough",
      "fs.gs.project.id" -> "gcp-project",
      "fs.azure.account.key.acct.blob.core.windows.net" -> "azure-key",
      "spark.sql.shuffle.partitions" -> "200")

    translate(props, Some("target")) shouldBe Map(
      "s3.endpoint" -> "https://passthrough.example.com",
      "s3.access-key-id" -> "AKIA-passthrough")
  }

  test("no target bucket means no per-bucket promotion") {
    // Required-parameter contract: with None, per-bucket keys drop, only global keys survive.
    val props = Map(
      "fs.s3a.bucket.some.endpoint" -> "https://some.example.com",
      "fs.s3a.endpoint" -> "https://global.example.com")

    val out = translate(props, None)

    out("s3.endpoint") shouldBe "https://global.example.com"
    out.values.toSet should not contain "https://some.example.com"
  }

  // --- hadoopToIcebergHdfsProperties -------------------------------------------------------
  //
  // These pin the HA translation that makes `hdfs://<nameservice>/...` reachable; see that
  // method's scaladoc for why the property is required rather than optional.

  private def hdfsProps(location: String, conf: Map[String, String]): Map[String, String] = {
    val hadoopConf = new org.apache.hadoop.conf.Configuration(false)
    conf.foreach { case (k, v) => hadoopConf.set(k, v) }
    CometIcebergNativeScan.hadoopToIcebergHdfsProperties(new java.net.URI(location), hadoopConf)
  }

  test("HA nameservice resolves to the comma-separated NameNode list, in declaration order") {
    val out = hdfsProps(
      "hdfs://nameservice1/warehouse/db/t/metadata.json",
      Map(
        "dfs.ha.namenodes.nameservice1" -> "nn1,nn2",
        "dfs.namenode.rpc-address.nameservice1.nn1" -> "host-a.example.com:8020",
        "dfs.namenode.rpc-address.nameservice1.nn2" -> "host-b.example.com:8020"))

    out shouldBe Map(
      "hdfs.name-node" -> "hdfs://host-a.example.com:8020,hdfs://host-b.example.com:8020")
  }

  test("a plain host:port authority needs no mapping") {
    // A property would only pin the scan to one endpoint.
    hdfsProps("hdfs://nn.example.com:8020/warehouse/db/t", Map.empty) shouldBe Map.empty
  }

  test("a partially resolved HA list yields nothing rather than a short failover list") {
    // Dropping nn2 would silently turn a failover into an outage.
    hdfsProps(
      "hdfs://nameservice1/warehouse",
      Map(
        "dfs.ha.namenodes.nameservice1" -> "nn1,nn2",
        "dfs.namenode.rpc-address.nameservice1.nn1" -> "host-a.example.com:8020")) shouldBe Map.empty
  }

  test("rpc-address already carrying the hdfs:// prefix is not double-prefixed") {
    hdfsProps(
      "hdfs://ns/warehouse",
      Map(
        "dfs.ha.namenodes.ns" -> "nn1",
        "dfs.namenode.rpc-address.ns.nn1" -> "hdfs://host-a.example.com:8020")) shouldBe
      Map("hdfs.name-node" -> "hdfs://host-a.example.com:8020")
  }

  test("non-hdfs and authority-less locations are ignored") {
    hdfsProps("s3://bucket/key", Map("dfs.ha.namenodes.bucket" -> "nn1")) shouldBe Map.empty
    hdfsProps("hdfs:///warehouse/db/t", Map.empty) shouldBe Map.empty
  }
}
