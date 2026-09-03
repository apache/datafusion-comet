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

/**
 * Unit tests for [[CometIcebergNativeScan.hadoopToIcebergS3Properties]]. The pinned iceberg-rust
 * S3 parser reads ONLY global `s3.*` keys (never `s3.bucket.*`), so the function drops per-bucket
 * keys and promotes just the TARGET bucket's keys to global `s3.*`. Pure-function assertions, so
 * a lightweight `AnyFunSuite` (no Spark session) suffices.
 */
class CometIcebergNativeScanSuite extends AnyFunSuite with Matchers {

  private def translate(
      props: Map[String, String],
      targetBucket: Option[String]): Map[String, String] =
    CometIcebergNativeScan.hadoopToIcebergS3Properties(props, targetBucket)

  test("full fs.s3a.* suffix mapping to global s3.* keys") {
    // Every suffix handled by hadoopS3aSuffixToIcebergGlobalKey, verified end to end. Mappings are
    // per-key independent (no cross-key interaction), so this table-driven case also covers the
    // "several global fs.s3a.* keys at once" scenario.
    val cases = Seq(
      "access.key" -> "s3.access-key-id",
      "secret.key" -> "s3.secret-access-key",
      "session.token" -> "s3.session-token",
      "endpoint" -> "s3.endpoint",
      "path.style.access" -> "s3.path-style-access",
      "endpoint.region" -> "s3.region")

    cases.foreach { case (hadoopSuffix, icebergKey) =>
      val out = translate(Map(s"fs.s3a.$hadoopSuffix" -> "v"), None)
      out should contain(icebergKey -> "v")
      // The Hadoop key itself is never passed through untranslated.
      out.keys should not contain s"fs.s3a.$hadoopSuffix"
    }
  }

  test("target bucket per-bucket keys are promoted to global s3.*") {
    val props = Map(
      "fs.s3a.bucket.target.access.key" -> "AKIA-target",
      "fs.s3a.bucket.target.secret.key" -> "secret-target",
      "fs.s3a.bucket.target.session.token" -> "token-target",
      "fs.s3a.bucket.target.endpoint" -> "https://target.example.com",
      "fs.s3a.bucket.target.path.style.access" -> "true",
      "fs.s3a.bucket.target.endpoint.region" -> "eu-central-1")

    val out = translate(props, Some("target"))

    out("s3.access-key-id") shouldBe "AKIA-target"
    out("s3.secret-access-key") shouldBe "secret-target"
    out("s3.session-token") shouldBe "token-target"
    out("s3.endpoint") shouldBe "https://target.example.com"
    out("s3.path-style-access") shouldBe "true"
    out("s3.region") shouldBe "eu-central-1"

    // Per-bucket keys are never emitted in s3.bucket.* form (the pinned parser ignores those).
    out.keys.foreach(k => k should not startWith "s3.bucket.")
  }

  test("non-target bucket per-bucket keys are dropped entirely") {
    val props = Map(
      "fs.s3a.bucket.other.access.key" -> "AKIA-other",
      "fs.s3a.bucket.other.endpoint" -> "https://other.example.com",
      "fs.s3a.bucket.other.path.style.access" -> "true")

    val out = translate(props, Some("target"))

    // A non-target bucket contributes nothing (not promoted, not emitted as s3.bucket.*).
    out shouldBe empty
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
    // The non-target bucket's values must not leak into the global keys.
    out.values.toSet should not contain "https://other.example.com"
    out.values.toSet should not contain "AKIA-other"
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

  test("keys already in iceberg s3.* form pass through unchanged") {
    val props = Map(
      "s3.endpoint" -> "https://passthrough.example.com",
      "s3.access-key-id" -> "AKIA-passthrough")

    val out = translate(props, None)

    out("s3.endpoint") shouldBe "https://passthrough.example.com"
    out("s3.access-key-id") shouldBe "AKIA-passthrough"
  }

  test("unrelated keys are ignored") {
    val props = Map(
      "fs.gs.project.id" -> "gcp-project",
      "fs.azure.account.key.acct.blob.core.windows.net" -> "azure-key",
      "spark.sql.shuffle.partitions" -> "200")

    translate(props, Some("target")) shouldBe empty
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
}
