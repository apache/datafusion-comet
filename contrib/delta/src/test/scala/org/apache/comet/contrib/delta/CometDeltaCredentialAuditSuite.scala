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

package org.apache.comet.contrib.delta

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.apache.comet.objectstore.NativeConfig

// Cred-audit regression coverage for the JVM-side credential plumbing into
// contrib-delta's native scan. Two layers are covered:
//
//   1. `NativeConfig.extractObjectStoreOptions` (base Comet, common module):
//      pulls `fs.<scheme>.*` keys from a Hadoop conf, keyed by the URI
//      scheme of the table root.
//   2. `CometDeltaNativeScan.augmentWithResolvedAwsCredentials`
//      (contrib-delta): for `s3:` / `s3a:` tables, resolves Hadoop's
//      AWSCredentialProviderList via reflection and lays the resolved
//      access/secret/session keys on top of the extracted options. Falls
//      back gracefully when hadoop-aws isn't on the classpath.
//
// Gaps documented (asserted as missing so the test forces a positive case
// when fixed):
//   * GCS keys (`fs.gs.*`) are extracted to the options map only when the
//     URI scheme is `gs` -- but `delta_storage_config_from_map` on the
//     native side has no `gcp_*` fields, so they get dropped further down.
//   * Per-bucket S3 keys (`fs.s3a.bucket.<name>.*`) ARE extracted by
//     NativeConfig but native maps only the global `fs.s3a.access.key` /
//     `fs.s3a.secret.key`. Per-bucket creds silently fall back to global.
//   * Hadoop-style Azure account keys (`fs.azure.account.key.<account>`,
//     OAuth, MSI, SAS tokens) ARE extracted for wasb/wasbs/abfss but
//     **not for the bare `abfs` scheme** — NativeConfig only registers
//     `fs.abfs.` for `abfs`. And even when extracted, native only checks
//     the three kernel-style `azure_*` keys.
//
// Each gap has a matching native-side assertion in
// `jni::tests::extract_storage_config_known_gaps` -- both must be removed
// together when the gap is closed.
class CometDeltaCredentialAuditSuite extends AnyFunSuite with Matchers {

  // === Layer 1: NativeConfig.extractObjectStoreOptions ===
  // These assertions document that the per-scheme extractor pulls the
  // right keys from a Hadoop conf. The companion piece (translation to
  // kernel-style keys) happens native-side.

  test("S3A keys extracted for s3:// and s3a:// schemes") {
    val conf = new Configuration()
    conf.set("fs.s3a.access.key", "AK")
    conf.set("fs.s3a.secret.key", "SK")
    conf.set("fs.s3a.session.token", "TOK")
    conf.set("fs.s3a.endpoint.region", "us-west-2")
    conf.set("fs.s3a.endpoint", "https://s3.example")
    conf.set("fs.s3a.path.style.access", "true")
    // Per-bucket key (extracted because of the global `fs.s3a.` prefix
    // match -- whether native applies it is a separate question covered
    // by the gap-marker test below).
    conf.set("fs.s3a.bucket.my-bucket.access.key", "PERBKT_AK")
    Seq("s3", "s3a").foreach { scheme =>
      val opts = NativeConfig.extractObjectStoreOptions(
        conf, new URI(s"$scheme://my-bucket/data"))
      assert(opts("fs.s3a.access.key") === "AK")
      assert(opts("fs.s3a.secret.key") === "SK")
      assert(opts("fs.s3a.session.token") === "TOK")
      assert(opts("fs.s3a.endpoint.region") === "us-west-2")
      assert(opts("fs.s3a.endpoint") === "https://s3.example")
      assert(opts("fs.s3a.path.style.access") === "true")
      assert(opts("fs.s3a.bucket.my-bucket.access.key") === "PERBKT_AK")
    }
  }

  test("Azure keys extracted for abfs / abfss / wasb / wasbs schemes") {
    val conf = new Configuration()
    conf.set("fs.azure.account.key.myacct.dfs.core.windows.net", "AZKEY")
    conf.set("fs.azure.account.oauth2.client.id", "CLIENT_ID")
    conf.set("fs.azure.account.oauth2.client.secret", "CLIENT_SECRET")
    conf.set("fs.azure.account.oauth.provider.type", "ClientCredsTokenProvider")
    conf.set("fs.abfs.io.threads", "8")
    conf.set("fs.wasb.block.size", "67108864")
    val expectedAzureKeys = Set(
      "fs.azure.account.key.myacct.dfs.core.windows.net",
      "fs.azure.account.oauth2.client.id",
      "fs.azure.account.oauth2.client.secret",
      "fs.azure.account.oauth.provider.type")
    // GAP marker: NativeConfig's abfs/abfss prefix lists are (`fs.abfs.`)
    // and (`fs.abfss.`, `fs.abfs.`) -- neither matches `fs.azure.`. So
    // OAuth/Managed-Identity creds (which Hadoop users have always set
    // under `fs.azure.*`) are dropped. Remove these gap assertions and
    // flip to positive containment when NativeConfig adds `fs.azure.` to
    // the abfs/abfss prefix lists.
    Seq("abfs", "abfss").foreach { scheme =>
      val opts = NativeConfig.extractObjectStoreOptions(
        conf, new URI(s"$scheme://container@acct.dfs.core.windows.net/data"))
      assert(opts.contains(s"fs.$scheme.io.threads") ||
        opts.contains("fs.abfs.io.threads"), s"[$scheme] missing abfs key")
      expectedAzureKeys.foreach { k =>
        assert(!opts.contains(k),
          s"[$scheme] GAP CLOSED: NativeConfig now extracts $k -- " +
            "update this test to assert positive containment instead")
      }
    }
    Seq("wasb", "wasbs").foreach { scheme =>
      val opts = NativeConfig.extractObjectStoreOptions(
        conf, new URI(s"$scheme://container@acct.blob.core.windows.net/data"))
      // wasb/wasbs uses both `fs.azure.` and `fs.wasb.` prefixes.
      expectedAzureKeys.foreach { k =>
        assert(opts.contains(k), s"[$scheme] missing $k in extracted opts")
      }
      assert(opts.contains("fs.wasb.block.size"), s"[$scheme] missing wasb key")
    }
  }

  test("GCS keys extracted for gs:// scheme") {
    val conf = new Configuration()
    conf.set("fs.gs.project.id", "my-project")
    conf.set("fs.gs.auth.service.account.json.keyfile", "/tmp/key.json")
    conf.set("fs.gs.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE")
    val opts = NativeConfig.extractObjectStoreOptions(
      conf, new URI("gs://my-bucket/data"))
    assert(opts("fs.gs.project.id") === "my-project")
    assert(opts("fs.gs.auth.service.account.json.keyfile") === "/tmp/key.json")
    assert(opts("fs.gs.auth.type") === "SERVICE_ACCOUNT_JSON_KEYFILE")
  }

  test("Non-cloud scheme returns only the libhdfs override if set, no creds") {
    val conf = new Configuration()
    conf.set("fs.s3a.access.key", "SHOULD_NOT_APPEAR")
    val opts = NativeConfig.extractObjectStoreOptions(
      conf, new URI("file:/tmp/local"))
    assert(opts.isEmpty || !opts.contains("fs.s3a.access.key"),
      s"local-fs scheme should not pick up s3a creds, got $opts")
  }

  // === Layer 2: augmentWithResolvedAwsCredentials ===
  // For s3/s3a tables, this resolves AWSCredentialProviderList via
  // reflection. Without hadoop-aws on the classpath the function returns
  // baseOptions unchanged.

  test("augmentWithResolvedAwsCredentials no-ops for non-s3 schemes") {
    val conf = new Configuration()
    val base = Map("fs.gs.project.id" -> "my-project")
    val augmented = CometDeltaNativeScan.augmentWithResolvedAwsCredentials(
      base, new URI("gs://my-bucket/data"), conf)
    assert(augmented === base,
      "augmentWithResolvedAwsCredentials must not touch non-s3 options")
  }

  test("augmentWithResolvedAwsCredentials preserves explicit keys") {
    val conf = new Configuration()
    val base = Map(
      "fs.s3a.access.key" -> "EXPLICIT_AK",
      "fs.s3a.secret.key" -> "EXPLICIT_SK")
    val augmented = CometDeltaNativeScan.augmentWithResolvedAwsCredentials(
      base, new URI("s3a://bucket/data"), conf)
    // When both keys are present in baseOptions, the function short-circuits
    // and returns baseOptions unchanged (no provider-chain lookup).
    assert(augmented("fs.s3a.access.key") === "EXPLICIT_AK")
    assert(augmented("fs.s3a.secret.key") === "EXPLICIT_SK")
  }

  // === Documented gap (Layer 2): per-bucket keys are extracted but the
  // augmentation function does not produce per-bucket entries. Multi-bucket
  // tables with provider-resolved creds get only global creds. ===

  test("GAP: augmentWithResolvedAwsCredentials does not produce per-bucket keys") {
    val conf = new Configuration()
    // Set per-bucket creds in the Hadoop conf; nothing in baseOptions.
    conf.set("fs.s3a.bucket.bucket-a.access.key", "PERBKT_AK")
    val base = Map.empty[String, String]
    val augmented = CometDeltaNativeScan.augmentWithResolvedAwsCredentials(
      base, new URI("s3a://bucket-a/data"), conf)
    // The augmentation only resolves the global keys via the credential
    // chain. If hadoop-aws isn't on the classpath, augmented === base.
    // Either way, no `fs.s3a.bucket.bucket-a.access.key` entry appears.
    assert(
      !augmented.contains("fs.s3a.bucket.bucket-a.access.key"),
      "per-bucket key was unexpectedly bridged; if intentional, " +
        "remove this gap test and add a positive assertion")
  }
}
