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

package org.apache.comet.objectstore

import java.net.URI

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.apache.hadoop.conf.Configuration

import org.apache.comet.CometConf.COMET_S3_COMPLIANT_SCHEMES_KEY

class NativeConfigSuite extends AnyFunSuite with Matchers {

  test("extractObjectStoreOptions - multiple cloud provider configurations") {
    val hadoopConf = new Configuration()
    // S3A configs
    hadoopConf.set("fs.s3a.access.key", "s3-access-key")
    hadoopConf.set("fs.s3a.secret.key", "s3-secret-key")
    hadoopConf.set("fs.s3a.endpoint.region", "us-east-1")
    hadoopConf.set("fs.s3a.bucket.special-bucket.access.key", "special-access-key")
    hadoopConf.set("fs.s3a.bucket.special-bucket.endpoint.region", "eu-central-1")

    // GCS configs
    hadoopConf.set("fs.gs.project.id", "gcp-project")

    // Azure configs
    hadoopConf.set("fs.azure.account.key.testaccount.blob.core.windows.net", "azure-key")

    // Should extract s3 options
    Seq("s3a://test-bucket/test-object", "s3://test-bucket/test-object").foreach { path =>
      val options = NativeConfig.extractObjectStoreOptions(hadoopConf, new URI(path))
      assert(options("fs.s3a.access.key") == "s3-access-key")
      assert(options("fs.s3a.secret.key") == "s3-secret-key")
      assert(options("fs.s3a.endpoint.region") == "us-east-1")
      assert(options("fs.s3a.bucket.special-bucket.access.key") == "special-access-key")
      assert(options("fs.s3a.bucket.special-bucket.endpoint.region") == "eu-central-1")
      assert(!options.contains("fs.gs.project.id"))
    }
    val gsOptions =
      NativeConfig.extractObjectStoreOptions(hadoopConf, new URI("gs://test-bucket/test-object"))
    assert(gsOptions("fs.gs.project.id") == "gcp-project")
    assert(!gsOptions.contains("fs.s3a.access.key"))

    val azureOptions = NativeConfig.extractObjectStoreOptions(
      hadoopConf,
      new URI("wasb://test-bucket/test-object"))
    assert(azureOptions("fs.azure.account.key.testaccount.blob.core.windows.net") == "azure-key")
    assert(!azureOptions.contains("fs.s3a.access.key"))

    // Unsupported scheme should return empty options
    val unsupportedOptions = NativeConfig.extractObjectStoreOptions(
      hadoopConf,
      new URI("unsupported://test-bucket/test-object"))
    assert(unsupportedOptions.isEmpty, "Unsupported scheme should return empty options")
  }

  test("extractObjectStoreOptions - ABFS forwards Hadoop fs.azure.* auth keys") {
    // ABFS auth (account keys, OAuth, MSI/Workload Identity, SAS) lives under fs.azure.*, not
    // fs.abfs.*. Verify abfs[s] forwards fs.azure.* (earlier versions dropped these credentials).
    val hadoopConf = new Configuration()
    hadoopConf.set("fs.azure.account.auth.type.myacct.dfs.core.windows.net", "OAuth")
    hadoopConf.set(
      "fs.azure.account.oauth.provider.type.myacct.dfs.core.windows.net",
      "org.apache.hadoop.fs.azurebfs.oauth2.WorkloadIdentityTokenProvider")
    hadoopConf.set("fs.azure.account.oauth2.client.id.myacct.dfs.core.windows.net", "client-123")
    hadoopConf.set("fs.azure.account.oauth2.msi.tenant.myacct.dfs.core.windows.net", "tenant-abc")
    hadoopConf.set(
      "fs.azure.account.oauth2.token.file.myacct.dfs.core.windows.net",
      "/var/run/secrets/azure/tokens/azure-identity-token")

    Seq(
      "abfs://data@myacct.dfs.core.windows.net/path/file.parquet",
      "abfss://data@myacct.dfs.core.windows.net/path/file.parquet").foreach { path =>
      val opts = NativeConfig.extractObjectStoreOptions(hadoopConf, new URI(path))
      assert(
        opts("fs.azure.account.oauth2.client.id.myacct.dfs.core.windows.net") == "client-123",
        s"client id should be forwarded for $path")
      assert(
        opts("fs.azure.account.oauth2.msi.tenant.myacct.dfs.core.windows.net") == "tenant-abc",
        s"tenant id should be forwarded for $path")
      assert(
        opts("fs.azure.account.oauth2.token.file.myacct.dfs.core.windows.net") ==
          "/var/run/secrets/azure/tokens/azure-identity-token",
        s"federated token file should be forwarded for $path")
      assert(
        opts("fs.azure.account.oauth.provider.type.myacct.dfs.core.windows.net") ==
          "org.apache.hadoop.fs.azurebfs.oauth2.WorkloadIdentityTokenProvider",
        s"oauth provider type should be forwarded for $path")
    }
  }

  test("extractObjectStoreOptions - forwards the substituted value of a ${...} reference") {
    // Hadoop's own consumers read values through Configuration#get, which expands a ${...}
    // reference against another conf entry. Forwarding the raw, unexpanded literal here would
    // give native a different credential than every Hadoop-side consumer sees.
    val hadoopConf = new Configuration()
    hadoopConf.set("my.custom.access.key", "expanded-access-key")
    hadoopConf.set("fs.s3a.access.key", "${my.custom.access.key}")

    val options =
      NativeConfig.extractObjectStoreOptions(hadoopConf, new URI("s3a://test-bucket/test-object"))
    assert(options("fs.s3a.access.key") == "expanded-access-key")
  }

  test(
    "extractObjectStoreOptions - a cyclic ${...} reference falls back to the raw value " +
      "instead of throwing") {
    // Configuration#get raises IllegalStateException once ${...} expansion recurses past
    // Hadoop's MAX_SUBST bound; a two-key mutual cycle triggers this on every call. Extraction
    // must still return a full options map rather than aborting for the whole object store.
    val hadoopConf = new Configuration()
    hadoopConf.set("fs.s3a.access.key", "${fs.s3a.secret.key}")
    hadoopConf.set("fs.s3a.secret.key", "${fs.s3a.access.key}")

    val options =
      NativeConfig.extractObjectStoreOptions(hadoopConf, new URI("s3a://test-bucket/test-object"))
    assert(options("fs.s3a.access.key") == "${fs.s3a.secret.key}")
    assert(options("fs.s3a.secret.key") == "${fs.s3a.access.key}")
  }

  test("extractObjectStoreOptions - blob:// forwards vendor fs.blob.<authority>.* keys as s3a") {
    // blob:// connectors use per-authority keys (just an endpoint, no region). object_store's
    // AmazonS3Builder reads fs.s3a.bucket.<b>.<suffix>, so Comet must translate fs.blob.<b>.* or
    // the read fails ("Failed to resolve region: Bucket not found").
    val hadoopConf = new Configuration()
    hadoopConf.set(COMET_S3_COMPLIANT_SCHEMES_KEY, "blob")
    hadoopConf.set("fs.blob.mybucket.endpoint", "https://s3-compat.example.internal")
    hadoopConf.set("fs.blob.mybucket.awsAccessKeyId", "AKIA-blob")
    hadoopConf.set("fs.blob.mybucket.awsSecretAccessKey", "secret-blob")
    // A different authority to make sure translation is per-authority.
    hadoopConf.set("fs.blob.other.endpoint", "https://other.example.internal")

    val opts = NativeConfig.extractObjectStoreOptions(
      hadoopConf,
      new URI("blob://mybucket/dataset/part-0.parquet"))

    assert(opts("fs.s3a.bucket.mybucket.endpoint") == "https://s3-compat.example.internal")
    assert(opts("fs.s3a.bucket.mybucket.access.key") == "AKIA-blob")
    assert(opts("fs.s3a.bucket.mybucket.secret.key") == "secret-blob")
    // Path-style is required by most S3-compatible services (signing targets the path form).
    assert(opts("fs.s3a.bucket.mybucket.path.style.access") == "true")
    // Per-authority translation must also apply to the second bucket seen in the same config.
    assert(opts("fs.s3a.bucket.other.endpoint") == "https://other.example.internal")
    // No region set by the vendor; Comet must not synthesize one (object_store defaults it).
    assert(!opts.contains("fs.s3a.bucket.mybucket.endpoint.region"))
  }

  test(
    "extractObjectStoreOptions - blob:// default authority lands at the resolved bucket scope") {
    // For `blob:///mybucket/...` the blob FS reports authority "default"; the real bucket is the
    // first path segment. `fs.blob.default.*` must land at `fs.s3a.bucket.mybucket.*`: overriding a
    // stale per-bucket key there, and never clobbering an unrelated global `fs.s3a.*`.
    val hadoopConf = new Configuration()
    hadoopConf.set(COMET_S3_COMPLIANT_SCHEMES_KEY, "blob")
    hadoopConf.set("fs.blob.default.endpoint", "s3-compat.example.com")
    hadoopConf.set("fs.blob.default.awsAccessKeyId", "AKIA-blob")
    hadoopConf.set("fs.blob.default.awsSecretAccessKey", "secret-blob")
    // A stale per-bucket key for the SAME bucket the URL resolves to -- the default must win it.
    hadoopConf.set("fs.s3a.bucket.mybucket.endpoint", "https://stale.example.internal")
    // An unrelated global s3a endpoint -- must be preserved, proving the default never leaks there.
    hadoopConf.set("fs.s3a.endpoint", "other-s3.example.com")

    val opts = NativeConfig.extractObjectStoreOptions(
      hadoopConf,
      new URI("blob:///mybucket/dataset/part-0.parquet"))
    assert(opts("fs.s3a.bucket.mybucket.endpoint") == "s3-compat.example.com")
    assert(opts("fs.s3a.bucket.mybucket.access.key") == "AKIA-blob")
    assert(opts("fs.s3a.bucket.mybucket.secret.key") == "secret-blob")
    assert(opts("fs.s3a.bucket.mybucket.path.style.access") == "true")
    // The unrelated global endpoint is intact: the default landed at bucket scope, not global.
    assert(opts("fs.s3a.endpoint") == "other-s3.example.com")
  }

  test("extractObjectStoreOptions - blob translation does not fire for s3:// / s3a://") {
    // The fs.blob.* namespace is scoped to blob:// URIs; do not surface it on plain s3/s3a URIs.
    val hadoopConf = new Configuration()
    hadoopConf.set(COMET_S3_COMPLIANT_SCHEMES_KEY, "blob")
    hadoopConf.set("fs.blob.mybucket.endpoint", "https://s3-compat.example.internal")
    val opts = NativeConfig.extractObjectStoreOptions(hadoopConf, new URI("s3a://mybucket/x"))
    assert(!opts.contains("fs.s3a.bucket.mybucket.endpoint"))
  }

  test("extractObjectStoreOptions - explicit blob authority beats default authority") {
    // `fs.blob.default.*` (promoted to the URL bucket) and an explicit `fs.blob.mybucket.*` both
    // resolve to `fs.s3a.bucket.mybucket.*`. The explicit authority must win the conflict
    // deterministically, independent of Hadoop config iteration order. Regression guard for the
    // default-before-explicit ordering in translateVendorKeys.
    val hadoopConf = new Configuration()
    hadoopConf.set(COMET_S3_COMPLIANT_SCHEMES_KEY, "blob")
    hadoopConf.set("fs.blob.default.endpoint", "https://default.example.internal")
    hadoopConf.set("fs.blob.default.awsAccessKeyId", "AKIA-default")
    hadoopConf.set("fs.blob.mybucket.endpoint", "https://explicit.example.internal")

    val opts = NativeConfig.extractObjectStoreOptions(
      hadoopConf,
      new URI("blob://mybucket/data/part-0.parquet"))
    // Explicit authority wins the conflicting endpoint...
    assert(opts("fs.s3a.bucket.mybucket.endpoint") == "https://explicit.example.internal")
    // ...and the default authority's non-conflicting key still lands at the same bucket scope.
    assert(opts("fs.s3a.bucket.mybucket.access.key") == "AKIA-default")
  }

  test("extractObjectStoreOptions - blob:// preserves dotted bucket authorities") {
    // Bucket names may contain dots (`my.bucket`); the authority group must keep the full name.
    val hadoopConf = new Configuration()
    hadoopConf.set(COMET_S3_COMPLIANT_SCHEMES_KEY, "blob")
    hadoopConf.set("fs.blob.my.bucket.endpoint", "https://s3-compat.example.internal")
    hadoopConf.set("fs.blob.my.bucket.awsAccessKeyId", "AKIA-dotted")
    hadoopConf.set("fs.blob.my.bucket.awsSecretAccessKey", "secret-dotted")

    val opts = NativeConfig.extractObjectStoreOptions(
      hadoopConf,
      new URI("blob://my.bucket/dataset/part-0.parquet"))

    assert(opts("fs.s3a.bucket.my.bucket.endpoint") == "https://s3-compat.example.internal")
    assert(opts("fs.s3a.bucket.my.bucket.access.key") == "AKIA-dotted")
    assert(opts("fs.s3a.bucket.my.bucket.secret.key") == "secret-dotted")
    assert(opts("fs.s3a.bucket.my.bucket.path.style.access") == "true")
  }

  test("extractObjectStoreOptions - S3-compliant schemes are opt-in (empty by default)") {
    // With no `fs.comet.s3Compliant.schemes`, blob is not claimed, so nothing is extracted.
    val hadoopConf = new Configuration()
    hadoopConf.set("fs.s3a.access.key", "s3-access-key")
    hadoopConf.set("fs.blob.mybucket.endpoint", "https://s3-compat.example.internal")

    val opts = NativeConfig.extractObjectStoreOptions(
      hadoopConf,
      new URI("blob://mybucket/dataset/part-0.parquet"))
    assert(opts.isEmpty, "blob must not be treated as S3-compliant unless opted in")
  }

  test("extractObjectStoreOptions - configured blob scheme reuses fs.s3a.* and copies the key") {
    val hadoopConf = new Configuration()
    hadoopConf.set(COMET_S3_COMPLIANT_SCHEMES_KEY, "blob")
    hadoopConf.set("fs.s3a.access.key", "s3-access-key")
    hadoopConf.set("fs.s3a.secret.key", "s3-secret-key")

    val opts = NativeConfig.extractObjectStoreOptions(
      hadoopConf,
      new URI("blob://test-bucket/test-object"))
    assert(opts("fs.s3a.access.key") == "s3-access-key")
    assert(opts("fs.s3a.secret.key") == "s3-secret-key")
    // The configured scheme list is forwarded to the native side.
    assert(opts(COMET_S3_COMPLIANT_SCHEMES_KEY) == "blob")
  }

  test("extractObjectStoreOptions - non-blob configured scheme translates fs.<scheme>.* keys") {
    // A configured alias uses its own vendor namespace (fs.minio.<auth>.*), case-insensitive list.
    val hadoopConf = new Configuration()
    hadoopConf.set(COMET_S3_COMPLIANT_SCHEMES_KEY, "MinIO, r2")
    hadoopConf.set("fs.minio.mybucket.endpoint", "https://minio.example.internal")
    hadoopConf.set("fs.minio.mybucket.awsAccessKeyId", "AKIA-minio")
    hadoopConf.set("fs.minio.mybucket.awsSecretAccessKey", "secret-minio")

    val opts = NativeConfig.extractObjectStoreOptions(
      hadoopConf,
      new URI("minio://mybucket/dataset/part-0.parquet"))
    assert(opts("fs.s3a.bucket.mybucket.endpoint") == "https://minio.example.internal")
    assert(opts("fs.s3a.bucket.mybucket.access.key") == "AKIA-minio")
    assert(opts("fs.s3a.bucket.mybucket.secret.key") == "secret-minio")
    assert(opts("fs.s3a.bucket.mybucket.path.style.access") == "true")
  }

  test("extractObjectStoreOptions - explicit per-bucket path.style.access=false is preserved") {
    // The synthesized path-style is a soft default: an explicit per-bucket false wins (the hatch).
    val hadoopConf = new Configuration()
    hadoopConf.set(COMET_S3_COMPLIANT_SCHEMES_KEY, "blob")
    hadoopConf.set("fs.blob.mybucket.endpoint", "https://s3-compat.example.internal")
    hadoopConf.set("fs.s3a.bucket.mybucket.path.style.access", "false")

    val opts = NativeConfig.extractObjectStoreOptions(
      hadoopConf,
      new URI("blob://mybucket/dataset/part-0.parquet"))
    assert(opts("fs.s3a.bucket.mybucket.endpoint") == "https://s3-compat.example.internal")
    assert(opts("fs.s3a.bucket.mybucket.path.style.access") == "false")
  }

  test(
    "extractObjectStoreOptions - global path.style.access does not suppress per-bucket synth") {
    // A global fs.s3a.path.style.access must NOT suppress the per-bucket synth: it may be ambient
    // or for other workloads, and per-bucket wins natively. Only a per-bucket setting is the hatch.
    val hadoopConf = new Configuration()
    hadoopConf.set(COMET_S3_COMPLIANT_SCHEMES_KEY, "blob")
    hadoopConf.set("fs.blob.mybucket.endpoint", "https://s3-compat.example.internal")
    hadoopConf.set("fs.s3a.path.style.access", "false")

    val opts = NativeConfig.extractObjectStoreOptions(
      hadoopConf,
      new URI("blob://mybucket/dataset/part-0.parquet"))
    assert(opts("fs.s3a.bucket.mybucket.path.style.access") == "true")
    assert(opts("fs.s3a.path.style.access") == "false")
  }

  test("extractObjectStoreOptions - vendor session token, region, path-style translate") {
    val hadoopConf = new Configuration()
    hadoopConf.set(COMET_S3_COMPLIANT_SCHEMES_KEY, "blob")
    hadoopConf.set("fs.blob.mybucket.endpoint", "https://s3-compat.example.internal")
    hadoopConf.set("fs.blob.mybucket.awsSessionToken", "session-token-xyz")
    hadoopConf.set("fs.blob.mybucket.region", "us-west-2")
    // An explicit vendor path-style must win over the synthesized endpoint default.
    hadoopConf.set("fs.blob.mybucket.pathStyleAccess", "false")

    val opts = NativeConfig.extractObjectStoreOptions(
      hadoopConf,
      new URI("blob://mybucket/dataset/part-0.parquet"))
    assert(opts("fs.s3a.bucket.mybucket.session.token") == "session-token-xyz")
    assert(opts("fs.s3a.bucket.mybucket.endpoint.region") == "us-west-2")
    assert(opts("fs.s3a.bucket.mybucket.path.style.access") == "false")
  }

  test("extractObjectStoreOptions - vendor key overrides conflicting same-scope fs.s3a.*") {
    // Real vendor keys override a same-scope fs.s3a.* value (the 403-misdirect guard).
    val hadoopConf = new Configuration()
    hadoopConf.set(COMET_S3_COMPLIANT_SCHEMES_KEY, "blob")
    hadoopConf.set("fs.s3a.bucket.mybucket.endpoint", "https://stale.example.internal")
    hadoopConf.set("fs.blob.mybucket.endpoint", "https://vendor.example.internal")

    val opts = NativeConfig.extractObjectStoreOptions(
      hadoopConf,
      new URI("blob://mybucket/dataset/part-0.parquet"))
    assert(opts("fs.s3a.bucket.mybucket.endpoint") == "https://vendor.example.internal")
  }

  test("bucketForUri - authority is the bucket for s3/s3a and configured aliases") {
    NativeConfig.bucketForUri(new URI("s3://mybucket/key"), Set.empty) shouldBe Some("mybucket")
    NativeConfig.bucketForUri(new URI("s3a://mybucket/key"), Set.empty) shouldBe Some("mybucket")
    // blob is only S3-family when opted in; its authority is still the bucket.
    NativeConfig.bucketForUri(new URI("blob://mybucket/key"), Set("blob")) shouldBe Some(
      "mybucket")
  }

  test("bucketForUri - authorityless alias promotes the first path segment") {
    // `blob:///mybucket/...` reports authority "default"; the real bucket is the first path segment
    // (matching the native rewrite), but only when the scheme is an opted-in S3-compliant alias.
    NativeConfig.bucketForUri(new URI("blob:///mybucket/key"), Set("blob")) shouldBe
      Some("mybucket")
    // Not opted in -> not S3-family -> no path promotion.
    NativeConfig.bucketForUri(new URI("blob:///mybucket/key"), Set.empty) shouldBe None
  }

  test(
    "bucketForUri - non-S3 scheme with no authority returns None, not the first path segment") {
    // Regression guard: a local Hadoop-catalog metadata path must NOT resolve to bucket `tmp`.
    NativeConfig.bucketForUri(
      new URI("file:///tmp/warehouse/db/t/metadata/v1.metadata.json"),
      Set("blob")) shouldBe None
    NativeConfig.bucketForUri(new URI("gs:///tmp/object"), Set("blob")) shouldBe None
  }

  test("s3FamilyBuckets - collects distinct buckets from S3-family URIs only") {
    val uris = Seq(
      new URI("s3://bucket-a/x"),
      new URI("s3a://bucket-a/y"), // same bucket, different scheme
      new URI("blob://bucket-b/z"), // alias, counts only when opted in
      new URI("file:///tmp/local"), // not S3-family -> ignored
      new URI("gs://gcs-bucket/g")
    ) // not S3-family -> ignored (no fs.s3a.* per-bucket surface)
    NativeConfig.s3FamilyBuckets(uris, Set("blob")) shouldBe Set("bucket-a", "bucket-b")
    // Without the opt-in, blob is not S3-family, so its bucket drops out.
    NativeConfig.s3FamilyBuckets(uris, Set.empty) shouldBe Set("bucket-a")
    NativeConfig.s3FamilyBuckets(Seq.empty, Set("blob")) shouldBe Set.empty
  }
}
