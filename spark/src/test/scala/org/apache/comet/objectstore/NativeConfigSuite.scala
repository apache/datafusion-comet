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

import org.apache.comet.CometConf.{COMET_LIBHDFS_SCHEMES_KEY, COMET_S3_COMPLIANT_SCHEMES_KEY}

class NativeConfigSuite extends AnyFunSuite with Matchers {

  /**
   * A Hadoop `Configuration` variable reference to `key`, as `Configuration#get` expands it.
   * Built by concatenation: written as a literal, it reads to scalac as a string missing its `s`
   * interpolator, and adding the `s` (escaping the dollar) is then flagged by scalafix as
   * redundant.
   */
  private def varRef(key: String): String = "${" + key + "}"

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

  test(
    "extractObjectStoreOptions - forwards the substituted value of a " +
      s"${varRef("...")} reference") {
    // Hadoop's own consumers read values through Configuration#get, which expands a ${...}
    // reference against another conf entry. Forwarding the raw, unexpanded literal here would
    // give native a different credential than every Hadoop-side consumer sees.
    val hadoopConf = new Configuration()
    hadoopConf.set("my.custom.access.key", "expanded-access-key")
    hadoopConf.set("fs.s3a.access.key", varRef("my.custom.access.key"))

    val options =
      NativeConfig.extractObjectStoreOptions(hadoopConf, new URI("s3a://test-bucket/test-object"))
    assert(options("fs.s3a.access.key") == "expanded-access-key")
  }

  test(
    s"extractObjectStoreOptions - a cyclic ${varRef("...")} reference falls back to the raw " +
      "value instead of throwing") {
    // Configuration#get raises IllegalStateException once ${...} expansion recurses past
    // Hadoop's MAX_SUBST bound; a two-key mutual cycle triggers this on every call. Extraction
    // must still return a full options map rather than aborting for the whole object store.
    val hadoopConf = new Configuration()
    hadoopConf.set("fs.s3a.access.key", varRef("fs.s3a.secret.key"))
    hadoopConf.set("fs.s3a.secret.key", varRef("fs.s3a.access.key"))

    val options =
      NativeConfig.extractObjectStoreOptions(hadoopConf, new URI("s3a://test-bucket/test-object"))
    assert(options("fs.s3a.access.key") == varRef("fs.s3a.secret.key"))
    assert(options("fs.s3a.secret.key") == varRef("fs.s3a.access.key"))
  }

  test(
    "extractObjectStoreOptions - alias forwards vendor fs.<scheme>.<authority>.* keys as s3a") {
    // Alias connectors use per-authority keys (just an endpoint, no region). object_store's
    // AmazonS3Builder reads fs.s3a.bucket.<b>.<suffix>, so Comet must translate fs.<scheme>.<b>.*
    // or the read fails ("Failed to resolve region: Bucket not found"). The scheme is not
    // hardcoded, and the configured list is case-insensitive.
    for ((schemeList, scheme) <- Seq("blob" -> "blob", "MinIO, r2" -> "minio")) {
      val hadoopConf = new Configuration()
      hadoopConf.set(COMET_S3_COMPLIANT_SCHEMES_KEY, schemeList)
      hadoopConf.set(s"fs.$scheme.mybucket.endpoint", "https://s3-compat.example.internal")
      hadoopConf.set(s"fs.$scheme.mybucket.awsAccessKeyId", "AKIA-alias")
      hadoopConf.set(s"fs.$scheme.mybucket.awsSecretAccessKey", "secret-alias")
      // A different authority to make sure translation is per-authority.
      hadoopConf.set(s"fs.$scheme.other.endpoint", "https://other.example.internal")

      val opts = NativeConfig.extractObjectStoreOptions(
        hadoopConf,
        new URI(s"$scheme://mybucket/dataset/part-0.parquet"))

      withClue(s"scheme=$scheme list=$schemeList: ") {
        assert(opts("fs.s3a.bucket.mybucket.endpoint") == "https://s3-compat.example.internal")
        assert(opts("fs.s3a.bucket.mybucket.access.key") == "AKIA-alias")
        assert(opts("fs.s3a.bucket.mybucket.secret.key") == "secret-alias")
        // Path-style is required by most S3-compatible services (signing targets the path form).
        assert(opts("fs.s3a.bucket.mybucket.path.style.access") == "true")
        // Per-authority translation must also apply to the second bucket in the same config.
        assert(opts("fs.s3a.bucket.other.endpoint") == "https://other.example.internal")
        // No region set by the vendor; Comet must not synthesize one (object_store defaults it).
        assert(!opts.contains("fs.s3a.bucket.mybucket.endpoint.region"))
      }
    }
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

  test("extractObjectStoreOptions - endpoint path-style synth: only per-bucket suppresses it") {
    // The synthesized path-style is a soft default: an explicit PER-BUCKET setting wins (the
    // escape hatch), but a GLOBAL one must not suppress it -- the global may be ambient or for
    // other s3a workloads, and per-bucket wins natively anyway.
    val hadoopConf = new Configuration()
    hadoopConf.set(COMET_S3_COMPLIANT_SCHEMES_KEY, "blob")
    hadoopConf.set("fs.blob.pinned.endpoint", "https://pinned.example.internal")
    hadoopConf.set("fs.s3a.bucket.pinned.path.style.access", "false")
    hadoopConf.set("fs.blob.synth.endpoint", "https://synth.example.internal")
    hadoopConf.set("fs.s3a.path.style.access", "false")

    val pinned = NativeConfig.extractObjectStoreOptions(
      hadoopConf,
      new URI("blob://pinned/dataset/part-0.parquet"))
    assert(pinned("fs.s3a.bucket.pinned.endpoint") == "https://pinned.example.internal")
    assert(pinned("fs.s3a.bucket.pinned.path.style.access") == "false")

    val synth = NativeConfig.extractObjectStoreOptions(
      hadoopConf,
      new URI("blob://synth/dataset/part-0.parquet"))
    assert(synth("fs.s3a.bucket.synth.path.style.access") == "true")
    assert(synth("fs.s3a.path.style.access") == "false")
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

  test("extractObjectStoreOptions over a scan's files forwards every scheme's options") {
    val hadoopConf = new Configuration()
    hadoopConf.set(COMET_LIBHDFS_SCHEMES_KEY, "hdfs")
    hadoopConf.set("fs.s3a.access.key", "s3-access-key")
    hadoopConf.set("fs.gs.project.id", "gcp-project")
    hadoopConf.set("fs.azure.account.key.acct.blob.core.windows.net", "azure-key")
    val uris = Seq(
      "file:///tmp/t/p=1/a.parquet",
      "hdfs://nn1/t/p=2/b.parquet",
      "s3a://bucket-a/t/p=3/c.parquet",
      "gs://bucket-b/t/p=4/d.parquet",
      "s3a://bucket-c/t/p=5/e.parquet").map(new URI(_))

    val opts = NativeConfig.extractObjectStoreOptions(hadoopConf, uris)
    assert(opts("fs.s3a.access.key") == "s3-access-key")
    assert(opts("fs.gs.project.id") == "gcp-project")
    assert(opts(COMET_LIBHDFS_SCHEMES_KEY) == "hdfs")
    // No file of the scan is on Azure.
    assert(!opts.contains("fs.azure.account.key.acct.blob.core.windows.net"))
    // The union of the options of each scheme on its own.
    val perScheme = uris.map(NativeConfig.extractObjectStoreOptions(hadoopConf, _))
    assert(opts == perScheme.reduce(_ ++ _))
    assert(NativeConfig.extractObjectStoreOptions(hadoopConf, Nil).isEmpty)
    // A file on Azure brings the Azure options.
    val withAzure = NativeConfig.extractObjectStoreOptions(
      hadoopConf,
      uris :+ new URI("wasbs://container@acct.blob.core.windows.net/t/f.parquet"))
    assert(withAzure("fs.azure.account.key.acct.blob.core.windows.net") == "azure-key")
    assert(withAzure("fs.s3a.access.key") == "s3-access-key")
  }

  test("extractObjectStoreOptions over a scan's files keeps two aliases' settings apart") {
    val hadoopConf = new Configuration()
    hadoopConf.set(COMET_S3_COMPLIANT_SCHEMES_KEY, "blob,wasabi")
    hadoopConf.set("fs.blob.default.endpoint", "https://blob.example.internal")
    hadoopConf.set("fs.wasabi.default.endpoint", "https://wasabi.example.internal")
    val uris = Seq("blob://bucket-a/t/1.parquet", "wasabi://bucket-b/t/2.parquet").map(new URI(_))

    Seq(uris, uris.reverse).foreach { files =>
      val opts = NativeConfig.extractObjectStoreOptions(hadoopConf, files)
      assert(opts("fs.s3a.bucket.bucket-a.endpoint") == "https://blob.example.internal")
      assert(opts("fs.s3a.bucket.bucket-b.endpoint") == "https://wasabi.example.internal")
      assert(!opts.contains("fs.s3a.endpoint"))
    }
  }

  test("extractObjectStoreOptions over a scan's files translates alias settings per bucket") {
    val hadoopConf = new Configuration()
    hadoopConf.set(COMET_S3_COMPLIANT_SCHEMES_KEY, "blob")
    hadoopConf.set("fs.blob.default.endpoint", "https://blob.example.internal")
    hadoopConf.set("fs.blob.default.awsAccessKeyId", "AKIA-blob")
    // A raw per-bucket key for an alias bucket: the alias translation must win it, even when
    // another file's scheme copies the raw key too.
    hadoopConf.set("fs.s3a.bucket.bucket-a.endpoint", "https://stale.example.internal")
    hadoopConf.set("fs.s3a.bucket.bucket-s3a.endpoint", "https://s3a.example.internal")
    val uris = Seq(
      "blob://bucket-a/t/1.parquet",
      "s3a://bucket-s3a/t/2.parquet",
      "blob:///bucket-b/t/3.parquet").map(new URI(_))

    val opts = NativeConfig.extractObjectStoreOptions(hadoopConf, uris)
    Seq("bucket-a", "bucket-b").foreach { bucket =>
      assert(opts(s"fs.s3a.bucket.$bucket.endpoint") == "https://blob.example.internal")
      assert(opts(s"fs.s3a.bucket.$bucket.access.key") == "AKIA-blob")
      assert(opts(s"fs.s3a.bucket.$bucket.path.style.access") == "true")
    }
    // The plain s3a bucket keeps its own settings, without the alias defaults.
    assert(opts("fs.s3a.bucket.bucket-s3a.endpoint") == "https://s3a.example.internal")
    assert(!opts.contains("fs.s3a.bucket.bucket-s3a.access.key"))
    // Whichever order the files come in.
    assert(NativeConfig.extractObjectStoreOptions(hadoopConf, uris.reverse) == opts)
  }

  test("extractObjectStoreOptions over a scan's files keeps each alias bucket's translation") {
    // Bucket b1's own translation keeps the default path style, while bucket b2's translation
    // derives b1's path style from b1's explicit endpoint. Only b1's own one may apply to b1.
    val hadoopConf = new Configuration()
    hadoopConf.set(COMET_S3_COMPLIANT_SCHEMES_KEY, "blob")
    hadoopConf.set("fs.blob.default.pathStyleAccess", "false")
    hadoopConf.set("fs.blob.b1.endpoint", "https://b1.example.internal")
    val b1 = new URI("blob://b1/t/1.parquet")
    val b2 = new URI("blob://b2/t/2.parquet")
    val alone = NativeConfig.extractObjectStoreOptions(hadoopConf, b1)
    assert(alone("fs.s3a.bucket.b1.path.style.access") == "false")

    Seq(Seq(b1, b2), Seq(b2, b1)).foreach { uris =>
      val opts = NativeConfig.extractObjectStoreOptions(hadoopConf, uris)
      assert(opts("fs.s3a.bucket.b1.endpoint") == "https://b1.example.internal")
      assert(opts("fs.s3a.bucket.b1.path.style.access") == "false", s"files $uris")
      assert(opts("fs.s3a.bucket.b2.path.style.access") == "false", s"files $uris")
    }
  }

  test("extractObjectStoreOptions over a scan's files translates no alias read through libhdfs") {
    val hadoopConf = new Configuration()
    hadoopConf.set(COMET_S3_COMPLIANT_SCHEMES_KEY, "blob")
    hadoopConf.set(COMET_LIBHDFS_SCHEMES_KEY, "hdfs,blob")
    hadoopConf.set("fs.s3a.endpoint", "http://s3.example.internal:19001")
    hadoopConf.set("fs.blob.default.endpoint", "http://blob.example.internal:19002")
    val s3a = new URI("s3a://bucket/t/1.parquet")
    val blob = new URI("blob://bucket/t/2.parquet")

    // The libhdfs store reads no `fs.s3a.*` keys, so the alias settings must not reach the
    // native S3 store of the same bucket.
    Seq(Seq(s3a, blob), Seq(blob, s3a)).foreach { uris =>
      val opts = NativeConfig.extractObjectStoreOptions(hadoopConf, uris)
      assert(opts("fs.s3a.endpoint") == "http://s3.example.internal:19001", s"files $uris")
      assert(!opts.contains("fs.s3a.bucket.bucket.endpoint"), s"files $uris")
      assert(!opts.contains("fs.s3a.bucket.bucket.path.style.access"), s"files $uris")
      assert(opts == NativeConfig.extractObjectStoreOptions(hadoopConf, Seq(s3a)))
    }

    // An alias read only through libhdfs gets no translated settings either.
    val blobOnly = NativeConfig.extractObjectStoreOptions(hadoopConf, Seq(blob))
    assert(!blobOnly.keys.exists(_.startsWith("fs.s3a.bucket.")))
    assert(blobOnly(COMET_LIBHDFS_SCHEMES_KEY) == "hdfs,blob")

    // An alias the native S3 store reads keeps its translation next to an s3a file.
    hadoopConf.set(COMET_LIBHDFS_SCHEMES_KEY, "hdfs")
    val nativeBlob = new URI("blob://bucket-b/t/3.parquet")
    Seq(Seq(s3a, nativeBlob), Seq(nativeBlob, s3a)).foreach { uris =>
      val opts = NativeConfig.extractObjectStoreOptions(hadoopConf, uris)
      assert(opts("fs.s3a.endpoint") == "http://s3.example.internal:19001", s"files $uris")
      assert(opts("fs.s3a.bucket.bucket-b.endpoint") == "http://blob.example.internal:19002")
      assert(opts("fs.s3a.bucket.bucket-b.path.style.access") == "true")
      assert(!opts.contains("fs.s3a.bucket.bucket.endpoint"), s"files $uris")
    }
  }

  test("bucketForUri - authority, alias path promotion, and non-S3 schemes") {
    // `blob:///mybucket/...` reports authority "default"; the real bucket is the first path
    // segment (matching the native rewrite), but path promotion applies ONLY to S3-family
    // schemes. Non-S3 schemes must yield None rather than a surprising first-segment bucket --
    // a local Hadoop-catalog metadata path must not resolve to bucket `tmp`.
    val cases = Seq(
      ("s3://mybucket/key", Set.empty[String], Some("mybucket")),
      ("s3a://mybucket/key", Set.empty[String], Some("mybucket")),
      // blob is only S3-family when opted in; its authority is still the bucket.
      ("blob://mybucket/key", Set("blob"), Some("mybucket")),
      ("blob:///mybucket/key", Set("blob"), Some("mybucket")),
      // Not opted in -> not S3-family -> no path promotion.
      ("blob:///mybucket/key", Set.empty[String], None),
      ("file:///tmp/warehouse/db/t/metadata/v1.metadata.json", Set("blob"), None),
      ("gs:///tmp/object", Set("blob"), None))

    for ((uri, schemes, expected) <- cases) {
      withClue(s"$uri with aliases $schemes: ") {
        NativeConfig.bucketForUri(new URI(uri), schemes) shouldBe expected
      }
    }
  }

  test("objectStoreKey - matches the native object store key for each path form") {
    // The same table is asserted natively by `object_store_key_matches_jvm_fixture` in
    // native/core/src/parquet/parquet_support.rs. Native scans group files by this key, and the
    // native planner rejects a partition whose files resolve to different stores. The scheme
    // lists are comma-separated; an empty libhdfs list means the `hdfs` default.
    case class Case(path: String, aliases: String, libhdfs: String, key: String, hdfs: Boolean)
    val account = "account.dfs.core.windows.net"
    val cases = Seq(
      // s3a is read through the s3 store; the bucket keeps its case and port.
      Case("s3a://Bucket.Upper/k.parquet", "", "", "s3://Bucket.Upper", hdfs = false),
      Case("s3://bucket:9000/k.parquet", "", "", "s3://bucket:9000", hdfs = false),
      Case("s3a://user:secret@bucket/k.parquet", "", "", "s3://bucket", hdfs = false),
      Case("S3A://bucket/k.parquet", "", "", "s3://bucket", hdfs = false),
      Case("s3a:///bucket/k.parquet", "", "", "s3://bucket", hdfs = false),
      Case("s3n://bucket/k.parquet", "", "", "s3n://bucket", hdfs = false),
      // A scheme routed through libhdfs keeps its spelling; the decision uses the scheme as
      // written, so listing s3 does not capture s3a. The last two share a key but not a store.
      Case("s3a://bucket/k.parquet", "", "s3a", "s3a://bucket", hdfs = true),
      Case("s3a://bucket/k.parquet", "", "s3", "s3://bucket", hdfs = false),
      Case("s3://bucket/k.parquet", "", "s3", "s3://bucket", hdfs = true),
      // An opted-in alias is read through the s3 store, promoting a hostless bucket, unless
      // libhdfs also lists it.
      Case("blob://bucket/k.parquet", "blob", "", "s3://bucket", hdfs = false),
      Case("blob:///bucket/k.parquet", "blob", "", "s3://bucket", hdfs = false),
      Case("blob:/bucket/k.parquet", "blob", "", "s3://bucket", hdfs = false),
      Case("blob://bucket/k.parquet", "", "", "blob://bucket", hdfs = false),
      Case("blob://bucket/k.parquet", "blob", "blob", "blob://bucket", hdfs = true),
      // Hostless plain s3 is not promoted.
      Case("s3:///bucket/k.parquet", "", "", "s3://", hdfs = false),
      // A listed URL-spec special scheme is never an alias.
      Case("file:///tmp/t/k.parquet", "file", "", "file://", hdfs = false),
      Case("file:/tmp/t/k.parquet", "", "", "file://", hdfs = false),
      Case(
        "HTTPS://Host.Example.com/k.parquet",
        "",
        "",
        "https://host.example.com",
        hdfs = false),
      Case("hdfs://nn:8020/t/k.parquet", "", "", "hdfs://nn:8020", hdfs = true),
      Case("hdfs:///t/k.parquet", "", "", "hdfs://", hdfs = true),
      Case("gs://bucket/k.parquet", "", "", "gs://bucket", hdfs = false),
      // ABFS keeps the container from the user info; WASB does not.
      Case(s"abfss://container@$account/k.parquet", "", "", s"abfss://container@$account", false),
      Case(s"abfs://container@$account/k.parquet", "", "", s"abfs://container@$account", false),
      Case(
        "wasbs://container@account.blob.core.windows.net/k.parquet",
        "",
        "",
        "wasbs://account.blob.core.windows.net",
        hdfs = false))

    for (c <- cases) {
      val libhdfs = if (c.libhdfs.isEmpty) Set("hdfs") else NativeConfig.parseSchemeSet(c.libhdfs)
      withClue(s"${c.path} with aliases '${c.aliases}' and libhdfs '${c.libhdfs}': ") {
        val key =
          NativeConfig.objectStoreKey(
            new URI(c.path),
            NativeConfig.parseSchemeSet(c.aliases),
            libhdfs)
        (key.key, key.isLibhdfs) shouldBe ((c.key, c.hdfs))
      }
    }
    // A path without a scheme is read from the local file system.
    NativeConfig.objectStoreKey(new URI("/tmp/t/k.parquet"), Set.empty, Set("hdfs")) shouldBe
      NativeConfig.ObjectStoreKey("file://", isLibhdfs = false)
  }

  test("resolveLibhdfsSchemes - hdfs when unset or blank, otherwise the configured list") {
    val conf = new Configuration(false)
    NativeConfig.resolveLibhdfsSchemes(conf) shouldBe Set("hdfs")
    conf.set(COMET_LIBHDFS_SCHEMES_KEY, "  ")
    NativeConfig.resolveLibhdfsSchemes(conf) shouldBe Set("hdfs")
    conf.set(COMET_LIBHDFS_SCHEMES_KEY, " S3A , fake ")
    NativeConfig.resolveLibhdfsSchemes(conf) shouldBe Set("s3a", "fake")
  }

  test("resolveS3CompliantSchemes - comma list is trimmed and lowercased, empty means none") {
    val conf = new Configuration(false)
    assert(
      NativeConfig.resolveS3CompliantSchemes(conf).isEmpty,
      "missing config must yield no aliases (opt-in default)")
    conf.set(COMET_S3_COMPLIANT_SCHEMES_KEY, " Blob , MINIO ,, r2 ")
    assert(
      NativeConfig.resolveS3CompliantSchemes(conf) == Set("blob", "minio", "r2"),
      "schemes must be split on commas, trimmed, lowercased, with blanks dropped")
  }
}
