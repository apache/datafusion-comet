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
    Seq(
      "s3a://test-bucket/test-object",
      "s3://test-bucket/test-object",
      "blob://test-bucket/test-object").foreach { path =>
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
    // Hadoop ABFS authentication (account keys, OAuth client credentials, Workload
    // Identity / MSI token providers, SAS tokens) all live under fs.azure.*, not
    // fs.abfs.*. Earlier versions of NativeConfig only forwarded fs.abfs.* for abfs[s]
    // schemes, which silently dropped every real credential. Verify the abfs[s] prefix
    // list now includes fs.azure.*.
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

  test("extractObjectStoreOptions - blob:// forwards vendor fs.blob.<authority>.* keys as s3a") {
    // Some blob:// connectors use per-authority Hadoop keys and never set a region: the AWS
    // SDK v1 client they build is happy with just an endpoint. Comet's native S3 path goes
    // through object_store's AmazonS3Builder, which reads `fs.s3a.bucket.<bucket>.<suffix>`.
    // When a user routes a bucket via `blob://` with an existing S3-compliant storage config,
    // Comet must translate those keys automatically or the read fails with `Failed to resolve
    // region: Bucket not found` (the AWS auto-detect HEAD).
    val hadoopConf = new Configuration()
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
    // Path-style is a common requirement of these S3-compatible services and must be
    // propagated so signing targets the endpoint's path form rather than
    // <bucket>.<endpoint-host>.
    assert(opts("fs.s3a.bucket.mybucket.path.style.access") == "true")
    // Per-authority translation must also apply to the second bucket seen in the same config.
    assert(opts("fs.s3a.bucket.other.endpoint") == "https://other.example.internal")
    // No region is set by the vendor connector; Comet must not synthesize one -- object_store's
    // builder will default it, and non-AWS services typically accept any region in the SigV4
    // credential.
    assert(!opts.contains("fs.s3a.bucket.mybucket.endpoint.region"))
  }

  test("extractObjectStoreOptions - blob:// endpoint wins over conflicting fs.s3a.*") {
    // For a blob:// URL, `fs.blob.<authority>.*` is the authoritative namespace. A user's
    // `fs.s3a.*` in the same Spark session usually targets an unrelated s3a-scheme workload;
    // leaking that endpoint into the blob connection silently sends credentials to the wrong
    // service and produces a misleading 403 "access key Id you provided does not exist".
    val hadoopConf = new Configuration()
    hadoopConf.set("fs.blob.default.endpoint", "s3-compat.example.com")
    hadoopConf.set("fs.blob.default.awsAccessKeyId", "AKIA-blob")
    hadoopConf.set("fs.blob.default.awsSecretAccessKey", "secret-blob")
    // Unrelated s3a-scheme endpoint the user also configured -- must NOT influence blob://.
    hadoopConf.set("fs.s3a.endpoint", "other-s3.example.com")
    hadoopConf.set("fs.s3a.endpoint.region", "other-region")

    val opts = NativeConfig.extractObjectStoreOptions(
      hadoopConf,
      new URI("blob:///mybucket/dataset/part-0.parquet"))
    assert(opts("fs.s3a.endpoint") == "s3-compat.example.com")
    assert(opts("fs.s3a.access.key") == "AKIA-blob")
    assert(opts("fs.s3a.secret.key") == "secret-blob")
    assert(opts("fs.s3a.path.style.access") == "true")
  }

  test("extractObjectStoreOptions - blob translation does not fire for s3:// / s3a://") {
    // The fs.blob.* namespace is scoped to blob:// URIs; do not surface it on plain s3/s3a URIs.
    val hadoopConf = new Configuration()
    hadoopConf.set("fs.blob.mybucket.endpoint", "https://s3-compat.example.internal")
    val opts = NativeConfig.extractObjectStoreOptions(hadoopConf, new URI("s3a://mybucket/x"))
    assert(!opts.contains("fs.s3a.bucket.mybucket.endpoint"))
  }

  test("extractObjectStoreOptions - blob:// maps fs.blob.default.* to global fs.s3a.*") {
    // Some blob:// filesystem implementations fall back to authority=\"default\" whenever the
    // URI has none. The actual S3 bucket in that case comes from the URL path, so per-bucket
    // fs.s3a.bucket.default.* would never match at runtime. Translate to global fs.s3a.* so the
    // credentials/endpoint apply to whichever bucket the URL path resolves to.
    val hadoopConf = new Configuration()
    hadoopConf.set("fs.blob.default.endpoint", "https://s3-compat.example.internal")
    hadoopConf.set("fs.blob.default.awsAccessKeyId", "AKIA-default")
    hadoopConf.set("fs.blob.default.awsSecretAccessKey", "secret-default")

    // `blob:///mybucket/...` -- URL authority is empty, bucket is in the path.
    val opts = NativeConfig.extractObjectStoreOptions(
      hadoopConf,
      new URI("blob:///mybucket/dataset/part-0.parquet"))

    // GLOBAL keys, not per-bucket.
    assert(opts("fs.s3a.endpoint") == "https://s3-compat.example.internal")
    assert(opts("fs.s3a.access.key") == "AKIA-default")
    assert(opts("fs.s3a.secret.key") == "secret-default")
    // Vendor backends typically use path-style; propagate globally so the real-bucket request
    // signs path-style even though the user did not set fs.s3a.path.style.access explicitly.
    assert(opts("fs.s3a.path.style.access") == "true")
    // Must NOT surface as fs.s3a.bucket.default.* -- that key would never match the real bucket
    // and the auto-region HEAD to AWS would still fire.
    assert(!opts.contains("fs.s3a.bucket.default.endpoint"))
    assert(!opts.contains("fs.s3a.bucket.default.access.key"))
  }
}
