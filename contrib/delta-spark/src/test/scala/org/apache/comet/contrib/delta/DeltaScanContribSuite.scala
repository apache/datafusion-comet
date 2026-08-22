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

import java.io.File
import java.net.URI
import java.nio.file.Files
import java.util.UUID

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.security.alias.CredentialProviderFactory
import org.apache.spark.sql.delta.actions.DeletionVectorDescriptor

import org.apache.comet.{CometConf, ExtendedExplainInfo}
import org.apache.comet.rules.CometScanRule

/**
 * Guards the claim-path behaviors that used to be enforced by core's now-deleted extension-SPI
 * suite: the contrib is never active when Comet exec or Comet scan is disabled, and the claim
 * hook runs before core's metadata-column guard.
 */
class DeltaScanContribSuite extends CometDeltaTestBase {

  test("contrib is inert when comet exec is disabled") {
    // The COMET_EXEC_ENABLED gate moved from core's (deleted) extension call site into
    // DeltaScanContrib.tryTransformV1; this pins it there.
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.range(0, 100).write.format("delta").save(path)

      withSQLConf(CometConf.COMET_EXEC_ENABLED.key -> "false") {
        val df = spark.read.format("delta").load(path)
        checkSparkAnswer(df)
        assert(deltaNativeScans(df).isEmpty)
      }
    }
  }

  test("contrib is inert when comet native scan is disabled") {
    // COMET_NATIVE_SCAN_ENABLED is checked in CometScanRule.transformScan before any V1
    // handling, so it short-circuits the CometScanContrib hook too.
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.range(0, 100).write.format("delta").save(path)

      withSQLConf(CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "false") {
        val df = spark.read.format("delta").load(path)
        checkSparkAnswer(df)
        assert(deltaNativeScans(df).isEmpty)
      }
    }
  }

  test("claim runs before core's metadata-column guard") {
    // A DV read's plan carries generated metadata columns that core's generic V1 guard
    // would decline; the scan still goes native because CometScanContrib.tryTransformV1
    // is consulted first (CometScanRule.transformV1Scan hook order).
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.range(0, 1000).selectExpr("id", "id * 2 as v").write.format("delta").save(path)
      spark.sql(
        s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
      spark.sql(s"DELETE FROM delta.`$path` WHERE id % 2 = 0")

      val df = spark.read.format("delta").load(path)
      checkSparkAnswer(df)
      assert(deltaNativeScans(df).nonEmpty)
    }
  }

  test("declined scan carries the contrib's fallback reason, not core's generic one") {
    // Disabling Spark's vectorized Parquet reader is a scan the contrib recognizes
    // (DeltaScanSupport.isDeltaScan) but explicitly declines (DeltaScanSupport.declineReason,
    // mirroring core's own vectorized-reader gate). Per the CometScanContrib ownership
    // contract the contrib still claims it (tagging its own fallback reason), so core's
    // generic V1 gate -- and its "Unsupported file format" message -- never runs on it.
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.range(0, 100).write.format("delta").save(path)

      withSQLConf(
        "spark.sql.parquet.enableVectorizedReader" -> "false",
        // CometTestBase flips this to "true" so the rest of the suite can exercise the
        // vectorized-off path against Comet's native scan; put it back to its real default so
        // this gate actually declines.
        CometConf.COMET_SCAN_ALLOW_DISABLED_PARQUET_VECTORIZED_READER.key -> "false") {
        val df = spark.read.format("delta").load(path)

        val (_, cometPlan) = checkSparkAnswerAndFallbackReason(
          df,
          "Native Delta scan is incompatible with " +
            "spark.sql.parquet.enableVectorizedReader=false")

        val reasons = new ExtendedExplainInfo().getFallbackReasons(cometPlan)
        assert(
          !reasons.exists(_.contains("Unsupported file format")),
          s"Did not expect core's generic fallback reason among: $reasons")
      }
    }
  }

  test("multiStoreReason declines data files spanning multiple object-store authorities") {
    // Same bucket, different keys: one authority, claimable.
    assert(
      DeltaScanSupport
        .multiStoreReason(
          Seq(new URI("s3a://bucket/a/part-0.parquet"), new URI("s3a://bucket/b/part-1.parquet")))
        .isEmpty)

    // Distinct buckets: two authorities, must decline (this is the shallow-clone-across-
    // buckets-plus-append shape the shared native scan builder cannot route correctly, since
    // it resolves the whole scan's ObjectStoreUrl from the first file only).
    val reason = DeltaScanSupport.multiStoreReason(
      Seq(new URI("s3a://bucket-a/part-0.parquet"), new URI("s3a://bucket-b/part-1.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("multiple object stores"))
    assert(reason.get.contains("bucket-a"))
    assert(reason.get.contains("bucket-b"))

    // file:// paths never carry an authority (host/port are always empty), so local scans
    // across distinct directories are unaffected.
    assert(
      DeltaScanSupport
        .multiStoreReason(
          Seq(new URI("file:///tmp/a/part-0.parquet"), new URI("file:///tmp/b/part-1.parquet")))
        .isEmpty)
  }

  test(
    "multiStoreReason declines cross-container abfss shallow clones (userinfo normalization)") {
    // Same storage account, different containers: URI#getHost drops the userinfo entirely, so
    // keying the authority on host alone would collapse containerA and containerB into one
    // authority and silently claim a cross-container shallow clone. getAuthority (used by
    // uriAuthority) keeps the userinfo, so this must decline.
    val reason = DeltaScanSupport.multiStoreReason(
      Seq(
        new URI("abfss://containerA@account.dfs.core.windows.net/a/part-0.parquet"),
        new URI("abfss://containerB@account.dfs.core.windows.net/b/part-1.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("multiple object stores"))

    // Same container: one authority, so multiStoreReason itself still passes this shape
    // unchanged (this gate was never touched by the userinfo work). But every abfss:// URI here
    // carries userinfo (the container) in its authority, so declineReason's earlier-firing
    // userInfoBearingAuthorityReason gate now declines this input before multiStoreReason ever
    // runs on it -- pinned directly here since multiStoreReason alone can no longer observe the
    // difference between this shape and a truly userinfo-free single-authority scan.
    val sameContainer = Seq(
      new URI("abfss://container@account.dfs.core.windows.net/a/part-0.parquet"),
      new URI("abfss://container@account.dfs.core.windows.net/b/part-1.parquet"))
    assert(DeltaScanSupport.multiStoreReason(sameContainer).isEmpty)
    assert(DeltaScanSupport.userInfoBearingAuthorityReason(sameContainer).isDefined)
  }

  test(
    "multiStoreReason declines distinct underscore-bearing GCS buckets " +
      "(URI#getHost null-collapse)") {
    // `gs://my_bucket` has an underscore reg-name, which URI#getHost cannot parse -- it returns
    // null for the WHOLE authority, not just an empty host. Keying uriAuthority on getHost alone
    // would make every underscore-bearing bucket normalize to the same "null host" authority
    // regardless of which bucket it actually is, so two distinct underscore buckets would
    // wrongly collapse into one authority and never decline -- even though the native side
    // parses `gs://my_bucket` and `gs://other_bucket` as genuinely different authorities and
    // would hard-error on them. getAuthority (used by uriAuthority) returns the raw authority
    // text regardless of RFC 3986 conformance, so this must decline instead.
    val reason = DeltaScanSupport.multiStoreReason(
      Seq(
        new URI("gs://my_bucket/a/part-0.parquet"),
        new URI("gs://other_bucket/b/part-1.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("multiple object stores"))

    // Same underscore-bearing bucket: one authority, claimable on both the JVM gate and the
    // native check (native side asserted in delta_spark_scan.rs's
    // same_underscore_host_bucket_files_pass).
    assert(
      DeltaScanSupport
        .multiStoreReason(
          Seq(
            new URI("gs://my_bucket/a/part-0.parquet"),
            new URI("gs://my_bucket/b/part-1.parquet")))
        .isEmpty)
  }

  test(
    "userInfoBearingAuthorityReason declines a single userinfo-bearing abfss authority " +
      "(the behavior change: one container alone is no longer claimable)") {
    val reason = DeltaScanSupport.userInfoBearingAuthorityReason(
      Seq(new URI("abfss://container@account.dfs.core.windows.net/a/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("userinfo"))
  }

  test(
    "userInfoBearingAuthorityReason declines two containers on one storage account " +
      "(the residual cross-container DV case from the maintainer's review)") {
    val reason = DeltaScanSupport.userInfoBearingAuthorityReason(
      Seq(
        new URI("abfss://source@account.dfs.core.windows.net/a/part-0.parquet"),
        new URI("abfss://clone@account.dfs.core.windows.net/_delta_log/dv/deletion_vector.bin")))
    assert(reason.isDefined)
  }

  test(
    "userInfoBearingAuthorityReason passes s3a data-file and deletion-vector paths " +
      "(no regression for the MinIO live suites)") {
    // Same bucket: userinfo-free authority, unaffected.
    assert(
      DeltaScanSupport
        .userInfoBearingAuthorityReason(
          Seq(
            new URI("s3a://bucket/a/part-0.parquet"),
            new URI("s3a://bucket/_delta_log/dv/deletion_vector.bin")))
        .isEmpty)

    // Distinct buckets, still no userinfo on either: this gate only inspects userinfo, so it is
    // unaffected by multiStoreReason's separate authority-count decline (ported from the deleted
    // storeIdentityCollisionReason suite's "passes distinct s3a buckets" case).
    assert(
      DeltaScanSupport
        .userInfoBearingAuthorityReason(
          Seq(
            new URI("s3a://bucket-a/part-0.parquet"),
            new URI("s3a://bucket-b/deletion_vector.bin")))
        .isEmpty)
  }

  test("userInfoBearingAuthorityReason passes file:// paths (no authority at all)") {
    assert(
      DeltaScanSupport
        .userInfoBearingAuthorityReason(
          Seq(new URI("file:///tmp/a/part-0.parquet"), new URI("file:///tmp/b/part-1.parquet")))
        .isEmpty)
  }

  test(
    "userInfoBearingAuthorityReason: underscore-bearing GCS bucket passes without userinfo, " +
      "declines with it (raw-authority parsing, not URI#getHost)") {
    // `gs://my_bucket` has an underscore reg-name that URI#getHost cannot parse (returns null
    // for the whole authority); no userinfo either way, so this must pass.
    assert(
      DeltaScanSupport
        .userInfoBearingAuthorityReason(Seq(new URI("gs://my_bucket/a/part-0.parquet")))
        .isEmpty)

    // Same underscore-bearing bucket, now with userinfo: uriUserInfo's raw last-`@` split still
    // finds it even though URI#getHost/getUserInfo would return null for this authority.
    val reason = DeltaScanSupport.userInfoBearingAuthorityReason(
      Seq(new URI("gs://u1@my_bucket/a/part-0.parquet")))
    assert(reason.isDefined)
  }

  test("userInfoBearingAuthorityReason passes an hdfs authority with no userinfo") {
    assert(
      DeltaScanSupport
        .userInfoBearingAuthorityReason(Seq(new URI("hdfs://nn:8020/table/part-0.parquet")))
        .isEmpty)
  }

  test(
    "userInfoBearingAuthorityReason redacts userinfo out of the decline reason (never leaks " +
      "embedded credentials)") {
    val reason = DeltaScanSupport.userInfoBearingAuthorityReason(
      Seq(new URI("s3a://AKIAEXAMPLE:secr3t@bucket/a/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("bucket"))
    assert(!reason.get.contains("secr3t"))
    assert(!reason.get.contains("AKIAEXAMPLE"))
  }

  test(
    "unsupportedSelectedSchemeReason declines an all-viewfs selection, naming the scheme and " +
      "the selected-file/DV wording") {
    val viewfsUri = new URI("viewfs://cluster/table/part-0.parquet")
    // Precondition: guards against a fail-open native build vacuously passing this test --
    // isNativelyReadableScheme falls back to TRUE when the native library can't be consulted
    // (see its doc), which would make viewfs look natively readable and this test pass for the
    // wrong reason regardless of whether the new gate is even wired up correctly.
    assert(!CometScanRule.isNativelyReadableScheme(viewfsUri))

    val reason = DeltaScanSupport.unsupportedSelectedSchemeReason(
      Seq(viewfsUri, new URI("viewfs://cluster/table/part-1.parquet")),
      Set("hdfs"))
    assert(reason.isDefined)
    assert(reason.get.contains("viewfs"))
    assert(reason.get.contains("data file or deletion vector"))
  }

  test(
    "unsupportedSelectedSchemeReason declines a mixed file:+viewfs selection with the scheme " +
      "reason (pins its ordering ahead of the authority gates)") {
    // A supported-scheme file alongside an unsupported-scheme one: this shape ALSO spans
    // multiple object-store authorities (multiStoreReason below would decline it too), but
    // declineReason places the scheme gate first, so callers must see the scheme reason here,
    // not whatever the authority gates would have said about this same input.
    val fileUri = new URI("file:///tmp/table/part-0.parquet")
    val viewfsUri = new URI("viewfs://cluster/table/part-1.parquet")
    assert(!CometScanRule.isNativelyReadableScheme(viewfsUri))

    val reason =
      DeltaScanSupport.unsupportedSelectedSchemeReason(Seq(fileUri, viewfsUri), Set("hdfs"))
    assert(reason.isDefined)
    assert(reason.get.contains("viewfs"))
    // Confirms this input really would ALSO trip multiStoreReason, so the assertion above is
    // meaningfully pinning which reason wins under declineReason's ordering, not merely proving
    // the scheme gate fires in isolation.
    assert(DeltaScanSupport.multiStoreReason(Seq(fileUri, viewfsUri)).isDefined)
  }

  test(
    "unsupportedSelectedSchemeReason declines a viewfs deletion-vector absolute path even when " +
      "every data file is file:// (proves dvUris is part of the gated URI set)") {
    val dvUri = new URI("viewfs://cluster/table/_delta_log/dv/deletion_vector.bin")
    assert(!CometScanRule.isNativelyReadableScheme(dvUri))

    val dataFileUris = Seq(new URI("file:///tmp/table/part-0.parquet"))
    val reason =
      DeltaScanSupport.unsupportedSelectedSchemeReason(dataFileUris :+ dvUri, Set("hdfs"))
    assert(reason.isDefined)
    assert(reason.get.contains("viewfs"))
  }

  test(
    "unsupportedSelectedSchemeReason passes all-file: and all-s3a: selections (no regression " +
      "for the MinIO live suites)") {
    assert(
      DeltaScanSupport
        .unsupportedSelectedSchemeReason(
          Seq(
            new URI("file:///tmp/a/part-0.parquet"),
            new URI("file:///tmp/b/deletion_vector.bin")),
          Set("hdfs"))
        .isEmpty)
    assert(
      DeltaScanSupport
        .unsupportedSelectedSchemeReason(
          Seq(
            new URI("s3a://bucket/a/part-0.parquet"),
            new URI("s3a://bucket/_delta_log/dv/deletion_vector.bin")),
          Set("hdfs"))
        .isEmpty)
  }

  test(
    "unsupportedSelectedSchemeReason passes an all-viewfs selection when viewfs is configured " +
      "as a libhdfs scheme (exemption honored on the new call site)") {
    val viewfsUri = new URI("viewfs://cluster/table/part-0.parquet")
    assert(!CometScanRule.isNativelyReadableScheme(viewfsUri))

    assert(
      DeltaScanSupport.unsupportedSelectedSchemeReason(Seq(viewfsUri), Set("viewfs")).isEmpty)
  }

  test("mergedObjectStoreOptions unions options across every authority without leaking schemes") {
    // Guards finding 8: the merge must reach a DV sidecar living on a different provider than
    // the data files (e.g. S3 data + ABFS deletion vector), and must never hand an unrelated
    // provider's credentials to a scan that never referenced it.
    val hadoopConf = new org.apache.hadoop.conf.Configuration(false)
    hadoopConf.set("fs.s3a.access.key", "s3-access-key")
    hadoopConf.set("fs.s3a.secret.key", "s3-secret-key")
    hadoopConf.set("fs.azure.account.key.acct.dfs.core.windows.net", "azure-account-key")

    val s3Uri = new URI("s3a://bucket/data.parquet")
    val abfssUri = new URI("abfss://container@acct.dfs.core.windows.net/dv.bin")

    val merged =
      CometDeltaNativeScan.mergedObjectStoreOptions(hadoopConf, Seq(s3Uri, abfssUri))
    assert(merged.get("fs.s3a.access.key").contains("s3-access-key"))
    assert(merged.get("fs.s3a.secret.key").contains("s3-secret-key"))
    assert(
      merged
        .get("fs.azure.account.key.acct.dfs.core.windows.net")
        .contains("azure-account-key"))

    // s3-only input must not leak the azure credentials into the merged map.
    val s3Only = CometDeltaNativeScan.mergedObjectStoreOptions(hadoopConf, Seq(s3Uri))
    assert(s3Only.get("fs.s3a.access.key").contains("s3-access-key"))
    assert(!s3Only.keys.exists(_.startsWith("fs.azure.")))
  }

  test(
    "storeUris dedups by authority: one representative URI per (scheme, authority), even " +
      "when DV files live at distinct paths on the same authority") {
    // No Spark session involved, and deliberately NOT a file:// scan: a local-path test can't
    // exercise a DV sidecar on a foreign authority (extractObjectStoreOptions returns an empty
    // map for file://), which is exactly the shape finding 8 fixes. Hand-build descriptors via
    // Delta's own factory methods instead of going through a real scan/claim.
    val tableRootPath = new Path("s3a://bucket-root/table")
    val firstFileUri = Some(new URI("s3a://bucket-root/table/part-0.parquet"))

    // Path-based ('p') DV on a different authority than the data files / table root.
    val foreignDv = DeletionVectorDescriptor
      .onDiskWithAbsolutePath("abfss://acct.dfs.core.windows.net/dv1.bin", 40, 4)
    // A SECOND, distinct path on the SAME foreign authority as `foreignDv` -- finding 2's shape:
    // before the per-authority dedup, N deletion-vector files on one external store yielded ~N
    // distinct URIs here (each independently walked by mergedObjectStoreOptions); now they
    // collapse to a single representative.
    val sameAuthoritySecondDv = DeletionVectorDescriptor
      .onDiskWithAbsolutePath("abfss://acct.dfs.core.windows.net/dv2.bin", 40, 4)
    // UUID-relative ('u') DV: resolves under the table root's authority (s3a/bucket-root), which
    // `firstFileUri` already represents -- must not add a second entry for that authority.
    val relativeDv = DeletionVectorDescriptor.onDiskWithRelativePath(UUID.randomUUID(), "", 40, 4)
    // Inline ('i') DV: no external URI at all; must not be resolved (would throw -- inline
    // descriptors fail `absolutePath`'s `isOnDisk` precondition) and must contribute nothing.
    val inlineDv = DeletionVectorDescriptor.inlineInLog(Array[Byte](1, 2, 3), 1)

    val uris = CometDeltaNativeScan.storeUris(
      Seq(foreignDv, sameAuthoritySecondDv, relativeDv, inlineDv),
      tableRootPath,
      firstFileUri)

    // Exactly one representative per authority: s3a/bucket-root (firstFileUri wins -- it is
    // first in candidate order, ahead of the table root and the relative DV's resolution) and
    // abfss/acct.dfs.core.windows.net (foreignDv wins over sameAuthoritySecondDv, the first DV
    // seen on that authority).
    assert(
      uris == Seq(firstFileUri.get, new URI("abfss://acct.dfs.core.windows.net/dv1.bin")),
      s"expected exactly one representative URI per authority, got: $uris")
  }

  test("storeUris always includes firstFileUri and the table root even with no DV descriptors") {
    val tableRootPath = new Path("file:///tmp/table")
    val firstFileUri = Some(new URI("file:///tmp/table/part-0.parquet"))

    // firstFileUri and tableRootPath share the same (empty) file:// authority, so the table root
    // is deduped away in favor of firstFileUri, which is first in candidate order.
    assert(
      CometDeltaNativeScan.storeUris(Seq.empty, tableRootPath, firstFileUri) ==
        Seq(firstFileUri.get))

    // No first file (e.g. an empty selected-partitions edge case): table root alone, no crash.
    assert(
      CometDeltaNativeScan.storeUris(Seq.empty, tableRootPath, None) ==
        Seq(tableRootPath.toUri))
  }

  test("user guide documents the native Delta scan config verbatim") {
    // Guards against the config's `.doc` drifting out of sync with the hand-written user-guide
    // page (the generated table only covers `docs/source/user-guide/latest`, so there is no
    // build-time check tying the two together).
    val docsPath = DeltaScanContribSuite.findRepoFile("docs/source/user-guide/latest/delta.md")
    docsPath match {
      case None =>
        cancel(
          "Could not locate docs/source/user-guide/latest/delta.md from this checkout; " +
            "skipping the docs drift guard.")
      case Some(file) =>
        val contents = scala.io.Source.fromFile(file, "UTF-8").mkString
        assert(
          contents.contains(DeltaScanConf.COMET_DELTA_NATIVE_ENABLED.key),
          s"Expected ${file.getAbsolutePath} to mention " +
            s"${DeltaScanConf.COMET_DELTA_NATIVE_ENABLED.key}")
        assert(
          contents.contains(DeltaScanConf.COMET_DELTA_NATIVE_ENABLED.doc),
          s"Expected ${file.getAbsolutePath} to contain the config's doc string verbatim")
    }
  }

  /**
   * Builds a real JCEKS keystore backing `hadoop.security.credential.provider.path`, seeded with
   * `entries`, and hands `test` a fresh [[Configuration]] already pointed at it (path only --
   * `entries` are NOT mirrored into the plain conf; callers add plain values themselves when a
   * case needs them). Uses `CredentialProviderFactory` directly (the real API `Configuration#
   * getPassword` reads through), not a hand-rolled keystore, so these tests exercise the actual
   * Hadoop credential-provider resolution path rather than a stand-in for it. The store password
   * defaults to `"none"` when neither `HADOOP_CREDSTORE_PASSWORD` nor a password file is set in
   * the test environment, which is the JCEKS provider's own documented default -- nothing extra
   * to configure here.
   */
  private def withJceks(entries: Map[String, String])(test: Configuration => Unit): Unit = {
    val storeFile = File.createTempFile("comet-delta-creds", ".jceks")
    // JavaKeyStoreProvider creates the backing file itself on first flush; a pre-existing empty
    // file (createTempFile always creates one) makes it treat the store as an existing, empty
    // keystore instead -- harmless either way for JCEKS, but deleting it first keeps this fixture
    // honest about what it is actually exercising (provider-created, not merely provider-opened).
    storeFile.delete()
    val providerPath = "jceks://file" + storeFile.getAbsolutePath
    try {
      val buildConf = new Configuration(false)
      buildConf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, providerPath)
      val provider = CredentialProviderFactory.getProviders(buildConf).get(0)
      entries.foreach { case (alias, value) =>
        provider.createCredentialEntry(alias, value.toCharArray)
      }
      provider.flush()

      val testConf = new Configuration(false)
      testConf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, providerPath)
      test(testConf)
    } finally {
      storeFile.delete()
    }
  }

  test(
    "credentialAliasReason declines when access/secret keys exist only in a JCEKS keystore, " +
      "naming the alias key but never the secret") {
    withJceks(Map("fs.s3a.access.key" -> "AKIAEXAMPLE", "fs.s3a.secret.key" -> "s3cr3tValue")) {
      conf =>
        val reason = DeltaScanSupport.credentialAliasReason(
          conf,
          Seq(new URI("s3a://mybucket/part-0.parquet")))
        assert(reason.isDefined)
        assert(reason.get.contains("fs.s3a.access.key"))
        assert(!reason.get.contains("AKIAEXAMPLE"))
        assert(!reason.get.contains("s3cr3tValue"))
    }
  }

  test(
    "credentialAliasReason passes when only plain keys are set and no provider path is " +
      "configured (zero-I/O precheck exit)") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.access.key", "AKIAPLAIN")
    conf.set("fs.s3a.secret.key", "plainSecret")
    assert(
      DeltaScanSupport
        .credentialAliasReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "credentialAliasReason passes when the provider path is set and the plain keys match the " +
      "keystore (the maintainer's requested case)") {
    withJceks(Map("fs.s3a.access.key" -> "AKIAMATCH", "fs.s3a.secret.key" -> "matchingSecret")) {
      conf =>
        conf.set("fs.s3a.access.key", "AKIAMATCH")
        conf.set("fs.s3a.secret.key", "matchingSecret")
        assert(
          DeltaScanSupport
            .credentialAliasReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
            .isEmpty)
    }
  }

  test(
    "credentialAliasReason declines when the keystore value differs from a shadowed plain " +
      "value") {
    withJceks(Map("fs.s3a.access.key" -> "AKIAKEYSTORE")) { conf =>
      conf.set("fs.s3a.access.key", "AKIADIFFERENTPLAIN")
      val reason = DeltaScanSupport.credentialAliasReason(
        conf,
        Seq(new URI("s3a://mybucket/part-0.parquet")))
      assert(reason.isDefined)
      assert(reason.get.contains("fs.s3a.access.key"))
      assert(!reason.get.contains("AKIAKEYSTORE"))
    }
  }

  test(
    "credentialAliasReason declines on an S3A-scoped provider path immediately, without " +
      "touching a nonexistent keystore (Arm A proves no keystore I/O)") {
    val tempDir = Files.createTempDirectory("comet-delta-no-keystore")
    try {
      val conf = new Configuration(false)
      val nonexistentPath = "jceks://file" + tempDir + "/does-not-exist.jceks"
      conf.set("fs.s3a.security.credential.provider.path", nonexistentPath)
      // No exception from a missing file is the point of this test: Arm A declines on the
      // presence of the S3A-scoped path key alone, never reading it.
      val reason = DeltaScanSupport.credentialAliasReason(
        conf,
        Seq(new URI("s3a://mybucket/part-0.parquet")))
      assert(reason.isDefined)
      assert(reason.get.contains("fs.s3a.security.credential.provider.path"))
    } finally {
      Files.delete(tempDir)
    }
  }

  test(
    "credentialAliasReason passes file:// URIs regardless of any provider path (S3-only scope)") {
    val conf = new Configuration(false)
    conf.set("hadoop.security.credential.provider.path", "jceks://file/nonexistent.jceks")
    assert(
      DeltaScanSupport
        .credentialAliasReason(conf, Seq(new URI("file:///tmp/table/part-0.parquet")))
        .isEmpty)
  }

  test(
    "credentialAliasReason declines via a per-bucket credential alias " +
      "(fs.s3a.bucket.mybucket.access.key)") {
    withJceks(Map("fs.s3a.bucket.mybucket.access.key" -> "AKIABUCKETSCOPED")) { conf =>
      val reason = DeltaScanSupport.credentialAliasReason(
        conf,
        Seq(new URI("s3a://mybucket/part-0.parquet")))
      assert(reason.isDefined)
      assert(reason.get.contains("fs.s3a.bucket.mybucket.access.key"))
      assert(!reason.get.contains("AKIABUCKETSCOPED"))
    }
  }

  test(
    "credentialAliasReason declines via a long-form per-bucket credential alias " +
      "(fs.s3a.bucket.mybucket.fs.s3a.access.key), a Hadoop S3AUtils.lookupPassword alias " +
      "the short-form check alone misses") {
    withJceks(
      Map(
        "fs.s3a.bucket.mybucket.fs.s3a.access.key" -> "AKIALONGFORM",
        "fs.s3a.bucket.mybucket.fs.s3a.secret.key" -> "longFormSecret")) { conf =>
      val reason = DeltaScanSupport.credentialAliasReason(
        conf,
        Seq(new URI("s3a://mybucket/part-0.parquet")))
      assert(reason.isDefined)
      assert(reason.get.contains("fs.s3a.bucket.mybucket.fs.s3a.access.key"))
      assert(!reason.get.contains("AKIALONGFORM"))
      assert(!reason.get.contains("longFormSecret"))
    }
  }

  test(
    "credentialAliasReason declines via a long-form per-bucket credential alias even when " +
      "different plain global keys are also configured (Hadoop would resolve the long-form " +
      "keystore value first; native reads only the differing plain globals)") {
    withJceks(
      Map(
        "fs.s3a.bucket.mybucket.fs.s3a.access.key" -> "AKIALONGFORM",
        "fs.s3a.bucket.mybucket.fs.s3a.secret.key" -> "longFormSecret")) { conf =>
      conf.set("fs.s3a.access.key", "AKIADIFFERENTGLOBAL")
      conf.set("fs.s3a.secret.key", "differentGlobalSecret")
      val reason = DeltaScanSupport.credentialAliasReason(
        conf,
        Seq(new URI("s3a://mybucket/part-0.parquet")))
      assert(reason.isDefined)
      assert(reason.get.contains("fs.s3a.bucket.mybucket.fs.s3a.access.key"))
      assert(!reason.get.contains("AKIALONGFORM"))
      assert(!reason.get.contains("longFormSecret"))
      assert(!reason.get.contains("AKIADIFFERENTGLOBAL"))
      assert(!reason.get.contains("differentGlobalSecret"))
    }
  }

  test(
    "credentialAliasReason declines on a long-form per-bucket provider path immediately, " +
      "without touching a nonexistent keystore (Arm A proves no keystore I/O)") {
    val tempDir = Files.createTempDirectory("comet-delta-no-keystore-long-bucket")
    try {
      val conf = new Configuration(false)
      val nonexistentPath = "jceks://file" + tempDir + "/does-not-exist.jceks"
      conf.set("fs.s3a.bucket.mybucket.fs.s3a.security.credential.provider.path", nonexistentPath)
      // No exception from a missing file is the point of this test: Arm A declines on the
      // presence of the long-form bucket-scoped path key alone, never reading it.
      val reason = DeltaScanSupport.credentialAliasReason(
        conf,
        Seq(new URI("s3a://mybucket/part-0.parquet")))
      assert(reason.isDefined)
      assert(
        reason.get.contains("fs.s3a.bucket.mybucket.fs.s3a.security.credential.provider.path"))
    } finally {
      Files.delete(tempDir)
    }
  }

  test(
    "credentialAliasReason passes when only plain global keys are set and no provider path " +
      "is configured, including the long-form bucket provider path (control: unaffected by " +
      "the new long-form aliases)") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.access.key", "AKIAPLAIN")
    conf.set("fs.s3a.secret.key", "plainSecret")
    assert(
      DeltaScanSupport
        .credentialAliasReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "credentialAliasReason declines without throwing when the keystore is corrupt/unreadable " +
      "(global arm try/catch containment)") {
    val corruptFile = File.createTempFile("comet-delta-corrupt-creds", ".jceks")
    try {
      Files.write(corruptFile.toPath, Array[Byte](1, 2, 3, 4, 5, 6, 7, 8))
      val conf = new Configuration(false)
      conf.set(
        "hadoop.security.credential.provider.path",
        "jceks://file" + corruptFile.getAbsolutePath)
      // Must not throw: a corrupt/unreadable keystore must decline this bucket, not escape and
      // abort planning for the whole session.
      val reason = DeltaScanSupport.credentialAliasReason(
        conf,
        Seq(new URI("s3a://mybucket/part-0.parquet")))
      assert(reason.isDefined)
    } finally {
      corruptFile.delete()
    }
  }
}

object DeltaScanContribSuite {

  /**
   * Walks up from a candidate root (the `comet.repo.root` system property when set, otherwise
   * `user.dir`) looking for `relativePath`. Handles both a repo-root working directory and a
   * module-root working directory (e.g. `contrib/delta-spark`) without hardcoding either.
   */
  private def findRepoFile(relativePath: String): Option[File] = {
    val startDir = Option(System.getProperty("comet.repo.root"))
      .map(new File(_))
      .getOrElse(new File(System.getProperty("user.dir")))
    Iterator
      .iterate(Option(startDir))(_.flatMap(d => Option(d.getParentFile)))
      .takeWhile(_.isDefined)
      .map(_.get)
      .map(new File(_, relativePath))
      .find(_.isFile)
  }
}
