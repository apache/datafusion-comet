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
import java.util.{Locale, UUID}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.s3a.S3AUtils
import org.apache.hadoop.security.alias.CredentialProviderFactory
import org.apache.spark.sql.delta.actions.DeletionVectorDescriptor

import org.apache.comet.{CometConf, ExtendedExplainInfo}
import org.apache.comet.rules.CometScanRule

/**
 * Guards the contrib claim path: the contrib is never active when Comet exec or Comet scan is
 * disabled, and the claim hook runs before core's metadata-column guard.
 */
class DeltaScanContribSuite extends CometDeltaTestBase {

  test("contrib is inert when comet exec is disabled") {
    // The COMET_EXEC_ENABLED gate lives in DeltaScanContrib.tryTransformV1; this pins it there.
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

  test(
    "vectorized reader disabled still claims natively when the safety conf allows it " +
      "(claim-direction control for the decline above)") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.range(0, 100).write.format("delta").save(path)

      withSQLConf(
        "spark.sql.parquet.enableVectorizedReader" -> "false",
        CometConf.COMET_SCAN_ALLOW_DISABLED_PARQUET_VECTORIZED_READER.key -> "true") {
        val df = spark.read.format("delta").load(path)
        checkSparkAnswer(df)
        assert(deltaNativeScans(df).nonEmpty)
      }
    }
  }

  test(
    "unsupportedSchemes declines an all-viewfs root-path selection (the same helper " +
      "declineReason applies to scanExec.relation.location.rootPaths, ahead of the " +
      "selected-file gate)") {
    val viewfsUri = new URI("viewfs://cluster/table")
    // Precondition, mirroring the selected-file scheme tests below: guards against a fail-open
    // native build vacuously passing this test.
    assert(!CometScanRule.isNativelyReadableScheme(viewfsUri, Set.empty))

    val schemes = DeltaScanSupport.unsupportedSchemes(Seq(viewfsUri), Set("hdfs"))
    assert(schemes == Set("viewfs"))
  }

  test(
    "unsupportedSchemes flags an opted-in S3-compliant alias scheme that core's gate admits " +
      "(the contrib never hands core its alias set)") {
    val blobUri = new URI("blob://bucket/table")
    // Precondition: core admits the alias once opted in, and only then.
    assert(CometScanRule.isNativelyReadableScheme(blobUri, Set("blob")))
    assert(!CometScanRule.isNativelyReadableScheme(blobUri, Set.empty))

    assert(DeltaScanSupport.unsupportedSchemes(Seq(blobUri), Set("hdfs")) == Set("blob"))
  }

  test(
    "s3CompliantAliasSchemeReason declines an opted-in alias root path with the explaining " +
      "reason, and passes when the scheme is not opted in or is plain s3a") {
    val blobUri = new URI("blob://bucket/table")
    val s3aUri = new URI("s3a://bucket/table")

    val optedIn = new Configuration(false)
    optedIn.set(CometConf.COMET_S3_COMPLIANT_SCHEMES_KEY, " Blob , minio ")
    val reason = DeltaScanSupport.s3CompliantAliasSchemeReason(optedIn, Seq(s3aUri, blobUri))
    assert(reason.isDefined)
    assert(reason.get.contains("blob"))
    assert(reason.get.contains(CometConf.COMET_S3_COMPLIANT_SCHEMES_KEY))
    assert(reason.get.contains("S3AFileSystem"))
    assert(DeltaScanSupport.s3CompliantAliasSchemeReason(optedIn, Seq(s3aUri)).isEmpty)

    val notOptedIn = new Configuration(false)
    assert(DeltaScanSupport.s3CompliantAliasSchemeReason(notOptedIn, Seq(blobUri)).isEmpty)
  }

  test("libhdfsSchemes parses the list exactly like core's scan gate (trim, lowercase, blanks)") {
    withSQLConf(CometConf.COMET_LIBHDFS_SCHEMES.key -> " HDFS , viewfs ,, ") {
      assert(DeltaScanSupport.libhdfsSchemes == Set("hdfs", "viewfs"))
    }
    assert(DeltaScanSupport.libhdfsSchemes == Set("hdfs"))
  }

  test("unsupportedSchemes passes an all-file: root-path selection (no regression)") {
    assert(
      DeltaScanSupport
        .unsupportedSchemes(Seq(new URI("file:///tmp/table")), Set("hdfs"))
        .isEmpty)
  }

  test(
    "unsupportedSchemes passes a root-path scheme configured as a libhdfs exemption " +
      "(exemption honored for the root-path call site too)") {
    val viewfsUri = new URI("viewfs://cluster/table")
    assert(!CometScanRule.isNativelyReadableScheme(viewfsUri, Set.empty))

    assert(DeltaScanSupport.unsupportedSchemes(Seq(viewfsUri), Set("viewfs")).isEmpty)
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
      "(cross-container deletion-vector authority on a single storage account)") {
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
    assert(!CometScanRule.isNativelyReadableScheme(viewfsUri, Set.empty))

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
    assert(!CometScanRule.isNativelyReadableScheme(viewfsUri, Set.empty))

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
    assert(!CometScanRule.isNativelyReadableScheme(dvUri, Set.empty))

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
    assert(!CometScanRule.isNativelyReadableScheme(viewfsUri, Set.empty))

    assert(
      DeltaScanSupport.unsupportedSelectedSchemeReason(Seq(viewfsUri), Set("viewfs")).isEmpty)
  }

  test("mergedObjectStoreOptions unions options across every authority without leaking schemes") {
    // The merge must reach a DV sidecar living on a different provider than the data files
    // (e.g. S3 data + ABFS deletion vector), and must never hand an unrelated provider's
    // credentials to a scan that never referenced it.
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
    // map for file://), which is exactly the shape that requires unioning object-store options
    // across every authority. Hand-build descriptors via Delta's own factory methods instead of
    // going through a real scan/claim.
    val tableRootPath = new Path("s3a://bucket-root/table")
    val firstFileUri = Some(new URI("s3a://bucket-root/table/part-0.parquet"))

    // Path-based ('p') DV on a different authority than the data files / table root.
    val foreignDv = DeletionVectorDescriptor
      .onDiskWithAbsolutePath("abfss://acct.dfs.core.windows.net/dv1.bin", 40, 4)
    // A SECOND, distinct path on the SAME foreign authority as `foreignDv` -- the shape that
    // motivates per-authority dedup: before dedup, N deletion-vector files on one external store
    // yielded ~N distinct URIs here (each independently walked by mergedObjectStoreOptions); now
    // they collapse to a single representative.
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
    "gcsHadoopOnlyAuthReason declines a gs data file relying only on a Hadoop service-account " +
      "keyfile, naming the key but never the value, and matches the scheme case-insensitively") {
    val conf = new Configuration(false)
    conf.set("fs.gs.auth.service.account.json.keyfile", "/secret/path/svc-key.json")
    val reason =
      DeltaScanSupport.gcsHadoopOnlyAuthReason(conf, Seq(new URI("GS://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("fs.gs.auth.service.account.json.keyfile"))
    assert(reason.get.contains("mybucket"))
    assert(!reason.get.contains("/secret/path/svc-key.json"))
  }

  test(
    "gcsHadoopOnlyAuthReason passes a gs URI when no fs.gs.auth.* key is set " +
      "(Application Default Credentials work in both engines)") {
    val conf = new Configuration(false)
    assert(
      DeltaScanSupport
        .gcsHadoopOnlyAuthReason(conf, Seq(new URI("gs://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "gcsHadoopOnlyAuthReason does not fire for s3a/file URIs even when fs.gs.auth.* is set " +
      "(scheme-scoped)") {
    val conf = new Configuration(false)
    conf.set("fs.gs.auth.service.account.json.keyfile", "/secret/path/svc-key.json")
    assert(
      DeltaScanSupport
        .gcsHadoopOnlyAuthReason(
          conf,
          Seq(
            new URI("s3a://mybucket/part-0.parquet"),
            new URI("file:///tmp/table/part-0.parquet")))
        .isEmpty)
  }

  test(
    "gcsHadoopOnlyAuthReason declines when local data files are mixed with an absolute gs " +
      "deletion-vector sidecar backed only by a Hadoop keyfile") {
    val conf = new Configuration(false)
    conf.set("fs.gs.auth.service.account.json.keyfile", "/secret/path/svc-key.json")
    val reason = DeltaScanSupport.gcsHadoopOnlyAuthReason(
      conf,
      Seq(
        new URI("file:///tmp/table/part-0.parquet"),
        new URI("gs://mybucket/_delta_log/deletion_vector_abc123.bin")))
    assert(reason.isDefined)
    assert(reason.get.contains("fs.gs.auth.service.account.json.keyfile"))
    assert(reason.get.contains("mybucket"))
  }

  test(
    "gcsHadoopOnlyAuthReason's decline reason names every offending fs.gs.auth.* key but never " +
      "any of their configured values") {
    val conf = new Configuration(false)
    conf.set("fs.gs.auth.service.account.json.keyfile", "/secret/path/svc-key.json")
    conf.set("fs.gs.auth.client.id", "super-secret-client-id-xyz")
    val reason =
      DeltaScanSupport.gcsHadoopOnlyAuthReason(conf, Seq(new URI("gs://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("fs.gs.auth.service.account.json.keyfile"))
    assert(reason.get.contains("fs.gs.auth.client.id"))
    assert(!reason.get.contains("/secret/path/svc-key.json"))
    assert(!reason.get.contains("super-secret-client-id-xyz"))
  }

  test(
    "gcsHadoopOnlyAuthReason declines a gs data file relying only on the legacy " +
      "google.cloud.auth.* connector prefix, naming the key but never the value") {
    val conf = new Configuration(false)
    conf.set("google.cloud.auth.service.account.json.keyfile", "/secret/path/svc-key.json")
    val reason =
      DeltaScanSupport.gcsHadoopOnlyAuthReason(conf, Seq(new URI("gs://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("google.cloud.auth.service.account.json.keyfile"))
    assert(reason.get.contains("mybucket"))
    assert(!reason.get.contains("/secret/path/svc-key.json"))
  }

  test(
    "gcsHadoopOnlyAuthReason does not fire for s3a/file URIs even when google.cloud.auth.* is " +
      "set (scheme-scoped)") {
    val conf = new Configuration(false)
    conf.set("google.cloud.auth.service.account.json.keyfile", "/secret/path/svc-key.json")
    assert(
      DeltaScanSupport
        .gcsHadoopOnlyAuthReason(
          conf,
          Seq(
            new URI("s3a://mybucket/part-0.parquet"),
            new URI("file:///tmp/table/part-0.parquet")))
        .isEmpty)
  }

  test(
    "gcsHadoopOnlyAuthReason's decline reason names offending keys under both fs.gs.auth. and " +
      "google.cloud.auth. but never any of their configured values") {
    val conf = new Configuration(false)
    conf.set("fs.gs.auth.client.id", "super-secret-client-id-xyz")
    conf.set("google.cloud.auth.service.account.json.keyfile", "/secret/path/svc-key.json")
    val reason =
      DeltaScanSupport.gcsHadoopOnlyAuthReason(conf, Seq(new URI("gs://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("fs.gs.auth.client.id"))
    assert(reason.get.contains("google.cloud.auth.service.account.json.keyfile"))
    assert(!reason.get.contains("super-secret-client-id-xyz"))
    assert(!reason.get.contains("/secret/path/svc-key.json"))
  }

  test(
    "gcsHadoopOnlyAuthReason declines a gs data file relying only on the deprecated " +
      "fs.gs.service.account.auth.keyfile key (reversed word order vs the modern " +
      "fs.gs.auth.service.account.* prefix), naming the key but never the value") {
    val conf = new Configuration(false)
    conf.set("fs.gs.service.account.auth.keyfile", "/secret/path/svc-key.p12")
    val reason =
      DeltaScanSupport.gcsHadoopOnlyAuthReason(conf, Seq(new URI("gs://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("fs.gs.service.account.auth.keyfile"))
    assert(reason.get.contains("mybucket"))
    assert(!reason.get.contains("/secret/path/svc-key.p12"))
  }

  test(
    "gcsHadoopOnlyAuthReason declines a gs data file relying only on the deprecated " +
      "fs.gs.service.account.auth.email key, naming the key but never the value") {
    val conf = new Configuration(false)
    conf.set("fs.gs.service.account.auth.email", "svc@example-project.iam.gserviceaccount.com")
    val reason =
      DeltaScanSupport.gcsHadoopOnlyAuthReason(conf, Seq(new URI("gs://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("fs.gs.service.account.auth.email"))
    assert(!reason.get.contains("svc@example-project.iam.gserviceaccount.com"))
  }

  test(
    "gcsHadoopOnlyAuthReason declines a gs data file relying only on the deprecated " +
      "google.cloud.service.account.auth.keyfile key, naming the key but never the value") {
    val conf = new Configuration(false)
    conf.set("google.cloud.service.account.auth.keyfile", "/secret/path/svc-key.p12")
    val reason =
      DeltaScanSupport.gcsHadoopOnlyAuthReason(conf, Seq(new URI("gs://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("google.cloud.service.account.auth.keyfile"))
    assert(!reason.get.contains("/secret/path/svc-key.p12"))
  }

  test(
    "gcsHadoopOnlyAuthReason declines a gs data file relying only on the deprecated " +
      "google.cloud.service.account.auth.email key, naming the key but never the value") {
    val conf = new Configuration(false)
    conf.set(
      "google.cloud.service.account.auth.email",
      "svc@example-project.iam.gserviceaccount.com")
    val reason =
      DeltaScanSupport.gcsHadoopOnlyAuthReason(conf, Seq(new URI("gs://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("google.cloud.service.account.auth.email"))
    assert(!reason.get.contains("svc@example-project.iam.gserviceaccount.com"))
  }

  test(
    "gcsHadoopOnlyAuthReason does not fire for s3a/file URIs even when the deprecated " +
      "fs.gs.service.account.auth.* prefix is set (scheme-scoped)") {
    val conf = new Configuration(false)
    conf.set("fs.gs.service.account.auth.keyfile", "/secret/path/svc-key.p12")
    assert(
      DeltaScanSupport
        .gcsHadoopOnlyAuthReason(
          conf,
          Seq(
            new URI("s3a://mybucket/part-0.parquet"),
            new URI("file:///tmp/table/part-0.parquet")))
        .isEmpty)
  }

  test(
    "gcsHadoopOnlyAuthReason declines on fs.gs.auth.type, a suffix no fixed prefix list ever " +
      "enumerated (predicate-based matching instead of a prefix table)") {
    val conf = new Configuration(false)
    conf.set("fs.gs.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE")
    val reason =
      DeltaScanSupport.gcsHadoopOnlyAuthReason(conf, Seq(new URI("gs://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("fs.gs.auth.type"))
    assert(reason.get.contains("mybucket"))
    assert(!reason.get.contains("SERVICE_ACCOUNT_JSON_KEYFILE"))
  }

  test(
    "gcsHadoopOnlyAuthReason declines on fs.gs.auth.client.id, naming the key but never the " +
      "value") {
    val conf = new Configuration(false)
    conf.set("fs.gs.auth.client.id", "super-secret-client-id-xyz")
    val reason =
      DeltaScanSupport.gcsHadoopOnlyAuthReason(conf, Seq(new URI("gs://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("fs.gs.auth.client.id"))
    assert(!reason.get.contains("super-secret-client-id-xyz"))
  }

  test(
    "s3ConfigDivergenceReason declines when access/secret keys exist only in a JCEKS " +
      "keystore, naming the base key and bucket but never the secret") {
    withJceks(Map("fs.s3a.access.key" -> "AKIAEXAMPLE", "fs.s3a.secret.key" -> "s3cr3tValue")) {
      conf =>
        val reason = DeltaScanSupport.s3ConfigDivergenceReason(
          conf,
          Seq(new URI("s3a://mybucket/part-0.parquet")))
        assert(reason.isDefined)
        assert(reason.get.contains("fs.s3a.access.key"))
        assert(reason.get.contains("mybucket"))
        assert(!reason.get.contains("AKIAEXAMPLE"))
        assert(!reason.get.contains("s3cr3tValue"))
    }
  }

  test(
    "s3ConfigDivergenceReason passes when only plain keys are set and no provider path is " +
      "configured (zero-I/O precheck exit)") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.access.key", "AKIAPLAIN")
    conf.set("fs.s3a.secret.key", "plainSecret")
    assert(
      DeltaScanSupport
        .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "s3ConfigDivergenceReason passes when the provider path is set and the plain keys match " +
      "the keystore (plain keys consistent with the credential provider)") {
    withJceks(Map("fs.s3a.access.key" -> "AKIAMATCH", "fs.s3a.secret.key" -> "matchingSecret")) {
      conf =>
        conf.set("fs.s3a.access.key", "AKIAMATCH")
        conf.set("fs.s3a.secret.key", "matchingSecret")
        assert(
          DeltaScanSupport
            .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
            .isEmpty)
    }
  }

  test(
    "s3ConfigDivergenceReason declines when the keystore value differs from a shadowed plain " +
      "value") {
    withJceks(Map("fs.s3a.access.key" -> "AKIAKEYSTORE")) { conf =>
      conf.set("fs.s3a.access.key", "AKIADIFFERENTPLAIN")
      val reason = DeltaScanSupport.s3ConfigDivergenceReason(
        conf,
        Seq(new URI("s3a://mybucket/part-0.parquet")))
      assert(reason.isDefined)
      assert(reason.get.contains("fs.s3a.access.key"))
      assert(!reason.get.contains("AKIAKEYSTORE"))
    }
  }

  test(
    "s3ConfigDivergenceReason declines on an S3A-scoped provider path immediately, without " +
      "touching a nonexistent keystore (Arm A proves no keystore I/O)") {
    val tempDir = Files.createTempDirectory("comet-delta-no-keystore")
    try {
      val conf = new Configuration(false)
      val nonexistentPath = "jceks://file" + tempDir + "/does-not-exist.jceks"
      conf.set("fs.s3a.security.credential.provider.path", nonexistentPath)
      // No exception from a missing file is the point of this test: Arm A declines on the
      // presence of the S3A-scoped path key alone, never reading it.
      val reason = DeltaScanSupport.s3ConfigDivergenceReason(
        conf,
        Seq(new URI("s3a://mybucket/part-0.parquet")))
      assert(reason.isDefined)
      assert(reason.get.contains("fs.s3a.security.credential.provider.path"))
    } finally {
      Files.delete(tempDir)
    }
  }

  test(
    "s3ConfigDivergenceReason passes file:// URIs regardless of any provider path " +
      "(S3-only scope)") {
    val conf = new Configuration(false)
    conf.set("hadoop.security.credential.provider.path", "jceks://file/nonexistent.jceks")
    assert(
      DeltaScanSupport
        .s3ConfigDivergenceReason(conf, Seq(new URI("file:///tmp/table/part-0.parquet")))
        .isEmpty)
  }

  test(
    "s3ConfigDivergenceReason declines via a per-bucket credential alias " +
      "(fs.s3a.bucket.mybucket.access.key)") {
    withJceks(Map("fs.s3a.bucket.mybucket.access.key" -> "AKIABUCKETSCOPED")) { conf =>
      val reason = DeltaScanSupport.s3ConfigDivergenceReason(
        conf,
        Seq(new URI("s3a://mybucket/part-0.parquet")))
      assert(reason.isDefined)
      assert(reason.get.contains("fs.s3a.access.key"))
      assert(reason.get.contains("mybucket"))
      assert(!reason.get.contains("AKIABUCKETSCOPED"))
    }
  }

  test(
    "s3ConfigDivergenceReason declines via a long-form per-bucket credential alias " +
      "(fs.s3a.bucket.mybucket.fs.s3a.access.key), a Hadoop S3AUtils.lookupPassword alias " +
      "the short-form check alone misses") {
    withJceks(
      Map(
        "fs.s3a.bucket.mybucket.fs.s3a.access.key" -> "AKIALONGFORM",
        "fs.s3a.bucket.mybucket.fs.s3a.secret.key" -> "longFormSecret")) { conf =>
      val reason = DeltaScanSupport.s3ConfigDivergenceReason(
        conf,
        Seq(new URI("s3a://mybucket/part-0.parquet")))
      assert(reason.isDefined)
      assert(reason.get.contains("fs.s3a.access.key"))
      assert(reason.get.contains("mybucket"))
      assert(!reason.get.contains("AKIALONGFORM"))
      assert(!reason.get.contains("longFormSecret"))
    }
  }

  test(
    "s3ConfigDivergenceReason declines via a long-form per-bucket credential alias even when " +
      "different plain global keys are also configured (Hadoop would resolve the long-form " +
      "keystore value first; native reads only the differing plain globals)") {
    withJceks(
      Map(
        "fs.s3a.bucket.mybucket.fs.s3a.access.key" -> "AKIALONGFORM",
        "fs.s3a.bucket.mybucket.fs.s3a.secret.key" -> "longFormSecret")) { conf =>
      conf.set("fs.s3a.access.key", "AKIADIFFERENTGLOBAL")
      conf.set("fs.s3a.secret.key", "differentGlobalSecret")
      val reason = DeltaScanSupport.s3ConfigDivergenceReason(
        conf,
        Seq(new URI("s3a://mybucket/part-0.parquet")))
      assert(reason.isDefined)
      assert(reason.get.contains("fs.s3a.access.key"))
      assert(reason.get.contains("mybucket"))
      assert(!reason.get.contains("AKIALONGFORM"))
      assert(!reason.get.contains("longFormSecret"))
      assert(!reason.get.contains("AKIADIFFERENTGLOBAL"))
      assert(!reason.get.contains("differentGlobalSecret"))
    }
  }

  test(
    "s3ConfigDivergenceReason declines on a long-form per-bucket provider path immediately, " +
      "without touching a nonexistent keystore (Arm A proves no keystore I/O)") {
    val tempDir = Files.createTempDirectory("comet-delta-no-keystore-long-bucket")
    try {
      val conf = new Configuration(false)
      val nonexistentPath = "jceks://file" + tempDir + "/does-not-exist.jceks"
      conf.set("fs.s3a.bucket.mybucket.fs.s3a.security.credential.provider.path", nonexistentPath)
      // No exception from a missing file is the point of this test: Arm A declines on the
      // presence of the long-form bucket-scoped path key alone, never reading it.
      val reason = DeltaScanSupport.s3ConfigDivergenceReason(
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
    "s3ConfigDivergenceReason passes when only plain global keys are set and no provider " +
      "path is configured, including the long-form bucket provider path (control: unaffected " +
      "by the new long-form aliases)") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.access.key", "AKIAPLAIN")
    conf.set("fs.s3a.secret.key", "plainSecret")
    assert(
      DeltaScanSupport
        .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "s3ConfigDivergenceReason declines without throwing when the keystore is " +
      "corrupt/unreadable (global arm try/catch containment)") {
    val corruptFile = File.createTempFile("comet-delta-corrupt-creds", ".jceks")
    try {
      Files.write(corruptFile.toPath, Array[Byte](1, 2, 3, 4, 5, 6, 7, 8))
      val conf = new Configuration(false)
      conf.set(
        "hadoop.security.credential.provider.path",
        "jceks://file" + corruptFile.getAbsolutePath)
      // Must not throw: a corrupt/unreadable keystore must decline this bucket, not escape and
      // abort planning for the whole session.
      val reason = DeltaScanSupport.s3ConfigDivergenceReason(
        conf,
        Seq(new URI("s3a://mybucket/part-0.parquet")))
      assert(reason.isDefined)
    } finally {
      corruptFile.delete()
    }
  }

  test(
    "s3ConfigDivergenceReason declines when a plain long-form bucket credential key is set " +
      "with nothing else (Hadoop resolves it, native's short-then-global lookup never sees " +
      "it), naming the base key and bucket but never a credential value") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.bucket.mybucket.fs.s3a.access.key", "AKIALONGPLAIN")
    conf.set("fs.s3a.bucket.mybucket.fs.s3a.secret.key", "longPlainSecret")
    val reason = DeltaScanSupport
      .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("fs.s3a.access.key"))
    assert(reason.get.contains("mybucket"))
    assert(!reason.get.contains("AKIALONGPLAIN"))
    assert(!reason.get.contains("longPlainSecret"))
  }

  test("s3ConfigDivergenceReason declines when a long-form bucket credential holds a Hadoop " +
    "${...} reference that DOES resolve, with nothing else set (substitution alone does not " +
    "erase the long-form divergence: native's short-then-global read never consults the long " +
    "form regardless of what it expands to), naming the base key and bucket but never a value") {
    val conf = new Configuration(false)
    conf.set("review.longFormAccess", "AKIALONGRESOLVED")
    conf.set("fs.s3a.bucket.mybucket.fs.s3a.access.key", "${review.longFormAccess}")
    val reason = DeltaScanSupport
      .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("fs.s3a.access.key"))
    assert(reason.get.contains("mybucket"))
    assert(!reason.get.contains("AKIALONGRESOLVED"))
    assert(!reason.get.contains("${review.longFormAccess}"))
  }

  test(
    "s3ConfigDivergenceReason declines when a plain long-form bucket credential diverges " +
      "from a different plain global value (Hadoop would use the long-form bucket value; " +
      "native would use the differing global)") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.bucket.mybucket.fs.s3a.access.key", "AKIALONGPLAIN")
    conf.set("fs.s3a.access.key", "AKIADIFFERENTGLOBALPLAIN")
    val reason = DeltaScanSupport
      .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("fs.s3a.access.key"))
    assert(reason.get.contains("mybucket"))
    assert(!reason.get.contains("AKIALONGPLAIN"))
    assert(!reason.get.contains("AKIADIFFERENTGLOBALPLAIN"))
  }

  test(
    "s3ConfigDivergenceReason declines when the plain long-form and short-form bucket " +
      "credential keys are set to DIFFERENT values (Hadoop's SimpleAWSCredentialsProvider " +
      "resolves the long pair; native resolves the short pair, so they diverge), naming the " +
      "base key and bucket but never a credential value") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.bucket.mybucket.fs.s3a.access.key", "long-ak")
    conf.set("fs.s3a.bucket.mybucket.fs.s3a.secret.key", "long-sk")
    conf.set("fs.s3a.bucket.mybucket.access.key", "short-ak")
    conf.set("fs.s3a.bucket.mybucket.secret.key", "short-sk")
    val reason = DeltaScanSupport
      .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("fs.s3a.access.key"))
    assert(reason.get.contains("mybucket"))
    assert(!reason.get.contains("long-ak"))
    assert(!reason.get.contains("short-ak"))
  }

  test(
    "control: S3AUtils#propagateBucketOptions folds a long-form bucket option into the " +
      "unread key fs.s3a.fs.s3a.endpoint, proving Hadoop itself ignores the long form for " +
      "general (non-credential) per-bucket options -- unlike lookupPassword for credentials, " +
      "no Comet gate exists (or is needed) for this case") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.bucket.mybucket.fs.s3a.endpoint", "long-form.example.com")
    conf.set("fs.s3a.endpoint", "global.example.com")

    // Real Hadoop code, not a Comet stand-in: S3AFileSystem#initialize assigns exactly this
    // result to the `conf` it reads ENDPOINT/PATH_STYLE_ACCESS/etc. from.
    val propagated = S3AUtils.propagateBucketOptions(conf, "mybucket")
    assert(propagated.get("fs.s3a.endpoint") == "global.example.com")
    assert(propagated.get("fs.s3a.fs.s3a.endpoint") == "long-form.example.com")
  }

  test(
    "s3ConfigDivergenceReason declines when a bucket-scoped credential references another " +
      "bucket-scoped key that Hadoop's real propagate-then-resolve order shadows the global " +
      "value with (Hadoop resolves the bucket-scoped referent; native, which never propagates " +
      "bucket options, still resolves the global one), naming the base key and bucket but " +
      "never a credential value") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.bucket.mybucket.access.key", "${fs.s3a.custom.ref}")
    conf.set("fs.s3a.bucket.mybucket.custom.ref", "bucket-scoped-value")
    conf.set("fs.s3a.custom.ref", "global-value")

    // Real Hadoop code, not a Comet stand-in: this is exactly what S3AFileSystem#initialize
    // assigns to the `conf` it later reads fs.s3a.access.key from -- the bucket-scoped
    // fs.s3a.bucket.mybucket.custom.ref overwrites the global fs.s3a.custom.ref BEFORE the
    // ${...} reference in the propagated fs.s3a.bucket.mybucket.access.key is ever substituted.
    val propagated = S3AUtils.propagateBucketOptions(conf, "mybucket")
    assert(propagated.get("fs.s3a.custom.ref") == "bucket-scoped-value")
    assert(propagated.get("fs.s3a.bucket.mybucket.access.key") == "bucket-scoped-value")

    val reason = DeltaScanSupport
      .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("fs.s3a.access.key"))
    assert(reason.get.contains("mybucket"))
    assert(!reason.get.contains("bucket-scoped-value"))
    assert(!reason.get.contains("global-value"))
  }

  test(
    "s3ConfigDivergenceReason passes when a bucket-scoped credential references another " +
      "bucket-scoped key whose propagated value happens to equal the global value (no actual " +
      "divergence, despite the same shadowing mechanism as the declining case above)") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.bucket.mybucket.access.key", "${fs.s3a.custom.ref}")
    conf.set("fs.s3a.bucket.mybucket.custom.ref", "same-value")
    conf.set("fs.s3a.custom.ref", "same-value")
    assert(
      DeltaScanSupport
        .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "s3ConfigDivergenceReason declines, without any keystore I/O, when a bucket-scoped " +
      "long-form credential-provider-path key (Arm A) is itself set via a ${...} reference to " +
      "another bucket-scoped key that Hadoop's real propagate-then-resolve order shadows the " +
      "global value with -- naming only the provider path key and bucket, never either " +
      "resolved path") {
    // Uses the LONG form (fs.s3a.bucket.B.fs.s3a.security.credential.provider.path), not the
    // short form, deliberately: propagateBucketOptions folds ANY fs.s3a.bucket.B.<rest> key into
    // a global fs.s3a.<rest> key. For the short form, <rest> is
    // "security.credential.provider.path", so it propagates into the GLOBAL S3A-scoped provider
    // path key itself (fs.s3a.security.credential.provider.path) -- correctly triggering the
    // OTHER Arm A branch instead, since real Hadoop would see the same thing. The long form's
    // <rest> is "fs.s3a.security.credential.provider.path", which propagates into the inert,
    // double-prefixed fs.s3a.fs.s3a.security.credential.provider.path key instead, isolating
    // the long-form bucket-scoped branch this test targets.
    val conf = new Configuration(false)
    conf.set(
      "fs.s3a.bucket.mybucket.fs.s3a.security.credential.provider.path",
      "${fs.s3a.custom.ref}")
    conf.set(
      "fs.s3a.bucket.mybucket.custom.ref",
      "jceks://file/does-not-exist-bucket-scoped.jceks")
    conf.set("fs.s3a.custom.ref", "jceks://file/does-not-exist-global.jceks")

    // Real Hadoop code, not a Comet stand-in: this is exactly what S3AFileSystem#initialize
    // assigns to the `conf` it later reads the bucket-scoped provider path from -- the
    // bucket-scoped fs.s3a.bucket.mybucket.custom.ref overwrites the global fs.s3a.custom.ref
    // BEFORE the ${...} reference in the propagated provider path key is ever substituted.
    val propagated = S3AUtils.propagateBucketOptions(conf, "mybucket")
    assert(
      propagated.get("fs.s3a.custom.ref") == "jceks://file/does-not-exist-bucket-scoped.jceks")
    assert(
      propagated.get("fs.s3a.bucket.mybucket.fs.s3a.security.credential.provider.path") ==
        "jceks://file/does-not-exist-bucket-scoped.jceks")
    // Confirms the long form's propagated target is the inert double-prefixed key, NOT the
    // global S3A-scoped provider path key -- i.e. this test genuinely isolates the long-form
    // bucket-scoped branch rather than accidentally exercising the global-S3A-path branch.
    assert(propagated.get("fs.s3a.security.credential.provider.path") == null)

    // Neither referenced path exists on disk -- if this gate mistakenly tried to open either
    // as a keystore instead of declining on the key's mere presence (Arm A), it would throw
    // rather than return a reason, which this test would catch.
    val reason = DeltaScanSupport
      .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("fs.s3a.bucket.mybucket.fs.s3a.security.credential.provider.path"))
    assert(reason.get.contains("mybucket"))
    assert(!reason.get.contains("does-not-exist-bucket-scoped"))
    assert(!reason.get.contains("does-not-exist-global"))
  }

  test(
    "s3ConfigDivergenceReason declines when the long-form and global bucket credential keys " +
      "share the same value but the short-form bucket keys are set to EMPTY strings (Hadoop's " +
      "SimpleAWSCredentialsProvider resolves the long pair via lookupPassword's skip-empty " +
      "semantics; native's get_config_trimmed resolves the short pair's mere PRESENCE, landing " +
      "on empty credentials instead), naming the base key and bucket but never a credential " +
      "value") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.bucket.mybucket.fs.s3a.access.key", "shared-ak")
    conf.set("fs.s3a.bucket.mybucket.fs.s3a.secret.key", "shared-sk")
    conf.set("fs.s3a.access.key", "shared-ak")
    conf.set("fs.s3a.secret.key", "shared-sk")
    conf.set("fs.s3a.bucket.mybucket.access.key", "")
    conf.set("fs.s3a.bucket.mybucket.secret.key", "")
    val reason = DeltaScanSupport
      .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("fs.s3a.access.key"))
    assert(reason.get.contains("mybucket"))
    assert(!reason.get.contains("shared-ak"))
  }

  test("s3ConfigDivergenceReason declines when the short-form bucket credential keys hold only " +
    "whitespace: native's get_config_trimmed still resolves the key's mere PRESENCE before " +
    "trimming its value, so a whitespace-only short-form key diverges from Hadoop's long-form " +
    "resolution exactly like an outright empty one") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.bucket.mybucket.fs.s3a.access.key", "shared-ak")
    conf.set("fs.s3a.bucket.mybucket.fs.s3a.secret.key", "shared-sk")
    conf.set("fs.s3a.access.key", "shared-ak")
    conf.set("fs.s3a.secret.key", "shared-sk")
    conf.set("fs.s3a.bucket.mybucket.access.key", "   ")
    conf.set("fs.s3a.bucket.mybucket.secret.key", "   ")
    val reason = DeltaScanSupport
      .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("fs.s3a.access.key"))
    assert(reason.get.contains("mybucket"))
  }

  test(
    "control: s3ConfigDivergenceReason passes when the short-form bucket credential keys are " +
      "absent rather than empty, so Hadoop's long-form resolution and native's short-then-global " +
      "resolution both land on the same shared pair") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.bucket.mybucket.fs.s3a.access.key", "shared-ak")
    conf.set("fs.s3a.bucket.mybucket.fs.s3a.secret.key", "shared-sk")
    conf.set("fs.s3a.access.key", "shared-ak")
    conf.set("fs.s3a.secret.key", "shared-sk")
    assert(
      DeltaScanSupport
        .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "control: s3ConfigDivergenceReason passes when a global-only value (no bucket override at " +
      "all, so Hadoop's and native's effective values are the exact same conf entry) carries " +
      "incidental leading/trailing whitespace, such as Hadoop's own multi-line " +
      "fs.s3a.aws.credentials.provider default -- trimming must apply symmetrically to both " +
      "sides of the comparison, or an untouched default value would diverge from itself and " +
      "decline every S3 scan") {
    val conf = new Configuration(false)
    conf.set(
      "fs.s3a.aws.credentials.provider",
      "\n  org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider,\n  " +
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider\n  ")
    assert(
      DeltaScanSupport
        .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "s3ConfigDivergenceReason declines when a per-bucket override redirects the ${...} " +
      "reference inside the short-form bucket endpoint while a long-form endpoint alias holds " +
      "the value native resolves (Hadoop's endpoint consumer is propagateBucketOptions plus " +
      "plain Configuration#get, which follows the redirected reference and never reads the " +
      "long form at all)") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.custom.ref", "https://store-a.example")
    conf.set("fs.s3a.bucket.data-bucket.custom.ref", "https://store-b.example")
    conf.set("fs.s3a.bucket.data-bucket.endpoint", "${fs.s3a.custom.ref}")
    conf.set("fs.s3a.bucket.data-bucket.fs.s3a.endpoint", "https://store-a.example")

    // Real Hadoop code, not a Comet stand-in: propagation overwrites the global referent with
    // the per-bucket custom.ref BEFORE the endpoint's ${...} reference is substituted, so
    // Hadoop's plain endpoint read lands on store-b -- while native, which never propagates,
    // expands the same reference against the original conf and lands on store-a.
    val propagated = S3AUtils.propagateBucketOptions(conf, "data-bucket")
    assert(propagated.get("fs.s3a.endpoint") == "https://store-b.example")

    val reason = DeltaScanSupport
      .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://data-bucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("fs.s3a.endpoint"))
    assert(reason.get.contains("data-bucket"))
    assert(!reason.get.contains("store-a"))
    assert(!reason.get.contains("store-b"))
  }

  test(
    "control: s3ConfigDivergenceReason passes the same endpoint shape without the per-bucket " +
      "referent override (the ${...} reference expands identically with and without " +
      "bucket-option propagation, so Hadoop's plain-get endpoint read and native agree)") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.custom.ref", "https://store-a.example")
    conf.set("fs.s3a.bucket.data-bucket.endpoint", "${fs.s3a.custom.ref}")
    conf.set("fs.s3a.bucket.data-bucket.fs.s3a.endpoint", "https://store-a.example")
    assert(
      DeltaScanSupport
        .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://data-bucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "s3ConfigDivergenceReason passes when ONLY the long-form bucket endpoint alias is set: " +
      "propagateBucketOptions folds it into the unread fs.s3a.fs.s3a.endpoint key, so Hadoop's " +
      "plain endpoint read and native's short-then-global read both resolve nothing") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.bucket.data-bucket.fs.s3a.endpoint", "https://store-a.example")
    assert(
      DeltaScanSupport
        .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://data-bucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "s3ConfigDivergenceReason declines an obsolete plaintext credential pair when " +
      "hadoop.security.credential.clear-text-fallback is false: Configuration#getPassword " +
      "ignores plain conf then, so Hadoop's SimpleAWSCredentialsProvider reports no " +
      "credentials and the chain proceeds to the environment -- while native would sign every " +
      "request with the stale static pair") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.access.key", "stale-ak")
    conf.set("fs.s3a.secret.key", "stale-sk")
    conf.set(
      "fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider," +
        "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
    conf.set("hadoop.security.credential.clear-text-fallback", "false")
    val reason = DeltaScanSupport
      .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("fs.s3a.access.key"))
    assert(reason.get.contains("mybucket"))
    assert(!reason.get.contains("stale-ak"))
    assert(!reason.get.contains("stale-sk"))
  }

  test(
    "control: s3ConfigDivergenceReason passes the same plaintext pair and provider chain when " +
      "clear-text-fallback keeps its default (true): getPassword falls back to plain conf, so " +
      "Hadoop and native resolve the identical static pair") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.access.key", "stale-ak")
    conf.set("fs.s3a.secret.key", "stale-sk")
    conf.set(
      "fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider," +
        "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
    assert(
      DeltaScanSupport
        .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "s3ConfigDivergenceReason passes with clear-text-fallback=false when no plaintext " +
      "credential is set anywhere: both sides resolve no credentials, and the provider-class " +
      "key itself stays comparable (its consumer is Configuration#getClasses, plain conf, " +
      "which the fallback flag never gates)") {
    val conf = new Configuration(false)
    conf.set(
      "fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider," +
        "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
    conf.set("hadoop.security.credential.clear-text-fallback", "false")
    assert(
      DeltaScanSupport
        .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  // -----------------------------------------------------------------------------------------
  // providerClassGateReason: declines an unsupported credential-provider class (or
  // invalid combination) before the scan is claimed, rather than letting it fail during
  // execution in s3.rs's build_aws_credential_provider_metadata.
  // -----------------------------------------------------------------------------------------

  private val nativeSupportedProviderClasses = Seq(
    "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider",
    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
    "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider",
    "software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider",
    "com.amazonaws.auth.ContainerCredentialsProvider",
    "com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper",
    "software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider",
    "com.amazonaws.auth.InstanceProfileCredentialsProvider",
    "software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider",
    "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
    "software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider",
    "com.amazonaws.auth.WebIdentityTokenCredentialsProvider",
    "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider",
    "com.amazonaws.auth.profile.ProfileCredentialsProvider",
    "software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider",
    "com.amazonaws.auth.AnonymousAWSCredentials")

  test(
    "providerClassGateReason passes when aws.credentials.provider is unset (native's " +
      "default AWS SDK provider chain)") {
    val conf = new Configuration(false)
    assert(
      DeltaScanSupport
        .providerClassGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test("providerClassGateReason passes for every credential provider class s3.rs supports") {
    nativeSupportedProviderClasses.foreach { className =>
      val conf = new Configuration(false)
      conf.set("fs.s3a.aws.credentials.provider", className)
      val reason =
        DeltaScanSupport
          .providerClassGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
      assert(reason.isEmpty, s"Expected $className to be claimable, but got: $reason")
    }
  }

  test(
    "providerClassGateReason declines an unsupported credential provider class, naming the " +
      "class and the bucket") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.aws.credentials.provider", "com.example.CustomCredentialsProvider")
    val reason =
      DeltaScanSupport
        .providerClassGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("com.example.CustomCredentialsProvider"))
    assert(reason.get.contains("mybucket"))
  }

  test(
    "providerClassGateReason declines via the per-bucket short form, honoring bucket-scoped " +
      "override (mirrors get_config's short-then-global resolution)") {
    val conf = new Configuration(false)
    conf.set(
      "fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    conf.set(
      "fs.s3a.bucket.mybucket.aws.credentials.provider",
      "com.example.CustomCredentialsProvider")
    val reason =
      DeltaScanSupport
        .providerClassGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("com.example.CustomCredentialsProvider"))
  }

  test(
    "providerClassGateReason passes a comma-separated list of entirely supported provider " +
      "classes (native chains them via build_chained_aws_credential_provider_metadata)") {
    val conf = new Configuration(false)
    conf.set(
      "fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider, " +
        "software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider")
    assert(
      DeltaScanSupport
        .providerClassGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "providerClassGateReason declines a comma-separated list containing one unsupported " +
      "class, naming only the unsupported one") {
    val conf = new Configuration(false)
    conf.set(
      "fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider,com.example.Bogus")
    val reason =
      DeltaScanSupport
        .providerClassGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("com.example.Bogus"))
    assert(!reason.get.contains("SimpleAWSCredentialsProvider"))
  }

  test(
    "providerClassGateReason declines an anonymous provider mixed with another provider " +
      "(native's build_credential_provider rejects this combination at execution time)") {
    val conf = new Configuration(false)
    conf.set(
      "fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider," +
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    val reason =
      DeltaScanSupport
        .providerClassGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("anonymous"))
  }

  test(
    "providerClassGateReason passes a solo anonymous provider (native returns None -- an " +
      "unsigned client -- rather than erroring; only a MIX with other providers is rejected)") {
    val conf = new Configuration(false)
    conf.set(
      "fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
    assert(
      DeltaScanSupport
        .providerClassGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "providerClassGateReason passes AssumedRoleCredentialProvider with an unset " +
      "assumed.role.credentials.provider (native defaults to its own always-supported " +
      "[Simple, EnvironmentVariable] fallback)") {
    val conf = new Configuration(false)
    conf.set(
      "fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider")
    assert(
      DeltaScanSupport
        .providerClassGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "providerClassGateReason declines AssumedRoleCredentialProvider whose " +
      "assumed.role.credentials.provider names an unsupported base provider class") {
    val conf = new Configuration(false)
    conf.set(
      "fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider")
    conf.set("fs.s3a.assumed.role.credentials.provider", "com.example.BogusBaseProvider")
    val reason =
      DeltaScanSupport
        .providerClassGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("com.example.BogusBaseProvider"))
    assert(reason.get.contains("fs.s3a.assumed.role.credentials.provider"))
  }

  test(
    "providerClassGateReason declines AssumedRoleCredentialProvider whose " +
      "assumed.role.credentials.provider names an anonymous base provider (native rejects ANY " +
      "anonymous entry here, not just a mix)") {
    val conf = new Configuration(false)
    conf.set(
      "fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider")
    conf.set(
      "fs.s3a.assumed.role.credentials.provider",
      "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
    val reason =
      DeltaScanSupport
        .providerClassGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("anonymous"))
  }

  test(
    "assumedRolePolicyGateReason declines when a global assumed-role session policy is " +
      "configured") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.assumed.role.policy", """{"Version":"2012-10-17","Statement":[]}""")
    val reason =
      DeltaScanSupport
        .assumedRolePolicyGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("fs.s3a.assumed.role.policy"))
    // The policy document itself is security-sensitive configuration; never leak it.
    assert(!reason.get.contains("2012-10-17"))
  }

  test(
    "assumedRolePolicyGateReason declines when a bucket-scoped assumed-role session policy " +
      "is configured") {
    val conf = new Configuration(false)
    conf.set(
      "fs.s3a.bucket.mybucket.assumed.role.policy",
      """{"Version":"2012-10-17","Statement":[]}""")
    val reason =
      DeltaScanSupport
        .assumedRolePolicyGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("fs.s3a.assumed.role.policy"))
    assert(!reason.get.contains("2012-10-17"))
  }

  test("assumedRolePolicyGateReason admits when no assumed-role session policy is configured") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.assumed.role.arn", "arn:aws:iam::123456789012:role/reader")
    assert(
      DeltaScanSupport
        .assumedRolePolicyGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "providerClassGateReason ignores assumed.role.credentials.provider when " +
      "AssumedRoleCredentialProvider is not itself in play (dead config on the native side)") {
    val conf = new Configuration(false)
    conf.set(
      "fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    conf.set("fs.s3a.assumed.role.credentials.provider", "com.example.BogusBaseProvider")
    assert(
      DeltaScanSupport
        .providerClassGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "providerClassGateReason passes when the global aws.credentials.provider key holds a " +
      "Hadoop variable reference that Configuration#get expands to a supported class " +
      "(post-substitution, native's plain-conf extraction sees the same expanded class name " +
      "the class-support check does, so no divergence exists to decline)") {
    val conf = new Configuration(false)
    conf.set("review.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    conf.set("fs.s3a.aws.credentials.provider", "${review.provider}")
    assert(
      DeltaScanSupport
        .providerClassGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "providerClassGateReason passes when a bucket-scoped short-form " +
      "aws.credentials.provider override holds a variable reference that expands to a " +
      "supported class, even though the global key is a different supported literal") {
    val conf = new Configuration(false)
    conf.set(
      "fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    conf.set("review.bucketProvider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    conf.set("fs.s3a.bucket.mybucket.aws.credentials.provider", "${review.bucketProvider}")
    assert(
      DeltaScanSupport
        .providerClassGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "providerClassGateReason passes when the assumed-role base-provider key holds a " +
      "variable reference that expands to a supported base class") {
    val conf = new Configuration(false)
    conf.set(
      "fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider")
    conf.set("review.baseProvider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    conf.set("fs.s3a.assumed.role.credentials.provider", "${review.baseProvider}")
    assert(
      DeltaScanSupport
        .providerClassGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "providerClassGateReason passes literal provider classes with no variable references " +
      "(unaffected by variable expansion, still runs the class-support gate)") {
    val conf = new Configuration(false)
    conf.set(
      "fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    assert(
      DeltaScanSupport
        .providerClassGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)

    val badConf = new Configuration(false)
    badConf.set("fs.s3a.aws.credentials.provider", "com.example.CustomCredentialsProvider")
    val reason =
      DeltaScanSupport
        .providerClassGateReason(badConf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("com.example.CustomCredentialsProvider"))
  }

  test(
    "providerClassGateReason declines rather than throws when a two-key mutual Hadoop " +
      "variable-reference cycle involves fs.s3a.aws.credentials.provider, called DIRECTLY " +
      "(not routed through s3ConfigDivergenceReason, which masks this for the same keys when " +
      "checked first -- this pins the gate's OWN containment, not that coupling)") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.aws.credentials.provider", "${fs.s3a.assumed.role.credentials.provider}")
    conf.set("fs.s3a.assumed.role.credentials.provider", "${fs.s3a.aws.credentials.provider}")
    val reason =
      DeltaScanSupport
        .providerClassGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("fs.s3a.aws.credentials.provider"))
    assert(reason.get.contains("IllegalStateException"))
    assert(!reason.get.contains("${fs.s3a.assumed.role.credentials.provider}"))
    assert(!reason.get.contains("${fs.s3a.aws.credentials.provider}"))
  }

  test(
    "s3ConfigDivergenceReason passes when both the plain long-form and short-form bucket " +
      "credential keys are set to the EQUAL value (Hadoop's long-first resolution and " +
      "native's short-then-global resolution agree)") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.bucket.mybucket.fs.s3a.access.key", "AKIASAMEBOTH")
    conf.set("fs.s3a.bucket.mybucket.access.key", "AKIASAMEBOTH")
    assert(
      DeltaScanSupport
        .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "s3ConfigDivergenceReason passes when the plain long-form bucket credential value equals " +
      "the plain global value (both sides resolve to the same value)") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.bucket.mybucket.fs.s3a.access.key", "AKIASAME")
    conf.set("fs.s3a.access.key", "AKIASAME")
    assert(
      DeltaScanSupport
        .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "s3ConfigDivergenceReason passes with only a plain short-form bucket credential key set " +
      "(control: unaffected by the long-form plain-value check)") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.bucket.mybucket.access.key", "AKIASHORTONLY")
    assert(
      DeltaScanSupport
        .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "s3ConfigDivergenceReason passes when a credential key holds a Hadoop variable reference " +
      "that Configuration#get expands to a literal (post-substitution, native's plain-conf " +
      "extraction forwards the SAME expanded value this comparator reads, so both sides agree)") {
    val conf = new Configuration(false)
    conf.set("review.access", "AKIAEXAMPLE")
    conf.set("fs.s3a.access.key", "${review.access}")
    assert(
      DeltaScanSupport
        .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "s3ConfigDivergenceReason passes when credential keys hold literal values with no " +
      "variable references") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.access.key", "AKIALITERAL")
    conf.set("fs.s3a.secret.key", "literalSecretValue")
    assert(
      DeltaScanSupport
        .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "s3ConfigDivergenceReason passes when a credential key references an undefined variable " +
      "(Hadoop leaves the literal unresolved, so native and Hadoop see the identical value)") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.access.key", "${undefined.var}")
    assert(
      DeltaScanSupport
        .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "s3ConfigDivergenceReason passes when a bucket-scoped short-form credential alias holds " +
      "a variable reference that expands identically for both sides (alias-set coverage " +
      "beyond the plain global key)") {
    val conf = new Configuration(false)
    conf.set("review.secret", "topSecretValue")
    conf.set("fs.s3a.bucket.mybucket.secret.key", "${review.secret}")
    assert(
      DeltaScanSupport
        .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "s3ConfigDivergenceReason does not throw for a credential key that is its own Hadoop " +
      "variable reference (Configuration#get's substitution loop converges immediately -- " +
      "the raw and expanded literals are already equal -- so this is the same safe shape as " +
      "an undefined variable, not a MAX_SUBST failure)") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.secret.key", "realSecretValue")
    conf.set("fs.s3a.access.key", "${fs.s3a.access.key}")
    val reason = DeltaScanSupport
      .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(
      reason.isEmpty,
      s"expected no decline (and no exception) for a literal " +
        s"self-reference, since it resolves to the same unexpanded text on both sides: $reason")
  }

  test(
    "s3ConfigDivergenceReason declines rather than throws when two credential keys form a " +
      "mutual Hadoop variable-reference cycle (Configuration#get raises IllegalStateException " +
      "once ${...} substitution recurses past Hadoop's MAX_SUBST bound)") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.access.key", "${fs.s3a.secret.key}")
    conf.set("fs.s3a.secret.key", "${fs.s3a.access.key}")
    val reason = DeltaScanSupport
      .s3ConfigDivergenceReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("IllegalStateException"))
    assert(!reason.get.contains("realSecretValue"))
  }

  test(
    "unsupportedEncryptionAlgorithmReason declines a bucket configured with global SSE-C, " +
      "naming the algorithm key and the algorithm but never the customer-provided key value") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.encryption.algorithm", "SSE-C")
    conf.set("fs.s3a.encryption.key", "c3VwZXItc2VjcmV0LWN1c3RvbWVyLWtleQ==")
    val reason = DeltaScanSupport
      .unsupportedEncryptionAlgorithmReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("fs.s3a.encryption.algorithm"))
    assert(reason.get.contains("SSE-C"))
    assert(reason.get.contains("mybucket"))
    assert(!reason.get.contains("c3VwZXItc2VjcmV0LWN1c3RvbWVyLWtleQ=="))
  }

  test(
    "unsupportedEncryptionAlgorithmReason matches the SSE-C algorithm value " +
      "case-insensitively, mirroring S3AEncryptionMethods#getMethod's equalsIgnoreCase " +
      "parsing") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.encryption.algorithm", "sse-c")
    val reason = DeltaScanSupport
      .unsupportedEncryptionAlgorithmReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("mybucket"))
  }

  test(
    "unsupportedEncryptionAlgorithmReason declines a bucket configured with the deprecated " +
      "fs.s3a.server-side-encryption-algorithm spelling of SSE-C, naming the algorithm key " +
      "actually consulted but never the customer-provided key value") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.server-side-encryption-algorithm", "SSE-C")
    conf.set("fs.s3a.server-side-encryption.key", "c3VwZXItc2VjcmV0LWN1c3RvbWVyLWtleQ==")
    val reason = DeltaScanSupport
      .unsupportedEncryptionAlgorithmReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    // Names EITHER spelling, never both/neither: hadoop-aws's S3AFileSystem statically registers
    // this exact pair as a Configuration-level deprecated alias (verified via javap --
    // S3AFileSystem.addDeprecatedKeys() calls Configuration.addDeprecations, a field static on
    // Hadoop's Configuration class, process-wide once S3AFileSystem's class has loaded anywhere
    // in this JVM -- which a real Spark job has always done by the time it evaluates this gate,
    // since reading the S3 table at all requires that class). Once registered,
    // Configuration#get resolves either literal key to the same value transparently, so which
    // name THIS gate happens to read the value under depends on whether that static
    // registration already ran elsewhere in the test JVM, not on anything this test controls.
    assert(
      reason.get.contains("fs.s3a.server-side-encryption-algorithm") ||
        reason.get.contains("fs.s3a.encryption.algorithm"))
    assert(reason.get.contains("mybucket"))
    assert(!reason.get.contains("c3VwZXItc2VjcmV0LWN1c3RvbWVyLWtleQ=="))
  }

  test(
    "unsupportedEncryptionAlgorithmReason declines only the bucket whose per-bucket " +
      "SHORT-form key sets SSE-C, leaving an unrelated bucket unaffected") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.bucket.secure-bucket.encryption.algorithm", "SSE-C")
    val declined = DeltaScanSupport.unsupportedEncryptionAlgorithmReason(
      conf,
      Seq(new URI("s3a://secure-bucket/part-0.parquet")))
    assert(declined.isDefined)
    assert(declined.get.contains("secure-bucket"))

    assert(
      DeltaScanSupport
        .unsupportedEncryptionAlgorithmReason(
          conf,
          Seq(new URI("s3a://other-bucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "unsupportedEncryptionAlgorithmReason DOES fire for SSE-C set only via the LONG " +
      "per-bucket form: S3AUtils#lookupBucketSecret is long-then-short, " +
      "decompiled from hadoop-aws 3.3.4's S3AUtils.class -- unlike a plain propagated option, " +
      "the encryption algorithm's bucket tier DOES consult fs.s3a.bucket.B.fs.s3a.encryption." +
      "algorithm, and Hadoop's own reader picks SSE-C from it, so this must decline exactly " +
      "like the short-form case above") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.bucket.secure-bucket.fs.s3a.encryption.algorithm", "SSE-C")
    val reason = DeltaScanSupport.unsupportedEncryptionAlgorithmReason(
      conf,
      Seq(new URI("s3a://secure-bucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("secure-bucket"))
    assert(reason.get.contains("SSE-C"))
  }

  test(
    "unsupportedEncryptionAlgorithmReason declines CSE-KMS (client-side encryption): the " +
      "native Parquet reader has no client-side decryption layer, so it would read raw " +
      "ciphertext where Hadoop's own reader, which decrypts client-side via the SDK, succeeds") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.encryption.algorithm", "CSE-KMS")
    val reason = DeltaScanSupport
      .unsupportedEncryptionAlgorithmReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("fs.s3a.encryption.algorithm"))
    assert(reason.get.contains("CSE-KMS"))
    assert(reason.get.contains("mybucket"))
  }

  test(
    "unsupportedEncryptionAlgorithmReason declines CSE-CUSTOM (client-side encryption) the " +
      "same way as CSE-KMS") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.encryption.algorithm", "CSE-CUSTOM")
    val reason = DeltaScanSupport
      .unsupportedEncryptionAlgorithmReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("CSE-CUSTOM"))
  }

  test(
    "unsupportedEncryptionAlgorithmReason declines an unrecognized future algorithm string " +
      "(allowlist semantics: anything not positively confirmed transparent declines, rather " +
      "than a blocklist that would silently admit a new Hadoop encryption method)") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.encryption.algorithm", "SOME-FUTURE-ALGORITHM")
    val reason = DeltaScanSupport
      .unsupportedEncryptionAlgorithmReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("SOME-FUTURE-ALGORITHM"))
  }

  test(
    "unsupportedEncryptionAlgorithmReason passes for AES256, SSE-KMS, DSSE-KMS, and for no " +
      "encryption configured at all (S3 decrypts these server-side algorithms transparently " +
      "on GET/HEAD given read permission alone; only SSE-C requires a client-sent key, and " +
      "only CSE-* requires client-side decryption)") {
    for (algorithm <- Seq("AES256", "SSE-KMS", "DSSE-KMS")) {
      val conf = new Configuration(false)
      conf.set("fs.s3a.encryption.algorithm", algorithm)
      conf.set("fs.s3a.encryption.key", "arn:aws:kms:us-east-1:123456789012:key/abc-123")
      assert(
        DeltaScanSupport
          .unsupportedEncryptionAlgorithmReason(
            conf,
            Seq(new URI("s3a://mybucket/part-0.parquet")))
          .isEmpty,
        s"expected $algorithm to be allowlisted")
    }

    val unsetConf = new Configuration(false)
    assert(
      DeltaScanSupport
        .unsupportedEncryptionAlgorithmReason(
          unsetConf,
          Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "unsupportedEncryptionAlgorithmReason declines when the algorithm is stored ONLY in a " +
      "JCEKS keystore as SSE-C, naming the algorithm key and value but never any keystore " +
      "material (buildEncryptionSecrets resolves the algorithm via getPassword, which this " +
      "gate now mirrors instead of the JCEKS-blind plain-conf read that used to under-decline " +
      "this case)") {
    withJceks(Map("fs.s3a.encryption.algorithm" -> "SSE-C")) { conf =>
      val reason = DeltaScanSupport
        .unsupportedEncryptionAlgorithmReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
      assert(reason.isDefined)
      assert(reason.get.contains("fs.s3a.encryption.algorithm"))
      assert(reason.get.contains("SSE-C"))
      assert(reason.get.contains("mybucket"))
    }
  }

  test(
    "unsupportedEncryptionAlgorithmReason passes a plaintext-only SSE-C algorithm when " +
      "clear-text-fallback is false and no credential provider is configured (getPassword " +
      "masks the plaintext value, so Hadoop's own buildEncryptionSecrets resolves NO algorithm " +
      "and issues plain GETs with no SSE-C key header -- exactly what native issues)") {
    // Admitting is safe on the shape's own terms: S3 enforces the customer-key-header
    // requirement at the protocol level against every reader, so on a genuinely SSE-C-encrypted
    // object both engines fail loudly and identically (400, no header sent), and on an
    // unencrypted object both read the same bytes. No config state here lets Hadoop decrypt
    // while native reads ciphertext.
    val conf = new Configuration(false)
    conf.set("fs.s3a.encryption.algorithm", "SSE-C")
    conf.set("hadoop.security.credential.clear-text-fallback", "false")
    assert(
      DeltaScanSupport
        .unsupportedEncryptionAlgorithmReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "unsupportedEncryptionAlgorithmReason passes when the algorithm is stored ONLY in a JCEKS " +
      "keystore as AES256 (allowlisted even through the keystore-aware resolution path)") {
    withJceks(Map("fs.s3a.encryption.algorithm" -> "AES256")) { conf =>
      assert(DeltaScanSupport
        .unsupportedEncryptionAlgorithmReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
    }
  }

  test(
    "unsupportedEncryptionAlgorithmReason declines without throwing when the keystore backing " +
      "the algorithm is corrupt/unreadable (global arm try/catch containment, same pattern as " +
      "s3ConfigDivergenceReason's corrupt-keystore test)") {
    val corruptFile = File.createTempFile("comet-delta-corrupt-encryption-creds", ".jceks")
    try {
      Files.write(corruptFile.toPath, Array[Byte](1, 2, 3, 4, 5, 6, 7, 8))
      val conf = new Configuration(false)
      conf.set(
        "hadoop.security.credential.provider.path",
        "jceks://file" + corruptFile.getAbsolutePath)
      // Must not throw: a corrupt/unreadable keystore must decline this bucket, not escape and
      // abort planning for the whole session.
      val reason = DeltaScanSupport
        .unsupportedEncryptionAlgorithmReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
      assert(reason.isDefined)
    } finally {
      corruptFile.delete()
    }
  }

  test(
    "unsupportedEncryptionAlgorithmReason declines on an S3A-scoped provider path immediately " +
      "when resolving the algorithm, without touching a nonexistent keystore (Arm A proves no " +
      "keystore I/O), even though no algorithm key is set in plain conf") {
    val tempDir = Files.createTempDirectory("comet-delta-encryption-no-keystore")
    try {
      val conf = new Configuration(false)
      val nonexistentPath = "jceks://file" + tempDir + "/does-not-exist.jceks"
      conf.set("fs.s3a.security.credential.provider.path", nonexistentPath)
      val reason = DeltaScanSupport
        .unsupportedEncryptionAlgorithmReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
      assert(reason.isDefined)
      assert(reason.get.contains("fs.s3a.security.credential.provider.path"))
    } finally {
      Files.delete(tempDir)
    }
  }

  test(
    "unsupportedEncryptionAlgorithmReason does not fire for non-S3 URIs even when SSE-C is " +
      "configured globally (scheme-scoped, no S3 bucket to derive from a file:// or gs:// " +
      "URI)") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.encryption.algorithm", "SSE-C")
    conf.set("fs.s3a.encryption.key", "c3VwZXItc2VjcmV0LWN1c3RvbWVyLWtleQ==")
    assert(
      DeltaScanSupport
        .unsupportedEncryptionAlgorithmReason(
          conf,
          Seq(
            new URI("file:///tmp/table/part-0.parquet"),
            new URI("gs://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "proxyGateReason declines a bucket configured with a global fs.s3a.proxy.host, naming the " +
      "key and bucket but never any proxy credential") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.proxy.host", "proxy.internal.example.com")
    conf.set("fs.s3a.proxy.port", "8080")
    conf.set("fs.s3a.proxy.username", "proxyuser")
    conf.set("fs.s3a.proxy.password", "proxySecretValue")
    val reason =
      DeltaScanSupport.proxyGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("fs.s3a.proxy.host"))
    assert(reason.get.contains("mybucket"))
    assert(!reason.get.contains("proxyuser"))
    assert(!reason.get.contains("proxySecretValue"))
    assert(!reason.get.contains("proxy.internal.example.com"))
  }

  test(
    "proxyGateReason declines via a short-form per-bucket fs.s3a.proxy.host " +
      "(fs.s3a.bucket.mybucket.proxy.host)") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.bucket.mybucket.proxy.host", "proxy.internal.example.com")
    val reason =
      DeltaScanSupport.proxyGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
    assert(reason.isDefined)
    assert(reason.get.contains("fs.s3a.proxy.host"))
    assert(reason.get.contains("mybucket"))
  }

  test(
    "proxyGateReason passes on a lone long-form per-bucket fs.s3a.proxy.host " +
      "(fs.s3a.bucket.mybucket.fs.s3a.proxy.host): propagateBucketOptions folds it into the " +
      "unread key fs.s3a.fs.s3a.proxy.host, and the host's real consumer is a plain getTrimmed " +
      "on the propagated conf that never checks any long-form alias") {
    // S3AUtils#initProxySupport (hadoop-aws 3.3.4) and AWSClientConfig#createProxyConfiguration
    // (3.4.x) both read the host as conf.getTrimmed("fs.s3a.proxy.host", ""), so a lone long
    // alias never routes Hadoop through a proxy, same fold as the fs.s3a.endpoint control above.
    val conf = new Configuration(false)
    conf.set("fs.s3a.bucket.mybucket.fs.s3a.proxy.host", "proxy.internal.example.com")
    assert(
      DeltaScanSupport
        .proxyGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "proxyGateReason passes when no fs.s3a.proxy.host is configured anywhere (zero-I/O, no " +
      "provider path set)") {
    val conf = new Configuration(false)
    assert(
      DeltaScanSupport
        .proxyGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "proxyGateReason declines only the bucket whose proxy host is actually configured, " +
      "leaving an unrelated bucket unaffected") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.bucket.proxied-bucket.proxy.host", "proxy.internal.example.com")
    val declined =
      DeltaScanSupport.proxyGateReason(conf, Seq(new URI("s3a://proxied-bucket/part-0.parquet")))
    assert(declined.isDefined)
    assert(declined.get.contains("proxied-bucket"))

    assert(
      DeltaScanSupport
        .proxyGateReason(conf, Seq(new URI("s3a://other-bucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "proxyGateReason does not fire for non-S3 URIs even when fs.s3a.proxy.host is configured " +
      "globally (scheme-scoped, no S3 bucket to derive from a file:// or gs:// URI)") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.proxy.host", "proxy.internal.example.com")
    assert(
      DeltaScanSupport
        .proxyGateReason(
          conf,
          Seq(
            new URI("file:///tmp/table/part-0.parquet"),
            new URI("gs://mybucket/part-0.parquet")))
        .isEmpty)
  }

  test(
    "proxyGateReason declines a plaintext fs.s3a.proxy.host even when a readable global " +
      "credential store is configured and clear-text-fallback is false: the host's real " +
      "consumer is a plain getTrimmed that consults neither the store nor the fallback flag") {
    // getPassword would hide this plaintext host (no store entry, conf fallback disabled), but
    // S3AUtils#initProxySupport / AWSClientConfig#createProxyConfiguration read it via plain
    // getTrimmed and route Hadoop through the proxy anyway, so the gate must still decline.
    withJceks(Map.empty) { conf =>
      conf.set("hadoop.security.credential.clear-text-fallback", "false")
      conf.set("fs.s3a.proxy.host", "proxy.internal.example.com")
      val reason =
        DeltaScanSupport.proxyGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
      assert(reason.isDefined)
      assert(reason.get.contains("fs.s3a.proxy.host"))
      assert(reason.get.contains("mybucket"))
      assert(!reason.get.contains("proxy.internal.example.com"))
    }
  }

  test(
    "proxyGateReason passes when fs.s3a.proxy.host exists only as a global credential-store " +
      "entry: the host's real consumer never calls getPassword, so a store-held host cannot " +
      "put a proxy into effect") {
    // The store entry is real and readable; only lookupPassword-family reads (proxy.username,
    // proxy.password) would find it. The host stays empty under plain getTrimmed, so Hadoop
    // itself never uses a proxy here and declining would be pure over-refusal.
    withJceks(Map("fs.s3a.proxy.host" -> "proxy.internal.example.com")) { conf =>
      assert(
        DeltaScanSupport
          .proxyGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
          .isEmpty)
    }
  }

  test(
    "proxyGateReason passes on an S3A-scoped provider path when no fs.s3a.proxy.host is set " +
      "in plain conf: no keystore, S3A-scoped or otherwise, can supply the host to its real " +
      "consumer, so provider configuration alone proves nothing about the proxy") {
    // The path points at a nonexistent store on purpose: passing here also proves the gate
    // performs no keystore I/O at all for the host, not even to rule the store out.
    val tempDir = Files.createTempDirectory("comet-delta-proxy-no-keystore")
    try {
      val conf = new Configuration(false)
      val nonexistentPath = "jceks://file" + tempDir + "/does-not-exist.jceks"
      conf.set("fs.s3a.security.credential.provider.path", nonexistentPath)
      assert(
        DeltaScanSupport
          .proxyGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
          .isEmpty)
    } finally {
      Files.delete(tempDir)
    }
  }

  test(
    "proxyGateReason still declines a plaintext fs.s3a.proxy.host when an S3A-scoped provider " +
      "path is also configured (plain getTrimmed sees the host regardless of any provider)") {
    val tempDir = Files.createTempDirectory("comet-delta-proxy-scoped-provider")
    try {
      val conf = new Configuration(false)
      val nonexistentPath = "jceks://file" + tempDir + "/does-not-exist.jceks"
      conf.set("fs.s3a.security.credential.provider.path", nonexistentPath)
      conf.set("fs.s3a.proxy.host", "proxy.internal.example.com")
      val reason =
        DeltaScanSupport.proxyGateReason(conf, Seq(new URI("s3a://mybucket/part-0.parquet")))
      assert(reason.isDefined)
      assert(reason.get.contains("fs.s3a.proxy.host"))
      assert(reason.get.contains("mybucket"))
    } finally {
      Files.delete(tempDir)
    }
  }

  // ---------------------------------------------------------------------------------------
  // Discovery harness: mechanically bounds the "which fs.s3a.* keys does this comparator need
  // to know about" model, rather than relying on someone noticing the next one by hand (which
  // is exactly how the SSE-C long-bucket-alias gap went unnoticed). A new key cannot even
  // compile into the comparator without a consumer-tier assignment (AllS3ConfigKeys is derived
  // from S3ConfigKeyConsumers), and the tier expectations below pin the assignments themselves.
  // Independent checks:
  //   (a) DeltaScanSupport.AllS3ConfigKeys must be a superset of native's OWN checked-in list
  //       of every fs.s3a.* property it reads (native/core/src/parquet/objectstore/s3.rs's
  //       NATIVE_S3A_CONFIG_PROPERTIES, itself mechanically verified against that file's call
  //       sites by a Rust unit test -- see that constant's doc).
  //   (b) Every fs.s3a.* key Hadoop's own Constants class declares that looks credential- or
  //       encryption-shaped (name contains key/secret/token/password/encryption) must be either
  //       covered by AllS3ConfigKeys or explicitly, individually documented as exempt -- a loud
  //       failure naming the key the moment Hadoop grows a new one nobody has classified yet.
  // ---------------------------------------------------------------------------------------

  test(
    "discovery harness: AllS3ConfigKeys is a superset of native's checked-in " +
      "NATIVE_S3A_CONFIG_PROPERTIES list (native/core/src/parquet/objectstore/s3.rs)") {
    val rustPath =
      DeltaScanContribSuite.findRepoFile("native/core/src/parquet/objectstore/s3.rs")
    rustPath match {
      case None =>
        cancel(
          "Could not locate native/core/src/parquet/objectstore/s3.rs from this checkout; " +
            "skipping the native-key-list superset guard.")
      case Some(file) =>
        val contents = scala.io.Source.fromFile(file, "UTF-8").mkString
        val marker = "NATIVE_S3A_CONFIG_PROPERTIES: &[&str] = &["
        val start = contents.indexOf(marker)
        assert(
          start >= 0,
          s"Expected ${file.getAbsolutePath} to declare NATIVE_S3A_CONFIG_PROPERTIES -- has " +
            "the constant been renamed or removed?")
        val end = contents.indexOf("];", start)
        assert(end > start, "Expected a `];`-terminated array literal after the marker")
        val arrayBody = contents.substring(start + marker.length, end)
        val nativeProperties =
          "\"([^\"]*)\"".r.findAllMatchIn(arrayBody).map(_.group(1)).toSet
        assert(
          nativeProperties.nonEmpty,
          "Parsed zero property names out of NATIVE_S3A_CONFIG_PROPERTIES -- the parser above " +
            "is likely out of sync with the constant's declaration syntax")

        val nativeKeys = nativeProperties.map(p => s"fs.s3a.$p")
        val comparatorKeys = DeltaScanSupport.AllS3ConfigKeys.toSet
        val uncovered = nativeKeys.diff(comparatorKeys)
        assert(
          uncovered.isEmpty,
          "Native reads fs.s3a.* key(s) that DeltaScanSupport.AllS3ConfigKeys does not compare, " +
            s"so a Hadoop-vs-native divergence on any of them would go undetected: " +
            s"${uncovered.toSeq.sorted.mkString(", ")} -- add the missing key(s) to " +
            "AllS3ConfigKeys")
    }
  }

  test(
    "discovery harness: every compared key carries exactly one consumer-tier assignment, and " +
      "the lookupPassword tier is exactly the credential trio (every other compared key's real " +
      "hadoop-aws 3.3.4 consumer is propagateBucketOptions plus a plain Configuration#get " +
      "family call, verified per key in S3ConfigKeyConsumers' doc)") {
    val keys = DeltaScanSupport.S3ConfigKeyConsumers.map(_._1)
    assert(
      keys.distinct == keys,
      "S3ConfigKeyConsumers assigns more than one tier to the same key -- exactly one " +
        "classification per key, declared beside it, is the whole point of the list")
    val passwordTier = DeltaScanSupport.S3ConfigKeyConsumers.collect {
      case (key, DeltaScanSupport.LookupPasswordConsumer) => key
    }
    assert(
      passwordTier == Seq("fs.s3a.access.key", "fs.s3a.secret.key", "fs.s3a.session.token"),
      "The lookupPassword tier changed. A key belongs there ONLY when its real hadoop-aws " +
        "consumer is S3AUtils#lookupPassword/#lookupBucketSecret -- verify against the " +
        "decompiled call site before updating this expectation, because the wrong tier is not " +
        "merely over-cautious: an equality comparator reading wider than the real consumer can " +
        "produce a false EQUALITY that admits a diverging scan")
  }

  test(
    "discovery harness: every credential/encryption-shaped fs.s3a.* key Hadoop's Constants " +
      "class declares is either compared by AllS3ConfigKeys or individually documented as " +
      "exempt") {
    val constantsClassName = "org.apache.hadoop.fs.s3a.Constants"
    val constantsClass =
      try {
        Some(Class.forName(constantsClassName))
      } catch {
        case _: ClassNotFoundException => None
      }
    constantsClass match {
      case None =>
        cancel(
          s"$constantsClassName is not on the test classpath (expected via the " +
            "spark-hadoop-cloud test dependency); skipping the sensitive-key coverage guard.")
      case Some(cls) =>
        val allS3aKeys = cls.getFields
          .filter { f =>
            f.getType == classOf[String] &&
            java.lang.reflect.Modifier.isStatic(f.getModifiers)
          }
          .flatMap { f =>
            f.get(null) match {
              case s: String if s.startsWith("fs.s3a.") => Some(s)
              case _ => None
            }
          }
          .toSet
        assert(
          allS3aKeys.size > 20,
          s"Expected many fs.s3a.* keys via reflection on $constantsClassName, found only " +
            s"${allS3aKeys.size} -- has the class's field layout changed in a way this " +
            "reflection no longer handles?")

        val sensitiveNameFragments =
          Seq("key", "secret", "token", "password", "encryption")
        val sensitiveKeys = allS3aKeys.filter { key =>
          val lower = key.toLowerCase(Locale.ROOT)
          sensitiveNameFragments.exists(lower.contains)
        }

        val comparatorKeys = DeltaScanSupport.AllS3ConfigKeys.toSet
        // Individually justified, one at a time -- NOT a blanket "everything encryption-shaped
        // is exempt" carve-out, which would have hidden the SSE-C long-bucket-alias gap just as easily as
        // never checking at all.
        val documentedExempt: Map[String, String] = Map(
          "fs.s3a.encryption.algorithm" ->
            ("handled by the dedicated unsupportedEncryptionAlgorithmReason/" +
              "effectiveEncryptionAlgorithm allowlist gate, not the generic comparator (needs " +
              "its own canonical/deprecated resolution cascade, not a flat single-key compare)"),
          "fs.s3a.server-side-encryption-algorithm" ->
            "deprecated alias of fs.s3a.encryption.algorithm, same dedicated gate",
          "fs.s3a.encryption.key" ->
            ("key MATERIAL for the algorithm above; never read for comparison at all -- the " +
              "allowlist gate declines on the ALGORITHM alone, so the key's value cannot " +
              "change the outcome, and never appears in a decline reason (see " +
              "effectiveEncryptionAlgorithm's doc)"),
          "fs.s3a.server-side-encryption.key" ->
            "deprecated alias of fs.s3a.encryption.key, same reasoning",
          "fs.s3a.encryption.cse.kms.region" ->
            ("CSE tuning, newer Hadoop only: consulted solely when the algorithm resolves to " +
              "a CSE variant, and the allowlist gate declines every CSE algorithm outright, " +
              "so this value can never influence an admitted scan; native never reads it"),
          "fs.s3a.encryption.cse.custom.keyring.class.name" ->
            "CSE tuning, newer Hadoop only, same reasoning as fs.s3a.encryption.cse.kms.region",
          "fs.s3a.encryption.cse.v1.compatibility.enabled" ->
            "CSE tuning, newer Hadoop only, same reasoning as fs.s3a.encryption.cse.kms.region",
          "fs.s3a.proxy.password" ->
            ("covered via the dedicated fs.s3a.proxy.host gate (proxyGateReason/" +
              "unsupportedProxyReason), not the generic comparator: the password (and the " +
              "sibling fs.s3a.proxy.username, not sensitive-shaped so never reaches this map) " +
              "only matters once a proxy is actually in effect, and any bucket with a " +
              "non-empty effective fs.s3a.proxy.host now declines outright, before any " +
              "credential comparison would even run -- so the deployment shape this key used " +
              "to be a KNOWN GAP for (a Hadoop deployment requiring a proxy for S3 egress " +
              "being silently claimed and connected to directly) can no longer reach this key " +
              "at all; the password's VALUE itself is still never read or forwarded to native, " +
              "same as before"),
          "fs.s3a.failinject.inconsistency.key.substring" ->
            ("hadoop-aws test-only S3 fault-injection knob (InconsistentAmazonS3Client " +
              "family), not a credential; matches the sensitive-name heuristic only " +
              "incidentally via \"key.substring\""))

        val unclassified = sensitiveKeys
          .diff(comparatorKeys)
          .diff(documentedExempt.keySet)
        assert(
          unclassified.isEmpty,
          "Hadoop's Constants class declares credential/encryption-shaped fs.s3a.* key(s) " +
            "this discovery harness has never classified (neither compared by " +
            s"AllS3ConfigKeys nor documented as exempt above): ${unclassified.toSeq.sorted
                .mkString(", ")} -- decide whether the key needs a gate, then either add it to " +
            "AllS3ConfigKeys or add a justified entry to `documentedExempt` in this test")
    }
  }

}

object DeltaScanContribSuite {

  /**
   * Walks up from a candidate root (the `comet.repo.root` system property when set, otherwise
   * `user.dir`) looking for `relativePath`. Handles both a repo-root working directory and a
   * module-root working directory (e.g. `contrib/delta-spark`) without hardcoding either.
   *
   * Package-visible (not `private`) so other suites in this package needing a repo-relative file
   * (e.g. [[JvmLowercaseParitySuite]]) can share it instead of duplicating it.
   */
  private[delta] def findRepoFile(relativePath: String): Option[File] = {
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
