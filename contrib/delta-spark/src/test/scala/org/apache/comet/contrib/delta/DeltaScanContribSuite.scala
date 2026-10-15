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
import java.util.UUID

import scala.jdk.CollectionConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.comet.CometDeltaNativeScanExec
import org.apache.spark.sql.delta.actions.DeletionVectorDescriptor

import org.apache.comet.{CometConf, ExtendedExplainInfo}
import org.apache.comet.objectstore.NativeConfig
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

  test("_metadata.file_block_start and file_block_length fall back on a split Delta file") {
    // When a file is split, Spark keeps a row group in the split holding its midpoint while
    // DataFusion keeps it in the split holding its first page, so these per-split values differ.
    withSQLConf("spark.sql.files.maxPartitionBytes" -> "4096") {
      withTempPath { dir =>
        val path = dir.getAbsolutePath
        spark
          .range(0, 5000)
          .selectExpr("id", "concat('value_', cast(id as string)) as s")
          .coalesce(1)
          .write
          .format("delta")
          .save(path)
        withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
          assert(
            spark.read.format("delta").load(path).rdd.getNumPartitions > 1,
            "the file has to be split across partitions for this test to mean anything")
        }

        val df = spark.read.format("delta").load(path)
        for (column <- Seq("file_block_start", "file_block_length")) {
          checkSparkAnswerAndFallbackReason(
            df.selectExpr("id", "s", s"_metadata.$column"),
            s"Native Delta scan does not support metadata column(s) $column")
        }
        // A filter alone consumes the value too, as does selecting the whole struct.
        checkSparkAnswerAndFallbackReason(
          df.filter("_metadata.file_block_start > 0").select("id"),
          "Native Delta scan does not support metadata column(s) file_block_start")
        checkSparkAnswerAndFallbackReason(
          df.selectExpr("id", "_metadata"),
          "Native Delta scan does not support metadata column(s) " +
            "file_block_start, file_block_length")
        // The per-file constants do not depend on which split reads a row group.
        checkDeltaNativeScanAnswer(df.selectExpr("id", "s", "_metadata.file_path"))
      }
    }
  }

  test("_metadata.file_block_start falls back on a split deletion-vector Delta file") {
    // A deletion-vector read puts every _metadata field in the scan output; the plain read stays
    // native (see the claim test above) while a query consuming file_block_start falls back.
    withSQLConf("spark.sql.files.maxPartitionBytes" -> "4096") {
      withTempPath { dir =>
        val path = dir.getAbsolutePath
        spark
          .range(0, 5000)
          .selectExpr("id", "concat('value_', cast(id as string)) as s")
          .coalesce(1)
          .write
          .format("delta")
          .save(path)
        spark.sql(
          s"ALTER TABLE delta.`$path` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
        spark.sql(s"DELETE FROM delta.`$path` WHERE id % 7 = 0")

        val df = spark.read.format("delta").load(path)
        checkDeltaNativeScanAnswer(df)
        val expected = "Native Delta scan does not support metadata column(s) file_block_start"
        val (_, cometPlan) = checkSparkAnswerAndFallbackReason(
          df.selectExpr("id", "s", "_metadata.file_block_start"),
          expected)
        // file_block_length is in the scan output too, but nothing above the scan reads it.
        val reasons = new ExtendedExplainInfo().getFallbackReasons(cometPlan)
        assert(
          reasons.exists(r => r.contains(expected) && !r.contains("file_block_length")),
          s"expected '$expected' naming only file_block_start among: $reasons")
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

  test(
    "objectStoreRejectedPathReason declines a root whose path object_store rejects and " +
      "stays None for an ordinary path or a libhdfs-exempt scheme") {
    val rejected = new URI("file:///tmp/dir%0A/data")
    // Precondition, mirroring the scheme tests above: guards against a fail-open native build
    // vacuously passing this test.
    assert(!CometScanRule.objectStoreAcceptsPath(rejected))
    assert(CometScanRule.objectStoreAcceptsPath(new URI("file:///tmp/table")))

    val reason = DeltaScanSupport.objectStoreRejectedPathReason(Seq(rejected), Set("hdfs"))
    assert(reason.isDefined)
    assert(reason.get.contains("cannot open path 'file:///tmp/dir%0A/data'"))
    assert(reason.get.contains("object_store rejects it"))

    assert(
      DeltaScanSupport
        .objectStoreRejectedPathReason(Seq(new URI("file:///tmp/table")), Set("hdfs"))
        .isEmpty)
    // A libhdfs-routed scheme never reaches object_store's path parser.
    assert(
      DeltaScanSupport
        .objectStoreRejectedPathReason(Seq(new URI("hdfs://nn/dir%0A/data")), Set("hdfs"))
        .isEmpty)
    // Userinfo is masked in the reason (redactedAuthority's invariant); the path is kept.
    val withUserInfo = DeltaScanSupport.objectStoreRejectedPathReason(
      Seq(new URI("s3a://key:secret@bucket/dir%0A/data")),
      Set("hdfs"))
    assert(withUserInfo.exists(_.contains("'s3a://***@bucket/dir%0A/data'")), s"$withUserInfo")
    assert(!withUserInfo.exists(_.contains("secret")))
  }

  test(
    "objectStoreRejectedPathReason declines a selected file whose basename object_store " +
      "rejects even though its parent directory is accepted") {
    // CONVERT TO DELTA keeps the source Parquet basenames, so the rejected character can sit in
    // the file name itself; a directory-only probe accepts the parent and misses it. Both URIs
    // come from Hadoop's Path, as the selected files do, so they render as `file:/...`.
    val file = new Path(new Path("file:/tmp/table"), "part-00000\n.snappy.parquet").toUri
    val parent = new Path(file).getParent.toUri
    assert(file == new URI("file:/tmp/table/part-00000%0A.snappy.parquet"), s"$file")
    assert(CometScanRule.objectStoreAcceptsPath(parent), s"directory probe rejected $parent")
    assert(!CometScanRule.objectStoreAcceptsPath(file))
    assert(DeltaScanSupport.objectStoreRejectedPathReason(Seq(parent), Set("hdfs")).isEmpty)

    val reason = DeltaScanSupport.objectStoreRejectedPathReason(Seq(file), Set("hdfs"))
    assert(
      reason.exists(
        _.contains("cannot open path 'file:/tmp/table/part-00000%0A.snappy.parquet': " +
          "object_store rejects it")),
      s"reason: $reason")
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

  test("multiStoreReason declines data files on more than one ABFS store or beside s3a") {
    // Core's extractScanObjectStoreOptions needs one ABFS container and account per scan and no
    // other scheme beside ABFS. This gate declines all three shapes before convert reaches it.
    val shapes = Seq(
      "two accounts" -> Seq(
        new URI("abfss://data@accountA.dfs.core.windows.net/t/part-0.parquet"),
        new URI("abfss://data@accountB.dfs.core.windows.net/t/part-1.parquet")),
      "two containers" -> Seq(
        new URI("abfss://dataA@account.dfs.core.windows.net/t/part-0.parquet"),
        new URI("abfss://dataB@account.dfs.core.windows.net/t/part-1.parquet")),
      "abfss and s3a" -> Seq(
        new URI("abfss://data@account.dfs.core.windows.net/t/part-0.parquet"),
        new URI("s3a://bucket/t/part-1.parquet")))
    shapes.foreach { case (shape, uris) =>
      val reason = DeltaScanSupport.multiStoreReason(uris)
      assert(reason.exists(_.contains("multiple object stores")), s"$shape: $reason")
    }
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
    // ABFS options are what Hadoop resolves for the account, not the fs.azure.* namespace.
    val abfsOptions = NativeConfig.extractObjectStoreOptions(hadoopConf, abfssUri)
    assert(abfsOptions.nonEmpty)
    abfsOptions.foreach { case (key, value) => assert(merged.get(key).contains(value), key) }

    // s3-only input must not leak the azure options into the merged map.
    val s3Only = CometDeltaNativeScan.mergedObjectStoreOptions(hadoopConf, Seq(s3Uri))
    assert(s3Only.get("fs.s3a.access.key").contains("s3-access-key"))
    assert(
      !s3Only.keys.exists(key => key.startsWith("fs.azure.") || key.startsWith("comet.azure.")))
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

  test("a scan that selects no files keeps the table root's object-store options") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark
        .range(0, 10)
        .selectExpr("id", "id % 2 AS p")
        .write
        .format("delta")
        .partitionBy("p")
        .save(path)
      // The scheme list rides along in every extracted map, so a local root's options are not
      // empty.
      withSQLConf(CometConf.COMET_S3_COMPLIANT_SCHEMES_KEY -> "minio") {
        val df = spark.read.format("delta").load(path).where("p = 5")
        assert(df.collect().isEmpty)
        val scans = deltaNativeScans(df).collect { case s: CometDeltaNativeScanExec => s }
        assert(scans.size == 1, s"expected one native Delta scan:\n${df.queryExecution}")
        val options =
          DeltaSparkScanEnvelope.unpack(scans.head.nativeOp).getCommon.getObjectStoreOptionsMap
        assert(options.asScala.toMap == Map(CometConf.COMET_S3_COMPLIANT_SCHEMES_KEY -> "minio"))
      }
    }
  }

  test("user guide documents every native Delta scan config verbatim, with its default") {
    // The config table on the user-guide page is hand-maintained: the doc build cannot see
    // DeltaSparkConfigProvider with the current module layout, so this is the only check tying
    // each entry's key, doc string, and default to the row GenerateDocs would render.
    val file = DeltaScanContribSuite
      .findRepoFile("docs/source/user-guide/latest/delta.md")
      .getOrElse(
        fail("Could not locate docs/source/user-guide/latest/delta.md from this checkout; " +
          "set -Dcomet.repo.root or run from the repo or module root"))
    val source = scala.io.Source.fromFile(file, "UTF-8")
    val tableRows =
      try source.getLines().filter(_.startsWith("| `")).toList
      finally source.close()
    // Renders the row GenerateDocs emits for an entry with a plain default and no env var, which
    // is every entry today; an entry using either needs the extra text added here as well.
    val expectedRows = DeltaScanConf.all.map { conf =>
      s"| `${conf.key}` | ${conf.doc.trim} | ${conf.defaultValueString} |"
    }
    expectedRows.foreach { row =>
      assert(
        tableRows.contains(row),
        s"Expected ${file.getAbsolutePath} to contain this table row verbatim:\n$row\n" +
          s"Rows present:\n${tableRows.mkString("\n")}")
    }
    val staleRows = tableRows.filterNot(expectedRows.contains)
    assert(
      staleRows.isEmpty,
      s"${file.getAbsolutePath} has table rows matching no entry in DeltaScanConf.all:\n" +
        staleRows.mkString("\n"))
  }

  test("CometDeltaS3Suite.s3Required arms the hard failure only for 1 or true") {
    // Trimmed and case-insensitive so a padded or upper-cased workflow value still counts;
    // anything else, including yes and 0, keeps the fail-soft default so a typo cannot arm it.
    assert(!CometDeltaS3Suite.s3Required(None))
    Seq("1", "true", "TRUE ", " True").foreach { v =>
      assert(CometDeltaS3Suite.s3Required(Some(v)), s"'$v' should arm the hard failure")
    }
    Seq("", " ", "0", "false", "yes", "on", "required", "11").foreach { v =>
      assert(!CometDeltaS3Suite.s3Required(Some(v)), s"'$v' must not arm the hard failure")
    }
  }

  test("CometDeltaS3Suite.requiredFailureMessage names the env var and the cause") {
    val message = CometDeltaS3Suite.requiredFailureMessage("no Docker daemon is reachable")
    assert(message.contains(CometDeltaS3Suite.S3_REQUIRED_ENV))
    assert(message.contains("no Docker daemon is reachable"))
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
