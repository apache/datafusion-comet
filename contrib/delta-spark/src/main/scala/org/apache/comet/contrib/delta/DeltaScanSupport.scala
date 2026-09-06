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

import java.io.IOException
import java.net.URI
import java.util.Locale

import scala.collection.mutable.{ListBuffer, Map => MutableMap}
import scala.jdk.CollectionConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.expressions.{Alias, GenericInternalRow, InputFileBlockLength, InputFileBlockStart, InputFileName}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns.getExistenceDefaultValues
import org.apache.spark.sql.comet.CometScanExec
import org.apache.spark.sql.delta.DeltaParquetFileFormat
import org.apache.spark.sql.delta.actions.DeletionVectorDescriptor
import org.apache.spark.sql.execution.{FileSourceScanExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}

import org.apache.comet.CometConf
import org.apache.comet.CometConf.COMET_LIBHDFS_SCHEMES
import org.apache.comet.objectstore.NativeConfig
import org.apache.comet.parquet.CometParquetUtils
import org.apache.comet.rules.{CometScanRule, CometScanTypeChecker}
import org.apache.comet.serde.operator.CometNativeScan
import org.apache.comet.shims.ShimFileFormat

/**
 * Claim/decline gates for the native Delta scan. Correctness rule: when in doubt, decline,
 * Spark's Delta reader handles the scan and results stay correct, just unaccelerated.
 */
object DeltaScanSupport {

  /**
   * Reader features the native path understands; anything else on the protocol declines the
   * table. `deletionVectors`/`columnMapping` are declined separately below for specific reasons.
   */
  private val understoodReaderFeatures: Set[String] =
    Set("columnMapping", "deletionVectors", "timestampNtz", "v2Checkpoint", "vacuumProtocolCheck")

  /**
   * Is this exactly Delta's DSv1 parquet format? Compared by class name, not `classOf`: a
   * `classOf` reference would raise `NoClassDefFoundError` and break every parquet scan when
   * delta-spark is absent from the classpath.
   */
  def isDeltaScan(scanExec: FileSourceScanExec): Boolean =
    scanExec.relation.fileFormat.getClass.getName ==
      "org.apache.spark.sql.delta.DeltaParquetFileFormat"

  /**
   * Claim-time artifacts [[declineReason]] already computes but [[CometDeltaNativeScan.convert]]
   * also needs -- threaded through by reference (populated only on the claimable path, right
   * before `declineReason` returns `None`) so a claimed scan does not pay to recompute either:
   * the Hadoop conf ([[org.apache.spark.sql.internal.SessionState#newHadoopConfWithOptions]] is
   * not cheap) and the deletion-vector descriptors (base64-decoded, non-trivial only for DV-shape
   * scans). One instance is created per claim attempt in `DeltaScanContrib` and passed to both
   * `declineReason` and `convert`.
   */
  private[delta] final class DeltaClaimMemo {
    var hadoopConf: Configuration = _
    var dvDescriptors: Seq[DeletionVectorDescriptor] = Seq.empty
  }

  /**
   * First reason this Delta scan cannot go native, or None when claimable (in which case `memo`
   * is populated for [[CometDeltaNativeScan.convert]] to reuse). Only called when [[isDeltaScan]]
   * is true. `scanHelper` is the [[CometScanExec]] built to drive `convert` on a claim, reused
   * for the multi-store gate below.
   */
  def declineReason(
      plan: SparkPlan,
      scanExec: FileSourceScanExec,
      scanHelper: CometScanExec,
      memo: DeltaClaimMemo): Option[String] = {
    val format = scanExec.relation.fileFormat.asInstanceOf[DeltaParquetFileFormat]
    val protocol = format.protocol
    val metadata = format.metadata
    // Name mode is supported via physical-name schemas; id mode needs the field-id path and
    // stays declined until validated. Hoisted here since several gates below reuse it.
    val cmMode = metadata.columnMappingMode.name
    // Descriptor deserialization is expensive, so hoist it into a `lazy val`, forced at most
    // once in this method; on the claimable path the result is handed to `convert` through
    // `memo` below, so a claimed scan deserializes the descriptors exactly once end to end.
    val tableRoot = scanExec.relation.location.rootPaths.head.toString
    lazy val dvDescriptors: Seq[DeletionVectorDescriptor] =
      selectedDvDescriptors(scanHelper, tableRoot)

    // Mirrors core's CometScanRule.isSchemaSupported so scan-time type gates (unsigned-small-int
    // fallback, collation, shredded-variant-struct) apply identically here. Pure in-memory check,
    // so it runs first, ahead of every I/O-bearing gate below.
    val schemaFallbackReasons = new ListBuffer[String]()
    val typeChecker = CometScanTypeChecker()
    val requiredSchemaSupported =
      typeChecker.isSchemaSupported(scanExec.requiredSchema, schemaFallbackReasons)
    val partitionSchemaSupported =
      typeChecker.isSchemaSupported(scanExec.relation.partitionSchema, schemaFallbackReasons)
    if (!requiredSchemaSupported || !partitionSchemaSupported) {
      return Some(
        "Native Delta scan does not support the schema: " + schemaFallbackReasons.mkString(", "))
    }

    if (format.isCDCRead) {
      return Some("Native Delta scan does not support Change Data Feed reads")
    }

    // Delta's DML machinery (findTouchedFiles) disables reader optimizations and needs real
    // row indexes from Spark's reader; claiming here would feed NULL indexes into DV construction.
    if (!format.optimizationsEnabled) {
      return Some("Native Delta scan does not support reads with reader optimizations disabled")
    }
    if (scanExec.requiredSchema.exists(_.name == DeltaParquetFileFormat.ROW_INDEX_COLUMN_NAME) ||
      scanExec.relation.dataSchema.exists(
        _.name == DeltaParquetFileFormat.ROW_INDEX_COLUMN_NAME)) {
      return Some("Native Delta scan does not support Delta's generated row-index column")
    }

    if (cmMode != "none" && cmMode != "name") {
      return Some(s"Native Delta scan does not support column mapping mode $cmMode")
    }
    // createPhysicalSchema wholesale-replaces field metadata, silently dropping EXISTS_DEFAULT.
    if (cmMode == "name" &&
      getExistenceDefaultValues(scanExec.requiredSchema).exists(_ != null)) {
      return Some(
        "Native Delta scan does not support column defaults together with column mapping")
    }
    // createPhysicalSchema rewrites nested StructField names too, and the native builder emits the
    // required schema verbatim as output, so name-sensitive expressions (e.g. to_json) would leak
    // physical names. Decline until a rename adapter exists.
    if (cmMode == "name" &&
      scanExec.requiredSchema.exists(f => containsNestedStruct(f.dataType))) {
      return Some("Native Delta scan does not support column mapping with nested struct fields")
    }

    val readerFeatures = protocol.readerFeatureNames
    val unknownFeatures = readerFeatures -- understoodReaderFeatures
    if (unknownFeatures.nonEmpty) {
      return Some(
        s"Native Delta scan does not support reader feature(s) ${unknownFeatures.mkString(", ")}")
    }

    // Non-constant metadata columns are generated per-row by Spark's reader and unsupported,
    // except Delta's DV bookkeeping columns, which the native path emits as constants.
    val knownColNames =
      scanExec.relation.dataSchema.map(_.name).toSet ++
        scanExec.relation.partitionSchema.map(_.name).toSet ++
        scanExec.fileConstantMetadataColumns.map(_.name).toSet ++
        CometDeltaNativeScan.internalColumnNames
    val unknownOutput = scanExec.output.map(_.name).filterNot(knownColNames.contains)
    if (unknownOutput.nonEmpty) {
      return Some(
        s"Native Delta scan does not support generated column(s) ${unknownOutput.mkString(", ")}")
    }

    // Deletion-vector shape invariants (see CometDeltaNativeScan.buildDvScanCommon).
    if (CometDeltaNativeScan.isDvShape(scanExec)) {
      // A row-index column WITHOUT is_row_deleted is Delta DML bookkeeping (real row indexes),
      // not a DV read; claiming it with a constant would corrupt the DVs being written.
      val hasIsRowDeleted =
        scanExec.requiredSchema.exists(_.name == CometDeltaNativeScan.IsRowDeletedColumn)
      val hasRowIndex =
        scanExec.requiredSchema.exists(_.name == CometDeltaNativeScan.RowIndexColumn)
      if (hasRowIndex && !hasIsRowDeleted) {
        return Some(
          "Native Delta scan does not support row-index reads outside a deletion-vector scan")
      }
      // Internal columns must form a suffix of the read schema so data-column positions agree
      // between Spark's output and the stripped native schema.
      val names = scanExec.requiredSchema.fields.map(_.name)
      val firstInternal = names.indexWhere(CometDeltaNativeScan.internalColumnNames.contains)
      if (!names.drop(firstInternal).forall(CometDeltaNativeScan.internalColumnNames.contains)) {
        return Some("Native Delta scan requires DV bookkeeping columns to trail the read schema")
      }
      // Native applies the DV itself and emits a dead constant for row-index, so the real value
      // must be provably unused above the scan.
      if (!rowIndexUnusedAbove(plan, scanExec)) {
        return Some(
          "Native Delta scan cannot supply _metadata.row_index values consumed by the query")
      }
      // The DV common builder does not serialize existence defaults yet.
      if (getExistenceDefaultValues(scanExec.requiredSchema).exists(_ != null)) {
        return Some(
          "Native Delta scan does not support column defaults together with deletion vectors")
      }
      // Bounds native's memory for expanded DV row selectors (delta_dv.rs), pessimistically
      // bounded by 2*cardinality + #row-groups; the conf below makes an over-pessimistic decline
      // recoverable.
      val maxDeletedRowsPerFile = DeltaScanConf.COMET_DELTA_MAX_DELETED_ROWS_PER_FILE.get()
      val oversizedCardinalities = dvDescriptors
        .map(_.cardinality)
        .filter(_ > maxDeletedRowsPerFile)
      if (oversizedCardinalities.nonEmpty) {
        return Some(
          "Native Delta scan does not support a deletion vector deleting " +
            s"${oversizedCardinalities.max} rows in a single file, exceeding " +
            s"${DeltaScanConf.COMET_DELTA_MAX_DELETED_ROWS_PER_FILE.key}=$maxDeletedRowsPerFile")
      }
    }

    // input_file_name & friends read from a thread-local Spark's FileScanRDD sets; the native scan
    // does not populate it, and Delta's DML find-touched-files scans use it (mirrors core's check
    // in CometScanRule.nativeScan).
    if (plan.exists(node =>
        node.expressions.exists(_.exists {
          case _: InputFileName | _: InputFileBlockStart | _: InputFileBlockLength => true
          case _ => false
        }))) {
      return Some(
        "Native Delta scan is not compatible with input_file_name, " +
          "input_file_block_start, or input_file_block_length")
    }

    // Row-index metadata columns are generated per-row by Spark's reader (mirrors core); the DV
    // shape's trailing row-index column is exempt since the gates above already proved it dead.
    if (!CometDeltaNativeScan.isDvShape(scanExec) &&
      ShimFileFormat.findRowIndexColumnIndexInSchema(scanExec.requiredSchema) >= 0) {
      return Some("Native Delta scan does not support row index generation")
    }

    // Mirror core's vectorized-reader compatibility gate.
    if (!SQLConf.get.getConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED) &&
      !CometConf.COMET_SCAN_ALLOW_DISABLED_PARQUET_VECTORIZED_READER.get()) {
      return Some(
        "Native Delta scan is incompatible with " +
          s"${SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key}=false")
    }

    // Decline ALL encrypted-parquet configurations (stricter than core): the exec node does not
    // yet wire the decryption-key broadcast to executors.
    val hadoopConf = scanExec.relation.sparkSession.sessionState
      .newHadoopConfWithOptions(scanExec.relation.options)
    // Populated now (rather than only at the very end) so it is available even though several
    // early-return gates below still lie ahead: cheap to set, and every one of those gates
    // declines the scan anyway, so `memo` is simply never read by `convert` in that case.
    memo.hadoopConf = hadoopConf
    if (CometParquetUtils.encryptionEnabled(hadoopConf)) {
      return Some("Native Delta scan does not support encrypted parquet")
    }

    // Nested-type column defaults cannot be serialized; a dropped default would misalign the
    // value/index lists consumed positionally on the native side. Mirrors core's
    // transformV1Scan gate.
    val possibleDefaultValues = getExistenceDefaultValues(scanExec.requiredSchema)
    if (possibleDefaultValues.exists(d =>
        d != null && (d.isInstanceOf[ArrayBasedMapData] || d
          .isInstanceOf[GenericInternalRow] || d.isInstanceOf[GenericArrayData]))) {
      return Some("Native Delta scan does not support default values for nested types")
    }

    // An opted-in S3-compliant alias scheme (fs.comet.s3Compliant.schemes) is declined before the
    // generic scheme gate below so the reason says why: core's native scan reads it through the
    // S3 client, but the S3 divergence gates further down model Hadoop's S3AFileSystem only.
    val rootUris = scanExec.relation.location.rootPaths.map(_.toUri)
    val aliasReason = s3CompliantAliasSchemeReason(hadoopConf, rootUris)
    if (aliasReason.isDefined) {
      return aliasReason
    }

    // Only claim scans whose root paths object_store (or the configured libhdfs schemes) can
    // actually read (mirrors core's unsupportedFsSchemes gate).
    val libhdfs = libhdfsSchemes
    val unsupportedRootSchemes = unsupportedSchemes(rootUris, libhdfs)
    if (unsupportedRootSchemes.nonEmpty) {
      return Some(
        "Native Delta scan does not support filesystem scheme(s) " +
          s"${unsupportedRootSchemes.mkString(", ")}")
    }

    // A recognized scheme can still carry a path object_store rejects (a directory name with a
    // newline surfaces as `%0A`), which native planning hard-fails on while Spark's reader opens
    // it. Mirrors core's root-path gate; the selected files' directories are probed below.
    val rejectedRoot = objectStoreRejectedPathReason(rootUris, libhdfs)
    if (rejectedRoot.isDefined) {
      return rejectedRoot
    }

    // A shallow clone can span multiple object-store authorities, but the native builder resolves
    // ObjectStoreUrl from only the FIRST selected file; force file listing and decline rather than
    // risk reading a later file through the wrong handle.
    val dataFileUris =
      scanHelper.selectedPartitions.iterator.flatMap(_.files).map(_.getPath.toUri).toSeq

    // Both gates below need the DV absolute-path URIs; dvDescriptors is already memoized.
    val dvUris = dvDescriptors
      .filter(_.storageType != DeletionVectorDescriptor.INLINE_DV_MARKER)
      .map(_.absolutePath(new Path(tableRoot)).toUri)

    // The root-path gate above only inspects the table root(s); selected files can resolve
    // through a different scheme (e.g. `viewfs:`). Checked before the authority gates below,
    // which presume every URI is natively resolvable.
    val unsupportedSelected = unsupportedSelectedSchemeReason(dataFileUris ++ dvUris, libhdfs)
    if (unsupportedSelected.isDefined) {
      return unsupportedSelected
    }

    // Same path probe for the directories the selected files live in (a shallow clone's source
    // can sit outside this root), once per distinct directory rather than once per file.
    val rejectedSelected =
      objectStoreRejectedPathReason(parentDirectories(dataFileUris ++ dvUris), libhdfs)
    if (rejectedSelected.isDefined) {
      return rejectedSelected
    }

    // Checked before multiStoreReason, which presumes every URI resolves to a single store
    // identity -- a userinfo-bearing authority provably does not (store keying drops userinfo).
    val userInfoReason = userInfoBearingAuthorityReason(dataFileUris ++ dvUris)
    if (userInfoReason.isDefined) {
      return userInfoReason
    }

    val multiStore = multiStoreReason(dataFileUris)
    if (multiStore.isDefined) {
      return multiStore
    }

    // GCS's zero-I/O, conf-only credential-forwarding gate; ordered alongside the S3 credential
    // gates below since all presume a single, well-formed store identity per URI.
    val gcsAuthReason = gcsHadoopOnlyAuthReason(hadoopConf, dataFileUris ++ dvUris)
    if (gcsAuthReason.isDefined) {
      return gcsAuthReason
    }

    // Zero-I/O, conf-only, like the GCS gate above: decline any bucket configured for an
    // encryption algorithm outside the allowlist (SSE-C, CSE-KMS, CSE-CUSTOM, or unknown) before
    // the credential-divergence gates below, which do not otherwise notice this table is readable
    // through Hadoop only because Hadoop's request factory (SSE-C) or SDK-level decryption layer
    // (CSE-*) does something native never learns about.
    val encryptionReason =
      unsupportedEncryptionAlgorithmReason(hadoopConf, dataFileUris ++ dvUris)
    if (encryptionReason.isDefined) {
      return encryptionReason
    }

    // Shared across the two gates below: propagateBucketOptions is a full Configuration deep
    // copy, and both gates would otherwise recompute it independently for the same bucket(s)
    // (once here, then again per-key inside s3ConfigDivergenceReason). One cache, populated
    // lazily per bucket on first use, makes it a single copy total per bucket across both gates.
    val propagatedConfCache = MutableMap.empty[String, Configuration]

    // Always zero-I/O (plain propagated-conf read, no keystore): native's S3 client has no
    // HTTP proxy support at all (no fs.s3a.proxy.* key is read anywhere in s3.rs), so a bucket
    // requiring a proxy for S3 egress must decline here rather than claim and then connect
    // directly, bypassing whatever network-segmentation/firewall policy required the proxy.
    val proxyReason = proxyGateReason(hadoopConf, dataFileUris ++ dvUris, propagatedConfCache)
    if (proxyReason.isDefined) {
      return proxyReason
    }

    // Zero-I/O, conf-only, like the proxy gate above: Hadoop's AssumedRoleCredentialProvider
    // sends fs.s3a.assumed.role.policy as the session policy of its STS AssumeRole request,
    // while native's assumed-role provider never reads the key -- a claimed scan would assume
    // the role WITHOUT the configured session restriction, silently widening permissions.
    val rolePolicyReason =
      assumedRolePolicyGateReason(hadoopConf, dataFileUris ++ dvUris, propagatedConfCache)
    if (rolePolicyReason.isDefined) {
      return rolePolicyReason
    }

    // Every fs.s3a.* option native's get_config (s3.rs) resolves must agree between what Hadoop
    // itself would use and what native would read from the forwarded, substituted conf (covers
    // long-form bucket credentials, JCEKS/credential-provider shadowing, and any other
    // short-vs-effective divergence in one mechanism); reuses hadoopConf from the encryption gate
    // above.
    val s3Reason =
      s3ConfigDivergenceReason(hadoopConf, dataFileUris ++ dvUris, propagatedConfCache)
    if (s3Reason.isDefined) {
      return s3Reason
    }

    // A credential-provider class native's build_aws_credential_provider_metadata (s3.rs) does
    // not recognize errors at scan EXECUTION time, after the scan was already claimed; decline
    // eagerly instead.
    val providerReason = providerClassGateReason(hadoopConf, dataFileUris ++ dvUris)
    if (providerReason.isDefined) {
      return providerReason
    }

    // Reuse core's generic native-scan gates (ignoreCorruptFiles/ignoreMissingFiles, AQE DPP on
    // Spark 3.4, exec enabled); tags its own fallback reasons.
    if (!CometNativeScan.isSupported(scanExec)) {
      return Some("Core native scan gates rejected the scan (see reasons above)")
    }

    // Claimable: hand the already-forced descriptors to `convert` via `memo` so it does not
    // deserialize them a second time.
    memo.dvDescriptors = dvDescriptors
    None
  }

  /**
   * Deletion-vector descriptors for every file this DV-shape scan selected, normalized to
   * absolute on-disk paths. Returns `Seq.empty` for the plain shape. Shared by the DV cardinality
   * gate and [[CometDeltaNativeScan.convert]]'s object-store option merge.
   */
  private[delta] def selectedDvDescriptors(
      scanHelper: CometScanExec,
      tableRoot: String): Seq[DeletionVectorDescriptor] = {
    if (!CometDeltaNativeScan.isDvShape(scanHelper.wrapped)) {
      return Seq.empty
    }
    val tableRootPath = new Path(tableRoot)
    scanHelper.selectedPartitions.iterator
      .flatMap(_.files)
      .flatMap { file =>
        file.metadata
          .get(DeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_ID_ENCODED)
          .map(enc => DeletionVectorDescriptor.deserializeFromBase64(enc.asInstanceOf[String]))
      }
      .map(_.copyWithAbsolutePath(tableRootPath))
      .toSeq
  }

  /**
   * The libhdfs scheme exemption set from [[org.apache.comet.CometConf.COMET_LIBHDFS_SCHEMES]],
   * parsed exactly like core's scan gate (`NativeConfig.parseSchemeSet`: split on commas,
   * trimmed, lowercased) and defaulting to `Set("hdfs")` when unset.
   */
  private[delta] def libhdfsSchemes: Set[String] = COMET_LIBHDFS_SCHEMES.get() match {
    case Some(s) => NativeConfig.parseSchemeSet(s)
    case None => Set("hdfs")
  }

  /**
   * Decline reason when any of `uris` uses a scheme opted in as an S3-compliant alias through
   * `fs.comet.s3Compliant.schemes` (e.g. `blob`), or `None`. Core's native Parquet scan admits
   * such a scheme and reads it through its S3 client, with `NativeConfig` translating the vendor
   * `fs.<scheme>.<authority>.*` keys into `fs.s3a.bucket.*` options. Spark, however, reads the
   * same table through the vendor's own Hadoop FileSystem, not `S3AFileSystem`, and every S3
   * divergence gate in this object ([[s3ConfigDivergenceReason]] and its siblings) is verified
   * against `S3AFileSystem`'s consumers only. With no model of how the vendor filesystem resolves
   * its configuration, whether native and Spark would agree cannot be decided, so the scan is
   * declined rather than claimed on a guess. Selected data-file and deletion-vector URIs under an
   * alias scheme are declined by the generic scheme gates, which never admit an alias (see
   * [[unsupportedSchemes]]).
   */
  private[delta] def s3CompliantAliasSchemeReason(
      hadoopConf: Configuration,
      uris: Seq[URI]): Option[String] = {
    val aliases = NativeConfig.resolveS3CompliantSchemes(hadoopConf)
    if (aliases.isEmpty) {
      return None
    }
    val found = uris
      .flatMap(uri => Option(uri.getScheme).map(_.toLowerCase(Locale.ROOT)))
      .filter(aliases.contains)
      .distinct
    if (found.isEmpty) {
      None
    } else {
      Some(
        "Native Delta scan does not support S3-compliant alias filesystem scheme(s) " +
          s"${found.sorted.mkString(", ")} (${CometConf.COMET_S3_COMPLIANT_SCHEMES_KEY}): " +
          "Spark reads them through a vendor filesystem whose S3 configuration resolution the " +
          "native scan's S3AFileSystem divergence model cannot verify")
    }
  }

  /**
   * The lowercased, deduplicated schemes among `uris` that neither `libhdfs` nor Comet's native
   * object_store layer ([[CometScanRule.isNativelyReadableScheme]]) can read. A `null` scheme is
   * tolerated, not flagged, since such a URI cannot come from a Hadoop-backed source. The alias
   * set handed to core's gate is deliberately empty: an `fs.comet.s3Compliant.schemes` alias is
   * never admitted here (see [[s3CompliantAliasSchemeReason]]), even though core admits it.
   */
  private[delta] def unsupportedSchemes(uris: Seq[URI], libhdfs: Set[String]): Set[String] = {
    uris
      .filter { uri =>
        val sch = uri.getScheme
        sch != null && {
          val sl = sch.toLowerCase(Locale.ROOT)
          !libhdfs.contains(sl) && !CometScanRule.isNativelyReadableScheme(uri, Set.empty)
        }
      }
      .map(_.getScheme.toLowerCase(Locale.ROOT))
      .toSet
  }

  /**
   * Decline reason naming the first of `uris` whose path object_store rejects even though it
   * recognizes the scheme ([[CometScanRule.objectStoreAcceptsPath]], e.g. a directory name
   * containing a newline, `%0A` in the URI), or `None`. Schemes in `libhdfs` never reach
   * object_store's path parser and are skipped, as is a `null` scheme (see
   * [[unsupportedSchemes]]); an S3-compliant alias is declined before this gate runs. The probe
   * is uncached, so callers pass root paths and [[parentDirectories]], never every file. The
   * reason masks any userinfo in the named URI ([[redactedAuthority]]).
   */
  private[delta] def objectStoreRejectedPathReason(
      uris: Seq[URI],
      libhdfs: Set[String]): Option[String] = {
    uris.distinct
      .find { uri =>
        val sch = uri.getScheme
        sch != null && !libhdfs.contains(sch.toLowerCase(Locale.ROOT)) &&
        !CometScanRule.objectStoreAcceptsPath(uri)
      }
      .map { uri =>
        // Mask userinfo (see redactedAuthority); the raw path keeps its percent encoding so the
        // reason shows the rejected sequence as written.
        val shown =
          if (uriUserInfo(uri).isEmpty) uri.toString
          else s"${redactedAuthority(uri)}${Option(uri.getRawPath).getOrElse("")}"
        s"Native Delta scan cannot open path '$shown': object_store rejects it " +
          "(e.g. an unsupported character in the path)"
      }
  }

  /**
   * The distinct parent directories of `uris`, in first-seen order, for
   * [[objectStoreRejectedPathReason]]: Delta names its data and deletion-vector files itself
   * (UUIDs), so a path object_store rejects can only come from a directory segment. A URI with no
   * parent (a root) is dropped.
   */
  private[delta] def parentDirectories(uris: Seq[URI]): Seq[URI] =
    uris.iterator.flatMap(uri => Option(new Path(uri).getParent).map(_.toUri)).toSeq.distinct

  /**
   * Decline reason when any of `uris` -- the scan's selected data-file and deletion-vector URIs
   * -- use a scheme [[unsupportedSchemes]] flags, or `None` when every URI is natively readable
   * (or libhdfs-exempt).
   */
  private[delta] def unsupportedSelectedSchemeReason(
      uris: Seq[URI],
      libhdfs: Set[String]): Option[String] = {
    val schemes = unsupportedSchemes(uris, libhdfs)
    if (schemes.isEmpty) {
      None
    } else {
      Some(
        "Native Delta scan does not support selected data file or deletion vector filesystem " +
          s"scheme(s) ${schemes.mkString(", ")}")
    }
  }

  /**
   * Decline reason when `uris` span more than one object-store authority (scheme + lowercased raw
   * authority, so e.g. `S3A://Bucket` and `s3a://bucket` collapse), or `None` when they share
   * one. `file://` paths carry no authority, so local scans across many directories are
   * unaffected.
   */
  private[delta] def multiStoreReason(uris: Seq[URI]): Option[String] = {
    val authorities = uris.map(uriAuthority).distinct
    if (authorities.size > 1) {
      Some(
        "Native Delta scan does not support data files spanning multiple object stores " +
          s"(found: ${authorities.sorted.mkString(", ")})")
    } else {
      None
    }
  }

  /**
   * Normalizes `uri` to a lowercased `scheme://authority` string, keyed on the raw `getAuthority`
   * rather than the parsed host/port/userinfo fields: `getHost` (and `getUserInfo`/`getPort`)
   * return `null` for the whole authority when it fails RFC 3986 `reg-name` syntax (e.g. an
   * underscore in a GCS bucket name, `gs://my_bucket`), which would silently collapse distinct
   * buckets into one empty-host key. A `null` authority normalizes to the empty string.
   */
  private[delta] def uriAuthority(uri: URI): String = {
    val scheme = Option(uri.getScheme).map(_.toLowerCase(Locale.ROOT)).getOrElse("")
    val authority = Option(uri.getAuthority).map(_.toLowerCase(Locale.ROOT)).getOrElse("")
    s"$scheme://$authority"
  }

  /**
   * The raw userinfo component of `uri`'s authority, or empty when none. Splits at the LAST `@`
   * rather than using `URI#getUserInfo`, which (like [[uriAuthority]]'s getters) returns `null`
   * for the whole authority on an RFC 3986 `reg-name` violation. Never lowercased: userinfo is
   * case-sensitive.
   */
  private[delta] def uriUserInfo(uri: URI): String = {
    val authority = Option(uri.getAuthority).getOrElse("")
    val at = authority.lastIndexOf('@')
    if (at >= 0) authority.substring(0, at) else ""
  }

  /**
   * Redacts `uri`'s authority to `scheme`, then `://`, then a literal `***` masking userinfo,
   * then `@host[:port]`, for embedding in a decline reason. NEVER interpolate `uri.getAuthority`
   * or [[uriUserInfo]] directly into a reason string: doing so would leak credentials embedded as
   * URI userinfo into the SQL plan's explain output, fallback-reason logging, or the Spark UI.
   */
  private[delta] def redactedAuthority(uri: URI): String = {
    val scheme = Option(uri.getScheme).map(_.toLowerCase(Locale.ROOT)).getOrElse("")
    val authority = Option(uri.getAuthority).getOrElse("")
    val at = authority.lastIndexOf('@')
    val hostPort = if (at >= 0) authority.substring(at + 1) else authority
    s"$scheme://***@$hostPort"
  }

  /**
   * Decline reason when any of `uris` carries userinfo in its authority (e.g. the container in an
   * abfss:// path), or `None` when none do. The native store cache, `ObjectStoreUrl`, and
   * DataFusion registry all key on scheme/host/port only, dropping userinfo, so two authorities
   * differing only in userinfo collide onto the same store handle.
   */
  private[delta] def userInfoBearingAuthorityReason(uris: Seq[URI]): Option[String] = {
    val offending = uris.filter(uri => uriUserInfo(uri).nonEmpty).map(redactedAuthority).distinct
    if (offending.isEmpty) {
      None
    } else {
      Some("Native Delta scan does not support object-store paths whose authority carries " +
        "userinfo (e.g. the container in an abfss:// path): the native object-store cache, " +
        "ObjectStoreUrl and DataFusion registry all key on scheme, host and port only, so two " +
        "containers on one storage account share a single store handle " +
        s"(found: ${offending.sorted.mkString(", ")})")
    }
  }

  /**
   * String-literal Hadoop conf keys consulted below. `hadoop-aws` is NOT on this module's runtime
   * classpath, so `org.apache.hadoop.fs.s3a.Constants` must never be referenced here (would raise
   * `NoClassDefFoundError` for sessions with no S3 dependency).
   */
  private val HadoopCredentialProviderPathKey = "hadoop.security.credential.provider.path"
  private val S3aCredentialProviderPathKey = "fs.s3a.security.credential.provider.path"

  /**
   * `CommonConfigurationKeysPublic.HADOOP_SECURITY_CREDENTIAL_CLEAR_TEXT_FALLBACK`, default
   * `true`, verified via `javap` against `hadoop-common` 3.3.4's
   * `Configuration#getPasswordFromConfig`: `getPassword` only falls back to reading a plaintext
   * conf value once `getBoolean(<this key>, true)` holds -- with the flag off, a plaintext value
   * is invisible to every `getPassword`-based resolver, even when no credential provider is
   * configured at all.
   */
  private val ClearTextFallbackKey = "hadoop.security.credential.clear-text-fallback"

  private def s3aBucketProviderPathKey(bucket: String): String =
    s"fs.s3a.bucket.$bucket.security.credential.provider.path"

  /**
   * The LONG form of [[s3aBucketProviderPathKey]]: `S3AUtils#lookupPassword` resolves per-bucket
   * overrides through both a long key (`fs.s3a.bucket.B.<full base key>`) and a short key; both
   * must be covered here too.
   */
  private def s3aBucketLongProviderPathKey(bucket: String): String =
    s"fs.s3a.bucket.$bucket.fs.s3a.security.credential.provider.path"

  private def nonEmptyConf(hadoopConf: Configuration, key: String): Boolean =
    Option(hadoopConf.get(key)).exists(_.nonEmpty)

  /**
   * The lowercase-scheme-checked S3/S3A bucket name from `uri`'s authority, or `None` when
   * `uri`'s scheme is not `s3`/`s3a`. Parses the raw authority manually rather than
   * `URI#getHost`, avoiding the same RFC 3986 `reg-name` pitfall as [[uriAuthority]].
   */
  private def s3Bucket(uri: URI): Option[String] = {
    val scheme = Option(uri.getScheme).map(_.toLowerCase(Locale.ROOT))
    if (scheme.contains("s3") || scheme.contains("s3a")) {
      val authority = Option(uri.getAuthority).getOrElse("")
      val at = authority.lastIndexOf('@')
      val hostAndPort = if (at >= 0) authority.substring(at + 1) else authority
      val colon = hostAndPort.lastIndexOf(':')
      val host = if (colon >= 0) hostAndPort.substring(0, colon) else hostAndPort
      if (host.isEmpty) None else Some(host)
    } else {
      None
    }
  }

  private def plainValue(hadoopConf: Configuration, key: String): Option[String] =
    Option(hadoopConf.get(key)).filter(_.nonEmpty)

  /**
   * How Hadoop's OWN consumer reads one of the keys compared by [[s3ConfigDivergenceReason]],
   * which decides how [[s3KeyDivergenceReason]] computes the Hadoop-effective side of its
   * equality check. Exactly two consumer families exist among [[AllS3ConfigKeys]] in `hadoop-aws`
   * 3.3.4, each verified via `javap`/CFR against the real call sites (cited per key on
   * [[S3ConfigKeyConsumers]]). The tier must mirror the key's ACTUAL consumer: resolving a
   * [[PropagatedOptionConsumer]] key through the wider `lookupPassword` cascade is NOT fail-safe
   * for a value-EQUALITY comparator -- a long-form alias value Hadoop itself never reads can
   * EQUAL native's resolution while Hadoop's true propagate-then-plain-get value differs, turning
   * a real divergence into a wrongly-claimed scan (the endpoint `${...}`-redirect shape pinned in
   * `DeltaScanContribSuite`).
   */
  private[delta] sealed trait S3ConfigConsumer

  /**
   * Read via `S3AUtils#lookupPassword(bucket, conf, baseKey)`, verified via `javap` against
   * `hadoop-aws` 3.3.4: builds `longBucketKey = "fs.s3a.bucket." + bucket + "." + baseKey` (the
   * FULL, already-`fs.s3a`-prefixed base key appended after the bucket segment) and reads it via
   * `Configuration#getPassword` BEFORE the short-bucket key, keeping the long value whenever
   * `getPassword` returns non-empty and only falling through to short-then-global otherwise.
   * `getPassword` is Hadoop-credential-provider-aware and skips plaintext conf entirely when
   * [[ClearTextFallbackKey]] is false. Modeled by [[hadoopLookupPasswordEffective]].
   */
  private[delta] case object LookupPasswordConsumer extends S3ConfigConsumer

  /**
   * Read via `S3AUtils#propagateBucketOptions` followed by a plain `Configuration#get`-family
   * call (`getTrimmed`/`getBoolean`/`getClasses`) against the propagated view: the short bucket
   * form wins only by having overwritten the global key during propagation, the long bucket form
   * folds into an unread `fs.s3a.fs.s3a.*` key, and neither a credential provider nor
   * [[ClearTextFallbackKey]] is ever consulted. Modeled as a plain `Configuration#get` on the
   * [[propagateBucketOptions]] result, which also expands `${...}` references under that
   * propagated view exactly like the real consumer.
   */
  private[delta] case object PropagatedOptionConsumer extends S3ConfigConsumer

  /**
   * Every `fs.s3a.*` base key that governs whether a claimed native scan actually behaves like
   * Hadoop's own reader would, paired with the consumer family Hadoop resolves it through -- ONE
   * list, with each key's resolution tier declared beside it, so a key can never sit in the
   * comparator without a deliberate classification (adding one without picking a tier does not
   * compile). The entries are every per-bucket `fs.s3a.*` base key native's S3 client's
   * `get_config` (s3.rs) resolves, verified directly against its call sites:
   * `extract_s3_config_options` (endpoint.region, path.style.access, endpoint,
   * requester.pays.enabled), `lookup_provider_class` (the Comet-specific
   * credential-provider-class activation key), and
   * `build_credential_provider`/`build_aws_credential_provider_metadata`/
   * `build_assume_role_credential_provider_metadata` (aws.credentials.provider,
   * assumed.role.credentials.provider, assumed.role.arn, assumed.role.session.name).
   *
   * Tier assignments, each verified via `javap`/CFR against `hadoop-aws` 3.3.4:
   *   - access.key/secret.key/session.token: `S3AUtils#getAWSAccessKeys` and
   *     `MarshalledCredentialBinding#fromFileSystem` (reached from
   *     `TemporaryAWSCredentialsProvider`) resolve all three via `S3AUtils#lookupPassword` --
   *     [[LookupPasswordConsumer]].
   *   - aws.credentials.provider and assumed.role.credentials.provider:
   *     `S3AUtils#buildAWSProviderList` -> `loadAWSProviderClasses` -> plain
   *     `Configuration#getClasses` -- [[PropagatedOptionConsumer]].
   *   - assumed.role.arn/session.name: `AssumedRoleCredentialProvider`'s constructor reads both
   *     via plain `Configuration#getTrimmed` -- [[PropagatedOptionConsumer]].
   *   - endpoint (`S3AFileSystem`: `getTrimmed`), endpoint.region (`DefaultS3ClientFactory`:
   *     `getTrimmed`), path.style.access (`S3AFileSystem`: `getBoolean`) --
   *     [[PropagatedOptionConsumer]].
   *   - requester.pays.enabled: not read anywhere in `hadoop-aws` 3.3.4 (the constant does not
   *     even exist in its `Constants` class); later releases read it via plain `getBoolean`
   *     against the propagated conf, so the plain tier is both the faithful forward model and
   *     inert on 3.3.4 -- [[PropagatedOptionConsumer]].
   *   - comet.credential.provider.class: Comet's own activation key, plain conf read on both
   *     sides, never a Hadoop key at all -- [[PropagatedOptionConsumer]].
   *
   * SYNC NOTE: the key list must stay a superset of native's `NATIVE_S3A_CONFIG_PROPERTIES`
   * constant (`native/core/src/parquet/objectstore/s3.rs`, property suffixes without the
   * `fs.s3a.` prefix) -- `DeltaScanContribSuite`'s discovery-harness test asserts this
   * mechanically against [[AllS3ConfigKeys]]. Literal strings, not the
   * [[AwsCredentialsProviderKey]] / [[AssumedRoleCredentialsProviderKey]] vals declared below,
   * purely to avoid a forward reference inside this `object` body; kept textually identical to
   * those two constants.
   */
  private[delta] val S3ConfigKeyConsumers: Seq[(String, S3ConfigConsumer)] = Seq(
    "fs.s3a.access.key" -> LookupPasswordConsumer,
    "fs.s3a.secret.key" -> LookupPasswordConsumer,
    "fs.s3a.session.token" -> LookupPasswordConsumer,
    "fs.s3a.aws.credentials.provider" -> PropagatedOptionConsumer,
    "fs.s3a.assumed.role.arn" -> PropagatedOptionConsumer,
    "fs.s3a.assumed.role.session.name" -> PropagatedOptionConsumer,
    "fs.s3a.assumed.role.credentials.provider" -> PropagatedOptionConsumer,
    "fs.s3a.endpoint" -> PropagatedOptionConsumer,
    "fs.s3a.endpoint.region" -> PropagatedOptionConsumer,
    "fs.s3a.path.style.access" -> PropagatedOptionConsumer,
    "fs.s3a.requester.pays.enabled" -> PropagatedOptionConsumer,
    "fs.s3a.comet.credential.provider.class" -> PropagatedOptionConsumer)

  /** The compared keys alone, in [[S3ConfigKeyConsumers]] order (discovery-harness surface). */
  private[delta] val AllS3ConfigKeys: Seq[String] = S3ConfigKeyConsumers.map(_._1)

  /**
   * The short-bucket-then-global value resolved for `baseKey` under `bucket` from `hadoopConf`,
   * skipping an empty value at either alias exactly like [[plainValue]]. NOT used by
   * [[s3ConfigDivergenceReason]]/[[s3KeyDivergenceReason]] any more -- every key checked there
   * resolves per its declared [[S3ConfigKeyConsumers]] tier (see [[s3KeyDivergenceReason]]),
   * neither of which matches this function's read. This function's one remaining caller is
   * [[shortThenGlobalOrReason]], which reads provider-CLASS strings (from the ORIGINAL,
   * unpropagated conf) for name-support validation in [[providerClassReason]]/
   * [[assumedRoleProviderClassReason]] -- by the time those run, [[s3ConfigDivergenceReason]] has
   * already proven Hadoop's and native's effective values agree for the same key, so whichever of
   * the two (equal) values this narrower read returns does not affect correctness there. NEVER
   * used to compute native's own effective value -- see [[nativeShortThenGlobal]] for that.
   */
  private def shortThenGlobal(
      hadoopConf: Configuration,
      bucket: String,
      baseKey: String): Option[String] = {
    val shortKey = s"fs.s3a.bucket.$bucket." + baseKey.stripPrefix("fs.s3a.")
    plainValue(hadoopConf, shortKey).orElse(plainValue(hadoopConf, baseKey))
  }

  /**
   * The short-bucket-then-global value native's `get_config` (s3.rs) resolves for `baseKey` under
   * `bucket` from the ORIGINAL, unpropagated `hadoopConf` -- `NativeConfig
   * .extractObjectStoreOptions` forwards `Configuration#get`'s substituted value for every
   * `fs.s3a.*` entry with no bucket-option propagation step of its own, so the original conf is
   * the right input here. Unlike [[shortThenGlobal]]/[[plainValue]], this mirrors `get_config`
   * faithfully: PRESENCE of the short-bucket key alone -- never its emptiness -- decides whether
   * native falls back to the global key (`get_config` is a plain `HashMap::get`, which returns
   * `Some` for a key explicitly set to `""`), so an explicitly empty or whitespace-only
   * short-bucket value resolves to `Some("")` here and never falls through to global -- the
   * OPPOSITE of Hadoop's own `getPassword`/`lookupPassword` semantics (see
   * [[hadoopLookupPasswordEffective]]), which treat empty as absent and keep trying the next
   * alias. The ONLY function used to compute native's effective value in
   * [[s3KeyDivergenceReason]].
   *
   * Deliberately does NOT apply `get_config_trimmed`'s `.trim()` here: [[s3KeyDivergenceReason]]
   * trims both this value and Hadoop's effective value together, symmetrically, at the point they
   * are compared, rather than one-sidedly here -- trimming only the native side would flag a
   * spurious divergence for a value neither side's whitespace actually changes the behavior of
   * once each side's own downstream parsing normalizes it (e.g. Hadoop's own multi-line
   * `fs.s3a.aws.credentials.provider` default, which both Hadoop and native additionally trim per
   * comma-separated entry after splitting), while a one-sided trim would make an
   * otherwise-identical default value look diverged for every bucket, never claiming natively at
   * all.
   */
  private def nativeShortThenGlobal(
      hadoopConf: Configuration,
      bucket: String,
      baseKey: String): Option[String] = {
    val shortKey = s"fs.s3a.bucket.$bucket." + baseKey.stripPrefix("fs.s3a.")
    Option(hadoopConf.get(shortKey)).orElse(Option(hadoopConf.get(baseKey)))
  }

  /**
   * Faithful in-memory replica of `S3AUtils#propagateBucketOptions` (`hadoop-aws`), which
   * `S3AFileSystem#initialize` calls FIRST, before any option or credential is read:
   * `Configuration conf = propagateBucketOptions(originalConf, bucket); ...; setConf(conf);` --
   * every subsequent `conf.get`/`getPassword` call in that filesystem instance, including
   * `${...}` variable substitution, resolves against this propagated view, not the original conf.
   * `hadoop-aws` is not on this module's runtime classpath (see the string-literal-keys note
   * above), so `S3AUtils#propagateBucketOptions` cannot be called directly; this reproduces its
   * logic verbatim using only `hadoop-common`'s `Configuration`:
   * {{{
   * public static Configuration propagateBucketOptions(Configuration source, String bucket) {
   *   final String bucketPrefix = FS_S3A_BUCKET_PREFIX + bucket + '.';
   *   final Configuration dest = new Configuration(source);
   *   for (Map.Entry<String, String> entry : source) {
   *     final String key = entry.getKey();
   *     final String value = entry.getValue(); // the (unexpanded) value
   *     if (!key.startsWith(bucketPrefix) || bucketPrefix.equals(key)) continue;
   *     final String stripped = key.substring(bucketPrefix.length());
   *     if (stripped.startsWith("bucket.") || "impl".equals(stripped)) {
   *       // ignored
   *     } else {
   *       final String generic = FS_S3A_PREFIX + stripped;
   *       dest.set(generic, value, ...); // overwrites any existing global value
   *     }
   *   }
   *   return dest;
   * }
   * }}}
   * Note the LONG bucket form (`fs.s3a.bucket.B.fs.s3a.<key>`) folds to an unread
   * `fs.s3a.fs.s3a.<key>` key here too, exactly like the real method -- `stripped` already starts
   * with `fs.s3a.` in that case, so prepending `fs.s3a.` again produces a key nothing ever reads.
   */
  private def propagateBucketOptions(hadoopConf: Configuration, bucket: String): Configuration = {
    val bucketPrefix = s"fs.s3a.bucket.$bucket."
    val dest = new Configuration(hadoopConf)
    hadoopConf.iterator().asScala.foreach { entry =>
      val key = entry.getKey
      if (key.startsWith(bucketPrefix) && key != bucketPrefix) {
        val stripped = key.substring(bucketPrefix.length)
        if (!stripped.startsWith("bucket.") && stripped != "impl") {
          dest.set(s"fs.s3a.$stripped", entry.getValue)
        }
      }
    }
    dest
  }

  /**
   * Canonical and deprecated Hadoop S3A encryption-algorithm config keys, verified via `javap`
   * against `hadoop-aws` 3.3.4's `org.apache.hadoop.fs.s3a.Constants`: `S3_ENCRYPTION_ALGORITHM =
   * "fs.s3a.encryption.algorithm"` (canonical) and `SERVER_SIDE_ENCRYPTION_ALGORITHM =
   * "fs.s3a.server-side-encryption-algorithm"` (DEPRECATED -- note the hyphen before "algorithm",
   * unlike the corresponding `*.key` constants below, which both use a `.key` suffix).
   * `hadoop-aws` is NOT on this module's runtime classpath, so these stay string literals, same
   * rationale as [[HadoopCredentialProviderPathKey]].
   */
  private val S3EncryptionAlgorithmKey = "fs.s3a.encryption.algorithm"
  private val DeprecatedS3EncryptionAlgorithmKey = "fs.s3a.server-side-encryption-algorithm"

  /**
   * The exact strings `S3AEncryptionMethods#getMethod` accepts, verified via `javap`/CFR against
   * `hadoop-aws` 3.3.4's `S3AEncryptionMethods` enum: `NONE("")`, `SSE_S3("AES256", serverSide =
   * true, requiresSecret = false)`, `SSE_KMS("SSE-KMS", serverSide = true, requiresSecret =
   * false)`, `SSE_C("SSE-C", serverSide = true, requiresSecret = true)`, `CSE_KMS("CSE-KMS",
   * serverSide = false, requiresSecret = true)`, `CSE_CUSTOM("CSE-CUSTOM", serverSide = false,
   * requiresSecret = true)`. `getMethod` parses case-insensitively
   * (`values().find(_.getMethod.equalsIgnoreCase(algorithm))`), matched below the same way.
   *
   * ALLOWLIST, not a blocklist (replaces the former SSE-C-only blocklist): only the algorithms S3
   * decrypts transparently on GET/HEAD given read permission alone, with NO extra request header
   * and NO client-side step, are safe for a native scan that forwards none of Hadoop's
   * `fs.s3a.encryption.*`/`fs.s3a.server-side-encryption*` options --
   *   - `AES256` (SSE_S3, `serverSide = true`): plain server-side encryption, transparent on GET.
   *   - `SSE-KMS` (SSE_KMS, `serverSide = true`): server-side, KMS-managed key, transparent on
   *     GET given KMS decrypt permission (no header).
   *   - `DSSE-KMS`: NOT present in this enum on `hadoop-aws` 3.3.4 (confirmed by the six values
   *     listed above) -- `S3AEncryptionMethods.getMethod("DSSE-KMS")` throws
   *     `IOException("Unknown encryption algorithm DSSE-KMS")` on this version, so
   *     `S3AUtils#buildEncryptionSecrets` (and therefore Hadoop's own reader) already fails
   *     before ever reading such a table under 3.3.4, meaning this string can never actually be
   *     the resolved value on the declared target version -- admitting it here is inert there.
   *     Included anyway, forward-compatible, for a newer `hadoop-aws` on the runtime classpath (a
   *     later Hadoop release; this module has no compile-time `hadoop-aws` dependency, see the
   *     string-literal-keys note above) where DSSE-KMS is a real, dual-layer, server-side
   *     algorithm decrypted transparently on GET the same way SSE-KMS is. Every other value
   *     declines: `SSE-C` (SSE_C is `serverSide = true` in Hadoop's own enum, but `requiresSecret
   *     \= true` -- S3 rejects a GET/HEAD for an SSE-C object outright (400 Bad Request) unless
   *     the customer key is resent as a request header on every call, so a native scan that never
   *     learns the key cannot succeed at all, where Hadoop's own reader -- whose request factory
   *     attaches the key -- would), `CSE-KMS`/`CSE-CUSTOM` (`serverSide = false`: client-side
   *     encryption decrypts object bytes locally in the SDK layer, which the native Parquet
   *     reader has no equivalent of -- it would read raw ciphertext), and any future/unknown
   *     value (a value `S3AEncryptionMethods.getMethod` itself would reject is certainly not one
   *     of the three confirmed-transparent algorithms above; declining is the only safe default
   *     for anything this gate cannot positively confirm).
   */
  private val AllowedEncryptionAlgorithms: Set[String] = Set("AES256", "SSE-KMS", "DSSE-KMS")

  /**
   * `bucket`'s effective encryption-algorithm key and value under `hadoopConf`, or `None` when
   * neither the canonical nor deprecated key is set anywhere consulted. Mirrors
   * `S3AUtils#buildEncryptionSecrets`'s real resolution order, verified via `javap`/CFR
   * decompilation of `hadoop-aws` 3.3.4's `S3AUtils.class`:
   * {{{
   * String algorithm = lookupBucketSecret(bucket, conf, "fs.s3a.encryption.algorithm");
   * if (algorithm == null)
   *   algorithm = lookupBucketSecret(bucket, conf, "fs.s3a.server-side-encryption-algorithm");
   * if (algorithm == null)
   *   algorithm = lookupPassword(null, conf, "fs.s3a.encryption.algorithm");
   * if (algorithm == null)
   *   algorithm = lookupPassword(null, conf, "fs.s3a.server-side-encryption-algorithm");
   * }}}
   * i.e. bucket-tier (canonical, then deprecated), THEN global-tier (canonical, then deprecated)
   * -- the two tiers are never interleaved key-by-key, so this must stay two explicit bucket-tier
   * lookups followed by two explicit global-tier lookups, not a single
   * [[hadoopLookupPasswordEffective]] call per key (which would let an unset canonical bucket key
   * fall through straight to the canonical GLOBAL value ahead of a SET deprecated bucket key, the
   * wrong answer).
   *
   * THE FIX for the SSE-C long-bucket-alias gap is entirely inside the bucket tier:
   * `lookupBucketSecret` itself is long-then-short, decompiled from `hadoop-aws` 3.3.4's
   * `S3AUtils.class`:
   * {{{
   * // longBucketKey  = fs.s3a.bucket.B.fs.s3a.<key>
   * String longBucketKey = String.format(BUCKET_PATTERN, bucket, baseKey);
   * String initialVal = getPassword(conf, longBucketKey, null, null);
   * // shortBucketKey = fs.s3a.bucket.B.<key>
   * String shortBucketKey = String.format(BUCKET_PATTERN, bucket, subkey);
   * // keeps initialVal (the LONG value) if non-empty
   * return getPassword(conf, shortBucketKey, initialVal, null);
   * }}}
   * i.e. the SAME long-bucket-key construction and long-wins-if-nonempty semantics as
   * `S3AUtils#lookupPassword` (see [[LookupPasswordConsumer]]/[[hadoopLookupPasswordEffective]])
   * -- the encryption algorithm is NOT one of the keys that flows through
   * `S3AUtils#propagateBucketOptions` (which folds an unrelated per-bucket LONG form into an
   * unread key). An earlier version of this function modeled the bucket tier as SHORT-only,
   * documented as "the LONG bucket form is genuinely never consulted for this key" -- that
   * documentation was wrong (this decompilation supersedes it): a bucket configured only via
   * `fs.s3a.bucket.B.fs.s3a.encryption.algorithm=SSE-C` bypassed the SSE-C gate entirely, because
   * Hadoop's own reader DOES read that long form (and picks SSE-C), while this function reported
   * `None` (nothing set) and the allowlist check below never even ran.
   *
   * The canonical-vs-deprecated distinction below is frequently moot in practice: `hadoop-aws`'s
   * `S3AFileSystem.addDeprecatedKeys()` statically registers `fs.s3a.server-side-encryption-*` as
   * `Configuration`-level deprecated aliases of `fs.s3a.encryption.*` (verified via `javap`), a
   * registration that lives in a static field on Hadoop's `Configuration` class -- process-wide
   * once `S3AFileSystem`'s class has loaded anywhere in the JVM, which a real scan has always
   * already done by the time this gate runs, since reading the S3 table at all requires loading
   * that class. Once active, `Configuration#get` resolves either literal key to the identical
   * value transparently, making the two-key cascade below redundant (but harmless) for that case;
   * it remains the operative path only when nothing else in the process has loaded
   * `S3AFileSystem` yet.
   *
   * ALSO walks the Hadoop-credential-provider (JCEKS) path via [[resolveViaCredentialAliases]]
   * for each of the four lookups below, matching `lookupBucketSecret`/`lookupPassword`'s real
   * per-alias `getPassword` calls (quoted above) exactly: both are `getPassword`, not plain
   * `Configuration#get`, so a bucket storing the algorithm name ONLY in a JCEKS keystore is
   * exactly as real a Hadoop deployment shape for this key as it is for the credential keys
   * [[hadoopLookupPasswordEffective]] already covers -- there is nothing algorithm-specific that
   * makes JCEKS storage implausible here, so an earlier version of this function skipping it
   * (documented at the time as "the algorithm NAME is not credential-sensitive data, so storing
   * it in a keystore is not a realistic Hadoop deployment pattern") was an unjustified, narrower
   * read than Hadoop's own resolver actually performs, under-declining a bucket whose algorithm
   * is keystore-only. [[resolveViaCredentialAliases]]'s Arm B/C split still means this is zero
   * extra I/O for the common case: keystore I/O only happens when a Hadoop credential-provider
   * path is actually configured for the bucket, contained in that function's own try/catch.
   * `bucketTier`/`globalTier` return `Left` (propagated straight through by [[orElseTier]]) when
   * [[resolveViaCredentialAliases]] cannot safely verify a tier at all (an S3A-scoped provider
   * path, or a corrupt/unreadable global keystore) -- correctly short-circuiting the whole
   * cascade with a decline rather than silently falling through to a later tier that might look
   * unset only because the true value was unverifiable.
   */
  private def effectiveEncryptionAlgorithm(
      hadoopConf: Configuration,
      bucket: String): Either[String, Option[(String, String)]] = {
    def bucketTier(baseKey: String): Either[String, Option[(String, String)]] = {
      val longKey = s"fs.s3a.bucket.$bucket.$baseKey"
      val shortKey = s"fs.s3a.bucket.$bucket." + baseKey.stripPrefix("fs.s3a.")
      resolveViaCredentialAliases(hadoopConf, bucket, Seq(longKey, shortKey))
        .map(_.map(baseKey -> _))
    }
    def globalTier(baseKey: String): Either[String, Option[(String, String)]] =
      resolveViaCredentialAliases(hadoopConf, bucket, Seq(baseKey))
        .map(_.map(baseKey -> _))

    // Short-circuits on Left (unverifiable tier) or Right(Some(_)) (resolved); only Right(None)
    // (tier definitively unset) falls through to `next`, mirroring buildEncryptionSecrets's
    // sequential `if (algorithm == null) algorithm = ...` cascade exactly.
    def orElseTier(
        current: Either[String, Option[(String, String)]],
        next: => Either[String, Option[(String, String)]])
        : Either[String, Option[(String, String)]] =
      current match {
        case Left(reason) => Left(reason)
        case Right(Some(value)) => Right(Some(value))
        case Right(None) => next
      }

    orElseTier(
      bucketTier(S3EncryptionAlgorithmKey),
      orElseTier(
        bucketTier(DeprecatedS3EncryptionAlgorithmKey),
        orElseTier(
          globalTier(S3EncryptionAlgorithmKey),
          globalTier(DeprecatedS3EncryptionAlgorithmKey))))
  }

  private def unsupportedEncryptionAlgorithmDeclineReason(
      bucket: String,
      algorithmKey: String,
      algorithm: String): String =
    s"Native Delta scan does not support $algorithmKey=$algorithm for $bucket " +
      "(the native S3 client only supports unencrypted objects and S3's transparent " +
      "server-side algorithms -- AES256/SSE-S3, SSE-KMS, and DSSE-KMS decrypt on GET/HEAD given " +
      "read permission alone, with no extra request header; SSE-C additionally requires the " +
      "customer-provided key resent as a header on every GET/HEAD request, which the native S3 " +
      "client's extract_s3_config_options never forwards, and CSE-KMS/CSE-CUSTOM decrypt object " +
      "bytes client-side, a layer the native Parquet reader does not have -- any of these would " +
      "fail outright or silently read ciphertext where Hadoop's own reader succeeds)"

  /**
   * First reason any bucket among `uris` is configured for an encryption algorithm the native S3
   * client cannot safely read, or `None` when claimable. Allowlist-based (see
   * [[AllowedEncryptionAlgorithms]]): only `AES256`/`SSE-KMS`/`DSSE-KMS` (and unset/empty) pass;
   * every other resolved value -- `SSE-C`, `CSE-KMS`, `CSE-CUSTOM`, or any unrecognized future
   * algorithm string -- declines. Deliberately NOT a blocklist keyed on `SSE-C` alone: an
   * allowlist is safe by construction against a Hadoop release adding a new encryption method
   * this gate has never heard of, where a blocklist would silently admit it. Never interpolates a
   * resolved key value, only key names, the bucket, and the (non-secret) algorithm name. Declines
   * on a `Left` from [[effectiveEncryptionAlgorithm]] too (an unverifiable credential-provider
   * arm, e.g. an S3A-scoped provider path or a corrupt/unreadable global keystore) -- the
   * algorithm cannot be ruled safe when it cannot be read at all.
   */
  private[delta] def unsupportedEncryptionAlgorithmReason(
      hadoopConf: Configuration,
      uris: Seq[URI]): Option[String] = {
    val buckets = uris.flatMap(s3Bucket).distinct
    buckets.foldLeft(Option.empty[String]) { (declined, bucket) =>
      if (declined.isDefined) {
        declined
      } else {
        effectiveEncryptionAlgorithm(hadoopConf, bucket) match {
          case Left(reason) => Some(reason)
          case Right(None) => None
          case Right(Some((key, value))) =>
            if (!AllowedEncryptionAlgorithms.exists(_.equalsIgnoreCase(value))) {
              Some(unsupportedEncryptionAlgorithmDeclineReason(bucket, key, value))
            } else {
              None
            }
        }
      }
    }
  }

  /**
   * Canonical Hadoop S3A HTTP-proxy host config key, verified via CFR decompilation of
   * `hadoop-aws` 3.3.4's `S3AUtils.class` (`initProxySupport`):
   * {{{
   * String proxyHost = conf.getTrimmed("fs.s3a.proxy.host", "");
   * int proxyPort = conf.getInt("fs.s3a.proxy.port", -1);
   * if (!proxyHost.isEmpty()) {
   *   ...
   *   String proxyUsername =
   *       S3AUtils.lookupPassword(bucket, conf, "fs.s3a.proxy.username", null, null);
   *   String proxyPassword =
   *       S3AUtils.lookupPassword(bucket, conf, "fs.s3a.proxy.password", null, null);
   *   ...
   * }
   * }}}
   * `fs.s3a.proxy.host`/`fs.s3a.proxy.port` resolve via a PLAIN, non-bucket-scoped, non-JCEKS
   * `Configuration#getTrimmed`/`getInt` call -- NOT `lookupPassword` -- against whatever conf
   * `S3AFileSystem#initialize` already ran through `propagateBucketOptions` before
   * `createAwsConf`/`initProxySupport` ever runs; only the SIBLING `fs.s3a.proxy.username`/
   * `fs.s3a.proxy.password` keys go through `lookupPassword` (bucket long/short/global,
   * JCEKS-aware). So the host is bucket-aware only via `propagateBucketOptions`'s short-bucket-
   * form folding, never the long-bucket form, and never a credential-provider read -- the SAME
   * shape as `endpoint`/`path.style.access` ([[PropagatedOptionConsumer]], see
   * [[S3ConfigKeyConsumers]]'s doc), not the credential family. `hadoop-aws` 3.4.x (the Spark 4.x
   * profiles' version) moves this code to `AWSClientConfig#createProxyConfiguration`/
   * `#createAsyncProxyConfiguration` but keeps the exact same reads, verified via `javap` against
   * 3.4.2: `conf.getTrimmed("fs.s3a.proxy.host", "")` for the host, `S3AUtils.lookupPassword` for
   * username/password only.
   *
   * [[proxyGateReason]] therefore resolves the host EXACTLY like its real consumer -- plain
   * `Configuration#getTrimmed` on the [[propagateBucketOptions]] result, no provider arms, no
   * [[ClearTextFallbackKey]] handling -- rather than through the wider `lookupPassword` cascade
   * an earlier version reused here. The wider cascade was wrong in BOTH directions for this key:
   * with only the GLOBAL Hadoop provider path set and [[ClearTextFallbackKey]] false,
   * `getPassword` hides a plaintext host that `getTrimmed` serves to Hadoop anyway (a missed
   * decline, the exact bypass this gate exists to close -- native has NO HTTP-proxy support of
   * any kind, no `fs.s3a.proxy.*` key is read anywhere in `s3.rs`); and an S3A-scoped provider
   * path or a lone long-form bucket alias declined a bucket whose real consumer can never see a
   * host from either source (pure over-refusal -- no keystore can supply the host to a plain
   * `getTrimmed`, and the long form folds into the unread `fs.s3a.fs.s3a.proxy.host`).
   */
  private val S3ProxyHostKey = "fs.s3a.proxy.host"

  private def unsupportedProxyReason(bucket: String, key: String): String =
    s"Native Delta scan does not support $key configured for $bucket (the native S3 client has " +
      "no HTTP proxy support at all -- no fs.s3a.proxy.* key is read anywhere in its object " +
      "store layer -- so a claimed scan would connect to S3 directly instead of routing through " +
      "the configured proxy, either bypassing an egress/network-segmentation policy or simply " +
      "failing to reach the endpoint)"

  /**
   * First reason any bucket among `uris` has an HTTP proxy configured via [[S3ProxyHostKey]], or
   * `None` when claimable. Reads the host EXACTLY like its real consumer (see
   * [[S3ProxyHostKey]]'s doc): plain `Configuration#getTrimmed` on the [[propagateBucketOptions]]
   * result, so this gate is always zero-I/O -- no credential provider is ever consulted for the
   * host, because none ever supplies it to Hadoop either. Ordered alongside
   * [[unsupportedEncryptionAlgorithmReason]] among the conf-only gates, ahead of
   * [[s3ConfigDivergenceReason]]. The try/catch guards `Configuration#get`'s
   * `IllegalStateException` on a `${...}` substitution cycle, same as [[s3KeyDivergenceReason]].
   * Never interpolates a resolved value: the proxy HOST is not secret, but naming it here would
   * be a strange place to first surface it, and proxy CREDENTIALS
   * (`fs.s3a.proxy.username`/`fs.s3a.proxy.password`, not read by this gate at all -- the whole
   * point of gating on the host is that a non-empty host declines before any proxy credential
   * would ever need to be forwarded) must never appear in a decline reason regardless.
   */
  private[delta] def proxyGateReason(
      hadoopConf: Configuration,
      uris: Seq[URI],
      propagatedConfCache: MutableMap[String, Configuration] = MutableMap.empty)
      : Option[String] = {
    val buckets = uris.flatMap(s3Bucket).distinct
    buckets.foldLeft(Option.empty[String]) { (declined, bucket) =>
      if (declined.isDefined) {
        declined
      } else {
        try {
          val propagatedConf =
            propagatedConfCache.getOrElseUpdate(
              bucket,
              propagateBucketOptions(hadoopConf, bucket))
          if (propagatedConf.getTrimmed(S3ProxyHostKey, "").nonEmpty) {
            Some(unsupportedProxyReason(bucket, S3ProxyHostKey))
          } else {
            None
          }
        } catch {
          case e @ (_: IOException | _: RuntimeException) =>
            Some(unverifiableValueReason(bucket, S3ProxyHostKey, e))
        }
      }
    }
  }

  /**
   * Canonical Hadoop S3A assumed-role session-policy key. `AssumedRoleCredentialProvider`'s
   * constructor reads it via a plain `Configuration#getTrimmed` against the
   * [[propagateBucketOptions]]-propagated conf (verified via `javap` against `hadoop-aws` 3.3.4
   * and 3.4.1: `conf.getTrimmed("fs.s3a.assumed.role.policy", "")` -- the same consumer shape as
   * `assumed.role.arn`/`session.name`, see [[S3ConfigKeyConsumers]]) and, when non-empty,
   * attaches it as the session policy of its STS AssumeRole request.
   */
  private val S3AssumedRolePolicyKey = "fs.s3a.assumed.role.policy"

  private def assumedRolePolicyReason(bucket: String, key: String): String =
    s"Native Delta scan does not support $key configured for $bucket (Hadoop sends the " +
      "configured session policy in its STS AssumeRole request, but the native S3 client's " +
      "assumed-role provider never reads or forwards this key, so a claimed scan would " +
      "assume the role WITHOUT the configured session restriction -- silently widening the " +
      "effective permissions instead of failing)"

  /**
   * First reason any bucket among `uris` configures an assumed-role session policy via
   * [[S3AssumedRolePolicyKey]], or `None` when claimable. Hadoop's
   * `AssumedRoleCredentialProvider` includes the policy in its AssumeRole request; native's
   * assumed-role provider does not, so a configured policy must decline until native supports it.
   * Resolved exactly like the key's real consumer (plain `getTrimmed` on the propagated conf,
   * mirroring [[proxyGateReason]]); declined whenever set, whether or not the current provider
   * chain names the assumed-role provider -- a policy that is dead config today can become live
   * through a provider-chain change native never re-validates. Never interpolates the policy
   * document itself, only the key and bucket.
   */
  private[delta] def assumedRolePolicyGateReason(
      hadoopConf: Configuration,
      uris: Seq[URI],
      propagatedConfCache: MutableMap[String, Configuration] = MutableMap.empty)
      : Option[String] = {
    val buckets = uris.flatMap(s3Bucket).distinct
    buckets.foldLeft(Option.empty[String]) { (declined, bucket) =>
      if (declined.isDefined) {
        declined
      } else {
        try {
          val propagatedConf =
            propagatedConfCache.getOrElseUpdate(
              bucket,
              propagateBucketOptions(hadoopConf, bucket))
          if (propagatedConf.getTrimmed(S3AssumedRolePolicyKey, "").nonEmpty) {
            Some(assumedRolePolicyReason(bucket, S3AssumedRolePolicyKey))
          } else {
            None
          }
        } catch {
          case e @ (_: IOException | _: RuntimeException) =>
            Some(unverifiableValueReason(bucket, S3AssumedRolePolicyKey, e))
        }
      }
    }
  }

  /**
   * `baseKey`'s long, short, then global per-bucket aliases, in Hadoop's own resolution order.
   */
  private def longThenShortThenGlobalAliases(bucket: String, baseKey: String): Seq[String] = {
    val suffix = baseKey.stripPrefix("fs.s3a.")
    Seq(s"fs.s3a.bucket.$bucket.fs.s3a.$suffix", s"fs.s3a.bucket.$bucket.$suffix", baseKey)
  }

  private def s3aScopedProviderPathReason(bucket: String, providerPathKey: String): String =
    "Native Delta scan cannot forward Hadoop credential-provider aliases for " +
      s"$bucket ($providerPathKey configures an S3A-scoped Hadoop credential provider that " +
      "Configuration#getPassword does not consult, so the native S3 client's credentials " +
      "cannot be verified)"

  private def unverifiableCredentialProviderReason(bucket: String, error: Throwable): String =
    "Native Delta scan cannot verify Hadoop credential-provider aliases for " +
      s"$bucket (reading $HadoopCredentialProviderPathKey raised " +
      s"${error.getClass.getName}), declining rather than risk missing credentials"

  /**
   * Three-way Hadoop credential-provider-path precheck shared by every `getPassword`-based
   * resolution below, a property of the bucket alone. An S3A- or bucket-scoped provider path,
   * which `Configuration#getPassword` never consults, yields [[UnverifiableProvider]] with no
   * keystore I/O. Only the global path set yields [[GlobalProviderOnly]], the one arm whose
   * callers do real keystore I/O and must wrap their own `getPassword` calls in try/catch. No
   * provider path anywhere yields [[NoProvider]], zero-I/O plain conf reads only.
   */
  private sealed trait CredentialProviderArm
  private case class UnverifiableProvider(offendingKey: String) extends CredentialProviderArm
  private case object GlobalProviderOnly extends CredentialProviderArm
  private case object NoProvider extends CredentialProviderArm

  private def credentialProviderArm(
      hadoopConf: Configuration,
      bucket: String): CredentialProviderArm = {
    val bucketPathKey = s3aBucketProviderPathKey(bucket)
    val bucketLongPathKey = s3aBucketLongProviderPathKey(bucket)
    val s3aPathSet = nonEmptyConf(hadoopConf, S3aCredentialProviderPathKey)
    val bucketPathSet = nonEmptyConf(hadoopConf, bucketPathKey)
    val bucketLongPathSet = nonEmptyConf(hadoopConf, bucketLongPathKey)
    if (s3aPathSet || bucketPathSet || bucketLongPathSet) {
      val offendingKey =
        if (s3aPathSet) S3aCredentialProviderPathKey
        else if (bucketPathSet) bucketPathKey
        else bucketLongPathKey
      UnverifiableProvider(offendingKey)
    } else if (nonEmptyConf(hadoopConf, HadoopCredentialProviderPathKey)) {
      GlobalProviderOnly
    } else {
      NoProvider
    }
  }

  /**
   * Resolves `aliases` in order under `hadoopConf`/`bucket`, keeping the first non-empty value,
   * or `Left(reason)` when the value cannot be safely verified. Dispatches on
   * [[credentialProviderArm]]: [[UnverifiableProvider]] declines with zero I/O;
   * [[GlobalProviderOnly]] resolves each alias via `Configuration#getPassword`, keystore I/O
   * contained in try/catch so a corrupt store declines this bucket rather than aborting planning;
   * [[NoProvider]] resolves each alias via zero-I/O [[plainValue]] reads, which honor
   * [[ClearTextFallbackKey]] the way a real `getPassword` consumer would. Callers such as
   * [[hadoopLookupPasswordEffective]] and [[effectiveEncryptionAlgorithm]] supply the alias lists
   * that match their consumer's real per-tier `getPassword` calls.
   */
  private def resolveViaCredentialAliases(
      hadoopConf: Configuration,
      bucket: String,
      aliases: Seq[String]): Either[String, Option[String]] =
    credentialProviderArm(hadoopConf, bucket) match {
      case UnverifiableProvider(offendingKey) =>
        Left(s3aScopedProviderPathReason(bucket, offendingKey))
      case GlobalProviderOnly =>
        try {
          Right(
            aliases.iterator
              .map(alias =>
                Option(hadoopConf.getPassword(alias)).map(new String(_)).filter(_.nonEmpty))
              .collectFirst { case Some(v) => v })
        } catch {
          case e @ (_: IOException | _: RuntimeException) =>
            Left(unverifiableCredentialProviderReason(bucket, e))
        }
      case NoProvider =>
        if (!hadoopConf.getBoolean(ClearTextFallbackKey, true)) {
          Right(None)
        } else {
          Right(aliases.flatMap(plainValue(hadoopConf, _)).headOption)
        }
    }

  /**
   * `bucket`'s effective value for `baseKey` in Hadoop's own `S3AUtils#lookupPassword` resolution
   * order -- long bucket alias, then short bucket alias, then global, each tried through a Hadoop
   * credential provider before falling back to plain conf (see [[resolveViaCredentialAliases]]
   * for the Arm A/B/C dispatch this delegates to) -- or `Left(reason)` when the value cannot be
   * safely verified.
   *
   * USED ONLY for keys whose real consumer IS `lookupPassword` -- the [[LookupPasswordConsumer]]
   * entries of [[S3ConfigKeyConsumers]], via [[s3KeyDivergenceReason]]. An earlier version ran
   * EVERY compared key through this function, reasoning that a wider read could only ever
   * over-decline; for a value-EQUALITY comparator that reasoning is half-true: the long-form
   * alias this function consults FIRST can hold exactly the value native resolves while Hadoop's
   * true propagate-then-plain-get value differs (e.g. a `${...}` reference whose referent
   * propagation redirects), producing a false EQUALITY that admits a really-diverging scan. A
   * [[PropagatedOptionConsumer]] key must therefore resolve like its actual consumer instead --
   * see [[s3KeyDivergenceReason]].
   *
   * Honors [[ClearTextFallbackKey]] (via [[resolveViaCredentialAliases]]'s [[NoProvider]] arm),
   * matching `getPassword`'s real refusal to read plaintext conf when the flag is off.
   *
   * `hadoopConf` must be a [[propagateBucketOptions]] result (the caller,
   * [[s3KeyDivergenceReason]], always passes one) so that `${...}` references embedded in any
   * alias resolve exactly like `S3AFileSystem#initialize`'s real propagate-then-resolve order.
   */
  private def hadoopLookupPasswordEffective(
      hadoopConf: Configuration,
      bucket: String,
      baseKey: String): Either[String, Option[String]] =
    resolveViaCredentialAliases(
      hadoopConf,
      bucket,
      longThenShortThenGlobalAliases(bucket, baseKey))

  private def effectiveValueDivergenceReason(bucket: String, key: String): String =
    s"Native Delta scan cannot forward $key for $bucket (Hadoop's effective value for this key " +
      "differs from what the native S3 client resolves, so its credentials or configuration " +
      "would differ from Hadoop's)"

  private def unverifiableValueReason(bucket: String, key: String, error: Throwable): String =
    s"Native Delta scan cannot verify $key for $bucket (Configuration#get raised " +
      s"${error.getClass.getName}), declining rather than risk forwarding a stale or " +
      "diverging value"

  /**
   * `None` when `baseKey`'s Hadoop-effective and native-effective values under `bucket` agree, or
   * a decline reason naming `baseKey` and `bucket` (never a value) when they diverge or either
   * side cannot be safely computed.
   *
   * Hadoop's effective value is computed against [[propagateBucketOptions]]'s result, mirroring
   * `S3AFileSystem#initialize`'s actual order (propagate bucket options into the conf FIRST, only
   * THEN read/substitute options against it), through the resolution `consumer` declares for the
   * key in [[S3ConfigKeyConsumers]]: [[LookupPasswordConsumer]] keys via
   * [[hadoopLookupPasswordEffective]] (long-then-short-then-global, keystore- and
   * [[ClearTextFallbackKey]]-aware), [[PropagatedOptionConsumer]] keys via a plain
   * `Configuration#get` on the propagated view (short-form wins by propagation alone; the long
   * form and any keystore/fallback handling are ignored, exactly like the key's real consumer).
   * Resolving a plain-consumer key through the wider `lookupPassword` cascade instead would let a
   * long-form alias value Hadoop never reads EQUAL native's resolution while Hadoop's true
   * plain-get value differs -- a false equality admitting a diverging scan, not merely an extra
   * decline. Native's effective value is always `nativeShortThenGlobal(hadoopConf, ...)` on the
   * ORIGINAL, unpropagated conf, matching `NativeConfig.extractObjectStoreOptions`'s actual
   * forwarding semantics (no propagation step) AND native's `get_config` presence-based (not
   * emptiness-based) short-vs-global fallback -- see [[nativeShortThenGlobal]]. Both values are
   * trimmed together, symmetrically, right before the equality check below (mirroring
   * `get_config_trimmed`'s `.trim()`, which native applies regardless of which alias it read)
   * rather than trimming [[nativeShortThenGlobal]]'s result on its own -- see
   * [[nativeShortThenGlobal]]'s doc for why a one-sided trim there would flag a spurious
   * divergence. Comparing against the propagated view (rather than the original conf, as an
   * earlier version of this check did) matters because propagation can change what a `${...}`
   * reference inside one bucket-scoped value resolves to: e.g.
   * `fs.s3a.bucket.B.access.key=${fs.s3a.custom.ref}` with `fs.s3a.bucket.B.custom.ref=X` and
   * global `fs.s3a.custom.ref=Y` propagates to `fs.s3a.custom.ref=X` (overwriting the global `Y`)
   * before the access key's `${...}` reference is ever substituted, so Hadoop resolves `X` while
   * a check against the unpropagated conf would (wrongly) also see `Y`, the same value native
   * forwards -- masking a real divergence. Wrapped in try/catch: `Configuration#get` raises
   * `IllegalStateException` once `${...}` substitution recurses past Hadoop's `MAX_SUBST` bound
   * (e.g. a two-key mutual reference cycle); declining is safer than crashing planning or
   * comparing a partially-substituted value.
   */
  private def s3KeyDivergenceReason(
      hadoopConf: Configuration,
      propagatedConf: Configuration,
      bucket: String,
      baseKey: String,
      consumer: S3ConfigConsumer): Option[String] = {
    try {
      val hadoopEffective: Either[String, Option[String]] = consumer match {
        case LookupPasswordConsumer =>
          hadoopLookupPasswordEffective(propagatedConf, bucket, baseKey)
        case PropagatedOptionConsumer =>
          Right(Option(propagatedConf.get(baseKey)))
      }
      hadoopEffective match {
        case Left(reason) => Some(reason)
        case Right(hadoopValue) =>
          val nativeValue = nativeShortThenGlobal(hadoopConf, bucket, baseKey)
          if (hadoopValue.map(_.trim) != nativeValue.map(_.trim)) {
            Some(effectiveValueDivergenceReason(bucket, baseKey))
          } else {
            None
          }
      }
    } catch {
      case e @ (_: IOException | _: RuntimeException) =>
        Some(unverifiableValueReason(bucket, baseKey, e))
    }
  }

  /**
   * First reason any bucket among `uris` cannot faithfully forward every [[AllS3ConfigKeys]]
   * option to native, or `None` when every key's Hadoop-effective and native-effective value
   * agrees for every S3/S3A bucket referenced. Only `s3`/`s3a` authorities matter here (ABFS/WASB
   * mooted by the userinfo gate, GCS handled by [[gcsHadoopOnlyAuthReason]]). One comparator
   * replaces the former per-case gate family (long-form bucket credentials, JCEKS/provider
   * shadowing, Hadoop `${...}` variable references): [[s3KeyDivergenceReason]] computes Hadoop's
   * effective value against a per-bucket [[propagateBucketOptions]] replica (matching
   * `S3AFileSystem#initialize`'s real propagate-then-resolve order), so a `${...}` reference that
   * resolves identically under that propagated view and under native's unpropagated forwarding is
   * no longer a divergence at all, while one that resolves differently (e.g. because propagation
   * shadowed a referenced key with a per-bucket override) IS still caught.
   * [[s3KeyDivergenceReason]] resolves each key through the consumer family
   * [[S3ConfigKeyConsumers]] declares for it, right beside the key itself: the SSE-C
   * long-bucket-alias bypass came from a key's resolution being decided implicitly, scattered
   * across call sites, so the classification is now a single visible list -- and the tier must
   * MATCH the key's real consumer in both directions, because an equality comparator resolving a
   * plain-get key through the wider `lookupPassword` cascade can manufacture a false EQUALITY
   * (long-form alias equal to native's value, true propagated plain value different) just as
   * readily as a false divergence. Never interpolates a resolved value, only key names.
   */
  private[delta] def s3ConfigDivergenceReason(
      hadoopConf: Configuration,
      uris: Seq[URI],
      propagatedConfCache: MutableMap[String, Configuration] = MutableMap.empty)
      : Option[String] = {
    val buckets = uris.flatMap(s3Bucket).distinct
    buckets.foldLeft(Option.empty[String]) { (declined, bucket) =>
      if (declined.isDefined) {
        declined
      } else {
        try {
          val propagatedConf =
            propagatedConfCache.getOrElseUpdate(
              bucket,
              propagateBucketOptions(hadoopConf, bucket))
          S3ConfigKeyConsumers.foldLeft(Option.empty[String]) {
            case (keyDeclined, (key, consumer)) =>
              if (keyDeclined.isDefined) {
                keyDeclined
              } else {
                s3KeyDivergenceReason(hadoopConf, propagatedConf, bucket, key, consumer)
              }
          }
        } catch {
          case e @ (_: IOException | _: RuntimeException) =>
            Some(unverifiableValueReason(bucket, AllS3ConfigKeys.head, e))
        }
      }
    }
  }

  /**
   * String-literal mirror of every credential-provider class name s3.rs's
   * `build_aws_credential_provider_metadata` recognizes (Hadoop S3A plus AWS SDK v1/v2 names).
   * `hadoop-aws` is NOT on this module's runtime classpath, so these stay string literals, never
   * `classOf` references.
   */
  private val SupportedCredentialProviderClasses: Set[String] = Set(
    "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider",
    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
    "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider",
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

  private val AnonymousCredentialProviderClasses: Set[String] = Set(
    "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider",
    "software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider",
    "com.amazonaws.auth.AnonymousAWSCredentials")

  private val HadoopAssumedRoleProviderClass =
    "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider"

  private val AwsCredentialsProviderKey = "fs.s3a.aws.credentials.provider"
  private val AssumedRoleCredentialsProviderKey = "fs.s3a.assumed.role.credentials.provider"

  /** Splits a comma-separated credential-provider-class list the same way s3.rs's parser does. */
  private def parseProviderClassNames(value: String): Seq[String] =
    value.split(",").map(_.trim).filter(_.nonEmpty).toSeq

  private def unsupportedProviderReason(bucket: String, key: String, className: String): String =
    s"Native Delta scan does not support the credential provider class $className " +
      s"configured via $key for $bucket (the native S3 client only supports a fixed set of " +
      "provider classes; an unsupported class would fail at scan execution time, after the " +
      "scan was already claimed, rather than at planning time)"

  private def mixedAnonymousProviderReason(bucket: String, key: String): String =
    s"Native Delta scan does not support $key for $bucket naming an anonymous credential " +
      "provider together with any other provider (the native S3 client rejects this " +
      "combination at scan execution time)"

  private def anonymousAssumedRoleProviderReason(bucket: String, key: String): String =
    s"Native Delta scan does not support an anonymous credential provider in $key for " +
      s"$bucket (the native S3 client does not allow an anonymous provider as the base " +
      "credentials for an assumed-role chain)"

  private def unsupportedProviderNameReason(
      bucket: String,
      key: String,
      names: Seq[String]): Option[String] =
    names
      .find(name => !SupportedCredentialProviderClasses.contains(name))
      .map(unsupportedProviderReason(bucket, key, _))

  /**
   * [[shortThenGlobal]] for `key` under `bucket`, or `Left(reason)` when `Configuration#get`
   * itself raises: Hadoop throws `IllegalStateException` once `${...}` expansion recurses past
   * its `MAX_SUBST` bound, e.g. a mutual reference cycle between two provider keys. Every
   * provider-class read below goes through this wrapper so the exception is caught here,
   * whichever entry point runs first. Reuses [[unverifiableValueReason]]'s message shape (names
   * the key and the exception class, never a value).
   */
  private def shortThenGlobalOrReason(
      hadoopConf: Configuration,
      bucket: String,
      key: String): Either[String, Option[String]] =
    try {
      Right(shortThenGlobal(hadoopConf, bucket, key))
    } catch {
      case e @ (_: IOException | _: RuntimeException) =>
        Left(unverifiableValueReason(bucket, key, e))
    }

  /**
   * Decline reason when `bucket`'s effective `assumed.role.credentials.provider` names an
   * unsupported class, or an anonymous one (native rejects ANY anonymous entry here, not just a
   * mix), or when reading it raises (see [[shortThenGlobalOrReason]]). Unset defaults to native's
   * own always-supported fallback, so `None` is safe.
   */
  private def assumedRoleProviderClassReason(
      hadoopConf: Configuration,
      bucket: String): Option[String] = {
    shortThenGlobalOrReason(hadoopConf, bucket, AssumedRoleCredentialsProviderKey) match {
      case Left(reason) => Some(reason)
      case Right(None) => None
      case Right(Some(value)) =>
        val names = parseProviderClassNames(value)
        unsupportedProviderNameReason(bucket, AssumedRoleCredentialsProviderKey, names).orElse {
          if (names.exists(AnonymousCredentialProviderClasses.contains)) {
            Some(anonymousAssumedRoleProviderReason(bucket, AssumedRoleCredentialsProviderKey))
          } else {
            None
          }
        }
    }
  }

  /**
   * Decline reason when `bucket`'s effective `aws.credentials.provider` names an unrecognized
   * class, mixes an anonymous provider with any other, a nested `AssumedRoleCredentialProvider`
   * sub-chain has the same problem, or reading either key raises (see
   * [[shortThenGlobalOrReason]]). Unset/empty falls back to native's default chain.
   */
  private def providerClassReason(hadoopConf: Configuration, bucket: String): Option[String] = {
    shortThenGlobalOrReason(hadoopConf, bucket, AwsCredentialsProviderKey) match {
      case Left(reason) => Some(reason)
      case Right(None) => None
      case Right(Some(value)) =>
        val names = parseProviderClassNames(value)
        unsupportedProviderNameReason(bucket, AwsCredentialsProviderKey, names)
          .orElse {
            if (names.length > 1 && names.exists(AnonymousCredentialProviderClasses.contains)) {
              Some(mixedAnonymousProviderReason(bucket, AwsCredentialsProviderKey))
            } else {
              None
            }
          }
          .orElse {
            if (names.contains(HadoopAssumedRoleProviderClass)) {
              assumedRoleProviderClassReason(hadoopConf, bucket)
            } else {
              None
            }
          }
    }
  }

  /**
   * First reason any bucket among `uris` names an unsupported credential-provider class, or
   * `None` when every named class is supported (or the key is unset). `NativeConfig` forwards
   * `Configuration#get`'s substituted value for every entry, the same read [[shortThenGlobal]]
   * performs here, so a `${...}` reference resolves identically for native and for this check.
   */
  private[delta] def providerClassGateReason(
      hadoopConf: Configuration,
      uris: Seq[URI]): Option[String] = {
    val buckets = uris.flatMap(s3Bucket).distinct
    buckets.foldLeft(Option.empty[String]) { (declined, bucket) =>
      if (declined.isDefined) declined else providerClassReason(hadoopConf, bucket)
    }
  }

  /**
   * True when `key` names a GCS authentication option under either Hadoop conf namespace the
   * `gcs-connector` reads (`fs.gs.*` or the legacy `google.cloud.*`) AND the key itself concerns
   * authentication. The connector's own `HadoopCredentialConfiguration` builds each auth setting
   * from a prefix crossed with a suffix (service-account keyfile/email/private-key, OAuth client
   * id/secret, impersonation, workload identity, and so on), including reversed-word-order
   * deprecated forms (`fs.gs.service.account.auth.keyfile`) alongside the modern ones
   * (`fs.gs.auth.service.account.json.keyfile`) -- enumerating every current and future suffix as
   * a fixed prefix list is a losing game the connector itself does not play; matching on
   * "namespace + contains auth" tracks the connector's own auth-vs-non-auth boundary instead of
   * chasing its naming history. `gcs-connector` is NOT on this module's runtime classpath by
   * default, so referencing an actual GCS auth class would risk `NoClassDefFoundError`, same
   * rationale as the S3A literals above.
   */
  private def isGcsAuthKey(key: String): Boolean =
    (key.startsWith("fs.gs.") || key.startsWith("google.cloud.")) && key.contains("auth")

  /**
   * True when `uri`'s scheme is `gs` (case-insensitive) -- the ONLY scheme object_store's
   * `ObjectStoreScheme::parse` (parquet_support.rs) routes to `GoogleCloudStorage`; `gcs` is not
   * recognized there and is deliberately excluded.
   */
  private def isGcsScheme(uri: URI): Boolean =
    Option(uri.getScheme).exists(_.equalsIgnoreCase("gs"))

  /**
   * The lowercase-scheme-checked GCS bucket name from `uri`'s authority (host, minus any userinfo
   * or port), or `None` when `uri`'s scheme is not `gs`. Parses the raw authority manually,
   * mirroring [[s3Bucket]]'s `URI#getHost`/RFC 3986 `reg-name` reasoning.
   */
  private def gcsBucket(uri: URI): Option[String] = {
    if (!isGcsScheme(uri)) {
      None
    } else {
      val authority = Option(uri.getAuthority).getOrElse("")
      val at = authority.lastIndexOf('@')
      val hostAndPort = if (at >= 0) authority.substring(at + 1) else authority
      val colon = hostAndPort.lastIndexOf(':')
      val host = if (colon >= 0) hostAndPort.substring(0, colon) else hostAndPort
      if (host.isEmpty) None else Some(host)
    }
  }

  /**
   * The non-empty Hadoop conf keys set on `hadoopConf` for which [[isGcsAuthKey]] holds, full key
   * names only -- NEVER their values, which are credential material and must never enter a
   * decline reason. Iterates the conf map directly: no provider resolution, no I/O.
   */
  private def gcsAuthKeys(hadoopConf: Configuration): Seq[String] =
    hadoopConf
      .iterator()
      .asScala
      .collect {
        case entry
            if isGcsAuthKey(entry.getKey) && entry.getValue != null &&
              entry.getValue.nonEmpty =>
          entry.getKey
      }
      .toSeq
      .distinct
      .sorted

  /**
   * Decline reason when any of `uris` resolves to a `gs://` authority AND `hadoopConf` sets any
   * key [[isGcsAuthKey]] flags, or `None` when claimable. Native forwards none of `fs.gs.*` (nor
   * any of the legacy/deprecated `google.cloud.*` namespaces) to the object store, so a scan
   * relying solely on Hadoop-side GCS credentials would claim here but then fail authentication
   * natively. Application Default Credentials work identically in both engines and need no Hadoop
   * conf key, so an ADC-only configuration still claims. Never interpolates a resolved value,
   * only key names.
   */
  private[delta] def gcsHadoopOnlyAuthReason(
      hadoopConf: Configuration,
      uris: Seq[URI]): Option[String] = {
    val gcsUris = uris.filter(isGcsScheme)
    if (gcsUris.isEmpty) {
      return None
    }
    val authKeys = gcsAuthKeys(hadoopConf)
    if (authKeys.isEmpty) {
      return None
    }
    val buckets = gcsUris.flatMap(gcsBucket).distinct.sorted
    Some(
      "Native Delta scan does not support GCS authentication configured only via Hadoop conf " +
        s"key(s) ${authKeys.mkString(", ")} for gs://${buckets.mkString(", gs://")} " +
        "(the native GCS client does not forward fs.gs.* options; only Application Default " +
        "Credentials -- environment or metadata-server -- are available natively)")
  }

  /**
   * True when `dataType` is, or structurally contains (through array elements or map keys/
   * values), a [[StructType]]. Only [[StructType]] fields carry Delta's physical, column-mapped
   * names; array/map labels themselves are never column-mapped.
   */
  private def containsNestedStruct(dataType: DataType): Boolean = dataType match {
    case _: StructType => true
    case ArrayType(elementType, _) => containsNestedStruct(elementType)
    case MapType(keyType, valueType, _) =>
      containsNestedStruct(keyType) || containsNestedStruct(valueType)
    case _ => false
  }

  /**
   * True when `node` is a positional-output union -- `UnionExec` or `CometUnionExec`. Both
   * compute output positionally from the FIRST child's attributes, so a value carried only by a
   * LATER branch needs an explicit positional walk below. Compared by class name (the
   * [[isDeltaScan]] idiom) to avoid a compile-time dependency; an unmatched name is still safe,
   * caught by the generic child-output safety net below.
   */
  private def isPositionalUnion(node: SparkPlan): Boolean = {
    val name = node.getClass.getSimpleName
    name == "UnionExec" || name == "CometUnionExec"
  }

  /**
   * True when the scan's row-index column value is provably dead above the scan. The standard DV
   * plan shape routes it only into a `named_struct(... row_index ...) AS _metadata` projection
   * whose result the final projection discards; anything else (a query actually selecting
   * `_metadata.row_index`, OR a write sink -- `DataWritingCommandExec`, `WriteFilesExec`, a DSv2
   * `V2TableWriteExec` -- persisting it) makes the value live and must decline. Conservative: any
   * unrecognized consumption pattern returns false.
   */
  private def rowIndexUnusedAbove(plan: SparkPlan, scanExec: FileSourceScanExec): Boolean = {
    val rowIndexAttrs = scanExec.output
      .filter(_.name == CometDeltaNativeScan.RowIndexColumn)
      .map(_.exprId)
      .toSet
    if (rowIndexAttrs.isEmpty) {
      return true
    }
    // Transitive taint analysis: everything derived from the row-index attribute within the
    // visible plan, via Project aliases or positionally across a union. The plan may be an AQE
    // stage fragment, so tainted values escaping to the fragment's own output must decline too.
    var tainted = rowIndexAttrs
    var changed = true
    while (changed) {
      changed = false
      plan.foreach {
        case p: ProjectExec =>
          p.projectList.foreach {
            case a: Alias
                if !tainted.contains(a.exprId) &&
                  a.references.exists(r => tainted.contains(r.exprId)) =>
              tainted += a.exprId
              changed = true
            case _ =>
          }
        case u if isPositionalUnion(u) =>
          // Output attributes carry the FIRST child's expression IDs, so a value tainted only in
          // a LATER branch is otherwise invisible; walk it forward positionally instead.
          // `children` can be re-parented by AQE after `output` is frozen, so an arity mismatch on
          // ANY child (which would make a positional zip silently truncate) forces a decline.
          if (u.children.exists(_.output.length != u.output.length)) {
            return false
          }
          u.children.foreach { child =>
            child.output.zip(u.output).foreach {
              case (from, to) if tainted.contains(from.exprId) && !tainted.contains(to.exprId) =>
                tainted += to.exprId
                changed = true
              case _ =>
            }
          }
        case _ =>
      }
    }
    val nonProjectConsumer = plan.exists {
      case _: ProjectExec => false
      case n if n ne scanExec =>
        n.expressions.exists(_.references.exists(r => tainted.contains(r.exprId)))
      case _ => false
    }
    val escapes = plan.output.exists(a => tainted.contains(a.exprId))
    // Generic safety net for every OTHER node, of ANY arity (joins and other multi-child
    // shapes, but also plain one-child nodes; positional unions and Project are exempt, already
    // handled precisely above -- Project's own output legitimately omits a tainted attribute it
    // dropped, which is not a leak). A tainted attribute a child contributes must either survive
    // into the node's own output under the SAME expression ID or be consumed by one of the
    // node's own expressions; otherwise decline. This catches two shapes: a multi-child node
    // dropping the side carrying the tainted attribute (e.g. a LEFT SEMI/ANTI join), and a
    // one-child WRITE SINK -- DataWritingCommandExec, WriteFilesExec, and the DSv2
    // AppendDataExec/OverwriteByExpressionExec/... family (V2TableWriteExec) -- that executes
    // its child purely for the side effect of persisting its rows and so has an EMPTY output of
    // its own. Such a sink neither preserves the tainted attribute (nothing survives into an
    // empty output) nor references it in an expression, so without this check it looks like an
    // inert pass-through even though the write persists whatever value the reader returned,
    // including a DV scan's dead synthetic row-index constant.
    val childOutputLeak = plan.exists {
      case u if isPositionalUnion(u) => false
      case _: ProjectExec => false
      case n if n.children.nonEmpty =>
        n.children.exists { c =>
          c.output.exists { attr =>
            tainted.contains(attr.exprId) &&
            !n.output.exists(_.exprId == attr.exprId) &&
            !n.expressions.exists(_.references.exists(_.exprId == attr.exprId))
          }
        }
      case _ => false
    }
    !nonProjectConsumer && !escapes && !childOutputLeak
  }
}
