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
import java.util.Locale

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.expressions.{Alias, ExprId, GenericInternalRow, InputFileBlockLength, InputFileBlockStart, InputFileName}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns.getExistenceDefaultValues
import org.apache.spark.sql.comet.CometScanExec
import org.apache.spark.sql.delta.DeltaParquetFileFormat
import org.apache.spark.sql.delta.actions.DeletionVectorDescriptor
import org.apache.spark.sql.execution.{FileSourceScanExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}

import org.apache.comet.CometConf
import org.apache.comet.objectstore.{NativeConfig, S3ConfigGate}
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
    // Unlike core, a required Variant root stays declined: this path lacks core's Variant gates.
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

    // file_block_start and file_block_length describe the split reading a row. For a split file,
    // DataFusion keeps a row group in the split holding its first page while Spark's reader
    // keeps it in the split holding its midpoint, so rows would report the wrong split. Core
    // declines the same two columns (https://github.com/apache/datafusion-comet/issues/6505).
    // A deletion-vector read carries Delta's whole _metadata struct, so the two columns can sit
    // in the scan output unused; only a value consumed above the scan declines.
    val consumedSplitDependentCols = scanExec.fileConstantMetadataColumns
      .filter(a => Set("file_block_start", "file_block_length").contains(a.name))
      .filterNot(a => attributesUnusedAbove(plan, scanExec, Set(a.exprId)))
    if (consumedSplitDependentCols.nonEmpty) {
      return Some(
        "Native Delta scan does not support metadata column(s) " +
          consumedSplitDependentCols.map(_.name).mkString(", "))
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
    // actually read (mirrors core's unsupportedFsSchemes gate). A scheme the reader options add
    // to the libhdfs list is declined; see NativeConfig.resolveScanLibhdfsSchemes.
    val libhdfsSchemes = NativeConfig.resolveScanLibhdfsSchemes(
      scanExec.relation.sparkSession.sessionState.newHadoopConf(),
      hadoopConf)
    val libhdfs = libhdfsSchemes.libhdfs
    val scanOnlyRoot = scanOnlyLibhdfsSchemeReason(rootUris, libhdfsSchemes.scanOnly)
    if (scanOnlyRoot.isDefined) {
      return scanOnlyRoot
    }
    val unsupportedRootSchemes = unsupportedSchemes(rootUris, libhdfs)
    if (unsupportedRootSchemes.nonEmpty) {
      return Some(
        "Native Delta scan does not support filesystem scheme(s) " +
          s"${unsupportedRootSchemes.mkString(", ")}")
    }

    // A recognized scheme can still carry a path object_store rejects (a directory name with a
    // newline surfaces as `%0A`), which native planning hard-fails on while Spark's reader opens
    // it. Mirrors core's root-path gate; the complete selected paths are probed below.
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
    val scanOnlySelected =
      scanOnlyLibhdfsSchemeReason(dataFileUris ++ dvUris, libhdfsSchemes.scanOnly)
    if (scanOnlySelected.isDefined) {
      return scanOnlySelected
    }
    val unsupportedSelected = unsupportedSelectedSchemeReason(dataFileUris ++ dvUris, libhdfs)
    if (unsupportedSelected.isDefined) {
      return unsupportedSelected
    }

    // Same path probe for every complete selected path, not just its directory: a shallow
    // clone's source can sit outside this root, and CONVERT TO DELTA keeps the source Parquet
    // basenames, so the rejected character can be in the file name itself. The probe is a
    // native URL parse with no I/O, so once per distinct URI costs less than the scan's own
    // per-file parse.
    val rejectedSelected = objectStoreRejectedPathReason(dataFileUris ++ dvUris, libhdfs)
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

    // Zero-I/O, conf-only, like the GCS gate above: every S3 setting whose resolution differs
    // between Comet's native S3 client and Hadoop's S3AFileSystem (unsupported encryption
    // algorithms, HTTP proxy, assumed-role session policy, scheme-less/STS endpoints, credential
    // and bucket-option divergence, credential-provider classes) declines here rather than
    // letting a claimed scan read under a different identity or endpoint than Spark's. Shared
    // with core's native Parquet scan, which applies the same gate when it forwards the options.
    val s3Reason = S3ConfigGate.declineReason(hadoopConf, dataFileUris ++ dvUris)
    if (s3Reason.isDefined) {
      return s3Reason
    }

    // Reuse core's generic native-scan gates (ignoreCorruptFiles/ignoreMissingFiles, AQE DPP on
    // Spark 3.4, exec enabled, existence default values, the Variant read confs, and a proto
    // representation for every serialized data and partition type); tags its own fallback
    // reasons.
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
   * Decline reason when any of `uris` uses a scheme opted in as an S3-compliant alias through
   * `fs.comet.s3Compliant.schemes` (e.g. `blob`), or `None`. Core's native Parquet scan admits
   * such a scheme and reads it through its S3 client, with `NativeConfig` translating the vendor
   * `fs.<scheme>.<authority>.*` keys into `fs.s3a.bucket.*` options. Spark, however, reads the
   * same table through the vendor's own Hadoop FileSystem, not `S3AFileSystem`, and every S3
   * divergence gate ([[org.apache.comet.objectstore.S3ConfigGate]]) is verified
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
   * Decline reason when any of `uris` uses a scheme in `scanOnly`, one listed in
   * `fs.comet.libhdfs.schemes` only through the scan's reader options, or `None`. Native would
   * route it to libhdfs, which never sees reader options.
   */
  private[delta] def scanOnlyLibhdfsSchemeReason(
      uris: Seq[URI],
      scanOnly: Set[String]): Option[String] = {
    val schemes = uris.flatMap(NativeConfig.lowerScheme).filter(scanOnly.contains).distinct
    if (schemes.isEmpty) {
      None
    } else {
      Some(
        "Native Delta scan does not support filesystem scheme(s) " +
          s"${schemes.sorted.mkString(", ")} listed in " +
          s"${CometConf.COMET_LIBHDFS_SCHEMES_KEY} only through reader options")
    }
  }

  /**
   * Decline reason naming the first of `uris` whose path object_store rejects even though it
   * recognizes the scheme ([[CometScanRule.objectStoreAcceptsPath]], e.g. a directory name
   * containing a newline, `%0A` in the URI), or `None`. Schemes in `libhdfs` never reach
   * object_store's path parser and are skipped, as is a `null` scheme (see
   * [[unsupportedSchemes]]); an S3-compliant alias is declined before this gate runs. The probe
   * is uncached but is a plain native URL parse with no I/O
   * ([[CometScanRule.objectStoreAcceptsPath]]), so callers pass every complete selected path
   * (root paths, data files and deletion vectors) once per distinct URI; a converted table can
   * carry the rejected character in a file basename. The reason masks any userinfo in the named
   * URI ([[redactedAuthority]]).
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
   * mirroring [[S3ConfigGate.s3Bucket]]'s `URI#getHost`/RFC 3986 `reg-name` reasoning.
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
  private def rowIndexUnusedAbove(plan: SparkPlan, scanExec: FileSourceScanExec): Boolean =
    attributesUnusedAbove(
      plan,
      scanExec,
      scanExec.output.filter(_.name == CometDeltaNativeScan.RowIndexColumn).map(_.exprId).toSet)

  /**
   * True when the values of the scan output attributes `attrIds` are provably dead above the
   * scan, by the analysis described on [[rowIndexUnusedAbove]].
   */
  private def attributesUnusedAbove(
      plan: SparkPlan,
      scanExec: FileSourceScanExec,
      attrIds: Set[ExprId]): Boolean = {
    if (attrIds.isEmpty) {
      return true
    }
    // Transitive taint analysis: everything derived from the seed attributes within the
    // visible plan, via Project aliases or positionally across a union. The plan may be an AQE
    // stage fragment, so tainted values escaping to the fragment's own output must decline too.
    var tainted = attrIds
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
