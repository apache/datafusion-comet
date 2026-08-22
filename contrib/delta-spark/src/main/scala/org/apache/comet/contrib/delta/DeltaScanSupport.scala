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
import org.apache.comet.parquet.CometParquetUtils
import org.apache.comet.rules.CometScanRule
import org.apache.comet.serde.operator.CometNativeScan
import org.apache.comet.shims.ShimFileFormat

/**
 * Claim/decline gates for the native Delta scan. Correctness rule: when in doubt, decline,
 * Spark's Delta reader handles the scan and results stay correct, just unaccelerated.
 */
object DeltaScanSupport {

  /**
   * Reader features the native path understands. Anything else on the protocol declines the
   * table. Note `deletionVectors` and `columnMapping` are declined separately (below) so their
   * fallback reasons are specific.
   */
  private val understoodReaderFeatures: Set[String] =
    Set("columnMapping", "deletionVectors", "timestampNtz", "v2Checkpoint", "vacuumProtocolCheck")

  /**
   * Is this exactly Delta's DSv1 parquet format (not a further subclass)? Compared by class name,
   * not `classOf`, deliberately: this is the first gate on every V1 scan, and it must stay inert
   * when the contrib jar is deployed without delta-spark on the classpath:
   * `classOf[DeltaParquetFileFormat]` here raises NoClassDefFoundError inside CometScanRule and
   * takes down every parquet scan in the session. When the name matches, delta-spark is
   * necessarily present (the instance exists), so the Delta types past this gate are safe.
   */
  def isDeltaScan(scanExec: FileSourceScanExec): Boolean =
    scanExec.relation.fileFormat.getClass.getName ==
      "org.apache.spark.sql.delta.DeltaParquetFileFormat"

  /**
   * Returns the first reason this Delta scan cannot go native, or None when it is claimable. Only
   * called when [[isDeltaScan]] is true. `scanHelper` is the same [[CometScanExec]] the caller
   * builds to drive [[CometDeltaNativeScan.convert]] on a claim, reused here (rather than listed
   * separately) to resolve the scan's selected files for the multi-store gate below.
   */
  def declineReason(
      plan: SparkPlan,
      scanExec: FileSourceScanExec,
      scanHelper: CometScanExec): Option[String] = {
    val format = scanExec.relation.fileFormat.asInstanceOf[DeltaParquetFileFormat]
    val protocol = format.protocol
    val metadata = format.metadata
    // Descriptor deserialization is the expensive part of both consumers below (the DV
    // cardinality gate and the store-identity collision gate); hoisted once here so it runs at
    // most once per claim attempt regardless of how many gates end up needing it. `lazy` because
    // most scans are not DV-shaped and selectedDvDescriptors short-circuits to Seq.empty for
    // them, but paying even that check is unnecessary work for gates that return earlier.
    val tableRoot = scanExec.relation.location.rootPaths.head.toString
    lazy val dvDescriptors: Seq[DeletionVectorDescriptor] =
      selectedDvDescriptors(scanHelper, tableRoot)

    if (format.isCDCRead) {
      return Some("Native Delta scan does not support Change Data Feed reads")
    }

    // Delta's DML machinery (findTouchedFiles with useMetadataRowIndex=false) injects a
    // generated row-index column directly into the data schema and disables reader
    // optimizations; its values must come from Spark's reader. Claiming such a scan would
    // feed NULL row indexes into deletion-vector construction, silently corrupting DML.
    if (!format.optimizationsEnabled) {
      return Some("Native Delta scan does not support reads with reader optimizations disabled")
    }
    if (scanExec.requiredSchema.exists(_.name == DeltaParquetFileFormat.ROW_INDEX_COLUMN_NAME) ||
      scanExec.relation.dataSchema.exists(
        _.name == DeltaParquetFileFormat.ROW_INDEX_COLUMN_NAME)) {
      return Some("Native Delta scan does not support Delta's generated row-index column")
    }

    // Name mode is supported by serializing physical-name schemas (the parquet reader then
    // matches file columns by name natively). Id mode needs the field-id path and stays
    // declined until validated.
    val cmMode = metadata.columnMappingMode.name
    if (cmMode != "none" && cmMode != "name") {
      return Some(s"Native Delta scan does not support column mapping mode $cmMode")
    }
    // createPhysicalSchema wholesale-replaces field metadata, silently dropping
    // EXISTS_DEFAULT; decline any column defaults under column mapping rather than
    // return nulls where a default belongs.
    if (cmMode == "name" &&
      getExistenceDefaultValues(scanExec.requiredSchema).exists(_ != null)) {
      return Some(
        "Native Delta scan does not support column defaults together with column mapping")
    }
    // createPhysicalSchema rewrites nested StructField names (not just top-level column
    // names) to their physical, column-mapped form, and the shared native builder uses the
    // required schema verbatim as the scan's output schema: struct fields below the top level
    // would carry physical names. Ordinal access (GetStructField) is unaffected, but
    // name-sensitive native expressions (e.g. to_json) read the Arrow struct field names
    // directly and would leak physical names into query results. Restoring logical names
    // natively needs a rename adapter/proto field for the logical schema (follow-up); decline
    // until then.
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

    // Non-constant metadata columns are generated per-row by Spark's reader and not
    // supported, except Delta's DV bookkeeping columns, which the native path emits as
    // constants (correct by construction once the DV is applied in the reader).
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
      // A row-index column WITHOUT is_row_deleted is not a DV read: it is Delta DML
      // bookkeeping (findTouchedFiles building deletion bitmaps from REAL row indexes).
      // Claiming it with constant row indexes would corrupt the DVs being written.
      val hasIsRowDeleted =
        scanExec.requiredSchema.exists(_.name == CometDeltaNativeScan.IsRowDeletedColumn)
      val hasRowIndex =
        scanExec.requiredSchema.exists(_.name == CometDeltaNativeScan.RowIndexColumn)
      if (hasRowIndex && !hasIsRowDeleted) {
        return Some(
          "Native Delta scan does not support row-index reads outside a deletion-vector scan")
      }
      // The internal columns must form a suffix of the read schema so data-column
      // positions agree between Spark's output and the stripped native schema.
      val names = scanExec.requiredSchema.fields.map(_.name)
      val firstInternal = names.indexWhere(CometDeltaNativeScan.internalColumnNames.contains)
      if (!names.drop(firstInternal).forall(CometDeltaNativeScan.internalColumnNames.contains)) {
        return Some("Native Delta scan requires DV bookkeeping columns to trail the read schema")
      }
      // The row-index column's real values are consumed inside the reader when Spark applies
      // the DV; native applies the DV itself and emits a dead constant instead, so the value
      // must be provably unused above the scan (beyond the _metadata reassembly that gets
      // discarded).
      if (!rowIndexUnusedAbove(plan, scanExec)) {
        return Some(
          "Native Delta scan cannot supply _metadata.row_index values consumed by the query")
      }
      // The DV common builder does not serialize existence defaults yet; decline rather
      // than silently return nulls for backfilled columns in old files.
      if (getExistenceDefaultValues(scanExec.requiredSchema).exists(_ != null)) {
        return Some(
          "Native Delta scan does not support column defaults together with deletion vectors")
      }
      // Bound the memory the native side will retain for expanded DV row selectors before
      // committing to native execution: applying a deletion vector expands it into per-row
      // RowSelectors that are reserved against the execution memory pool at scan time (see
      // delta_dv.rs), and an alternating deleted/retained bitmap produces one non-coalescing
      // selector per row. A row group's selector count is bounded above by
      // 2*cardinality + #row-groups (each deleted row splits at most one run into a
      // select/skip pair, plus one selector per row-group boundary), so the descriptor's
      // cardinality -- deserialized at planning time via selectedDvDescriptors, no bitmap
      // decode needed -- is a sound, pessimistic upper bound on the native reservation.
      // Pessimistic by design: a large but CONTIGUOUS deletion is declined the same as a
      // large alternating one, even though it would retain far fewer selectors natively; the
      // conf below makes that recoverable.
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

    // input_file_name & friends read from InputFileBlockHolder, a thread-local set by Spark's
    // FileScanRDD; the native scan does not populate it. Delta's own DELETE/UPDATE/MERGE
    // find-touched-files scans use input_file_name, so this gate is load-bearing for DML
    // correctness (mirrors core's check in CometScanRule.nativeScan).
    if (plan.exists(node =>
        node.expressions.exists(_.exists {
          case _: InputFileName | _: InputFileBlockStart | _: InputFileBlockLength => true
          case _ => false
        }))) {
      return Some(
        "Native Delta scan is not compatible with input_file_name, " +
          "input_file_block_start, or input_file_block_length")
    }

    // Row-index metadata columns are generated per-row by Spark's reader (mirrors core).
    // The DV shape's trailing row-index column is exempt: the gates above already proved its
    // values are dead and the native path emits a constant for it.
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

    // Decline ALL encrypted-parquet configurations (stricter than core): the exec node does
    // not yet wire the decryption-key broadcast to executors, so claiming even a
    // supported-encryption scan would fail at execution.
    val hadoopConf = scanExec.relation.sparkSession.sessionState
      .newHadoopConfWithOptions(scanExec.relation.options)
    if (CometParquetUtils.encryptionEnabled(hadoopConf)) {
      return Some("Native Delta scan does not support encrypted parquet")
    }

    // Nested-type column defaults (schema-evolution backfill of map/struct/array columns)
    // cannot be serialized; a silently-dropped default would misalign the value/index lists
    // consumed positionally on the native side. Mirrors core's transformV1Scan gate.
    val possibleDefaultValues = getExistenceDefaultValues(scanExec.requiredSchema)
    if (possibleDefaultValues.exists(d =>
        d != null && (d.isInstanceOf[ArrayBasedMapData] || d
          .isInstanceOf[GenericInternalRow] || d.isInstanceOf[GenericArrayData]))) {
      return Some("Native Delta scan does not support default values for nested types")
    }

    // Only claim scans whose root paths object_store (or the configured libhdfs schemes) can
    // actually read; otherwise a custom Hadoop FileSystem would fail at execution instead of
    // falling back gracefully. Mirrors core's unsupportedFsSchemes gate.
    val libhdfs = libhdfsSchemes
    val unsupportedRootSchemes =
      unsupportedSchemes(scanExec.relation.location.rootPaths.map(_.toUri), libhdfs)
    if (unsupportedRootSchemes.nonEmpty) {
      return Some(
        "Native Delta scan does not support filesystem scheme(s) " +
          s"${unsupportedRootSchemes.mkString(", ")}")
    }

    // A Delta shallow clone across buckets followed by an append is a valid table whose data
    // files span multiple object-store authorities. The shared native scan builder resolves
    // the whole scan's ObjectStoreUrl from the FIRST selected file only and then strips every
    // other file down to its bare object-store path, so a later file under a different store
    // would silently read through the first file's store handle -- normally a NoSuchKey, but
    // the wrong data if a same-named key happens to exist in both stores. Force file listing
    // here (scanHelper is already built for the claim path, so this is not extra work) and
    // decline rather than risk it.
    val dataFileUris =
      scanHelper.selectedPartitions.iterator.flatMap(_.files).map(_.getPath.toUri).toSeq

    // Hoisted next to dataFileUris: both the selected-file scheme gate immediately below and the
    // store-identity collision gate further down need the DV absolute-path URIs, and
    // dvDescriptors is already a memoized lazy val that short-circuits to Seq.empty for non-DV
    // shapes, so computing dvUris here costs nothing extra for scans that never reach it.
    val dvUris = dvDescriptors
      .filter(_.storageType != DeletionVectorDescriptor.INLINE_DV_MARKER)
      .map(_.absolutePath(new Path(tableRoot)).toUri)

    // The root-path gate above only inspects the table's root(s); a valid Delta shallow clone
    // can have a supported root (e.g. `file:`) while its *selected* data files or deletion
    // vectors resolve through a scheme the native side cannot read (e.g. `viewfs:`, reachable
    // even though the table root itself never carries that scheme). Checked BEFORE
    // multiStoreReason/userInfoBearingAuthorityReason below, deliberately: an unreadable scheme
    // is a stronger, more actionable decline than "spans multiple stores" -- the authority gates
    // below presume every URI they compare is natively resolvable in the first place, which
    // isn't true here -- and this ordering also catches a mixed `file:`+`viewfs:` selection for
    // the right (scheme) reason rather than whatever the authority gates would say about it.
    val unsupportedSelected = unsupportedSelectedSchemeReason(dataFileUris ++ dvUris, libhdfs)
    if (unsupportedSelected.isDefined) {
      return unsupportedSelected
    }

    // Checked BEFORE multiStoreReason, for the same reason the scheme gate above runs first:
    // multiStoreReason presumes every URI it compares is natively resolvable to a single store
    // identity in the first place, and a userinfo-bearing authority provably is not -- the
    // native object-store cache, ObjectStoreUrl, and DataFusion registry all key on scheme, host
    // and port only (parquet_support.rs's url_key), dropping userinfo entirely, so two
    // authorities differing only in userinfo (e.g. two containers on one ABFS storage account)
    // collapse onto the SAME store handle regardless of how many distinct authorities the scan's
    // data files span. This also covers the narrower case a shallow clone + DELETE can produce --
    // every data file staying under one container while only the new deletion-vector sidecar
    // lands under another -- without needing a second, authority-collision-specific gate: any
    // userinfo at all is unsafe here, so this is a stronger, more actionable decline than
    // "spans multiple stores" and gives an Azure user one stable reason regardless of how many
    // containers the scan happens to touch.
    val userInfoReason = userInfoBearingAuthorityReason(dataFileUris ++ dvUris)
    if (userInfoReason.isDefined) {
      return userInfoReason
    }

    val multiStore = multiStoreReason(dataFileUris)
    if (multiStore.isDefined) {
      return multiStore
    }

    // Credentials that resolve ONLY through a Hadoop credential provider (JCEKS et al.) are
    // invisible to the plain-conf extraction this contrib forwards to the native S3 client (see
    // credentialAliasReason's doc); reuses the hadoopConf built for the encryption gate above.
    val credentialReason = credentialAliasReason(hadoopConf, dataFileUris ++ dvUris)
    if (credentialReason.isDefined) {
      return credentialReason
    }

    // Reuse core's generic native-scan gates (ignoreCorruptFiles/ignoreMissingFiles,
    // AQE DPP on Spark 3.4, exec enabled). This tags its own fallback reasons.
    if (!CometNativeScan.isSupported(scanExec)) {
      return Some("Core native scan gates rejected the scan (see reasons above)")
    }

    None
  }

  /**
   * Deletion-vector descriptors for every file this DV-shape scan selected, deserialized once at
   * planning time and normalized to absolute on-disk paths via `copyWithAbsolutePath` (a no-op
   * for inline and already-absolute descriptors), so callers never need `tableRoot` again to
   * resolve a UUID-relative sidecar. Returns `Seq.empty` for the plain shape
   * ([[CometDeltaNativeScan.isDvShape]] false on the wrapped scan): only DV reads carry the
   * row-index-filter metadata this deserializes.
   *
   * Shared plumbing: finding 8's cross-authority object-store option merge
   * ([[CometDeltaNativeScan.convert]]) and finding 3's DV cardinality decline gate both need
   * every selected file's descriptor; this is the one planning-time deserialization pass for both
   * consumers.
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
   * lowercased and defaulting to `Set("hdfs")` when unset. Hoisted out of the root-path scheme
   * gate in [[declineReason]] so the same parsed set can be reused, without re-parsing the conf,
   * by the selected-file scheme gate below.
   */
  private[delta] def libhdfsSchemes: Set[String] = COMET_LIBHDFS_SCHEMES.get() match {
    case Some(s) =>
      s.split(",").map(_.trim.toLowerCase(Locale.ROOT)).filter(_.nonEmpty).toSet
    case None => Set("hdfs")
  }

  /**
   * The lowercased, deduplicated schemes among `uris` that neither `libhdfs` nor Comet's native
   * object_store layer ([[CometScanRule.isNativelyReadableScheme]]) can read. A `null` scheme (no
   * scheme component at all) is tolerated, not flagged: such a URI cannot come from a
   * Hadoop-backed source in the first place, so there is nothing to gate. Factored out of
   * [[declineReason]]'s two scheme gates (table roots, and selected data files/deletion vectors)
   * so both share one predicate and it is directly unit-testable without a Spark session.
   */
  private[delta] def unsupportedSchemes(uris: Seq[URI], libhdfs: Set[String]): Set[String] = {
    uris
      .filter { uri =>
        val sch = uri.getScheme
        sch != null && {
          val sl = sch.toLowerCase(Locale.ROOT)
          !libhdfs.contains(sl) && !CometScanRule.isNativelyReadableScheme(uri)
        }
      }
      .map(_.getScheme.toLowerCase(Locale.ROOT))
      .toSet
  }

  /**
   * Returns a decline reason when any of `uris` -- the scan's selected data-file and deletion-
   * vector URIs -- use a filesystem scheme [[unsupportedSchemes]] flags, or `None` when every URI
   * is natively readable (or libhdfs-exempt). Factored out of [[declineReason]] (see its call
   * site for the ordering rationale relative to [[userInfoBearingAuthorityReason]] and
   * [[multiStoreReason]]) so it is directly unit-testable without a Spark session, mirroring how
   * those two gates are factored below.
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
   * Returns a decline reason when `uris` span more than one object-store authority (scheme plus
   * the URI's raw authority component -- userinfo, host, and port together -- all lowercased so
   * e.g. `S3A://Bucket:1234` and `s3a://bucket:1234` collapse to the same authority), or `None`
   * when every URI shares a single authority. `file://` paths never carry an authority, so purely
   * local scans across any number of distinct directories are unaffected. Factored out of
   * [[declineReason]] so it is directly unit-testable without a Spark session.
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
   * Normalizes `uri` to a lowercased `scheme://authority` string, keyed on the URI's raw
   * `getAuthority` rather than the individually-parsed host/port/userinfo fields. Two pitfalls
   * that motivate this:
   *   - `getAuthority` already includes userinfo (e.g. the container in
   *     `abfss://container@account.dfs.core.windows.net`), so two containers on the same storage
   *     account no longer collapse into one authority the way `getHost` alone would.
   *   - `getHost` (and `getUserInfo`/`getPort`) return `null` for the *entire* authority when it
   *     doesn't conform to RFC 3986's `reg-name` syntax -- e.g. an underscore in a GCS bucket
   *     name (`gs://my_bucket`) -- silently collapsing distinct buckets into the same empty-host
   *     key. `getAuthority` returns the raw authority text regardless of RFC conformance, so it
   *     stays accurate for exactly the URIs where the structured getters fail.
   *
   * A `null` authority (schemes with no authority component, e.g. `file:///tmp/x`) normalizes to
   * the empty string.
   */
  private[delta] def uriAuthority(uri: URI): String = {
    val scheme = Option(uri.getScheme).map(_.toLowerCase(Locale.ROOT)).getOrElse("")
    val authority = Option(uri.getAuthority).map(_.toLowerCase(Locale.ROOT)).getOrElse("")
    s"$scheme://$authority"
  }

  /**
   * The raw userinfo component of `uri`'s authority (e.g. the container in
   * `abfss://container@account.dfs.core.windows.net`), or the empty string when the authority
   * carries none. Splits the raw authority text at the LAST `@` rather than using
   * `URI#getUserInfo` -- as with [[uriAuthority]], the structured getters (`getUserInfo`,
   * `getHost`, `getPort`) return `null` for the WHOLE authority when it doesn't conform to RFC
   * 3986's `reg-name` syntax (e.g. an underscore in a GCS bucket name, `gs://my_bucket`), which
   * would silently hide a real userinfo component on an otherwise-valid-looking authority. The
   * last `@` is always exactly the userinfo delimiter in a URI authority's grammar regardless of
   * reg-name conformance. Never lowercased: userinfo is case-sensitive, and is exactly the
   * component the native object-store cache, `ObjectStoreUrl`, and DataFusion registry all drop
   * when keying their store handle (`url_key` in `parquet_support.rs`) -- which is what makes ANY
   * non-empty userinfo here provably unsafe for a native scan; see
   * [[userInfoBearingAuthorityReason]].
   */
  private[delta] def uriUserInfo(uri: URI): String = {
    val authority = Option(uri.getAuthority).getOrElse("")
    val at = authority.lastIndexOf('@')
    if (at >= 0) authority.substring(0, at) else ""
  }

  /**
   * Redacts `uri`'s authority to `scheme` + `://` + a literal `***` + `@host[:port]` for
   * embedding in a decline reason: userinfo replaced with a literal `***`, scheme lowercased,
   * host/port copied verbatim (raw, not lowercased) from the authority text following the same
   * last-`@` split as [[uriUserInfo]]. NEVER interpolate `uri.getAuthority` or [[uriUserInfo]]
   * directly into a reason string -- doing so would leak credentials embedded as URI userinfo
   * (e.g. the access/secret key pair in `s3a://AKIA...:secret@bucket`) into the SQL plan's
   * explain output, fallback-reason logging, and the Spark UI.
   */
  private[delta] def redactedAuthority(uri: URI): String = {
    val scheme = Option(uri.getScheme).map(_.toLowerCase(Locale.ROOT)).getOrElse("")
    val authority = Option(uri.getAuthority).getOrElse("")
    val at = authority.lastIndexOf('@')
    val hostPort = if (at >= 0) authority.substring(at + 1) else authority
    s"$scheme://***@$hostPort"
  }

  /**
   * Returns a decline reason when any of `uris` -- the scan's selected data-file and deletion-
   * vector URIs -- carries userinfo in its authority (e.g. the container in
   * `abfss://container@account.dfs.core.windows.net`), or `None` when none do. The native
   * object-store cache, `ObjectStoreUrl`, and DataFusion registry all key their store handle on
   * scheme, host, and port only (`url_key` in `parquet_support.rs`) -- userinfo is dropped
   * entirely -- so a userinfo-bearing authority is provably unsafe to claim: a second scan (or,
   * within one scan, a deletion-vector sidecar left under a different container after a Delta
   * shallow clone plus a DELETE) whose authority differs only in userinfo would silently collide
   * with, and be served through, this scan's cached store handle -- normally a missing-object
   * error, but a same-named object under both identities would silently return the wrong data.
   * REPLACES the narrower storeIdentityKey/storeIdentityCollisionReason pairing this contrib used
   * to carry: declining on ANY userinfo, scheme-agnostically, is a strict superset of the cases
   * that narrower pairing could ever fire on (a store-identity-key collision requires at least
   * one of the colliding URIs to carry userinfo in the first place), so nothing that pairing
   * caught is missed here, and its cross-authority interpolation of full, unredacted authorities
   * -- including any embedded userinfo credentials -- into the fallback reason no longer exists.
   * Factored out of [[declineReason]] (see its call site for the ordering rationale relative to
   * [[multiStoreReason]]) so it is directly unit-testable without a Spark session.
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
   * String-literal Hadoop conf keys consulted by [[credentialAliasReason]] below. `hadoop-aws` is
   * NOT on this module's runtime classpath (`spark-hadoop-cloud` is test-scope only, per the
   * contrib pom), so `org.apache.hadoop.fs.s3a.Constants` must never be referenced here: doing so
   * would raise `NoClassDefFoundError` inside this claim/decline gate the first time it runs in a
   * session with no S3 dependency on the classpath at all, taking down every Delta scan in that
   * session, not just S3 ones.
   */
  private val HadoopCredentialProviderPathKey = "hadoop.security.credential.provider.path"
  private val S3aCredentialProviderPathKey = "fs.s3a.security.credential.provider.path"

  private def s3aBucketProviderPathKey(bucket: String): String =
    s"fs.s3a.bucket.$bucket.security.credential.provider.path"

  /**
   * The LONG form of [[s3aBucketProviderPathKey]]: `S3AUtils#lookupPassword` (Hadoop 3.4.1)
   * resolves every per-bucket override through both a long key (`fs.s3a.bucket.B.<full base
   * key>`, i.e. the base key with the `fs.s3a.bucket.B.` prefix simply prepended) and a short key
   * (`fs.s3a.bucket.B.<base key minus its "fs.s3a." prefix>`) -- see [[s3aCredentialAliases]]
   * below for why both forms must be covered here too.
   */
  private def s3aBucketLongProviderPathKey(bucket: String): String =
    s"fs.s3a.bucket.$bucket.fs.s3a.security.credential.provider.path"

  /**
   * The aliases the native S3 credentials provider chain tries, per bucket, in the SAME order
   * native code uses (`s3.rs`'s per-bucket-then-global resolution): a per-bucket override, if
   * present, wins over the global one. This is the union of what native actually reads (short
   * bucket form + global) PLUS the LONG bucket form Hadoop's own `S3AUtils#lookupPassword`
   * (Hadoop 3.4.1) also consults: for each base key (e.g. `fs.s3a.access.key`) it builds
   * `longBucketKey = fs.s3a.bucket.B.fs.s3a.access.key` and `shortBucketKey =
   * fs.s3a.bucket.B.access.key`, resolves the long key FIRST, lets the short key OVERRIDE it when
   * set, and only then falls back to the global key. A JCEKS entry that exists ONLY under the
   * long alias is therefore resolved by Hadoop's own S3A credential lookup but is invisible to
   * native's short-bucket-and-global-only extraction -- a provider-shadowed value under ANY alias
   * in this list means native's plain-conf resolution diverges from Hadoop's, so the caller must
   * decline. List order only affects which alias name appears in the decline reason, never
   * whether one is returned: [[verifyGlobalProviderAliases]] checks every alias independently.
   */
  private def s3aCredentialAliases(bucket: String): Seq[String] =
    Seq(
      s"fs.s3a.bucket.$bucket.fs.s3a.access.key",
      s"fs.s3a.bucket.$bucket.fs.s3a.secret.key",
      s"fs.s3a.bucket.$bucket.fs.s3a.session.token",
      s"fs.s3a.bucket.$bucket.access.key",
      s"fs.s3a.bucket.$bucket.secret.key",
      s"fs.s3a.bucket.$bucket.session.token",
      "fs.s3a.access.key",
      "fs.s3a.secret.key",
      "fs.s3a.session.token")

  private def nonEmptyConf(hadoopConf: Configuration, key: String): Boolean =
    Option(hadoopConf.get(key)).exists(_.nonEmpty)

  /**
   * The lowercase-scheme-checked S3/S3A bucket name from `uri`'s authority (host, minus any
   * userinfo or port), or `None` when `uri`'s scheme is not `s3`/`s3a`. Parses the raw authority
   * text manually (last `@`, then last `:`) rather than using `URI#getHost`, mirroring
   * [[uriUserInfo]] and [[redactedAuthority]] above: the same RFC 3986 `reg-name` pitfall
   * documented on [[uriAuthority]] applies here too (an underscore, a valid S3 bucket-name
   * character, makes `getHost` return `null` for the WHOLE authority rather than just an empty
   * host).
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

  private def s3aScopedProviderPathReason(bucket: String, providerPathKey: String): String =
    "Native Delta scan cannot forward Hadoop credential-provider aliases for " +
      s"$bucket ($providerPathKey configures an S3A-scoped Hadoop credential provider that " +
      "Configuration#getPassword does not consult, so the native S3 client's credentials " +
      "cannot be verified)"

  private def shadowedCredentialAliasReason(bucket: String, alias: String): String =
    "Native Delta scan cannot forward Hadoop credential-provider aliases for " +
      s"$bucket ($alias resolves through $HadoopCredentialProviderPathKey but is not present " +
      "as a plain configuration value, so the native S3 client would have no credentials)"

  private def unverifiableCredentialProviderReason(bucket: String, error: Throwable): String =
    "Native Delta scan cannot verify Hadoop credential-provider aliases for " +
      s"$bucket (reading $HadoopCredentialProviderPathKey raised " +
      s"${error.getClass.getName}), declining rather than risk missing credentials"

  /**
   * Compares, for every alias [[s3aCredentialAliases]] lists for `bucket`, `Configuration#get`
   * (plain) against `Configuration#getPassword` (providers on `hadoop.security.credential.
   * provider.path` FIRST, plain conf as fallback): a resolved, non-empty password that differs
   * from the plain value -- including a plain value that is entirely absent -- means the alias is
   * invisible or wrong from the plain-conf extraction the native S3 client actually reads, so the
   * scan must decline. Only called once the caller has established the GLOBAL provider path is
   * the sole path configured for `bucket` (the S3A-scoped keys are declined earlier, without ever
   * reaching here, in [[bucketCredentialAliasReason]]).
   *
   * The whole comparison runs inside try/catch: `getPassword` performs real keystore I/O (opens
   * and parses the credential store named by the provider path), and a corrupt or unreadable
   * store must decline this bucket rather than let the exception escape and abort planning for
   * the entire session.
   */
  private def verifyGlobalProviderAliases(
      hadoopConf: Configuration,
      bucket: String): Option[String] = {
    try {
      s3aCredentialAliases(bucket).foldLeft(Option.empty[String]) { (declined, alias) =>
        if (declined.isDefined) {
          declined
        } else {
          val resolved =
            Option(hadoopConf.getPassword(alias)).map(new String(_)).filter(_.nonEmpty)
          resolved match {
            case Some(value) if !Option(hadoopConf.get(alias)).contains(value) =>
              Some(shadowedCredentialAliasReason(bucket, alias))
            case _ => None
          }
        }
      }
    } catch {
      case e @ (_: IOException | _: RuntimeException) =>
        Some(unverifiableCredentialProviderReason(bucket, e))
    }
  }

  /**
   * The decline reason for `bucket` alone, or `None` when nothing about `bucket`'s credentials
   * routes through a Hadoop credential provider. `globalPathSet`/`s3aPathSet` are hoisted by the
   * caller ([[credentialAliasReason]]) since they do not vary per bucket; only the per-bucket
   * provider-path key is looked up here.
   *
   * Zero-I/O precheck: when none of the four provider-path keys (global, S3A-scoped, or this
   * bucket's own short- or long-form S3A-scoped override) are set, returns `None` immediately --
   * a config-map lookup only, no keystore access -- so a scan with no credential provider
   * configured anywhere never pays for a single keystore read.
   *
   * Arm A: `fs.s3a.security.credential.provider.path` and its per-bucket short and long forms
   * (`fs.s3a.bucket.B.security.credential.provider.path` and
   * `fs.s3a.bucket.B.fs.s3a.security.credential.provider.path` -- `S3AUtils#lookupPassword`
   * resolves the per-bucket override through both) exist solely to point S3A's OWN credential
   * resolution (`S3AUtils#lookupPassword`, a conf-cloning dance this contrib must not
   * reimplement) at a provider. `Configuration#getPassword` -- the only resolution this gate can
   * perform without reimplementing S3A's internals -- does not consult any of these keys, so when
   * one is set this declines immediately, without a single keystore read: there is no faithful
   * way to tell whether it is shadowing a plain value.
   *
   * Arm B: only the global path is set, which `Configuration#getPassword` DOES consult; delegates
   * to [[verifyGlobalProviderAliases]].
   */
  private def bucketCredentialAliasReason(
      hadoopConf: Configuration,
      bucket: String,
      globalPathSet: Boolean,
      s3aPathSet: Boolean): Option[String] = {
    val bucketPathKey = s3aBucketProviderPathKey(bucket)
    val bucketLongPathKey = s3aBucketLongProviderPathKey(bucket)
    val bucketPathSet = nonEmptyConf(hadoopConf, bucketPathKey)
    val bucketLongPathSet = nonEmptyConf(hadoopConf, bucketLongPathKey)
    if (!globalPathSet && !s3aPathSet && !bucketPathSet && !bucketLongPathSet) {
      None
    } else if (s3aPathSet || bucketPathSet || bucketLongPathSet) {
      val offendingKey =
        if (s3aPathSet) S3aCredentialProviderPathKey
        else if (bucketPathSet) bucketPathKey
        else bucketLongPathKey
      Some(s3aScopedProviderPathReason(bucket, offendingKey))
    } else {
      verifyGlobalProviderAliases(hadoopConf, bucket)
    }
  }

  /**
   * Returns the first reason a native S3 scan cannot faithfully forward this table's Hadoop
   * credentials, or `None` when claimable. `uris` is the scan's data-file and deletion-vector
   * URIs (same set [[userInfoBearingAuthorityReason]] and [[multiStoreReason]] above inspect);
   * only `s3`/`s3a` authorities are relevant here (ABFS/WASB are mooted by the userinfo gate
   * above, GCS is out of scope for this gate).
   *
   * `Configuration#getPassword` resolves an alias by checking every provider on
   * `hadoop.security.credential.provider.path` FIRST, falling back to the plain configuration
   * value only when no provider has it. A keystore-only alias -- or one that SHADOWS a different
   * plain value -- is therefore invisible (or wrong) to the plain-conf extraction this contrib
   * forwards to the native Simple credentials provider: the native side would either have no
   * credentials at all, or silently use the wrong ones. When in doubt, decline; Spark's own Delta
   * reader resolves the same alias correctly through Hadoop's `FileSystem` machinery.
   *
   * Never interpolates a resolved credential value into the returned reason -- only alias/key
   * NAMES -- so a decline reason can never leak a secret into the SQL plan's explain output,
   * fallback-reason logging, or the Spark UI.
   */
  private[delta] def credentialAliasReason(
      hadoopConf: Configuration,
      uris: Seq[URI]): Option[String] = {
    val buckets = uris.flatMap(s3Bucket).distinct
    if (buckets.isEmpty) {
      return None
    }
    val globalPathSet = nonEmptyConf(hadoopConf, HadoopCredentialProviderPathKey)
    val s3aPathSet = nonEmptyConf(hadoopConf, S3aCredentialProviderPathKey)
    buckets.foldLeft(Option.empty[String]) { (declined, bucket) =>
      if (declined.isDefined) {
        declined
      } else {
        bucketCredentialAliasReason(hadoopConf, bucket, globalPathSet, s3aPathSet)
      }
    }
  }

  /**
   * True when `dataType` is, or structurally contains (through array elements or map keys/
   * values), a [[StructType]]. Array and map are structural container types whose own
   * "element"/"key"/"value" labels are never column-mapped; only the [[StructType]] fields
   * reachable through them carry Delta's physical, column-mapped names.
   */
  private def containsNestedStruct(dataType: DataType): Boolean = dataType match {
    case _: StructType => true
    case ArrayType(elementType, _) => containsNestedStruct(elementType)
    case MapType(keyType, valueType, _) =>
      containsNestedStruct(keyType) || containsNestedStruct(valueType)
    case _ => false
  }

  /**
   * True when `node` is a positional-output union -- [[org.apache.spark.sql.execution.UnionExec]]
   * (pre Comet-exec conversion) or `org.apache.spark.sql.comet.CometUnionExec` (already
   * converted, e.g. reused unchanged across an AQE stage boundary). Both compute their own output
   * positionally from the FIRST child's attributes (`operators.scala:1541-1557`'s
   * `CometUnionExec` is built directly from the wrapped `UnionExec`'s own output), so a live
   * value carried by any LATER branch is invisible to the alias-following and
   * expression-reference checks below unless it is explicitly walked across the position it
   * shares with that first child's attribute. Compared by simple class name (the idiom
   * [[isDeltaScan]] also uses) rather than `isInstanceOf`: this keeps the check working uniformly
   * across both the pre- and post-conversion node types without a compile-time dependency on
   * `org.apache.spark.sql.comet` internals here. `ReusedExchangeExec` shares the same
   * first-child-positional-output shape but is unreachable here: `ReuseExchangeAndSubquery` only
   * runs after Comet's columnar rules (including this claim/decline gate), so this rule never
   * observes one. A name that fails to match either `UnionExec` or `CometUnionExec` (e.g. a
   * future Spark/Comet union variant) is still safe: the generic multi-child safety net below
   * treats it like any other unrecognized multi-child node and declines rather than silently
   * missing the taint.
   */
  private def isPositionalUnion(node: SparkPlan): Boolean = {
    val name = node.getClass.getSimpleName
    name == "UnionExec" || name == "CometUnionExec"
  }

  /**
   * True when the scan's row-index column value is provably dead above the scan. The standard DV
   * plan shape routes it only into a `named_struct(... row_index ...) AS _metadata` projection
   * whose result the final projection discards; anything else (a query actually selecting
   * `_metadata.row_index`) makes the value live and must decline. Conservative: any unrecognized
   * consumption pattern returns false.
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
    // VISIBLE plan, either via Project aliases or, positionally, across a union. The plan handed
    // to this rule may be an AQE stage fragment, so anything tainted that reaches the fragment's
    // own output escapes to invisible consumers and must decline. Non-Project consumption of any
    // tainted attribute (a Filter, Aggregate, Join key, ...) declines outright.
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
          // UnionExec.expressions is empty and its output attributes carry the FIRST child's
          // expression IDs, so a value tainted only in a LATER branch is otherwise invisible to
          // both checks below. Walk it forward positionally: a tainted attribute at position i in
          // ANY child taints the union's own output attribute at position i.
          //
          // Arity is guaranteed equal for plain UnionExec (its own output is
          // children.map(_.output).transpose). CometUnionExec's `output`, though, is a frozen
          // snapshot captured at CometExecRule conversion time while `children` can be
          // re-parented afterward by AQE rewrites (coalesce, skew join, ...); if those two ever
          // disagree in width, a positional zip would SILENTLY TRUNCATE to the shorter side and
          // drop whichever trailing attributes fall outside it -- the unsound direction for a
          // decline analysis (a live value could go unnoticed). Fail safe instead: an arity
          // mismatch on ANY child forces an outright decline for this scan rather than trusting
          // a zip that cannot be positionally sound.
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
    // Generic safety net for every OTHER multi-child node (joins, etc; positional unions are
    // exempt -- they are already handled precisely above, and applying this check to them would
    // misfire on every union: a later branch's attribute legitimately carries a DIFFERENT
    // expression ID than the union's own output at that position even when nothing is wrong). A
    // tainted attribute a child contributes must either survive into the node's own output under
    // the SAME expression ID or be consumed by one of the node's own expressions (e.g. a join
    // condition); otherwise some other renaming/dropping this analysis does not model may have
    // hidden a live value from the checks above, so decline rather than risk it. This is what
    // catches e.g. a LEFT SEMI/ANTI join dropping the side that carried the tainted attribute.
    val multiChildLeak = plan.exists {
      case u if isPositionalUnion(u) => false
      case n if n.children.size >= 2 =>
        n.children.exists { c =>
          c.output.exists { attr =>
            tainted.contains(attr.exprId) &&
            !n.output.exists(_.exprId == attr.exprId) &&
            !n.expressions.exists(_.references.exists(_.exprId == attr.exprId))
          }
        }
      case _ => false
    }
    !nonProjectConsumer && !escapes && !multiChildLeak
  }
}
