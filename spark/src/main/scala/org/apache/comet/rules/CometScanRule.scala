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

package org.apache.comet.rules

import java.net.URI
import java.util.Locale

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, DynamicPruningExpression, EqualTo, Expression, GenericInternalRow, InputFileBlockLength, InputFileBlockStart, InputFileName, Literal, PlanExpression}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.{sideBySide, ArrayBasedMapData, GenericArrayData, MetadataColumnHelper}
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns.getExistenceDefaultValues
import org.apache.spark.sql.comet.{CometBatchScanExec, CometScanExec}
import org.apache.spark.sql.execution.{FileSourceScanExec, FilterExec, InSubqueryExec, ProjectExec, SparkPlan, SubqueryAdaptiveBroadcastExec}
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.csv.CSVScan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import org.apache.comet.{CometConf, CometNativeException, DataTypeSupport}
import org.apache.comet.CometConf._
import org.apache.comet.CometSparkSessionExtensions.{isCometLoaded, withInfo, withInfos}
import org.apache.comet.DataTypeSupport.isComplexType
import org.apache.comet.delta.DeltaReflection
import org.apache.comet.iceberg.{CometIcebergNativeScanMetadata, IcebergReflection}
import org.apache.comet.objectstore.NativeConfig
import org.apache.comet.parquet.CometParquetUtils.{encryptionEnabled, isEncryptionConfigSupported}
import org.apache.comet.parquet.Native
import org.apache.comet.serde.operator.{CometDeltaNativeScan, CometIcebergNativeScan, CometNativeScan}
import org.apache.comet.shims.{CometTypeShim, ShimFileFormat, ShimSubqueryBroadcast}

/**
 * Spark physical optimizer rule for replacing Spark scans with Comet scans.
 */
case class CometScanRule(session: SparkSession)
    extends Rule[SparkPlan]
    with CometTypeShim
    with ShimSubqueryBroadcast {

  private lazy val showTransformations = CometConf.COMET_EXPLAIN_TRANSFORMATIONS.get()

  override def apply(plan: SparkPlan): SparkPlan = {
    val newPlan = _apply(plan)
    if (showTransformations && !newPlan.fastEquals(plan)) {
      logInfo(s"""
           |=== Applying Rule $ruleName ===
           |${sideBySide(plan.treeString, newPlan.treeString).mkString("\n")}
           |""".stripMargin)
    }
    newPlan
  }

  private def _apply(plan: SparkPlan): SparkPlan = {
    if (!isCometLoaded(conf)) return plan

    // Phase 3 pre-pass: undo Delta's PreprocessTableWithDVs plan rewrite for
    // DV-in-use scans so the scan reaches our normal Delta native path. Delta's
    // rule wraps a DV-bearing scan in:
    //   ProjectExec(userOutput,
    //     FilterExec(__delta_internal_is_row_deleted = 0,
    //       ProjectExec([...userCols, is_row_deleted, _metadata_structs...],
    //         FileSourceScanExec(has is_row_deleted in output))))
    // We strip the wrappers, returning a FileSourceScanExec whose output is the
    // original userOutput and whose required/data schemas no longer carry the
    // synthetic column. CometScanRule's scan-level transform below then picks
    // up the clean scan and routes it through nativeDeltaScan, at which point
    // kernel's per-file `deleted_row_indexes` feed into our DeltaDvFilterExec
    // wrapper on the native side.
    //
    // Gated on COMET_DELTA_NATIVE_ENABLED: if the user has turned off Comet's
    // Delta path, we must leave Delta's own plan rewrite intact so vanilla
    // Spark+Delta applies the DV via DeltaParquetFileFormat at read time.
    val stripped = if (CometConf.COMET_DELTA_NATIVE_ENABLED.get(conf)) {
      stripDeltaDvWrappers(plan)
    } else {
      plan
    }

    def isSupportedScanNode(plan: SparkPlan): Boolean = plan match {
      case _: FileSourceScanExec => true
      case _: BatchScanExec => true
      case _ => false
    }

    def hasMetadataCol(plan: SparkPlan): Boolean = {
      plan.expressions.exists(_.exists {
        case a: Attribute =>
          a.isMetadataCol
        case _ => false
      })
    }

    def isIcebergMetadataTable(scanExec: BatchScanExec): Boolean = {
      // List of Iceberg metadata tables:
      // https://iceberg.apache.org/docs/latest/spark-queries/#inspecting-tables
      val metadataTableSuffix = Set(
        "history",
        "metadata_log_entries",
        "snapshots",
        "entries",
        "files",
        "manifests",
        "partitions",
        "position_deletes",
        "all_data_files",
        "all_delete_files",
        "all_entries",
        "all_manifests")

      metadataTableSuffix.exists(suffix => scanExec.table.name().endsWith(suffix))
    }

    val fullPlan = plan

    def transformScan(scanNode: SparkPlan): SparkPlan = scanNode match {
      case scan if !CometConf.COMET_NATIVE_SCAN_ENABLED.get(conf) =>
        withInfo(scan, "Comet Scan is not enabled")

      case scan if hasMetadataCol(scan) =>
        withInfo(scan, "Metadata column is not supported")

      // data source V1
      case scanExec: FileSourceScanExec =>
        transformV1Scan(fullPlan, scanExec)

      // data source V2
      case scanExec: BatchScanExec =>
        if (isIcebergMetadataTable(scanExec)) {
          withInfo(scanExec, "Iceberg Metadata tables are not supported")
        } else {
          transformV2Scan(scanExec)
        }
    }

    stripped.transform {
      case scan if isSupportedScanNode(scan) => transformScan(scan)
    }
  }

  /**
   * Plan-tree rewrite that undoes Delta's `PreprocessTableWithDVs` Catalyst Strategy for
   * DV-bearing reads. Delta's strategy runs at logical-to-physical conversion and wraps every
   * DV-in-use Delta scan in an outer Project + Filter subtree that references a synthetic
   * `__delta_internal_is_row_deleted` column produced by `DeltaParquetFileFormat`'s runtime
   * reader. Since Comet reads via its own tuned parquet path (not Delta's file format), that
   * synthetic column never gets produced, and the downstream Filter silently drops everything.
   *
   * We detect the pattern and replace it with a clean `FileSourceScanExec` whose required schema
   * + output no longer mention the synthetic column. Kernel provides the actual DV row indexes on
   * the driver side, and `DeltaDvFilterExec` applies them at execution time.
   */
  private def stripDeltaDvWrappers(plan: SparkPlan): SparkPlan = {
    plan.transformUp {
      case proj @ ProjectExec(projectList, FilterExec(cond, inner))
          if isDeltaDvFilterPattern(cond) =>
        val userOutput = projectList.map(_.toAttribute)
        findAndStripDeltaScanBelow(inner, userOutput).getOrElse(proj)
    }
  }

  /** Matches `__delta_internal_is_row_deleted = 0` (the filter Delta injects). */
  private def isDeltaDvFilterPattern(cond: Expression): Boolean = {
    def isRowDeletedRef(name: String): Boolean =
      name.equalsIgnoreCase(DeltaReflection.IsRowDeletedColumnName)
    cond match {
      case EqualTo(attr: AttributeReference, lit: Literal) if isRowDeletedRef(attr.name) =>
        lit.value != null && lit.value.toString == "0"
      case EqualTo(lit: Literal, attr: AttributeReference) if isRowDeletedRef(attr.name) =>
        lit.value != null && lit.value.toString == "0"
      case _ => false
    }
  }

  /**
   * Recursively descend through the Filter's child subtree looking for a Delta
   * `FileSourceScanExec` whose output contains the synthetic is-row-deleted column. On match,
   * rebuild it without the synthetic column. Returns None if no such scan is found (in which case
   * we leave the original Project/Filter in place).
   */
  private def findAndStripDeltaScanBelow(
      plan: SparkPlan,
      userOutput: Seq[Attribute]): Option[SparkPlan] = plan match {
    case scan: FileSourceScanExec
        if DeltaReflection.isDeltaFileFormat(scan.relation.fileFormat) &&
          scan.output.exists(_.name.equalsIgnoreCase(DeltaReflection.IsRowDeletedColumnName)) =>
      Some(rebuildDeltaScanWithoutDvColumn(scan, userOutput))
    case other if other.children.size == 1 =>
      // Single-child wrappers (Project, ColumnarToRow, etc.) Delta may insert between
      // its Filter and the real scan. Drop the wrapper entirely - the stripped scan
      // already produces the final user output shape.
      findAndStripDeltaScanBelow(other.children.head, userOutput)
    case _ => None
  }

  /**
   * Produce a new `FileSourceScanExec` whose output is exactly `userOutput` (the attributes of
   * the outer `ProjectExec` we are replacing). Dropping the synthetic
   * `__delta_internal_is_row_deleted` column is not enough: Delta's inner Project typically also
   * introduces `_metadata` struct attributes that would otherwise leak into the stripped scan's
   * output and make it wider than its Union siblings, which later explodes in
   * `UnionExec.output.transpose`.
   *
   * We therefore anchor the rebuild on `userOutput` and require each attribute to be resolvable
   * in the underlying scan (by exprId or name).
   */
  private def rebuildDeltaScanWithoutDvColumn(
      scan: FileSourceScanExec,
      userOutput: Seq[Attribute]): FileSourceScanExec = {
    val dvName = DeltaReflection.IsRowDeletedColumnName
    val scanByExprId = scan.output.map(a => a.exprId -> a).toMap
    val scanByName = scan.output.map(a => a.name.toLowerCase(Locale.ROOT) -> a).toMap
    val resolved = userOutput.map { u =>
      scanByExprId
        .get(u.exprId)
        .orElse(scanByName.get(u.name.toLowerCase(Locale.ROOT)))
        .getOrElse(u)
    }
    val newOutput = resolved.filterNot(_.name == dvName)
    val newRequiredSchema =
      StructType(newOutput.map(a => StructField(a.name, a.dataType, a.nullable)))
    val newDataSchema =
      StructType(scan.relation.dataSchema.fields.filterNot(_.name == dvName))
    val newRelation = scan.relation.copy(dataSchema = newDataSchema)(scan.relation.sparkSession)
    // Spark's filter pushdown may have moved Delta's injected `is_row_deleted = 0`
    // predicate into `dataFilters`. The column no longer exists in our rebuilt scan, so
    // any filter that references it must also be dropped - otherwise downstream code
    // (e.g. our native Delta serde) sees a predicate it can't translate and falls back.
    val newDataFilters = scan.dataFilters.filterNot { f =>
      f.references.exists(_.name == dvName)
    }
    scan.copy(
      relation = newRelation,
      output = newOutput,
      requiredSchema = newRequiredSchema,
      dataFilters = newDataFilters)
  }

  private def transformV1Scan(plan: SparkPlan, scanExec: FileSourceScanExec): SparkPlan = {

    scanExec.relation match {
      case r: HadoopFsRelation =>
        // Delta Lake (V1 path): detect BEFORE the DPP fallback check below,
        // because Delta's native path handles DPP through partition pruning
        // at execution time (DPP expressions are filtered out of the
        // planning-time InterpretedPredicate and applied by Spark post-scan).
        if (DeltaReflection.isDeltaFileFormat(r.fileFormat)) {
          return nativeDeltaScan(session, scanExec, r, hadoopConfOrNull = null)
            .getOrElse(scanExec)
        }
        // DPP fallback for non-Delta scans (DataFusion/Iceberg-compat paths
        // don't support DPP natively).
        if (COMET_DPP_FALLBACK_ENABLED.get() &&
          scanExec.partitionFilters.exists(isDynamicPruningFilter)) {
          return withInfo(scanExec, "Dynamic Partition Pruning is not supported")
        }
        if (!CometScanExec.isFileFormatSupported(r.fileFormat)) {
          return withInfo(scanExec, s"Unsupported file format ${r.fileFormat}")
        }
        val hadoopConf = r.sparkSession.sessionState.newHadoopConfWithOptions(r.options)

        // TODO is this restriction valid for all native scan types?
        val possibleDefaultValues = getExistenceDefaultValues(scanExec.requiredSchema)
        if (possibleDefaultValues.exists(d => {
            d != null && (d.isInstanceOf[ArrayBasedMapData] || d
              .isInstanceOf[GenericInternalRow] || d.isInstanceOf[GenericArrayData])
          })) {
          // Spark already converted these to Java-native types, so we can't check SQL types.
          // ArrayBasedMapData, GenericInternalRow, GenericArrayData correspond to maps, structs,
          // and arrays respectively.
          withInfo(
            scanExec,
            "Full native scan disabled because default values for nested types are not supported")
          return scanExec
        }

        COMET_NATIVE_SCAN_IMPL.get() match {
          case SCAN_AUTO | SCAN_NATIVE_DATAFUSION =>
            nativeDataFusionScan(plan, session, scanExec, r, hadoopConf).getOrElse(scanExec)
          case SCAN_NATIVE_ICEBERG_COMPAT =>
            nativeIcebergCompatScan(session, scanExec, r, hadoopConf).getOrElse(scanExec)
        }

      case _ =>
        withInfo(scanExec, s"Unsupported relation ${scanExec.relation}")
    }
  }

  private def nativeDataFusionScan(
      plan: SparkPlan,
      session: SparkSession,
      scanExec: FileSourceScanExec,
      r: HadoopFsRelation,
      hadoopConf: Configuration): Option[SparkPlan] = {
    if (!COMET_EXEC_ENABLED.get()) {
      withInfo(
        scanExec,
        s"$SCAN_NATIVE_DATAFUSION scan requires ${COMET_EXEC_ENABLED.key} to be enabled")
      return None
    }
    if (!CometNativeScan.isSupported(scanExec)) {
      return None
    }
    if (encryptionEnabled(hadoopConf) && !isEncryptionConfigSupported(hadoopConf)) {
      withInfo(scanExec, s"$SCAN_NATIVE_DATAFUSION does not support encryption")
      return None
    }
    if (scanExec.fileConstantMetadataColumns.nonEmpty) {
      withInfo(scanExec, "Native DataFusion scan does not support metadata columns")
      return None
    }
    // input_file_name, input_file_block_start, and input_file_block_length read from
    // InputFileBlockHolder, a thread-local set by Spark's FileScanRDD. The native DataFusion
    // scan does not use FileScanRDD, so these expressions would return empty/default values.
    if (plan.exists(node =>
        node.expressions.exists(_.exists {
          case _: InputFileName | _: InputFileBlockStart | _: InputFileBlockLength => true
          case _ => false
        }))) {
      withInfo(
        scanExec,
        "Native DataFusion scan is not compatible with input_file_name, " +
          "input_file_block_start, or input_file_block_length")
      return None
    }
    if (ShimFileFormat.findRowIndexColumnIndexInSchema(scanExec.requiredSchema) >= 0) {
      withInfo(scanExec, "Native DataFusion scan does not support row index generation")
      return None
    }
    if (session.sessionState.conf.getConf(SQLConf.PARQUET_FIELD_ID_READ_ENABLED) &&
      ParquetUtils.hasFieldIds(scanExec.requiredSchema)) {
      withInfo(scanExec, "Native DataFusion scan does not support Parquet field ID matching")
      return None
    }
    if (!isSchemaSupported(scanExec, SCAN_NATIVE_DATAFUSION, r)) {
      return None
    }
    Some(CometScanExec(scanExec, session, SCAN_NATIVE_DATAFUSION))
  }

  private def nativeIcebergCompatScan(
      session: SparkSession,
      scanExec: FileSourceScanExec,
      r: HadoopFsRelation,
      hadoopConf: Configuration): Option[SparkPlan] = {
    if (encryptionEnabled(hadoopConf) && !isEncryptionConfigSupported(hadoopConf)) {
      withInfo(scanExec, s"$SCAN_NATIVE_ICEBERG_COMPAT does not support encryption")
      return None
    }
    if (!isSchemaSupported(scanExec, SCAN_NATIVE_ICEBERG_COMPAT, r)) {
      return None
    }
    Some(CometScanExec(scanExec, session, SCAN_NATIVE_ICEBERG_COMPAT))
  }

  /**
   * Delta Lake native scan path (V1 relations). Gated on
   * [[CometConf.COMET_DELTA_NATIVE_ENABLED]]; returns None when the feature flag is off so the
   * caller's `.getOrElse(scanExec)` falls back to Spark's Delta reader.
   *
   * Schema / type validation reuses the native Iceberg checker since both paths converge on
   * Comet's ParquetSource under the hood.
   */
  private def nativeDeltaScan(
      session: SparkSession,
      scanExec: FileSourceScanExec,
      r: HadoopFsRelation,
      hadoopConfOrNull: Configuration): Option[SparkPlan] = {
    if (!CometConf.COMET_DELTA_NATIVE_ENABLED.get()) {
      withInfo(
        scanExec,
        s"Native Delta scan disabled because ${CometConf.COMET_DELTA_NATIVE_ENABLED.key} " +
          "is not enabled")
      return None
    }
    if (!COMET_EXEC_ENABLED.get()) {
      withInfo(scanExec, s"Native Delta scan requires ${COMET_EXEC_ENABLED.key} to be enabled")
      return None
    }
    val hadoopConf = Option(hadoopConfOrNull).getOrElse(
      r.sparkSession.sessionState.newHadoopConfWithOptions(r.options))
    if (encryptionEnabled(hadoopConf) && !isEncryptionConfigSupported(hadoopConf)) {
      withInfo(scanExec, s"Native Delta scan does not support encryption")
      return None
    }
    if (!isSchemaSupported(scanExec, SCAN_NATIVE_DELTA_COMPAT, r)) {
      return None
    }

    // All Delta pre-materialised file indexes are handled natively now:
    //   - `TahoeBatchFileIndex`: MERGE/UPDATE/DELETE post-join rewrites.
    //   - `TahoeChangeFileIndex`: CDC change-data-feed file reads.
    //   - `CdcAddFileIndex` + `TahoeRemoveFileIndex`: CDC insert/delete
    //     branches. Delta stashes the `_change_type` / `_commit_version` /
    //     `_commit_timestamp` metadata columns into `AddFile.partitionValues`
    //     (with a matching `partitionSchema`), so the native scan materialises
    //     them via the standard partition-column path once we fetch the
    //     augmented AddFile list through `matchingFiles(Seq.empty, Seq.empty)`.
    //
    // `TahoeLogFileIndexWithCloudFetch` is a variant we haven't validated yet.
    val fileIndexClassName = r.location.getClass.getName
    if (fileIndexClassName.endsWith(".TahoeLogFileIndexWithCloudFetch")) {
      withInfo(
        scanExec,
        s"Native Delta scan has not validated the cloud-fetch variant " +
          s"($fileIndexClassName).")
      return None
    }

    // Validate filesystem schemes from the scan's root paths. We used to call
    // `location.inputFiles` here to enumerate every file, but for Delta's
    // `TahoeLogFileIndex` that forces `Snapshot.cachedState` -- which flips
    // `stateReconstructionTriggered = true` on the snapshot and breaks Delta
    // tests that assert it stays false (e.g. `ChecksumSuite` "Incremental
    // checksums: post commit snapshot should have a checksum without
    // triggering state reconstruction"). `rootPaths` is cheap and, for Delta
    // tables, always shares the scheme of every file under it.
    val supportedSchemes =
      Set("file", "s3", "s3a", "gs", "gcs", "abfss", "abfs", "wasbs", "wasb", "oss")
    val rootPaths = scanExec.relation.location.rootPaths
    if (rootPaths.nonEmpty) {
      val schemes = rootPaths.map(p => p.toUri.getScheme).filter(_ != null).toSet
      val unsupported = schemes -- supportedSchemes
      if (unsupported.nonEmpty) {
        withInfo(
          scanExec,
          s"Native Delta scan does not support filesystem schemes: " +
            unsupported.mkString(", "))
        return None
      }
    }

    // Row tracking: Delta's own analyzer leaves a plain `row_id` (and
    // `row_commit_version`) attribute in the scan's requiredSchema when the
    // query references `_metadata.row_id`, relying on `DeltaParquetFileFormat`'s
    // reader to synthesise it. Once we swap the file format to
    // `CometParquetFileFormat` that synthesis is gone, so the read fails with
    // "Required column 'row_id' is missing in data file". For tables that have
    // the column materialised (the common case after a MERGE / UPDATE / rowTracking
    // backfill), rewrite the scan to read the physical materialised column and
    // wrap the result in a projection that restores the logical name.
    applyRowTrackingRewrite(scanExec, r, session).getOrElse {
      Some(CometScanExec(scanExec, session, SCAN_NATIVE_DELTA_COMPAT))
    }
  }

  /**
   * Rewrite `scanExec` so any `row_id` / `row_commit_version` attributes in its output /
   * requiredSchema refer to the physical (materialised) column names stored in the Delta table
   * metadata. Returns `Some(Some(plan))` when a rewrite was applied, `Some(None)` when we
   * detected a row-tracking column we can't translate (no materialised name available), and
   * `None` when the scan has no row-tracking columns and should be processed normally.
   */
  private def applyRowTrackingRewrite(
      scanExec: FileSourceScanExec,
      r: HadoopFsRelation,
      session: SparkSession): Option[Option[SparkPlan]] = {
    // Short-circuit when the scan output has neither row-tracking column.
    val RowIdName = "row_id"
    val RowCommitVersionName = "row_commit_version"
    val hasRowIdField = scanExec.requiredSchema.fieldNames.exists { n =>
      n.equalsIgnoreCase(RowIdName) || n.equalsIgnoreCase(RowCommitVersionName)
    }
    if (!hasRowIdField) return None

    val cfg = DeltaReflection.extractMetadataConfiguration(r).getOrElse(Map.empty)
    val rowIdPhysical = cfg.get(DeltaReflection.MaterializedRowIdColumnProp)
    val rowVerPhysical = cfg.get(DeltaReflection.MaterializedRowCommitVersionColumnProp)

    // We only translate when the column is actually materialised. Without a physical
    // name we would be guessing, so decline native acceleration and let Spark's
    // reader handle the synthesis.
    if (rowIdPhysical.isEmpty && rowVerPhysical.isEmpty) {
      withInfo(
        scanExec,
        "Native Delta scan: row-tracking columns present but no materialised column " +
          "names in Delta metadata; synthesis from baseRowId + row_index is Phase 3.")
      return Some(None)
    }

    import org.apache.spark.sql.catalyst.expressions.{Add, Alias, AttributeReference, Coalesce}
    // For each row-tracking field, rewrite the schemas to use the physical name and
    // build the alias expressions the outer Project needs to rename back. Non-row-tracking
    // fields are passed through unchanged.
    val renames = scala.collection.mutable.ArrayBuffer.empty[(String, String)]
    def physicalFor(logical: String): Option[String] =
      if (logical.equalsIgnoreCase(RowIdName)) rowIdPhysical
      else if (logical.equalsIgnoreCase(RowCommitVersionName)) rowVerPhysical
      else None

    // Materialised row-id / row-commit-version columns are nullable at the storage
    // level even when the logical attribute is non-nullable: Delta stores null for
    // rows that still need baseRowId + row_index synthesis (phase 3). Forcing the
    // rewritten field to be nullable avoids the parquet reader rejecting
    // legitimate nulls as a schema violation.
    val newRequiredFields = scanExec.requiredSchema.fields.map { f =>
      physicalFor(f.name) match {
        case Some(phys) =>
          renames += ((f.name, phys))
          StructField(phys, f.dataType, nullable = true, f.metadata)
        case None => f
      }
    }
    val newDataFields = r.dataSchema.fields.map { f =>
      physicalFor(f.name) match {
        case Some(phys) => StructField(phys, f.dataType, nullable = true, f.metadata)
        case None => f
      }
    }

    if (renames.isEmpty) return None

    // Phase 3: add the `_tmp_metadata_row_index` metadata column so Comet's
    // `NativeBatchReader` generates per-row file-relative row indexes, and wrap
    // the underlying FileIndex with `RowTrackingAugmentedFileIndex` so every
    // PartitionedFile carries its `AddFile.baseRowId` as a synthetic partition
    // value. The outer Project below then builds:
    //     row_id = coalesce(materialized_row_id, __comet_base_row_id + row_index)
    // which reproduces what Delta's own reader does internally.
    import org.apache.spark.sql.types.LongType
    val RowIndexColName = "_tmp_metadata_row_index"
    val BaseRowIdColName = "__comet_base_row_id"
    val includeRowIdSynth = renames.exists { case (logical, _) =>
      logical.equalsIgnoreCase(RowIdName)
    }

    val DefaultRowCommitVersionColName = "__comet_default_row_commit_version"
    val includeRowVerSynth = renames.exists { case (logical, _) =>
      logical.equalsIgnoreCase(RowCommitVersionName)
    }
    val needSynth = includeRowIdSynth || includeRowVerSynth

    val infoByFileName: Map[String, DeltaReflection.RowTrackingFileInfo] =
      if (needSynth) DeltaReflection.extractRowTrackingInfoByFileName(r.location)
      else Map.empty

    val extraRequiredFields = scala.collection.mutable.ArrayBuffer.empty[StructField]
    val extraDataFields = scala.collection.mutable.ArrayBuffer.empty[StructField]
    if (includeRowIdSynth) {
      val rowIndexField = StructField(RowIndexColName, LongType, nullable = true)
      extraRequiredFields += rowIndexField
      extraDataFields += rowIndexField
    }

    val finalRequiredSchema = StructType(newRequiredFields ++ extraRequiredFields)
    val finalDataSchema = StructType(newDataFields ++ extraDataFields)
    val finalLocation =
      if (needSynth) {
        new org.apache.comet.delta.RowTrackingAugmentedFileIndex(
          r.location,
          infoByFileName,
          BaseRowIdColName,
          DefaultRowCommitVersionColName)
      } else {
        r.location
      }

    // Build the scan output attributes with physical names. The outer Project restores
    // the original exprId under the logical alias so nothing downstream cares about
    // the renaming.
    val origOutput = scanExec.output
    val renameMap: Map[String, String] = renames.toMap
    val baseNewOutput = origOutput.map { a =>
      renameMap.get(a.name) match {
        case Some(phys) =>
          AttributeReference(phys, a.dataType, nullable = true, a.metadata)(qualifier =
            a.qualifier)
        case None => a
      }
    }
    val rowIndexAttr =
      AttributeReference(RowIndexColName, LongType, nullable = true)()
    val baseRowIdAttr =
      AttributeReference(BaseRowIdColName, LongType, nullable = true)()
    val defaultVerAttr =
      AttributeReference(DefaultRowCommitVersionColName, LongType, nullable = true)()
    val extraOutputAttrs = scala.collection.mutable.ArrayBuffer.empty[AttributeReference]
    if (includeRowIdSynth) extraOutputAttrs += rowIndexAttr
    if (needSynth) {
      extraOutputAttrs += baseRowIdAttr
      extraOutputAttrs += defaultVerAttr
    }
    val newOutput: Seq[Attribute] =
      if (extraOutputAttrs.isEmpty) baseNewOutput
      else baseNewOutput ++ extraOutputAttrs

    val newPartitionSchema =
      if (needSynth) {
        r.partitionSchema
          .add(StructField(BaseRowIdColName, LongType, nullable = true))
          .add(StructField(DefaultRowCommitVersionColName, LongType, nullable = true))
      } else {
        r.partitionSchema
      }

    val newRelation = r.copy(
      location = finalLocation,
      dataSchema = finalDataSchema,
      partitionSchema = newPartitionSchema)(r.sparkSession)
    val newScan = scanExec.copy(
      relation = newRelation,
      output = newOutput,
      requiredSchema = finalRequiredSchema)
    val cometScan = CometScanExec(newScan, session, SCAN_NATIVE_DELTA_COMPAT)

    val projectExprs = origOutput.map { a =>
      renameMap.get(a.name) match {
        case Some(phys) if a.name.equalsIgnoreCase(RowIdName) && includeRowIdSynth =>
          val physAttr = baseNewOutput.find(_.name == phys).get
          // row_id = coalesce(materialized, base_row_id + row_index)
          val synth = Add(baseRowIdAttr, rowIndexAttr)
          Alias(Coalesce(Seq(physAttr, synth)), a.name)(
            exprId = a.exprId,
            qualifier = a.qualifier)
        case Some(phys) if a.name.equalsIgnoreCase(RowCommitVersionName) && includeRowVerSynth =>
          val physAttr = baseNewOutput.find(_.name == phys).get
          // row_commit_version = coalesce(materialized, default_row_commit_version)
          Alias(Coalesce(Seq(physAttr, defaultVerAttr)), a.name)(
            exprId = a.exprId,
            qualifier = a.qualifier)
        case Some(phys) =>
          val physAttr = baseNewOutput.find(_.name == phys).get
          Alias(physAttr, a.name)(exprId = a.exprId, qualifier = a.qualifier)
        case None => a
      }
    }
    Some(Some(ProjectExec(projectExprs, cometScan)))
  }

  private def transformV2Scan(scanExec: BatchScanExec): SparkPlan = {

    scanExec.scan match {
      case scan: CSVScan if COMET_CSV_V2_NATIVE_ENABLED.get() =>
        val fallbackReasons = new ListBuffer[String]()
        val schemaSupported =
          CometBatchScanExec.isSchemaSupported(scan.readDataSchema, fallbackReasons)
        if (!schemaSupported) {
          fallbackReasons += s"Schema ${scan.readDataSchema} is not supported"
        }
        val partitionSchemaSupported =
          CometBatchScanExec.isSchemaSupported(scan.readPartitionSchema, fallbackReasons)
        if (!partitionSchemaSupported) {
          fallbackReasons += s"Partition schema ${scan.readPartitionSchema} is not supported"
        }
        val corruptedRecordsColumnName =
          SQLConf.get.getConf(SQLConf.COLUMN_NAME_OF_CORRUPT_RECORD)
        val containsCorruptedRecordsColumn =
          !scan.readDataSchema.fieldNames.contains(corruptedRecordsColumnName)
        if (!containsCorruptedRecordsColumn) {
          fallbackReasons += "Comet doesn't support the processing of corrupted records"
        }
        val isInferSchemaEnabled = scan.options.getBoolean("inferSchema", false)
        if (isInferSchemaEnabled) {
          fallbackReasons += "Comet doesn't support inferSchema=true option"
        }
        val delimiter =
          Option(scan.options.get("delimiter"))
            .orElse(Option(scan.options.get("sep")))
            .getOrElse(",")
        val isSingleCharacterDelimiter = delimiter.length == 1
        if (!isSingleCharacterDelimiter) {
          fallbackReasons +=
            s"Comet supports only single-character delimiters, but got: '$delimiter'"
        }
        if (schemaSupported && partitionSchemaSupported && containsCorruptedRecordsColumn
          && !isInferSchemaEnabled && isSingleCharacterDelimiter) {
          CometBatchScanExec(
            scanExec.clone().asInstanceOf[BatchScanExec],
            runtimeFilters = scanExec.runtimeFilters)
        } else {
          withInfos(scanExec, fallbackReasons.toSet)
        }

      // Iceberg scan - detected by class name
      case _
          if scanExec.scan.getClass.getName ==
            "org.apache.iceberg.spark.source.SparkBatchQueryScan" =>
        val fallbackReasons = new ListBuffer[String]()

        // Native Iceberg scan requires both configs to be enabled
        if (!COMET_ICEBERG_NATIVE_ENABLED.get()) {
          fallbackReasons += "Native Iceberg scan disabled because " +
            s"${COMET_ICEBERG_NATIVE_ENABLED.key} is not enabled"
          return withInfos(scanExec, fallbackReasons.toSet)
        }

        if (!COMET_EXEC_ENABLED.get()) {
          fallbackReasons += "Native Iceberg scan disabled because " +
            s"${COMET_EXEC_ENABLED.key} is not enabled"
          return withInfos(scanExec, fallbackReasons.toSet)
        }

        val typeChecker = CometScanTypeChecker(SCAN_NATIVE_DATAFUSION)
        val schemaSupported =
          typeChecker.isSchemaSupported(scanExec.scan.readSchema(), fallbackReasons)

        if (!schemaSupported) {
          fallbackReasons += "Comet extension is not enabled for " +
            s"${scanExec.scan.getClass.getSimpleName}: Schema not supported"
        }

        // Extract all Iceberg metadata once using reflection.
        // If any required reflection fails, this returns None, and we fall back to Spark.
        // First get metadataLocation and catalogProperties which are needed by the factory.
        val tableOpt = IcebergReflection.getTable(scanExec.scan)

        val metadataLocationOpt = tableOpt.flatMap { table =>
          IcebergReflection.getMetadataLocation(table)
        }

        val metadataOpt = metadataLocationOpt.flatMap { metadataLocation =>
          try {
            val session = org.apache.spark.sql.SparkSession.active
            val hadoopConf = session.sessionState.newHadoopConf()

            // For REST catalogs, the metadata file may not exist on disk since metadata
            // is fetched via HTTP. Check if file exists; if not, use table location instead.
            val metadataUri = new java.net.URI(metadataLocation)

            val metadataFile = new java.io.File(metadataUri.getPath)

            val effectiveLocation =
              if (!metadataFile.exists() && metadataUri.getScheme == "file") {
                // Metadata file doesn't exist (REST catalog with InMemoryFileIO or similar)
                // Use table location instead for FileIO initialization

                tableOpt
                  .flatMap { table =>
                    try {
                      val locationMethod = table.getClass.getMethod("location")
                      val tableLocation = locationMethod.invoke(table).asInstanceOf[String]
                      Some(tableLocation)
                    } catch {
                      case _: Exception =>
                        Some(metadataLocation)
                    }
                  }
                  .getOrElse(metadataLocation)
              } else {
                metadataLocation
              }

            val effectiveUri = new java.net.URI(effectiveLocation)

            val hadoopS3Options = NativeConfig.extractObjectStoreOptions(hadoopConf, effectiveUri)

            val hadoopDerivedProperties =
              CometIcebergNativeScan.hadoopToIcebergS3Properties(hadoopS3Options)

            // Extract vended credentials from FileIO (REST catalog credential vending).
            // FileIO properties take precedence over Hadoop-derived properties because
            // they contain per-table credentials vended by the REST catalog.
            val fileIOProperties = tableOpt
              .flatMap(IcebergReflection.getFileIOProperties)
              .map(CometIcebergNativeScan.filterStorageProperties)
              .getOrElse(Map.empty)

            val catalogProperties = hadoopDerivedProperties ++ fileIOProperties

            val result = CometIcebergNativeScanMetadata
              .extract(scanExec.scan, effectiveLocation, catalogProperties)

            result
          } catch {
            case e: Exception =>
              logError(
                s"Failed to extract catalog properties from Iceberg scan: ${e.getMessage}",
                e)
              None
          }
        }

        // If metadata extraction failed, fall back to Spark
        val metadata = metadataOpt match {
          case Some(m) => m
          case None =>
            fallbackReasons += "Failed to extract Iceberg metadata via reflection"
            return withInfos(scanExec, fallbackReasons.toSet)
        }

        // Now perform all validation using the pre-extracted metadata
        // Check if table uses a FileIO implementation compatible with iceberg-rust

        val fileIOCompatible = IcebergReflection.getFileIO(metadata.table) match {
          case Some(_) =>
            // InMemoryFileIO is now supported with table location fallback for REST catalogs
            true
          case None =>
            fallbackReasons += "Could not check FileIO compatibility"
            false
        }

        // Check Iceberg table format version

        val formatVersionSupported = IcebergReflection.getFormatVersion(metadata.table) match {
          case Some(formatVersion) =>
            if (formatVersion > 2) {
              fallbackReasons += "Iceberg table format version " +
                s"$formatVersion is not supported. " +
                "Comet only supports Iceberg table format V1 and V2"
              false
            } else {
              true
            }
          case None =>
            fallbackReasons += "Could not verify Iceberg table format version"
            false
        }

        // Single-pass validation of all FileScanTasks
        val taskValidation =
          try {
            CometScanRule.validateIcebergFileScanTasks(metadata.tasks)
          } catch {
            case e: Exception =>
              fallbackReasons += "Iceberg reflection failure: Could not validate " +
                s"FileScanTasks: ${e.getMessage}"
              return withInfos(scanExec, fallbackReasons.toSet)
          }

        // Check if all files are Parquet format and use supported filesystem schemes
        val allSupportedFilesystems = if (taskValidation.unsupportedSchemes.isEmpty) {
          true
        } else {
          fallbackReasons += "Iceberg scan contains files with unsupported filesystem " +
            s"schemes: ${taskValidation.unsupportedSchemes.mkString(", ")}. " +
            "Comet only supports: file, s3, s3a, gs, gcs, oss, abfss, abfs, wasbs, wasb"
          false
        }

        if (!taskValidation.allParquet) {
          fallbackReasons += "Iceberg scan contains non-Parquet files (ORC or Avro). " +
            "Comet only supports Parquet files in Iceberg tables"
        }

        // Partition values are deserialized via iceberg-rust's Literal::try_from_json()
        // which has incomplete type support (binary/fixed unimplemented, decimals limited)
        val partitionTypesSupported = (for {
          partitionSpec <- IcebergReflection.getPartitionSpec(metadata.table)
        } yield {
          val unsupportedTypes =
            IcebergReflection.validatePartitionTypes(partitionSpec, metadata.scanSchema)

          if (unsupportedTypes.nonEmpty) {
            unsupportedTypes.foreach { case (fieldName, typeStr, reason) =>
              fallbackReasons +=
                s"Partition column '$fieldName' with type $typeStr is not yet supported by " +
                  s"iceberg-rust: $reason"
            }
            false
          } else {
            true
          }
        }).getOrElse {
          // Fall back to Spark if reflection fails - cannot verify safety
          val msg =
            "Iceberg reflection failure: Could not verify partition types compatibility"
          logError(msg)
          fallbackReasons += msg
          false
        }

        // Get filter expressions for complex predicates check
        val filterExpressionsOpt = IcebergReflection.getFilterExpressions(scanExec.scan)

        // IS NULL/NOT NULL on complex types fail because iceberg-rust's accessor creation
        // only handles primitive fields. Nested field filters work because Iceberg Java
        // pre-binds them to field IDs. Element/key access filters don't push down to FileScanTasks.
        val complexTypePredicatesSupported = filterExpressionsOpt
          .map { filters =>
            // Empty filters can't trigger accessor issues
            if (filters.isEmpty) {
              true
            } else {
              val readSchema = scanExec.scan.readSchema()

              // Identify complex type columns that would trigger accessor creation failures
              val complexColumns = readSchema
                .filter(field => isComplexType(field.dataType))
                .map(_.name)
                .toSet

              // Detect IS NULL/NOT NULL on complex columns (pattern: is_null(ref(name="col")))
              // Nested field filters use different patterns and don't trigger this issue
              val hasComplexNullCheck = filters.asScala.exists { expr =>
                val exprStr = expr.toString
                val isNullCheck = exprStr.contains("is_null") || exprStr.contains("not_null")
                if (isNullCheck) {
                  complexColumns.exists { colName =>
                    exprStr.contains(s"""ref(name="$colName")""")
                  }
                } else {
                  false
                }
              }

              if (hasComplexNullCheck) {
                fallbackReasons += "IS NULL / IS NOT NULL predicates on complex type columns " +
                  "(struct/array/map) are not yet supported by iceberg-rust " +
                  "(nested field filters like address.city = 'NYC' are supported)"
                false
              } else {
                true
              }
            }
          }
          .getOrElse {
            // Fall back to Spark if reflection fails - cannot verify safety
            val msg =
              "Iceberg reflection failure: Could not check for complex type predicates"
            logError(msg)
            fallbackReasons += msg
            false
          }

        // Check for unsupported transform functions in residual expressions
        // iceberg-rust can only handle identity transforms in residuals; all other transforms
        // (truncate, bucket, year, month, day, hour) must fall back to Spark
        val transformFunctionsSupported = taskValidation.nonIdentityTransform match {
          case Some(transformType) =>
            fallbackReasons +=
              s"Iceberg transform function '$transformType' in residual expression " +
                "is not yet supported by iceberg-rust. " +
                "Only identity transforms are supported."
            false
          case None =>
            true
        }

        // Check for unsupported struct types in delete files
        val deleteFileTypesSupported = {
          var hasUnsupportedDeletes = false

          try {
            if (!taskValidation.deleteFiles.isEmpty) {
              taskValidation.deleteFiles.asScala.foreach { deleteFile =>
                val equalityFieldIds = IcebergReflection.getEqualityFieldIds(deleteFile)

                if (!equalityFieldIds.isEmpty) {
                  // Look up field types
                  equalityFieldIds.asScala.foreach { fieldId =>
                    val fieldInfo = IcebergReflection.getFieldInfo(
                      metadata.scanSchema,
                      fieldId.asInstanceOf[Int])
                    fieldInfo match {
                      case Some((fieldName, fieldType)) =>
                        if (fieldType.contains("struct")) {
                          hasUnsupportedDeletes = true
                          fallbackReasons +=
                            s"Equality delete on unsupported column type '$fieldName' " +
                              s"($fieldType) is not yet supported by iceberg-rust. " +
                              "Struct types in equality deletes " +
                              "require datum conversion support that is not yet implemented."
                        }
                      case None =>
                    }
                  }
                }
              }
            }
          } catch {
            case e: Exception =>
              // Reflection failure means we cannot verify safety - must fall back
              hasUnsupportedDeletes = true
              fallbackReasons += "Iceberg reflection failure: Could not verify delete file " +
                s"types for safety: ${e.getMessage}"
          }

          !hasUnsupportedDeletes
        }

        // Check that all DPP subqueries use InSubqueryExec which we know how to handle.
        // Future Spark versions might introduce new subquery types we haven't tested.
        val dppSubqueriesSupported = {
          val unsupportedSubqueries = scanExec.runtimeFilters.collect {
            case DynamicPruningExpression(e) if !e.isInstanceOf[InSubqueryExec] =>
              e.getClass.getSimpleName
          }
          // Check for multi-index DPP which we don't support yet.
          // SPARK-46946 changed SubqueryAdaptiveBroadcastExec from index: Int to indices: Seq[Int]
          // as a preparatory refactor for future features (Null Safe Equality DPP, multiple
          // equality predicates). Currently indices always has one element, but future Spark
          // versions might use multiple indices.
          val multiIndexDpp = scanExec.runtimeFilters.exists {
            case DynamicPruningExpression(e: InSubqueryExec) =>
              e.plan match {
                case sab: SubqueryAdaptiveBroadcastExec =>
                  getSubqueryBroadcastIndices(sab).length > 1
                case _ => false
              }
            case _ => false
          }
          if (unsupportedSubqueries.nonEmpty) {
            fallbackReasons +=
              s"Unsupported DPP subquery types: ${unsupportedSubqueries.mkString(", ")}. " +
                "CometIcebergNativeScanExec only supports InSubqueryExec for DPP"
            false
          } else if (multiIndexDpp) {
            // See SPARK-46946 for context on multi-index DPP
            fallbackReasons +=
              "Multi-index DPP (indices.length > 1) is not yet supported. " +
                "See SPARK-46946 for context."
            false
          } else {
            true
          }
        }

        if (schemaSupported && fileIOCompatible && formatVersionSupported &&
          taskValidation.allParquet && allSupportedFilesystems && partitionTypesSupported &&
          complexTypePredicatesSupported && transformFunctionsSupported &&
          deleteFileTypesSupported && dppSubqueriesSupported) {
          CometBatchScanExec(
            scanExec.clone().asInstanceOf[BatchScanExec],
            runtimeFilters = scanExec.runtimeFilters,
            nativeIcebergScanMetadata = Some(metadata))
        } else {
          withInfos(scanExec, fallbackReasons.toSet)
        }

      case other =>
        withInfo(
          scanExec,
          s"Unsupported scan: ${other.getClass.getName}. " +
            "Comet Scan only supports Parquet and Iceberg Parquet file formats")
    }
  }

  private def isDynamicPruningFilter(e: Expression): Boolean =
    e.exists(_.isInstanceOf[PlanExpression[_]])

  private def isSchemaSupported(
      scanExec: FileSourceScanExec,
      scanImpl: String,
      r: HadoopFsRelation): Boolean = {
    val fallbackReasons = new ListBuffer[String]()
    val typeChecker = CometScanTypeChecker(scanImpl)
    val schemaSupported =
      typeChecker.isSchemaSupported(scanExec.requiredSchema, fallbackReasons)
    if (!schemaSupported) {
      withInfo(
        scanExec,
        s"Unsupported schema ${scanExec.requiredSchema} " +
          s"for $scanImpl: ${fallbackReasons.mkString(", ")}")
      return false
    }
    val partitionSchemaSupported =
      typeChecker.isSchemaSupported(r.partitionSchema, fallbackReasons)
    if (!partitionSchemaSupported) {
      withInfo(
        scanExec,
        s"Unsupported partitioning schema ${scanExec.requiredSchema} " +
          s"for $scanImpl: ${fallbackReasons
              .mkString(", ")}")
      return false
    }
    true
  }
}

case class CometScanTypeChecker(scanImpl: String) extends DataTypeSupport with CometTypeShim {

  // this class is intended to be used with a specific scan impl
  assert(scanImpl != CometConf.SCAN_AUTO)

  override def isTypeSupported(
      dt: DataType,
      name: String,
      fallbackReasons: ListBuffer[String]): Boolean = {
    dt match {
      case ShortType if CometConf.COMET_PARQUET_UNSIGNED_SMALL_INT_CHECK.get() =>
        fallbackReasons += s"$scanImpl scan may not handle unsigned UINT_8 correctly for $dt. " +
          s"Set ${CometConf.COMET_PARQUET_UNSIGNED_SMALL_INT_CHECK.key}=false to allow " +
          "native execution if your data does not contain unsigned small integers. " +
          CometConf.COMPAT_GUIDE
        false
      case dt if isStringCollationType(dt) =>
        // we don't need specific support for collation in scans, but this
        // is a convenient place to force the whole query to fall back to Spark for now
        false
      case s: StructType if s.fields.isEmpty =>
        false
      case _ =>
        super.isTypeSupported(dt, name, fallbackReasons)
    }
  }
}

object CometScanRule extends Logging {

  /**
   * Validating object store configs can cause requests to be made to S3 APIs (such as when
   * resolving the region for a bucket). We use a cache to reduce the number of S3 calls.
   *
   * The key is the config map converted to a string. The value is the reason that the config is
   * not valid, or None if the config is valid.
   */
  val configValidityMap = new mutable.HashMap[String, Option[String]]()

  /**
   * We do not expect to see a large number of unique configs within the lifetime of a Spark
   * session, but we reset the cache once it reaches a fixed size to prevent it growing
   * indefinitely.
   */
  val configValidityMapMaxSize = 1024

  def validateObjectStoreConfig(
      filePath: String,
      hadoopConf: Configuration,
      fallbackReasons: mutable.ListBuffer[String]): Unit = {
    val objectStoreConfigMap =
      NativeConfig.extractObjectStoreOptions(hadoopConf, URI.create(filePath))

    val cacheKey = objectStoreConfigMap
      .map { case (k, v) =>
        s"$k=$v"
      }
      .toList
      .sorted
      .mkString("\n")

    if (configValidityMap.size >= configValidityMapMaxSize) {
      logWarning("Resetting S3 object store validity cache")
      configValidityMap.clear()
    }

    configValidityMap.get(cacheKey) match {
      case Some(Some(reason)) =>
        fallbackReasons += reason
      case Some(None) =>
      // previously validated
      case _ =>
        try {
          val objectStoreOptions = objectStoreConfigMap.asJava
          Native.validateObjectStoreConfig(filePath, objectStoreOptions)
        } catch {
          case e: CometNativeException =>
            val reason = "Object store config not supported by " +
              s"$SCAN_NATIVE_ICEBERG_COMPAT: ${e.getMessage}"
            fallbackReasons += reason
            configValidityMap.put(cacheKey, Some(reason))
        }
    }

  }

  /**
   * Single-pass validation of Iceberg FileScanTasks.
   *
   * Consolidates file format, filesystem scheme, residual transform, and delete file checks into
   * one iteration for better performance with large tables.
   */
  def validateIcebergFileScanTasks(tasks: java.util.List[_]): IcebergTaskValidationResult = {
    // scalastyle:off classforname
    val contentScanTaskClass = Class.forName(IcebergReflection.ClassNames.CONTENT_SCAN_TASK)
    val contentFileClass = Class.forName(IcebergReflection.ClassNames.CONTENT_FILE)
    val fileScanTaskClass = Class.forName(IcebergReflection.ClassNames.FILE_SCAN_TASK)
    val unboundPredicateClass = Class.forName(IcebergReflection.ClassNames.UNBOUND_PREDICATE)
    // scalastyle:on classforname

    // Cache all method lookups outside the loop
    val fileMethod = contentScanTaskClass.getMethod("file")
    val formatMethod = contentFileClass.getMethod("format")
    val pathMethod = contentFileClass.getMethod("path")
    val residualMethod = contentScanTaskClass.getMethod("residual")
    val deletesMethod = fileScanTaskClass.getMethod("deletes")
    val termMethod = unboundPredicateClass.getMethod("term")

    val supportedSchemes =
      Set("file", "s3", "s3a", "gs", "gcs", "oss", "abfss", "abfs", "wasbs", "wasb")

    var allParquet = true
    val unsupportedSchemes = mutable.Set[String]()
    var nonIdentityTransform: Option[String] = None
    val deleteFiles = new java.util.ArrayList[Any]()

    tasks.asScala.foreach { task =>
      val dataFile = fileMethod.invoke(task)

      // File format check
      val fileFormat = formatMethod.invoke(dataFile).toString
      if (fileFormat != IcebergReflection.FileFormats.PARQUET) {
        allParquet = false
      }

      // Filesystem scheme check for data file
      try {
        val filePath = pathMethod.invoke(dataFile).toString
        val uri = new URI(filePath)
        val scheme = uri.getScheme
        if (scheme != null && !supportedSchemes.contains(scheme)) {
          unsupportedSchemes += scheme
        }
      } catch {
        case _: java.net.URISyntaxException => // ignore
      }

      // Residual transform check (short-circuit if already found unsupported)
      if (nonIdentityTransform.isEmpty && fileScanTaskClass.isInstance(task)) {
        try {
          val residual = residualMethod.invoke(task)
          if (unboundPredicateClass.isInstance(residual)) {
            val term = termMethod.invoke(residual)
            try {
              val transformMethod = term.getClass.getMethod("transform")
              transformMethod.setAccessible(true)
              val transform = transformMethod.invoke(term)
              val transformStr = transform.toString
              if (transformStr != IcebergReflection.Transforms.IDENTITY) {
                nonIdentityTransform = Some(transformStr)
              }
            } catch {
              case _: NoSuchMethodException => // No transform = simple reference, OK
            }
          }
        } catch {
          case _: Exception => // Skip tasks where we can't get residual
        }
      }

      // Collect delete files and check their schemes
      if (fileScanTaskClass.isInstance(task)) {
        try {
          val deletes = deletesMethod.invoke(task).asInstanceOf[java.util.List[_]]
          deleteFiles.addAll(deletes)

          deletes.asScala.foreach { deleteFile =>
            IcebergReflection.extractFileLocation(contentFileClass, deleteFile).foreach {
              deletePath =>
                try {
                  val deleteUri = new URI(deletePath)
                  val deleteScheme = deleteUri.getScheme
                  if (deleteScheme != null && !supportedSchemes.contains(deleteScheme)) {
                    unsupportedSchemes += deleteScheme
                  }
                } catch {
                  case _: java.net.URISyntaxException => // ignore
                }
            }
          }
        } catch {
          case _: Exception => // ignore errors accessing delete files
        }
      }
    }

    IcebergTaskValidationResult(
      allParquet,
      unsupportedSchemes.toSet,
      nonIdentityTransform,
      deleteFiles)
  }
}

/**
 * Result of single-pass validation over Iceberg FileScanTasks.
 */
case class IcebergTaskValidationResult(
    allParquet: Boolean,
    unsupportedSchemes: Set[String],
    nonIdentityTransform: Option[String],
    deleteFiles: java.util.List[_])
