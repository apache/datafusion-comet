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

import scala.jdk.CollectionConverters._

import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.comet.{CometScanExec, DeltaPlanDataInjector}
import org.apache.spark.sql.delta.DeltaParquetFileFormat
import org.apache.spark.sql.delta.RowIndexFilterType
import org.apache.spark.sql.delta.actions.DeletionVectorDescriptor
import org.apache.spark.sql.execution.{FileSourceScanExec, ScalarSubquery => ExecScalarSubquery}
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.types.{ByteType, LongType, MetadataBuilder, StructField, StructType}

import org.apache.comet.objectstore.NativeConfig
import org.apache.comet.serde.OperatorOuterClass
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.QueryPlanSerde.{exprToProto, serializeDataType}
import org.apache.comet.serde.operator.{literalToProto, partition2Proto, schema2Proto, CometNativeScan}
import org.apache.comet.shims.ShimFileFormat

/**
 * Serde for the native Delta scan. Two shapes:
 *   - Plain reads reuse core's `NativeScanCommon` builder wholesale.
 *   - Deletion-vector reads: Delta's planner appends `__delta_internal_is_row_deleted` (tinyint)
 *     and Spark's row-index temp column (bigint) to the read schema and filters on is_row_deleted
 *     above the scan. The native reader applies the DV as a row selection, so both internal
 *     columns are emitted as per-file constants (0), the parquet read schema is stripped to the
 *     real data columns, and the DV descriptor ships per file for native to fetch and decode.
 */
object CometDeltaNativeScan
    extends Logging
    with org.apache.spark.sql.catalyst.expressions.PredicateHelper {

  val IsRowDeletedColumn: String = DeltaParquetFileFormat.IS_ROW_DELETED_COLUMN_NAME
  val RowIndexColumn: String = ShimFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME

  private[delta] val internalColumnNames: Set[String] = Set(IsRowDeletedColumn, RowIndexColumn)

  // Prefix for the internal columns' slots in the partition schema, mirroring core's
  // _comet_metadata_ prefix rationale: DataFusion matches partition columns by name.
  // [[allocateUniqueInternalFields]] additionally suffixes on collision with a real column.
  private val deltaConstFieldPrefix = "_comet_delta_"

  def isDvShape(scanExec: FileSourceScanExec): Boolean =
    scanExec.requiredSchema.exists(f => internalColumnNames.contains(f.name))

  private def deltaFormat(scanExec: FileSourceScanExec): DeltaParquetFileFormat =
    scanExec.relation.fileFormat.asInstanceOf[DeltaParquetFileFormat]

  private def columnMappingMode(scanExec: FileSourceScanExec): String =
    deltaFormat(scanExec).metadata.columnMappingMode.name

  /**
   * Under column mapping, parquet files store physical column names (stable UUIDs / ids), so the
   * schemas passed to the native parquet reader must be physical. Positions and structure are
   * preserved, so output binding and projection are unaffected. The scan's internal DV columns
   * are not part of the table schema and must be stripped before calling this.
   *
   * `private[delta]` (not `private`): [[DeltaScanSupport.declineReason]]'s non-ASCII
   * case-insensitive name gate reuses this exact conversion to compute the names native sees
   * under column mapping, rather than re-deriving physical names with separate logic.
   */
  private[delta] def toPhysical(scanExec: FileSourceScanExec, schema: StructType): StructType = {
    val format = deltaFormat(scanExec)
    if (format.metadata.columnMappingMode.name == "none") {
      schema
    } else {
      // Name mode matches file columns by physical NAME. Strip the parquet.field.id metadata
      // createPhysicalSchema also stamps: files written before the column-mapping upgrade have
      // no field ids and would fail the reader's id expectations.
      stripFieldIds(org.apache.spark.sql.delta.DeltaColumnMapping
        .createPhysicalSchema(schema, format.metadata.schema, format.metadata.columnMappingMode))
    }
  }

  private def stripFieldIds(schema: StructType): StructType = {
    import org.apache.spark.sql.types._
    def stripType(dt: DataType): DataType = dt match {
      case s: StructType => stripFieldIds(s)
      case a: ArrayType => a.copy(elementType = stripType(a.elementType))
      case m: MapType =>
        m.copy(keyType = stripType(m.keyType), valueType = stripType(m.valueType))
      case other => other
    }
    StructType(schema.fields.map { f =>
      val metadata = new MetadataBuilder()
        .withMetadata(f.metadata)
        .remove("parquet.field.id")
        // Sibling key Delta stamps on array/map fields under IcebergCompat/Uniform.
        .remove("parquet.field.nested.ids")
        .build()
      f.copy(dataType = stripType(f.dataType), metadata = metadata)
    })
  }

  /**
   * Build the planning-time `DeltaScan` operator (common data only; file partitions are injected
   * lazily at execution). Returns None when an output data type cannot be serialized or the plan
   * shape is not one we can translate faithfully. `memo` is the same claim-memo instance
   * [[DeltaScanSupport.declineReason]] populated on this claim; its `hadoopConf` and
   * `dvDescriptors` are reused here rather than recomputed.
   */
  def convert(
      scanExec: FileSourceScanExec,
      scanHelper: CometScanExec,
      memo: DeltaScanSupport.DeltaClaimMemo): Option[Operator] = {
    val relation = scanExec.relation

    val firstFileUri = scanHelper.selectedPartitions
      .flatMap(_.files.headOption)
      .headOption
      .map(_.getPath.toUri)

    val hadoopConf = memo.hadoopConf

    val tableRootPath = relation.location.rootPaths.head
    val tableRoot = tableRootPath.toString

    val commonOpt = if (!isDvShape(scanExec)) {
      // Under column mapping (name mode) the parquet reader must see physical names;
      // positions are preserved so output binding and projection stay untouched.
      CometNativeScan.buildNativeScanCommon(
        source = scanExec.simpleStringWithNodeId(),
        output = scanExec.output,
        requiredSchema = toPhysical(scanExec, scanExec.requiredSchema),
        dataSchema = toPhysical(scanExec, relation.dataSchema),
        partitionSchema = toPhysical(scanExec, relation.partitionSchema),
        fileConstantMetadataColumns = scanExec.fileConstantMetadataColumns,
        dataFilters = scanHelper.supportedDataFilters,
        firstFileUri = firstFileUri,
        hadoopConf = hadoopConf,
        conf = scanExec.conf)
    } else {
      buildDvScanCommon(scanExec, scanHelper, firstFileUri, hadoopConf)
    }

    commonOpt.map { commonBuilder =>
      // Already forced by declineReason on this claim; reused rather than deserialized again.
      val dvDescriptors = memo.dvDescriptors
      // Union object-store options over every authority a partition of this scan may need a
      // store for, not just the first data file's scheme.
      commonBuilder.putAllObjectStoreOptions(
        mergedObjectStoreOptions(
          hadoopConf,
          storeUris(dvDescriptors, tableRootPath, firstFileUri)).asJava)

      val common = commonBuilder.build()
      // Effective session rebase read modes, resolved through ParquetOptions exactly as
      // ParquetFileFormat.buildReaderWithPartitionValues resolves them (per-relation
      // `datetimeRebaseMode` / `int96RebaseMode` options win over the session conf, whose
      // per-Spark-version default -- EXCEPTION on 3.x, CORRECTED on 4.0 -- SQLConf supplies).
      // Native consults them only for files whose footer metadata does not decide the rebase
      // policy on its own, mirroring DataSourceUtils.getRebaseSpec's modeByConfig fallback.
      val parquetReadOptions =
        new org.apache.spark.sql.execution.datasources.parquet.ParquetOptions(
          relation.options,
          scanExec.conf)
      val deltaCommon = OperatorOuterClass.DeltaSparkScanCommon
        .newBuilder()
        .setTableRoot(tableRoot)
        .setColumnMappingMode(columnMappingMode(scanExec))
        .setSourceKey(DeltaPlanDataInjector.sourceKey(tableRoot, common))
        .setDatetimeRebaseModeInRead(parquetReadOptions.datetimeRebaseModeInRead)
        .setInt96RebaseModeInRead(parquetReadOptions.int96RebaseModeInRead)
        .build()
      val deltaScan = OperatorOuterClass.DeltaSparkScan
        .newBuilder()
        .setCommon(common)
        .setDeltaCommon(deltaCommon)
      Operator
        .newBuilder()
        .setPlanId(scanExec.id)
        .setContribScan(DeltaSparkScanEnvelope.pack(deltaScan.build()))
        .build()
    }
  }

  /**
   * One representative store URI per distinct object-store authority this scan's partitions may
   * need options for: the data-file authority (`firstFileUri`), the table root unconditionally
   * (UUID-relative DV sidecars resolve against it), and every distinct on-disk DV authority from
   * `descriptors` (inline DVs carry no external URI and are filtered out). Deduping by authority
   * rather than full URI keeps this O(distinct authorities) instead of O(files), keeping the
   * FIRST URI seen per authority so `firstFileUri`/the table root win over a same-authority DV
   * path.
   */
  private[delta] def storeUris(
      descriptors: Seq[DeletionVectorDescriptor],
      tableRootPath: Path,
      firstFileUri: Option[java.net.URI]): Seq[java.net.URI] = {
    val dvAuthorityUris = descriptors
      .filter(_.storageType != DeletionVectorDescriptor.INLINE_DV_MARKER)
      .map(_.absolutePath(tableRootPath).toUri)
    val candidates = firstFileUri.toSeq ++ Seq(tableRootPath.toUri) ++ dvAuthorityUris
    val byAuthority = scala.collection.mutable.LinkedHashMap.empty[String, java.net.URI]
    candidates.foreach(uri =>
      byAuthority.getOrElseUpdate(DeltaScanSupport.uriAuthority(uri), uri))
    byAuthority.values.toSeq
  }

  /**
   * Unions `NativeConfig.extractObjectStoreOptions` over every `uris` authority. Safe to union
   * rather than pick one: extracted keys are scheme-disjoint prefixes (`fs.s3a.*` vs
   * `fs.azure.*`, ...), so options for different schemes never collide, and re-extracting the
   * same scheme from two URIs is idempotent.
   */
  private[delta] def mergedObjectStoreOptions(
      hadoopConf: org.apache.hadoop.conf.Configuration,
      uris: Seq[java.net.URI]): Map[String, String] =
    uris.foldLeft(Map.empty[String, String]) { (merged, uri) =>
      merged ++ NativeConfig.extractObjectStoreOptions(hadoopConf, uri)
    }

  /**
   * Harvest subquery-bearing predicates for this scan from its covering FilterExec. Spark 3.x
   * strips them from a scan's `dataFilters` at planning (`FileSourceStrategy` routes them to the
   * post-scan filter only), while Spark 4.x keeps them in `dataFilters`; collecting them here at
   * claim time gives the execution-time resolve-and-push path the same inputs on every version,
   * and the dedup below keeps Spark 4.x from carrying duplicates. Reference containment alone
   * does not prove pushing a predicate down is semantics-preserving, so `spineToScan` also
   * requires every intervening operator to commute with the push (an intervening
   * LIMIT/Sort/Aggregate/join etc. stops the walk and leaves the filter where Spark placed it:
   * missed pruning only).
   */
  def subqueryFiltersFromParent(
      plan: org.apache.spark.sql.execution.SparkPlan,
      scanExec: FileSourceScanExec): Seq[org.apache.spark.sql.catalyst.expressions.Expression] = {
    import org.apache.spark.sql.catalyst.expressions.{PlanExpression, SubqueryExpression}
    import org.apache.spark.sql.execution.{FilterExec, ProjectExec, SparkPlan}

    // Whether every node from `node` down to `scanExec` is one pushdown can safely cross: a
    // deterministic ProjectExec is 1:1 on rows and a deterministic FilterExec only removes rows,
    // so moving a predicate over the scan's output through either preserves semantics -- mirroring
    // Spark's own PushPredicateThroughNonJoin/CollapseProject rules. A nondeterministic node (or
    // anything else: LIMIT/TopN, Sort, Aggregate, Window, joins, ...) can change which rows survive
    // to matter, so it stops the walk and the filter is left uncollected (missed pruning only).
    def spineToScan(node: SparkPlan): Boolean = node match {
      case n if n eq scanExec => true
      case p: ProjectExec if p.projectList.forall(_.deterministic) => spineToScan(p.child)
      case f: FilterExec if f.condition.deterministic => spineToScan(f.child)
      case _ => false
    }

    // Nearest FilterExec whose spine down to the scan is Project/Filter-only (the DV shape
    // interposes such nodes between them, so do not require a direct parent-child edge).
    val filtersAboveScan = plan.collect {
      case f: FilterExec if spineToScan(f.child) => f
    }
    filtersAboveScan.lastOption
      .map { f =>
        splitConjunctivePredicates(f.condition)
          .filter(_.deterministic)
          .filter(_.references.subsetOf(scanExec.outputSet))
          .filter(p =>
            SubqueryExpression.hasSubquery(p) || p.exists(_.isInstanceOf[PlanExpression[_]]))
          .filterNot(p => scanExec.dataFilters.exists(_.semanticEquals(p)))
      }
      .getOrElse(Seq.empty)
  }

  /**
   * Execution-time scalar-subquery data filters of a scan. `hasResolvedFilters` is true whenever
   * pushdown is enabled and such filters exist, whether or not they bind or serialize; `protos`
   * holds only the ones that serialized.
   */
  case class ResolvedSubqueryFilters(
      hasResolvedFilters: Boolean,
      protos: Seq[org.apache.comet.serde.ExprOuterClass.Expr])

  private val NoResolvedSubqueryFilters = ResolvedSubqueryFilters(false, Seq.empty)

  /**
   * Resolve scalar-subquery data filters at execution time and serialize them for native
   * pushdown, mirroring `CometNativeScanExec.serializedPartitionData`. `supportedDataFilters`
   * excludes PlanExpressions at planning time (subquery results do not exist yet), so these
   * bounds reach the native reader only through this path. Filters that fail to serialize are
   * skipped: Spark keeps a covering FilterExec above the scan, so this is missed pruning only.
   * Their presence is still reported, since native keys the safe timestamp conversion on the scan
   * being filtered at all, as the core scan does for its resolved filters.
   *
   * Known core-parity limitation: when fused under a parent native operator,
   * `ensureSubqueriesResolved` has already called `updateResult()` on these subqueries and this
   * path calls it again (`ScalarSubquery.updateResult` re-executes unconditionally); benign here
   * since the subquery's snapshot is pinned at analysis, but wasteful. Fix belongs in core.
   */
  def resolvedSubqueryFilters(
      dataFilters: Seq[org.apache.spark.sql.catalyst.expressions.Expression],
      output: Seq[org.apache.spark.sql.catalyst.expressions.Attribute],
      requiredSchema: StructType,
      conf: org.apache.spark.sql.internal.SQLConf): ResolvedSubqueryFilters = {
    if (!conf.getConf(org.apache.spark.sql.internal.SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED)) {
      return NoResolvedSubqueryFilters
    }
    val subqueryFilters = dataFilters.filter(_.exists(_.isInstanceOf[ExecScalarSubquery]))
    if (subqueryFilters.isEmpty) {
      return NoResolvedSubqueryFilters
    }
    // Same binding guard as the DV shape's plan-time filters: references limited to the
    // data-column prefix of the output, where positions agree with the native read schema.
    // Guard BEFORE updateResult so discarded filters never execute their subqueries.
    val strippedLen = requiredSchema.count(f => !internalColumnNames.contains(f.name))
    val dataColIds = output.take(strippedLen).map(_.exprId).toSet
    val pushableFilters =
      subqueryFilters.filter(_.references.forall(r => dataColIds.contains(r.exprId)))
    pushableFilters.foreach(_.foreach {
      case s: ExecScalarSubquery => s.updateResult()
      case _ =>
    })
    val protos = pushableFilters
      .flatMap { filter =>
        // MergeScalarSubqueries can fuse several scalar subqueries into one struct-returning
        // subquery accessed via GetStructField; fold that whole subtree to a literal (a bare
        // GetStructField-over-Literal would not serialize).
        val resolved = filter.transform {
          case g @ org.apache.spark.sql.catalyst.expressions
                .GetStructField(_: ExecScalarSubquery, _, _) =>
            Literal.create(g.eval(null), g.dataType)
          case s: ExecScalarSubquery =>
            Literal.create(s.eval(null), s.dataType)
        }
        val proto = exprToProto(resolved, output)
        if (proto.isEmpty) {
          logWarning(s"Could not serialize resolved scalar subquery filter: $resolved")
        }
        proto
      }
    ResolvedSubqueryFilters(hasResolvedFilters = true, protos)
  }

  /**
   * Allocate the partition-schema slots for the DV shape's internal columns
   * (`internalColumnNames`), with names collision-free against the physical data schema, the
   * physical partition schema, and the constant-metadata slots already allocated for this scan
   * (plus each other): DataFusion substitutes partition constants BY NAME, so an unprefixed,
   * un-uniquified slot could collide with a real column and silently replace its data with the
   * bookkeeping constant. `buildDvScanCommon` keys `internalIndexByName` by each field's ORIGINAL
   * name from `requiredSchema`, so the renaming here only changes the proto's field name.
   */
  private[delta] def allocateUniqueInternalFields(
      requiredSchema: StructType,
      physicalDataSchema: StructType,
      physicalPartitionSchema: StructType,
      constantMetadataFields: Seq[StructField]): Seq[StructField] = {
    val reserved = scala.collection.mutable.LinkedHashSet[String]()
    reserved ++= physicalDataSchema.fields.map(_.name)
    reserved ++= physicalPartitionSchema.fields.map(_.name)
    reserved ++= constantMetadataFields.map(_.name)
    requiredSchema.fields.toSeq
      .filter(f => internalColumnNames.contains(f.name))
      .map { f =>
        var name = s"$deltaConstFieldPrefix${f.name}"
        while (reserved.contains(name)) {
          name = name + "_"
        }
        reserved += name
        StructField(name, f.dataType, f.nullable)
      }
  }

  /**
   * DV shape common builder. Layout invariants (declined by DeltaScanSupport when violated): scan
   * output = requiredSchema attrs (data columns, then the internal columns as a suffix) followed
   * by partition and constant-metadata columns. The parquet read schema strips the internal
   * columns; they are appended to the partition schema as per-file constants, so the projection
   * vector routes them from the constants block.
   */
  private def buildDvScanCommon(
      scanExec: FileSourceScanExec,
      scanHelper: CometScanExec,
      firstFileUri: Option[java.net.URI],
      hadoopConf: org.apache.hadoop.conf.Configuration)
      : Option[OperatorOuterClass.NativeScanCommon.Builder] = {
    val relation = scanExec.relation
    val output = scanExec.output
    val requiredSchema = scanExec.requiredSchema

    val commonBuilder = OperatorOuterClass.NativeScanCommon.newBuilder()
    commonBuilder.setSource(scanExec.simpleStringWithNodeId())

    val scanTypes = output.flatMap(attr => serializeDataType(attr.dataType))
    if (scanTypes.length != output.length) {
      return None
    }
    commonBuilder.addAllFields(scanTypes.asJava)

    val strippedRequired =
      StructType(requiredSchema.filterNot(f => internalColumnNames.contains(f.name)))
    val strippedLen = strippedRequired.length
    val requiredLen = requiredSchema.length

    // Keep only data filters that bind identically in the output and the native index space:
    // references limited to the first strippedLen output attributes. Internal-column filters
    // (is_row_deleted = 0) are trivially true after native DV application.
    if (scanExec.conf.getConf(
        org.apache.spark.sql.internal.SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED)) {
      commonBuilder.setHasDataFilters(scanHelper.supportedDataFilters.nonEmpty)
      val dataColIds = output.take(strippedLen).map(_.exprId).toSet
      val filterProtos = scanHelper.supportedDataFilters
        .filter(_.references.forall(r => dataColIds.contains(r.exprId)))
        .flatMap(f => exprToProto(f, output))
      commonBuilder.addAllDataFilters(filterProtos.asJava)
    }

    // Real partition columns carry physical names in the proto, same as the data/required
    // schemas: a retained physical data name can otherwise collide with a partition column's
    // LOGICAL name after a rename history, and DataFusion's by-name partition rewrite would then
    // replace the data projection with the partition constant. constantMetadataFields/
    // internalFields are synthetic slots, not table columns, so they are not physicalized.
    val physicalDataSchema = toPhysical(scanExec, relation.dataSchema)
    val physicalPartitionSchema = toPhysical(scanExec, relation.partitionSchema)
    // Constant metadata and real partition columns follow the required schema in the output,
    // exactly like the plain shape. Names are uniquified against the physical data/partition
    // schemas for the same by-name-collision reason [[allocateUniqueInternalFields]] exists.
    val constantMetadataFields = CometNativeScan.uniqueConstantMetadataFields(
      scanExec.fileConstantMetadataColumns,
      physicalDataSchema.fields.map(_.name).toSet ++ physicalPartitionSchema.fields
        .map(_.name)
        .toSet)
    val internalFields = allocateUniqueInternalFields(
      requiredSchema,
      physicalDataSchema = physicalDataSchema,
      physicalPartitionSchema = physicalPartitionSchema,
      constantMetadataFields = constantMetadataFields)
    val partitionSchemaFields =
      physicalPartitionSchema.fields.toSeq ++ constantMetadataFields ++ internalFields

    // Protos carry physical names (column mapping); index math below stays logical.
    val partitionSchemaProto = schema2Proto(partitionSchemaFields)
    val requiredSchemaProto = schema2Proto(toPhysical(scanExec, strippedRequired))
    val dataSchemaProto = schema2Proto(physicalDataSchema)

    // Projection: data columns from the (stripped) read schema; internal columns from their
    // constants slots at the END of the partition fields; the output tail (real partitions +
    // constant metadata) positionally from the head of the partition fields.
    val dataSchema = relation.dataSchema
    val internalBase = dataSchema.length + partitionSchemaFields.length - internalFields.length
    val internalIndexByName = requiredSchema.fields.toSeq
      .filter(f => internalColumnNames.contains(f.name))
      .zipWithIndex
      .map { case (f, i) => f.name -> (internalBase + i) }
      .toMap
    val projectionVector = output.zipWithIndex.map { case (attr, i) =>
      val idx = if (internalColumnNames.contains(attr.name)) {
        internalIndexByName(attr.name)
      } else if (i < requiredLen) {
        dataSchema.fieldIndex(attr.name)
      } else {
        dataSchema.length + (i - requiredLen)
      }
      idx.toLong.asInstanceOf[java.lang.Long]
    }
    commonBuilder.addAllProjectionVector(projectionVector.asJava)

    commonBuilder.addAllDataSchema(dataSchemaProto.asJava)
    commonBuilder.addAllRequiredSchema(requiredSchemaProto.asJava)
    commonBuilder.addAllPartitionSchema(partitionSchemaProto.asJava)

    CometNativeScan.populateScanConfFlags(
      commonBuilder,
      strippedRequired,
      firstFileUri,
      hadoopConf,
      scanExec.conf)

    Some(commonBuilder)
  }

  /** Serialize one file partition into a DeltaSparkScan proto with per-file DV descriptors. */
  def serializePartition(
      filePartition: FilePartition,
      scanExec: FileSourceScanExec,
      tableRoot: String): Array[Byte] = {
    val relation = scanExec.relation
    val sparkPartition = partition2Proto(
      filePartition,
      relation.partitionSchema,
      scanExec.fileConstantMetadataColumns,
      ShimFileFormat.fileConstantMetadataExtractors(relation.fileFormat))

    val dvShape = isDvShape(scanExec)

    val deltaPartition = OperatorOuterClass.DeltaSparkFilePartition.newBuilder()
    sparkPartition.getPartitionedFileList.asScala.zip(filePartition.files.toSeq).foreach {
      case (fileProto, file) =>
        val fileBuilder = fileProto.toBuilder
        if (dvShape) {
          // Append the internal-constant values after the real partition/constant-metadata
          // values, matching the order of the appended partition-schema fields.
          scanExec.requiredSchema.fields
            .filter(f => internalColumnNames.contains(f.name))
            .foreach { f =>
              val lit = f.dataType match {
                case ByteType => Literal(0.toByte, ByteType)
                case LongType => Literal(0L, LongType)
                case other =>
                  // Fixed internal invariant (observed Delta 3.3 types); fail loudly on
                  // drift rather than emit a plausible-looking constant.
                  throw new IllegalStateException(
                    s"Unexpected type $other for Delta internal column ${f.name}")
              }
              fileBuilder.addPartitionValues(
                literalToProto(lit, s"delta internal constant ${f.name}"))
            }
        }
        val dfb = OperatorOuterClass.DeltaSparkPartitionedFile
          .newBuilder()
          .setFile(fileBuilder.build())
        extractDvDescriptor(file, tableRoot).foreach(dfb.setDv)
        deltaPartition.addPartitionedFile(dfb.build())
    }

    OperatorOuterClass.DeltaSparkScan
      .newBuilder()
      .setFilePartition(deltaPartition.build())
      .build()
      .toByteArray
  }

  /**
   * Pull the DV descriptor Delta attached to this file (base64 under
   * `row_index_filter_id_encoded`), resolving UUID-relative paths to absolute URLs and
   * Z85-decoding inline bitmaps here on the JVM where delta-spark's codecs live.
   */
  private def extractDvDescriptor(
      file: PartitionedFile,
      tableRoot: String): Option[OperatorOuterClass.DeltaSparkDvDescriptor] = {
    val encoded = file.otherConstantMetadataColumnValues
      .get(DeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_ID_ENCODED)
    val filterType = file.otherConstantMetadataColumnValues
      .get(DeltaParquetFileFormat.FILE_ROW_INDEX_FILTER_TYPE)
    encoded.map { enc =>
      filterType match {
        case Some(RowIndexFilterType.IF_CONTAINED) | None =>
        case other =>
          // DeltaScanSupport declines CDF reads, the only source of inverted filters;
          // reaching here means a gate was bypassed -- fail loudly rather than corrupt.
          throw new IllegalStateException(
            s"Native Delta scan cannot apply row index filter type $other")
      }
      val desc = DeletionVectorDescriptor.deserializeFromBase64(enc.asInstanceOf[String])
      val builder = OperatorOuterClass.DeltaSparkDvDescriptor
        .newBuilder()
        .setStorageType(desc.storageType)
        .setSizeInBytes(desc.sizeInBytes)
        .setCardinality(desc.cardinality)
      if (desc.storageType == DeletionVectorDescriptor.INLINE_DV_MARKER) {
        // Delegates to core, which owns the shaded/relocated dependency this field's setter
        // is generated against, so this module's source never has to name that package.
        CometNativeScan.setDvInlineData(builder, desc.inlineData)
      } else {
        // Same convention as data-file paths (SparkPath.urlEncoded): a raw Hadoop path
        // with spaces or % characters would be mangled by the native URL parse.
        builder.setAbsolutePath(
          org.apache.spark.paths.SparkPath
            .fromPath(desc.absolutePath(new Path(tableRoot)))
            .urlEncoded)
        desc.offset.foreach(builder.setOffset)
      }
      builder.build()
    }
  }
}
