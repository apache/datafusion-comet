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
 *     above the scan. The native reader applies the DV as a row selection, so surviving rows are
 *     by construction not deleted: both internal columns are emitted as per-file constants (0),
 *     the parquet read schema is stripped to the real data columns, and the DV descriptor ships
 *     per file for the native side to fetch and decode.
 */
object CometDeltaNativeScan
    extends Logging
    with org.apache.spark.sql.catalyst.expressions.PredicateHelper {

  val IsRowDeletedColumn: String = DeltaParquetFileFormat.IS_ROW_DELETED_COLUMN_NAME
  val RowIndexColumn: String = ShimFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME

  private[delta] val internalColumnNames: Set[String] = Set(IsRowDeletedColumn, RowIndexColumn)

  // Prefix for the internal columns' slots in the partition schema, mirroring core's
  // _comet_metadata_ prefix rationale: DataFusion matches partition columns by name.
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
   * preserved, so all positional output binding and projection are unaffected. The scan's
   * internal DV columns are not part of the table schema and must be stripped before calling
   * this.
   */
  private def toPhysical(scanExec: FileSourceScanExec, schema: StructType): StructType = {
    val format = deltaFormat(scanExec)
    if (format.metadata.columnMappingMode.name == "none") {
      schema
    } else {
      // Name mode matches file columns by physical NAME. createPhysicalSchema also stamps
      // parquet.field.id metadata, but files written before the column-mapping upgrade have
      // no field ids and would fail the reader's id expectations, strip the ids so the
      // reader stays purely name-based (id mode, when enabled, will keep them).
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
   * shape is not one we can translate faithfully.
   */
  def convert(scanExec: FileSourceScanExec, scanHelper: CometScanExec): Option[Operator] = {
    val relation = scanExec.relation

    val firstFileUri = scanHelper.selectedPartitions
      .flatMap(_.files.headOption)
      .headOption
      .map(_.getPath.toUri)

    val hadoopConf = relation.sparkSession.sessionState
      .newHadoopConfWithOptions(relation.options)

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
        partitionSchema = relation.partitionSchema,
        fileConstantMetadataColumns = scanExec.fileConstantMetadataColumns,
        dataFilters = scanHelper.supportedDataFilters,
        firstFileUri = firstFileUri,
        hadoopConf = hadoopConf,
        conf = scanExec.conf)
    } else {
      buildDvScanCommon(scanExec, scanHelper, firstFileUri, hadoopConf)
    }

    commonOpt.map { commonBuilder =>
      // Union object-store options over every authority a partition of this scan may need a
      // store for, not just the first data file's scheme (finding 8).
      val dvDescriptors = DeltaScanSupport.selectedDvDescriptors(scanHelper, tableRoot)
      commonBuilder.putAllObjectStoreOptions(
        mergedObjectStoreOptions(
          hadoopConf,
          storeUris(dvDescriptors, tableRootPath, firstFileUri)).asJava)

      val common = commonBuilder.build()
      val deltaCommon = OperatorOuterClass.DeltaSparkScanCommon
        .newBuilder()
        .setTableRoot(tableRoot)
        .setColumnMappingMode(columnMappingMode(scanExec))
        .setSourceKey(DeltaPlanDataInjector.sourceKey(tableRoot, common))
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
   * (UUID-relative DV sidecars resolve against it, and it is cheap to include even when absent),
   * and every distinct on-disk DV authority from `descriptors`. Inline DVs are filtered out --
   * they carry no external URI, only embedded bytes. Deduping by authority (rather than by full
   * URI) keeps this O(distinct authorities) instead of O(files): a table with N deletion-vector
   * files on the same external store previously produced ~N distinct URIs here, each
   * independently fed into `mergedObjectStoreOptions`'s `extractObjectStoreOptions` walk over
   * `hadoopConf`. Candidates are deduped keeping the FIRST URI seen per authority, so callers can
   * rely on `firstFileUri`/the table root winning over any DV path that happens to share their
   * authority. Factored out of [[convert]] so the URI-assembly logic (in particular the
   * `storageType` filter and `absolutePath` resolution) is directly unit-testable with hand-built
   * [[DeletionVectorDescriptor]] fixtures, without a Spark session or real selected files (a
   * `file://` scan alone can't exercise a foreign-authority DV).
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
   * Unions `NativeConfig.extractObjectStoreOptions` over every `uris` authority. Safe to simply
   * union rather than pick one: the extracted keys are scheme-disjoint prefixes (`fs.s3a.*` vs
   * `fs.azure.*`, ...), so options for different schemes never collide, and re-extracting the
   * same scheme from two URIs is idempotent. Factored out of [[convert]] so it is directly
   * unit-testable without a Spark session.
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
   * post-scan filter only), while Spark 4.x keeps them in `dataFilters`. Collecting them here at
   * claim time gives the execution-time resolve-and-push path the same inputs on every Spark
   * version; the dedup keeps Spark 4.x from carrying duplicates.
   *
   * Safety comes from the plan walk, not just the reference guard: a filter is only harvested
   * when every operator between it and the scan commutes with pushing the predicate into the scan
   * (see `spineToScan`). Reference containment alone proves the predicate is expressible over the
   * scan's output, not that moving it there is semantics-preserving -- an intervening LIMIT/TopN
   * (or Sort, Aggregate, Window, join, ...) can change which rows the predicate would have
   * applied to, so those stop the walk and the filter is left where Spark placed it.
   */
  def subqueryFiltersFromParent(
      plan: org.apache.spark.sql.execution.SparkPlan,
      scanExec: FileSourceScanExec): Seq[org.apache.spark.sql.catalyst.expressions.Expression] = {
    import org.apache.spark.sql.catalyst.expressions.{PlanExpression, SubqueryExpression}
    import org.apache.spark.sql.execution.{FilterExec, ProjectExec, SparkPlan}

    // Whether every node on the path from `node` down to `scanExec` is one that pushdown can
    // safely cross: a deterministic ProjectExec is 1:1 on rows (an Alias mints a new exprId, so
    // it cannot alias over the scan's own output attributes, which is what the reference guard
    // below requires) and a deterministic FilterExec only removes rows, so moving a predicate
    // expressed over the scan's output through either preserves the query's semantics. A
    // nondeterministic projection breaks that: a deterministic conjunct does not commute with it,
    // because pushing the predicate into the scan changes which rows survive to have
    // nondeterministic expressions (e.g. monotonically_increasing_id()) evaluated over them,
    // changing the result -- so both guards require `.deterministic`, mirroring Spark's own
    // PushPredicateThroughNonJoin/CollapseProject rules. Anything else (LIMIT/TopN, Sort,
    // Aggregate, Window, joins, Union, Sample, ...) can reorder or drop rows in ways that make
    // "push the predicate down to the scan" change the result, so an unrecognized node stops the
    // walk: the filter is left uncollected (missed pruning only, never a correctness issue).
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
   * Resolve scalar-subquery data filters at execution time and serialize them for native
   * pushdown, mirroring `CometNativeScanExec.serializedPartitionData`. `supportedDataFilters`
   * excludes PlanExpressions at planning time (subquery results do not exist yet), so these
   * bounds reach the native reader only through this path. Filters that fail to serialize are
   * skipped: Spark keeps a covering FilterExec above the scan, so this is missed pruning only,
   * never a correctness issue.
   *
   * Known core-parity limitation: when the scan is fused under a parent native operator,
   * `ensureSubqueriesResolved` has already called `updateResult()` on these subqueries and this
   * path calls it again (Spark's ScalarSubquery.updateResult re-executes unconditionally). Core's
   * CometNativeScanExec has the identical double-execution; benign for Delta (the subquery's
   * snapshot is pinned at analysis), wasteful for expensive subqueries. Fix belongs in core.
   */
  def resolvedSubqueryFilters(
      dataFilters: Seq[org.apache.spark.sql.catalyst.expressions.Expression],
      output: Seq[org.apache.spark.sql.catalyst.expressions.Attribute],
      requiredSchema: StructType,
      conf: org.apache.spark.sql.internal.SQLConf)
      : Seq[org.apache.comet.serde.ExprOuterClass.Expr] = {
    if (!conf.getConf(org.apache.spark.sql.internal.SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED)) {
      return Seq.empty
    }
    val subqueryFilters = dataFilters.filter(_.exists(_.isInstanceOf[ExecScalarSubquery]))
    if (subqueryFilters.isEmpty) {
      return Seq.empty
    }
    // Same binding guard as the DV shape's plan-time filters: references limited to the
    // data-column prefix of the output, where positions agree between the output and the
    // native read schema. For the plain shape strippedLen is the full required schema.
    // Guard BEFORE updateResult so discarded filters never execute their subqueries.
    val strippedLen = requiredSchema.count(f => !internalColumnNames.contains(f.name))
    val dataColIds = output.take(strippedLen).map(_.exprId).toSet
    val pushableFilters =
      subqueryFilters.filter(_.references.forall(r => dataColIds.contains(r.exprId)))
    pushableFilters.foreach(_.foreach {
      case s: ExecScalarSubquery => s.updateResult()
      case _ =>
    })
    pushableFilters
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

    // Keep only data filters that bind identically in the output and the native
    // (strippedRequired ++ partitionFields) index spaces: references limited to the first
    // strippedLen output attributes. Internal-column filters (is_row_deleted = 0) are
    // trivially true after native DV application, and Spark's Filter above the scan
    // re-evaluates everything anyway.
    if (scanExec.conf.getConf(
        org.apache.spark.sql.internal.SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED)) {
      val dataColIds = output.take(strippedLen).map(_.exprId).toSet
      val filterProtos = scanHelper.supportedDataFilters
        .filter(_.references.forall(r => dataColIds.contains(r.exprId)))
        .flatMap(f => exprToProto(f, output))
      commonBuilder.addAllDataFilters(filterProtos.asJava)
    }

    // Constant metadata columns and real partition columns follow the required schema in the
    // output, exactly like the plain shape.
    val constantMetadataFields = scanExec.fileConstantMetadataColumns.map(attr =>
      StructField(
        s"${CometNativeScan.constantMetadataFieldPrefix}${attr.name}",
        attr.dataType,
        attr.nullable))
    val internalFields = requiredSchema.fields.toSeq
      .filter(f => internalColumnNames.contains(f.name))
      .map(f => StructField(s"$deltaConstFieldPrefix${f.name}", f.dataType, f.nullable))
    val partitionSchemaFields = relation.partitionSchema.fields.toSeq ++
      constantMetadataFields ++ internalFields

    // Protos carry physical names (column mapping); index math below stays logical.
    val partitionSchemaProto = schema2Proto(partitionSchemaFields)
    val requiredSchemaProto = schema2Proto(toPhysical(scanExec, strippedRequired))
    val dataSchemaProto = schema2Proto(toPhysical(scanExec, relation.dataSchema))

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
        // The comet-spark jar relocates protobuf, so use the shaded ByteString.
        builder.setInlineData(
          org.apache.comet.shaded.protobuf.ByteString.copyFrom(desc.inlineData))
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
