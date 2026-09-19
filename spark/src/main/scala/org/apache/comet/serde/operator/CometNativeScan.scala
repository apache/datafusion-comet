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

package org.apache.comet.serde.operator

import java.net.URI

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, Literal}
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns.getExistenceDefaultValues
import org.apache.spark.sql.comet.{CometNativeExec, CometNativeScanExec, CometScanExec}
import org.apache.spark.sql.execution.{FileSourceScanExec, InSubqueryExec, SubqueryAdaptiveBroadcastExec}
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StructField, StructType}

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.CometConf.COMET_EXEC_ENABLED
import org.apache.comet.CometSparkSessionExtensions.{hasFallbackReason, isSpark35Plus, isSpark41Plus, withFallbackReason}
import org.apache.comet.objectstore.NativeConfig
import org.apache.comet.parquet.CometParquetUtils
import org.apache.comet.serde.{CometOperatorSerde, Compatible, OperatorOuterClass, SupportLevel}
import org.apache.comet.serde.ExprOuterClass.Expr
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.QueryPlanSerde.{exprToProto, serializeDataType}
import org.apache.comet.shims.CometTypeShim

/**
 * Validation and serde logic for Comet's native Parquet scan.
 */
object CometNativeScan extends CometOperatorSerde[CometScanExec] with CometTypeShim with Logging {

  // DataFusion's table_partition_cols literal substitution matches by name, so a bare name
  // like "file_size" could collide with a real column of the same name. Prefix to avoid it.
  private[comet] val constantMetadataFieldPrefix = "_comet_metadata_"

  /**
   * Build synthetic constant-metadata field names, uniquified against `reservedNames` (physical
   * data and partition schema names): DataFusion substitutes partition constants BY NAME, so a
   * colliding user/partition column would otherwise silently receive the constant metadata value
   * instead of its own. Binding is purely positional (`partition2Proto` keys off the ORIGINAL
   * attribute name), so renaming here is always safe.
   */
  private[comet] def uniqueConstantMetadataFields(
      fileConstantMetadataColumns: Seq[AttributeReference],
      reservedNames: Set[String]): Seq[StructField] = {
    val reserved = scala.collection.mutable.LinkedHashSet[String]()
    reserved ++= reservedNames
    fileConstantMetadataColumns.map { attr =>
      var name = s"$constantMetadataFieldPrefix${attr.name}"
      while (reserved.contains(name)) {
        name = name + "_"
      }
      reserved += name
      StructField(name, attr.dataType, attr.nullable)
    }
  }

  /** Determine whether the scan is supported and tag the Spark plan with any fallback reasons */
  def isSupported(scanExec: FileSourceScanExec): Boolean = {

    if (hasFallbackReason(scanExec)) {
      // this node has already been tagged with fallback reasons
      return false
    }

    if (!COMET_EXEC_ENABLED.get()) {
      withFallbackReason(
        scanExec,
        s"Full native scan disabled because ${COMET_EXEC_ENABLED.key} disabled")
    }

    // AQE DPP (SubqueryAdaptiveBroadcastExec) is converted to CometSubqueryBroadcastExec
    // by CometPlanAdaptiveDynamicPruningFilters (queryStageOptimizerRule, Spark 3.5+).
    // Non-AQE DPP (SubqueryBroadcastExec/SubqueryExec) is converted by
    // CometExecRule.convertSubqueryBroadcasts. Both are resolved through the lazy
    // partition serialization path in CometNativeScanExec.
    //
    // On Spark 3.4, injectQueryStageOptimizerRule is unavailable, so the AQE DPP conversion
    // rule can't run. CometScanRule.transformV1Scan rejects AQE DPP on 3.4, so this check
    // is a safety net: if the scan somehow reached here with AQE DPP on 3.4, reject it.
    if (!isSpark35Plus && scanExec.partitionFilters.exists(isAqeDynamicPruningFilter)) {
      withFallbackReason(scanExec, "Native DataFusion scan does not support AQE DPP on Spark 3.4")
    }

    if (SQLConf.get.ignoreCorruptFiles ||
      scanExec.relation.options
        .get("ignorecorruptfiles") // Spark sets this to lowercase.
        .contains("true")) {
      withFallbackReason(scanExec, "Full native scan disabled because ignoreCorruptFiles enabled")
    }

    if (SQLConf.get.ignoreMissingFiles ||
      scanExec.relation.options
        .get("ignoremissingfiles") // Spark sets this to lowercase.
        .contains("true")) {

      withFallbackReason(scanExec, "Full native scan disabled because ignoreMissingFiles enabled")
    }

    // the scan is supported if no fallback reasons were added to the node
    !hasFallbackReason(scanExec)
  }

  /** Detects AQE DPP (SubqueryAdaptiveBroadcastExec), as opposed to non-AQE DPP. */
  private def isAqeDynamicPruningFilter(e: Expression): Boolean =
    e.exists {
      case sub: InSubqueryExec => sub.plan.isInstanceOf[SubqueryAdaptiveBroadcastExec]
      case _ => false
    }

  override def enabledConfig: Option[ConfigEntry[Boolean]] = None

  override def getSupportLevel(operator: CometScanExec): SupportLevel = {
    // all checks happen in CometScanRule before ScanExec is converted to CometScanExec, so
    // we always report compatible here because this serde object is for the converted CometScanExec
    Compatible()
  }

  override def convert(
      scan: CometScanExec,
      builder: Operator.Builder,
      childOp: OperatorOuterClass.Operator*): Option[OperatorOuterClass.Operator] = {
    // Extract object store options from first file (S3 configs apply to all files in scan).
    // Use selectedPartitions (static) instead of getFilePartitions() because at planning time
    // DPP subqueries haven't been resolved yet. Object store options don't depend on DPP.
    val firstFileUri = scan.selectedPartitions
      .flatMap(_.files.headOption)
      .headOption
      .map(_.getPath.toUri)

    // Collect S3/cloud storage configurations
    val hadoopConf = scan.relation.sparkSession.sessionState
      .newHadoopConfWithOptions(scan.relation.options)

    buildNativeScanCommon(
      source = scan.simpleStringWithNodeId(),
      output = scan.output,
      requiredSchema = scan.requiredSchema,
      dataSchema = scan.relation.dataSchema,
      partitionSchema = scan.relation.partitionSchema,
      fileConstantMetadataColumns = scan.wrapped.fileConstantMetadataColumns,
      dataFilters = scan.supportedDataFilters,
      firstFileUri = firstFileUri,
      hadoopConf = hadoopConf,
      conf = scan.conf) match {
      case Some(commonBuilder) =>
        // Sink operators don't have children
        builder.clearChildren()
        val nativeScanBuilder = OperatorOuterClass.NativeScan.newBuilder()
        // Set common data in NativeScan (file_partition will be populated at execution time)
        nativeScanBuilder.setCommon(commonBuilder.build())
        Some(builder.setNativeScan(nativeScanBuilder).build())
      case None =>
        // There are unsupported scan type
        withFallbackReason(
          scan,
          s"unsupported Comet operator: ${scan.nodeName}, due to unsupported data types above")
        None
    }
  }

  /**
   * Build the `NativeScanCommon` proto shared by the core parquet scan and contrib scans that
   * delegate to the same native parquet machinery (e.g. a Delta scan contrib, which passes
   * physical-name schemas under column mapping). Returns `None` when an output data type cannot
   * be serialized; the caller is responsible for tagging a fallback reason.
   *
   * Visibility note: `private[comet]` means a contrib caller must live under an
   * `org.apache.comet.*` package (the same constraint `PlanDataInjector` implementers have).
   */
  private[comet] def buildNativeScanCommon(
      source: String,
      output: Seq[Attribute],
      requiredSchema: StructType,
      dataSchema: StructType,
      partitionSchema: StructType,
      fileConstantMetadataColumns: Seq[AttributeReference],
      dataFilters: Seq[Expression],
      firstFileUri: Option[URI],
      hadoopConf: Configuration,
      conf: SQLConf): Option[OperatorOuterClass.NativeScanCommon.Builder] = {
    val commonBuilder = OperatorOuterClass.NativeScanCommon.newBuilder()

    // Set source in common (used as part of injection key)
    commonBuilder.setSource(source)

    val scanTypes = output.flatten { attr =>
      serializeDataType(attr.dataType)
    }

    if (scanTypes.length != output.length) {
      // There are unsupported scan types
      return None
    }
    commonBuilder.addAllFields(scanTypes.asJava)

    if (conf.getConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED)) {
      commonBuilder.setHasDataFilters(dataFilters.nonEmpty)
      val filterProtos = new ListBuffer[Expr]()
      for (filter <- dataFilters) {
        exprToProto(filter, output) match {
          case Some(proto) => filterProtos += proto
          case _ =>
            logWarning(s"Unsupported data filter $filter")
        }
      }
      commonBuilder.addAllDataFilters(filterProtos.asJava)
    }

    val possibleDefaultValues = getExistenceDefaultValues(requiredSchema)
    if (possibleDefaultValues.exists(_ != null)) {
      // Our schema has default values. Serialize two lists, one with the default values
      // and another with the indexes in the schema so the native side can map missing
      // columns to these default values.
      val (defaultValues, indexes) = possibleDefaultValues.iterator.zipWithIndex
        .filter { case (expr, _) => expr != null }
        .map { case (expr, index) =>
          // ResolveDefaultColumnsUtil.getExistenceDefaultValues has evaluated these
          // expressions and they should now just be literals.
          (Literal(expr), index.toLong.asInstanceOf[java.lang.Long])
        }
        .toList
        .unzip
      commonBuilder.addAllDefaultValues(defaultValues.flatMap(exprToProto(_, output)).asJava)
      commonBuilder.addAllDefaultValuesIndexes(indexes.asJava)
    }

    // Constant metadata columns (file_path, file_name, file_size, file_block_start,
    // file_block_length, file_modification_time) are known before opening the file and
    // constant for every row read from it, exactly like partition columns. Spark places
    // them immediately after partition columns in the scan output
    // (FileSourceStrategy.scala: readDataColumns ++ generatedMetadataColumns ++
    // partitionColumns ++ constantMetadataColumns), so appending them after the real
    // partition schema here keeps the two in lockstep.
    val constantMetadataFields = uniqueConstantMetadataFields(
      fileConstantMetadataColumns,
      dataSchema.fields.map(_.name).toSet ++ partitionSchema.fields.map(_.name).toSet)
    val partitionSchemaFields = partitionSchema.fields.toSeq ++ constantMetadataFields
    val partitionSchemaProto = schema2Proto(partitionSchemaFields)
    val requiredSchemaProto = schema2Proto(requiredSchema)

    // Spark's required schema can prune a Variant column, including one nested under an
    // unrequested struct, while the complete relation schema still contains that unsupported
    // type. Exclude unread roots and replace requested roots with their already-validated,
    // pruned required fields so Variant never enters the native reader data schema. A requested
    // Variant is rejected by CometScanRule and CometExecRule before reaching this point.
    val nativeDataSchema = StructType(dataSchema.fields.flatMap { field =>
      if (containsVariantType(field.dataType)) {
        requiredSchema.fields.find(requiredField => conf.resolver(requiredField.name, field.name))
      } else {
        Some(field)
      }
    })
    val dataSchemaProto = schema2Proto(nativeDataSchema)

    val dataSchemaIndexes = requiredSchema.map(field => {
      nativeDataSchema.fieldIndex(field.name)
    })
    val partitionSchemaIndexes = nativeDataSchema.fields.length until
      (nativeDataSchema.length + partitionSchemaFields.length)

    val projectionVector = (dataSchemaIndexes ++ partitionSchemaIndexes).map(idx =>
      idx.toLong.asInstanceOf[java.lang.Long])

    commonBuilder.addAllProjectionVector(projectionVector.asJava)

    // In `CometScanRule`, we ensure partitionSchema (including constant metadata columns)
    // is supported.
    assert(partitionSchemaProto.length == partitionSchemaFields.length)

    commonBuilder.addAllDataSchema(dataSchemaProto.asJava)
    commonBuilder.addAllRequiredSchema(requiredSchemaProto.asJava)
    commonBuilder.addAllPartitionSchema(partitionSchemaProto.asJava)

    populateScanConfFlags(commonBuilder, requiredSchema, firstFileUri, hadoopConf, conf)

    Some(commonBuilder)
  }

  /**
   * Populate the configuration-derived flags of a `NativeScanCommon`: session timezone, case
   * sensitivity, struct-nullness legacy flag, field-ID matching, type promotion, encryption, and
   * object-store options. Shared with contrib scans that assemble their own schemas/projection
   * (e.g. the Delta contrib's deletion-vector shape) so new flags added here reach them without
   * drift.
   */
  private[comet] def populateScanConfFlags(
      commonBuilder: OperatorOuterClass.NativeScanCommon.Builder,
      requiredSchema: StructType,
      firstFileUri: Option[URI],
      hadoopConf: Configuration,
      conf: SQLConf): Unit = {
    commonBuilder.setSessionTimezone(conf.getConfString("spark.sql.session.timeZone"))
    commonBuilder.setCaseSensitive(conf.getConf[Boolean](SQLConf.CASE_SENSITIVE))

    // SPARK-53535 (Spark 4.1+): when reading a struct whose requested fields are all
    // missing in the Parquet file, the new default preserves the parent struct's
    // nullness from the file (so non-null parents materialize as a struct of all-null
    // fields). Pre-4.1 Spark hardcodes the legacy behavior (whole struct null), which
    // matches the Comet default we use as fallback.
    val returnNullStructConfKey =
      "spark.sql.legacy.parquet.returnNullStructIfAllFieldsMissing"
    val returnNullStructDefault = if (isSpark41Plus) "false" else "true"
    commonBuilder.setReturnNullStructIfAllFieldsMissing(
      conf.getConfString(returnNullStructConfKey, returnNullStructDefault).toBoolean)

    // Field-ID matching: only ask the native side to do extra work when the conf is on AND
    // the requested schema actually carries IDs. Spark's ParquetReadSupport applies the same
    // gate before invoking matchIdField.
    val useFieldId =
      conf.getConf(SQLConf.PARQUET_FIELD_ID_READ_ENABLED) &&
        ParquetUtils.hasFieldIds(requiredSchema)
    commonBuilder.setUseFieldId(useFieldId)
    commonBuilder.setIgnoreMissingFieldId(conf.getConf(SQLConf.IGNORE_MISSING_PARQUET_FIELD_ID))

    commonBuilder.setAllowTypePromotion(CometConf.COMET_SCHEMA_EVOLUTION_ENABLED)
    commonBuilder.setAllowTimestampLtzToNtz(CometConf.COMET_ALLOW_TIMESTAMP_LTZ_AS_NTZ)

    commonBuilder.setEncryptionEnabled(CometParquetUtils.encryptionEnabled(hadoopConf))

    firstFileUri.foreach { uri =>
      val objectStoreOptions =
        NativeConfig.extractObjectStoreOptions(hadoopConf, uri)
      objectStoreOptions.foreach { case (key, value) =>
        commonBuilder.putObjectStoreOptions(key, value)
      }
    }
  }

  override def createExec(nativeOp: Operator, op: CometScanExec): CometNativeExec = {
    CometNativeScanExec(nativeOp, op.wrapped, op.session, op)
  }

  /**
   * Sets the `inline_data` bytes field on a `DeltaSparkDvDescriptor` builder. The shade plugin
   * relocates `com.google.protobuf.ByteString` when packaged, rewriting bytecode descriptors but
   * not a Scala method's own pickled signature, so a helper returning `ByteString` directly would
   * disagree with the packaged jar's Java-generated `setInlineData(ByteString)`. Keeping the
   * protobuf type out of this method's signature sidesteps that, letting out-of-tree modules
   * (e.g. Delta contrib) call this whether compiled against unshaded or shaded classes.
   */
  def setDvInlineData(
      builder: OperatorOuterClass.DeltaSparkDvDescriptor.Builder,
      bytes: Array[Byte]): OperatorOuterClass.DeltaSparkDvDescriptor.Builder =
    builder.setInlineData(com.google.protobuf.ByteString.copyFrom(bytes))
}
