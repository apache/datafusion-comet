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

package org.apache.comet.planner.gates

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.DynamicPruningExpression
import org.apache.spark.sql.comet.CometBatchScanExec
import org.apache.spark.sql.execution.{InSubqueryExec, SubqueryAdaptiveBroadcastExec}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.csv.CSVScan
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf._
import org.apache.comet.DataTypeSupport.isComplexType
import org.apache.comet.iceberg.{CometIcebergNativeScanMetadata, IcebergReflection}
import org.apache.comet.objectstore.NativeConfig
import org.apache.comet.rules.{CometScanRule, CometScanTypeChecker}
import org.apache.comet.serde.operator.CometIcebergNativeScan
import org.apache.comet.shims.ShimSubqueryBroadcast

/**
 * Classifies a V2 BatchScanExec for CometPlanner. Wraps the validation and metadata extraction
 * that CometScanRule.transformV2Scan runs today, with two differences: returns a pure ADT (no
 * CometBatchScanExec plan wrapper), and never mutates the scan's info set. Callers attach
 * withInfo entries from the returned reasons themselves.
 *
 * Duplicates logic that also lives in `CometScanRule.transformV2Scan`. Both copies are live (one
 * per registered rule path) until the legacy rule is deleted.
 */
sealed trait V2ScanClassification

object V2ScanClassification {

  /**
   * Iceberg scan through iceberg-rust. Carries extracted metadata so Phase 3 can build protobuf
   * directly without another reflection round-trip.
   */
  final case class IcebergConvertible(metadata: CometIcebergNativeScanMetadata)
      extends V2ScanClassification

  /** Native CSV V2 scan. No metadata needed at planning time. */
  case object CsvConvertible extends V2ScanClassification

  /** Not convertible. Carries human-readable fallback reasons for `withInfo`. */
  final case class NotConvertible(reasons: Set[String]) extends V2ScanClassification
}

object V2ScanClassifier extends Logging with ShimSubqueryBroadcast {

  def classify(scanExec: BatchScanExec, conf: SQLConf): V2ScanClassification = {
    val result = scanExec.scan match {
      case scan: CSVScan if COMET_CSV_V2_NATIVE_ENABLED.get(conf) =>
        classifyCsv(scan, scanExec)

      case _
          if scanExec.scan.getClass.getName ==
            "org.apache.iceberg.spark.source.SparkBatchQueryScan" =>
        classifyIceberg(scanExec, conf)

      case other =>
        V2ScanClassification.NotConvertible(
          Set(
            s"Unsupported scan: ${other.getClass.getName}. " +
              "Comet Scan only supports Parquet and Iceberg Parquet file formats"))
    }
    result match {
      case V2ScanClassification.IcebergConvertible(metadata) =>
        assert(
          metadata.metadataLocation != null && metadata.metadataLocation.nonEmpty,
          s"IcebergConvertible returned without metadataLocation scan=${scanExec.id}")
      case _ =>
    }
    result
  }

  private def classifyCsv(scan: CSVScan, scanExec: BatchScanExec): V2ScanClassification = {
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
      V2ScanClassification.CsvConvertible
    } else {
      V2ScanClassification.NotConvertible(fallbackReasons.toSet)
    }
  }

  private def classifyIceberg(scanExec: BatchScanExec, conf: SQLConf): V2ScanClassification = {
    val fallbackReasons = new ListBuffer[String]()

    if (!COMET_ICEBERG_NATIVE_ENABLED.get(conf)) {
      fallbackReasons += "Native Iceberg scan disabled because " +
        s"${COMET_ICEBERG_NATIVE_ENABLED.key} is not enabled"
      return V2ScanClassification.NotConvertible(fallbackReasons.toSet)
    }

    if (!COMET_EXEC_ENABLED.get(conf)) {
      fallbackReasons += "Native Iceberg scan disabled because " +
        s"${COMET_EXEC_ENABLED.key} is not enabled"
      return V2ScanClassification.NotConvertible(fallbackReasons.toSet)
    }

    val typeChecker = CometScanTypeChecker(SCAN_NATIVE_DATAFUSION)
    val schemaSupported =
      typeChecker.isSchemaSupported(scanExec.scan.readSchema(), fallbackReasons)
    if (!schemaSupported) {
      fallbackReasons += "Comet extension is not enabled for " +
        s"${scanExec.scan.getClass.getSimpleName}: Schema not supported"
    }

    val tableOpt = IcebergReflection.getTable(scanExec.scan)
    val metadataLocationOpt =
      tableOpt.flatMap(table => IcebergReflection.getMetadataLocation(table))

    val metadataOpt = metadataLocationOpt.flatMap { metadataLocation =>
      try {
        val session = org.apache.spark.sql.SparkSession.active
        val hadoopConf = session.sessionState.newHadoopConf()

        // REST catalogs may not have the metadata file on disk; use the table location as a
        // fallback so FileIO initialisation succeeds against the remote object store.
        val metadataUri = new java.net.URI(metadataLocation)
        val metadataFile = new java.io.File(metadataUri.getPath)
        val effectiveLocation =
          if (!metadataFile.exists() && metadataUri.getScheme == "file") {
            tableOpt
              .flatMap { table =>
                try {
                  val locationMethod = table.getClass.getMethod("location")
                  val tableLocation = locationMethod.invoke(table).asInstanceOf[String]
                  Some(tableLocation)
                } catch {
                  case _: Exception => Some(metadataLocation)
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

        // FileIO properties take precedence because they contain per-table vended credentials.
        val fileIOProperties = tableOpt
          .flatMap(IcebergReflection.getFileIOProperties)
          .map(CometIcebergNativeScan.filterStorageProperties)
          .getOrElse(Map.empty)

        val catalogProperties = hadoopDerivedProperties ++ fileIOProperties
        CometIcebergNativeScanMetadata.extract(
          scanExec.scan,
          effectiveLocation,
          catalogProperties)
      } catch {
        case e: Exception =>
          logError(s"Failed to extract catalog properties from Iceberg scan: ${e.getMessage}", e)
          None
      }
    }

    val metadata = metadataOpt match {
      case Some(m) => m
      case None =>
        fallbackReasons += "Failed to extract Iceberg metadata via reflection"
        return V2ScanClassification.NotConvertible(fallbackReasons.toSet)
    }

    val fileIOCompatible = IcebergReflection.getFileIO(metadata.table) match {
      case Some(fileIO)
          if fileIO.getClass.getName == "org.apache.iceberg.inmemory.InMemoryFileIO" =>
        fallbackReasons += "InMemoryFileIO is not supported by Comet's native reader"
        false
      case Some(_) => true
      case None =>
        fallbackReasons += "Could not check FileIO compatibility"
        false
    }

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

    val taskValidation =
      try {
        CometScanRule.validateIcebergFileScanTasks(metadata.tasks)
      } catch {
        case e: Exception =>
          fallbackReasons += "Iceberg reflection failure: Could not validate " +
            s"FileScanTasks: ${e.getMessage}"
          return V2ScanClassification.NotConvertible(fallbackReasons.toSet)
      }

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
      val msg = "Iceberg reflection failure: Could not verify partition types compatibility"
      logError(msg)
      fallbackReasons += msg
      false
    }

    val filterExpressionsOpt = IcebergReflection.getFilterExpressions(scanExec.scan)

    // IS NULL/NOT NULL on complex types fail because iceberg-rust's accessor creation only
    // handles primitives. Nested field filters work because Iceberg Java pre-binds them to
    // field IDs; element/key access filters don't push down to FileScanTasks.
    val complexTypePredicatesSupported = filterExpressionsOpt
      .map { filters =>
        if (filters.isEmpty) {
          true
        } else {
          val readSchema = scanExec.scan.readSchema()
          val complexColumns = readSchema
            .filter(field => isComplexType(field.dataType))
            .map(_.name)
            .toSet

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
        val msg = "Iceberg reflection failure: Could not check for complex type predicates"
        logError(msg)
        fallbackReasons += msg
        false
      }

    val transformFunctionsSupported = taskValidation.nonIdentityTransform match {
      case Some(transformType) =>
        fallbackReasons +=
          s"Iceberg transform function '$transformType' in residual expression " +
            "is not yet supported by iceberg-rust. " +
            "Only identity transforms are supported."
        false
      case None => true
    }

    val deleteFileTypesSupported = {
      var hasUnsupportedDeletes = false
      try {
        if (!taskValidation.deleteFiles.isEmpty) {
          taskValidation.deleteFiles.asScala.foreach { deleteFile =>
            val equalityFieldIds = IcebergReflection.getEqualityFieldIds(deleteFile)
            if (!equalityFieldIds.isEmpty) {
              equalityFieldIds.asScala.foreach { fieldId =>
                val fieldInfo =
                  IcebergReflection.getFieldInfo(metadata.scanSchema, fieldId.asInstanceOf[Int])
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
          hasUnsupportedDeletes = true
          fallbackReasons += "Iceberg reflection failure: Could not verify delete file " +
            s"types for safety: ${e.getMessage}"
      }
      !hasUnsupportedDeletes
    }

    // CometIcebergNativeScanExec only supports InSubqueryExec for DPP. SPARK-46946 changed
    // SubqueryAdaptiveBroadcastExec to indices: Seq[Int] as preparation for Null Safe Equality
    // DPP; today indices is always length 1 but future versions may introduce multi-index DPP.
    val dppSubqueriesSupported = {
      val unsupportedSubqueries = scanExec.runtimeFilters.collect {
        case DynamicPruningExpression(e) if !e.isInstanceOf[InSubqueryExec] =>
          e.getClass.getSimpleName
      }
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
      V2ScanClassification.IcebergConvertible(metadata)
    } else {
      V2ScanClassification.NotConvertible(fallbackReasons.toSet)
    }
  }
}
