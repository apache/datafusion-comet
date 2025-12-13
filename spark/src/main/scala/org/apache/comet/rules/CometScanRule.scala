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

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, GenericInternalRow, PlanExpression}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.{sideBySide, ArrayBasedMapData, GenericArrayData, MetadataColumnHelper}
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns.getExistenceDefaultValues
import org.apache.spark.sql.comet.{CometBatchScanExec, CometScanExec}
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import org.apache.comet.{CometConf, CometNativeException, DataTypeSupport}
import org.apache.comet.CometConf._
import org.apache.comet.CometSparkSessionExtensions.{hasExplainInfo, isCometLoaded, withInfo, withInfos}
import org.apache.comet.DataTypeSupport.isComplexType
import org.apache.comet.iceberg.{CometIcebergNativeScanMetadata, IcebergReflection}
import org.apache.comet.objectstore.NativeConfig
import org.apache.comet.parquet.{CometParquetScan, Native, SupportsComet}
import org.apache.comet.parquet.CometParquetUtils.{encryptionEnabled, isEncryptionConfigSupported}
import org.apache.comet.serde.operator.CometNativeScan
import org.apache.comet.shims.CometTypeShim

/**
 * Spark physical optimizer rule for replacing Spark scans with Comet scans.
 */
case class CometScanRule(session: SparkSession) extends Rule[SparkPlan] with CometTypeShim {

  import CometScanRule._

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

    def transformScan(plan: SparkPlan): SparkPlan = plan match {
      case scan if !CometConf.COMET_NATIVE_SCAN_ENABLED.get(conf) =>
        withInfo(scan, "Comet Scan is not enabled")

      case scan if hasMetadataCol(scan) =>
        withInfo(scan, "Metadata column is not supported")

      // data source V1
      case scanExec: FileSourceScanExec =>
        transformV1Scan(scanExec)

      // data source V2
      case scanExec: BatchScanExec =>
        if (isIcebergMetadataTable(scanExec)) {
          withInfo(scanExec, "Iceberg Metadata tables are not supported")
        } else {
          transformV2Scan(scanExec)
        }
    }

    plan.transform {
      case scan if isSupportedScanNode(scan) => transformScan(scan)
    }
  }

  private def transformV1Scan(scanExec: FileSourceScanExec): SparkPlan = {

    if (COMET_DPP_FALLBACK_ENABLED.get() &&
      scanExec.partitionFilters.exists(isDynamicPruningFilter)) {
      return withInfo(scanExec, "Dynamic Partition Pruning is not supported")
    }

    scanExec.relation match {
      case r: HadoopFsRelation =>
        if (!CometScanExec.isFileFormatSupported(r.fileFormat)) {
          return withInfo(scanExec, s"Unsupported file format ${r.fileFormat}")
        }

        var scanImpl = COMET_NATIVE_SCAN_IMPL.get()

        val hadoopConf = scanExec.relation.sparkSession.sessionState
          .newHadoopConfWithOptions(scanExec.relation.options)

        // if scan is auto then pick the best available scan
        if (scanImpl == SCAN_AUTO) {
          scanImpl = selectScan(scanExec, r.partitionSchema, hadoopConf)
        }

        if (scanImpl == SCAN_NATIVE_DATAFUSION && !CometNativeScan.isSupported(scanExec)) {
          return scanExec
        }

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
            "Full native scan disabled because nested types for default values are not supported")
        }

        if (encryptionEnabled(hadoopConf) && scanImpl != CometConf.SCAN_NATIVE_COMET) {
          if (!isEncryptionConfigSupported(hadoopConf)) {
            withInfo(scanExec, s"$scanImpl does not support encryption")
          }
        }

        // check that schema is supported
        checkSchema(scanExec, scanImpl, r)

        if (hasExplainInfo(scanExec)) {
          // could not accelerate, and plan is already tagged with fallback reasons
          scanExec
        } else {
          // this is confusing, but we always insert a CometScanExec here, which may replaced
          // with a CometNativeExec when CometExecRule runs, depending on the scanImpl value.
          CometScanExec(scanExec, session, scanImpl)
        }

      case _ =>
        withInfo(scanExec, s"Unsupported relation ${scanExec.relation}")
    }
  }

  private def transformV2Scan(scanExec: BatchScanExec): SparkPlan = {

    scanExec.scan match {
      case scan: ParquetScan =>
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

        if (scan.pushedAggregate.nonEmpty) {
          fallbackReasons += "Comet does not support pushed aggregate"
        }

        if (schemaSupported && partitionSchemaSupported && scan.pushedAggregate.isEmpty) {
          val cometScan = CometParquetScan(session, scanExec.scan.asInstanceOf[ParquetScan])
          CometBatchScanExec(
            scanExec.copy(scan = cometScan),
            runtimeFilters = scanExec.runtimeFilters)
        } else {
          withInfos(scanExec, fallbackReasons.toSet)
        }

      // Iceberg scan - patched version implementing SupportsComet interface
      case s: SupportsComet if !COMET_ICEBERG_NATIVE_ENABLED.get() =>
        val fallbackReasons = new ListBuffer[String]()

        if (!s.isCometEnabled) {
          fallbackReasons += "Comet extension is not enabled for " +
            s"${scanExec.scan.getClass.getSimpleName}: not enabled on data source side"
        }

        val schemaSupported =
          CometBatchScanExec.isSchemaSupported(scanExec.scan.readSchema(), fallbackReasons)

        if (!schemaSupported) {
          fallbackReasons += "Comet extension is not enabled for " +
            s"${scanExec.scan.getClass.getSimpleName}: Schema not supported"
        }

        if (s.isCometEnabled && schemaSupported) {
          // When reading from Iceberg, we automatically enable type promotion
          SQLConf.get.setConfString(COMET_SCHEMA_EVOLUTION_ENABLED.key, "true")
          CometBatchScanExec(
            scanExec.clone().asInstanceOf[BatchScanExec],
            runtimeFilters = scanExec.runtimeFilters)
        } else {
          withInfos(scanExec, fallbackReasons.toSet)
        }

      // Iceberg scan - detected by class name (works with unpatched Iceberg)
      case _
          if scanExec.scan.getClass.getName ==
            "org.apache.iceberg.spark.source.SparkBatchQueryScan" =>
        // scalastyle:off println
        println(s"=== CometScanRule: Detected Iceberg SparkBatchQueryScan ===")
        // scalastyle:on println

        val fallbackReasons = new ListBuffer[String]()

        // Native Iceberg scan requires both configs to be enabled
        if (!COMET_ICEBERG_NATIVE_ENABLED.get()) {
          fallbackReasons += "Native Iceberg scan disabled because " +
            s"${COMET_ICEBERG_NATIVE_ENABLED.key} is not enabled"
          // scalastyle:off println
          println(s"=== Fallback: COMET_ICEBERG_NATIVE_ENABLED not enabled ===")
          // scalastyle:on println
          return withInfos(scanExec, fallbackReasons.toSet)
        }

        if (!COMET_EXEC_ENABLED.get()) {
          fallbackReasons += "Native Iceberg scan disabled because " +
            s"${COMET_EXEC_ENABLED.key} is not enabled"
          // scalastyle:off println
          println(s"=== Fallback: COMET_EXEC_ENABLED not enabled ===")
          // scalastyle:on println
          return withInfos(scanExec, fallbackReasons.toSet)
        }

        // scalastyle:off println
        println(s"=== CometScanRule: Both configs enabled, checking schema ===")
        // scalastyle:on println

        val typeChecker = CometScanTypeChecker(SCAN_NATIVE_DATAFUSION)
        val schemaSupported =
          typeChecker.isSchemaSupported(scanExec.scan.readSchema(), fallbackReasons)

        if (!schemaSupported) {
          fallbackReasons += "Comet extension is not enabled for " +
            s"${scanExec.scan.getClass.getSimpleName}: Schema not supported"
          // scalastyle:off println
          println(s"=== Fallback: Schema not supported ===")
          // scalastyle:on println
        }

        // scalastyle:off println
        println(s"=== CometScanRule: Schema check passed, extracting metadata ===")
        // scalastyle:on println

        // Extract all Iceberg metadata once using reflection.
        // If any required reflection fails, this returns None, and we fall back to Spark.
        // First get metadataLocation and catalogProperties which are needed by the factory.
        val tableOpt = IcebergReflection.getTable(scanExec.scan)
        tableOpt.foreach { table =>
          logInfo(s"Iceberg table class: ${table.getClass.getName}")
        }

        // scalastyle:off println
        if (tableOpt.isEmpty) {
          println(s"=== Failed to get Iceberg table via reflection ===")
        } else {
          println(s"=== Got Iceberg table: ${tableOpt.get.getClass.getName} ===")
        }
        // scalastyle:on println

        val metadataLocationOpt = tableOpt.flatMap { table =>
          val metadataLoc = IcebergReflection.getMetadataLocation(table)
          metadataLoc match {
            case Some(loc) =>
              logInfo(s"Iceberg metadata location: $loc")
              // scalastyle:off println
              println(s"=== Got metadata location: $loc ===")
              // scalastyle:on println
            case None =>
              logInfo(s"Iceberg metadata location not available (likely REST catalog)")
              // scalastyle:off println
              println(s"=== Metadata location not available ===")
              // scalastyle:on println
          }
          metadataLoc
        }

        val metadataOpt = metadataLocationOpt.flatMap { metadataLocation =>
          // scalastyle:off println
          println(s"=== Starting metadata extraction for location: $metadataLocation ===")
          // scalastyle:on println
          try {
            val session = org.apache.spark.sql.SparkSession.active
            val hadoopConf = session.sessionState.newHadoopConf()

            // For REST catalogs, the metadata file may not exist on disk since metadata
            // is fetched via HTTP. Check if file exists; if not, use table location instead.
            val metadataUri = new java.net.URI(metadataLocation)
            // scalastyle:off println
            println(s"=== metadataUri: $metadataUri, scheme: ${metadataUri.getScheme}, path: ${metadataUri.getPath} ===")
            // scalastyle:on println

            val metadataFile = new java.io.File(metadataUri.getPath)
            // scalastyle:off println
            println(s"=== metadataFile: ${metadataFile.getAbsolutePath}, exists: ${metadataFile.exists()} ===")
            // scalastyle:on println

            val effectiveLocation = if (!metadataFile.exists() && metadataUri.getScheme == "file") {
              // Metadata file doesn't exist (REST catalog with InMemoryFileIO or similar)
              // Use table location instead for FileIO initialization
              // scalastyle:off println
              println(s"=== Metadata file doesn't exist, attempting to get table location ===")
              // scalastyle:on println

              tableOpt.flatMap { table =>
                try {
                  val locationMethod = table.getClass.getMethod("location")
                  val tableLocation = locationMethod.invoke(table).asInstanceOf[String]
                  // scalastyle:off println
                  println(s"=== REST catalog detected: metadata file doesn't exist, using table location: $tableLocation ===")
                  // scalastyle:on println
                  Some(tableLocation)
                } catch {
                  case e: Exception =>
                    // scalastyle:off println
                    println(s"=== Could not get table location, using metadata location anyway: ${e.getMessage} ===")
                    e.printStackTrace()
                    // scalastyle:on println
                    Some(metadataLocation)
                }
              }.getOrElse(metadataLocation)
            } else {
              // scalastyle:off println
              println(s"=== Metadata file exists or not file:// scheme, using metadata location ===")
              // scalastyle:on println
              metadataLocation
            }

            // scalastyle:off println
            println(s"=== effectiveLocation: $effectiveLocation ===")
            // scalastyle:on println

            val effectiveUri = new java.net.URI(effectiveLocation)
            // scalastyle:off println
            println(s"=== effectiveUri: $effectiveUri ===")
            // scalastyle:on println

            val hadoopS3Options = NativeConfig.extractObjectStoreOptions(hadoopConf, effectiveUri)
            // scalastyle:off println
            println(s"=== hadoopS3Options: $hadoopS3Options ===")
            // scalastyle:on println

            val catalogProperties =
              org.apache.comet.serde.operator.CometIcebergNativeScan
                .hadoopToIcebergS3Properties(hadoopS3Options)
            // scalastyle:off println
            println(s"=== catalogProperties: $catalogProperties ===")
            // scalastyle:on println

            // scalastyle:off println
            println(s"=== Calling CometIcebergNativeScanMetadata.extract with location: $effectiveLocation ===")
            // scalastyle:on println

            val result = CometIcebergNativeScanMetadata
              .extract(scanExec.scan, effectiveLocation, catalogProperties)

            // scalastyle:off println
            result match {
              case Some(metadata) =>
                println(s"=== CometIcebergNativeScanMetadata.extract returned Some(metadata) ===")
              case None =>
                println(s"=== CometIcebergNativeScanMetadata.extract returned None ===")
            }
            // scalastyle:on println

            result
          } catch {
            case e: Exception =>
              // scalastyle:off println
              println(s"=== Failed to extract catalog properties from Iceberg scan: ${e.getMessage} ===")
              e.printStackTrace()
              // scalastyle:on println
              logError(
                s"Failed to extract catalog properties from Iceberg scan: ${e.getMessage}",
                e)
              None
          }
        }

        // scalastyle:off println
        if (metadataOpt.isEmpty) {
          println(s"=== metadataOpt is None, will fall back ===")
        } else {
          println(s"=== metadataOpt is Some, proceeding with validation ===")
        }
        // scalastyle:on println

        // If metadata extraction failed, fall back to Spark
        val metadata = metadataOpt match {
          case Some(m) =>
            // scalastyle:off println
            println(s"=== Got metadata, proceeding with validation ===")
            // scalastyle:on println
            m
          case None =>
            fallbackReasons += "Failed to extract Iceberg metadata via reflection"
            // scalastyle:off println
            println(s"=== No metadata, falling back. Reasons: ${fallbackReasons.mkString(", ")} ===")
            // scalastyle:on println
            return withInfos(scanExec, fallbackReasons.toSet)
        }

        // Now perform all validation using the pre-extracted metadata
        // Check if table uses a FileIO implementation compatible with iceberg-rust
        // scalastyle:off println
        println(s"=== Starting FileIO compatibility check ===")
        // scalastyle:on println

        val fileIOCompatible = IcebergReflection.getFileIO(metadata.table) match {
          case Some(fileIO) =>
            val fileIOClassName = fileIO.getClass.getName
            // scalastyle:off println
            println(s"=== FileIO class: $fileIOClassName ===")
            // scalastyle:on println
            // InMemoryFileIO is now supported with table location fallback for REST catalogs
            true
          case None =>
            fallbackReasons += "Could not check FileIO compatibility"
            // scalastyle:off println
            println(s"=== Could not get FileIO, falling back ===")
            // scalastyle:on println
            false
        }

        // scalastyle:off println
        println(s"=== FileIO compatible: $fileIOCompatible ===")
        // scalastyle:on println

        // Check Iceberg table format version
        // scalastyle:off println
        println(s"=== Checking format version ===")
        // scalastyle:on println

        val formatVersionSupported = IcebergReflection.getFormatVersion(metadata.table) match {
          case Some(formatVersion) =>
            // scalastyle:off println
            println(s"=== Format version: $formatVersion ===")
            // scalastyle:on println
            if (formatVersion > 2) {
              fallbackReasons += "Iceberg table format version " +
                s"$formatVersion is not supported. " +
                "Comet only supports Iceberg table format V1 and V2"
              // scalastyle:off println
              println(s"=== Format version $formatVersion not supported, falling back ===")
              // scalastyle:on println
              false
            } else {
              // scalastyle:off println
              println(s"=== Format version $formatVersion supported ===")
              // scalastyle:on println
              true
            }
          case None =>
            fallbackReasons += "Could not verify Iceberg table format version"
            // scalastyle:off println
            println(s"=== Could not get format version, falling back ===")
            // scalastyle:on println
            false
        }

        // scalastyle:off println
        println(s"=== Checking file formats and schemes ===")
        // scalastyle:on println

        // Check if all files are Parquet format and use supported filesystem schemes
        val (allParquetFiles, unsupportedSchemes) =
          IcebergReflection.validateFileFormatsAndSchemes(metadata.tasks)

        // scalastyle:off println
        println(s"=== allParquetFiles: $allParquetFiles, unsupportedSchemes: ${unsupportedSchemes.mkString(", ")} ===")
        // scalastyle:on println

        val allSupportedFilesystems = if (unsupportedSchemes.isEmpty) {
          true
        } else {
          fallbackReasons += "Iceberg scan contains files with unsupported filesystem " +
            s"schemes: ${unsupportedSchemes.mkString(", ")}. " +
            "Comet only supports: file, s3, s3a, gs, gcs, oss, abfss, abfs, wasbs, wasb"
          false
        }

        if (!allParquetFiles) {
          fallbackReasons += "Iceberg scan contains non-Parquet files (ORC or Avro). " +
            "Comet only supports Parquet files in Iceberg tables"
        }

        // scalastyle:off println
        println(s"=== Checking partition types ===")
        // scalastyle:on println

        // Partition values are deserialized via iceberg-rust's Literal::try_from_json()
        // which has incomplete type support (binary/fixed unimplemented, decimals limited)
        val partitionTypesSupported = (for {
          partitionSpec <- IcebergReflection.getPartitionSpec(metadata.table)
        } yield {
          val unsupportedTypes =
            IcebergReflection.validatePartitionTypes(partitionSpec, metadata.scanSchema)

          // scalastyle:off println
          println(s"=== unsupportedTypes: ${unsupportedTypes.size} ===")
          // scalastyle:on println

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
        val transformFunctionsSupported =
          try {
            IcebergReflection.findNonIdentityTransformInResiduals(metadata.tasks) match {
              case Some(transformType) =>
                // Found unsupported transform
                fallbackReasons +=
                  s"Iceberg transform function '$transformType' in residual expression " +
                    "is not yet supported by iceberg-rust. " +
                    "Only identity transforms are supported."
                false
              case None =>
                // No unsupported transforms found - safe to use native execution
                true
            }
          } catch {
            case e: Exception =>
              // Reflection failure - cannot verify safety, must fall back
              fallbackReasons += "Iceberg reflection failure: Could not check for " +
                s"transform functions in residuals: ${e.getMessage}"
              false
          }

        // Check for unsupported struct types in delete files
        val deleteFileTypesSupported = {
          var hasUnsupportedDeletes = false

          try {
            val deleteFiles = IcebergReflection.getDeleteFiles(metadata.tasks)

            if (!deleteFiles.isEmpty) {
              deleteFiles.asScala.foreach { deleteFile =>
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

        // scalastyle:off println
        println(s"=== Final validation results: ===")
        println(s"===   schemaSupported: $schemaSupported ===")
        println(s"===   fileIOCompatible: $fileIOCompatible ===")
        println(s"===   formatVersionSupported: $formatVersionSupported ===")
        println(s"===   allParquetFiles: $allParquetFiles ===")
        println(s"===   allSupportedFilesystems: $allSupportedFilesystems ===")
        println(s"===   partitionTypesSupported: $partitionTypesSupported ===")
        println(s"===   complexTypePredicatesSupported: $complexTypePredicatesSupported ===")
        println(s"===   transformFunctionsSupported: $transformFunctionsSupported ===")
        println(s"===   deleteFileTypesSupported: $deleteFileTypesSupported ===")
        // scalastyle:on println

        if (schemaSupported && fileIOCompatible && formatVersionSupported && allParquetFiles &&
          allSupportedFilesystems && partitionTypesSupported &&
          complexTypePredicatesSupported && transformFunctionsSupported &&
          deleteFileTypesSupported) {
          // scalastyle:off println
          println(s"=== ALL CHECKS PASSED - Creating CometBatchScanExec with native Iceberg scan ===")
          // scalastyle:on println
          CometBatchScanExec(
            scanExec.clone().asInstanceOf[BatchScanExec],
            runtimeFilters = scanExec.runtimeFilters,
            nativeIcebergScanMetadata = Some(metadata))
        } else {
          // scalastyle:off println
          println(s"=== Some checks failed - Falling back to Spark ===")
          println(s"=== Fallback reasons: ${fallbackReasons.mkString(", ")} ===")
          // scalastyle:on println
          withInfos(scanExec, fallbackReasons.toSet)
        }

      case other =>
        withInfo(
          scanExec,
          s"Unsupported scan: ${other.getClass.getName}. " +
            "Comet Scan only supports Parquet and Iceberg Parquet file formats")
    }
  }

  private def selectScan(
      scanExec: FileSourceScanExec,
      partitionSchema: StructType,
      hadoopConf: Configuration): String = {

    val fallbackReasons = new ListBuffer[String]()

    // native_iceberg_compat only supports local filesystem and S3
    if (scanExec.relation.inputFiles
        .forall(path => path.startsWith("file://") || path.startsWith("s3a://"))) {

      val filePath = scanExec.relation.inputFiles.headOption
      if (filePath.exists(_.startsWith("s3a://"))) {
        validateObjectStoreConfig(filePath.get, hadoopConf, fallbackReasons)
      }
    } else {
      fallbackReasons += s"$SCAN_NATIVE_ICEBERG_COMPAT only supports local filesystem and S3"
    }

    val typeChecker = CometScanTypeChecker(SCAN_NATIVE_ICEBERG_COMPAT)
    val schemaSupported =
      typeChecker.isSchemaSupported(scanExec.requiredSchema, fallbackReasons)
    val partitionSchemaSupported =
      typeChecker.isSchemaSupported(partitionSchema, fallbackReasons)

    def hasUnsupportedType(dataType: DataType): Boolean = {
      dataType match {
        case s: StructType => s.exists(field => hasUnsupportedType(field.dataType))
        case a: ArrayType => hasUnsupportedType(a.elementType)
        case m: MapType =>
          // maps containing complex types are not supported
          isComplexType(m.keyType) || isComplexType(m.valueType) ||
          hasUnsupportedType(m.keyType) || hasUnsupportedType(m.valueType)
        case dt if isStringCollationType(dt) => true
        case _ => false
      }
    }

    val knownIssues =
      scanExec.requiredSchema.exists(field => hasUnsupportedType(field.dataType)) ||
        partitionSchema.exists(field => hasUnsupportedType(field.dataType))

    if (knownIssues) {
      fallbackReasons += "Schema contains data types that are not supported by " +
        s"$SCAN_NATIVE_ICEBERG_COMPAT"
    }

    val cometExecEnabled = COMET_EXEC_ENABLED.get()
    if (!cometExecEnabled) {
      fallbackReasons += s"$SCAN_NATIVE_ICEBERG_COMPAT requires ${COMET_EXEC_ENABLED.key}=true"
    }

    if (cometExecEnabled && schemaSupported && partitionSchemaSupported && !knownIssues &&
      fallbackReasons.isEmpty) {
      logInfo(s"Auto scan mode selecting $SCAN_NATIVE_ICEBERG_COMPAT")
      SCAN_NATIVE_ICEBERG_COMPAT
    } else {
      logInfo(
        s"Auto scan mode falling back to $SCAN_NATIVE_COMET due to " +
          s"${fallbackReasons.mkString(", ")}")
      SCAN_NATIVE_COMET
    }
  }

  private def isDynamicPruningFilter(e: Expression): Boolean =
    e.exists(_.isInstanceOf[PlanExpression[_]])

  def checkSchema(scanExec: FileSourceScanExec, scanImpl: String, r: HadoopFsRelation): Unit = {
    val fallbackReasons = new ListBuffer[String]()
    val typeChecker = CometScanTypeChecker(scanImpl)
    val schemaSupported =
      typeChecker.isSchemaSupported(scanExec.requiredSchema, fallbackReasons)
    if (!schemaSupported) {
      withInfo(scanExec, s"Unsupported schema ${scanExec.requiredSchema} for $scanImpl")
    }
    val partitionSchemaSupported =
      typeChecker.isSchemaSupported(r.partitionSchema, fallbackReasons)
    if (!partitionSchemaSupported) {
      fallbackReasons += s"Unsupported partitioning schema ${r.partitionSchema} for $scanImpl"
    }
    withInfos(scanExec, fallbackReasons.toSet)
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
      case ByteType | ShortType
          if scanImpl != CometConf.SCAN_NATIVE_COMET &&
            !CometConf.COMET_SCAN_ALLOW_INCOMPATIBLE.get() =>
        fallbackReasons += s"$scanImpl scan cannot read $dt when " +
          s"${CometConf.COMET_SCAN_ALLOW_INCOMPATIBLE.key} is false. ${CometConf.COMPAT_GUIDE}."
        false
      case _: StructType | _: ArrayType | _: MapType if scanImpl == CometConf.SCAN_NATIVE_COMET =>
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
}
