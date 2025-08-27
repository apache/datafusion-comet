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

import scala.collection.JavaConverters
import scala.collection.mutable.ListBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, GenericInternalRow, PlanExpression}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData, MetadataColumnHelper}
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns.getExistenceDefaultValues
import org.apache.spark.sql.comet.{CometBatchScanExec, CometScanExec}
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import org.apache.comet.{CometConf, DataTypeSupport}
import org.apache.comet.CometConf._
import org.apache.comet.CometSparkSessionExtensions.{isCometLoaded, isCometScanEnabled, withInfo, withInfos}
import org.apache.comet.DataTypeSupport.isComplexType
import org.apache.comet.objectstore.NativeConfig
import org.apache.comet.parquet.{CometParquetScan, Native, SupportsComet}
import org.apache.comet.shims.CometTypeShim

/**
 * Spark physical optimizer rule for replacing Spark scans with Comet scans.
 */
case class CometScanRule(session: SparkSession) extends Rule[SparkPlan] with CometTypeShim {

  private lazy val showTransformations = CometConf.COMET_EXPLAIN_TRANSFORMATIONS.get()

  override def apply(plan: SparkPlan): SparkPlan = {
    val newPlan = _apply(plan)
    if (showTransformations) {
      logInfo(s"\nINPUT: $plan\nOUTPUT: $newPlan")
    }
    newPlan
  }

  private def _apply(plan: SparkPlan): SparkPlan = {
    if (!isCometLoaded(conf) || !isCometScanEnabled(conf)) {
      if (!isCometLoaded(conf)) {
        withInfo(plan, "Comet is not enabled")
      } else if (!isCometScanEnabled(conf)) {
        withInfo(plan, "Comet Scan is not enabled")
      }
      plan
    } else {

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

      plan.transform {
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
    }
  }

  private def isDynamicPruningFilter(e: Expression): Boolean =
    e.exists(_.isInstanceOf[PlanExpression[_]])

  private def transformV1Scan(scanExec: FileSourceScanExec): SparkPlan = {

    if (COMET_DPP_FALLBACK_ENABLED.get() &&
      scanExec.partitionFilters.exists(isDynamicPruningFilter)) {
      return withInfo(scanExec, "Dynamic Partition Pruning is not supported")
    }

    scanExec.relation match {
      case r: HadoopFsRelation =>
        val fallbackReasons = new ListBuffer[String]()
        if (!CometScanExec.isFileFormatSupported(r.fileFormat)) {
          fallbackReasons += s"Unsupported file format ${r.fileFormat}"
          return withInfos(scanExec, fallbackReasons.toSet)
        }

        var scanImpl = COMET_NATIVE_SCAN_IMPL.get()

        // if scan is auto then pick the best available scan
        if (scanImpl == SCAN_AUTO) {
          scanImpl = selectScan(scanExec, r.partitionSchema)
        }

        if (scanImpl == SCAN_NATIVE_DATAFUSION && !COMET_EXEC_ENABLED.get()) {
          fallbackReasons +=
            s"Full native scan disabled because ${COMET_EXEC_ENABLED.key} disabled"
          return withInfos(scanExec, fallbackReasons.toSet)
        }

        if (scanImpl == CometConf.SCAN_NATIVE_DATAFUSION && (SQLConf.get.ignoreCorruptFiles ||
            scanExec.relation.options
              .get("ignorecorruptfiles") // Spark sets this to lowercase.
              .contains("true"))) {
          fallbackReasons +=
            "Full native scan disabled because ignoreCorruptFiles enabled"
          return withInfos(scanExec, fallbackReasons.toSet)
        }

        if (scanImpl == CometConf.SCAN_NATIVE_DATAFUSION && (SQLConf.get.ignoreMissingFiles ||
            scanExec.relation.options
              .get("ignoremissingfiles") // Spark sets this to lowercase.
              .contains("true"))) {
          fallbackReasons +=
            "Full native scan disabled because ignoreMissingFiles enabled"
          return withInfos(scanExec, fallbackReasons.toSet)
        }

        if (scanImpl == CometConf.SCAN_NATIVE_DATAFUSION && scanExec.bucketedScan) {
          // https://github.com/apache/datafusion-comet/issues/1719
          fallbackReasons +=
            "Full native scan disabled because bucketed scan is not supported"
          return withInfos(scanExec, fallbackReasons.toSet)
        }

        val possibleDefaultValues = getExistenceDefaultValues(scanExec.requiredSchema)
        if (possibleDefaultValues.exists(d => {
            d != null && (d.isInstanceOf[ArrayBasedMapData] || d
              .isInstanceOf[GenericInternalRow] || d.isInstanceOf[GenericArrayData])
          })) {
          // Spark already converted these to Java-native types, so we can't check SQL types.
          // ArrayBasedMapData, GenericInternalRow, GenericArrayData correspond to maps, structs,
          // and arrays respectively.
          fallbackReasons +=
            "Full native scan disabled because nested types for default values are not supported"
          return withInfos(scanExec, fallbackReasons.toSet)
        }

        val encryptionEnabled: Boolean =
          conf.getConfString("parquet.crypto.factory.class", "").nonEmpty &&
            conf.getConfString("parquet.encryption.kms.client.class", "").nonEmpty

        if (scanImpl != CometConf.SCAN_NATIVE_COMET && encryptionEnabled) {
          fallbackReasons +=
            "Full native scan disabled because encryption is not supported"
          return withInfos(scanExec, fallbackReasons.toSet)
        }

        val typeChecker = CometScanTypeChecker(scanImpl)
        val schemaSupported =
          typeChecker.isSchemaSupported(scanExec.requiredSchema, fallbackReasons)
        val partitionSchemaSupported =
          typeChecker.isSchemaSupported(r.partitionSchema, fallbackReasons)

        if (!schemaSupported) {
          fallbackReasons += s"Unsupported schema ${scanExec.requiredSchema} for $scanImpl"
        }
        if (!partitionSchemaSupported) {
          fallbackReasons += s"Unsupported partitioning schema ${r.partitionSchema} for $scanImpl"
        }

        if (schemaSupported && partitionSchemaSupported) {
          // this is confusing, but we always insert a CometScanExec here, which may replaced
          // with a CometNativeExec when CometExecRule runs, depending on the scanImpl value.
          CometScanExec(scanExec, session, scanImpl)
        } else {
          withInfos(scanExec, fallbackReasons.toSet)
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
          val cometScan = CometParquetScan(scanExec.scan.asInstanceOf[ParquetScan])
          CometBatchScanExec(
            scanExec.copy(scan = cometScan),
            runtimeFilters = scanExec.runtimeFilters)
        } else {
          withInfos(scanExec, fallbackReasons.toSet)
        }

      // Iceberg scan
      case s: SupportsComet =>
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

      case other =>
        withInfo(
          scanExec,
          s"Unsupported scan: ${other.getClass.getName}. " +
            "Comet Scan only supports Parquet and Iceberg Parquet file formats")
    }
  }

  private def selectScan(scanExec: FileSourceScanExec, partitionSchema: StructType): String = {

    val fallbackReasons = new ListBuffer[String]()

    // native_iceberg_compat only supports local filesystem and S3
    if (!scanExec.relation.inputFiles
        .forall(path => path.startsWith("file://") || path.startsWith("s3a://"))) {
      fallbackReasons += s"$SCAN_NATIVE_ICEBERG_COMPAT only supports local filesystem and S3"
    }

    val filePath = scanExec.relation.inputFiles.head

    // TODO how to get Hadoop config from driver?
    val conf = new Configuration()
    val objectStoreOptions =
      JavaConverters.mapAsJavaMap(
        NativeConfig.extractObjectStoreOptions(conf, URI.create(filePath)));

    if (!Native.isValidObjectStore(filePath, objectStoreOptions)) {
      fallbackReasons += s"Object store config not supported by $SCAN_NATIVE_ICEBERG_COMPAT"
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
        case dt => isStringCollationType(dt)
        case _: StringType =>
          // we only support `case object StringType` and not other instances of `class StringType`
          dataType != StringType
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
