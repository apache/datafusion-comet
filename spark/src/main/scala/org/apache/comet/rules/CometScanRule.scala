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

import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, PlanExpression}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.MetadataColumnHelper
import org.apache.spark.sql.comet.{CometBatchScanExec, CometScanExec}
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, ByteType, DataType, MapType, ShortType, StructType}

import org.apache.comet.{CometConf, DataTypeSupport}
import org.apache.comet.CometConf._
import org.apache.comet.CometSparkSessionExtensions.{isCometLoaded, isCometScanEnabled, withInfo, withInfos}
import org.apache.comet.parquet.{CometParquetScan, SupportsComet}

/**
 * Spark physical optimizer rule for replacing Spark scans with Comet scans.
 */
case class CometScanRule(session: SparkSession) extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
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

      plan.transform {
        case scan if hasMetadataCol(scan) =>
          withInfo(scan, "Metadata column is not supported")

        // data source V1
        case scanExec: FileSourceScanExec =>
          transformV1Scan(scanExec)

        // data source V2
        case scanExec: BatchScanExec =>
          transformV2Scan(scanExec)
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

        // if scan is auto then pick best available scan
        if (scanImpl == SCAN_AUTO) {
          // TODO these checks are not yet exhaustive. For example, native_datafusion does
          //  not support reading from object stores such as S3 yet

          val typeChecker = CometScanTypeChecker(SCAN_NATIVE_ICEBERG_COMPAT)
          val schemaSupported =
            typeChecker.isSchemaSupported(scanExec.requiredSchema, fallbackReasons)
          val partitionSchemaSupported =
            typeChecker.isSchemaSupported(r.partitionSchema, fallbackReasons)

          // additional checks for known issues
          def isComplexType(dt: DataType): Boolean = dt match {
            case _: StructType | _: ArrayType | _: MapType => true
            case _ => false
          }

          def hasKnownIssues(dataType: DataType): Boolean = {
            dataType match {
              case s: StructType => s.exists(field => hasKnownIssues(field.dataType))
              case a: ArrayType => hasKnownIssues(a.elementType)
              case m: MapType => isComplexType(m.keyType) || isComplexType(m.valueType)
              case _ => false
            }
          }

          val knownIssues =
            scanExec.requiredSchema.exists(field => hasKnownIssues(field.dataType)) ||
              r.partitionSchema.exists(field => hasKnownIssues(field.dataType))

          if (COMET_EXEC_ENABLED
              .get() && schemaSupported && partitionSchemaSupported &&
            !scanExec.bucketedScan && !knownIssues) {
            scanImpl = SCAN_NATIVE_ICEBERG_COMPAT
          }
        }

        if (scanImpl == SCAN_AUTO) {
          scanImpl = SCAN_NATIVE_COMET
        }

        if (scanImpl == SCAN_NATIVE_DATAFUSION && !COMET_EXEC_ENABLED.get()) {
          fallbackReasons +=
            s"Full native scan disabled because ${COMET_EXEC_ENABLED.key} disabled"
          return withInfos(scanExec, fallbackReasons.toSet)
        }

        if (scanImpl == SCAN_NATIVE_DATAFUSION && scanExec.bucketedScan) {
          // https://github.com/apache/datafusion-comet/issues/1719
          fallbackReasons +=
            "Full native scan disabled because bucketed scan is not supported"
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

}

case class CometScanTypeChecker(scanImpl: String) extends DataTypeSupport {
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
      case _ =>
        super.isTypeSupported(dt, name, fallbackReasons)
    }
  }
}
