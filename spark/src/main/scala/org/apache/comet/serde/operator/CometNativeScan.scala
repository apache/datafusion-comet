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

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, PlanExpression}
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns.getExistenceDefaultValues
import org.apache.spark.sql.comet.{CometNativeExec, CometNativeScanExec, CometScanExec}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.CometConf.COMET_EXEC_ENABLED
import org.apache.comet.CometSparkSessionExtensions.{hasExplainInfo, withInfo}
import org.apache.comet.objectstore.NativeConfig
import org.apache.comet.parquet.CometParquetUtils
import org.apache.comet.serde.{CometOperatorSerde, Compatible, OperatorOuterClass, SupportLevel}
import org.apache.comet.serde.ExprOuterClass.Expr
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.QueryPlanSerde.{exprToProto, serializeDataType}
import org.apache.comet.shims.ShimFileFormat

/**
 * Validation and serde logic for `native_datafusion` scans.
 */
object CometNativeScan extends CometOperatorSerde[CometScanExec] with Logging {

  /** Determine whether the scan is supported and tag the Spark plan with any fallback reasons */
  def isSupported(scanExec: FileSourceScanExec): Boolean = {

    if (hasExplainInfo(scanExec)) {
      // this node has already been tagged with fallback reasons
      return false
    }

    if (!COMET_EXEC_ENABLED.get()) {
      withInfo(scanExec, s"Full native scan disabled because ${COMET_EXEC_ENABLED.key} disabled")
    }

    // Native DataFusion doesn't support subqueries/dynamic pruning
    if (scanExec.partitionFilters.exists(isDynamicPruningFilter)) {
      withInfo(scanExec, "Native DataFusion scan does not support subqueries/dynamic pruning")
    }

    if (SQLConf.get.ignoreCorruptFiles ||
      scanExec.relation.options
        .get("ignorecorruptfiles") // Spark sets this to lowercase.
        .contains("true")) {
      withInfo(scanExec, "Full native scan disabled because ignoreCorruptFiles enabled")
    }

    if (SQLConf.get.ignoreMissingFiles ||
      scanExec.relation.options
        .get("ignoremissingfiles") // Spark sets this to lowercase.
        .contains("true")) {

      withInfo(scanExec, "Full native scan disabled because ignoreMissingFiles enabled")
    }

    if (scanExec.fileConstantMetadataColumns.nonEmpty) {
      withInfo(scanExec, "Native DataFusion scan does not support metadata columns")
    }

    if (scanExec.bucketedScan) {
      withInfo(scanExec, "Native DataFusion scan does not support bucketed scans")
    }

    if (ShimFileFormat.findRowIndexColumnIndexInSchema(scanExec.requiredSchema) >= 0) {
      withInfo(scanExec, "Native DataFusion scan does not support row index generation")
    }

    // the scan is supported if no fallback reasons were added to the node
    !hasExplainInfo(scanExec)
  }

  private def isDynamicPruningFilter(e: Expression): Boolean =
    e.exists(_.isInstanceOf[PlanExpression[_]])

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
    val nativeScanBuilder = OperatorOuterClass.NativeScan.newBuilder()
    nativeScanBuilder.setSource(scan.simpleStringWithNodeId())

    val scanTypes = scan.output.flatten { attr =>
      serializeDataType(attr.dataType)
    }

    if (scanTypes.length == scan.output.length) {
      nativeScanBuilder.addAllFields(scanTypes.asJava)

      // Sink operators don't have children
      builder.clearChildren()

      if (scan.conf.getConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED) &&
        CometConf.COMET_RESPECT_PARQUET_FILTER_PUSHDOWN.get(scan.conf)) {

        val dataFilters = new ListBuffer[Expr]()
        for (filter <- scan.supportedDataFilters) {
          exprToProto(filter, scan.output) match {
            case Some(proto) => dataFilters += proto
            case _ =>
              logWarning(s"Unsupported data filter $filter")
          }
        }
        nativeScanBuilder.addAllDataFilters(dataFilters.asJava)
      }

      val possibleDefaultValues = getExistenceDefaultValues(scan.requiredSchema)
      if (possibleDefaultValues.exists(_ != null)) {
        // Our schema has default values. Serialize two lists, one with the default values
        // and another with the indexes in the schema so the native side can map missing
        // columns to these default values.
        val (defaultValues, indexes) = possibleDefaultValues.zipWithIndex
          .filter { case (expr, _) => expr != null }
          .map { case (expr, index) =>
            // ResolveDefaultColumnsUtil.getExistenceDefaultValues has evaluated these
            // expressions and they should now just be literals.
            (Literal(expr), index.toLong.asInstanceOf[java.lang.Long])
          }
          .unzip
        nativeScanBuilder.addAllDefaultValues(
          defaultValues.flatMap(exprToProto(_, scan.output)).toIterable.asJava)
        nativeScanBuilder.addAllDefaultValuesIndexes(indexes.toIterable.asJava)
      }

      var firstPartition: Option[PartitionedFile] = None
      val filePartitions = scan.getFilePartitions()
      val filePartitionsProto = filePartitions.map { partition =>
        if (firstPartition.isEmpty) {
          firstPartition = partition.files.headOption
        }
        partition2Proto(partition, scan.relation.partitionSchema)
      }
      nativeScanBuilder.addAllFilePartitions(filePartitionsProto.asJava)

      val partitionSchema = schema2Proto(scan.relation.partitionSchema.fields)
      val requiredSchema = schema2Proto(scan.requiredSchema.fields)
      val dataSchema = schema2Proto(scan.relation.dataSchema.fields)

      val dataSchemaIndexes = scan.requiredSchema.fields.map(field => {
        scan.relation.dataSchema.fieldIndex(field.name)
      })
      val partitionSchemaIndexes = Array
        .range(
          scan.relation.dataSchema.fields.length,
          scan.relation.dataSchema.length + scan.relation.partitionSchema.fields.length)

      val projectionVector = (dataSchemaIndexes ++ partitionSchemaIndexes).map(idx =>
        idx.toLong.asInstanceOf[java.lang.Long])

      nativeScanBuilder.addAllProjectionVector(projectionVector.toIterable.asJava)

      // In `CometScanRule`, we ensure partitionSchema is supported.
      assert(partitionSchema.length == scan.relation.partitionSchema.fields.length)

      nativeScanBuilder.addAllDataSchema(dataSchema.toIterable.asJava)
      nativeScanBuilder.addAllRequiredSchema(requiredSchema.toIterable.asJava)
      nativeScanBuilder.addAllPartitionSchema(partitionSchema.toIterable.asJava)
      nativeScanBuilder.setSessionTimezone(scan.conf.getConfString("spark.sql.session.timeZone"))
      nativeScanBuilder.setCaseSensitive(scan.conf.getConf[Boolean](SQLConf.CASE_SENSITIVE))

      // Collect S3/cloud storage configurations
      val hadoopConf = scan.relation.sparkSession.sessionState
        .newHadoopConfWithOptions(scan.relation.options)

      nativeScanBuilder.setEncryptionEnabled(CometParquetUtils.encryptionEnabled(hadoopConf))

      firstPartition.foreach { partitionFile =>
        val objectStoreOptions =
          NativeConfig.extractObjectStoreOptions(hadoopConf, partitionFile.pathUri)
        objectStoreOptions.foreach { case (key, value) =>
          nativeScanBuilder.putObjectStoreOptions(key, value)
        }
      }

      Some(builder.setNativeScan(nativeScanBuilder).build())

    } else {
      // There are unsupported scan type
      withInfo(
        scan,
        s"unsupported Comet operator: ${scan.nodeName}, due to unsupported data types above")
      None
    }

  }

  override def createExec(nativeOp: Operator, op: CometScanExec): CometNativeExec = {
    CometNativeScanExec(nativeOp, op.wrapped, op.session)
  }
}
