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
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StructField, StructType}

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.CometConf.COMET_EXEC_ENABLED
import org.apache.comet.CometSparkSessionExtensions.{hasExplainInfo, withInfo}
import org.apache.comet.objectstore.NativeConfig
import org.apache.comet.parquet.CometParquetUtils
import org.apache.comet.serde.{CometOperatorSerde, Compatible, OperatorOuterClass, SupportLevel}
import org.apache.comet.serde.ExprOuterClass.Expr
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.QueryPlanSerde.{exprToProto, serializeDataType}

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

    // Dynamic partition pruning (DPP) is now supported!
    // The dynamicallySelectedPartitions in CometScanExec evaluates DPP filters
    // and returns the filtered file list. Native scan receives these pre-filtered
    // files, so partition-level pruning works correctly.
    // Note: DPP filters are excluded from dataFilters to avoid pushing subqueries
    // to native execution (see supportedDataFilters in CometScanExec).

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
      filePartitions.foreach { partition =>
        if (firstPartition.isEmpty) {
          firstPartition = partition.files.headOption
        }
        partition2Proto(partition, nativeScanBuilder, scan.relation.partitionSchema)
      }

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

      // Add runtime filter bounds if available
      // These are pushed down from join operators to enable I/O reduction
      addRuntimeFilterBounds(scan, nativeScanBuilder)

      Some(builder.setNativeScan(nativeScanBuilder).build())

    } else {
      // There are unsupported scan type
      withInfo(
        scan,
        s"unsupported Comet operator: ${scan.nodeName}, due to unsupported data types above")
      None
    }

  }

  private def schema2Proto(
      fields: Array[StructField]): Array[OperatorOuterClass.SparkStructField] = {
    val fieldBuilder = OperatorOuterClass.SparkStructField.newBuilder()
    fields.map(field => {
      fieldBuilder.setName(field.name)
      fieldBuilder.setDataType(serializeDataType(field.dataType).get)
      fieldBuilder.setNullable(field.nullable)
      fieldBuilder.build()
    })
  }

  private def partition2Proto(
      partition: FilePartition,
      nativeScanBuilder: OperatorOuterClass.NativeScan.Builder,
      partitionSchema: StructType): Unit = {
    val partitionBuilder = OperatorOuterClass.SparkFilePartition.newBuilder()
    partition.files.foreach(file => {
      // Process the partition values
      val partitionValues = file.partitionValues
      assert(partitionValues.numFields == partitionSchema.length)
      val partitionVals =
        partitionValues.toSeq(partitionSchema).zipWithIndex.map { case (value, i) =>
          val attr = partitionSchema(i)
          val valueProto = exprToProto(Literal(value, attr.dataType), Seq.empty)
          // In `CometScanRule`, we have already checked that all partition values are
          // supported. So, we can safely use `get` here.
          assert(
            valueProto.isDefined,
            s"Unsupported partition value: $value, type: ${attr.dataType}")
          valueProto.get
        }

      val fileBuilder = OperatorOuterClass.SparkPartitionedFile.newBuilder()
      partitionVals.foreach(fileBuilder.addPartitionValues)
      fileBuilder
        .setFilePath(file.filePath.toString)
        .setStart(file.start)
        .setLength(file.length)
        .setFileSize(file.fileSize)
      partitionBuilder.addPartitionedFile(fileBuilder.build())
    })
    nativeScanBuilder.addFilePartitions(partitionBuilder.build())
  }

  override def createExec(nativeOp: Operator, op: CometScanExec): CometNativeExec = {
    CometNativeScanExec(nativeOp, op.wrapped, op.session)
  }

  /**
   * Add runtime filter bounds to the native scan for row-group pruning. Runtime filters are
   * extracted from data filters that contain range predicates (GreaterThanOrEqual,
   * LessThanOrEqual) or IN predicates.
   */
  private def addRuntimeFilterBounds(
      scan: CometScanExec,
      nativeScanBuilder: OperatorOuterClass.NativeScan.Builder): Unit = {
    import org.apache.spark.sql.catalyst.expressions._

    // Extract runtime filter bounds from data filters
    scan.supportedDataFilters.foreach {
      case GreaterThanOrEqual(attr: AttributeReference, Literal(value, dataType)) =>
        val boundBuilder = OperatorOuterClass.RuntimeFilterBound.newBuilder()
        boundBuilder.setColumnName(attr.name)
        boundBuilder.setColumnIndex(scan.output.indexWhere(_.name == attr.name))
        boundBuilder.setFilterType("minmax")
        exprToProto(Literal(value, dataType), scan.output).foreach { minProto =>
          boundBuilder.setMinValue(minProto.getLiteral)
        }
        nativeScanBuilder.addRuntimeFilterBounds(boundBuilder.build())

      case LessThanOrEqual(attr: AttributeReference, Literal(value, dataType)) =>
        val boundBuilder = OperatorOuterClass.RuntimeFilterBound.newBuilder()
        boundBuilder.setColumnName(attr.name)
        boundBuilder.setColumnIndex(scan.output.indexWhere(_.name == attr.name))
        boundBuilder.setFilterType("minmax")
        exprToProto(Literal(value, dataType), scan.output).foreach { maxProto =>
          boundBuilder.setMaxValue(maxProto.getLiteral)
        }
        nativeScanBuilder.addRuntimeFilterBounds(boundBuilder.build())

      case And(
            GreaterThanOrEqual(attr1: AttributeReference, Literal(minVal, minType)),
            LessThanOrEqual(attr2: AttributeReference, Literal(maxVal, maxType)))
          if attr1.name == attr2.name =>
        // Combined range filter: column >= min AND column <= max
        val boundBuilder = OperatorOuterClass.RuntimeFilterBound.newBuilder()
        boundBuilder.setColumnName(attr1.name)
        boundBuilder.setColumnIndex(scan.output.indexWhere(_.name == attr1.name))
        boundBuilder.setFilterType("minmax")
        exprToProto(Literal(minVal, minType), scan.output).foreach { minProto =>
          boundBuilder.setMinValue(minProto.getLiteral)
        }
        exprToProto(Literal(maxVal, maxType), scan.output).foreach { maxProto =>
          boundBuilder.setMaxValue(maxProto.getLiteral)
        }
        nativeScanBuilder.addRuntimeFilterBounds(boundBuilder.build())

      case InSet(attr: AttributeReference, values) if values.size <= 10 =>
        // Small IN filter - pass individual values
        val boundBuilder = OperatorOuterClass.RuntimeFilterBound.newBuilder()
        boundBuilder.setColumnName(attr.name)
        boundBuilder.setColumnIndex(scan.output.indexWhere(_.name == attr.name))
        boundBuilder.setFilterType("in")
        values.foreach { value =>
          exprToProto(Literal(value, attr.dataType), scan.output).foreach { valProto =>
            boundBuilder.addInValues(valProto.getLiteral)
          }
        }
        nativeScanBuilder.addRuntimeFilterBounds(boundBuilder.build())

      case _ => // Other filters are handled by data_filters
    }
  }
}
