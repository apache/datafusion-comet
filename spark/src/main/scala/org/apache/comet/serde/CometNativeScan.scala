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

package org.apache.comet.serde

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns.getExistenceDefaultValues
import org.apache.spark.sql.comet.CometScanExec
import org.apache.spark.sql.execution.datasources.{FilePartition, FileScanRDD, PartitionedFile}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceRDD, DataSourceRDDPartition}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StructField, StructType}

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.objectstore.NativeConfig
import org.apache.comet.parquet.CometParquetUtils
import org.apache.comet.serde.ExprOuterClass.Expr
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.QueryPlanSerde.{exprToProto, serializeDataType}

object CometNativeScan extends CometOperatorSerde[CometScanExec] with Logging {

  override def enabledConfig: Option[ConfigEntry[Boolean]] = None

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
        for (filter <- scan.dataFilters) {
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

      // TODO: modify CometNativeScan to generate the file partitions without instantiating RDD.
      var firstPartition: Option[PartitionedFile] = None
      scan.inputRDD match {
        case rdd: DataSourceRDD =>
          val partitions = rdd.partitions
          partitions.foreach(p => {
            val inputPartitions = p.asInstanceOf[DataSourceRDDPartition].inputPartitions
            inputPartitions.foreach(partition => {
              if (firstPartition.isEmpty) {
                firstPartition = partition.asInstanceOf[FilePartition].files.headOption
              }
              partition2Proto(
                partition.asInstanceOf[FilePartition],
                nativeScanBuilder,
                scan.relation.partitionSchema)
            })
          })
        case rdd: FileScanRDD =>
          rdd.filePartitions.foreach(partition => {
            if (firstPartition.isEmpty) {
              firstPartition = partition.files.headOption
            }
            partition2Proto(partition, nativeScanBuilder, scan.relation.partitionSchema)
          })
        case _ =>
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

}
