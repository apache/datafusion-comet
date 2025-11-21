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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.comet.{CometNativeExec, CometNativeWriteExec}
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.{InsertIntoHadoopFsRelationCommand, WriteFilesExec}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.serde.{CometOperatorSerde, OperatorOuterClass}
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.QueryPlanSerde.serializeDataType

/**
 * CometOperatorSerde implementation for DataWritingCommandExec that converts Parquet write
 * operations to use Comet's native Parquet writer.
 */
object CometDataWritingCommandExec extends CometOperatorSerde[DataWritingCommandExec] {

  override def enabledConfig: Option[ConfigEntry[Boolean]] =
    Some(CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED)

  override def convert(
      op: DataWritingCommandExec,
      builder: Operator.Builder,
      childOp: Operator*): Option[OperatorOuterClass.Operator] = {
    op.cmd match {
      case cmd: InsertIntoHadoopFsRelationCommand =>
        // Check if this is a Parquet write
        cmd.fileFormat match {
          case _: ParquetFileFormat =>
            try {
              // Create native ParquetWriter operator
              val scanOp = OperatorOuterClass.Scan
                .newBuilder()
                .setSource("write_source")
                .setArrowFfiSafe(true)

              // Add fields from the query output schema
              val scanTypes = cmd.query.output.flatMap { attr =>
                serializeDataType(attr.dataType)
              }

              if (scanTypes.length != cmd.query.output.length) {
                withInfo(op, "Cannot serialize data types for native write")
                return None
              }

              scanTypes.foreach(scanOp.addFields)

              val scanOperator = Operator
                .newBuilder()
                .setPlanId(op.id)
                .setScan(scanOp.build())
                .build()

              // Get output path
              val outputPath = cmd.outputPath.toString

              val writerOp = OperatorOuterClass.ParquetWriter
                .newBuilder()
                .setOutputPath(outputPath)
                // TODO: Get compression from options
                .setCompression(OperatorOuterClass.CompressionCodec.Snappy)
                // Add column names to preserve them across FFI boundary
                .addAllColumnNames(cmd.query.output.map(_.name).asJava)
                .build()

              // Build the ParquetWriter operator
              val writerOperator = Operator
                .newBuilder()
                .setPlanId(op.id)
                .addChildren(scanOperator)
                .setParquetWriter(writerOp)
                .build()

              Some(writerOperator)
            } catch {
              case e: Exception =>
                withInfo(
                  op,
                  "Failed to convert DataWritingCommandExec to native execution: " +
                    s"${e.getMessage}")
                None
            }
          case _ =>
            // Not a Parquet write, skip
            None
        }
      case _ =>
        // Not a write command we handle
        None
    }
  }

  override def createExec(nativeOp: Operator, op: DataWritingCommandExec): CometNativeExec = {
    val cmd = op.cmd.asInstanceOf[InsertIntoHadoopFsRelationCommand]
    val outputPath = cmd.outputPath.toString

    // Get the child plan from the WriteFilesExec or use the child directly
    val childPlan = op.child match {
      case writeFiles: WriteFilesExec =>
        // The WriteFilesExec child should already be a Comet operator
        writeFiles.child
      case other =>
        // Fallback: use the child directly
        other
    }

    CometNativeWriteExec(nativeOp, childPlan, outputPath)
  }
}
