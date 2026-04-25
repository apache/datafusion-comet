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

package org.apache.spark.sql.comet

import scala.jdk.CollectionConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.schema.Type.Repetition
import org.apache.spark.sql.catalyst.expressions.{DynamicPruningExpression, Expression, Literal}
import org.apache.spark.sql.comet.shims.ShimParquetSchemaError
import org.apache.spark.sql.execution.datasources.{FilePartition, SchemaColumnConvertNotSupportedException}
import org.apache.spark.sql.types._

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.isSpark40Plus
import org.apache.comet.parquet.{FooterReader, TypeUtil}

object CometScanUtils {

  /**
   * Filters unused DynamicPruningExpression expressions - one which has been replaced with
   * DynamicPruningExpression(Literal.TrueLiteral) during Physical Planning
   */
  def filterUnusedDynamicPruningExpressions(predicates: Seq[Expression]): Seq[Expression] = {
    predicates.filterNot(_ == DynamicPruningExpression(Literal.TrueLiteral))
  }

  /**
   * Validate per-file schema compatibility by reading actual Parquet file metadata.
   *
   * For each file in the scan, reads the Parquet footer and validates each required column
   * against the actual file schema. This catches mismatches that table-level schema checks miss
   * (e.g., when `spark.read.schema(...)` specifies a type incompatible with the file).
   *
   * Checks structural mismatches (scalar as array, primitive as struct/map) on all Spark
   * versions. On Spark 4.0+, also validates primitive-to-primitive type mismatches via
   * TypeUtil.checkParquetType. On Spark 3.x, primitive type checks are left to the execution path
   * which has proper config awareness (schema evolution).
   */
  def validatePerFileSchemaCompatibility(
      hadoopConf: Configuration,
      requiredSchema: StructType,
      partitionColumnNames: Set[String],
      caseSensitive: Boolean,
      filePartitions: Seq[FilePartition]): Unit = {

    for {
      partition <- filePartitions
      file <- partition.files
    } {
      val filePath = file.filePath.toString()
      // Read footer; skip files with unsupported Parquet types (TIMESTAMP(NANOS), INTERVAL)
      val footerOpt =
        try {
          Some(FooterReader.readFooter(hadoopConf, file))
        } catch {
          case _: Exception => None
        }
      footerOpt.foreach { footer =>
        val fileSchema = footer.getFileMetaData.getSchema

        requiredSchema.fields.foreach { field =>
          val fieldName = field.name
          // Skip partition columns - their values come from directory paths, not the file
          val isPartitionCol = if (caseSensitive) {
            partitionColumnNames.contains(fieldName)
          } else {
            partitionColumnNames.exists(_.equalsIgnoreCase(fieldName))
          }
          if (!isPartitionCol) {
            val parquetFieldOpt = {
              val fields = fileSchema.getFields.asScala
              if (caseSensitive) fields.find(_.getName == fieldName)
              else fields.find(_.getName.equalsIgnoreCase(fieldName))
            }

            parquetFieldOpt.foreach { parquetField =>
              field.dataType match {
                case _: ArrayType =>
                  // A REPEATED primitive/group is a valid legacy 2-level Parquet array.
                  // Only reject when the file has a non-repeated primitive.
                  if (parquetField.isPrimitive &&
                    parquetField.getRepetition != Repetition.REPEATED) {
                    throwSchemaMismatch(
                      filePath,
                      fieldName,
                      field.dataType.catalogString,
                      parquetField.asPrimitiveType.getPrimitiveTypeName.toString)
                  }

                case _: StructType | _: MapType =>
                  if (parquetField.isPrimitive) {
                    throwSchemaMismatch(
                      filePath,
                      fieldName,
                      field.dataType.catalogString,
                      parquetField.asPrimitiveType.getPrimitiveTypeName.toString)
                  }

                case _ =>
                  if (parquetField.isPrimitive) {
                    // TypeUtil.checkParquetType for primitive type validation.
                    // On Spark 3.x with schema evolution enabled, suppress SCNSE errors
                    // since TypeUtil allows extra conversions (Int->Long).
                    val schemaEvolutionEnabled =
                      CometConf.COMET_SCHEMA_EVOLUTION_ENABLED.get()
                    val descriptor =
                      fileSchema.getColumnDescription(Array(parquetField.getName))
                    if (descriptor != null) {
                      try {
                        TypeUtil.checkParquetType(descriptor, field.dataType)
                      } catch {
                        case scnse: SchemaColumnConvertNotSupportedException
                            if !schemaEvolutionEnabled || isSpark40Plus =>
                          throw ShimParquetSchemaError.parquetColumnMismatchError(
                            filePath,
                            fieldName,
                            field.dataType.catalogString,
                            scnse.getPhysicalType,
                            scnse)
                        case _: SchemaColumnConvertNotSupportedException =>
                        // Schema evolution on Spark 3.x - suppress
                        case re: RuntimeException =>
                          // TypeUtil.convertErrorForTimestampNTZ throws RuntimeException
                          // for LTZ→NTZ on Spark 3.x. Preserve original message so tests
                          // can assert on it (e.g., "Unable to create Parquet converter").
                          throw ShimParquetSchemaError.parquetRuntimeError(filePath, re)
                      }
                    }
                  }
                // else: complex type for non-Array/Struct/Map - let execution handle
              }
            }
          }
        }
      }
    }
  }

  private def throwSchemaMismatch(
      filePath: String,
      column: String,
      expectedType: String,
      actualType: String): Unit = {
    val scnse = new SchemaColumnConvertNotSupportedException(column, actualType, expectedType)
    throw ShimParquetSchemaError.parquetColumnMismatchError(
      filePath,
      column,
      expectedType,
      actualType,
      scnse)
  }
}
