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
   * For primitive columns, delegates to [[TypeUtil.checkParquetType]] which mirrors Spark's
   * `ParquetVectorUpdaterFactory` logic and is version-aware (Spark 4 type promotion). For
   * complex types, checks kind-level mismatches (e.g., reading a scalar as an array).
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
      val footer = FooterReader.readFooter(hadoopConf, file)
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
                // Only reject when the file has a non-repeated primitive (genuine scalar).
                if (parquetField.isPrimitive &&
                  parquetField.getRepetition != Repetition.REPEATED) {
                  throwSchemaMismatch(
                    filePath,
                    fieldName,
                    field.dataType.catalogString,
                    parquetField.asPrimitiveType.getPrimitiveTypeName.toString)
                }

              case _: StructType | _: MapType =>
                // Read schema expects struct/map; file must have a group type
                if (parquetField.isPrimitive) {
                  throwSchemaMismatch(
                    filePath,
                    fieldName,
                    field.dataType.catalogString,
                    parquetField.asPrimitiveType.getPrimitiveTypeName.toString)
                }

              case _ =>
                if (parquetField.isPrimitive) {
                  // Primitive -> Primitive: use TypeUtil.checkParquetType which mirrors
                  // Spark's ParquetVectorUpdaterFactory logic.
                  // TypeUtil may not cover all Spark types (e.g., intervals, collated
                  // strings). Only rethrow for types TypeUtil explicitly knows about
                  // (basic primitives, decimals, timestamps). For unknown types, let
                  // the execution path handle errors with proper context.
                  try {
                    val descriptor =
                      fileSchema.getColumnDescription(Array(parquetField.getName))
                    if (descriptor != null) {
                      TypeUtil.checkParquetType(descriptor, field.dataType)
                    }
                  } catch {
                    case scnse: SchemaColumnConvertNotSupportedException
                        if isTypeUtilKnownType(field.dataType) =>
                      throw ShimParquetSchemaError.parquetColumnMismatchError(
                        filePath,
                        fieldName,
                        field.dataType.catalogString,
                        scnse.getPhysicalType,
                        scnse)
                    case _: SchemaColumnConvertNotSupportedException =>
                    // TypeUtil doesn't know this type - skip, let execution handle it
                  }
                } else {
                  // File has complex type, read schema expects primitive -> mismatch
                  throwSchemaMismatch(filePath, fieldName, field.dataType.catalogString, "group")
                }
            }
          }
        }
      }
    }
  }

  /**
   * Returns true if TypeUtil.checkParquetType covers this Spark type. For types it doesn't know
   * (intervals, collated strings, etc.), we skip rethrow and let the execution path handle
   * errors.
   */
  private def isTypeUtilKnownType(dt: DataType): Boolean = dt match {
    case BooleanType | ByteType | ShortType | IntegerType | LongType => true
    case FloatType | DoubleType => true
    case StringType | BinaryType => true
    case DateType | TimestampType => true
    case _: DecimalType => true
    case _ => false
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
