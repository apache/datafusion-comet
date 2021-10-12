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

package org.apache.comet.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

object CometParquetUtils {
  private val PARQUET_FIELD_ID_WRITE_ENABLED = "spark.sql.parquet.fieldId.write.enabled"
  private val PARQUET_FIELD_ID_READ_ENABLED = "spark.sql.parquet.fieldId.read.enabled"
  private val IGNORE_MISSING_PARQUET_FIELD_ID = "spark.sql.parquet.fieldId.read.ignoreMissing"

  def writeFieldId(conf: SQLConf): Boolean =
    conf.getConfString(PARQUET_FIELD_ID_WRITE_ENABLED, "false").toBoolean

  def writeFieldId(conf: Configuration): Boolean =
    conf.getBoolean(PARQUET_FIELD_ID_WRITE_ENABLED, false)

  def readFieldId(conf: SQLConf): Boolean =
    conf.getConfString(PARQUET_FIELD_ID_READ_ENABLED, "false").toBoolean

  def ignoreMissingIds(conf: SQLConf): Boolean =
    conf.getConfString(IGNORE_MISSING_PARQUET_FIELD_ID, "false").toBoolean

  // The following is copied from QueryExecutionErrors
  // TODO: remove after dropping Spark 3.2.0 support and directly use
  //       QueryExecutionErrors.foundDuplicateFieldInFieldIdLookupModeError
  def foundDuplicateFieldInFieldIdLookupModeError(
      requiredId: Int,
      matchedFields: String): Throwable = {
    new RuntimeException(s"""
         |Found duplicate field(s) "$requiredId": $matchedFields
         |in id mapping mode
     """.stripMargin.replaceAll("\n", " "))
  }

  // The followings are copied from org.apache.spark.sql.execution.datasources.parquet.ParquetUtils
  // TODO: remove after dropping Spark 3.2.0 support and directly use ParquetUtils
  /**
   * A StructField metadata key used to set the field id of a column in the Parquet schema.
   */
  val FIELD_ID_METADATA_KEY = "parquet.field.id"

  /**
   * Whether there exists a field in the schema, whether inner or leaf, has the parquet field ID
   * metadata.
   */
  def hasFieldIds(schema: StructType): Boolean = {
    def recursiveCheck(schema: DataType): Boolean = {
      schema match {
        case st: StructType =>
          st.exists(field => hasFieldId(field) || recursiveCheck(field.dataType))

        case at: ArrayType => recursiveCheck(at.elementType)

        case mt: MapType => recursiveCheck(mt.keyType) || recursiveCheck(mt.valueType)

        case _ =>
          // No need to really check primitive types, just to terminate the recursion
          false
      }
    }
    if (schema.isEmpty) false else recursiveCheck(schema)
  }

  def hasFieldId(field: StructField): Boolean =
    field.metadata.contains(FIELD_ID_METADATA_KEY)

  def getFieldId(field: StructField): Int = {
    require(
      hasFieldId(field),
      s"The key `$FIELD_ID_METADATA_KEY` doesn't exist in the metadata of " + field)
    try {
      Math.toIntExact(field.metadata.getLong(FIELD_ID_METADATA_KEY))
    } catch {
      case _: ArithmeticException | _: ClassCastException =>
        throw new IllegalArgumentException(
          s"The key `$FIELD_ID_METADATA_KEY` must be a 32-bit integer")
    }
  }
}
