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

package org.apache.spark.sql.comet.parquet

import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils
import org.apache.spark.sql.types._

trait ShimCometParquetUtils {
  def foundDuplicateFieldInFieldIdLookupModeError(
      requiredId: Int,
      matchedFields: String): Throwable = {
    QueryExecutionErrors.foundDuplicateFieldInFieldIdLookupModeError(requiredId, matchedFields)
  }

  val FIELD_ID_METADATA_KEY = ParquetUtils.FIELD_ID_METADATA_KEY

  def hasFieldIds(schema: StructType): Boolean = ParquetUtils.hasFieldIds(schema)

  def hasFieldId(field: StructField): Boolean = ParquetUtils.hasFieldId(field)

  def getFieldId(field: StructField): Int = ParquetUtils.getFieldId (field)
}
