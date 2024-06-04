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
import org.apache.spark.sql.comet.shims.ShimCometParquetUtils
import org.apache.spark.sql.internal.SQLConf

object CometParquetUtils extends ShimCometParquetUtils {
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
}
