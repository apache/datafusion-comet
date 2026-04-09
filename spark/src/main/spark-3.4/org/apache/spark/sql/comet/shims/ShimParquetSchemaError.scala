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

package org.apache.spark.sql.comet.shims

import org.apache.spark.SparkException
import org.apache.spark.sql.execution.datasources.SchemaColumnConvertNotSupportedException

/**
 * Spark 3.4: matches Spark's native cause chain depth.
 *
 * Spark's FileScanRDD wraps errors as: SparkException (file-level) → SparkException
 * (column-level) → SCNSE
 *
 * Tests assert at different levels:
 *   - SPARK-35640: getMessage.contains("Parquet column cannot be converted in file")
 *   - SPARK-34212: getCause.getCause.isInstanceOf[SCNSE]
 *   - row group skipping: getMessage.contains("Column: [a], Expected: bigint, Found: INT32")
 *
 * The outer message must include the column details so getMessage() checks pass.
 */
object ShimParquetSchemaError {
  def parquetColumnMismatchError(
      filePath: String,
      column: String,
      expectedType: String,
      actualType: String,
      cause: SchemaColumnConvertNotSupportedException): SparkException = {
    val columnMsg =
      s"Parquet column cannot be converted in file $filePath. " +
        s"Column: [$column], Expected: $expectedType, Found: $actualType"
    // Inner SparkException (column-level) — what getCause returns
    val inner = new SparkException(columnMsg, cause)
    // Outer SparkException (file-level) — includes column details in message
    // so getMessage().contains() checks work at any level
    new SparkException(columnMsg, inner)
  }

  /** Wrap a RuntimeException (e.g., from TypeUtil.convertErrorForTimestampNTZ) */
  def parquetRuntimeError(filePath: String, cause: RuntimeException): SparkException = {
    new SparkException(cause.getMessage, cause)
  }
}
