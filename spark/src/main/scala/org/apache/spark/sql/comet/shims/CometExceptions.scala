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

import org.apache.spark.SparkUpgradeException
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.SQLConf

/**
 * Builders for Comet exceptions that need Spark internals but no per-version shimming.
 *
 * This lives in `org.apache.spark.sql.comet.shims` rather than in `org.apache.comet` because the
 * exception types it constructs are `private[spark]`; it is deliberately outside the per-version
 * source roots, since every API it touches resolves identically on Spark 3.4 through 4.x.
 */
object CometExceptions {

  /**
   * The failure Comet raises for a Parquet file whose dates/timestamps were written in the legacy
   * hybrid (Julian + Gregorian) calendar, which Comet's native scan cannot rebase.
   *
   * Classified as Spark's own `SparkUpgradeException` with the `READ_ANCIENT_DATETIME` condition:
   * that is the exception Spark raises for the same data, and one its `FileScanRDD` deliberately
   * rethrows rather than wrapping in `FAILED_READ_FILE`, since the file is not corrupt. The
   * templated message explains the calendar ambiguity accurately but advises setting Spark's
   * rebase mode, which does not apply to Comet, so Comet's own remedy travels as the cause.
   */
  def legacyDatetimeRebase(params: Map[String, Any]): SparkUpgradeException = {
    val filePath = params.get("filePath").map(_.toString).filter(_.nonEmpty)
    val cause = new RuntimeException(
      params.get("message").map(_.toString).getOrElse("") +
        filePath.map(p => s" File: $p").getOrElse(""))
    new SparkUpgradeException(
      "INCONSISTENT_BEHAVIOR_CROSS_VERSION.READ_ANCIENT_DATETIME",
      Map(
        "format" -> "Parquet",
        "config" -> QueryExecutionErrors.toSQLConf(SQLConf.PARQUET_REBASE_MODE_IN_READ.key),
        "option" -> QueryExecutionErrors.toDSOption("datetimeRebaseMode")),
      cause)
  }
}
