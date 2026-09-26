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

package org.apache.comet

import org.apache.spark.SparkConf
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.CometIcebergNativeScanExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

/**
 * Residual pushdown for predicates that only reach the Iceberg scan filter when Iceberg's SQL
 * extensions are installed.
 *
 * Spark resolves an Iceberg system function such as `bucket` to a `StaticInvoke`, which it cannot
 * translate into a data source predicate. The extensions' `ReplaceStaticInvoke` rule rewrites it
 * so Iceberg can push the comparison, which puts an `UnboundTransform` term into each file's
 * residual. CometIcebergNativeSuite runs without the extensions, so it never sees one.
 */
class CometIcebergResidualPushdownSuite
    extends CometTestBase
    with CometIcebergTestBase
    with AdaptiveSparkPlanHelper {

  override protected def sparkConf: SparkConf =
    super.sparkConf.set(
      "spark.sql.extensions",
      "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

  // A transform term's ref() is its source column. Pushing `bucket(4, id) = 2` as `id = 2` would
  // have iceberg-rust drop every matching row whose id is not 2, and the exact filter above the
  // scan cannot restore them. CometScanRule falls back for a bare transform predicate, so every
  // query nests the transform under AND, OR or NOT, which is what reaches the native scan.
  test("transform predicates in a residual are not pushed as their source column") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.test_cat" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.test_cat.type" -> "hadoop",
        "spark.sql.catalog.test_cat.warehouse" -> warehouseDir.getAbsolutePath,
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {

        // Unpartitioned, so Iceberg can settle none of these predicates from partition values
        // and keeps each one whole in the residual.
        spark.sql("""
          CREATE TABLE test_cat.db.transform_residual (
            id INT,
            data STRING,
            ts TIMESTAMP
          ) USING iceberg
          TBLPROPERTIES ('format-version' = '2')
        """)
        spark.sql("""
          INSERT INTO test_cat.db.transform_residual
          SELECT
            CAST(id AS INT),
            CONCAT('d', id),
            CAST(DATE_ADD(DATE '2024-01-01', CAST(id AS INT)) AS TIMESTAMP)
          FROM range(100)
        """)

        val table = "test_cat.db.transform_residual"
        Seq(
          // The pushed id = 2 would keep one row, and bucket(4, 2) is not 2.
          s"SELECT id FROM $table WHERE test_cat.system.bucket(4, id) = 2 AND data > 'a'",
          s"SELECT id FROM $table WHERE test_cat.system.bucket(4, id) = 2 OR id = 50",
          // The pushed id != 2 would drop the id = 2 row, which this predicate keeps.
          s"SELECT id FROM $table WHERE NOT (test_cat.system.bucket(4, id) = 2) AND data > 'a'",
          // The pushed data = 'd1' would drop d10 through d19.
          s"SELECT id FROM $table WHERE test_cat.system.truncate(2, data) = 'd1' AND id >= 0",
          // The pushed literal is a day count, which would be compared to ts as microseconds.
          s"SELECT id FROM $table WHERE test_cat.system.days(ts) = DATE '2024-01-05' AND id >= 0")
          .foreach { query =>
            val (_, cometPlan) = checkSparkAnswer(query)
            val scans = collect(cometPlan) { case scan: CometIcebergNativeScanExec => scan }
            assert(
              scans.length == 1,
              s"Expected the residual to reach a native scan, but found ${scans.length} for " +
                s"$query. Plan:\n$cometPlan")
          }

        spark.sql(s"DROP TABLE $table")
      }
    }
  }
}
