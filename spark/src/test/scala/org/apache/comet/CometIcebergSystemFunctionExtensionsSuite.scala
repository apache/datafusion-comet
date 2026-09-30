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
import org.apache.spark.sql.catalyst.expressions.ApplyFunctionExpression
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.internal.SQLConf

/**
 * Iceberg's system functions once Iceberg's SQL extensions have rewritten them.
 *
 * The extensions' `ReplaceStaticInvoke` rule turns a system-function call that a filter compares
 * with a constant from a `StaticInvoke` into an `ApplyFunctionExpression`, so that Iceberg can
 * push the comparison into its scan. Over any other source the filter stays, and Comet evaluates
 * it with the same native kernels as the `StaticInvoke` that CometIcebergSystemFunctionSuite
 * covers. `spark.sql.extensions` is static, so this path needs a suite of its own.
 */
class CometIcebergSystemFunctionExtensionsSuite extends CometTestBase with CometIcebergTestBase {

  override protected def sparkConf: SparkConf =
    super.sparkConf.set(
      "spark.sql.extensions",
      "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

  test("rewritten temporal filters match Iceberg on pre-1970 timestamps ending in .999999") {
    assume(icebergAvailable, "Iceberg not available in classpath")
    withTempIcebergDir { warehouseDir =>
      withSQLConf(
        "spark.sql.catalog.ice" -> "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.ice.type" -> "hadoop",
        "spark.sql.catalog.ice.warehouse" -> warehouseDir.getAbsolutePath,
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC",
        SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> "TIMESTAMP_MICROS") {
        withTable("pre_epoch") {
          // A parquet table, so that no scan absorbs the filter and Comet evaluates it. Iceberg
          // places a pre-1970 timestamp ending in .999999 by the second before it: the first row
          // is in hour -8761, day 1968-12-31, month -13, and year -2, and the second in hour -2.
          sql("CREATE TABLE pre_epoch (id INT, ts TIMESTAMP) USING parquet")
          sql("""INSERT INTO pre_epoch VALUES
            (1, TIMESTAMP '1969-01-01 00:00:00.999999'),
            (2, TIMESTAMP '1969-12-31 23:00:00.999999'),
            (3, TIMESTAMP '1969-12-31 22:30:00'),
            (4, TIMESTAMP '1968-12-31 12:00:00')""")
          Seq(
            "ice.system.hours(ts) = -2",
            "ice.system.days(ts) = DATE '1968-12-31'",
            "ice.system.months(ts) = -13",
            "ice.system.years(ts) = -2").foreach { predicate =>
            val df = sql(s"SELECT id FROM pre_epoch WHERE $predicate")
            val plan = df.queryExecution.optimizedPlan
            assert(
              plan
                .collect { case filter: Filter => filter.condition }
                .exists(_.find(_.isInstanceOf[ApplyFunctionExpression]).isDefined),
              s"expected Iceberg's extensions to rewrite $predicate:\n$plan")
            checkSparkAnswerAndOperator(df)
          }
        }
      }
    }
  }
}
