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

package org.apache.comet.contrib.delta

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.comet.{CometDeltaNativeScanExec, CometScanExec}
import org.apache.spark.sql.execution.{FileSourceScanExec, QueryExecution}

// Investigates the Spark-3.5 OptimizeGeneratedColumnSuite *PartitionExpr* cluster
// (~20+ tests) failing as: List() did not equal List("((year <= 2021) OR ...)").
//
// Those tests call getPushedPartitionFilters(qe), which only matches
//   case scan: FileSourceScanExec => scan.partitionFilters
// With Comet enabled, the scan is a CometScanExec (wrapping FileSourceScanExec) or a
// CometDeltaNativeScanExec (carrying the original FileSourceScanExec as `originalPlan`),
// so the extractor returns Nil and the assertion fails.
//
// Delta's OptimizeGeneratedColumn is a LOGICAL optimizer rule -- it fires before physical
// planning, independent of Comet -- so the partition filter SHOULD still be present on
// Comet's scan node. This suite proves that: it reproduces the exact YearPartitionExpr
// scenario, extracts partitionFilters THROUGH the Comet wrapper, and asserts the same
// expected filter. Green => the failure is a harness extraction gap (fix: teach the diff's
// getPushedPartitionFilters to see Comet scans), NOT a real Comet behavior change.
class CometDeltaGeneratedColumnPartitionFilterReproSuite extends CometDeltaTestBase {

  // Mirror of the suite's FileSourceScanExec-only extractor, extended to see Comet scans.
  private def pushedPartitionFilters(qe: QueryExecution): Seq[Expression] =
    qe.executedPlan.collectFirst {
      case s: FileSourceScanExec => s.partitionFilters
      case s: CometScanExec => s.partitionFilters
      case s: CometDeltaNativeScanExec => s.originalPlan.partitionFilters
    }.getOrElse(Nil)

  test("OptimizeGeneratedColumn pushes a partition filter visible through Comet's scan") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    // Generated columns require a named (catalog) table -- path-based delta.`/path` does not
    // support them -- so mirror the suite's withTableName + named CREATE TABLE.
    val table = "comet_gencol_year"
    spark.sql(s"DROP TABLE IF EXISTS $table")
    try {
      // Generated columns must be created via the DeltaTable builder API (the suite's
      // createTable helper) -- the SQL `GENERATED ALWAYS AS` path hits Spark's V2
      // validateGeneratedColumns guard, which rejects it outside that builder.
      io.delta.tables.DeltaTable
        .create(spark)
        .tableName(table)
        .addColumn("eventTime", "TIMESTAMP")
        .addColumn(
          io.delta.tables.DeltaTable
            .columnBuilder("year")
            .dataType("INT")
            .generatedAlwaysAs("YEAR(eventTime)")
            .build())
        .partitionedBy("year")
        .execute()
      spark.sql(s"INSERT INTO $table (eventTime) VALUES (TIMESTAMP '2020-06-01 12:00:00')")

      val qe = spark.sql(
        s"SELECT * FROM $table WHERE eventTime < '2021-01-01 18:00:00'").queryExecution
      val filters = pushedPartitionFilters(qe).map(_.sql)
      info(s"DIAG scan node = ${qe.executedPlan.getClass.getSimpleName}; " +
        s"partitionFilters = $filters")
      assert(
        filters == Seq("((year <= 2021) OR ((year <= 2021) IS NULL))"),
        s"expected the generated-column partition filter on the Comet scan, got: $filters")
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $table")
    }
  }
}
