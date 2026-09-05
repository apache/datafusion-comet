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

import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.execution.{FileSourceScanExec, QueryExecution, SparkPlan}
import org.apache.spark.sql.util.QueryExecutionListener

import org.apache.comet.ExtendedExplainInfo

/**
 * Repro for Delta's own DeletionVectorsSuite expectation: DELETE on a DV-enabled table must WRITE
 * deletion vectors (not rewrite files) with Comet active. Mirrors "DELETE with DVs - on a table
 * with no prior DVs".
 */
class CometDeltaDmlReproSuite extends CometDeltaTestBase {

  /**
   * Every [[SparkPlan]] Delta's own internal DataFrame actions executed during `body`, captured
   * via a [[QueryExecutionListener]] rather than the outer statement's own plan: Delta's DML
   * commands (DELETE/UPDATE/MERGE) drive `findTouchedFiles` through separate internal
   * `collect`/`count` actions on their own [[QueryExecution]]s, invisible to `df.queryExecution`
   * on the outer SQL statement.
   */
  private def capturePlansDuring(body: => Unit): Seq[SparkPlan] = {
    val plans = ListBuffer.empty[SparkPlan]
    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        plans += qe.executedPlan
      }
      override def onFailure(
          funcName: String,
          qe: QueryExecution,
          exception: Exception): Unit = {}
    }
    spark.listenerManager.register(listener)
    try {
      body
    } finally {
      spark.listenerManager.unregister(listener)
    }
    plans.toSeq
  }

  test(
    "DELETE's internal deletion-vector-generating scan declines the row-index-outside-a-DV-" +
      "scan reason (the read-side counterpart of the DV-write repro above)") {
    withSQLConf("spark.databricks.delta.properties.defaults.enableDeletionVectors" -> "true") {
      withTempPath { dir =>
        val path = dir.getAbsolutePath
        spark.range(0, 1000, 1, 4).write.format("delta").save(path)

        val capturedPlans = capturePlansDuring {
          spark.sql(s"DELETE FROM delta.`$path` WHERE id % 2 = 0 AND id < 200")
        }

        // Before writing a deletion vector, DELETE must first learn WHICH rows matched the
        // predicate, so it reads each candidate file's `_metadata.row_index` directly (a bare
        // row-index column, with no `is_row_deleted` alongside it -- unlike a normal DV-applying
        // read, no existing DV is applied to this scan, since the very DV being computed does not
        // exist yet). DeltaScanSupport.declineReason's hasRowIndex-without-hasIsRowDeleted gate
        // exists precisely to keep this bookkeeping scan on Spark's reader: claiming it with a
        // dead constant row-index would feed wrong (constant) row indexes into the DV this DELETE
        // is trying to build. This must remain a plain Spark FileSourceScanExec here, never a
        // CometDeltaNativeScanExec.
        val declinedRowIndexScans = capturedPlans.flatMap { plan =>
          collectWithSubqueries(stripAQEPlan(plan)) {
            case f: FileSourceScanExec
                if DeltaScanSupport.isDeltaScan(f) &&
                  f.requiredSchema.exists(_.name == CometDeltaNativeScan.RowIndexColumn) &&
                  !f.requiredSchema.exists(_.name == CometDeltaNativeScan.IsRowDeletedColumn) =>
              f
          }
        }
        assert(
          declinedRowIndexScans.nonEmpty,
          "expected to observe at least one internal row-index-only scan while DELETE " +
            "computed which rows to mark in the new deletion vector")

        val reasons =
          declinedRowIndexScans.flatMap(f => new ExtendedExplainInfo().getFallbackReasons(f))
        assert(
          reasons.exists(_.contains("row-index reads outside a deletion-vector scan")),
          "expected the internal row-index scan to carry the row-index-outside-a-DV-scan " +
            s"decline reason, got: ${reasons.mkString(", ")}")

        val log = DeltaLog.forTable(spark, path)
        val withDvs = log.update().allFiles.collect().count(_.deletionVector != null)
        assert(withDvs > 0, s"expected at least one file to have a DV written, got $withDvs")
        assert(spark.read.format("delta").load(path).count() == 900)
      }
    }
  }

  test("DELETE writes DVs with useMetadataRowIndex=true (metadata row-index DML shape)") {
    withSQLConf(
      "spark.databricks.delta.properties.defaults.enableDeletionVectors" -> "true",
      "spark.databricks.delta.deletionVectors.useMetadataRowIndex" -> "true") {
      withTempPath { dir =>
        val path = dir.getAbsolutePath
        spark.range(0, 1000, 1, 500).write.format("delta").save(path)
        spark.sql(s"DELETE FROM delta.`$path` WHERE id % 2 = 0 AND id < 200")

        val log = DeltaLog.forTable(spark, path)
        val withDvs = log.update().allFiles.collect().count(_.deletionVector != null)
        assert(withDvs == 100, s"expected 100 files with DVs, got $withDvs")
        assert(spark.read.format("delta").load(path).count() == 900)
      }
    }
  }

  test("DELETE writes DVs rather than rewriting files") {
    withSQLConf(
      "spark.databricks.delta.properties.defaults.enableDeletionVectors" -> "true",
      "spark.databricks.delta.delete.deletionVectors.persistent" -> "true") {
      withTempDir { base =>
        // Mirror Delta's DeletionVectorsTestUtils: paths with spaces and a literal %2a.
        val dir = new java.io.File(base, "s p a r k %2a")
        val path = dir.getAbsolutePath
        spark.range(0, 1000, 1, 500).write.format("delta").save(path)
        spark.sql(s"DELETE FROM delta.`$path` WHERE id % 2 = 0 AND id < 200")

        val log = DeltaLog.forTable(spark, path)
        val files = log.update().allFiles.collect()
        val withDvs = files.count(_.deletionVector != null)
        assert(files.length == 500, s"expected 500 files, got ${files.length}")
        assert(withDvs == 100, s"expected 100 files with DVs, got $withDvs")
        assert(spark.read.format("delta").load(path).count() == 900)
      }
    }
  }
}
