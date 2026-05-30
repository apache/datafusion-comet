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

// In package org.apache.spark.sql.delta for access to Action.logSchema / SingleAction.
package org.apache.spark.sql.delta

import java.io.File

import org.apache.spark.sql.delta.actions.{Action, SingleAction}
import org.apache.spark.sql.functions.col

import org.apache.comet.contrib.delta.CometDeltaTestBase

// Integration regression guard for the core GetStructField null-mask fix (surfaced as
// DeltaIncrementalSetTransactionsSuite's scala.MatchError(null,null)). It exercises the exact
// shape that triggered it: a Spark-coalesce-rewritten checkpoint parquet (all-null
// checkpointMetadata + sidecar within the wide Action.logSchema) read with the imposed
// Action.logSchema and filtered
//   checkpointMetadata.version IS NOT NULL OR sidecar.path IS NOT NULL
// which must be EMPTY. CONFIRMED to leak (8 rows) before the fix; passes after. A simpler
// hand-built/narrow schema does NOT reproduce -- the wide nested schema is required -- so this
// guard is intentionally checkpoint-shaped.
class CometDeltaCheckpointFilterReproSuite extends CometDeltaTestBase {

  test("checkpointMetadata/sidecar IS NOT NULL OR filter is empty on a rewritten checkpoint") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("ckpt_filter") { tablePath =>
      spark.range(0, 100, 1, 1).toDF("id").write.format("delta").save(tablePath)
      (1 to 6).foreach { i =>
        spark.range(i * 100, i * 100 + 20, 1, 1).toDF("id")
          .write.format("delta").mode("append").save(tablePath)
      }
      val log = DeltaLog.forTable(spark, tablePath)
      log.checkpoint()
      val logDir = new File(log.logPath.toUri)
      val checkpointPath = logDir.listFiles()
        .filter(_.getName.endsWith(".checkpoint.parquet"))
        .map(_.getAbsolutePath)
        .head

      // Rewrite the checkpoint via a Spark coalesce(1) write with SingleAction.encoder.schema
      // (physically carries the all-null checkpointMetadata/sidecar columns), replacing the
      // Delta-written file -- mirrors DeltaIncrementalSetTransactionsSuite's corruption step.
      val ss = spark
      import ss.implicits._
      val cpDf = spark.read.schema(SingleAction.encoder.schema).parquet(checkpointPath)
      val n = cpDf.count().toInt
      val corrupted = cpDf.orderBy(col("txn.appId").asc_nulls_first).as[SingleAction].take(n - 1)
      val tmp = new File(logDir, "_tmp_cp").getAbsolutePath
      corrupted.toSeq.toDS().coalesce(1).write.mode("overwrite").parquet(tmp)
      val written = new File(tmp).listFiles().filter(_.getName.startsWith("part")).head
      logDir.listFiles().filter(_.getName.startsWith(".0")).foreach(_.delete())
      val cpFile = new File(checkpointPath)
      assert(cpFile.delete(), "delete old checkpoint")
      assert(written.renameTo(cpFile), "rename rewritten checkpoint")

      val leaked = spark.read.schema(Action.logSchema).parquet(checkpointPath)
        .select("checkpointMetadata", "sidecar")
        .where("checkpointMetadata.version is not null or sidecar.path is not null")
        .count()
      assert(leaked == 0L,
        s"nested OR filter leaked $leaked all-null rows (GetStructField null-mask not applied)")
    }
  }
}
