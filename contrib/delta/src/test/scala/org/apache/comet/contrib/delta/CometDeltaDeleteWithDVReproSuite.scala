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

// Repro for the Spark-3.5 DeleteSQLWithDeletionVectorsSuite cluster:
//   [INTERNAL_ERROR] The Spark SQL phase planning failed ... in
//   DeletionVectorBitmapGenerator.buildDeletionVectors (DELETE with deletion vectors
//   scans files emitting _metadata.row_index to build the DV bitmap).
// Reproduce a DV-enabled DELETE so the local log shows the full (untruncated) cause.
class CometDeltaDeleteWithDVReproSuite extends CometDeltaTestBase {

  test("DELETE on a deletion-vector table does not crash; result matches vanilla") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    // DeleteSQLWithDeletionVectorsSuite sets useMetadataRowIndex=false (see its beforeAll).
    // With it FALSE, Delta does NOT use the Parquet _metadata.row_index column; instead it
    // injects explicit "row index filter columns" into the scan schema for DV bitmap
    // generation. That is the scan shape that crashes during planning -- the default (true)
    // path goes through _metadata.row_index, which we already support, so reproducing the
    // crash requires turning this off.
    val key = "spark.databricks.delta.deletionVectors.useMetadataRowIndex"
    val prev = spark.conf.getOption(key)
    spark.conf.set(key, "false")
    info(s"useMetadataRowIndex now = ${spark.conf.get(key)}")
    try {
      withDeltaTable("delete_dv") { tablePath =>
        val ss = spark
        import ss.implicits._
        // Mirror DeleteSQLWithDeletionVectorsSuite "by path - Partition=true" exactly:
        // a 4-row table partitioned by key, DV enabled, then the same four sequential
        // DELETEs. Later DELETEs run against files that ALREADY carry a deletion vector
        // -- so the scan needs `_metadata.row_index` (-> `_tmp_metadata_row_index`) to
        // apply the existing DV AND `__delta_internal_row_index` for the new DV bitmap
        // (useMetadataRowIndex=false). Both row-index columns in one required_schema is
        // what trips CometDeltaNativeScan's single-row-index emit assertion.
        Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value")
          .write.format("delta").partitionBy("key")
          .option("delta.enableDeletionVectors", "true")
          .save(tablePath)

        def del(where: String): Unit =
          spark.sql(s"DELETE FROM delta.`$tablePath` WHERE $where")

        del("value = 4 and key = 3") // matches nothing
        del("value = 4 and key = 1") // deletes (1,4) -> table now has a DV
        del("value = 2 or key = 1") // deletes (2,2),(1,1) -> scans files WITH existing DVs
        del("key = 0 or value = 99") // deletes (0,3)

        val nativeRows = spark.read.format("delta").load(tablePath)
          .as[(Int, Int)].collect().sorted
        assert(
          nativeRows.isEmpty,
          s"DELETE-with-DV result wrong: expected all rows deleted, got ${nativeRows.toSeq}")
      }
    } finally {
      prev match {
        case Some(v) => spark.conf.set(key, v)
        case None => spark.conf.unset(key)
      }
    }
  }
}
