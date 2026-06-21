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

import org.apache.spark.sql.functions.col

// Repro for RowIdSuite "row_id column with row ids disabled".
// When row tracking is DISABLED, `row_id` (and `row_commit_version`) are ordinary user
// column names with no special meaning. Comet's native Delta scan derived its synthetic
// emit flags purely from the column NAME (emitRowId = fieldNames.exists(_ ~= "row_id")),
// so a physical user column named `row_id` was mistaken for the row-tracking synthetic:
// stripped from the parquet read and SYNTHESIZED (baseRowId + row_index) instead of read,
// returning wrong values. The synthetic path must only engage when row tracking is enabled.
class CometDeltaRowIdColumnCollisionReproSuite extends CometDeltaTestBase {

  test("user column named row_id is read verbatim when row tracking is disabled") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    val key = "spark.databricks.delta.properties.defaults.enableRowTracking"
    val prev = spark.conf.getOption(key)
    spark.conf.set(key, "false")
    try {
      withDeltaTable("row_id_collision") { tablePath =>
        val ss = spark
        import ss.implicits._
        spark
          .range(start = 0, end = 1000, step = 1, numPartitions = 5)
          .select((col("id") + 10000L).as("row_id"))
          .write
          .format("delta")
          .save(tablePath)

        val got = spark.read.format("delta").load(tablePath)
          .as[Long].collect().sorted
        val expected = (0 until 1000).map(_ + 10000L).toArray
        assert(
          got.sameElements(expected),
          s"user row_id column misread: got ${got.take(5).toSeq}..${got.takeRight(2).toSeq} " +
            s"(${got.length} rows); expected 10000..10999")
      }
    } finally {
      prev match {
        case Some(v) => spark.conf.set(key, v)
        case None => spark.conf.unset(key)
      }
    }
  }
}
