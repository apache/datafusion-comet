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

// Deterministic mirror of DeltaColumnMappingSuite "column mapping batch scan should detect
// physical name changes" (id mode). df2 is analyzed before the table is overwritten with new
// physical names/field-ids; reading it afterward (schema-on-read check off) must yield NULLs.
// Native-only fresh collect (no vanilla-first collect, which would cache the pinned snapshot
// and mask the bug).
class CometDeltaColumnMappingPhysicalNameReproSuite extends CometDeltaTestBase {

  test("column mapping batch scan should detect physical name changes [id]") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withSQLConf("spark.databricks.delta.properties.defaults.columnMapping.mode" -> "id") {
      withDeltaTable("cm_physical_name") { tablePath =>
        spark.range(10).toDF("id").write.format("delta").save(tablePath)
        val df2 = spark.read.format("delta").load(tablePath)
        df2.queryExecution.analyzed
        withSQLConf(
          "spark.databricks.delta.columnMapping.reuseColumnMetadataDuringOverwrite" -> "false") {
          spark.range(10).toDF("id")
            .write.format("delta").option("overwriteSchema", "true").mode("overwrite")
            .save(tablePath)
        }
        withSQLConf("spark.databricks.delta.checkLatestSchemaOnRead" -> "false") {
          val rows = df2.collect()
          val nonNull = rows.count(!_.isNullAt(0))
          assert(
            rows.length == 10 && nonNull == 0,
            s"stale physical name should read NULL: ${rows.length} rows, $nonNull non-null " +
              s"(sample=${rows.take(5).map(r => if (r.isNullAt(0)) "null" else r.getLong(0)).toSeq})")
        }
      }
    }
  }
}
