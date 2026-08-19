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

// Local red->green guard for task #78 (a regression introduced by the kernel-schema-shipping work,
// caught by the own-suite DeltaColumnMappingSuite "physical name changes" / "explicit id matching").
//
// Delta's schema-on-read ESCAPE HATCH: a DataFrame analyzed at version V (column-mapping physical
// name X for `id`) is read AFTER the table's schema was overwritten with a NEW physical name (Y),
// with the schema-on-read safety check disabled. Spark reads the CURRENT files (physical name Y)
// under the ANALYSIS-TIME physical name (X) -> X absent from the files -> NULL.
//
// Kernel models this correctly on its own: `ScanBuilder::with_schema` is meant to take the READER's
// (analysis-time) schema, and kernel resolves physical names from THAT schema's own column-mapping
// annotations (`StateInfo::try_new` -> `make_physical`), then null-fills columns whose field-id
// changed since analysis. The bug: the kernel-read driver fed `with_schema` the LIVE snapshot
// schema (current physical name Y) instead of the analysis-time schema, so Comet read the new data
// instead of null-filling. checkSparkAnswerAndOperator therefore RED (Comet returns data, Spark
// returns NULL) before the fix and GREEN (both NULL) after.
class CometDeltaSchemaChangeReproSuite extends CometDeltaTestBase {

  test("kernel-read: schema change since analysis null-fills (escape hatch, #78)") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("schema_change_since_analysis") { tablePath =>
      // Column-mapping (name mode) table; write 10 rows -> `id` gets physical name X.
      spark.sql(s"""CREATE TABLE delta.`$tablePath` (id BIGINT)
                   |USING delta
                   |TBLPROPERTIES (
                   |  'delta.columnMapping.mode' = 'name',
                   |  'delta.minReaderVersion' = '2',
                   |  'delta.minWriterVersion' = '5')""".stripMargin)
      spark.range(10).toDF("id").write.format("delta").mode("append").save(tablePath)

      // DataFrame analyzed against the current version (physical name X).
      val df = spark.read.format("delta").load(tablePath)

      // Overwrite the schema with a fresh physical name (reuse of column-mapping metadata disabled),
      // so `id`'s physical name becomes Y on disk.
      withSQLConf(
        "spark.databricks.delta.columnMapping.reuseColumnMetadataDuringOverwrite" -> "false") {
        spark
          .range(10)
          .toDF("id")
          .write
          .format("delta")
          .option("overwriteSchema", "true")
          .mode("overwrite")
          .save(tablePath)
      }

      // Read the pre-overwrite DataFrame with the schema-on-read check disabled. Spark reads the
      // current (Y) files under the analysis-time name (X) -> NULL. Comet must match (kernel
      // field-id matching null-fills). Before the fix Comet fed kernel the live snapshot schema and
      // read the new data -> mismatch.
      withSQLConf("spark.databricks.delta.checkLatestSchemaOnRead" -> "false") {
        checkSparkAnswerAndOperator(df)
      }
    }
  }
}
