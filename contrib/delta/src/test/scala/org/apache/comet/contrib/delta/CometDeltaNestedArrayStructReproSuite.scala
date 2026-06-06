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

import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructType}

// Repro for the dominant Delta 4.1 own-suite failure family: ~95 "schema evolution -
// array of struct / nested / map of struct" MERGE tests panic in the kernel-read scan with
//
//   native panic: assertion `left == right` failed
//   left:  List(Field { name: "element", data_type: Struct([...]), nullable: true })
//   right: List(Field {                  data_type: Struct([...]), nullable: true })   // name ""
//
// (and the same mismatch hit in arrow-select coalesce.rs:539). A *plain* read of a consistent
// array<struct> table does NOT trip it -- the trigger is a MERGE with schema evolution over an
// array<struct> column, where the read batch's nested list-element field is named "element"
// (Spark convention) but `DeltaKernelScanExec.output_schema` carries an empty nested name, so
// DataFusion's coalescer asserts `batch.schema == output_schema` and panics. The old ParquetSource
// path normalized nested names via Comet's SparkSchemaAdapter.
//
// Mirrors MergeIntoSchemaEvolutionSuite "array of struct with same columns but in different order
// which can be casted implicitly - by name".
class CometDeltaNestedArrayStructReproSuite extends CometDeltaTestBase {

  test("MERGE array<struct> with reordered nested fields (schema evolution)") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    import testImplicits._

    val targetSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(new StructType().add("a", IntegerType).add("b", IntegerType)))
    // Source's struct fields are in the opposite order (b, a) -> implicit cast by name.
    val sourceSchema = new StructType()
      .add("key", StringType)
      .add("value", ArrayType(new StructType().add("b", IntegerType).add("a", IntegerType)))

    withDeltaTable("merge_arr_struct_reorder") { tablePath =>
      spark.read
        .schema(targetSchema)
        .json(Seq("""{ "key": "A", "value": [ { "a": 1, "b": 2 } ] }""").toDS)
        .write
        .format("delta")
        .save(tablePath)

      spark.read
        .schema(sourceSchema)
        .json(
          Seq(
            """{ "key": "A", "value": [ { "b": 4, "a": 3 } ] }""",
            """{ "key": "B", "value": [ { "b": 2, "a": 5 } ] }""").toDS)
        .createOrReplaceTempView("merge_src")

      withSQLConf("spark.databricks.delta.schema.autoMerge.enabled" -> "true") {
        spark.sql(s"""
          MERGE INTO delta.`$tablePath` t
          USING merge_src s
          ON t.key = s.key
          WHEN MATCHED THEN UPDATE SET *
          WHEN NOT MATCHED THEN INSERT *
        """)
      }

      // Read back the evolved array<struct> table through the kernel-read scan; before the fix this
      // panics on the nested list-element name mismatch. checkSparkAnswerAndOperator asserts native
      // engagement (so a silent fallback can't mask the bug) and result parity with vanilla.
      checkSparkAnswerAndOperator(
        spark.read.format("delta").load(tablePath).orderBy("key"))
    }
  }
}
