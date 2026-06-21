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

  // Guards a regression introduced by the nested-name reconcile fix: a MERGE UPDATE that touches a
  // struct field scans the target with ZERO data columns, yielding an empty column vec with an empty
  // output_schema. `RecordBatch::try_new` cannot infer the row count from zero columns ("must either
  // specify a row count or at least one column"); append_partition_columns must carry it explicitly.
  // Mirrors MergeIntoSchemaEvolutionSuite "existing top-level column assignment qualified with target
  // name". (COUNT(*) is a poor proxy here -- Delta answers it metadata-only via LocalTableScan, so no
  // Comet scan runs.)
  test("MERGE UPDATE of a struct field (zero-data-column target scan)") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    import testImplicits._
    withDeltaTable("merge_struct_update") { tablePath =>
      Seq((0, 1), (2, 9))
        .toDF("a", "nested_a")
        .selectExpr("a", "named_struct('a', nested_a) as target")
        .write
        .format("delta")
        .save(tablePath)
      Seq((2, 3))
        .toDF("a", "nested_a")
        .selectExpr("a", "named_struct('a', nested_a) as target")
        .createOrReplaceTempView("merge_struct_src")
      spark.sql(s"""
        MERGE INTO delta.`$tablePath` t
        USING merge_struct_src s
        ON t.a = s.a
        WHEN MATCHED THEN UPDATE SET t.target.a = s.target.a
      """)
      checkSparkAnswerAndOperator(
        spark.read.format("delta").load(tablePath).orderBy("a"))
    }
  }

  // The dominant "FAILED_READ_FILE on test%file%prefix" own-suite family is actually a NESTED
  // schema-evolution bug, not a path bug (the % is incidental to Delta's test file prefix). An old
  // data file has a struct with N fields; after ALTER ADD COLUMNS the table schema expects N+1, and
  // align_batch_to_schema's arrow_cast cannot add the missing nested field -- it errors
  // ("Incorrect number of arrays for StructArray fields, expected N+1 got N"), which
  // map_file_read_error then classifies as FAILED_READ_FILE. Spark null-fills the added field. This
  // asserts Comet matches Spark (null-fill), so it is RED (Comet throws) before the recursive
  // reconcile fix and GREEN after.
  test("read after ALTER ADD nested struct field (schema-evolution null-fill)") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("nested_add_field") { tablePath =>
      // Data file written with struct<a:int, b:int>.
      spark
        .range(4)
        .selectExpr("named_struct('a', cast(id as int), 'b', cast(id * 2 as int)) as s")
        .write
        .format("delta")
        .save(tablePath)
      // Evolve the schema: add a nested field `c`. The existing file still has only a,b.
      spark.sql(s"ALTER TABLE delta.`$tablePath` ADD COLUMNS (s.c int)")
      // Reading the old file must null-fill s.c. Comet errored ("3 vs 2") before the fix.
      checkSparkAnswerAndOperator(spark.read.format("delta").load(tablePath))
    }
  }

  // Guards the zero-data-column row-count regression introduced by the nested-name reconcile work:
  // a literal projection reads ZERO data columns from an unpartitioned table but still scans for the
  // row count. append_partition_columns built the batch with `RecordBatch::try_new`, which can't
  // infer the row count from zero columns ("must either specify a row count or at least one
  // column"); it must use `try_new_with_options(with_row_count)`. (COUNT(*) is a poor proxy here --
  // Delta answers it metadata-only via LocalTableScan, so no Comet scan runs.)
  test("literal projection over unpartitioned table (zero data columns)") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("zero_data_literal") { tablePath =>
      spark.range(5).toDF("id").write.format("delta").save(tablePath)
      checkSparkAnswerAndOperator(
        spark.read.format("delta").load(tablePath).selectExpr("1 as x"))
    }
  }
}
