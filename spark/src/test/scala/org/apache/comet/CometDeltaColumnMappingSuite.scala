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

import org.apache.spark.sql.comet.CometDeltaNativeScanExec

class CometDeltaColumnMappingSuite extends CometDeltaTestBase {

  test("deletion vectors: accelerates DV-in-use tables via native DV filter") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("dv_accel") { tablePath =>
      val ss = spark
      import ss.implicits._

      (0 until 20)
        .map(i => (i.toLong, s"name_$i"))
        .toDF("id", "name")
        .repartition(1)
        .write
        .format("delta")
        .option("delta.enableDeletionVectors", "true")
        .option("delta.minReaderVersion", "3")
        .option("delta.minWriterVersion", "7")
        .save(tablePath)

      assertDeltaNativeMatches(tablePath, identity)

      spark.sql(s"DELETE FROM delta.`$tablePath` WHERE id % 3 = 0")

      val df = spark.read.format("delta").load(tablePath)
      val plan = df.queryExecution.executedPlan
      val deltaScans = collect(plan) { case s: CometDeltaNativeScanExec => s }
      assert(
        deltaScans.nonEmpty,
        s"expected Comet to accelerate a DV-in-use table, but plan has no " +
          s"CometDeltaNativeScanExec:\n$plan")

      val nativeRows = df.collect().toSeq.map(normalizeRow)
      withSQLConf(CometConf.COMET_DELTA_NATIVE_ENABLED.key -> "false") {
        val vanillaRows = spark.read
          .format("delta")
          .load(tablePath)
          .collect()
          .toSeq
          .map(normalizeRow)
        assert(
          nativeRows.sortBy(_.mkString("|")) == vanillaRows.sortBy(_.mkString("|")),
          s"native=$nativeRows\nvanilla=$vanillaRows")
      }
      assert(nativeRows.size == 13, s"expected 13 rows after DELETE, got ${nativeRows.size}")

      spark.sql(s"DELETE FROM delta.`$tablePath` WHERE id >= 18")
      val df2 = spark.read.format("delta").load(tablePath)
      val rows2 = df2.collect().toSeq.map(normalizeRow)
      assert(rows2.size == 12, s"expected 12 rows after second DELETE, got ${rows2.size}")
      val plan2 = df2.queryExecution.executedPlan
      assert(
        collect(plan2) { case s: CometDeltaNativeScanExec => s }.nonEmpty,
        s"expected Comet to still accelerate after second DELETE, got:\n$plan2")
    }
  }

  test("column mapping: name mode read after rename") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("col_mapping_name") { tablePath =>
      val ss = spark
      import ss.implicits._

      (0 until 8)
        .map(i => (i.toLong, s"name_$i", i * 1.5))
        .toDF("id", "name", "score")
        .write
        .format("delta")
        .option("delta.columnMapping.mode", "name")
        .option("delta.minReaderVersion", "2")
        .option("delta.minWriterVersion", "5")
        .save(tablePath)

      assertDeltaNativeMatches(tablePath, identity)

      spark.sql(s"ALTER TABLE delta.`$tablePath` RENAME COLUMN name TO full_name")

      assertDeltaNativeMatches(tablePath, identity)
      assertDeltaNativeMatches(tablePath, _.select("id", "full_name"))
    }
  }

  test("column mapping: id mode") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("col_mapping_id") { tablePath =>
      val ss = spark
      import ss.implicits._

      (0 until 6)
        .map(i => (i.toLong, s"name_$i"))
        .toDF("id", "name")
        .write
        .format("delta")
        .option("delta.columnMapping.mode", "id")
        .option("delta.minReaderVersion", "2")
        .option("delta.minWriterVersion", "5")
        .save(tablePath)

      assertDeltaNativeMatches(tablePath, identity)
      assertDeltaNativeMatches(tablePath, _.where("id > 2"))
    }
  }

  test("column mapping + deletion vectors combined") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("col_map_dv") { tablePath =>
      val ss = spark
      import ss.implicits._

      (0 until 20)
        .map(i => (i.toLong, s"name_$i", i * 1.5))
        .toDF("id", "name", "score")
        .repartition(1)
        .write
        .format("delta")
        .option("delta.columnMapping.mode", "name")
        .option("delta.minReaderVersion", "3")
        .option("delta.minWriterVersion", "7")
        .option("delta.enableDeletionVectors", "true")
        .save(tablePath)

      assertDeltaNativeMatches(tablePath, identity)

      spark.sql(s"ALTER TABLE delta.`$tablePath` RENAME COLUMN name TO full_name")
      withSQLConf("spark.databricks.delta.deletionVectors.useMetadataRowIndex" -> "false") {
        spark.sql(s"DELETE FROM delta.`$tablePath` WHERE id % 4 = 0")
        val df = spark.read.format("delta").load(tablePath)
        val nativeRows = df.collect().toSeq.map(normalizeRow)
        withSQLConf(CometConf.COMET_DELTA_NATIVE_ENABLED.key -> "false") {
          val vanillaRows = spark.read
            .format("delta")
            .load(tablePath)
            .collect()
            .toSeq
            .map(normalizeRow)
          assert(
            nativeRows.sortBy(_.mkString("|")) == vanillaRows.sortBy(_.mkString("|")),
            s"col mapping + DV: native=$nativeRows\nvanilla=$vanillaRows")
        }
        assert(nativeRows.size == 15, s"expected 15 rows after DELETE, got ${nativeRows.size}")
      }
    }
  }

  test("column mapping + schema evolution combined") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("col_map_evolve") { tablePath =>
      val ss = spark
      import ss.implicits._

      (0 until 10)
        .map(i => (i.toLong, s"name_$i"))
        .toDF("id", "name")
        .write
        .format("delta")
        .option("delta.columnMapping.mode", "name")
        .option("delta.minReaderVersion", "2")
        .option("delta.minWriterVersion", "5")
        .save(tablePath)

      (10 until 15)
        .map(i => (i.toLong, s"name_$i", i * 2.0))
        .toDF("id", "name", "score")
        .write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(tablePath)

      assertDeltaNativeMatches(tablePath, identity)
      assertDeltaNativeMatches(tablePath, _.where("score IS NOT NULL"))
    }
  }
}
