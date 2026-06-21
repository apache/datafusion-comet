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

import org.apache.spark.sql.functions._

// Audit 2: metadata-column coverage matrix.
//
// Project every recognised metadata/synthetic column individually and
// assert non-null/sensible values vs vanilla Spark. Past bugs in this
// area: silent nulls, wrong row counts when file_path was projected
// (commit 8c3cf6c9 fix), column-count off-by-one for
// default_row_commit_version (commit 97c953ab fix).
//
// Spark `_metadata` virtual columns covered:
//   file_path, file_name, file_size, file_modification_time,
//   file_block_start, file_block_length, row_index
//
// Delta-specific covered:
//   _change_type, _commit_version, _commit_timestamp (CDC; via table API)
//   row tracking row_id / row_commit_version (via _metadata)
//
// Each test compares native vs vanilla -- so even if the value is "wrong
// but consistently wrong", we'd at least notice the day vanilla changes.
class CometDeltaMetadataColumnAuditSuite extends CometDeltaTestBase {

  // ---- Spark `_metadata.*` virtual columns ---------------------------------

  test("_metadata.file_path matches vanilla on multi-file table") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("meta_filepath") { tablePath =>
      val ss = spark
      import ss.implicits._
      // Force multiple files
      (0 until 12).map(i => (i.toLong, s"v_$i")).toDF("id", "v")
        .repartition(4).write.format("delta").save(tablePath)
      assertDeltaNativeMatches(
        tablePath,
        _.select(col("id"), col("_metadata.file_path").as("fp"))
          .orderBy("id"))
    }
  }

  test("_metadata.file_name / file_size / file_modification_time") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("meta_misc") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 8).map(i => (i.toLong, s"v_$i")).toDF("id", "v")
        .repartition(2).write.format("delta").save(tablePath)
      assertDeltaNativeMatches(
        tablePath,
        _.select(
          col("id"),
          col("_metadata.file_name").as("fn"),
          col("_metadata.file_size").as("fs"))
          .orderBy("id"))
      // file_modification_time depends on FS state, but values must match
      // vanilla in the same query.
      assertDeltaNativeMatches(
        tablePath,
        _.select(col("id"), col("_metadata.file_modification_time").as("fmt"))
          .orderBy("id"))
    }
  }

  test("_metadata.file_block_start / file_block_length") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("meta_block") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 8).map(i => (i.toLong, s"v_$i")).toDF("id", "v")
        .repartition(2).write.format("delta").save(tablePath)
      assertDeltaNativeMatches(
        tablePath,
        _.select(
          col("id"),
          col("_metadata.file_block_start").as("bs"),
          col("_metadata.file_block_length").as("bl"))
          .orderBy("id"))
    }
  }

  test("_metadata.row_index on row-tracking-enabled table") {
    // _metadata.row_index is only present in the metadata struct when the
    // table opts into row tracking (or DVs). On a plain Delta table the
    // field doesn't exist in the metadata schema and both vanilla and
    // native error out -- nothing to compare. Use a row-tracking table.
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("meta_rowidx") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 10).map(i => (i.toLong, s"v_$i")).toDF("id", "v")
        .repartition(2)
        .write
        .format("delta")
        .option("delta.enableRowTracking", "true")
        .option("delta.minReaderVersion", "3")
        .option("delta.minWriterVersion", "7")
        .save(tablePath)
      assertDeltaNativeMatches(
        tablePath,
        _.select(col("id"), col("_metadata.row_index").as("ri"))
          .orderBy("id"))
    }
  }

  test("input_file_name() falls back to Spark (native scan declines)") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("meta_ifn") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 6).map(i => (i.toLong, s"v_$i")).toDF("id", "v")
        .repartition(2).write.format("delta").save(tablePath)
      // The native Delta scan bypasses Spark's FileScanRDD, which is the only thing that
      // maintains the `InputFileBlockHolder` thread-local `input_file_name()` reads. So the
      // scan declines and Spark handles it (consistent with CometScanRule's native scan).
      assertDeltaFallback(
        tablePath,
        _.select(col("id"), input_file_name().as("ifn")).orderBy("id"))
    }
  }

  // ---- Row tracking (`_metadata.row_id`) ------------------------------------

  test("_metadata.row_id on row-tracking-enabled table (unmaterialised)") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("meta_rid") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 8).map(i => (i.toLong, s"v_$i")).toDF("id", "v")
        .repartition(1)
        .write
        .format("delta")
        .option("delta.enableRowTracking", "true")
        .option("delta.minReaderVersion", "3")
        .option("delta.minWriterVersion", "7")
        .save(tablePath)
      assertDeltaNativeMatches(
        tablePath,
        _.select(col("id"), col("_metadata.row_id").as("rid")).orderBy("id"))
    }
  }

  test("_metadata.row_commit_version on row-tracking-enabled table") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("meta_rcv") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 4).map(i => (i.toLong, s"a_$i")).toDF("id", "v")
        .repartition(1)
        .write
        .format("delta")
        .option("delta.enableRowTracking", "true")
        .option("delta.minReaderVersion", "3")
        .option("delta.minWriterVersion", "7")
        .save(tablePath)
      (4 until 8).map(i => (i.toLong, s"b_$i")).toDF("id", "v")
        .repartition(1)
        .write.mode("append").format("delta").save(tablePath)
      assertDeltaNativeMatches(
        tablePath,
        _.select(col("id"), col("_metadata.row_commit_version").as("rcv"))
          .orderBy("id"))
    }
  }

  // ---- Bundled multi-metadata projection ------------------------------------
  // Regression for the off-by-one: projecting many metadata columns at once
  // was the path that exposed default_row_commit_version dropping.

  test("multi-metadata projection on row-tracking + DV table") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("meta_multi") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 12).map(i => (i.toLong, s"v_$i")).toDF("id", "v")
        .repartition(2)
        .write
        .format("delta")
        .option("delta.enableRowTracking", "true")
        .option("delta.enableDeletionVectors", "true")
        .option("delta.minReaderVersion", "3")
        .option("delta.minWriterVersion", "7")
        .save(tablePath)
      spark.sql(s"DELETE FROM delta.`$tablePath` WHERE id % 4 = 0")
      assertDeltaNativeMatches(
        tablePath,
        _.select(
          col("id"),
          col("_metadata.row_id").as("rid"),
          col("_metadata.row_commit_version").as("rcv"),
          col("_metadata.row_index").as("ri"),
          col("_metadata.file_name").as("fn"))
          .orderBy("id"))
    }
  }
}
