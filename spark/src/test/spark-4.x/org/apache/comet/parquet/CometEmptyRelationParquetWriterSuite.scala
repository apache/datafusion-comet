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

package org.apache.comet.parquet

import java.io.File

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.comet.CometEmptyRelationExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

import org.apache.comet.CometConf

class CometEmptyRelationParquetWriterSuite extends CometParquetWriterTestBase {

  test("EmptyRelationExec keeps AQE empty parquet output readable") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.SHUFFLE_PARTITIONS.key -> "2",
      "spark.sql.optimizer.plannedWrite.enabled" -> "true",
      CometConf.COMET_SPARK_TO_ARROW_ENABLED.key -> "false",
      CometConf.COMET_SHUFFLE_CONVERT_FROM_SPARK_PLAN_ENABLED.key -> "false") {
      withNativeWriter {
        withTempPath { dir =>
          for (nativeEmpty <- Seq(false, true)) {
            withSQLConf(CometConf.COMET_EXEC_EMPTY_RELATION_ENABLED.key -> nativeEmpty.toString) {
              val outputPath = new File(dir, s"empty_$nativeEmpty.parquet").getAbsolutePath
              withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
                sql("SELECT CAST(1 AS INT) AS k, CAST(2 AS BIGINT) AS v").write.parquet(
                  outputPath)
              }
              // AQE replaces the completed empty shuffle with EmptyRelationExec and reruns
              // preparation of the whole write command, including native-writer eligibility.
              val empty = sql(
                "SELECT CAST(id % 2 AS INT) AS k, sum(id) AS v " +
                  "FROM range(0, 10, 1, 2) WHERE id < 0 GROUP BY id % 2")
              val plan = captureWritePlan(
                path => empty.write.mode(SaveMode.Overwrite).parquet(path),
                outputPath)
              withClue(s"nativeEmpty=$nativeEmpty\n$plan\n") {
                // Infer the schema from the output files, as a normal downstream reader does.
                // Supplying an explicit schema would hide the absence of a Parquet data file.
                withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
                  val readback = spark.read.parquet(outputPath)
                  assert(readback.schema == StructType(empty.schema.map(_.copy(nullable = true))))
                  assert(readback.collect().isEmpty)
                }
                if (nativeEmpty) {
                  assert(collect(plan) { case e: CometEmptyRelationExec => e }.nonEmpty)
                  // A native empty relation is a zero-partition RDD, which is what
                  // CometDataWritingCommand declines on the Spark 3.x path: that writer only maps
                  // existing partitions, so no task runs and no file is written. Spark 4.0+ goes
                  // through the WriteFilesExec seam instead, where CometWriteFilesExec swaps in a
                  // dummy single-partition RDD exactly as Spark's own WriteFilesExec does, so
                  // partition 0 still writes the schema-only file the readback above needs.
                  assertHasCometNativeWriteExec(plan)
                } else {
                  // Spark's own EmptyRelationExec is not a Comet operator, so
                  // CometWriteFiles.requiresNativeChildren keeps the write on Spark.
                  assertNoCometNativeWriteExec(plan)
                }
              }
            }
          }
        }
      }
    }
  }
}
