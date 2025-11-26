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

import scala.util.Random

import org.apache.spark.sql.{CometTestBase, DataFrame}
import org.apache.spark.sql.comet.CometNativeWriteExec
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf
import org.apache.comet.testing.{DataGenOptions, FuzzDataGenerator, SchemaGenOptions}

class CometParquetWriterSuite extends CometTestBase {

  test("basic parquet write") {
    // no support for fully native scan as input yet
    assume(CometConf.COMET_NATIVE_SCAN_IMPL.get() != CometConf.SCAN_NATIVE_DATAFUSION)

    withTempPath { dir =>
      val outputPath = new File(dir, "output.parquet").getAbsolutePath

      // Create test data and write it to a temp parquet file first
      withTempPath { inputDir =>
        val inputPath = new File(inputDir, "input.parquet").getAbsolutePath
        val schema = FuzzDataGenerator.generateSchema(
          SchemaGenOptions(generateArray = false, generateStruct = false, generateMap = false))
        val df = FuzzDataGenerator.generateDataFrame(
          new Random(42),
          spark,
          schema,
          1000,
          DataGenOptions(generateNegativeZero = false))
        withSQLConf(
          CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "false",
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Denver") {
          df.write.parquet(inputPath)
        }

        withSQLConf(
          CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Halifax",
          CometConf.getOperatorAllowIncompatConfigKey(classOf[DataWritingCommandExec]) -> "true",
          CometConf.COMET_EXEC_ENABLED.key -> "true") {
          val df = spark.read.parquet(inputPath)

          // Use a listener to capture the execution plan during write
          var capturedPlan: Option[QueryExecution] = None

          val listener = new org.apache.spark.sql.util.QueryExecutionListener {
            override def onSuccess(
                funcName: String,
                qe: QueryExecution,
                durationNs: Long): Unit = {
              // Capture plans from write operations
              if (funcName == "save" || funcName.contains("command")) {
                capturedPlan = Some(qe)
              }
            }

            override def onFailure(
                funcName: String,
                qe: QueryExecution,
                exception: Exception): Unit = {}
          }

          spark.listenerManager.register(listener)

          try {
            // Perform native write
            df.write.parquet(outputPath)

            // Wait for listener to be called with timeout
            val maxWaitTimeMs = 15000
            val checkIntervalMs = 100
            val maxIterations = maxWaitTimeMs / checkIntervalMs
            var iterations = 0

            while (capturedPlan.isEmpty && iterations < maxIterations) {
              Thread.sleep(checkIntervalMs)
              iterations += 1
            }

            // Verify that CometNativeWriteExec was used
            assert(
              capturedPlan.isDefined,
              s"Listener was not called within ${maxWaitTimeMs}ms - no execution plan captured")

            capturedPlan.foreach { qe =>
              val executedPlan = qe.executedPlan
              val hasNativeWrite = executedPlan.exists {
                case _: CometNativeWriteExec => true
                case d: DataWritingCommandExec =>
                  d.child.exists {
                    case _: CometNativeWriteExec => true
                    case _ => false
                  }
                case _ => false
              }

              assert(
                hasNativeWrite,
                s"Expected CometNativeWriteExec in the plan, but got:\n${executedPlan.treeString}")
            }
          } finally {
            spark.listenerManager.unregister(listener)
          }

          // Verify the data was written correctly
          val resultDf = spark.read.parquet(outputPath)
          assert(resultDf.count() == 1000, "Expected 1000 rows to be written")

          // Verify multiple part files were created
          val outputDir = new File(outputPath)
          val partFiles = outputDir.listFiles().filter(_.getName.startsWith("part-"))
          // With 1000 rows and default parallelism, we should get multiple partitions
          assert(partFiles.length > 1, "Expected multiple part files to be created")

          // read with and without Comet and compare
          var sparkDf: DataFrame = null
          var cometDf: DataFrame = null
          withSQLConf(CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "false") {
            sparkDf = spark.read.parquet(outputPath)
          }
          withSQLConf(CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "true") {
            cometDf = spark.read.parquet(outputPath)
          }
          checkAnswer(sparkDf, cometDf)
        }
      }
    }
  }

}
