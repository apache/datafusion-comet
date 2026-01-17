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

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.functions._

import org.apache.comet.CometConf

class CometParquetWriterCommitSuite extends CometTestBase {

  private val nativeWriteConf = Seq(
    CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
    CometConf.COMET_EXEC_ENABLED.key -> "true",
    CometConf.getOperatorAllowIncompatConfigKey(classOf[DataWritingCommandExec]) -> "true")

  private def hasDataFiles(dir: File): Boolean = {
    if (!dir.exists()) return false
    dir.listFiles().exists(f => f.getName.startsWith("part-") && f.getName.endsWith(".parquet"))
  }

  test("_temporary folder is created during write and cleaned up after commit") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output").getAbsolutePath

      val df = spark
        .range(0, 100000, 1, 4)
        .selectExpr("id", "id * 2 as value")

      withSQLConf(nativeWriteConf: _*) {
        @volatile var writeStarted = false
        @volatile var writeException: Option[Throwable] = None
        val writeThread = new Thread(() => {
          try {
            writeStarted = true
            df.write.parquet(outputPath)
          } catch {
            case e: Throwable => writeException = Some(e)
          }
        })
        writeThread.start()

        CometWriteTestHelpers.waitForCondition(writeStarted, timeoutMs = 5000)

        val tempExists = CometWriteTestHelpers.waitForCondition(
          CometWriteTestHelpers.hasTemporaryFolder(outputPath),
          timeoutMs = 10000)

        if (tempExists) {
          assert(
            CometWriteTestHelpers.hasTemporaryFolder(outputPath),
            "_temporary folder should be created during write")

          val tempFileCount = CometWriteTestHelpers.countTemporaryFiles(outputPath)
          assert(tempFileCount > 0, s"Expected temp files during write, found $tempFileCount")
        }

        writeThread.join(30000)
        assert(!writeThread.isAlive, "Write should complete within 30 seconds")

        writeException.foreach(throw _)

        assert(
          !CometWriteTestHelpers.hasTemporaryFolder(outputPath),
          "_temporary folder should be cleaned up after successful commit")

        val outputDir = new File(outputPath)
        assert(hasDataFiles(outputDir), "Final data files should exist")

        val readDf = spark.read.parquet(outputPath)
        assert(readDf.count() == 100000, "All rows should be committed")
      }
    }
  }

  test("_temporary folder is cleaned up on task failure") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output").getAbsolutePath

      val divideByZero = udf((x: Long) => { x / (x - 100) })
      val df = spark
        .range(0, 1000, 1, 1) // single partition to avoid race conditions
        .select(divideByZero(col("id")).as("value"))

      withSQLConf(nativeWriteConf: _*) {
        intercept[Exception] {
          df.write.parquet(outputPath)
        }

        // small delay for cleanup to complete
        Thread.sleep(1000)
        assert(
          !CometWriteTestHelpers.hasTemporaryFolder(outputPath),
          "_temporary folder should be cleaned up after task failure")

        val outputDir = new File(outputPath)
        if (outputDir.exists()) {
          assert(!hasDataFiles(outputDir), "No data files should exist after failure")
        }
      }
    }
  }

  test("_temporary folder handles concurrent tasks correctly") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output").getAbsolutePath

      val df = spark
        .range(0, 50000, 1, 10)
        .selectExpr("id", "id * 2 as value")

      withSQLConf(nativeWriteConf: _*) {
        @volatile var writeStarted = false
        @volatile var writeException: Option[Throwable] = None
        val writeThread = new Thread(() => {
          try {
            writeStarted = true
            df.write.parquet(outputPath)
          } catch {
            case e: Throwable => writeException = Some(e)
          }
        })
        writeThread.start()

        CometWriteTestHelpers.waitForCondition(writeStarted, timeoutMs = 5000)

        val tempAppeared = CometWriteTestHelpers.waitForCondition(
          CometWriteTestHelpers.hasTemporaryFolder(outputPath),
          timeoutMs = 10000)

        if (tempAppeared) {
          val subfolders = CometWriteTestHelpers.getTemporarySubfolders(outputPath)
          assert(subfolders.nonEmpty, "Should have job tracking folders")
        }

        writeThread.join(30000)

        writeException.foreach(throw _)

        assert(
          !CometWriteTestHelpers.hasTemporaryFolder(outputPath),
          "_temporary should be cleaned up after commit")

        val outputDir = new File(outputPath)
        assert(hasDataFiles(outputDir), "Data files should exist")

        val readDf = spark.read.parquet(outputPath)
        assert(readDf.count() == 50000, "All rows should be committed")
      }
    }
  }

  test("_temporary folder is cleaned up on overwrite") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output").getAbsolutePath

      withSQLConf(nativeWriteConf: _*) {
        spark.range(1000).write.parquet(outputPath)
        assert(
          !CometWriteTestHelpers.hasTemporaryFolder(outputPath),
          "_temporary should be cleaned up after first write")

        val count1 = spark.read.parquet(outputPath).count()
        assert(count1 == 1000, "First write should have 1000 rows")

        spark.range(500).write.mode("overwrite").parquet(outputPath)
        assert(
          !CometWriteTestHelpers.hasTemporaryFolder(outputPath),
          "_temporary should be cleaned up after overwrite")

        val count2 = spark.read.parquet(outputPath).count()
        assert(count2 == 500, "Overwrite should result in 500 rows")
      }
    }
  }

  test("small writes may not create visible _temporary folder") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output").getAbsolutePath

      withSQLConf(nativeWriteConf: _*) {
        spark.range(10).write.parquet(outputPath)

        assert(
          !CometWriteTestHelpers.hasTemporaryFolder(outputPath),
          "_temporary should not exist after completion")

        val outputDir = new File(outputPath)
        assert(hasDataFiles(outputDir), "Data files should exist")

        val readDf = spark.read.parquet(outputPath)
        assert(readDf.count() == 10, "Should have 10 rows")
      }
    }
  }

  test("multiple concurrent writes to different paths are isolated") {
    withTempPath { dir1 =>
      withTempPath { dir2 =>
        val outputPath1 = new File(dir1, "output1").getAbsolutePath
        val outputPath2 = new File(dir2, "output2").getAbsolutePath

        withSQLConf(nativeWriteConf: _*) {
          val df1 = spark.range(0, 10000, 1, 4)
          val df2 = spark.range(10000, 20000, 1, 4)

          val thread1 = new Thread(() => df1.write.parquet(outputPath1))
          val thread2 = new Thread(() => df2.write.parquet(outputPath2))

          thread1.start()
          thread2.start()

          thread1.join(30000)
          thread2.join(30000)

          assert(!CometWriteTestHelpers.hasTemporaryFolder(outputPath1))
          assert(!CometWriteTestHelpers.hasTemporaryFolder(outputPath2))

          assert(spark.read.parquet(outputPath1).count() == 10000)
          assert(spark.read.parquet(outputPath2).count() == 10000)
        }
      }
    }
  }

  test("no stale _temporary folders from previous operations") {
    withTempPath { dir =>
      val outputPath = new File(dir, "output").getAbsolutePath

      withSQLConf(nativeWriteConf: _*) {
        spark.range(100).write.parquet(outputPath)
        assert(!CometWriteTestHelpers.hasTemporaryFolder(outputPath))

        spark.range(200).write.mode("overwrite").parquet(outputPath)
        assert(!CometWriteTestHelpers.hasTemporaryFolder(outputPath))

        spark.range(300).write.mode("overwrite").parquet(outputPath)
        assert(!CometWriteTestHelpers.hasTemporaryFolder(outputPath))

        assert(spark.read.parquet(outputPath).count() == 300)

        val dirs = CometWriteTestHelpers.listDirectories(outputPath)
        assert(!dirs.exists(_.startsWith("_temporary")), "No _temporary folders should exist")
      }
    }
  }
}
