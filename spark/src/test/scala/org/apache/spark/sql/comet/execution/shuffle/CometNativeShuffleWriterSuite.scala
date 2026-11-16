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

package org.apache.spark.sql.comet.execution.shuffle

import java.io.File

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.CometConf

/**
 * Checks the plan [[CometNativeShuffleWriter]] hands to native code. Lives in the
 * `execution.shuffle` package so it can call the `private[shuffle]` `buildUnifiedPlan`.
 */
class CometNativeShuffleWriterSuite extends CometTestBase {

  import testImplicits._

  test("the write buffer size reaches native code in bytes") {
    withSQLConf(
      CometConf.COMET_SHUFFLE_MODE.key -> "native",
      "spark.sql.adaptive.enabled" -> "false") {
      withParquetTable((0 until 10).map(i => (i, s"row-$i")), "tbl") {
        val exchange = sql("SELECT * FROM tbl")
          .repartition(3, $"_1")
          .queryExecution
          .executedPlan
          .collectFirst { case value: CometShuffleExchangeExec => value }
          .getOrElse(fail("Expected a native Comet shuffle exchange"))
        val dependency = exchange.shuffleDependency
          .asInstanceOf[CometShuffleDependency[Int, ColumnarBatch, ColumnarBatch]]
        val context: TaskContext = TaskContext.empty()

        // Native code sizes the writers for the data and spill files from this field, in bytes.
        def writeBufferSize: Int =
          new CometNativeShuffleWriter[Int, ColumnarBatch](
            dependency.nativeShuffleSpec.get,
            dependency.outputPartitioning.get,
            dependency.outputAttributes,
            dependency.shuffleWriteMetrics,
            dependency.numParts,
            dependency.shuffleId,
            context.taskAttemptId(),
            context,
            context.taskMetrics().shuffleWriteMetrics,
            dependency.rangePartitionBounds)
            .buildUnifiedPlan("unused.data")
            .getShuffleWriter
            .getWriteBufferSize

        assert(writeBufferSize == 1024 * 1024)
        withSQLConf(CometConf.COMET_SHUFFLE_NATIVE_WRITE_BUFFER_SIZE.key -> "8m") {
          assert(writeBufferSize == 8 * 1024 * 1024)
        }
        // A bare number is a byte count.
        withSQLConf(CometConf.COMET_SHUFFLE_NATIVE_WRITE_BUFFER_SIZE.key -> "65536") {
          assert(writeBufferSize == 65536)
        }
      }
    }
  }

  test("native shuffle temp file stays in a local dir named local.data") {
    withTempDir { dir =>
      val base = new File(dir, "local.data")
      assert(base.mkdir())
      val dataFile = new File(base, "shuffle_0_3_0.data")
      val resolver = SparkEnv.get.shuffleManager.shuffleBlockResolver
        .asInstanceOf[IndexShuffleBlockResolver]
      val temp = CometNativeShuffleWriter.localTempDataFile(resolver, dataFile)
      assert(temp.getParentFile.getName == "local.data")
      assert(!temp.getPath.contains("local.data.tmp"))
    }
  }

  test("native shuffle stop(false) deletes a tracked temp file") {
    withTempDir { dir =>
      val failed = new File(dir, "shuffle_0_0_0.data.failed")
      assert(failed.createNewFile())
      val writer = new CometNativeShuffleWriter[Int, ColumnarBatch](null, null, null, null, 0, 0,
        0L, null, null)
      writer.localTempData = Some(failed)
      assert(writer.stop(false).isEmpty)
      assert(!failed.exists())

      val committed = new File(dir, "shuffle_0_0_0.data.committed")
      assert(committed.createNewFile())
      writer.localTempData = Some(committed)
      assert(writer.stop(true).isDefined)
      assert(committed.exists())
    }
  }
}
