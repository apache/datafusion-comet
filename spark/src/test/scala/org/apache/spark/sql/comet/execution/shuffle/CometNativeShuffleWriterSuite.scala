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

import org.apache.spark.TaskContext
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
}
