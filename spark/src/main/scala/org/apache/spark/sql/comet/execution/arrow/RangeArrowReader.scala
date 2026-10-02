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

package org.apache.spark.sql.comet.execution.arrow

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{BigIntVector, BitVectorHelper}
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.TaskContext
import org.apache.spark.unsafe.Platform

/**
 * Produces the values of one `RangeExec` partition as Arrow batches of a single non-null `BIGINT`
 * column.
 *
 * It follows the loop in Spark's generated code for `RangeExec` (`doProduce`), which walks the
 * partition in batches of [[RangeArrowReader.SparkBatchSize]] values using wrapping `long`
 * arithmetic, and keeps its place within a batch across Arrow batches. Computing each value as
 * the partition's start plus a multiple of the step would differ where that arithmetic overflows,
 * as it does for steps close to the `Long` range.
 *
 * @param partitionStart
 *   the partition's first value
 * @param numElements
 *   the number of values in the partition
 * @param onBatch
 *   called with the row count of each batch produced
 */
private[comet] class RangeArrowReader(
    allocator: BufferAllocator,
    arrowSchema: Schema,
    partitionStart: Long,
    step: Long,
    numElements: Long,
    maxRecordsPerBatch: Int,
    taskContext: TaskContext,
    onBatch: Int => Unit)
    extends ArrowReader(allocator) {
  import RangeArrowReader.SparkBatchSize

  require(maxRecordsPerBatch > 0, "Maximum records per batch must be positive")

  // The state of Spark's loop: the first value of the current batch, the end of the current
  // batch, and the number of values not yet assigned to a batch.
  private var nextIndex: Long = partitionStart
  private var batchEnd: Long = partitionStart
  private var numElementsTodo: Long = numElements
  // How many values the current batch holds, and how many of them have been produced.
  private var localEnd: Int = 0
  private var localIdx: Int = 0
  private var finished: Boolean = false

  override protected def readSchema(): Schema = arrowSchema

  override def bytesRead(): Long = 0L

  override protected def closeReadSource(): Unit = ()

  override def loadNextBatch(): Boolean = {
    prepareLoadNextBatch()
    if (finished) {
      return false
    }
    if (taskContext != null) {
      taskContext.killTaskIfInterrupted()
    }
    val vector = getVectorSchemaRoot.getVector(0).asInstanceOf[BigIntVector]
    vector.allocateNew(maxRecordsPerBatch)
    val address = vector.getDataBufferAddress
    var rows = 0
    while (rows < maxRecordsPerBatch && !finished) {
      if (localIdx >= localEnd) {
        // The current batch is done, so start the next one where it ended.
        nextIndex = batchEnd
        val nextBatchTodo =
          if (numElementsTodo > SparkBatchSize) {
            numElementsTodo -= SparkBatchSize
            SparkBatchSize
          } else {
            val todo = numElementsTodo
            numElementsTodo = 0
            todo
          }
        if (nextBatchTodo == 0) {
          finished = true
        } else {
          batchEnd += nextBatchTodo * step
          localEnd = ((batchEnd - nextIndex) / step).toInt
          localIdx = 0
        }
      } else {
        val count = math.min(localEnd - localIdx, maxRecordsPerBatch - rows)
        // Adding the step each time gives the same wrapped values as Spark's
        // `localIdx * step + nextIndex`, with less work per value.
        var value = localIdx.toLong * step + nextIndex
        var offset = address + rows.toLong * BigIntVector.TYPE_WIDTH
        val end = offset + count.toLong * BigIntVector.TYPE_WIDTH
        while (offset < end) {
          Platform.putLong(null, offset, value)
          value += step
          offset += BigIntVector.TYPE_WIDTH
        }
        localIdx += count
        rows += count
      }
    }
    if (rows == 0) {
      false
    } else {
      vector.getValidityBuffer.setOne(0L, BitVectorHelper.getValidityBufferSize(rows).toLong)
      vector.setValueCount(rows)
      getVectorSchemaRoot.setRowCount(rows)
      onBatch(rows)
      true
    }
  }
}

private[comet] object RangeArrowReader {

  /** The number of values per batch in Spark's generated code for `RangeExec`. */
  val SparkBatchSize: Long = 1000L
}
