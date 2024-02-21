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

import org.apache.spark._
import org.apache.spark.sql.comet.CometMetricNode
import org.apache.spark.sql.vectorized._

import org.apache.comet.CometConf.{COMET_BATCH_SIZE, COMET_DEBUG_ENABLED, COMET_EXEC_MEMORY_FRACTION}
import org.apache.comet.vector.NativeUtil

/**
 * An iterator class used to execute Comet native query. It takes an input iterator which comes
 * from Comet Scan and is expected to produce batches of Arrow Arrays. During consuming this
 * iterator, it will consume input iterator and pass Arrow Arrays to Comet native engine by
 * addresses. Even after the end of input iterator, this iterator still possibly continues
 * executing native query as there might be blocking operators such as Sort, Aggregate. The API
 * `hasNext` can be used to check if it is the end of this iterator (i.e. the native query is
 * done).
 *
 * @param inputs
 *   The input iterators producing sequence of batches of Arrow Arrays.
 * @param protobufQueryPlan
 *   The serialized bytes of Spark execution plan.
 */
class CometExecIterator(
    val id: Long,
    inputs: Seq[Iterator[ColumnarBatch]],
    protobufQueryPlan: Array[Byte],
    nativeMetrics: CometMetricNode)
    extends Iterator[ColumnarBatch] {

  private val nativeLib = new Native()
  private val nativeUtil = new NativeUtil
  private val cometBatchIterators = inputs.map { iterator =>
    new CometBatchIterator(iterator, nativeUtil)
  }.toArray
  private val plan = {
    val configs = createNativeConf
    nativeLib.createPlan(id, configs, cometBatchIterators, protobufQueryPlan, nativeMetrics)
  }

  private var nextBatch: Option[ColumnarBatch] = None
  private var currentBatch: ColumnarBatch = null
  private var closed: Boolean = false

  private def executeNative(): ExecutionState = {
    val result = nativeLib.executePlan(plan)

    val flag = result(0)
    if (flag == -1) EOF
    else if (flag == 1) {
      val numRows = result(1)
      val addresses = result.slice(2, result.length)
      Batch(numRows = numRows.toInt, addresses = addresses)
    } else {
      throw new IllegalStateException(s"Invalid native flag: $flag")
    }
  }

  /**
   * Creates a new configuration map to be passed to the native side.
   */
  private def createNativeConf: java.util.HashMap[String, String] = {
    val result = new java.util.HashMap[String, String]()
    val conf = SparkEnv.get.conf

    val maxMemory = CometSparkSessionExtensions.getCometMemoryOverhead(conf)
    result.put("memory_limit", String.valueOf(maxMemory))
    result.put("memory_fraction", String.valueOf(COMET_EXEC_MEMORY_FRACTION.get()))
    result.put("batch_size", String.valueOf(COMET_BATCH_SIZE.get()))
    result.put("debug_native", String.valueOf(COMET_DEBUG_ENABLED.get()))

    // Strip mandatory prefix spark. which is not required for DataFusion session params
    conf.getAll.foreach {
      case (k, v) if k.startsWith("spark.datafusion") =>
        result.put(k.replaceFirst("spark\\.", ""), v)
      case _ =>
    }

    result
  }

  /** Execution result from Comet native */
  trait ExecutionState

  /** A new batch is available */
  case class Batch(numRows: Int, addresses: Array[Long]) extends ExecutionState

  /** The execution is finished - no more batch */
  case object EOF extends ExecutionState

  def getNextBatch(): Option[ColumnarBatch] = {
    executeNative() match {
      case EOF => None
      case Batch(numRows, addresses) =>
        val cometVectors = nativeUtil.importVector(addresses)
        Some(new ColumnarBatch(cometVectors.toArray, numRows))
    }
  }

  override def hasNext: Boolean = {
    if (closed) return false

    if (nextBatch.isDefined) {
      return true
    }

    nextBatch = getNextBatch()

    if (nextBatch.isEmpty) {
      close()
      false
    } else {
      true
    }
  }

  override def next(): ColumnarBatch = {
    if (currentBatch != null) {
      // Eagerly release Arrow Arrays in the previous batch
      currentBatch.close()
      currentBatch = null
    }

    if (nextBatch.isEmpty && !hasNext) {
      throw new NoSuchElementException("No more element")
    }

    currentBatch = nextBatch.get
    nextBatch = None
    currentBatch
  }

  def close(): Unit = synchronized {
    if (!closed) {
      if (currentBatch != null) {
        currentBatch.close()
        currentBatch = null
      }
      nativeLib.releasePlan(plan)

      // The allocator thoughts the exported ArrowArray and ArrowSchema structs are not released,
      // so it will report:
      // Caused by: java.lang.IllegalStateException: Memory was leaked by query.
      // Memory leaked: (516) Allocator(ROOT) 0/516/808/9223372036854775807 (res/actual/peak/limit)
      // Suspect this seems a false positive leak, because there is no reported memory leak at JVM
      // when profiling. `allocator` reports a leak because it calculates the accumulated number
      // of memory allocated for ArrowArray and ArrowSchema. But these exported ones will be
      // released in native side later.
      // More to clarify it. For ArrowArray and ArrowSchema, Arrow will put a release field into the
      // memory region which is a callback function pointer (C function) that could be called to
      // release these structs in native code too. Once we wrap their memory addresses at native
      // side using FFI ArrowArray and ArrowSchema, and drop them later, the callback function will
      // be called to release the memory.
      // But at JVM, the allocator doesn't know about this fact so it still keeps the accumulated
      // number.
      // Tried to manually do `release` and `close` that can make the allocator happy, but it will
      // cause JVM runtime failure.

      // allocator.close()
      closed = true
    }
  }
}
