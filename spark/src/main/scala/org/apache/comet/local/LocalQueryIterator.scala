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

package org.apache.comet.local

import org.apache.spark.{SparkEnv, TaskContext, TaskKilledException}
import org.apache.spark.sql.comet.CometExec
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.{CometConf, CometExecIterator, SparkErrorConverter}
import org.apache.comet.exceptions.CometQueryExecutionException
import org.apache.comet.vector.NativeUtil

/** Task-thread-owned Arrow batches; native close can also interrupt a concurrent result pull. */
private[local] class LocalQueryIterator(spec: LocalQuerySpec, context: TaskContext)
    extends Iterator[ColumnarBatch]
    with AutoCloseable {
  private val native = new NativeLocal
  private val util = new NativeUtil
  private var id = 0L
  private var closed = false
  private var pending: ColumnarBatch = _
  private var previous: ColumnarBatch = _

  // Shared by every native plan of the task, and released when the task completes.
  private val taskMemory = CometExecIterator.taskMemory(context, CometExec.newIterId)

  // Install cleanup before allocating the native execution.
  context.addTaskCompletionListener[Unit](_ => close())
  try {
    checkCancellation()
    // The memory pool, spill directories and session settings of any Comet plan in this task.
    val memory = CometExecIterator.getMemoryConfig(SparkEnv.get.conf)
    // Partitions may run ahead holding as much of the result as the driver may collect.
    val maxResultSize = SparkEnv.get.conf.getSizeAsBytes("spark.driver.maxResultSize", "1g")
    id = native.createQuery(
      spec.plan,
      CometExecIterator.serializeCometSQLConfs(),
      spec.partitions,
      spec.columns,
      taskMemory.manager,
      SparkEnv.get.blockManager.getLocalDiskDirs,
      CometConf.COMET_BATCH_SIZE.get(),
      memory.offHeapMode,
      memory.memoryPoolType,
      memory.memoryLimit,
      context.taskAttemptId(),
      context.cpus().toLong,
      if (maxResultSize > 0) maxResultSize else Long.MaxValue)
  } catch {
    case failure: Throwable =>
      throw executionFailure(failure)
  }

  private def checkCancellation(): Unit = {
    if (context.isInterrupted()) throw new TaskKilledException("Local query cancelled")
  }

  private def closeAfterFailure(failure: Throwable): Unit = {
    try close()
    catch { case cleanup: Throwable => failure.addSuppressed(cleanup) }
  }

  private def executionFailure(failure: Throwable): Throwable = {
    closeAfterFailure(failure)
    failure match {
      case e: CometQueryExecutionException => SparkErrorConverter.convertToSparkException(e)
      case other => other
    }
  }

  override def hasNext: Boolean = {
    if (closed) return false
    if (pending != null) return true
    try {
      if (previous != null) {
        previous.close()
        previous = null
      }
      pending = util
        .getNextBatch(
          spec.columns,
          (arrays, schemas) => {
            var rows = -2L
            while (rows == -2L) {
              checkCancellation()
              rows = native.nextBatch(id, arrays, schemas)
            }
            checkCancellation()
            rows
          })
        .orNull
      if (pending == null) close()
      pending != null
    } catch {
      case failure: Throwable =>
        throw executionFailure(failure)
    }
  }

  override def next(): ColumnarBatch = {
    if (!hasNext) throw new NoSuchElementException("Local query exhausted")
    previous = pending
    pending = null
    previous
  }

  override def close(): Unit = {
    if (!closed) {
      closed = true
      var failure: Throwable = null
      def attempt(cleanup: => Unit): Unit = {
        try cleanup
        catch {
          case caught: Throwable =>
            if (failure == null) failure = caught else failure.addSuppressed(caught)
        }
      }
      // Cancel native production before releasing imported Arrow buffers.
      if (id != 0L) attempt(native.close(id))
      id = 0L
      if (pending != null) attempt(pending.close())
      pending = null
      if (previous != null) attempt(previous.close())
      previous = null
      attempt(util.close())
      if (failure != null) throw failure
    }
  }
}
