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

import org.apache.arrow.c.{ArrowArrayStream, Data}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.CometArrowAllocator
import org.apache.comet.vector.NativeUtil

/**
 * Marker for Comet operators that can produce Arrow data destined for a Comet native executor
 * directly as the C Stream Interface, skipping the intermediate `RDD[ColumnarBatch]` layer.
 */
trait CometNativeArrowSource extends SparkPlan {
  def doExecuteAsArrowStream(): RDD[ArrowArrayStream]
}

object CometArrowStream {

  /**
   * Native side asserts `Timestamp(Microsecond, Some("UTC"))` regardless of session timezone;
   * Spark's internal timestamp representation is always UTC microseconds anyway, and a non-UTC
   * timezone here would only show up as schema metadata that breaks Arrow RowConverter
   * validation. See COMET-2720.
   */
  val NATIVE_TIMEZONE: String = "UTC"

  /**
   * Wrap an `RDD[ColumnarBatch]` whose batches are Arrow-backed into an `RDD[ArrowArrayStream]`.
   */
  def wrapColumnarBatchRDD(
      rdd: RDD[ColumnarBatch],
      sparkSchema: StructType,
      timeZoneId: String,
      name: String): RDD[ArrowArrayStream] = {
    // Arrow `Schema` is not Serializable; only Spark's `StructType` is. Build the Arrow schema
    // inside the per-task body so the closure cleaner doesn't try to ship a Schema across.
    rdd.mapPartitionsInternal { batchIter =>
      val arrowSchema = Utils.toArrowSchema(sparkSchema, timeZoneId)
      stream(name, allocator => new ColumnarBatchArrowReader(allocator, arrowSchema, batchIter))
    }
  }

  /**
   * Wrap a single per-partition `Iterator[ColumnarBatch]` (Arrow-backed) and return the exported
   * `ArrowArrayStream`. For callers outside `CometExecRDD` that hand a JNI input slot directly to
   * a `CometExecIterator`.
   */
  def fromColumnarBatchIter(
      iter: Iterator[ColumnarBatch],
      sparkSchema: StructType,
      timeZoneId: String,
      name: String): ArrowArrayStream = {
    val arrowSchema = Utils.toArrowSchema(sparkSchema, timeZoneId)
    stream(name, allocator => new ColumnarBatchArrowReader(allocator, arrowSchema, iter)).next()
  }

  /**
   * Allocate a child allocator, build a reader, export it as an `ArrowArrayStream`, and register
   * task-completion cleanup. Returns a single-element iterator so this composes with
   * `RDD.mapPartitionsInternal`.
   *
   * Close ordering: when native drops its `ArrowArrayStreamReader`, the C release callback fires
   * synchronously into `ExportedArrayStreamPrivateData.close` -> `reader.close` -> the VSR's
   * buffers are released. The task-completion listener registered here runs strictly after that
   * (Spark fires listeners in reverse registration order, and the listener that drops the native
   * plan is registered later by `CometExecIterator`), so `allocator.close` finds zero outstanding
   * bytes.
   */
  def stream(
      name: String,
      readerFactory: BufferAllocator => ArrowReader): Iterator[ArrowArrayStream] = {
    val context = TaskContext.get()
    val allocator = CometArrowAllocator.newChildAllocator(name, 0, Long.MaxValue)
    var reader: ArrowReader = null
    var arrowStream: ArrowArrayStream = null
    try {
      reader = readerFactory(allocator)
      arrowStream = ArrowArrayStream.allocateNew(allocator)
      Data.exportArrayStream(allocator, reader, arrowStream)
    } catch {
      case t: Throwable =>
        // Roll back partial setup before rethrowing -- nothing has been registered with
        // TaskContext yet, so without this the allocator (and possibly the reader/stream) leaks.
        if (arrowStream != null) {
          try arrowStream.close()
          catch { case _: Throwable => () }
        }
        if (reader != null) {
          try reader.close()
          catch { case _: Throwable => () }
        }
        try allocator.close()
        catch { case _: Throwable => () }
        throw t
    }
    if (context != null) {
      val streamRef = arrowStream
      context.addTaskCompletionListener[Unit] { _ =>
        streamRef.close()
        allocator.close()
      }
    }
    Iterator.single(arrowStream)
  }

  /**
   * Drive an `ArrowReader` from a per-task body and emit `ColumnarBatch`es wrapping the reader's
   * stable VSR. Lifecycle: the supplied factory builds the reader against a fresh child
   * allocator; both close at task completion. This is the non-native consumer path
   * (`doExecuteColumnar`) -- the native consumer path uses [[stream]] to export instead.
   */
  def readerBatchIter(
      name: String,
      readerFactory: BufferAllocator => ArrowReader): Iterator[ColumnarBatch] = {
    val context = TaskContext.get()
    val allocator = CometArrowAllocator.newChildAllocator(name, 0, Long.MaxValue)
    val reader =
      try readerFactory(allocator)
      catch {
        case t: Throwable =>
          try allocator.close()
          catch { case _: Throwable => () }
          throw t
      }
    if (context != null) {
      context.addTaskCompletionListener[Unit] { _ =>
        reader.close()
        allocator.close()
      }
    }
    new Iterator[ColumnarBatch] {
      // Lazily prefetch one batch so `hasNext` can answer without consuming.
      private var loaded: Boolean = false
      private var hasMore: Boolean = false

      private def ensureLoaded(): Unit = {
        if (!loaded) {
          hasMore = reader.loadNextBatch()
          loaded = true
        }
      }

      override def hasNext: Boolean = {
        ensureLoaded()
        hasMore
      }

      override def next(): ColumnarBatch = {
        ensureLoaded()
        if (!hasMore) {
          throw new NoSuchElementException("No more batches")
        }
        loaded = false
        NativeUtil.rootAsBatch(reader.getVectorSchemaRoot)
      }
    }
  }
}
