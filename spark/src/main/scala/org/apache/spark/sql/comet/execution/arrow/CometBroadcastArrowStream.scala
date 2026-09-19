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

/*
 * Owns one Arrow C stream export for a task-local broadcast input. Each export gets an independent
 * reader and child allocator, with cleanup owned by the parent input's task listener. Native
 * move-import transfers the C release callback out of the Java wrapper; closing the wrapper
 * also releases any export native code never claimed.
 */

import org.apache.arrow.c.{ArrowArrayStream, Data}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.util.AutoCloseables
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.CometArrowAllocator

/**
 * Explicit owner for a lazily exported broadcast stream's wrapper and allocator. Its parent
 * marker installs the task listener before native execution, so opening this owner must not
 * install a later listener that could close the export before the native reader drops it.
 */
private[comet] class CometBroadcastArrowStream private (
    val stream: ArrowArrayStream,
    allocator: BufferAllocator)
    extends AutoCloseable {
  private var closed = false

  /**
   * Release an export if native never claimed it, then free the C wrapper and allocator. Native
   * move-import clears the release pointer, making release a no-op for already claimed streams.
   * The parent calls this after native drops its reader and batch references; repeated calls do
   * nothing.
   */
  override def close(): Unit = synchronized {
    if (!closed) {
      closed = true
      var failure: Throwable = null
      try stream.release()
      catch { case error: Throwable => failure = error }
      if (failure == null) AutoCloseables.close(stream, allocator)
      else AutoCloseables.close(failure, stream, allocator)
      if (failure != null) throw failure
    }
  }
}

private[comet] object CometBroadcastArrowStream {

  /**
   * Reconcile the source's actual Arrow schema and export an owned stream without task listeners.
   * On any failure close the source, reader, wrapper and allocator before rethrowing the original
   * error. Source ownership transfers to the exported reader's release callback on success.
   */
  def open(
      source: Iterator[ColumnarBatch] with AutoCloseable,
      schema: StructType,
      name: String): CometBroadcastArrowStream = {
    val allocator = CometArrowAllocator.newChildAllocator(name, 0, Long.MaxValue)
    var reader: ArrowReader = null
    var stream: ArrowArrayStream = null
    try {
      val expected = Utils.toArrowSchema(schema, CometArrowStream.NATIVE_TIMEZONE)
      val (actual, reconciled) = CometArrowStream.reconcileStreamSchema(name, expected, source)
      reader = new ColumnarBatchArrowReader(allocator, actual, reconciled) {
        private var readerClosed = false

        /** Make rollback and an export release safe when both encounter the same reader. */
        override def close(): Unit = synchronized {
          if (!readerClosed) {
            readerClosed = true
            super.close()
          }
        }

        /** Release a partial decoder without draining it when native abandons the stream. */
        override protected def closeReadSource(): Unit = source.close()
      }
      stream = ArrowArrayStream.allocateNew(allocator)
      Data.exportArrayStream(allocator, reader, stream)
      new CometBroadcastArrowStream(stream, allocator)
    } catch {
      case failure: Throwable =>
        if (stream != null) {
          try stream.release()
          catch { case cleanup: Throwable => failure.addSuppressed(cleanup) }
        }
        AutoCloseables.close(failure, stream, reader, source, allocator)
        throw failure
    }
  }
}
