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
import org.apache.arrow.util.AutoCloseables
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.CometArrowAllocator

/**
 * Owns an exported broadcast stream's wrapper and allocator. Cleanup belongs to the input's early
 * task listener; registering another listener here would run before native releases its reader.
 */
private[comet] class CometBroadcastArrowStream private (
    val stream: ArrowArrayStream,
    allocator: BufferAllocator)
    extends AutoCloseable {
  private var closed = false

  /**
   * Native move-import clears the release pointer. Release therefore cleans up only unclaimed
   * exports; the parent has already dropped native readers before closing the wrapper/allocator.
   */
  override def close(): Unit = synchronized {
    if (!closed) {
      closed = true
      try stream.release()
      catch {
        case failure: Throwable =>
          AutoCloseables.close(failure, stream, allocator)
          throw failure
      }
      AutoCloseables.close(stream, allocator)
    }
  }
}

private[comet] object CometBroadcastArrowStream {

  /**
   * Transfer source ownership to the export's release callback, unwinding partial acquisition on
   * failure. The physical schema must match the decoded vectors, including fallback casts.
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
