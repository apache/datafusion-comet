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

package org.apache.spark.sql.comet

import scala.collection.mutable.ArrayBuffer

import org.apache.arrow.c.ArrowArrayStream
import org.apache.arrow.util.AutoCloseables
import org.apache.spark.{CometBroadcastMemoryManager, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.comet.CometTaskContextShim
import org.apache.spark.sql.comet.execution.arrow.{ArrowReaderIterator, CometBroadcastArrowStream}
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.types.{BooleanType, ByteType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructType, TimestampNTZType, TimestampType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.io.ChunkedByteBuffer

/**
 * Task-local broadcast identity with replayable Arrow streams opened only on cache misses or
 * fallback. Register cleanup before CometExecIterator: Spark's reverse listener order must drop
 * native readers before their JVM stream wrappers and allocators. Only native copies outlive
 * tasks.
 */
class CometBroadcastInput private[comet] (
    broadcast: Broadcast[Array[ChunkedByteBuffer]],
    schema: StructType,
    name: String,
    context: TaskContext,
    memoryManager: CometBroadcastMemoryManager)
    extends AutoCloseable {

  private val streams = ArrayBuffer.empty[CometBroadcastArrowStream]
  private var closed = false

  context.addTaskCompletionListener[Unit](_ => close())

  def getBroadcastId(): Long = broadcast.id

  def getMemoryManager(): CometBroadcastMemoryManager = memoryManager

  /**
   * Native move-import claims the C stream; retain its wrapper and allocator until task
   * completion. Each call opens an independent decoder, so failed cache admission can replay the
   * broadcast.
   */
  def openStream(): ArrowArrayStream = synchronized {
    require(!closed, "Cannot open a completed broadcast input")
    withTaskContext {
      val source = new BroadcastBatchIterator
      val stream = CometBroadcastArrowStream.open(source, schema, name)
      streams += stream
      stream.stream
    }
  }

  override def close(): Unit = synchronized {
    if (!closed) {
      closed = true
      val owned = streams.toArray
      streams.clear()
      AutoCloseables.close(owned: _*)
    }
  }

  /**
   * JNI callbacks can run outside Spark's executor thread. Decode and check cancellation with the
   * captured task, then restore the calling thread's context.
   */
  private def withTaskContext[T](body: => T): T = {
    val previous = TaskContext.get()
    CometTaskContextShim.set(context)
    try {
      context.killTaskIfInterrupted()
      body
    } finally {
      if (previous == null) CometTaskContextShim.unset()
      else CometTaskContextShim.set(previous)
    }
  }

  /**
   * Opening a stream may decode its first chunk for schema reconciliation. Early close releases
   * that IPC reader without opening the remaining chunks.
   */
  private class BroadcastBatchIterator extends Iterator[ColumnarBatch] with AutoCloseable {
    private lazy val chunks = broadcast.value.iterator
    private var batches: Iterator[ColumnarBatch] = Iterator.empty
    private var stopped = false

    override def hasNext: Boolean = withTaskContext {
      if (stopped) false
      else {
        while (!batches.hasNext && chunks.hasNext) {
          batches = Utils.decodeBatches(chunks.next(), name)
        }
        batches.hasNext
      }
    }

    override def next(): ColumnarBatch = {
      if (!hasNext) throw new NoSuchElementException("No more broadcast batches")
      batches.next()
    }

    override def close(): Unit = {
      if (!stopped) {
        stopped = true
        batches match {
          case reader: ArrowReaderIterator => reader.close()
          case _ =>
        }
        batches = Iterator.empty
      }
    }
  }
}

private[comet] object CometBroadcastInput {

  /** Admit fixed-width fields and strings; native still checks the physical UTF8 encoding. */
  def supportsSchema(schema: StructType): Boolean = schema.fields.forall { field =>
    field.dataType match {
      case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType |
          DateType | TimestampType | TimestampNTZType | StringType | _: DecimalType =>
        true
      case _ => false
    }
  }
}
