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

/*
 * Passes a Spark broadcast to native execution as a task-local handle, without decoding it on
 * a cache hit. On a miss or ordinary-join fallback, native code can request fresh Arrow streams
 * over the same broadcast. Each stream has its own decoder; task completion closes the exported
 * wrappers and allocators after native execution drops their readers.
 */

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
 * Task-local JNI input that identifies a real Spark broadcast without opening its payload. Native
 * execution may open fresh streams on a cache miss or admission fallback. Each stream has
 * independent decoder state; only Rust-owned copies may outlive this task. Register cleanup here,
 * before CometExecIterator, so reverse task-listener order drops native readers first.
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

  /** Return the actual materialization ID without evaluating Broadcast.value or decoding rows. */
  def getBroadcastId(): Long = broadcast.id

  /** Return the executor-lifetime owner, or null when the caller must use the uncached path. */
  def getMemoryManager(): CometBroadcastMemoryManager = memoryManager

  /**
   * Export a new stream over this broadcast, preserving the captured task context on JNI threads.
   * The caller moves the C stream into its native reader; this object retains its wrapper and
   * allocator until task completion. Failure closes all resources from this attempt and leaves
   * earlier streams intact. Calls after close or task cancellation fail before reading data.
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

  /**
   * Release unclaimed exports and all wrapper/allocator storage after native readers are dropped.
   * Idempotent; attempts every stream even if one cleanup fails. It never reads unopened data.
   */
  override def close(): Unit = synchronized {
    if (!closed) {
      closed = true
      val owned = streams.toArray
      streams.clear()
      AutoCloseables.close(owned: _*)
    }
  }

  /**
   * Native callbacks may arrive on threads without Spark's task-local context. Install the
   * captured context for decoder work and cancellation checks, then restore the caller's context.
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
   * A fresh decoder cursor for one export. Marker construction and cache hits never read
   * Broadcast.value; stream opening reconciles the schema and may decode the first chunk. An
   * early drop closes the current IPC reader without draining remaining chunks.
   */
  private class BroadcastBatchIterator extends Iterator[ColumnarBatch] with AutoCloseable {
    private lazy val chunks = broadcast.value.iterator
    private var batches: Iterator[ColumnarBatch] = Iterator.empty
    private var stopped = false

    /** Advance across empty/exhausted IPC chunks; no new chunk is opened after close. */
    override def hasNext: Boolean = withTaskContext {
      if (stopped) false
      else {
        while (!batches.hasNext && chunks.hasNext) {
          batches = Utils.decodeBatches(chunks.next(), name)
        }
        batches.hasNext
      }
    }

    /** Return the next decoded batch, whose ownership passes to the Arrow stream reader. */
    override def next(): ColumnarBatch = {
      if (!hasNext) throw new NoSuchElementException("No more broadcast batches")
      batches.next()
    }

    /** Close the current IPC reader if present; empty chunks have no reader to release. */
    private def closeCurrent(): Unit = {
      batches match {
        case reader: ArrowReaderIterator => reader.close()
        case _ =>
      }
      batches = Iterator.empty
    }

    /** Idempotently close the current reader without opening or draining remaining chunks. */
    override def close(): Unit = {
      if (!stopped) {
        stopped = true
        closeCurrent()
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
