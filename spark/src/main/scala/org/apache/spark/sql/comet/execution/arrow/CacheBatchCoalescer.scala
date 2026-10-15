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

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.vector.NativeUtil

/**
 * The columnar input of a cache build, with its small batches combined.
 *
 * The columnar write stores each batch it is handed as one cached batch, and nothing upstream
 * bounds how small a batch can be. A shuffle read returns at least one batch per map output
 * block, so a relation cached from a shuffle over many map tasks would otherwise hold thousands
 * of batches of a few hundred rows. Each carries its own message metadata, compression framing
 * and statistics row, and every read of the cache pays for the batch count.
 *
 * A batch the writer can store as it stands, Arrow-backed and laid out the way the reader reads
 * it, is passed through when it holds at least half of `targetRows`, so full native batches are
 * still written without a copy. Every other batch is copied into batches of `targetRows` rows,
 * the size the row path cuts at, and split where it crosses that size. A small batch is worth the
 * copy, and a batch the writer cannot store as it stands has to be copied into Arrow anyway, so
 * combining it costs nothing more. The copy reads each column through the `ColumnVector`
 * interface, as `ArrowWriter.writeColumns` does for any input, so it handles dictionary-encoded
 * and nested columns, and offsets that do not start at zero, as a native producer's sliced arrays
 * can have. Arrow's `VectorAppender` assumes they start at zero.
 *
 * Row order is kept: the rows copied so far are emitted before a batch that passes through. Each
 * batch is copied before the input is advanced, because a native producer releases the batch it
 * returned last when it is asked for the next one.
 *
 * The batches built here are owned here. Each is released when the next batch is requested, and
 * whatever is still open when the task completes is released then. A batch passed through still
 * belongs to its producer.
 */
private[arrow] class CacheBatchCoalescer(
    input: Iterator[ColumnarBatch],
    attrs: Seq[Attribute],
    targetRows: Int,
    allocator: BufferAllocator)
    extends Iterator[ColumnarBatch] {

  require(targetRows > 0, s"The target batch size must be positive, but was $targetRows")

  private val arrowSchema =
    Utils.toArrowSchema(Utils.fromAttributes(attrs), CometArrowStream.NATIVE_TIMEZONE)
  private val readerFields = arrowSchema.getFields.asScala.toIndexedSeq

  // A batch at least this large that the writer can store as it stands is not copied.
  private val passThroughRows = math.max(1, targetRows / 2)

  // The batch being filled.
  private var pending: VectorSchemaRoot = null
  private var pendingWriter: ArrowWriter = null
  private var pendingRows = 0

  // Batches built here and filled, in input order, ahead of `held`.
  private val filled = mutable.Queue.empty[VectorSchemaRoot]

  // A batch that passes through, waiting for the rows copied ahead of it to be emitted.
  private var held: ColumnarBatch = null

  // The batch the next call to `next` returns, and the root behind it when it was built here.
  private var ready: ColumnarBatch = null
  private var readyRoot: VectorSchemaRoot = null

  // The root behind the batch `next` returned last, released once the consumer asks for more.
  private var emitted: VectorSchemaRoot = null

  private var closed = false

  Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => close()))

  override def hasNext: Boolean = {
    if (ready == null && !closed) {
      release(emitted)
      emitted = null
      advance()
    }
    ready != null
  }

  override def next(): ColumnarBatch = {
    if (!hasNext) {
      throw new NoSuchElementException("No more cache input batches")
    }
    val batch = ready
    emitted = readyRoot
    ready = null
    readyRoot = null
    batch
  }

  /** Releases every batch built here that is still open. Idempotent. */
  def close(): Unit = {
    if (!closed) {
      closed = true
      release(emitted)
      release(readyRoot)
      release(pending)
      filled.foreach(release)
      filled.clear()
      emitted = null
      ready = null
      readyRoot = null
      pending = null
      pendingWriter = null
      held = null
    }
  }

  private def advance(): Unit = {
    while (filled.isEmpty && held == null && input.hasNext) {
      val batch = input.next()
      if (passesThrough(batch)) {
        finishPending()
        held = batch
      } else {
        append(batch)
      }
    }
    if (filled.isEmpty && held == null) {
      // The input is exhausted.
      finishPending()
    }
    if (filled.nonEmpty) {
      readyRoot = filled.dequeue()
      ready = NativeUtil.rootAsBatch(readyRoot)
    } else if (held != null) {
      ready = held
      held = null
    }
  }

  private def passesThrough(batch: ColumnarBatch): Boolean =
    batch.numRows() >= passThroughRows &&
      Utils.isArrowBacked(batch) &&
      CachedBatchIpc.matchesReaderLayout(batch, readerFields)

  // Copies every row of `batch`, filling batches of targetRows rows.
  private def append(batch: ColumnarBatch): Unit = {
    val numRows = batch.numRows()
    var start = 0
    while (start < numRows) {
      if (pending == null) {
        startPending()
      }
      val rows = math.min(numRows - start, targetRows - pendingRows)
      pendingWriter.writeColumns(batch, start, rows)
      pendingRows += rows
      start += rows
      if (pendingRows == targetRows) {
        finishPending()
      }
    }
  }

  private def startPending(): Unit = {
    val root = VectorSchemaRoot.create(arrowSchema, allocator)
    try {
      pendingWriter = ArrowWriter.create(root, targetRows)
    } catch {
      case NonFatal(e) =>
        root.close()
        throw e
    }
    pending = root
    pendingRows = 0
  }

  private def finishPending(): Unit = {
    if (pending != null) {
      if (pendingRows > 0) {
        pendingWriter.finish()
        filled.enqueue(pending)
      } else {
        pending.close()
      }
      pending = null
      pendingWriter = null
      pendingRows = 0
    }
  }

  private def release(root: VectorSchemaRoot): Unit = {
    if (root != null) {
      root.close()
    }
  }
}
