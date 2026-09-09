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

import java.util.{ArrayList => JArrayList}

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

import org.apache.arrow.memory.{ArrowBuf, BufferAllocator}
import org.apache.arrow.util.AutoCloseables
import org.apache.arrow.vector.{FieldVector, VectorLoader, VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Copies an Arrow-backed `ColumnarBatch` produced by a Comet operator into buffers owned by a
 * caller-supplied allocator, so the copy stays valid after the source batch is gone.
 *
 * A consumer that has to hold a batch across an advance of the producing iterator needs its own
 * bytes rather than a retained reference to the producer's. `CometExecIterator` closes the
 * previous `ColumnarBatch` before pulling the next one, "to guarantee safety at the native side
 * before we overwrite the buffer memory shared across batches in the native side" -- the native
 * side recycles that memory, so a retained reference would be read back as the following batch's
 * contents.
 *
 * The copy is a bulk `memcpy` per Arrow buffer, not a per-row loop: the batch is unloaded to its
 * buffer list, each buffer is copied, and the result is loaded into a fresh root.
 */
private[comet] object CometBatchDeepCopy {

  /**
   * Deep-copies `batch` into a new `VectorSchemaRoot` allocated from `allocator`. The caller owns
   * the returned root and must close it. `batch` is left open and unmodified.
   */
  def copy(batch: ColumnarBatch, allocator: BufferAllocator): VectorSchemaRoot = {
    // Plain vectors decoded from dictionary-encoded columns. We own these and close them once
    // their contents have been copied into the target root.
    val decoded = ListBuffer.empty[FieldVector]
    try {
      val sourceVectors = CometArrowVectors.materialize(batch, allocator, decoded)
      val fields = sourceVectors.asScala.map(_.getField).asJava

      // Borrows `batch`'s field vectors, so it must not be closed: that would release references
      // the source batch still owns. The row count is passed to the constructor rather than set
      // afterwards, because `setRowCount` would call `setValueCount` on the borrowed vectors and
      // so mutate the source batch.
      val transient = new VectorSchemaRoot(fields, sourceVectors, batch.numRows())

      val target = VectorSchemaRoot.create(new Schema(fields), allocator)
      var loaded = false
      try {
        val sourceBatch = new VectorUnloader(transient).getRecordBatch
        try {
          val copiedBatch = copyRecordBatch(sourceBatch, allocator)
          try {
            // `load` retains the copies into `target` and sets its row count, so `target` keeps
            // the bytes alive after the batch below is closed.
            new VectorLoader(target).load(copiedBatch)
          } finally {
            copiedBatch.close()
          }
        } finally {
          sourceBatch.close()
        }
        loaded = true
        target
      } finally {
        if (!loaded) target.close()
      }
    } finally {
      AutoCloseables.close(decoded.asJava)
    }
  }

  /**
   * A record batch whose buffers are `memcpy`ed copies of `source`'s, allocated from `allocator`.
   *
   * Built with `retainBuffers = false`, so the batch adopts the references created here instead
   * of adding its own: closing it releases the copies, and the caller has nothing else to clean
   * up on the success path.
   */
  private def copyRecordBatch(
      source: ArrowRecordBatch,
      allocator: BufferAllocator): ArrowRecordBatch = {
    val copies = new JArrayList[ArrowBuf](source.getBuffers.size())
    try {
      source.getBuffers.asScala.foreach { src =>
        val length = src.readableBytes()
        val dst = allocator.buffer(length)
        if (length > 0) {
          dst.setBytes(0, src, src.readerIndex(), length)
        }
        dst.writerIndex(length)
        copies.add(dst)
      }
      new ArrowRecordBatch(
        source.getLength,
        source.getNodes,
        copies,
        source.getBodyCompression,
        // Variadic counts describe the view-type data buffers, which are copied along with the
        // rest, so they carry over unchanged.
        source.getVariadicBufferCounts,
        true,
        false)
    } catch {
      case failure: Throwable =>
        AutoCloseables.close(failure, copies)
        throw failure
    }
  }
}
