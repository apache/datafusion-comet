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
import org.apache.arrow.vector.{FieldVector, VectorLoader, VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.dictionary.DictionaryEncoder
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.vector.{CometDictionaryVector, CometVector}

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
 * buffer list, each buffer is copied, and the result is loaded into a fresh root. Dictionary
 * columns are decoded first so the copy has the column's logical type.
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
      val sourceVectors = new JArrayList[FieldVector](batch.numCols())
      var i = 0
      while (i < batch.numCols()) {
        sourceVectors.add(
          materialize(batch.column(i).asInstanceOf[CometVector], allocator, decoded))
        i += 1
      }

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
          // `VectorLoader.load` also sets the target's row count from the batch.
          copyRecordBatch(sourceBatch, allocator) { copiedBatch =>
            new VectorLoader(target).load(copiedBatch)
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
      decoded.foreach(v =>
        try v.close()
        catch { case _: Throwable => () })
    }
  }

  /**
   * The Arrow vector holding `column`'s logical values. Dictionary-encoded columns are decoded,
   * because their `getValueVector` exposes the index vector, whose buffer layout does not match
   * the field the column advertises.
   */
  private def materialize(
      column: CometVector,
      allocator: BufferAllocator,
      decoded: ListBuffer[FieldVector]): FieldVector = column match {
    case d: CometDictionaryVector =>
      val indices = d.getValueVector
      val dictionary = d.provider.lookup(indices.getField.getDictionary.getId)
      val plain =
        DictionaryEncoder.decode(indices, dictionary, allocator).asInstanceOf[FieldVector]
      decoded += plain
      plain
    case other => other.getValueVector.asInstanceOf[FieldVector]
  }

  /**
   * Runs `use` on a record batch whose buffers are `memcpy`ed copies of `source`'s, allocated
   * from `allocator`, then releases those copies.
   *
   * The batch is built with `retainBuffers = true`, so it holds its own reference to each copy
   * and the references created here are dropped immediately. Whatever `use` retains (a
   * `VectorLoader.load`, say) survives the batch being closed.
   */
  private def copyRecordBatch(source: ArrowRecordBatch, allocator: BufferAllocator)(
      use: ArrowRecordBatch => Unit): Unit = {
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
      val copiedBatch = new ArrowRecordBatch(
        source.getLength,
        source.getNodes,
        copies,
        source.getBodyCompression,
        // Variadic counts describe the view-type data buffers, which are copied along with the
        // rest, so they carry over unchanged.
        source.getVariadicBufferCounts,
        true)
      try {
        use(copiedBatch)
      } finally {
        copiedBatch.close()
      }
    } finally {
      copies.asScala.foreach(buf =>
        try buf.close()
        catch { case _: Throwable => () })
    }
  }
}
