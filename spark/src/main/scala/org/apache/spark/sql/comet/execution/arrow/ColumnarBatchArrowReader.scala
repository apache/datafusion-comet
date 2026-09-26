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

import scala.jdk.CollectionConverters._

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.comet.CometTaskArrowAllocator
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.vector.{CometVector, CometVectorUtils}

/**
 * `ArrowReader` over an iterator of Arrow-backed `ColumnarBatch`es. The unload/load step
 * decouples this reader's stable VSR from the source's buffers: `loadRecordBatch` takes its own
 * retained references, so each source batch can be closed right after loading while a batch
 * already exported to native stays valid via the export's independent ref. The reader allocator
 * and borrowed plain-column buffers must share a root allocator, as Arrow requires when
 * associating those existing buffers with the reader's vectors.
 */
private[comet] class ColumnarBatchArrowReader(
    allocator: BufferAllocator,
    arrowSchema: Schema,
    source: Iterator[ColumnarBatch])
    extends ArrowReader(allocator) {

  override protected def readSchema(): Schema = arrowSchema

  override def bytesRead(): Long = 0L

  override protected def closeReadSource(): Unit = ()

  // The source is consumed; the reader retains its own buffers before decoded temporaries close.
  override def loadNextBatch(): Boolean = {
    prepareLoadNextBatch()

    if (!source.hasNext) {
      return false
    }

    val src = source.next()
    try {
      val columns = (0 until src.numCols()).map(src.column(_).asInstanceOf[CometVector])
      CometVectorUtils.withDecodedVectors(columns, allocator) { sourceVectors =>
        // The stable root has the logical schema, so dictionary indices must be decoded before
        // unloading. loadRecordBatch closes the record batch after retaining its buffers.
        val transient = new VectorSchemaRoot(sourceVectors.asJava)
        transient.setRowCount(src.numRows())
        loadRecordBatch(new VectorUnloader(transient).getRecordBatch)
        // Do not close this borrowed root: src and the helper own its vectors.
      }
    } finally {
      // Closing the source leaves this reader's allocator owning the buffers it retained, which
      // Arrow does without telling the listener of the allocator they came from. A task allocator
      // is told here instead, so the task stops paying for the batch once native has it.
      val sourceAllocators = (0 until src.numCols())
        .map(src.column)
        .collect { case v: CometVector => v.getValueVector.getAllocator }
        // A NullVector owns no buffers and has no allocator.
        .filter(_ != null)
        .distinct
      src.close()
      sourceAllocators.foreach(CometTaskArrowAllocator.reconcile)
    }
    true
  }
}
