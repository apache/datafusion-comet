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

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{FieldVector, VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.dictionary.DictionaryEncoder
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.vector.{CometDictionaryVector, CometVector}

/**
 * `ArrowReader` over an iterator of Arrow-backed `ColumnarBatch`es. Each `loadNextBatch` unloads
 * the source's `FieldVector`s into a transient `ArrowRecordBatch` (retains buffers), loads it
 * into this reader's stable VSR via `loadFieldBuffers` (release-and-replace), then closes the
 * source batch. The unload/load step decouples this reader's VSR ownership from whatever the
 * source does with its own buffers.
 */
private[comet] class ColumnarBatchArrowReader(
    allocator: BufferAllocator,
    arrowSchema: Schema,
    source: Iterator[ColumnarBatch])
    extends ArrowReader(allocator) {

  override protected def readSchema(): Schema = arrowSchema

  override def bytesRead(): Long = 0L

  override protected def closeReadSource(): Unit = ()

  override def loadNextBatch(): Boolean = {
    prepareLoadNextBatch()

    if (!source.hasNext) {
      return false
    }

    val src = source.next()
    var materialized: JArrayList[FieldVector] = null
    try {
      val sourceVectors = new JArrayList[FieldVector](src.numCols())
      var i = 0
      while (i < src.numCols()) {
        val col = src.column(i).asInstanceOf[CometVector]
        val fv = col match {
          case d: CometDictionaryVector =>
            // Stable VSR was built from the logical (non-dict) schema, so a dict-encoded
            // source's indices layout would mismatch the dest buffer count on load. Native
            // unpacks downstream anyway via copy_or_unpack_array.
            val indices = d.getValueVector
            val dictionary = d.provider.lookup(indices.getField.getDictionary.getId)
            val plain = DictionaryEncoder
              .decode(indices, dictionary, allocator)
              .asInstanceOf[FieldVector]
            if (materialized == null) materialized = new JArrayList[FieldVector]()
            materialized.add(plain)
            plain
          case _ =>
            col.getValueVector.asInstanceOf[FieldVector]
        }
        sourceVectors.add(fv)
        i += 1
      }
      val transient = new VectorSchemaRoot(sourceVectors)
      transient.setRowCount(src.numRows())

      val unloader = new VectorUnloader(transient)
      val rb = unloader.getRecordBatch
      try {
        loadRecordBatch(rb)
      } finally {
        rb.close()
      }
      // Do not close `transient`. It shares FieldVectors with `src`; closing `src` below
      // releases the producer-side refs. Closing `transient` would double-release.
    } finally {
      if (materialized != null) {
        var j = 0
        while (j < materialized.size()) {
          try materialized.get(j).close()
          catch { case _: Throwable => () }
          j += 1
        }
      }
      src.close()
    }
    true
  }
}
