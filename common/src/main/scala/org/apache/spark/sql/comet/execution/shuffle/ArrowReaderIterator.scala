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

package org.apache.spark.sql.comet.execution.shuffle

import java.nio.channels.ReadableByteChannel

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.CometConf
import org.apache.comet.vector.{NativeUtil, StreamReader}

class ArrowReaderIterator(channel: ReadableByteChannel) extends Iterator[ColumnarBatch] {

  private val nativeUtil = new NativeUtil

  private val maxBatchSize = CometConf.COMET_BATCH_SIZE.get(SQLConf.get)

  private val reader = StreamReader(channel)
  private var currentIdx = -1
  private var batch = nextBatch()
  private var previousBatch: ColumnarBatch = null
  private var currentBatch: ColumnarBatch = null

  override def hasNext: Boolean = {
    if (batch.isDefined) {
      return true
    }

    batch = nextBatch()
    if (batch.isEmpty) {
      return false
    }
    true
  }

  override def next(): ColumnarBatch = {
    if (!hasNext) {
      throw new NoSuchElementException
    }

    val nextBatch = batch.get
    val batchRows = nextBatch.numRows()
    val numRows = Math.min(batchRows - currentIdx, maxBatchSize)

    // Release the previous sliced batch.
    // If it is not released, when closing the reader, arrow library will complain about
    // memory leak.
    if (currentBatch != null) {
      // Close plain arrays in the previous sliced batch.
      // The dictionary arrays will be closed when closing the entire batch.
      currentBatch.close()
    }

    currentBatch = nativeUtil.takeRows(nextBatch, currentIdx, numRows)
    currentIdx += numRows

    if (currentIdx == batchRows) {
      // We cannot close the batch here, because if there is dictionary array in the batch,
      // the dictionary array will be closed immediately, and the returned sliced batch will
      // be invalid.
      previousBatch = batch.get

      batch = None
      currentIdx = -1
    }

    currentBatch
  }

  private def nextBatch(): Option[ColumnarBatch] = {
    if (previousBatch != null) {
      previousBatch.close()
      previousBatch = null
    }
    currentIdx = 0
    reader.nextBatch()
  }

  def close(): Unit =
    synchronized {
      if (currentBatch != null) {
        currentBatch.close()
      }
      reader.close()
    }
}
