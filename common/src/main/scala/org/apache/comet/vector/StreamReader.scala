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

package org.apache.comet.vector

import java.nio.channels.ReadableByteChannel

import scala.collection.JavaConverters.collectionAsScalaIterableConverter

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ReadChannel}
import org.apache.arrow.vector.ipc.message.MessageChannelReader
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/**
 * A reader that consumes Arrow data from an input channel, and produces Comet batches.
 */
case class StreamReader(channel: ReadableByteChannel) extends AutoCloseable {
  private var allocator = new RootAllocator(Long.MaxValue)
  private val channelReader = new MessageChannelReader(new ReadChannel(channel), allocator)
  private var arrowReader = new ArrowStreamReader(channelReader, allocator)
  private var root = arrowReader.getVectorSchemaRoot

  def nextBatch(): Option[ColumnarBatch] = {
    if (arrowReader.loadNextBatch()) {
      Some(rootAsBatch(root))
    } else {
      None
    }
  }

  private def rootAsBatch(root: VectorSchemaRoot): ColumnarBatch = {
    val columns = root.getFieldVectors.asScala.map { vector =>
      // Native shuffle always uses decimal128.
      CometVector.getVector(vector, true, arrowReader).asInstanceOf[ColumnVector]
    }.toArray
    val batch = new ColumnarBatch(columns)
    batch.setNumRows(root.getRowCount)
    batch
  }

  override def close(): Unit = {
    if (root != null) {
      arrowReader.close()
      root.close()
      allocator.close()

      arrowReader = null
      root = null
      allocator = null
    }
  }
}
