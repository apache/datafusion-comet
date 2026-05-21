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

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ReadChannel}
import org.apache.arrow.vector.ipc.message.MessageChannelReader
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.CometArrowAllocator

/**
 * A reader that consumes Arrow data from an input channel, and produces Comet batches.
 */
case class StreamReader(channel: ReadableByteChannel, source: String) extends AutoCloseable {
  private val channelReader =
    new MessageChannelReader(new ReadChannel(channel), CometArrowAllocator)
  private var arrowReader = new ArrowStreamReader(channelReader, CometArrowAllocator)
  private var root = arrowReader.getVectorSchemaRoot

  def nextBatch(): Option[ColumnarBatch] = {
    if (arrowReader.loadNextBatch()) {
      Some(rootAsBatch(root))
    } else {
      None
    }
  }

  private def rootAsBatch(root: VectorSchemaRoot): ColumnarBatch = {
    NativeUtil.rootAsBatch(root, arrowReader)
  }

  override def close(): Unit = {
    if (root != null) {
      arrowReader.close()
      root.close()

      arrowReader = null
      root = null
    }
  }
}
