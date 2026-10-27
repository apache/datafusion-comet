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

import java.io.ByteArrayOutputStream
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.channels.Channels

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{IntVector, VectorSchemaRoot}
import org.apache.arrow.vector.dictionary.DictionaryProvider
import org.apache.arrow.vector.ipc.ArrowStreamWriter

/** Local and remote readers consume the same framed Arrow IPC format. */
private[shuffle] object NativeShuffleTestFrames {

  def frameBytes(payload: Byte*): Array[Byte] = ByteBuffer
    .allocate(20 + payload.size)
    .order(ByteOrder.LITTLE_ENDIAN)
    .putLong(12L + payload.size)
    .putLong(0L)
    .put(Array[Byte](78, 79, 78, 69)) // NONE codec
    .put(payload.toArray)
    .array()

  def arrowFrame(root: VectorSchemaRoot, dictionaries: DictionaryProvider = null): Array[Byte] = {
    val output = new ByteArrayOutputStream()
    val writer = new ArrowStreamWriter(root, dictionaries, Channels.newChannel(output))
    val payload =
      try {
        writer.start()
        writer.writeBatch()
        writer.end()
        output.toByteArray
      } finally writer.close()
    val frame = frameBytes(payload: _*)
    ByteBuffer
      .wrap(frame)
      .order(ByteOrder.LITTLE_ENDIAN)
      .putLong(8, root.getFieldVectors.size().toLong)
    frame
  }

  def intFrame(input: Seq[Int] = Seq(-1, 0, 1)): Array[Byte] = {
    val allocator = new RootAllocator(Long.MaxValue)
    val values = new IntVector("value", allocator)
    val root = VectorSchemaRoot.of(values)
    try {
      values.allocateNew()
      input.zipWithIndex.foreach { case (value, index) => values.setSafe(index, value) }
      values.setValueCount(input.size)
      root.setRowCount(input.size)
      arrowFrame(root)
    } finally {
      root.close()
      allocator.close()
    }
  }
}
