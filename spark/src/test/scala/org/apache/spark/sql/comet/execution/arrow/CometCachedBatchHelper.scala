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

import java.io.{DataInputStream, DataOutputStream}
import java.nio.ByteBuffer
import java.nio.channels.Channels

import org.apache.arrow.vector.ipc.ArrowStreamReader
import org.apache.spark.SparkEnv
import org.apache.spark.io.CompressionCodec
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.util.io.{ChunkedByteBuffer, ChunkedByteBufferOutputStream}

import org.apache.comet.CometArrowAllocator

/**
 * Test-only access to the internals of `CometCachedBatch`.
 *
 * A top-level `private` class in Scala is visible to its own package, so this shim needs no
 * reflection; it exists so tests outside `org.apache.spark.sql.comet.execution.arrow` can assert
 * on the cached payload's shape.
 */
object CometCachedBatchHelper {

  /** Number of independently decodable column streams in a cached batch. */
  def numColumnStreams(batch: CachedBatch): Int =
    batch.asInstanceOf[CometCachedBatch].columns.length

  /** Serialized size of each column stream, in column order. */
  def columnStreamSizes(batch: CachedBatch): Seq[Long] =
    batch.asInstanceOf[CometCachedBatch].columns.map(_.size).toSeq

  /**
   * Replace one column's stream with bytes that cannot be decoded, in place.
   *
   * Reading a column this has corrupted fails; reading any other column only succeeds if that
   * column's stream was never touched. That is the difference between decoding what was projected
   * and decoding everything and projecting afterwards, so it is what the projection tests assert
   * on rather than timings.
   */
  def corruptColumnStream(batch: CachedBatch, index: Int): Unit = {
    val columns = batch.asInstanceOf[CometCachedBatch].columns
    columns(index) = new ChunkedByteBuffer(Array(ByteBuffer.wrap(Array[Byte](1, 2, 3, 4))))
  }

  /** Whether each column's stream stores that column dictionary encoded, in column order. */
  def columnsAreDictionaryEncoded(batch: CachedBatch): Seq[Boolean] =
    batch.asInstanceOf[CometCachedBatch].columns.toSeq.map { buffer =>
      val in = new DataInputStream(codec.compressedInputStream(buffer.toInputStream()))
      val reader = new ArrowStreamReader(Channels.newChannel(in), CometArrowAllocator)
      try {
        reader.getVectorSchemaRoot.getSchema.getFields.get(0).getDictionary != null
      } finally {
        reader.close()
      }
    }

  /**
   * Drop the last `dropBytes` of one column's decoded Arrow stream, in place.
   *
   * [[corruptColumnStream]] replaces the stream outright, so a reader over it fails on the very
   * first message, before it has allocated anything. This keeps the stream genuine up to the cut:
   * the reader parses the schema and loads the column's dictionary, and only then runs out of
   * input part way through the record batch that indexes into it. The cut is made on the decoded
   * bytes rather than the compressed ones because a small column compresses to a single block,
   * and truncating that fails the decompressor before Arrow reads anything at all.
   */
  def truncateColumnStream(batch: CachedBatch, index: Int, dropBytes: Int): Unit = {
    val columns = batch.asInstanceOf[CometCachedBatch].columns

    val decodedStream = new DataInputStream(
      codec.compressedInputStream(columns(index).toInputStream()))
    val decoded =
      try {
        val buffer = new java.io.ByteArrayOutputStream()
        val chunk = new Array[Byte](8192)
        var read = decodedStream.read(chunk)
        while (read >= 0) {
          buffer.write(chunk, 0, read)
          read = decodedStream.read(chunk)
        }
        buffer.toByteArray
      } finally {
        decodedStream.close()
      }
    require(
      decoded.length > dropBytes,
      s"column $index decodes to ${decoded.length} bytes, too few to drop $dropBytes")

    val cbbos = new ChunkedByteBufferOutputStream(1024 * 1024, ByteBuffer.allocate)
    val out = new DataOutputStream(codec.compressedOutputStream(cbbos))
    try {
      out.write(decoded, 0, decoded.length - dropBytes)
    } finally {
      out.close()
    }
    columns(index) = cbbos.toChunkedByteBuffer
  }

  private def codec: CompressionCodec = CompressionCodec.createCodec(SparkEnv.get.conf)
}
