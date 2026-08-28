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

import java.io.ByteArrayInputStream
import java.nio.channels.Channels

import scala.jdk.CollectionConverters._

import org.apache.arrow.flatbuf.{MessageHeader, RecordBatch => FlatBufRecordBatch}
import org.apache.arrow.vector.TypeLayout
import org.apache.arrow.vector.ipc.ReadChannel
import org.apache.arrow.vector.ipc.message.MessageSerializer
import org.apache.arrow.vector.types.pojo.Field
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.types.StructType

/**
 * Test-only access to the internals of `CometCachedBatch`.
 *
 * A top-level `private` class in Scala is visible to its own package, so this shim needs no
 * reflection; it exists so tests outside `org.apache.spark.sql.comet.execution.arrow` can assert
 * on the cached payload's shape.
 *
 * The IPC buffer arithmetic below is deliberately re-derived here rather than reused from
 * `CachedBatchIpc`. A helper that called into the code under test would inherit any bug in it and
 * still agree with itself, so the assertions built on this would pass for the wrong reason.
 */
object CometCachedBatchHelper {

  /** The raw cached payload: one encapsulated Arrow IPC record batch message and its body. */
  private def payload(batch: CachedBatch): Array[Byte] =
    batch.asInstanceOf[CometCachedBatch].bytes

  /** Stored size of the whole cached batch, in bytes. */
  def payloadSize(batch: CachedBatch): Long = payload(batch).length.toLong

  /**
   * Whether the payload begins with a Schema message rather than going straight to the record
   * batch.
   *
   * Comet stores no schema per cached batch -- the reader rebuilds it from the cached relation's
   * attributes -- so this is false, and a regression to a self-describing stream would show up
   * here rather than only as a footprint number.
   */
  def hasSchemaMessage(batch: CachedBatch): Boolean =
    readMetadata(payload(batch))._1.headerType() == MessageHeader.Schema

  /**
   * The on-body (offset, length) of every Arrow buffer belonging to each top-level column, in
   * column order.
   */
  def columnBufferRanges(batch: CachedBatch, cacheSchema: StructType): Seq[Seq[(Long, Long)]] = {
    val data = payload(batch)
    val (_, recordBatch) = readMetadata(data)
    val fields = arrowFields(cacheSchema)
    val starts = fields.scanLeft(0)(_ + bufferCount(_)).toArray

    fields.indices.map { i =>
      (starts(i) until starts(i) + bufferCount(fields(i))).map { j =>
        val buffer = recordBatch.buffers(j)
        (buffer.offset(), buffer.length())
      }
    }
  }

  /** Stored size of each top-level column: the sum of its buffers' on-body lengths. */
  def columnSizes(batch: CachedBatch, cacheSchema: StructType): Seq[Long] =
    columnBufferRanges(batch, cacheSchema).map(_.map(_._2).sum)

  /**
   * Whether any of a column's buffers is actually stored compressed.
   *
   * Arrow prefixes each compressed buffer with its uncompressed length, and falls back to storing
   * a buffer verbatim (length prefix `-1`) when compressing it would not make it smaller. Small
   * buffers routinely take that fallback, so [[corruptColumn]] only has something to corrupt when
   * this is true; the projection tests assert it as a precondition rather than assuming it.
   */
  def columnIsCompressed(batch: CachedBatch, cacheSchema: StructType, index: Int): Boolean = {
    val data = payload(batch)
    val start = bodyStart(data)
    columnBufferRanges(batch, cacheSchema)(index).exists { case (offset, length) =>
      length > 8 && uncompressedLength(data, start + offset.toInt) > 0
    }
  }

  /**
   * Scramble one column's compressed bytes in place, leaving every other column byte-identical.
   *
   * Reading a column this has corrupted fails in the decompressor; reading any other column only
   * succeeds if this column's buffers were never copied out of the payload. That is the
   * difference between decoding what was projected and decoding everything and projecting
   * afterwards, so it is what the projection tests assert on rather than timings.
   *
   * Each buffer's 8-byte uncompressed-length prefix is left intact and only the compressed bytes
   * after it are overwritten, so a read of this column fails while decompressing rather than by
   * trying to allocate a nonsense length. Requires the column to have a genuinely compressed
   * buffer -- see [[columnIsCompressed]].
   */
  def corruptColumn(batch: CachedBatch, cacheSchema: StructType, index: Int): Unit = {
    val data = payload(batch)
    val start = bodyStart(data)
    var corrupted = false

    columnBufferRanges(batch, cacheSchema)(index).foreach { case (offset, length) =>
      val bufferStart = start + offset.toInt
      if (length > 8 && uncompressedLength(data, bufferStart) > 0) {
        var i = bufferStart + 8
        while (i < bufferStart + length.toInt) {
          // A fixed pattern rather than random bytes, so a failure reproduces exactly.
          data(i) = (0xa5 ^ i).toByte
          i += 1
        }
        corrupted = true
      }
    }

    require(
      corrupted,
      s"column $index of the cached batch has no compressed buffer to corrupt; " +
        "the test needs data that Arrow actually compresses")
  }

  /**
   * Truncate the tail of one column's compressed bytes, in place, padding with zeros so every
   * other column keeps its offset.
   *
   * [[corruptColumn]] rewrites the whole compressed payload, which fails as soon as the
   * decompressor looks at it. This keeps the leading bytes genuine, so a decoder gets a stream
   * that starts out valid and then runs out, exercising a failure part way through a column
   * rather than at its first byte.
   */
  def truncateColumn(batch: CachedBatch, cacheSchema: StructType, index: Int): Unit = {
    val data = payload(batch)
    val start = bodyStart(data)
    var truncated = false

    columnBufferRanges(batch, cacheSchema)(index).foreach { case (offset, length) =>
      val bufferStart = start + offset.toInt
      if (length > 32 && uncompressedLength(data, bufferStart) > 0 && !truncated) {
        var i = bufferStart + length.toInt - 16
        while (i < bufferStart + length.toInt) {
          data(i) = 0
          i += 1
        }
        truncated = true
      }
    }

    require(
      truncated,
      s"column $index of the cached batch has no compressed buffer long enough to truncate")
  }

  /**
   * Scramble only the last compressed buffer of one column, leaving its earlier buffers genuine.
   *
   * A string column stores offsets and data as separate compressed buffers, so this makes the
   * decoder decompress one buffer of the column successfully and then fail on the next. That is a
   * different failure point from [[corruptColumn]], which takes out a column's first buffer and
   * so fails before anything of it has been decompressed.
   */
  def corruptTrailingBuffer(batch: CachedBatch, cacheSchema: StructType, index: Int): Unit = {
    val data = payload(batch)
    val start = bodyStart(data)
    val compressed = columnBufferRanges(batch, cacheSchema)(index).filter {
      case (offset, length) =>
        length > 8 && uncompressedLength(data, start + offset.toInt) > 0
    }
    require(
      compressed.length > 1,
      s"column $index has ${compressed.length} compressed buffers; this needs at least two so a " +
        "decode can succeed on one and then fail on the next")

    val (offset, length) = compressed.last
    val bufferStart = start + offset.toInt
    var i = bufferStart + 8
    while (i < bufferStart + length.toInt) {
      data(i) = (0xa5 ^ i).toByte
      i += 1
    }
  }

  /** The Arrow fields the read path rebuilds for `cacheSchema`. */
  private def arrowFields(cacheSchema: StructType): Seq[Field] =
    Utils
      .toArrowSchema(cacheSchema, CometArrowStream.NATIVE_TIMEZONE)
      .getFields
      .asScala
      .toSeq

  /**
   * Buffers a field occupies in the record batch body, including every descendant, in the
   * depth-first order the body lays them out.
   */
  private def bufferCount(field: Field): Int =
    TypeLayout.getTypeBufferCount(field.getType) +
      field.getChildren.asScala.map(bufferCount).sum

  private def readMetadata(data: Array[Byte]) = {
    val channel = new ReadChannel(Channels.newChannel(new ByteArrayInputStream(data)))
    val metadata = MessageSerializer.readMessage(channel)
    require(metadata != null, "cached payload holds no IPC message")
    (
      metadata.getMessage,
      metadata.getMessage.header(new FlatBufRecordBatch()).asInstanceOf[FlatBufRecordBatch])
  }

  /** Offset of the record batch body within the payload; the body is its tail. */
  private def bodyStart(data: Array[Byte]): Int = {
    val channel = new ReadChannel(Channels.newChannel(new ByteArrayInputStream(data)))
    val metadata = MessageSerializer.readMessage(channel)
    require(metadata != null, "cached payload holds no IPC message")
    data.length - metadata.getMessageBodyLength.toInt
  }

  /**
   * The uncompressed-length prefix Arrow writes ahead of a compressed buffer, little-endian. A
   * value of -1 means the buffer was stored verbatim because compressing it did not pay.
   */
  private def uncompressedLength(data: Array[Byte], bufferStart: Int): Long = {
    var value = 0L
    var i = 7
    while (i >= 0) {
      value = (value << 8) | (data(bufferStart + i) & 0xffL)
      i -= 1
    }
    value
  }
}
