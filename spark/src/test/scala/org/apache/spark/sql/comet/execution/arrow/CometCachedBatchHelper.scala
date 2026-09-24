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
import java.nio.ByteBuffer
import java.nio.channels.Channels

import scala.jdk.CollectionConverters._

import org.apache.arrow.flatbuf.{BodyCompressionMethod, MessageHeader, RecordBatch => FlatBufRecordBatch}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{TypeLayout, VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.compression.{CompressionCodec, NoCompressionCodec}
import org.apache.arrow.vector.ipc.{ReadChannel, WriteChannel}
import org.apache.arrow.vector.ipc.message.{ArrowBodyCompression, ArrowRecordBatch, MessageMetadataResult, MessageSerializer}
import org.apache.arrow.vector.types.pojo.Field
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.io.{ChunkedByteBuffer, ChunkedByteBufferOutputStream}

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
  private def payload(batch: CachedBatch): ChunkedByteBuffer =
    batch.asInstanceOf[CometCachedBatch].bytes

  /**
   * The payload flattened into one array, for reading. Test payloads are far below the 2 GiB a
   * single array can hold, which is the limit the chunked format exists to avoid in production.
   */
  private def payloadBytes(batch: CachedBatch): Array[Byte] = payload(batch).toArray

  /** How many heap chunks the payload is stored in. */
  def chunkCount(batch: CachedBatch): Int = payload(batch).getChunks().length

  /** The payload's total size in bytes, summed over its chunks. */
  def payloadSize(batch: CachedBatch): Long =
    payload(batch).getChunks().map(_.remaining.toLong).sum

  /**
   * Overwrite `bytes.length` bytes of the payload in place starting at `start`, walking the
   * chunks so a range that crosses a chunk boundary is written into both.
   */
  private def writeThrough(batch: CachedBatch, start: Long, bytes: Array[Byte]): Unit = {
    var chunkStart = 0L
    payload(batch).getChunks().foreach { chunk =>
      val size = chunk.remaining
      var i = math.max(start, chunkStart)
      val end = math.min(start + bytes.length, chunkStart + size)
      while (i < end) {
        chunk.put(chunk.position() + (i - chunkStart).toInt, bytes((i - start).toInt))
        i += 1
      }
      chunkStart += size
    }
  }

  /**
   * A copy of every batch's payload, so a test can undo what [[corruptColumn]] scrambled.
   *
   * Materializing the cache is the expensive part of a corruption test, and a test that gives
   * each column of a relation a turn needs the payload back as it was between turns. Restoring
   * beats re-caching by the column count.
   */
  def snapshotPayloads(batches: Array[CachedBatch]): Array[Array[Byte]] =
    batches.map(payloadBytes)

  /** Put back what [[snapshotPayloads]] captured. Corruption never changes a payload's length. */
  def restorePayloads(batches: Array[CachedBatch], snapshot: Array[Array[Byte]]): Unit =
    batches.zip(snapshot).foreach { case (batch, bytes) => writeThrough(batch, 0, bytes) }

  /**
   * Whether the write path would unload `batch`'s vectors as they stand, or convert them first.
   *
   * A thin shim rather than a re-derivation: this is the decision under test, not arithmetic to
   * check it against.
   */
  def writesDirectly(batch: ColumnarBatch, cacheSchema: StructType): Boolean =
    Utils
      .isArrowBacked(batch) && CachedBatchIpc.matchesReaderLayout(batch, arrowFields(cacheSchema))

  /**
   * Serialize one batch the way the cache writer does, with a caller-chosen codec.
   *
   * Another thin shim, for tests about the write path itself rather than about what a cached
   * relation reads back. The codec is the parameter that matters: handing in one that throws is
   * how a compression failure part way through a batch is reached deterministically.
   */
  def serialize(
      batch: ColumnarBatch,
      codec: CompressionCodec,
      allocator: BufferAllocator,
      chunkSize: Int = 1024 * 1024): ChunkedByteBuffer =
    CachedBatchIpc.serialize(batch, codec, allocator, chunkSize)._1

  /** A payload [[serialize]] wrote, as the cached batch the writer would have stored it in. */
  def cachedBatch(payload: ChunkedByteBuffer, numRows: Int): CachedBatch =
    CometCachedBatch(numRows, payload.size, InternalRow.empty, payload)

  /**
   * Decode the `selected` columns of a cached batch the way a scan does, into a root the caller
   * owns. Another thin shim, for tests about what the read path leaves allocated.
   */
  def load(
      batch: CachedBatch,
      cacheSchema: StructType,
      selected: Array[Int],
      allocator: BufferAllocator): VectorSchemaRoot =
    new CachedBatchIpc.Projection(arrowFields(cacheSchema).toIndexedSeq, selected)
      .load(payload(batch), allocator)

  /**
   * A cached batch whose payload records `codec` as the byte that compressed its body.
   *
   * The writer cannot produce one: it takes that byte from the codec it was handed, and
   * `CompressionUtil.CodecType` has three values. So the record batch is assembled here instead,
   * with the buffers written plain -- what they hold does not matter, since the read path has to
   * reject an unrecognized byte before it decompresses anything. Everything else about the
   * payload is what the writer writes, so a read gets that far.
   *
   * As with [[serialize]], `batch`'s vectors are left cleared.
   */
  def cachedBatchWithBodyCompression(batch: ColumnarBatch, codec: Byte): CachedBatch = {
    val (vectors, _) = Utils.getBatchFieldVectors(batch)
    val root = new VectorSchemaRoot(vectors.asJava)
    val plain = new VectorUnloader(root, true, NoCompressionCodec.INSTANCE, true).getRecordBatch
    try {
      // Retains each buffer, so `plain` and this both own one and both are closed below.
      val tagged = new ArrowRecordBatch(
        plain.getLength,
        plain.getNodes,
        plain.getBuffers,
        new ArrowBodyCompression(codec, BodyCompressionMethod.BUFFER),
        true)
      try {
        root.clear()
        val out = new ChunkedByteBufferOutputStream(1024 * 1024, ByteBuffer.allocate)
        try {
          MessageSerializer.serialize(new WriteChannel(Channels.newChannel(out)), tagged)
        } finally {
          out.close()
        }
        val payload = out.toChunkedByteBuffer
        cachedBatch(payload, batch.numRows())
      } finally {
        tagged.close()
      }
    } finally {
      plain.close()
    }
  }

  /**
   * Whether the payload begins with a Schema message rather than going straight to the record
   * batch.
   *
   * Comet stores no schema per cached batch -- the reader rebuilds it from the cached relation's
   * attributes -- so this is false, and a regression to a self-describing stream would show up
   * here rather than only as a footprint number.
   */
  def hasSchemaMessage(batch: CachedBatch): Boolean =
    readMessage(payloadBytes(batch)).getMessage.headerType() == MessageHeader.Schema

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
  def columnIsCompressed(batch: CachedBatch, cacheSchema: StructType, index: Int): Boolean =
    compressedRanges(batch, cacheSchema, index).nonEmpty

  /**
   * Whether any of a column's buffers took that fallback: stored verbatim behind a `-1` length
   * prefix because compressing it would not have made it smaller.
   */
  def columnHasRawBuffer(batch: CachedBatch, cacheSchema: StructType, index: Int): Boolean = {
    val data = payloadBytes(batch)
    val bodyStart = data.length - readMessage(data).getMessageBodyLength
    columnBufferRanges(batch, cacheSchema)(index).exists { case (offset, length) =>
      length > 8 && uncompressedLength(data, bodyStart + offset) == -1L
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
   * Requires the column to have a genuinely compressed buffer -- see [[columnIsCompressed]].
   */
  def corruptColumn(batch: CachedBatch, cacheSchema: StructType, index: Int): Unit = {
    val ranges = compressedRanges(batch, cacheSchema, index)
    require(
      ranges.nonEmpty,
      s"column $index of the cached batch has no compressed buffer to corrupt; " +
        "the test needs data that Arrow actually compresses")
    ranges.foreach { case (start, length) => scramble(batch, start, length) }
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
    val ranges = compressedRanges(batch, cacheSchema, index)
    require(
      ranges.length > 1,
      s"column $index has ${ranges.length} compressed buffers; this needs at least two so a " +
        "decode can succeed on one and then fail on the next")
    val (start, length) = ranges.last
    scramble(batch, start, length)
  }

  /**
   * The absolute (start, length) of each of a column's buffers that Arrow actually compressed.
   *
   * `start` is an index into the payload, not an offset within the body, so callers can write
   * through it directly. A buffer shorter than its 8-byte uncompressed-length prefix, or one
   * whose prefix reads `-1`, was stored verbatim and is excluded: overwriting it would change the
   * values a read returns rather than making the read fail.
   */
  private def compressedRanges(
      batch: CachedBatch,
      cacheSchema: StructType,
      index: Int): Seq[(Long, Long)] = {
    val data = payloadBytes(batch)
    val bodyStart = data.length - readMessage(data).getMessageBodyLength
    columnBufferRanges(batch, cacheSchema)(index).collect {
      case (offset, length) if length > 8 && uncompressedLength(data, bodyStart + offset) > 0 =>
        (bodyStart + offset, length)
    }
  }

  /**
   * Overwrite a compressed buffer's payload, leaving its uncompressed-length prefix intact.
   *
   * The whole payload is rewritten, frame header included, so every zstd release rejects it
   * outright. Corrupting only a frame's tail is not enough: whether that is detected depends on
   * the zstd-jni each Spark version ships -- Comet takes it from Spark rather than from
   * arrow-compression, and 1.5.5 (Spark 3.4, 3.5) decodes a frame whose last bytes have been
   * zeroed that 1.5.7 (Spark 4.x) reports as corrupt.
   */
  private def scramble(batch: CachedBatch, start: Long, length: Long): Unit = {
    val from = start + 8
    // A fixed pattern rather than random bytes, so a failure reproduces exactly.
    val pattern = Array.tabulate((length - 8).toInt)(k => (0xa5 ^ (from + k).toInt).toByte)
    writeThrough(batch, from, pattern)
  }

  /**
   * The on-body (offset, length) of every Arrow buffer belonging to each top-level column, in
   * column order.
   */
  private def columnBufferRanges(
      batch: CachedBatch,
      cacheSchema: StructType): Seq[Seq[(Long, Long)]] = {
    val data = payloadBytes(batch)
    val recordBatch =
      readMessage(data).getMessage
        .header(new FlatBufRecordBatch())
        .asInstanceOf[FlatBufRecordBatch]
    val fields = arrowFields(cacheSchema)
    val starts = fields.scanLeft(0)(_ + bufferCount(_)).toArray

    fields.indices.map { i =>
      (starts(i) until starts(i + 1)).map { j =>
        val buffer = recordBatch.buffers(j)
        (buffer.offset(), buffer.length())
      }
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

  /** The payload's leading IPC message, carrying both its header and its body length. */
  private def readMessage(data: Array[Byte]): MessageMetadataResult = {
    val channel = new ReadChannel(Channels.newChannel(new ByteArrayInputStream(data)))
    val metadata = MessageSerializer.readMessage(channel)
    require(metadata != null, "cached payload holds no IPC message")
    metadata
  }

  /**
   * The uncompressed-length prefix Arrow writes ahead of a compressed buffer, little-endian. A
   * value of -1 means the buffer was stored verbatim because compressing it did not pay.
   */
  private def uncompressedLength(data: Array[Byte], bufferStart: Long): Long = {
    var value = 0L
    var i = 7
    while (i >= 0) {
      value = (value << 8) | (data(bufferStart.toInt + i) & 0xffL)
      i -= 1
    }
    value
  }
}
