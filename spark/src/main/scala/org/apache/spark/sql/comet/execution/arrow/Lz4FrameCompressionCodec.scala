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

import scala.util.control.NonFatal

import org.apache.arrow.memory.{ArrowBuf, BufferAllocator}
import org.apache.arrow.vector.compression.{AbstractCompressionCodec, CompressionUtil}
import org.apache.spark.SparkException
import org.apache.spark.internal.Logging

import net.jpountz.lz4.{LZ4Compressor, LZ4Exception, LZ4Factory, LZ4SafeDecompressor}

/**
 * Arrow's LZ4_FRAME compression codec, on lz4-java.
 *
 * Arrow ships an LZ4_FRAME codec of its own, but it is commons-compress's pure-Java LZ4, which
 * writes orders of magnitude more slowly than zstd and produces larger output. This writes the
 * same format with lz4-java, the JNI-accelerated library behind `spark.io.compression.codec`:
 * each buffer is one standard LZ4 frame behind Arrow's 8-byte uncompressed-length prefix, which
 * `AbstractCompressionCodec` reads and writes, so any Arrow reader can decode it. lz4-java comes
 * from Spark's own classpath, as zstd-jni does for Arrow's zstd codec.
 *
 * Every frame has the same header: independent blocks of up to 4 MiB, and no checksums, content
 * size or dictionary. The reader accepts no other, since it only ever reads frames this codec
 * wrote. It checks every length the frame records before reading through it, because Comet turns
 * off Arrow's own bounds checks (`arrow.enable_unsafe_memory_access`), so a corrupt length would
 * otherwise read or write past the buffers.
 *
 * Lengths are written in the platform's byte order, as the rest of the payload is, which is the
 * little-endian order the format requires on every platform Comet runs on.
 */
private[comet] object Lz4FrameCompressionCodec extends AbstractCompressionCodec with Logging {

  private val MaxBlockSize = 4 * 1024 * 1024

  // The magic number, then the frame descriptor: FLG (version 01, independent blocks), BD (a
  // 4 MiB maximum block size) and HC, the second byte of the xxHash32 of FLG and BD with a zero
  // seed. HC is a constant because FLG and BD are.
  private val Magic = 0x184d2204
  private val Flg: Byte = 0x60
  private val Bd: Byte = 0x70
  private val Hc: Byte = 0x73
  private val HeaderSize = 7

  // Each block is led by its length, whose high bit marks a block stored as it was because
  // compressing it would not have made it smaller. A zero length ends the frame.
  private val LengthSize = 4
  private val StoredBlock = 0x80000000

  private val Prefix = CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH

  // fastestInstance() takes lz4-java's JNI implementation only when lz4-java was loaded by the
  // system class loader, and silently settles for its Java one otherwise, which is the case under
  // `mvn exec:java`. Ask for the JNI one directly, and use Java only where it cannot load.
  private lazy val factory: LZ4Factory =
    try LZ4Factory.nativeInstance()
    catch {
      case e @ (NonFatal(_) | _: LinkageError) =>
        logWarning(
          "Could not load lz4-java's native library, so Comet's in-memory cache uses its " +
            "slower Java implementation of lz4",
          e)
        LZ4Factory.fastestJavaInstance()
    }
  private lazy val compressor: LZ4Compressor = factory.fastCompressor()
  private lazy val decompressor: LZ4SafeDecompressor = factory.safeDecompressor()

  override def getCodecType: CompressionUtil.CodecType = CompressionUtil.CodecType.LZ4_FRAME

  override protected def doCompress(
      allocator: BufferAllocator,
      uncompressed: ArrowBuf): ArrowBuf = {
    val length = uncompressed.writerIndex()
    val fullBlocks = length / MaxBlockSize
    val lastBlock = (length % MaxBlockSize).toInt
    val bound = Prefix + HeaderSize +
      fullBlocks * (LengthSize + compressor.maxCompressedLength(MaxBlockSize)) +
      (if (lastBlock > 0) LengthSize + compressor.maxCompressedLength(lastBlock) else 0) +
      LengthSize
    val frame = allocator.buffer(bound)
    try {
      frame.setInt(Prefix, Magic)
      frame.setByte(Prefix + 4, Flg)
      frame.setByte(Prefix + 5, Bd)
      frame.setByte(Prefix + 6, Hc)
      var at = Prefix + HeaderSize
      var from = 0L
      while (from < length) {
        val n = math.min(MaxBlockSize.toLong, length - from).toInt
        val room = compressor.maxCompressedLength(n)
        val packed = compressor.compress(
          uncompressed.nioBuffer(from, n),
          0,
          n,
          frame.nioBuffer(at + LengthSize, room),
          0,
          room)
        if (packed < n) {
          frame.setInt(at, packed)
          at += LengthSize + packed
        } else {
          frame.setInt(at, n | StoredBlock)
          frame.setBytes(at + LengthSize, uncompressed, from, n.toLong)
          at += LengthSize + n
        }
        from += n
      }
      frame.setInt(at, 0)
      frame.writerIndex(at + LengthSize)
      frame
    } catch {
      case NonFatal(e) =>
        frame.close()
        throw e
    }
  }

  override protected def doDecompress(
      allocator: BufferAllocator,
      compressed: ArrowBuf): ArrowBuf = {
    val length = readUncompressedLength(compressed)
    val end = compressed.writerIndex()
    if (end < Prefix + HeaderSize ||
      compressed.getInt(Prefix) != Magic ||
      compressed.getByte(Prefix + 4) != Flg ||
      compressed.getByte(Prefix + 5) != Bd ||
      compressed.getByte(Prefix + 6) != Hc) {
      throw corrupt("the buffer does not begin with the frame header Comet writes")
    }

    val plain = allocator.buffer(length)
    try {
      var at = Prefix + HeaderSize
      var to = 0L
      var ended = false
      while (!ended) {
        if (end - at < LengthSize) {
          throw corrupt("the frame ends before its end mark")
        }
        val word = compressed.getInt(at)
        at += LengthSize
        if (word == 0) {
          ended = true
        } else {
          val n = word & ~StoredBlock
          if (n > end - at) {
            throw corrupt(s"a block of $n bytes runs past the end of the frame")
          }
          if ((word & StoredBlock) != 0) {
            if (n > length - to) {
              throw corrupt(s"the frame holds more than its recorded $length bytes")
            }
            plain.setBytes(to, compressed, at, n.toLong)
            to += n
          } else {
            // A block never decodes to more than the maximum block size, and must not decode
            // past the length the frame records.
            val room = math.min(length - to, MaxBlockSize.toLong).toInt
            val decoded =
              try {
                decompressor.decompress(
                  compressed.nioBuffer(at, n),
                  0,
                  n,
                  plain.nioBuffer(to, room),
                  0,
                  room)
              } catch {
                case e: LZ4Exception =>
                  throw corrupt(s"a block does not decode within $room bytes", e)
              }
            to += decoded
          }
          at += n
        }
      }
      if (to != length) {
        throw corrupt(s"the frame decodes to $to bytes rather than its recorded $length")
      }
      plain.writerIndex(length)
      plain
    } catch {
      case NonFatal(e) =>
        plain.close()
        throw e
    }
  }

  private def corrupt(reason: String, cause: Throwable = null): SparkException =
    new SparkException(s"Comet cached batch holds a corrupt LZ4 frame: $reason", cause)
}
