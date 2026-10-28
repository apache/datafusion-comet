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

import org.apache.arrow.compression.ZstdCompressionCodec
import org.apache.arrow.memory.{ArrowBuf, BufferAllocator}
import org.apache.arrow.vector.compression.CompressionUtil
import org.apache.spark.SparkException

import com.github.luben.zstd.Zstd

/**
 * Arrow's zstd codec, with the destination capacity it hands zstd corrected.
 *
 * Arrow 18.3.0's `ZstdCompressionCodec.doCompress` allocates `compressBound(n) + 8` bytes and
 * points zstd 8 bytes in, past the uncompressed-length prefix, but still tells it the capacity is
 * `compressBound(n) + 8`. What zstd guarantees is that it never writes past the capacity it is
 * given, not that it never writes past `compressBound`: while it entropy-codes the literals of a
 * block that turns out not to compress, it fills the destination up to that capacity before it
 * falls back to a stored block, so the attempt can run up to 8 bytes past the buffer. The
 * allocator's power-of-two rounding absorbs that unless the request lands within 8 bytes below a
 * power of two, and when it does, the overrun lands in whatever follows the buffer without any
 * error. With Native Memory Tracking on, the JVM aborts when the buffer is freed (#7305).
 *
 * Fixed upstream by apache/arrow-java#1119 for Arrow 20.0.0; this is that fix. Only `doCompress`
 * differs, so decompression stays Arrow's.
 */
private[comet] class CometZstdCompressionCodec(level: Int) extends ZstdCompressionCodec(level) {

  override protected def doCompress(
      allocator: BufferAllocator,
      uncompressed: ArrowBuf): ArrowBuf = {
    val capacity = Zstd.compressBound(uncompressed.writerIndex())
    val compressed = allocator.buffer(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH + capacity)
    val written = Zstd.compressUnsafe(
      compressed.memoryAddress() + CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH,
      capacity,
      uncompressed.memoryAddress(),
      uncompressed.writerIndex(),
      level)
    if (Zstd.isError(written)) {
      compressed.close()
      throw new SparkException(
        s"zstd failed to compress a cached batch: ${Zstd.getErrorName(written)}")
    }
    compressed.writerIndex(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH + written)
    compressed
  }
}
