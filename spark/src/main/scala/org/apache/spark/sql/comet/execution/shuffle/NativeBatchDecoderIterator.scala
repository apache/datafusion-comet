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

import java.io.{EOFException, InputStream}
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.channels.{Channels, ReadableByteChannel}

import org.apache.spark.TaskContext
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.Native
import org.apache.comet.vector.NativeUtil

/**
 * This iterator wraps a Spark input stream that is reading shuffle blocks generated by the Comet
 * native ShuffleWriterExec and then calls native code to decompress and decode the shuffle blocks
 * and use Arrow FFI to return the Arrow record batch.
 */
case class NativeBatchDecoderIterator(
    var in: InputStream,
    taskContext: TaskContext,
    decodeTime: SQLMetric)
    extends Iterator[ColumnarBatch] {
  private var nextBatch: Option[ColumnarBatch] = None
  private var finished = false;
  private val longBuf = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN)
  private val native = new Native()
  private val nativeUtil = new NativeUtil()

  import NativeBatchDecoderIterator.threadLocalDataBuf

  if (taskContext != null) {
    taskContext.addTaskCompletionListener[Unit](_ => {
      close()
    })
  }

  private val channel: ReadableByteChannel = if (in != null) {
    Channels.newChannel(in)
  } else {
    null
  }

  def hasNext(): Boolean = {
    if (channel == null || finished) {
      return false
    }
    if (nextBatch.isDefined) {
      return true
    }

    // read compressed batch size from header
    try {
      longBuf.clear()
      while (longBuf.hasRemaining && channel.read(longBuf) >= 0) {}
    } catch {
      case _: EOFException =>
        close()
        return false
    }

    // If we reach the end of the stream, we are done, or if we read partial length
    // then the stream is corrupted.
    if (longBuf.hasRemaining) {
      if (longBuf.position() == 0) {
        close()
        return false
      }
      throw new EOFException("Data corrupt: unexpected EOF while reading compressed ipc lengths")
    }

    // get compressed length (including headers)
    longBuf.flip()
    val compressedLength = longBuf.getLong.toInt

    // read field count from header
    longBuf.clear()
    while (longBuf.hasRemaining && channel.read(longBuf) >= 0) {}
    if (longBuf.hasRemaining) {
      throw new EOFException("Data corrupt: unexpected EOF while reading field count")
    }
    longBuf.flip()
    val fieldCount = longBuf.getLong.toInt

    // read body
    val bytesToRead = compressedLength - 8
    var dataBuf = threadLocalDataBuf.get()
    if (dataBuf.capacity() < bytesToRead) {
      val newCapacity = bytesToRead * 2
      // scalastyle:off println
      val threadId = Thread.currentThread().getId
      println(s"[$threadId] Growing buffer from ${dataBuf.capacity()} to $newCapacity bytes")
      dataBuf = ByteBuffer.allocateDirect(newCapacity)
      threadLocalDataBuf.set(dataBuf)
    }
    dataBuf.clear()
    dataBuf.limit(bytesToRead)
    while (dataBuf.hasRemaining && channel.read(dataBuf) >= 0) {}
    if (dataBuf.hasRemaining) {
      throw new EOFException("Data corrupt: unexpected EOF while reading compressed batch")
    }

    // make native call to decode batch
    val startTime = System.nanoTime()
    nextBatch = nativeUtil.getNextBatch(
      fieldCount,
      (arrayAddrs, schemaAddrs) => {
        native.decodeShuffleBlock(dataBuf, arrayAddrs, schemaAddrs)
      })
    decodeTime.add(System.nanoTime() - startTime)

    true
  }

  def next(): ColumnarBatch = {
    if (nextBatch.isDefined) {
      val ret = nextBatch.get
      nextBatch = None
      ret
    } else {
      throw new IllegalStateException()
    }
  }

  def close(): Unit = {
    synchronized {
      if (in != null) {
        in.close()
        in = null
      }
      finished = true
    }
  }

}

object NativeBatchDecoderIterator {
  private val threadLocalDataBuf: ThreadLocal[ByteBuffer] = ThreadLocal.withInitial(() => {
    ByteBuffer.allocateDirect(128 * 1024)
  })
}
