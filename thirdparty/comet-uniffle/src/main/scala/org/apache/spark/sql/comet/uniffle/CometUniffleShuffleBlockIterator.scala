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

package org.apache.spark.sql.comet.uniffle

import java.nio.{ByteBuffer, ByteOrder}

import scala.annotation.tailrec

import org.apache.spark.internal.Logging
import org.apache.uniffle.client.api.ShuffleReadClient
import org.apache.uniffle.client.response.ShuffleBlock

import org.apache.comet.shuffle.CometShuffleBlockIterator

class CometUniffleShuffleBlockIterator(
    startPartition: Int,
    endPartition: Int,
    createShuffleReadClient: Int => ShuffleReadClient)
    extends CometShuffleBlockIterator
    with Logging {
  private var currentPartition: Int = startPartition
  private var current: ByteBuffer = _
  private var currentShuffleReadClient: ShuffleReadClient = createShuffleReadClient(
    currentPartition)

  private val headerBuf = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN)
  private val INITIAL_BUFFER_SIZE = 128 * 1024
  private var dataBuf = ByteBuffer.allocateDirect(INITIAL_BUFFER_SIZE)
  private var currentBlockLength = 0

  var currentBatchFieldCount: Int = _

  override def hasNext: Int = {
    headerBuf.clear()
    if (!readFully(headerBuf)) {
      if (headerBuf.position() == 0) {
        return -1
      }
      throw new IllegalStateException("Unexpected end of shuffle data while reading block header")
    }
    headerBuf.flip()

    val compressedLength = headerBuf.getLong
    currentBatchFieldCount = headerBuf.getLong.toInt

    // Subtract 8 because compressedLength includes the 8-byte field count we already read
    val bytesToRead = compressedLength - 8
    if (bytesToRead > Integer.MAX_VALUE) {
      throw new IllegalStateException(
        "Native shuffle block size of " + bytesToRead + " exceeds maximum of "
          + Integer.MAX_VALUE + ". Try reducing spark.comet.columnar.shuffle.batch.size.")
    }
    currentBlockLength = bytesToRead.toInt

    if (dataBuf.capacity < currentBlockLength) {
      val newCapacity = Math.min(bytesToRead * 2L, Integer.MAX_VALUE).toInt
      dataBuf = ByteBuffer.allocateDirect(newCapacity)
    }
    dataBuf.clear
    dataBuf.limit(currentBlockLength)

    if (!readFully(dataBuf)) {
      throw new IllegalStateException(
        "Unexpected end of shuffle data while reading block of length " + currentBlockLength)
    }

    currentBlockLength
  }

  private def readFully(target: ByteBuffer): Boolean = {
    while (target.hasRemaining) {
      if (current == null || !current.hasRemaining) {
        val nextBlock = nextShuffleBlock()
        if (nextBlock.isEmpty) {
          return false
        }
        current = nextBlock.get
      }

      val oldLimit = current.limit()
      try {
        current.limit(current.position() + Math.min(target.remaining(), current.remaining()))
        target.put(current)
      } finally {
        current.limit(oldLimit)
      }
    }
    true
  }

  @tailrec
  private def nextShuffleBlock(): Option[ByteBuffer] = {
    val shuffleBlock: ShuffleBlock = if (currentShuffleReadClient != null) {
      currentShuffleReadClient.readShuffleBlockData
    } else {
      null
    }
    val rawData = if (shuffleBlock != null) {
      shuffleBlock.getByteBuffer
    } else {
      null
    }
    if (rawData == null) {
      if (currentShuffleReadClient != null) {
        currentShuffleReadClient.checkProcessedBlockIds()
        currentShuffleReadClient.logStatics()
        currentShuffleReadClient.close()
        currentShuffleReadClient = null
      }
      currentPartition += 1
      if (currentPartition >= endPartition) {
        return None
      }

      currentShuffleReadClient = createShuffleReadClient(currentPartition)
      return nextShuffleBlock()
    }
    Some(rawData)
  }

  override def getBuffer: ByteBuffer = dataBuf

  override def getCurrentBlockLength: Int = currentBlockLength

  override def close(): Unit = {
    if (current != null) {
      current = null
    }
    if (dataBuf != null) {
      dataBuf = ByteBuffer.allocateDirect(INITIAL_BUFFER_SIZE)
    }
    if (currentShuffleReadClient != null) {
      currentShuffleReadClient.close()
      currentShuffleReadClient = null
    }
  }
}
