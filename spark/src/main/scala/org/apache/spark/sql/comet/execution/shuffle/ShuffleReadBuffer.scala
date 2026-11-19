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

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong

/**
 * The direct buffer a reduce task reads each compressed shuffle block into for native code to
 * decode. A task owns one and hands it to every [[NativeBatchDecoderIterator]] it creates, so the
 * buffer grows to the largest block the task has read and stays there, rather than being
 * allocated again for every block: a zero-filled `allocateDirect` goes through the JDK's direct
 * memory reservation, which can force a full GC near the limit. The direct-read path
 * (`CometShuffleBlockIterator`) keeps its buffer the same way.
 *
 * The task thread fills the buffer, outside the iterator's monitor, so that task completion can
 * close the input it reads from. Completion then calls [[release]], which drops the reference and
 * leaves the memory to the buffer's cleaner once nothing else holds it: a read that is still in
 * flight keeps its own reference, so the memory is never freed under it.
 */
private[comet] final class ShuffleReadBuffer {
  import ShuffleReadBuffer._

  @volatile private var buffer: ByteBuffer = _

  /**
   * The buffer, grown if it holds fewer than `bytes`, cleared and with its limit at `bytes`,
   * ready for one block's body to be read into it. The contents of the previous block are gone.
   */
  def prepare(bytes: Int): ByteBuffer = {
    var buf = buffer
    if (buf == null || buf.capacity() < bytes) {
      val capacity =
        if (bytes <= INITIAL_CAPACITY) INITIAL_CAPACITY
        else (bytes * 2L).min(Integer.MAX_VALUE).toInt
      buf = ByteBuffer.allocateDirect(capacity)
      allocations.incrementAndGet()
      buffer = buf
    }
    buf.clear()
    buf.limit(bytes)
    buf
  }

  /** Lets go of the buffer. A later [[prepare]] allocates a new one. */
  def release(): Unit = {
    buffer = null
  }

  /** The capacity of the buffer held, or 0 once released or before the first block. */
  private[shuffle] def capacity: Int = {
    val buf = buffer
    if (buf == null) 0 else buf.capacity()
  }
}

private[comet] object ShuffleReadBuffer {

  /** What the first block gets unless it is larger. Most blocks at the default batch size are. */
  val INITIAL_CAPACITY: Int = 128 * 1024

  /** Direct buffers allocated so far by every instance, for tests to pin the count per task. */
  private[shuffle] val allocations = new AtomicLong
}
