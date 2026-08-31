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

package org.apache.comet;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

/** Provides codec-prefixed Arrow IPC broadcast blocks to native code via JNI. */
public final class CometBroadcastBlockIterator extends CometShuffleBlockIterator {

  private static final int INITIAL_BUFFER_SIZE = 128 * 1024;

  private Iterator<ByteBuffer[]> blocks;
  private ByteBuffer dataBuf = ByteBuffer.allocateDirect(INITIAL_BUFFER_SIZE);
  private boolean closed = false;
  private int currentBlockLength = 0;

  public CometBroadcastBlockIterator(Iterator<ByteBuffer[]> blocks) {
    // Native uses the same block-iterator JNI protocol for shuffle and broadcast inputs. The
    // superclass stream is never read because every protocol method is overridden here.
    super(new ByteArrayInputStream(new byte[0]));
    this.blocks = blocks;
  }

  @Override
  public int hasNext() throws IOException {
    if (closed) {
      return -1;
    }

    ByteBuffer[] block = null;
    long blockSize = 0;
    while (blocks.hasNext() && block == null) {
      ByteBuffer[] candidate = blocks.next();
      long candidateSize = 0;
      for (ByteBuffer chunk : candidate) {
        candidateSize += chunk.remaining();
      }
      if (candidateSize > 0) {
        block = candidate;
        blockSize = candidateSize;
      }
    }
    if (block == null) {
      close();
      return -1;
    }

    if (blockSize > Integer.MAX_VALUE) {
      throw new IllegalStateException(
          "Native broadcast block size of "
              + blockSize
              + " exceeds the direct-read maximum of "
              + Integer.MAX_VALUE
              + " bytes");
    }

    currentBlockLength = (int) blockSize;
    if (dataBuf.capacity() < currentBlockLength) {
      long doubled = Math.max((long) dataBuf.capacity() * 2L, blockSize);
      dataBuf = ByteBuffer.allocateDirect((int) Math.min(doubled, Integer.MAX_VALUE));
    }

    dataBuf.clear();
    dataBuf.limit(currentBlockLength);
    for (ByteBuffer chunk : block) {
      dataBuf.put(chunk.duplicate());
    }
    return currentBlockLength;
  }

  @Override
  public ByteBuffer getBuffer() {
    return dataBuf;
  }

  @Override
  public int getCurrentBlockLength() {
    return currentBlockLength;
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      closed = true;
      blocks = null;
      super.close();
    }
  }
}
