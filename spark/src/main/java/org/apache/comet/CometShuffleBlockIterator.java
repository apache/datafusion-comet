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

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

/**
 * Provides raw compressed shuffle blocks to native code via JNI.
 *
 * <p>Reads block headers (compressed length + field count) from a shuffle InputStream and loads the
 * compressed body into a DirectByteBuffer. Native code pulls blocks by calling hasNext() and
 * getBuffer().
 *
 * <p>The DirectByteBuffer returned by getBuffer() is only valid until the next hasNext() call.
 * Native code must fully consume it (via read_ipc_compressed which allocates new memory for the
 * decompressed data) before pulling the next block.
 */
public class CometShuffleBlockIterator implements Closeable {

  private static final int INITIAL_BUFFER_SIZE = 128 * 1024;

  /** Block format header: 8-byte length + 8-byte field count. */
  private static final int BLOCK_HEADER_SIZE = 16;
  /** IPC stream format header: 8-byte length only. */
  private static final int IPC_STREAM_HEADER_SIZE = 8;

  private final ReadableByteChannel channel;
  private final InputStream inputStream;
  private final ByteBuffer headerBuf = ByteBuffer.allocate(BLOCK_HEADER_SIZE)
      .order(ByteOrder.LITTLE_ENDIAN);
  private ByteBuffer dataBuf = ByteBuffer.allocateDirect(INITIAL_BUFFER_SIZE);
  private boolean closed = false;
  private int currentBlockLength = 0;
  private final boolean isIpcStream;

  public CometShuffleBlockIterator(InputStream in) {
    this(in, false);
  }

  public CometShuffleBlockIterator(InputStream in, boolean isIpcStream) {
    this.inputStream = in;
    this.channel = Channels.newChannel(in);
    this.isIpcStream = isIpcStream;
  }

  /**
   * Reads the next block header and loads the body into the internal buffer. Called by native code
   * via JNI.
   *
   * <p>Block format header: 8-byte compressedLength (includes field count but not itself) + 8-byte
   * fieldCount (discarded). Body is: 4-byte codec prefix + compressed IPC data.
   *
   * <p>IPC stream format header: 8-byte length. Body is: raw Arrow IPC stream data.
   *
   * @return the body length in bytes, or -1 if EOF
   */
  public int hasNext() throws IOException {
    if (closed) {
      return -1;
    }

    int headerSize = isIpcStream ? IPC_STREAM_HEADER_SIZE : BLOCK_HEADER_SIZE;
    headerBuf.clear();
    headerBuf.limit(headerSize);
    while (headerBuf.hasRemaining()) {
      int bytesRead = channel.read(headerBuf);
      if (bytesRead < 0) {
        if (headerBuf.position() == 0) {
          close();
          return -1;
        }
        throw new EOFException("Data corrupt: unexpected EOF while reading batch header");
      }
    }
    headerBuf.flip();
    long length = headerBuf.getLong();

    long bytesToRead;
    if (isIpcStream) {
      // IPC stream: length is the IPC stream data size
      bytesToRead = length;
    } else {
      // Block format: length includes the 8-byte field count we already read
      headerBuf.getLong(); // discard field count
      bytesToRead = length - 8;
    }

    if (bytesToRead > Integer.MAX_VALUE) {
      throw new IllegalStateException(
          "Native shuffle block size of "
              + bytesToRead
              + " exceeds maximum of "
              + Integer.MAX_VALUE
              + ". Try reducing shuffle batch size.");
    }

    currentBlockLength = (int) bytesToRead;

    if (dataBuf.capacity() < currentBlockLength) {
      int newCapacity = (int) Math.min(bytesToRead * 2L, Integer.MAX_VALUE);
      dataBuf = ByteBuffer.allocateDirect(newCapacity);
    }

    dataBuf.clear();
    dataBuf.limit(currentBlockLength);
    while (dataBuf.hasRemaining()) {
      int bytesRead = channel.read(dataBuf);
      if (bytesRead < 0) {
        throw new EOFException("Data corrupt: unexpected EOF while reading compressed batch");
      }
    }

    return currentBlockLength;
  }

  /**
   * Returns the DirectByteBuffer containing the current block's compressed bytes (4-byte codec
   * prefix + compressed IPC data). Called by native code via JNI.
   */
  public ByteBuffer getBuffer() {
    return dataBuf;
  }

  /** Returns the length of the current block in bytes. Called by native code via JNI. */
  public int getCurrentBlockLength() {
    return currentBlockLength;
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      closed = true;
      inputStream.close();
    }
  }
}
