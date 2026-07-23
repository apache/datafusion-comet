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

package org.apache.comet.shuffle;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Supplies encoded shuffle blocks to Comet's native execution engine through JNI.
 *
 * <p>Unlike a standard Java iterator, {@link #hasNext()} advances to the next block, loads it into
 * a direct buffer, and returns its length. The current block must be consumed before advancing
 * again because implementations may reuse the buffer.
 */
public interface CometShuffleBlockIterator {

  /**
   * Advances to the next shuffle block and makes it available through {@link #getBuffer()}.
   *
   * @return the current block length in bytes, or {@code -1} when no blocks remain
   * @throws IOException if the next block cannot be read
   */
  int hasNext() throws IOException;

  /**
   * Returns the direct byte buffer containing the current shuffle block.
   *
   * <p>The first {@link #getCurrentBlockLength()} bytes contain the block produced by the most
   * recent successful call to {@link #hasNext()}. The buffer is valid only until the next call to
   * {@code hasNext()} or {@link #close()}.
   *
   * @return the direct byte buffer containing the current block
   */
  ByteBuffer getBuffer();

  /**
   * Returns the number of valid bytes in the current block buffer.
   *
   * @return the current block length in bytes
   */
  int getCurrentBlockLength();

  /**
   * Releases resources held by this iterator.
   *
   * @throws IOException if an underlying resource cannot be closed
   */
  void close() throws IOException;
}
