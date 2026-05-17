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

package org.apache.comet.vector;

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.util.MemoryUtil;
import org.apache.arrow.vector.FieldVector;

/**
 * Helpers that copy the contents of a {@link CometDecodedVector} (whose underlying Arrow buffers
 * live in the shaded {@code org.apache.comet.shaded.arrow.*} package after the comet-common jar is
 * built) into destination buffer addresses provided by the caller.
 *
 * <p>Callers in {@code comet-spark} reference the unshaded {@code org.apache.arrow.*} classes
 * supplied by Spark at runtime. Direct cross-package access from the spark module would fail with a
 * {@code ClassCastException}. Crossing the boundary via raw memory addresses (long primitives)
 * sidesteps the class identity issue: the bytes on disk are identical regardless of which Arrow
 * Java distribution produced them.
 *
 * <p>All traversals use {@code getFieldBuffers()} and {@code getChildrenFromFields()} — the same
 * API that {@code VectorUnloader} uses — so buffer ordering and counts are consistent between the
 * source (shaded) and destination (unshaded) sides.
 */
public final class CometVectorIpcCopier {

  private CometVectorIpcCopier() {}

  /**
   * Returns the readable byte counts of all buffers in {@code cometVec}'s underlying Arrow tree, in
   * depth-first order (the same order {@code VectorUnloader} uses).
   *
   * <p>The caller can use this to size destination buffers before calling {@link
   * #copyBuffersToAddresses}.
   */
  public static long[] bufferReadableBytes(CometDecodedVector cometVec) {
    List<Long> sizes = new ArrayList<>();
    collectBufferSizes((FieldVector) cometVec.getValueVector(), sizes);
    long[] out = new long[sizes.size()];
    for (int i = 0; i < sizes.size(); i++) {
      out[i] = sizes.get(i);
    }
    return out;
  }

  /**
   * Returns the {@code valueCount} of every {@link FieldVector} node in {@code cometVec}'s tree, in
   * depth-first order. The first entry is the value count of the top-level vector; subsequent
   * entries are for nested children (struct fields, list elements).
   */
  public static int[] valueCounts(CometDecodedVector cometVec) {
    List<Integer> counts = new ArrayList<>();
    collectValueCounts((FieldVector) cometVec.getValueVector(), counts);
    int[] out = new int[counts.size()];
    for (int i = 0; i < counts.size(); i++) {
      out[i] = counts.get(i);
    }
    return out;
  }

  /**
   * Copies all of {@code cometVec}'s buffer bytes into {@code destAddresses}, in the same
   * depth-first order as {@link #bufferReadableBytes}. Each destination address must be backed by
   * at least the corresponding entry from {@code bufferReadableBytes} bytes of writable memory.
   */
  public static void copyBuffersToAddresses(CometDecodedVector cometVec, long[] destAddresses) {
    walkAndCopy((FieldVector) cometVec.getValueVector(), destAddresses, new int[] {0});
  }

  private static void collectBufferSizes(FieldVector vec, List<Long> out) {
    for (ArrowBuf buf : vec.getFieldBuffers()) {
      out.add(buf.readableBytes());
    }
    for (FieldVector child : vec.getChildrenFromFields()) {
      collectBufferSizes(child, out);
    }
  }

  private static void collectValueCounts(FieldVector vec, List<Integer> out) {
    out.add(vec.getValueCount());
    for (FieldVector child : vec.getChildrenFromFields()) {
      collectValueCounts(child, out);
    }
  }

  private static void walkAndCopy(FieldVector vec, long[] addrs, int[] cursor) {
    for (ArrowBuf buf : vec.getFieldBuffers()) {
      if (cursor[0] >= addrs.length) {
        throw new IllegalArgumentException(
            "destAddresses too small at cursor="
                + cursor[0]
                + " (have "
                + addrs.length
                + " addresses)");
      }
      MemoryUtil.copyMemory(buf.memoryAddress(), addrs[cursor[0]], buf.readableBytes());
      cursor[0]++;
    }
    for (FieldVector child : vec.getChildrenFromFields()) {
      walkAndCopy(child, addrs, cursor);
    }
  }
}
