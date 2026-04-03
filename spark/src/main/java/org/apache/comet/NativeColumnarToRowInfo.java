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

/**
 * Container for the result of native columnar to row conversion.
 *
 * <p>This class holds the memory address of the converted row data buffer and metadata about each
 * row (offsets and lengths). The native side allocates and owns the memory buffer, and this class
 * provides the JVM with the information needed to read the UnsafeRow data.
 *
 * <p>Memory Layout of the buffer:
 *
 * <pre>
 * ┌─────────────────────────────────────────────────────────────┐
 * │ Row 0: [null bitset][fixed-width values][variable-length]  │
 * ├─────────────────────────────────────────────────────────────┤
 * │ Row 1: [null bitset][fixed-width values][variable-length]  │
 * ├─────────────────────────────────────────────────────────────┤
 * │ ...                                                         │
 * └─────────────────────────────────────────────────────────────┘
 * </pre>
 *
 * <p>The offsets array provides the byte offset from memoryAddress where each row starts. The
 * lengths array provides the total byte length of each row.
 */
public class NativeColumnarToRowInfo {
  /** The memory address of the buffer containing all converted rows. */
  public final long memoryAddress;

  /** The byte offset from memoryAddress where each row starts. */
  public final int[] offsets;

  /** The total byte length of each row. */
  public final int[] lengths;

  /**
   * Constructs a NativeColumnarToRowInfo with the given memory address and row metadata.
   *
   * @param memoryAddress The memory address of the buffer containing converted row data.
   * @param offsets The byte offset for each row from the base memory address.
   * @param lengths The byte length of each row.
   */
  public NativeColumnarToRowInfo(long memoryAddress, int[] offsets, int[] lengths) {
    this.memoryAddress = memoryAddress;
    this.offsets = offsets;
    this.lengths = lengths;
  }

  /**
   * Returns the number of rows in this result.
   *
   * @return The number of rows.
   */
  public int numRows() {
    return offsets != null ? offsets.length : 0;
  }
}
