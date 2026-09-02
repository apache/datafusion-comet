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

/**
 * Receives complete encoded shuffle blocks from a native partition writer.
 *
 * <p>Instances belong to one Spark task. Implementations must be safe to invoke from native worker
 * threads, which do not inherit the Spark task thread's thread-local context.
 */
@FunctionalInterface
public interface ShufflePartitionPusher {

  /** Reserves an upper bound before a native worker starts encoding a shuffle frame. */
  default void reservePartitionData(int maxLength) throws IOException {}

  /**
   * Acknowledges that native encoding buffers and JNI local references have been released, on both
   * success and failure. Implementations may retain admission until asynchronous pushes also
   * finish.
   */
  default void releasePartitionDataReservation() {}

  /** Returns the largest encoding reservation this callback can admit before allocating buffers. */
  default int maxReservationBytes() {
    return Integer.MAX_VALUE;
  }

  /** Returns the largest complete frame that this callback can safely accept. */
  default int maxFrameBytes() {
    return Integer.MAX_VALUE - 8;
  }

  /** Pushes one complete, length-prefixed Arrow IPC block for the given output partition. */
  void pushPartitionData(int partitionId, byte[] data, int length) throws IOException;
}
