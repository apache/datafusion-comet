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

/**
 * Callback used by the native shuffle writer to push encoded partition data through a JVM shuffle
 * implementation.
 */
public interface ShufflePartitionPusher {

  /**
   * Pushes encoded shuffle data for an output partition.
   *
   * @param partitionId the output partition that receives the data
   * @param bytes the byte array containing the encoded shuffle data
   * @param length the number of valid bytes in {@code bytes}
   * @return the number of bytes consumed by the shuffle implementation
   */
  int pushPartitionData(int partitionId, byte[] bytes, int length);
}
