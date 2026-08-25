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

/** Task-owned callback for sending complete native Comet frames to a remote shuffle service. */
@FunctionalInterface
public interface ShufflePartitionPusher {

  /**
   * Accepts one complete, independently decodable Comet frame for an output partition.
   *
   * <p>The implementation must own any bytes it needs after this method returns. Success means
   * acceptance, not remote map commit. Transport headers must not be included in the return value.
   * Partial acceptance, cancellation, and supersession must fail rather than return a short count.
   * The task owner must drain accepted pushes and commit or abort the attempt separately.
   *
   * @param partitionId the zero-based output partition
   * @param bytes the complete encoded frame
   * @param length the number of valid bytes in {@code bytes}
   * @return exactly {@code length} on success
   * @throws IOException if the frame cannot be accepted
   */
  int pushPartitionData(int partitionId, byte[] bytes, int length) throws IOException;
}
