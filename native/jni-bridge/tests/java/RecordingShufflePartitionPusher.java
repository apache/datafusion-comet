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
import java.util.Arrays;

public final class RecordingShufflePartitionPusher implements ShufflePartitionPusher {
  public int calls;
  public int partitionId;
  public int adjustment;
  public int failureMode;
  public int reservationCalls;
  public int reservationReleases;
  public int reservedBytes;
  public boolean reservedBeforePush;
  public byte[] lastBytes;
  public final IOException failure = new IOException("recorded push failure");

  @Override
  public void reservePartitionData(int maxLength) throws IOException {
    reservationCalls++;
    if (failureMode == 2) {
      throw failure;
    }
    reservedBytes = maxLength;
  }

  @Override
  public void releasePartitionDataReservation() {
    reservationReleases++;
    reservedBytes = 0;
  }

  @Override
  public int pushPartitionData(int partitionId, byte[] bytes, int length) throws IOException {
    calls++;
    if (failureMode != 0) {
      throw failure;
    }
    reservedBeforePush = reservedBytes >= length;
    reservedBytes = 0;
    this.partitionId = partitionId;
    lastBytes = Arrays.copyOf(bytes, length);
    return length + adjustment;
  }
}
