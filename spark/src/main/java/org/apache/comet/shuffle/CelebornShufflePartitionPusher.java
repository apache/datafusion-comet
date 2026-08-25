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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

/** Sends complete native Comet frames through an existing, task-scoped Celeborn shuffle client. */
public final class CelebornShufflePartitionPusher implements ShufflePartitionPusher {

  // Celeborn prefixes every accepted payload with four transport-level integers.
  private static final int CELEBORN_BATCH_HEADER_BYTES = 4 * Integer.BYTES;

  private final Object shuffleClient;
  private final Method pushOrMergeData;
  private final int shuffleId;
  private final int mapId;
  private final int encodedAttemptId;
  private final int numMappers;
  private final int numPartitions;

  /**
   * Binds the existing Celeborn client and all shuffle identity to one map task.
   *
   * <p>The client is deliberately accepted as an {@link Object}: Celeborn is provided by the Spark
   * executor, and Comet must remain usable when the optional Celeborn client is absent. Its public
   * raw-push method is resolved once so an unsupported client fails before any data is written.
   *
   * @param shuffleClient the existing Celeborn {@code ShuffleClientImpl} for this application
   * @param shuffleId the Celeborn shuffle ID, which can differ from the Spark shuffle ID
   * @param mapId the logical map-partition index
   * @param encodedAttemptId the combined Spark stage and task attempt number
   * @param numMappers the number of map tasks in the shuffle
   * @param numPartitions the number of output reduce partitions
   */
  public CelebornShufflePartitionPusher(
      Object shuffleClient,
      int shuffleId,
      int mapId,
      int encodedAttemptId,
      int numMappers,
      int numPartitions) {
    if (shuffleClient == null) {
      throw new IllegalArgumentException("Celeborn shuffle client must not be null");
    }
    if (shuffleId < 0) {
      throw new IllegalArgumentException("Celeborn shuffle ID must not be negative");
    }
    if (mapId < 0) {
      throw new IllegalArgumentException("Celeborn map ID must not be negative");
    }
    if (encodedAttemptId < 0) {
      throw new IllegalArgumentException("Celeborn encoded attempt ID must not be negative");
    }
    if (numMappers <= 0) {
      throw new IllegalArgumentException("Celeborn mapper count must be positive");
    }
    if (mapId >= numMappers) {
      throw new IllegalArgumentException("Celeborn map ID is outside the mapper count");
    }
    if (numPartitions <= 0) {
      throw new IllegalArgumentException("Celeborn partition count must be positive");
    }

    Method method;
    try {
      method =
          shuffleClient
              .getClass()
              .getMethod(
                  "pushOrMergeData",
                  int.class,
                  int.class,
                  int.class,
                  int.class,
                  byte[].class,
                  int.class,
                  int.class,
                  int.class,
                  int.class,
                  boolean.class,
                  boolean.class);
    } catch (NoSuchMethodException | SecurityException e) {
      throw new IllegalArgumentException(
          "Celeborn shuffle client does not provide the required public raw-push API", e);
    }
    if (method.getReturnType() != int.class || Modifier.isStatic(method.getModifiers())) {
      throw new IllegalArgumentException(
          "Celeborn raw-push API must be an instance method returning an int");
    }

    this.shuffleClient = shuffleClient;
    this.pushOrMergeData = method;
    this.shuffleId = shuffleId;
    this.mapId = mapId;
    this.encodedAttemptId = encodedAttemptId;
    this.numMappers = numMappers;
    this.numPartitions = numPartitions;
  }

  @Override
  public int pushPartitionData(int partitionId, byte[] bytes, int length) throws IOException {
    if (partitionId < 0 || partitionId >= numPartitions) {
      throw new IOException("Celeborn output partition is outside this task's partition count");
    }
    if (bytes == null) {
      throw new IOException("Celeborn shuffle frame must not be null");
    }
    if (length <= 0 || length > bytes.length) {
      throw new IOException("Celeborn shuffle frame length must be within the supplied bytes");
    }
    if (length > Integer.MAX_VALUE - CELEBORN_BATCH_HEADER_BYTES) {
      throw new IOException("Celeborn shuffle frame and transport header exceed the byte limit");
    }

    final int accepted;
    try {
      // doPush=true sends this complete frame immediately. skipCompress=true preserves the
      // existing Comet frame, which already owns its compression and framing format.
      accepted =
          (int)
              pushOrMergeData.invoke(
                  shuffleClient,
                  shuffleId,
                  mapId,
                  encodedAttemptId,
                  partitionId,
                  bytes,
                  0,
                  length,
                  numMappers,
                  numPartitions,
                  true,
                  true);
    } catch (IllegalAccessException e) {
      throw new IOException("Cannot invoke the public Celeborn raw-push API", e);
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      if (cause instanceof Error) {
        throw (Error) cause;
      }
      throw new IOException("Celeborn raw shuffle push failed", cause);
    }

    int expected = length + CELEBORN_BATCH_HEADER_BYTES;
    if (accepted != expected) {
      throw new IOException(
          "Celeborn raw shuffle push accepted "
              + accepted
              + " bytes; expected "
              + expected
              + " including its transport header");
    }

    // ShufflePartitionPusher reports Comet payload bytes, never Celeborn transport bytes.
    return length;
  }
}
