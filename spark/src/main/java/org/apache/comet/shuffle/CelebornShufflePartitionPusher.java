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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/** Adapts complete Comet shuffle frames to an existing, task-owned Celeborn shuffle client. */
public final class CelebornShufflePartitionPusher implements ShufflePartitionPusher {

  private static final int CELEBORN_BATCH_HEADER_BYTES = 4 * Integer.BYTES;
  private static final int MINIMUM_COMET_FRAME_BYTES = 2 * Long.BYTES;

  private final Object shuffleClient;
  private final Method pushOrMergeData;
  private final int shuffleId;
  private final int mapId;
  private final int encodedAttemptId;
  private final int numMappers;
  private final int numPartitions;

  /**
   * Captures all shuffle and map-attempt identity before native worker threads invoke this pusher.
   *
   * <p>Celeborn remains an optional, application-provided dependency: accepting its client as an
   * {@link Object} and resolving its public raw-push method avoids a compile-time dependency.
   *
   * @param shuffleClient the application's existing Celeborn shuffle client
   * @param shuffleId the Celeborn shuffle identifier for this task
   * @param mapId the logical map partition
   * @param encodedAttemptId the Celeborn-encoded stage and task attempt
   * @param numMappers the total number of map partitions
   * @param numPartitions the total number of reduce partitions
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

    final Method pushMethod;
    try {
      pushMethod =
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

    if (pushMethod.getReturnType() != int.class || Modifier.isStatic(pushMethod.getModifiers())) {
      throw new IllegalArgumentException(
          "Celeborn raw-push API must be an instance method returning an int");
    }

    this.shuffleClient = shuffleClient;
    this.pushOrMergeData = pushMethod;
    this.shuffleId = shuffleId;
    this.mapId = mapId;
    this.encodedAttemptId = encodedAttemptId;
    this.numMappers = numMappers;
    this.numPartitions = numPartitions;
  }

  /** Sends exactly one complete, already-compressed Comet shuffle frame to Celeborn. */
  @Override
  public void pushPartitionData(int partitionId, byte[] data, int length) throws IOException {
    if (partitionId < 0 || partitionId >= numPartitions) {
      throw new IOException("Celeborn output partition is outside this task's partition count");
    }
    if (data == null) {
      throw new IOException("Celeborn shuffle frame must not be null");
    }
    if (length > Integer.MAX_VALUE - CELEBORN_BATCH_HEADER_BYTES) {
      throw new IOException("Celeborn shuffle frame and transport header exceed the byte limit");
    }
    if (length < MINIMUM_COMET_FRAME_BYTES || length > data.length) {
      throw new IOException("Celeborn shuffle frame length must describe one complete frame");
    }

    final long declaredBodyLength = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getLong();
    if (declaredBodyLength != (long) length - Long.BYTES) {
      throw new IOException(
          "Celeborn shuffle frame declares "
              + declaredBodyLength
              + " body bytes, but contains "
              + (length - Long.BYTES));
    }

    final int accepted;
    try {
      accepted =
          (int)
              pushOrMergeData.invoke(
                  shuffleClient,
                  shuffleId,
                  mapId,
                  encodedAttemptId,
                  partitionId,
                  data,
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
  }
}
