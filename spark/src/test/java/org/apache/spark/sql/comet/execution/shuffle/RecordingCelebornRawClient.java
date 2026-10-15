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

package org.apache.spark.sql.comet.execution.shuffle;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import scala.Product2;
import scala.collection.Iterator;

import org.apache.spark.shuffle.ShuffleReader;

/** Exposes the pinned Celeborn read contract without making Celeborn a Comet test dependency. */
public final class RecordingCelebornRawClient {

  private static final AtomicBoolean BROADCAST_DECODER_REGISTERED = new AtomicBoolean(false);

  public static void registerBroadcastDecoder() {
    BROADCAST_DECODER_REGISTERED.set(true);
  }

  public static boolean broadcastDecoderRegistered() {
    return BROADCAST_DECODER_REGISTERED.get();
  }

  public interface MetricsCallback {
    void incBytesRead(long bytes);

    void incReadTime(long time);

    default void incRemoteReadRetryCount(long count) {}

    default void recordRemoteReadWorker(String worker) {}
  }

  public interface OptionalMetricsReporter {
    void incCelebornRemoteReadRetryCount(long count);
  }

  public static final class BackendReader implements ShuffleReader<Object, Object> {
    private final RecordingCelebornRawClient client;

    public BackendReader(RecordingCelebornRawClient client) {
      this.client = client;
    }

    public RecordingCelebornRawClient shuffleClient() {
      return client;
    }

    @Override
    public Iterator<Product2<Object, Object>> read() {
      throw new UnsupportedOperationException("The delegated row reader must never be consumed");
    }
  }

  /** Newer clients insert a coalesced-partition map into the stock 15-argument API. */
  public static final class CoalescedClient {
    public final RecordingCelebornRawClient delegate = new RecordingCelebornRawClient();

    public boolean isShuffleStageEnd(int shuffleId) {
      return delegate.isShuffleStageEnd(shuffleId);
    }

    public ReduceFileGroups updateFileGroup(int shuffleId, int partitionId) throws IOException {
      return delegate.updateFileGroup(shuffleId, partitionId);
    }

    public boolean reportShuffleFetchFailure(int appShuffleId, int shuffleId, long taskId)
        throws IOException {
      return delegate.reportShuffleFetchFailure(appShuffleId, shuffleId, taskId);
    }

    public InputStream readPartition(
        int shuffleId,
        int appShuffleId,
        int partitionId,
        int attemptNumber,
        long taskId,
        int startMapIndex,
        int endMapIndex,
        Object exceptionMaker,
        ArrayList<Object> locations,
        ArrayList<Object> streamHandlers,
        Map<String, Object> pushFailedBatches,
        Map<String, Object> chunksRange,
        Map<String, Object> coalescedPartitionInfos,
        int[] mapAttempts,
        MetricsCallback metricsCallback,
        boolean needDecompress)
        throws IOException {
      if (coalescedPartitionInfos != null) {
        throw new AssertionError("Native readers must not request physical-skew coalescing");
      }
      return delegate.readPartition(
          shuffleId,
          appShuffleId,
          partitionId,
          attemptNumber,
          taskId,
          startMapIndex,
          endMapIndex,
          exceptionMaker,
          locations,
          streamHandlers,
          pushFailedBatches,
          chunksRange,
          mapAttempts,
          metricsCallback,
          needDecompress);
    }
  }

  public static final class ReduceFileGroups {
    public Map<Integer, Set<Object>> partitionGroups = new HashMap<>();
    public Map<String, Object> pushFailedBatches = new HashMap<>();
    public int[] mapAttempts = new int[] {0};
  }

  public static final class ReadRequest {
    public final int shuffleId;
    public final int appShuffleId;
    public final int partitionId;
    public final int attemptNumber;
    public final long taskId;
    public final int startMapIndex;
    public final int endMapIndex;
    public final Object exceptionMaker;
    public final ArrayList<Object> locations;
    public final ArrayList<Object> streamHandlers;
    public final Map<String, Object> pushFailedBatches;
    public final Map<String, Object> chunksRange;
    public final Map<String, Object> coalescedPartitionInfos;
    public final int[] mapAttempts;
    public final MetricsCallback metricsCallback;
    public final boolean needDecompress;

    ReadRequest(
        int shuffleId,
        int appShuffleId,
        int partitionId,
        int attemptNumber,
        long taskId,
        int startMapIndex,
        int endMapIndex,
        Object exceptionMaker,
        ArrayList<Object> locations,
        ArrayList<Object> streamHandlers,
        Map<String, Object> pushFailedBatches,
        Map<String, Object> chunksRange,
        Map<String, Object> coalescedPartitionInfos,
        int[] mapAttempts,
        MetricsCallback metricsCallback,
        boolean needDecompress) {
      this.shuffleId = shuffleId;
      this.appShuffleId = appShuffleId;
      this.partitionId = partitionId;
      this.attemptNumber = attemptNumber;
      this.taskId = taskId;
      this.startMapIndex = startMapIndex;
      this.endMapIndex = endMapIndex;
      this.exceptionMaker = exceptionMaker;
      this.locations = locations;
      this.streamHandlers = streamHandlers;
      this.pushFailedBatches = pushFailedBatches;
      this.chunksRange = chunksRange;
      this.coalescedPartitionInfos = coalescedPartitionInfos;
      this.mapAttempts = mapAttempts;
      this.metricsCallback = metricsCallback;
      this.needDecompress = needDecompress;
    }
  }

  public final ReduceFileGroups fileGroups = new ReduceFileGroups();
  public final Map<Integer, InputStream> streams = new HashMap<>();
  public final List<ReadRequest> requests = new ArrayList<>();
  public int updateFileGroupCalls;
  public int stageEndChecks;
  public int failureReports;
  public int cleanupCalls;
  public int timeoutFailures;
  public boolean stageEnded = true;
  public boolean invalidateOnFetchFailure = true;
  public boolean requiresBroadcastDecoder;
  public CountDownLatch readPartitionStarted;
  public CountDownLatch allowReadPartition;
  public IOException updateFileGroupFailure;
  public IOException readPartitionFailure;
  public IOException reportFetchFailureFailure;
  public Runnable beforeUpdateFileGroup;

  public boolean isShuffleStageEnd(int shuffleId) {
    stageEndChecks += 1;
    return stageEnded;
  }

  public ReduceFileGroups updateFileGroup(int shuffleId, int partitionId) throws IOException {
    updateFileGroupCalls += 1;
    if (beforeUpdateFileGroup != null) {
      beforeUpdateFileGroup.run();
    }
    if (requiresBroadcastDecoder && !broadcastDecoderRegistered()) {
      throw new IOException("Celeborn's broadcast reducer-file-group decoder was not registered");
    }
    if (timeoutFailures > 0) {
      timeoutFailures -= 1;
      throw new IOException("reducer-file-group RPC timed out", new TimeoutException());
    }
    if (updateFileGroupFailure != null) {
      throw updateFileGroupFailure;
    }
    return fileGroups;
  }

  public InputStream readPartition(
      int shuffleId,
      int appShuffleId,
      int partitionId,
      int attemptNumber,
      long taskId,
      int startMapIndex,
      int endMapIndex,
      Object exceptionMaker,
      ArrayList<Object> locations,
      ArrayList<Object> streamHandlers,
      Map<String, Object> pushFailedBatches,
      Map<String, Object> chunksRange,
      int[] mapAttempts,
      MetricsCallback metricsCallback,
      boolean needDecompress)
      throws IOException {
    requests.add(
        new ReadRequest(
            shuffleId,
            appShuffleId,
            partitionId,
            attemptNumber,
            taskId,
            startMapIndex,
            endMapIndex,
            exceptionMaker,
            locations,
            streamHandlers,
            pushFailedBatches,
            chunksRange,
            null,
            mapAttempts,
            metricsCallback,
            needDecompress));
    if (readPartitionFailure != null) {
      throw readPartitionFailure;
    }
    if (readPartitionStarted != null) {
      readPartitionStarted.countDown();
      try {
        if (!allowReadPartition.await(5, TimeUnit.SECONDS)) {
          throw new IOException("timed out waiting to open the test reducer stream");
        }
      } catch (InterruptedException failure) {
        Thread.currentThread().interrupt();
        throw new IOException("interrupted while opening the test reducer stream", failure);
      }
    }
    return streams.get(partitionId);
  }

  public boolean reportShuffleFetchFailure(int appShuffleId, int shuffleId, long taskId)
      throws IOException {
    failureReports += 1;
    if (reportFetchFailureFailure != null) {
      throw reportFetchFailureFailure;
    }
    return invalidateOnFetchFailure;
  }

  public boolean cleanupShuffle(int shuffleId) {
    cleanupCalls += 1;
    return true;
  }
}
