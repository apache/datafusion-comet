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
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

/** Sends complete native Comet frames through an existing, task-scoped Celeborn shuffle client. */
public final class CelebornShufflePartitionPusher implements ShufflePartitionPusher {

  // Celeborn prefixes every accepted payload with four transport-level integers.
  private static final int CELEBORN_BATCH_HEADER_BYTES = 4 * Integer.BYTES;
  private static final int DEFAULT_MAX_IN_FLIGHT_BYTES = 256 * 1024 * 1024;
  private static final long COMPLETION_RECONCILIATION_INTERVAL_MILLIS = 10;
  private static final StackWalker COMPLETION_CALL_STACK = StackWalker.getInstance();
  // One daemon only samples counters; it owns no client/configuration and stops retaining idle
  // maps.
  private static final ScheduledThreadPoolExecutor COMPLETION_RECONCILER =
      createCompletionReconciler();

  private final Object shuffleClient;
  private final Method pushOrMergeData;
  private final Method mapperEnd;
  private final Method cleanup;
  private final Method getPushState;
  private final Method setPushMetricsCallback;
  private final Class<?> pushMetricsCallbackClass;
  private final Field clientPushStates;
  private final Field inFlightRequestTracker;
  private final Field totalInFlightRequests;
  private final Field pushStateException;
  private final ExecutorShufflePushAdmission admission;
  private final int shuffleId;
  private final int mapId;
  private final int encodedAttemptId;
  private final int numMappers;
  private final int numPartitions;
  private final int maxFrameBytes;
  private final int maxReservationBytes;
  private final AtomicLongArray partitionLengths;
  private final Object lifecycleLock = new Object();
  private final Object cleanupLock = new Object();
  private final ThreadLocal<PushReservation> encodingReservation = new ThreadLocal<>();
  private final ArrayDeque<PushReservation> pendingPushes = new ArrayDeque<>();
  private final IdentityHashMap<Object, ObservedPushState> observedPushStates =
      new IdentityHashMap<>();

  private State state = State.OPEN;
  private int activePushes;
  private int activeClientPushes;
  private int activeEncoders;
  private ScheduledFuture<?> completionReconciliation;
  private Thread mapperEndThread;
  private boolean cleanupStarted;
  private boolean cleanupAfterActivePushes;
  private boolean finalCleanupStarted;

  private enum State {
    OPEN,
    FINISHING,
    FINISHED,
    ABORTED
  }

  private static final class PushReservation {
    private int bytes;
    private ObservedPushState pushState;
    private boolean submitted;

    private PushReservation(int bytes) {
      this.bytes = bytes;
    }
  }

  private static final class ObservedPushState {
    private final LongAdder inFlightRequests;
    private final AtomicReference<?> exception;
    private long submittedPushes;
    private long metricsCompletions;
    private long metricsOnlyFailures;
    private long releasedPushes;
    private boolean failedBeforeObservation;
    private boolean recoveredMissedFailure;

    private ObservedPushState(LongAdder inFlightRequests, AtomicReference<?> exception) {
      this.inFlightRequests = inFlightRequests;
      this.exception = exception;
    }
  }

  private static ScheduledThreadPoolExecutor createCompletionReconciler() {
    ScheduledThreadPoolExecutor executor =
        new ScheduledThreadPoolExecutor(
            1,
            runnable -> {
              Thread thread =
                  new Thread(runnable, "comet-celeborn-shuffle-push-admission-reconciler");
              thread.setDaemon(true);
              return thread;
            });
    executor.setRemoveOnCancelPolicy(true);
    return executor;
  }

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
    this(
        shuffleClient,
        shuffleId,
        mapId,
        encodedAttemptId,
        numMappers,
        numPartitions,
        DEFAULT_MAX_IN_FLIGHT_BYTES);
  }

  /** Binds a task-owned adapter to byte admission shared by its executor-side Celeborn client. */
  public CelebornShufflePartitionPusher(
      Object shuffleClient,
      int shuffleId,
      int mapId,
      int encodedAttemptId,
      int numMappers,
      int numPartitions,
      int maxInFlightBytes) {
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
    if (maxInFlightBytes <= CELEBORN_BATCH_HEADER_BYTES) {
      throw new IllegalArgumentException("Celeborn in-flight byte limit must fit a request");
    }

    Method pushOrMergeData;
    try {
      pushOrMergeData =
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
    if (pushOrMergeData.getReturnType() != int.class
        || Modifier.isStatic(pushOrMergeData.getModifiers())) {
      throw new IllegalArgumentException(
          "Celeborn raw-push API must be an instance method returning an int");
    }

    Method mapperEndMethod;
    try {
      mapperEndMethod =
          resolveLifecycleMethod(
              shuffleClient, "mapperEnd", int.class, int.class, int.class, int.class, int.class);
    } catch (IllegalArgumentException unsupportedStandardMapperEnd) {
      // Older application-provided Celeborn clients did not require the reducer count.
      mapperEndMethod =
          resolveLifecycleMethod(
              shuffleClient, "mapperEnd", int.class, int.class, int.class, int.class);
    }
    Method cleanupMethod =
        resolveLifecycleMethod(shuffleClient, "cleanup", int.class, int.class, int.class);
    final Method getPushStateMethod;
    final Method setPushMetricsCallbackMethod;
    final Class<?> pushMetricsCallbackClass;
    final Field clientPushStatesField;
    final Field inFlightRequestTrackerField;
    final Field totalInFlightRequestsField;
    final Field pushStateExceptionField;
    try {
      getPushStateMethod = shuffleClient.getClass().getMethod("getPushState", String.class);
      setPushMetricsCallbackMethod = resolvePushMetricsCallback(getPushStateMethod.getReturnType());
      if (setPushMetricsCallbackMethod == null) {
        // Apache Celeborn does not expose push-completion callbacks. Its in-flight request
        // tracker still records every accepted request and completion, so periodic reconciliation
        // provides the same completion-backed admission without requiring a patched client.
        pushMetricsCallbackClass = null;
      } else {
        pushMetricsCallbackClass = setPushMetricsCallbackMethod.getParameterTypes()[0];
        pushMetricsCallbackClass.getMethod("incPushDataCount", long.class);
      }
      clientPushStatesField = resolveClientPushStates(shuffleClient.getClass());
      inFlightRequestTrackerField =
          getPushStateMethod.getReturnType().getDeclaredField("inFlightRequestTracker");
      totalInFlightRequestsField =
          inFlightRequestTrackerField.getType().getDeclaredField("totalInflightReqs");
      pushStateExceptionField = getPushStateMethod.getReturnType().getDeclaredField("exception");
      if (totalInFlightRequestsField.getType() != LongAdder.class) {
        throw new NoSuchFieldException("InFlightRequestTracker.totalInflightReqs: LongAdder");
      }
      if (pushStateExceptionField.getType() != AtomicReference.class) {
        throw new NoSuchFieldException("PushState.exception: AtomicReference");
      }
      clientPushStatesField.setAccessible(true);
      inFlightRequestTrackerField.setAccessible(true);
      totalInFlightRequestsField.setAccessible(true);
      pushStateExceptionField.setAccessible(true);
    } catch (ReflectiveOperationException | RuntimeException e) {
      throw new IllegalArgumentException(
          "Celeborn shuffle client does not provide observable completion-backed push admission",
          e);
    }
    if (Modifier.isStatic(getPushStateMethod.getModifiers())
        || (setPushMetricsCallbackMethod != null
            && (Modifier.isStatic(setPushMetricsCallbackMethod.getModifiers())
                || setPushMetricsCallbackMethod.getReturnType() != void.class
                || !pushMetricsCallbackClass.isInterface()))) {
      throw new IllegalArgumentException("Celeborn push-state completion API is incompatible");
    }

    this.shuffleClient = shuffleClient;
    this.pushOrMergeData = pushOrMergeData;
    this.mapperEnd = mapperEndMethod;
    this.cleanup = cleanupMethod;
    this.getPushState = getPushStateMethod;
    this.setPushMetricsCallback = setPushMetricsCallbackMethod;
    this.pushMetricsCallbackClass = pushMetricsCallbackClass;
    this.clientPushStates = clientPushStatesField;
    this.inFlightRequestTracker = inFlightRequestTrackerField;
    this.totalInFlightRequests = totalInFlightRequestsField;
    this.pushStateException = pushStateExceptionField;
    this.admission = ExecutorShufflePushAdmission.forClient(shuffleClient, maxInFlightBytes);
    this.shuffleId = shuffleId;
    this.mapId = mapId;
    this.encodedAttemptId = encodedAttemptId;
    this.numMappers = numMappers;
    this.numPartitions = numPartitions;
    this.maxReservationBytes = maxInFlightBytes - CELEBORN_BATCH_HEADER_BYTES;
    this.maxFrameBytes = maxReservationBytes / 3;
    this.partitionLengths = new AtomicLongArray(numPartitions);
  }

  private static Method resolvePushMetricsCallback(Class<?> pushStateClass) {
    for (Method method : pushStateClass.getMethods()) {
      if (method.getName().equals("setMetricsCallback") && method.getParameterCount() == 1) {
        return method;
      }
    }
    return null;
  }

  private static Field resolveClientPushStates(Class<?> clientClass) throws NoSuchFieldException {
    for (Class<?> current = clientClass; current != null; current = current.getSuperclass()) {
      try {
        Field field = current.getDeclaredField("pushStates");
        if (!Map.class.isAssignableFrom(field.getType())
            || Modifier.isStatic(field.getModifiers())) {
          throw new NoSuchFieldException("ShuffleClientImpl.pushStates: instance Map");
        }
        return field;
      } catch (NoSuchFieldException ignored) {
        // Test clients and application wrappers can declare the pinned field on a superclass.
      }
    }
    throw new NoSuchFieldException("ShuffleClientImpl.pushStates: instance Map");
  }

  private static Method resolveLifecycleMethod(
      Object shuffleClient, String name, Class<?>... parameterTypes) {
    final Method method;
    try {
      method = shuffleClient.getClass().getMethod(name, parameterTypes);
    } catch (NoSuchMethodException | SecurityException e) {
      throw new IllegalArgumentException(
          "Celeborn shuffle client does not provide the required public " + name + " API", e);
    }
    if (method.getReturnType() != void.class || Modifier.isStatic(method.getModifiers())) {
      throw new IllegalArgumentException(
          "Celeborn " + name + " API must be an instance method returning void");
    }
    return method;
  }

  @Override
  public void reservePartitionData(int maxLength) throws IOException {
    if (maxLength <= 0 || maxLength > maxReservationBytes) {
      throw new IOException("Celeborn native frame reservation exceeds its byte limit");
    }
    if (encodingReservation.get() != null) {
      throw new IOException("Celeborn native frame already has a reservation on this thread");
    }

    final int requestBytes = maxLength + CELEBORN_BATCH_HEADER_BYTES;
    admission.acquire(requestBytes, this::isAborted);
    synchronized (lifecycleLock) {
      if (state != State.OPEN) {
        admission.release(requestBytes);
        throw new IOException("Celeborn shuffle map attempt no longer accepts frame encoding");
      }
      activeEncoders++;
      encodingReservation.set(new PushReservation(requestBytes));
    }
  }

  @Override
  public void releasePartitionDataReservation() {
    final PushReservation reservation = encodingReservation.get();
    if (reservation == null) {
      return;
    }
    encodingReservation.remove();
    synchronized (lifecycleLock) {
      activeEncoders--;
      lifecycleLock.notifyAll();
    }
    admission.release(reservation.bytes);
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

    beginPush();
    final int requestBytes = length + CELEBORN_BATCH_HEADER_BYTES;
    PushReservation reservation = null;
    boolean registered = false;
    boolean submitted = false;
    Throwable pushFailure = null;
    try {
      reservation = claimEncodingReservation(requestBytes, length);
      final Object pushState =
          getPushState.invoke(shuffleClient, shuffleId + "-" + mapId + "-" + encodedAttemptId);
      ObservedPushState observedPushState = observePushState(pushState);

      // doPush=true sends this complete frame immediately. skipCompress=true preserves the
      // existing Comet frame, which already owns its compression and framing format.
      beginClientPush(reservation, observedPushState);
      registered = true;
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
                    bytes,
                    0,
                    length,
                    numMappers,
                    numPartitions,
                    true,
                    true);
        submitted = accepted > 0;

        if (submitted) {
          // The native frame and JNI payload overlap Celeborn's copied request only until its
          // raw-push call returns. Completion callbacks can run inline inside that call, so keep
          // all three copies charged while the reservation remains unsubmitted.
          shrinkSubmittedReservation(reservation, requestBytes);
        }

        if (submitted && isAborted()) {
          // Cleanup removes the map entry, but Celeborn can still submit using the PushState it
          // already captured. A side-effect-free lookup distinguishes that state from a genuine
          // post-cleanup replacement without creating an empty state and losing the live request.
          Object resumedPushState =
              ((Map<?, ?>) clientPushStates.get(shuffleClient))
                  .get(shuffleId + "-" + mapId + "-" + encodedAttemptId);
          if (resumedPushState != null && resumedPushState != pushState) {
            observedPushState = observePushState(resumedPushState);
            recoverUnobservedFailure(observedPushState);
          }
        }
        if (submitted) {
          acceptClientPush(reservation, observedPushState);
        }
      } finally {
        endClientPush();
      }

      if (isAborted()) {
        throw new IOException("Celeborn shuffle map attempt was aborted during its push");
      }

      int expected = requestBytes;
      if (accepted != expected) {
        throw new IOException(
            "Celeborn raw shuffle push accepted "
                + accepted
                + " bytes; expected "
                + expected
                + " including its transport header");
      }

      partitionLengths.addAndGet(partitionId, length);

      // ShufflePartitionPusher reports Comet payload bytes, never Celeborn transport bytes.
      return length;
    } catch (IllegalAccessException e) {
      IOException failure = new IOException("Cannot invoke the public Celeborn raw-push API", e);
      pushFailure = failure;
      abortAndSuppress(failure);
      throw failure;
    } catch (InvocationTargetException e) {
      Throwable failure = unwrapFailure("Celeborn raw shuffle push failed", e);
      pushFailure = failure;
      abortAndSuppress(failure);
      throwFailure(failure);
      throw new AssertionError("unreachable");
    } catch (IOException | RuntimeException | Error failure) {
      pushFailure = failure;
      abortAndSuppress(failure);
      throw failure;
    } finally {
      if (reservation != null && !submitted) {
        if (registered) {
          releaseUnsubmittedPush(reservation);
        } else {
          admission.release(reservation.bytes);
        }
      }
      try {
        endPush();
      } catch (IOException cleanupFailure) {
        if (pushFailure == null) {
          throw cleanupFailure;
        }
        if (cleanupFailure != pushFailure) {
          pushFailure.addSuppressed(cleanupFailure);
        }
      }
    }
  }

  /**
   * Drain all asynchronous Celeborn pushes, commit this map attempt, and return its payload sizes.
   *
   * <p>All native frames use {@code doPush=true}, so there is no merge buffer to flush. The
   * deployed Celeborn client's {@code mapperEnd} waits for every in-flight push, propagates any
   * asynchronous failure, and reports map completion to its lifecycle manager.
   */
  public long[] finish() throws IOException {
    try {
      synchronized (lifecycleLock) {
        if (state == State.FINISHED) {
          return snapshotPartitionLengths();
        }
        if (state != State.OPEN) {
          throw new IOException("Celeborn shuffle map attempt is not available for completion");
        }
        state = State.FINISHING;
        while ((activePushes != 0 || activeEncoders != 0) && state == State.FINISHING) {
          lifecycleLock.wait();
        }
        if (state != State.FINISHING) {
          throw new IOException("Celeborn shuffle map attempt was aborted before completion");
        }
      }

      synchronized (lifecycleLock) {
        if (state != State.FINISHING) {
          throw new IOException("Celeborn shuffle map attempt was aborted before mapperEnd");
        }
        mapperEndThread = Thread.currentThread();
      }
      try {
        if (mapperEnd.getParameterCount() == 5) {
          mapperEnd.invoke(
              shuffleClient, shuffleId, mapId, encodedAttemptId, numMappers, numPartitions);
        } else {
          mapperEnd.invoke(shuffleClient, shuffleId, mapId, encodedAttemptId, numMappers);
        }
      } finally {
        synchronized (lifecycleLock) {
          mapperEndThread = null;
        }
      }

      synchronized (lifecycleLock) {
        if (state != State.FINISHING) {
          throw new IOException("Celeborn shuffle map attempt was aborted during completion");
        }
        state = State.FINISHED;
        lifecycleLock.notifyAll();
      }
      return snapshotPartitionLengths();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      IOException failure =
          new IOException("Interrupted while draining Celeborn shuffle pushes", e);
      abortAndSuppress(failure);
      throw failure;
    } catch (IllegalAccessException e) {
      IOException failure = new IOException("Cannot invoke the public Celeborn mapperEnd API", e);
      abortAndSuppress(failure);
      throw failure;
    } catch (InvocationTargetException e) {
      Throwable failure = unwrapFailure("Celeborn shuffle map completion failed", e);
      abortAndSuppress(failure);
      throwFailure(failure);
      throw new AssertionError("unreachable");
    } catch (IOException | RuntimeException | Error failure) {
      abortAndSuppress(failure);
      throw failure;
    }
  }

  /** Cancel this attempt and wake any Celeborn push blocked on transport backpressure. */
  public void abort() throws IOException {
    final boolean shouldCleanup;
    final Thread completionThread;
    synchronized (lifecycleLock) {
      if (state != State.FINISHED) {
        state = State.ABORTED;
      }
      shouldCleanup = !cleanupStarted;
      if (shouldCleanup) {
        cleanupAfterActivePushes = activeClientPushes > 0;
      }
      cleanupStarted = true;
      completionThread = mapperEndThread;
      lifecycleLock.notifyAll();
    }
    if (completionThread != null && completionThread != Thread.currentThread()) {
      completionThread.interrupt();
    }
    if (!shouldCleanup) {
      return;
    }

    cleanupAttempt();
  }

  private void cleanupAttempt() throws IOException {
    synchronized (cleanupLock) {
      try {
        cleanup.invoke(shuffleClient, shuffleId, mapId, encodedAttemptId);
      } catch (IllegalAccessException e) {
        throw new IOException("Cannot invoke the public Celeborn cleanup API", e);
      } catch (InvocationTargetException e) {
        throwFailure(unwrapFailure("Celeborn shuffle map cleanup failed", e));
      }
    }
  }

  private void beginPush() throws IOException {
    synchronized (lifecycleLock) {
      if (state != State.OPEN) {
        throw new IOException("Celeborn shuffle map attempt no longer accepts partition data");
      }
      activePushes++;
    }
  }

  private PushReservation claimEncodingReservation(int requestBytes, int frameBytes)
      throws IOException {
    PushReservation reservation = encodingReservation.get();
    if (reservation != null) {
      final long overlappingBytes = 3L * frameBytes + (long) CELEBORN_BATCH_HEADER_BYTES;
      if (overlappingBytes > reservation.bytes) {
        throw new IOException("Celeborn shuffle push exceeds its native encoding reservation");
      }
      encodingReservation.remove();
      synchronized (lifecycleLock) {
        activeEncoders--;
        lifecycleLock.notifyAll();
      }
      if (overlappingBytes < reservation.bytes) {
        admission.release(reservation.bytes - (int) overlappingBytes);
        return new PushReservation((int) overlappingBytes);
      }
      return reservation;
    }

    admission.acquire(requestBytes, this::isAborted);
    return new PushReservation(requestBytes);
  }

  private void shrinkSubmittedReservation(PushReservation reservation, int requestBytes) {
    final int releasedBytes;
    synchronized (lifecycleLock) {
      releasedBytes = reservation.bytes - requestBytes;
      reservation.bytes = requestBytes;
    }
    if (releasedBytes > 0) {
      admission.release(releasedBytes);
    }
  }

  private ObservedPushState observePushState(Object pushState)
      throws IllegalAccessException, InvocationTargetException {
    synchronized (lifecycleLock) {
      ObservedPushState observed = observedPushStates.get(pushState);
      if (observed != null) {
        return observed;
      }

      final Object tracker = inFlightRequestTracker.get(pushState);
      final ObservedPushState created =
          new ObservedPushState(
              (LongAdder) totalInFlightRequests.get(tracker),
              (AtomicReference<?>) pushStateException.get(pushState));
      if (setPushMetricsCallback != null) {
        Object callback =
            Proxy.newProxyInstance(
                pushMetricsCallbackClass.getClassLoader(),
                new Class<?>[] {pushMetricsCallbackClass},
                (proxy, method, arguments) -> {
                  if (method.getName().equals("incPushDataCount")) {
                    boolean metricsOnlyFailure =
                        created.inFlightRequests.sum() > 0 && isTerminalFailureCallback();
                    completeAcceptedPushes(created, (long) arguments[0], metricsOnlyFailure);
                  } else if (method.getName().equals("hashCode")) {
                    return System.identityHashCode(proxy);
                  } else if (method.getName().equals("equals")) {
                    return proxy == arguments[0];
                  } else if (method.getName().equals("toString")) {
                    return "Celeborn native shuffle push completion callback";
                  }
                  return null;
                });
        setPushMetricsCallback.invoke(pushState, callback);
      }
      created.failedBeforeObservation = created.exception.get() != null;
      observedPushStates.put(pushState, created);
      return created;
    }
  }

  private void recoverUnobservedFailure(ObservedPushState pushState) {
    synchronized (lifecycleLock) {
      if (pushState.failedBeforeObservation && !pushState.recoveredMissedFailure) {
        // A recreated state's inline terminal failure used Celeborn's NOOP metrics callback. The
        // pinned failure path leaves one tracker batch behind but records its public exception.
        pushState.metricsOnlyFailures++;
        pushState.recoveredMissedFailure = true;
      }
    }
  }

  private void beginClientPush(PushReservation reservation, ObservedPushState pushState)
      throws IOException {
    synchronized (lifecycleLock) {
      if (state == State.ABORTED) {
        throw new IOException("Celeborn shuffle map attempt was aborted before its client push");
      }
      activeClientPushes++;
      reservation.pushState = pushState;
      pendingPushes.addLast(reservation);
      ensureCompletionReconciliation();
    }
  }

  private void acceptClientPush(PushReservation reservation, ObservedPushState pushState) {
    synchronized (lifecycleLock) {
      reservation.pushState = pushState;
      reservation.submitted = true;
      pushState.submittedPushes++;
    }
    reconcileAcceptedPushes();
  }

  private static boolean isTerminalFailureCallback() {
    // In the pinned raw-push client only onFailure emits metrics without removing its batch.
    // Skip the stack walk for the common final successful request, whose tracker is already zero.
    return COMPLETION_CALL_STACK.walk(
        frames ->
            frames
                .filter(
                    frame ->
                        frame.getMethodName().equals("onFailure")
                            || frame.getMethodName().equals("onSuccess"))
                .findFirst()
                .map(frame -> frame.getMethodName().equals("onFailure"))
                .orElse(false));
  }

  private void completeAcceptedPushes(
      ObservedPushState pushState, long completed, boolean metricsOnlyFailure) {
    synchronized (lifecycleLock) {
      if (completed > 0) {
        pushState.metricsCompletions += completed;
        if (metricsOnlyFailure) {
          pushState.metricsOnlyFailures += completed;
        }
      }
    }
    reconcileAcceptedPushes();
  }

  private void ensureCompletionReconciliation() {
    if (completionReconciliation == null || completionReconciliation.isDone()) {
      completionReconciliation =
          COMPLETION_RECONCILER.scheduleWithFixedDelay(
              this::reconcileAcceptedPushes,
              COMPLETION_RECONCILIATION_INTERVAL_MILLIS,
              COMPLETION_RECONCILIATION_INTERVAL_MILLIS,
              TimeUnit.MILLISECONDS);
    }
  }

  /**
   * Celeborn's MAP_ENDED retry paths remove their transport batch without invoking its metrics
   * callback. The pinned tracker preserves its in-flight request count even after cleanup, so the
   * actual count also reconciles silent completions without releasing cancelled live requests.
   */
  private void reconcileAcceptedPushes() {
    final ArrayList<PushReservation> completed = new ArrayList<>();
    synchronized (lifecycleLock) {
      for (ObservedPushState pushState : observedPushStates.values()) {
        if (setPushMetricsCallback == null
            && (state == State.OPEN || state == State.FINISHING)
            && pushState.exception.get() != null
            && !pushState.recoveredMissedFailure) {
          // Apache Celeborn records a terminal push failure without removing its transport batch.
          // Its first exception proves exactly one request completed unsuccessfully. Cancellation
          // also installs an exception, so never infer completion after this attempt was aborted.
          pushState.metricsOnlyFailures++;
          pushState.recoveredMissedFailure = true;
        }
        long transportRequests =
            Math.max(0L, pushState.inFlightRequests.sum() - pushState.metricsOnlyFailures);
        long trackerCompletions = Math.max(0L, pushState.submittedPushes - transportRequests);
        long knownCompletions =
            hasUnsubmittedReservation(pushState)
                ? trackerCompletions
                : Math.max(pushState.metricsCompletions, trackerCompletions);
        while (pushState.releasedPushes < knownCompletions) {
          PushReservation reservation = smallestPendingReservation(pushState);
          if (reservation == null) {
            break;
          }
          pendingPushes.remove(reservation);
          pushState.releasedPushes++;
          completed.add(reservation);
        }
      }

      if (pendingPushes.isEmpty() && activeClientPushes == 0) {
        stopCompletionReconciliation();
      }
    }
    for (PushReservation reservation : completed) {
      admission.release(reservation.bytes);
    }
  }

  private PushReservation smallestPendingReservation(ObservedPushState pushState) {
    // Metrics report a completion count without its batch identity. Releasing the smallest
    // eligible reservation never restores more bytes than the request that actually completed.
    PushReservation smallest = null;
    for (PushReservation reservation : pendingPushes) {
      if (reservation.pushState == pushState
          && reservation.submitted
          && (smallest == null || reservation.bytes < smallest.bytes)) {
        smallest = reservation;
      }
    }
    return smallest;
  }

  private boolean hasUnsubmittedReservation(ObservedPushState pushState) {
    for (PushReservation reservation : pendingPushes) {
      if (reservation.pushState == pushState && !reservation.submitted) {
        return true;
      }
    }
    return false;
  }

  private void stopCompletionReconciliation() {
    if (completionReconciliation != null) {
      completionReconciliation.cancel(false);
      completionReconciliation = null;
    }
  }

  private void releaseUnsubmittedPush(PushReservation reservation) {
    final boolean pending;
    synchronized (lifecycleLock) {
      pending = pendingPushes.remove(reservation);
      if (pendingPushes.isEmpty() && activeClientPushes == 0) {
        stopCompletionReconciliation();
      }
    }
    if (pending) {
      admission.release(reservation.bytes);
    }
  }

  private void endClientPush() {
    synchronized (lifecycleLock) {
      activeClientPushes--;
      if (pendingPushes.isEmpty() && activeClientPushes == 0) {
        stopCompletionReconciliation();
      }
    }
  }

  private void endPush() throws IOException {
    final boolean cleanupAfterPush;
    synchronized (lifecycleLock) {
      activePushes--;
      if (activePushes == 0) {
        lifecycleLock.notifyAll();
      }
      cleanupAfterPush =
          activePushes == 0
              && state == State.ABORTED
              && cleanupAfterActivePushes
              && !finalCleanupStarted;
      if (cleanupAfterPush) {
        finalCleanupStarted = true;
      }
    }
    if (cleanupAfterPush) {
      cleanupAttempt();
    }
  }

  private boolean isAborted() {
    synchronized (lifecycleLock) {
      return state == State.ABORTED;
    }
  }

  public int numPartitions() {
    return numPartitions;
  }

  /** Largest native frame whose overlapping native, JNI, and Celeborn copies fit admission. */
  public int maxFrameBytes() {
    return maxFrameBytes;
  }

  private long[] snapshotPartitionLengths() {
    long[] sizes = new long[numPartitions];
    for (int partition = 0; partition < numPartitions; partition++) {
      sizes[partition] = partitionLengths.get(partition);
    }
    return sizes;
  }

  private void abortAndSuppress(Throwable original) {
    try {
      // A stock client can report terminal failure synchronously without a completion callback.
      // Reconcile its exception before abort() marks the attempt cancelled and hides that signal.
      reconcileAcceptedPushes();
      abort();
    } catch (Throwable cleanupFailure) {
      if (cleanupFailure != original) {
        original.addSuppressed(cleanupFailure);
      }
    }
  }

  private static Throwable unwrapFailure(String message, InvocationTargetException failure) {
    Throwable cause = failure.getCause();
    return cause instanceof IOException
            || cause instanceof RuntimeException
            || cause instanceof Error
        ? cause
        : new IOException(message, cause);
  }

  private static void throwFailure(Throwable failure) throws IOException {
    if (failure instanceof IOException) {
      throw (IOException) failure;
    }
    if (failure instanceof RuntimeException) {
      throw (RuntimeException) failure;
    }
    throw (Error) failure;
  }
}
