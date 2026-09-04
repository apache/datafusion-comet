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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

/** Adapts complete Comet shuffle frames to an existing, task-owned Celeborn shuffle client. */
public final class CelebornShufflePartitionPusher implements ShufflePartitionPusher {

  private static final int CELEBORN_BATCH_HEADER_BYTES = 4 * Integer.BYTES;
  private static final int MINIMUM_COMET_FRAME_BYTES = 2 * Long.BYTES;
  private static final int MAX_JVM_ARRAY_BYTES = Integer.MAX_VALUE - 8;
  private static final int DEFAULT_MAX_IN_FLIGHT_BYTES = 256 * 1024 * 1024;
  private static final long RECONCILIATION_INTERVAL_MILLIS = 10;

  // This daemon never owns client state: reconciliations are cancelled and removed once the
  // associated task's transport requests complete.
  private static final ScheduledThreadPoolExecutor COMPLETION_RECONCILER =
      createCompletionReconciler();

  private final Object shuffleClient;
  private final Method pushOrMergeData;
  private final Method computeBatchCRC;
  private final Method mapperEnd;
  private final Method cleanup;
  private final Method getPushState;
  private final Field clientPushStates;
  private final Field inFlightRequestTracker;
  private final Field totalInFlightRequests;
  private final Field pushStateException;
  private final Field cryptoHandler;
  private final ExecutorShufflePushAdmission admission;
  private final CelebornTransportCallbackTracker transportCallbacks;
  private final Object submissionLock = new Object();
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
  private Thread activeClientThread;
  private Thread mapperEndThread;
  private boolean cleanupRequested;
  private boolean cleanupClaimed;
  private IOException asynchronousFailure;

  private enum State {
    OPEN,
    FINISHING,
    FINISHED,
    ABORTED
  }

  private static final class PushReservation {
    private final int bytes;
    private ObservedPushState pushState;
    private CelebornTransportCallbackTracker.Push transportPush;
    private Object clientPushState;
    private boolean claimed;
    private boolean submitted;
    private boolean nativeReleased;
    private boolean transportComplete;
    private boolean released;
    private boolean pushStateFallback;

    private PushReservation(int bytes, boolean nativeOwned) {
      this.bytes = bytes;
      this.nativeReleased = !nativeOwned;
    }
  }

  private static final class ObservedPushState {
    private final LongAdder inFlightRequests;
    private final AtomicReference<?> exception;
    private long fallbackSubmittedPushes;
    private long fallbackReleasedPushes;

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
    this(
        shuffleClient,
        shuffleId,
        mapId,
        encodedAttemptId,
        numMappers,
        numPartitions,
        MAX_JVM_ARRAY_BYTES,
        DEFAULT_MAX_IN_FLIGHT_BYTES);
  }

  /** Binds one task to byte admission shared by its executor-side Celeborn client. */
  public CelebornShufflePartitionPusher(
      Object shuffleClient,
      int shuffleId,
      int mapId,
      int encodedAttemptId,
      int numMappers,
      int numPartitions,
      int maxInFlightBytes) {
    this(
        shuffleClient,
        shuffleId,
        mapId,
        encodedAttemptId,
        numMappers,
        numPartitions,
        MAX_JVM_ARRAY_BYTES,
        maxInFlightBytes);
  }

  /** Binds one task to independently configured complete-frame and executor byte limits. */
  public CelebornShufflePartitionPusher(
      Object shuffleClient,
      int shuffleId,
      int mapId,
      int encodedAttemptId,
      int numMappers,
      int numPartitions,
      int configuredMaxFrameBytes,
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
    if (configuredMaxFrameBytes < MINIMUM_COMET_FRAME_BYTES) {
      throw new IllegalArgumentException("Celeborn maximum frame size must fit one Comet frame");
    }
    if (maxInFlightBytes < 3 * MINIMUM_COMET_FRAME_BYTES + CELEBORN_BATCH_HEADER_BYTES) {
      throw new IllegalArgumentException(
          "Celeborn in-flight byte limit must fit all copies of one Comet frame");
    }

    String unavailable = nativePushCompletionUnavailableReason(shuffleClient.getClass());
    if (unavailable != null) {
      // Do not silently use PushState counters: callbacks, retries and timed-out writes can
      // still retain payloads after those counters report completion.
      throw new UnsupportedOperationException(unavailable);
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

    Method integrityMethod = null;
    try {
      integrityMethod =
          shuffleClient
              .getClass()
              .getMethod(
                  "computeBatchCRC",
                  int.class,
                  int.class,
                  int.class,
                  int.class,
                  byte[].class,
                  int.class,
                  int.class);
    } catch (NoSuchMethodException missing) {
      // Older Celeborn clients account for integrity inside pushOrMergeData instead.
      try {
        for (Method method : shuffleClient.getClass().getMethods()) {
          if (method.getName().equals("computeBatchCRC")) {
            throw new IllegalArgumentException(
                "Celeborn integrity-accounting API has an incompatible signature", missing);
          }
        }
      } catch (SecurityException failure) {
        throw new IllegalArgumentException(
            "Cannot inspect the optional Celeborn integrity-accounting API", failure);
      }
    } catch (SecurityException failure) {
      throw new IllegalArgumentException(
          "Cannot resolve the optional Celeborn integrity-accounting API", failure);
    }

    if (integrityMethod != null
        && (integrityMethod.getReturnType() != void.class
            || Modifier.isStatic(integrityMethod.getModifiers()))) {
      throw new IllegalArgumentException(
          "Celeborn integrity-accounting API must be an instance method returning void");
    }

    Method mapperEndMethod =
        optionalLifecycleMethod(
            shuffleClient, "mapperEnd", int.class, int.class, int.class, int.class, int.class);
    if (mapperEndMethod == null) {
      mapperEndMethod =
          optionalLifecycleMethod(
              shuffleClient, "mapperEnd", int.class, int.class, int.class, int.class);
    }
    Method cleanupMethod =
        optionalLifecycleMethod(shuffleClient, "cleanup", int.class, int.class, int.class);

    Method pushStateMethod = null;
    Field pushStatesField = null;
    Field trackerField = null;
    Field requestCountField = null;
    Field exceptionField = null;
    try {
      pushStateMethod = shuffleClient.getClass().getMethod("getPushState", String.class);
      if (Modifier.isStatic(pushStateMethod.getModifiers())) {
        throw new NoSuchMethodException("getPushState must be an instance method");
      }
      Class<?> pushStateClass = pushStateMethod.getReturnType();
      trackerField = declaredField(pushStateClass, "inFlightRequestTracker");
      requestCountField = declaredField(trackerField.getType(), "totalInflightReqs");
      exceptionField = declaredField(pushStateClass, "exception");
      if (requestCountField.getType() != LongAdder.class
          || exceptionField.getType() != AtomicReference.class) {
        throw new NoSuchFieldException("Celeborn PushState transport tracker is incompatible");
      }
      trackerField.setAccessible(true);
      requestCountField.setAccessible(true);
      exceptionField.setAccessible(true);
      try {
        pushStatesField = declaredField(shuffleClient.getClass(), "pushStates");
        if (Map.class.isAssignableFrom(pushStatesField.getType())
            && !Modifier.isStatic(pushStatesField.getModifiers())) {
          pushStatesField.setAccessible(true);
        } else {
          pushStatesField = null;
        }
      } catch (NoSuchFieldException ignored) {
        // Wrappers need not provide the optional side-effect-free state lookup.
      }
    } catch (NoSuchMethodException missing) {
      // Compatibility-only clients can still push synchronously. Asynchronous clients must
      // expose getPushState in addition to safely published transport hooks.
      pushStateMethod = null;
      pushStatesField = null;
      trackerField = null;
      requestCountField = null;
      exceptionField = null;
    } catch (ReflectiveOperationException | RuntimeException failure) {
      throw new IllegalArgumentException(
          "Celeborn shuffle client does not provide observable completion-backed push admission",
          failure);
    }

    this.shuffleClient = shuffleClient;
    this.pushOrMergeData = pushMethod;
    this.computeBatchCRC = integrityMethod;
    this.mapperEnd = mapperEndMethod;
    this.cleanup = cleanupMethod;
    this.getPushState = pushStateMethod;
    this.clientPushStates = pushStatesField;
    this.inFlightRequestTracker = trackerField;
    this.totalInFlightRequests = requestCountField;
    this.pushStateException = exceptionField;
    this.cryptoHandler = optionalCryptoHandler(shuffleClient);
    requireUnencryptedClient();
    this.admission = ExecutorShufflePushAdmission.forClient(shuffleClient, maxInFlightBytes);
    this.transportCallbacks =
        getPushState == null ? null : admission.transportCallbacks(shuffleClient);
    this.shuffleId = shuffleId;
    this.mapId = mapId;
    this.encodedAttemptId = encodedAttemptId;
    this.numMappers = numMappers;
    this.numPartitions = numPartitions;
    this.maxReservationBytes = maxInFlightBytes - CELEBORN_BATCH_HEADER_BYTES;
    this.maxFrameBytes =
        Math.min(Math.min(configuredMaxFrameBytes, MAX_JVM_ARRAY_BYTES), maxReservationBytes / 3);
    this.partitionLengths = new AtomicLongArray(numPartitions);
  }

  /** Returns the reason an optional client cannot safely track native push completion, or null. */
  public static String nativePushCompletionUnavailableReason(Class<?> shuffleClientClass) {
    return CelebornTransportCallbackTracker.unavailableReason(shuffleClientClass);
  }

  private static Method optionalLifecycleMethod(
      Object shuffleClient, String name, Class<?>... parameterTypes) {
    final Method method;
    try {
      method = shuffleClient.getClass().getMethod(name, parameterTypes);
    } catch (NoSuchMethodException missing) {
      return null;
    } catch (SecurityException failure) {
      throw new IllegalArgumentException("Cannot resolve the Celeborn " + name + " API", failure);
    }
    if (method.getReturnType() != void.class || Modifier.isStatic(method.getModifiers())) {
      throw new IllegalArgumentException(
          "Celeborn " + name + " API must be an instance method returning void");
    }
    return method;
  }

  private static Field declaredField(Class<?> owner, String name) throws NoSuchFieldException {
    for (Class<?> current = owner; current != null; current = current.getSuperclass()) {
      try {
        return current.getDeclaredField(name);
      } catch (NoSuchFieldException missing) {
        // Application wrappers and compatibility clients can inherit these members.
      }
    }
    throw new NoSuchFieldException(owner.getName() + "." + name);
  }

  private static Field optionalCryptoHandler(Object client) {
    try {
      Field field = declaredField(client.getClass(), "cryptoHandler");
      if (Modifier.isStatic(field.getModifiers()) || field.getType() != Optional.class) {
        throw new IllegalArgumentException("Cannot inspect the Celeborn client's encryption state");
      }
      field.setAccessible(true);
      return field;
    } catch (NoSuchFieldException absent) {
      // Celeborn 0.6 has no encryption handler. Normal Spark construction is also guarded by
      // spark.io.encryption.enabled in the factory, including legacy client APIs.
      return null;
    }
  }

  private void requireUnencryptedClient() {
    if (cryptoHandler == null) {
      return;
    }
    try {
      Object handler = cryptoHandler.get(shuffleClient);
      if (!(handler instanceof Optional) || ((Optional<?>) handler).isPresent()) {
        // Stock SparkCryptoHandler retains a per-thread high-water buffer and creates additional
        // encrypted copies. It has no bounded-workspace contract. Reject before encoding or raw
        // submission; never remove the handler or downgrade the shared client's encryption.
        throw new UnsupportedOperationException(
            "Encrypted native Celeborn shuffle is not supported by bounded push admission; "
                + "use ordinary Spark shuffle with encryption enabled");
      }
    } catch (IllegalAccessException failure) {
      throw new IllegalStateException(
          "Cannot inspect the Celeborn client's encryption state", failure);
    }
  }

  @Override
  public void reservePartitionData(int maxLength) throws IOException {
    requireUnencryptedClient();
    if (maxLength <= 0 || maxLength > maxReservationBytes) {
      throw new IOException("Celeborn native frame reservation exceeds its byte limit");
    }
    if (encodingReservation.get() != null) {
      throw new IOException("Celeborn native frame already has a reservation on this thread");
    }

    int requestBytes = maxLength + CELEBORN_BATCH_HEADER_BYTES;
    admission.acquire(requestBytes, this::isAborted);
    synchronized (lifecycleLock) {
      if (state != State.OPEN) {
        admission.release(requestBytes);
        throw new IOException("Celeborn shuffle map attempt no longer accepts frame encoding");
      }
      activeEncoders++;
      encodingReservation.set(new PushReservation(requestBytes, true));
    }
  }

  @Override
  public void releasePartitionDataReservation() {
    PushReservation reservation = encodingReservation.get();
    if (reservation == null) {
      return;
    }
    encodingReservation.remove();
    int released;
    synchronized (lifecycleLock) {
      reservation.nativeReleased = true;
      if (!reservation.claimed) {
        // Encoding failed (or was split) before entering the Java push path.
        reservation.transportComplete = true;
      }
      released = releasableBytes(reservation);
      activeEncoders--;
      lifecycleLock.notifyAll();
    }
    admission.release(released);
  }

  /** Sends exactly one complete, already-compressed Comet shuffle frame to Celeborn. */
  @Override
  public void pushPartitionData(int partitionId, byte[] data, int length) throws IOException {
    requireUnencryptedClient();
    validateFrame(partitionId, data, length);
    beginPush();
    PushReservation reservation;
    try {
      // Admission can wait for an older request's completion. Never hold the submission lock
      // while waiting; callbacks and retries must remain able to make progress.
      reservation = claimEncodingReservation(length);
    } catch (IOException | RuntimeException | Error failure) {
      abortAndSuppress(failure);
      try {
        endPush();
      } catch (IOException cleanupFailure) {
        if (cleanupFailure != failure) {
          failure.addSuppressed(cleanupFailure);
        }
      }
      throw failure;
    }
    synchronized (submissionLock) {
      pushClaimedPartitionData(partitionId, data, length, reservation);
    }
  }

  private void pushClaimedPartitionData(
      int partitionId, byte[] data, int length, PushReservation reservation) throws IOException {
    boolean registered = false;
    boolean submitted = false;
    boolean insideClient = false;
    Throwable failure = null;
    try {
      beginClientPush();
      insideClient = true;
      awaitPushStateFallbackSlot();

      if (computeBatchCRC != null) {
        computeBatchCRC.invoke(
            shuffleClient, shuffleId, mapId, encodedAttemptId, partitionId, data, 0, length);
      }

      Object pushState = null;
      ObservedPushState observed = null;
      if (getPushState != null) {
        pushState = getPushState.invoke(shuffleClient, mapKey());
        if (pushState == null) {
          throw new IOException("Celeborn returned a null push state for this map attempt");
        }
        observed = observePushState(pushState);
        registerPendingPush(reservation, pushState, observed);
        registered = true;
      }

      int accepted;
      try (CelebornTransportCallbackTracker.Push transportPush =
          transportCallbacks == null ? null : transportCallbacks.beginPush()) {
        synchronized (lifecycleLock) {
          reservation.transportPush = transportPush;
          if (state == State.ABORTED) {
            throw new IOException(
                "Celeborn shuffle map attempt was aborted before its client invocation");
          }
        }
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
      }
      submitted = accepted > 0;
      if (submitted && observed != null) {
        if (clientPushStates != null) {
          Object current = ((Map<?, ?>) clientPushStates.get(shuffleClient)).get(mapKey());
          if (current != null && current != pushState) {
            pushState = current;
            observed = observePushState(current);
          }
        }
        markSubmitted(reservation, pushState, observed);
      }

      int minimumAccepted = length + CELEBORN_BATCH_HEADER_BYTES;
      if (accepted < minimumAccepted) {
        throw new IOException(
            "Celeborn raw shuffle push accepted "
                + accepted
                + " bytes; expected at least "
                + minimumAccepted
                + " including its transport header");
      }
      if (accepted > reservation.bytes) {
        throw new IOException(
            "Celeborn encrypted shuffle request exceeds its reserved in-flight byte limit");
      }
      throwIfAsyncFailure();
      if (isAborted()) {
        throw new IOException("Celeborn shuffle map attempt was aborted during its push");
      }
      partitionLengths.addAndGet(partitionId, length);
    } catch (IllegalAccessException cause) {
      failure = new IOException("Cannot invoke the public Celeborn raw-push API", cause);
      abortAndSuppress(failure);
      throw (IOException) failure;
    } catch (InvocationTargetException cause) {
      failure = unwrapFailure("Celeborn raw shuffle push failed", cause);
      abortAndSuppress(failure);
      throwFailure(failure);
      throw new AssertionError("unreachable");
    } catch (IOException | RuntimeException | Error cause) {
      failure = cause;
      abortAndSuppress(cause);
      throw cause;
    } finally {
      if (insideClient) {
        endClientPush();
      }
      IOException deferredCleanupFailure = null;
      if (reservation != null) {
        try {
          if (registered && !submitted) {
            releaseUnsubmittedPush(reservation);
          } else if (!registered) {
            completeTransport(reservation);
          }
        } catch (IOException cleanupFailure) {
          if (failure == null) {
            deferredCleanupFailure = cleanupFailure;
          } else if (cleanupFailure != failure) {
            failure.addSuppressed(cleanupFailure);
          }
        }
      }
      try {
        endPush();
      } catch (IOException cleanupFailure) {
        if (failure == null) {
          if (deferredCleanupFailure == null) {
            deferredCleanupFailure = cleanupFailure;
          } else if (cleanupFailure != deferredCleanupFailure) {
            deferredCleanupFailure.addSuppressed(cleanupFailure);
          }
        } else if (cleanupFailure != failure) {
          failure.addSuppressed(cleanupFailure);
        }
      }
      if (deferredCleanupFailure != null) {
        throw deferredCleanupFailure;
      }
    }
  }

  private void validateFrame(int partitionId, byte[] data, int length) throws IOException {
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
    if (length > maxFrameBytes) {
      throw new IOException("Celeborn shuffle frame exceeds its configured maximum frame size");
    }

    long declaredBodyLength = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getLong();
    if (declaredBodyLength != (long) length - Long.BYTES) {
      throw new IOException(
          "Celeborn shuffle frame declares "
              + declaredBodyLength
              + " body bytes, but contains "
              + (length - Long.BYTES));
    }
  }

  private PushReservation claimEncodingReservation(int frameBytes) throws IOException {
    int required = Math.addExact(Math.multiplyExact(frameBytes, 3), CELEBORN_BATCH_HEADER_BYTES);
    PushReservation reservation = encodingReservation.get();
    if (reservation != null) {
      if (required > reservation.bytes) {
        throw new IOException("Celeborn shuffle push exceeds its native encoding reservation");
      }
      synchronized (lifecycleLock) {
        if (reservation.claimed) {
          throw new IOException("Celeborn native frame reservation has already been submitted");
        }
        reservation.claimed = true;
      }
      // The native Vec and JNI array still exist after this method and the Java push return.
      // Keep the entire bound until native explicitly acknowledges their retirement. In
      // particular, a fast network callback must not free bytes still owned by the encoder.
      return reservation;
    }
    admission.acquire(required, this::isAborted);
    return new PushReservation(required, false);
  }

  private ObservedPushState observePushState(Object pushState) throws IllegalAccessException {
    synchronized (lifecycleLock) {
      ObservedPushState observed = observedPushStates.get(pushState);
      if (observed != null) {
        return observed;
      }
      Object tracker = inFlightRequestTracker.get(pushState);
      AtomicReference<?> exception = (AtomicReference<?>) pushStateException.get(pushState);
      ObservedPushState created =
          new ObservedPushState((LongAdder) totalInFlightRequests.get(tracker), exception);
      observedPushStates.put(pushState, created);
      return created;
    }
  }

  private void beginPush() throws IOException {
    synchronized (lifecycleLock) {
      if (asynchronousFailure != null) {
        throw asynchronousFailure;
      }
      if (state != State.OPEN) {
        throw new IOException("Celeborn shuffle map attempt no longer accepts partition data");
      }
      activePushes++;
    }
  }

  private void beginClientPush() throws IOException {
    synchronized (lifecycleLock) {
      if (state == State.ABORTED) {
        throw new IOException("Celeborn shuffle map attempt was aborted before its client push");
      }
      activeClientPushes++;
      activeClientThread = Thread.currentThread();
    }
  }

  private void awaitPushStateFallbackSlot() throws IOException {
    synchronized (lifecycleLock) {
      while (state != State.ABORTED && hasPendingPushStateFallback()) {
        try {
          lifecycleLock.wait();
        } catch (InterruptedException failure) {
          Thread.currentThread().interrupt();
          throw new IOException(
              "Interrupted while waiting for a prior Celeborn fallback push", failure);
        }
      }
      if (state == State.ABORTED) {
        throw new IOException("Celeborn shuffle map attempt was aborted before its client push");
      }
    }
  }

  private void registerPendingPush(
      PushReservation reservation, Object clientPushState, ObservedPushState pushState)
      throws IOException {
    synchronized (lifecycleLock) {
      if (state == State.ABORTED) {
        throw new IOException("Celeborn shuffle map attempt was aborted before submission");
      }
      reservation.clientPushState = clientPushState;
      reservation.pushState = pushState;
      pendingPushes.addLast(reservation);
      if (completionReconciliation == null || completionReconciliation.isDone()) {
        completionReconciliation =
            COMPLETION_RECONCILER.scheduleWithFixedDelay(
                this::safelyReconcileAcceptedPushes,
                RECONCILIATION_INTERVAL_MILLIS,
                RECONCILIATION_INTERVAL_MILLIS,
                TimeUnit.MILLISECONDS);
      }
    }
  }

  private void markSubmitted(
      PushReservation reservation, Object clientPushState, ObservedPushState pushState)
      throws IOException {
    synchronized (lifecycleLock) {
      reservation.clientPushState = clientPushState;
      reservation.pushState = pushState;
      reservation.submitted = true;
      registerPushStateFallback(reservation);
    }
    reconcileAcceptedPushes();
  }

  private void registerPushStateFallback(PushReservation reservation) {
    if (!reservation.pushStateFallback
        && (reservation.transportPush == null
            || !reservation.transportPush.usesTransportOwnership())) {
      reservation.pushStateFallback = true;
      reservation.pushState.fallbackSubmittedPushes++;
    }
  }

  // All reservation transitions are protected by lifecycleLock. Native retirement and transport
  // completion may arrive in either order; neither alone proves that the overlapping copies died.
  private int releasableBytes(PushReservation reservation) {
    if (!reservation.released && reservation.nativeReleased && reservation.transportComplete) {
      reservation.released = true;
      return reservation.bytes;
    }
    return 0;
  }

  private void completeTransport(PushReservation reservation) {
    int released;
    synchronized (lifecycleLock) {
      reservation.transportComplete = true;
      released = releasableBytes(reservation);
    }
    admission.release(released);
  }

  private void safelyReconcileAcceptedPushes() {
    try {
      reconcileAcceptedPushes();
    } catch (IOException | RuntimeException | Error cause) {
      IOException failure =
          cause instanceof IOException
              ? (IOException) cause
              : new IOException("Celeborn push completion reconciliation failed", cause);
      synchronized (lifecycleLock) {
        if (asynchronousFailure == null) {
          asynchronousFailure = failure;
        } else {
          failure = asynchronousFailure;
        }
      }
      abortAndSuppress(failure);
    }
  }

  private void reconcileAcceptedPushes() throws IOException {
    ArrayList<PushReservation> completed = new ArrayList<>();
    IOException detectedFailure = null;
    synchronized (lifecycleLock) {
      Iterator<PushReservation> pending = pendingPushes.iterator();
      while (pending.hasNext()) {
        PushReservation reservation = pending.next();
        if (!reservation.submitted) {
          continue;
        }
        registerPushStateFallback(reservation);
        if (!reservation.pushStateFallback && reservation.transportPush.isComplete()) {
          pending.remove();
          completed.add(reservation);
        }
      }
      for (ObservedPushState observed : observedPushStates.values()) {
        Object failure = observed.exception.get();
        boolean cleanedUp =
            failure instanceof IOException
                && "Cleaned Up".equals(((IOException) failure).getMessage());
        if (failure instanceof IOException && !cleanedUp) {
          IOException observedFailure = (IOException) failure;
          if (asynchronousFailure == null) {
            asynchronousFailure = observedFailure;
            detectedFailure = asynchronousFailure;
          }
        }
        if (hasPendingTransportOwnership(observed)) {
          // A stock counter cannot distinguish a fallback request from a transport-owned request.
          // Wait for precise requests on this push state before crediting fallback completions.
          continue;
        }
        boolean terminalFailure =
            failure instanceof IOException
                && !cleanedUp
                && isTerminalRawPushFailure((IOException) failure);
        long counterCredits = terminalFailure ? 1L : 0L;
        long retainedRequests = Math.max(0L, observed.inFlightRequests.sum() - counterCredits);
        long completions = Math.max(0L, observed.fallbackSubmittedPushes - retainedRequests);
        while (observed.fallbackReleasedPushes < completions) {
          PushReservation reservation = smallestFallbackReservation(observed);
          if (reservation == null
              || (reservation.transportPush != null
                  && !reservation.transportPush.retainedTransportOwnershipComplete())) {
            break;
          }
          pendingPushes.remove(reservation);
          observed.fallbackReleasedPushes++;
          completed.add(reservation);
        }
      }
      if (pendingPushes.isEmpty() && activeClientPushes == 0) {
        stopCompletionReconciliation();
      }
      if (!completed.isEmpty()) {
        lifecycleLock.notifyAll();
      }
    }
    for (PushReservation reservation : completed) {
      completeTransport(reservation);
    }
    if (detectedFailure != null) {
      abortAndSuppress(detectedFailure);
    }
    performDeferredCleanupIfReady();
  }

  private static boolean isTerminalRawPushFailure(IOException failure) {
    String message = failure.getMessage();
    return message != null
        && message.startsWith("Push data to ")
        && message.contains(" failed for shuffle ")
        && message.contains(" batch ");
  }

  private boolean hasPendingTransportOwnership(ObservedPushState observed) {
    for (PushReservation reservation : pendingPushes) {
      if (reservation.pushState == observed
          && reservation.submitted
          && !reservation.pushStateFallback) {
        return true;
      }
    }
    return false;
  }

  private PushReservation smallestFallbackReservation(ObservedPushState observed) {
    PushReservation smallest = null;
    for (PushReservation reservation : pendingPushes) {
      if (reservation.pushState == observed
          && reservation.submitted
          && reservation.pushStateFallback
          && (smallest == null || reservation.bytes < smallest.bytes)) {
        smallest = reservation;
      }
    }
    return smallest;
  }

  private void stopCompletionReconciliation() {
    if (completionReconciliation != null) {
      completionReconciliation.cancel(false);
      completionReconciliation = null;
    }
  }

  private void refreshPendingPushState(PushReservation reservation) throws IOException {
    if (clientPushStates == null) {
      return;
    }
    final Object current;
    try {
      Object states = clientPushStates.get(shuffleClient);
      if (!(states instanceof Map<?, ?>)) {
        throw new IOException("Celeborn returned incompatible push-state storage");
      }
      current = ((Map<?, ?>) states).get(mapKey());
      if (current == null || current == reservation.clientPushState) {
        return;
      }
      ObservedPushState observed = observePushState(current);
      synchronized (lifecycleLock) {
        reservation.clientPushState = current;
        reservation.pushState = observed;
      }
    } catch (IllegalAccessException failure) {
      throw new IOException("Cannot inspect Celeborn push-state storage", failure);
    }
  }

  private void releaseUnsubmittedPush(PushReservation reservation) throws IOException {
    // The raw call may recreate its PushState during cleanup before throwing. Resolve that state
    // again before deciding whether counter-backed completion owns the published request.
    refreshPendingPushState(reservation);
    boolean removed;
    synchronized (lifecycleLock) {
      if (reservation.transportPush != null
          && reservation.transportPush.usesTransportOwnership()
          && !reservation.transportPush.isComplete()) {
        // The raw call may throw after publishing transport or retry work. Keep its entire
        // reservation until that work returns, even though the call never reported acceptance.
        reservation.submitted = true;
        removed = false;
      } else if (reservation.transportPush == null
          || !reservation.transportPush.usesTransportOwnership()) {
        // The raw call may have published an uninstrumented request before throwing. Counter
        // reconciliation releases it immediately when no request was actually retained.
        reservation.submitted = true;
        registerPushStateFallback(reservation);
        removed = false;
      } else {
        removed = pendingPushes.remove(reservation);
      }
      if (pendingPushes.isEmpty() && activeClientPushes == 0) {
        stopCompletionReconciliation();
      }
    }
    if (removed) {
      completeTransport(reservation);
    }
  }

  private void endClientPush() {
    synchronized (lifecycleLock) {
      if (activeClientThread == Thread.currentThread()) {
        activeClientThread = null;
      }
      activeClientPushes--;
      if (pendingPushes.isEmpty() && activeClientPushes == 0) {
        stopCompletionReconciliation();
      }
    }
  }

  private void endPush() throws IOException {
    synchronized (lifecycleLock) {
      activePushes--;
      if (activePushes == 0) {
        lifecycleLock.notifyAll();
      }
    }
    performDeferredCleanupIfReady();
  }

  /** Drains asynchronous requests, commits this map, and returns Comet bytes per partition. */
  public long[] finish() throws IOException {
    try {
      synchronized (lifecycleLock) {
        if (asynchronousFailure != null) {
          throw asynchronousFailure;
        }
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
        if (asynchronousFailure != null) {
          throw asynchronousFailure;
        }
        if (state != State.FINISHING) {
          throw new IOException("Celeborn shuffle map attempt was aborted before completion");
        }
        if (mapperEnd == null) {
          throw new IOException(
              "Celeborn shuffle client does not provide the required public mapperEnd API");
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
      reconcileAcceptedPushes();
      throwIfAsyncFailure();

      synchronized (lifecycleLock) {
        if (state != State.FINISHING) {
          throw new IOException("Celeborn shuffle map attempt was aborted during completion");
        }
        state = State.FINISHED;
        lifecycleLock.notifyAll();
      }
      return snapshotPartitionLengths();
    } catch (InterruptedException cause) {
      Thread.currentThread().interrupt();
      IOException failure = new IOException("Interrupted while draining Celeborn pushes", cause);
      abortAndSuppress(failure);
      throw failure;
    } catch (IllegalAccessException cause) {
      IOException failure =
          new IOException("Cannot invoke the public Celeborn mapperEnd API", cause);
      abortAndSuppress(failure);
      throw failure;
    } catch (InvocationTargetException cause) {
      Throwable failure = unwrapFailure("Celeborn shuffle map completion failed", cause);
      abortAndSuppress(failure);
      throwFailure(failure);
      throw new AssertionError("unreachable");
    } catch (IOException | RuntimeException | Error failure) {
      abortAndSuppress(failure);
      throw failure;
    }
  }

  /** Cancels this map without releasing request bytes before actual transport completion. */
  public void abort() throws IOException {
    Thread completionThread;
    synchronized (lifecycleLock) {
      if (state == State.FINISHED) {
        return;
      }
      state = State.ABORTED;
      cleanupRequested = true;
      if (activeClientThread != null && activeClientThread != Thread.currentThread()) {
        // Interrupt while ownership is protected by lifecycleLock. Otherwise the raw call could
        // return and this Thread could resume unrelated work before the interrupt is delivered.
        activeClientThread.interrupt();
      }
      completionThread = mapperEndThread;
      lifecycleLock.notifyAll();
    }
    if (completionThread != null && completionThread != Thread.currentThread()) {
      completionThread.interrupt();
    }
    performDeferredCleanupIfReady();
  }

  private void performDeferredCleanupIfReady() throws IOException {
    boolean performCleanup;
    synchronized (lifecycleLock) {
      // Stock Celeborn cleanup records "Cleaned Up" in PushState. A later failure callback then
      // returns before retiring the stock request counter. Wait for raw calls to be classified and
      // for counter-only callbacks to complete. Seal exact owners against a later global downgrade
      // before cleanup prevents their callbacks and queued retries from invoking stock code.
      performCleanup =
          cleanupRequested
              && !cleanupClaimed
              && activePushes == 0
              && sealPendingTransportOwnershipForCleanup();
      if (performCleanup) {
        cleanupClaimed = true;
      }
    }
    if (performCleanup) {
      cleanupAttempt();
    }
  }

  private boolean hasPendingPushStateFallback() {
    for (PushReservation reservation : pendingPushes) {
      registerPushStateFallback(reservation);
      if (reservation.pushStateFallback) {
        return true;
      }
    }
    return false;
  }

  private boolean sealPendingTransportOwnershipForCleanup() {
    ArrayList<CelebornTransportCallbackTracker.Push> exactPushes = new ArrayList<>();
    for (PushReservation reservation : pendingPushes) {
      registerPushStateFallback(reservation);
      if (reservation.pushStateFallback || reservation.transportPush == null) {
        return false;
      }
      exactPushes.add(reservation.transportPush);
    }
    if (CelebornTransportCallbackTracker.Push.trySealCohortForCancellation(exactPushes)) {
      return true;
    }
    for (PushReservation reservation : pendingPushes) {
      registerPushStateFallback(reservation);
    }
    return false;
  }

  private void cleanupAttempt() throws IOException {
    if (cleanup == null) {
      return;
    }
    synchronized (cleanupLock) {
      try {
        cleanup.invoke(shuffleClient, shuffleId, mapId, encodedAttemptId);
      } catch (IllegalAccessException failure) {
        throw new IOException("Cannot invoke the public Celeborn cleanup API", failure);
      } catch (InvocationTargetException failure) {
        throwFailure(unwrapFailure("Celeborn shuffle map cleanup failed", failure));
      }
    }
  }

  private String mapKey() {
    return shuffleId + "-" + mapId + "-" + encodedAttemptId;
  }

  private boolean isAborted() {
    synchronized (lifecycleLock) {
      return state == State.ABORTED;
    }
  }

  private void throwIfAsyncFailure() throws IOException {
    synchronized (lifecycleLock) {
      if (asynchronousFailure != null) {
        throw asynchronousFailure;
      }
    }
  }

  public int numPartitions() {
    return numPartitions;
  }

  /** Largest configured frame whose native, JNI, and Celeborn copies fit shared admission. */
  @Override
  public int maxFrameBytes() {
    return maxFrameBytes;
  }

  @Override
  public int maxReservationBytes() {
    return maxReservationBytes;
  }

  private long[] snapshotPartitionLengths() {
    long[] sizes = new long[numPartitions];
    for (int partition = 0; partition < numPartitions; partition++) {
      sizes[partition] = partitionLengths.get(partition);
    }
    return sizes;
  }

  private void abortAndSuppress(Throwable failure) {
    try {
      abort();
    } catch (Throwable cleanupFailure) {
      if (cleanupFailure != failure) {
        failure.addSuppressed(cleanupFailure);
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
