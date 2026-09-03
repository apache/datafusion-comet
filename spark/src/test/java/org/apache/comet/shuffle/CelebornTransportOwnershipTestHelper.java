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
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;

/** Real Netty write-lifetime checks invoked by CelebornShufflePartitionPusherSuite. */
public final class CelebornTransportOwnershipTestHelper {
  private CelebornTransportOwnershipTestHelper() {}

  @SuppressWarnings({"rawtypes", "unchecked"})
  public static void assertBootstrapFailureDoesNotEscapeClientCreation() throws Exception {
    ShuffleClient shuffle = new ShuffleClient();
    ExecutorShufflePushAdmission admission = ExecutorShufflePushAdmission.forClient(shuffle, 64);
    Client existing = shuffle.factory.createClient(true);
    Client incompatible = null;
    Client ordinary = null;
    CelebornTransportCallbackTracker.Push push = null;
    try {
      CelebornTransportCallbackTracker tracker = admission.transportCallbacks(shuffle);
      push = tracker.beginPush();
      check(push != null, "the compatible factory must initially support transport ownership");

      // Celeborn treats bootstrap exceptions as fatal to its shared client factory. A Comet-only
      // reflection mismatch must instead disable tracking and let ordinary clients keep opening.
      incompatible = shuffle.factory.createClient(false);
      check(!push.usesTransportOwnership(), "the active push must switch to counter fallback");

      // An existing request map may already be instrumented. Once the push falls back, publishing
      // a request with an unknown callback shape must delegate without further reflection.
      ConcurrentHashMap rawRequests = existing.handler.outstandingPushes;
      Object unknownRequest = new Object();
      rawRequests.put(1L, unknownRequest);
      check(
          rawRequests.get(1L) == unknownRequest,
          "fallback publication must not inspect an unknown request shape");

      push.close();
      push = null;
      check(tracker.beginPush() == null, "a failed bootstrap must persistently select fallback");
      ordinary = shuffle.factory.createClient(true);
    } finally {
      if (push != null) {
        push.close();
      }
      existing.underlying.close();
      if (incompatible != null) {
        incompatible.underlying.close();
      }
      if (ordinary != null) {
        ordinary.underlying.close();
      }
      ExecutorShufflePushAdmission.releaseClient(shuffle);
      shuffle.pushDataRetryPool.shutdownNow();
    }
  }

  public static void assertBootstrapHookFollowsClientLifetime() throws Exception {
    Factory factory = new Factory();
    Bootstrap before = ignored -> {};
    Bootstrap after = ignored -> {};
    factory.clientBootstraps.add(before);
    factory.clientBootstraps.add(after);
    ShuffleClient first = new ShuffleClient(factory, Executors.newSingleThreadExecutor());
    ShuffleClient second = new ShuffleClient(factory, Executors.newSingleThreadExecutor());
    try {
      ExecutorShufflePushAdmission firstAdmission =
          ExecutorShufflePushAdmission.forClient(first, 64);
      CelebornTransportCallbackTracker.Push firstPush =
          firstAdmission.transportCallbacks(first).beginPush();
      firstPush.close();

      ExecutorShufflePushAdmission secondAdmission =
          ExecutorShufflePushAdmission.forClient(second, 64);
      CelebornTransportCallbackTracker.Push secondPush =
          secondAdmission.transportCallbacks(second).beginPush();
      secondPush.close();

      check(factory.clientBootstraps.size() == 3, "shared factories need one Comet bootstrap");
      Bootstrap cometBootstrap = factory.clientBootstraps.get(2);
      check(Proxy.isProxyClass(cometBootstrap.getClass()), "the Comet bootstrap must be a proxy");

      ExecutorShufflePushAdmission.releaseClient(first);
      check(factory.clientBootstraps.size() == 3, "the first owner must retain the shared hook");
      ExecutorShufflePushAdmission.releaseClient(first);
      check(factory.clientBootstraps.size() == 3, "client release must be idempotent");

      ExecutorShufflePushAdmission.releaseClient(second);
      check(factory.clientBootstraps.size() == 2, "the last owner must remove the Comet hook");
      check(
          factory.clientBootstraps.get(0) == before && factory.clientBootstraps.get(1) == after,
          "hook removal must preserve unrelated bootstraps and their order");

      Client gapClient = factory.createClient();
      check(
          !Proxy.isProxyClass(gapClient.channel.getClass()),
          "clients opened without a Comet owner must remain ordinary");
      check(
          gapClient.handler.outstandingPushes.getClass() == ConcurrentHashMap.class,
          "clients opened without a Comet owner must keep their ordinary request map");

      // A factory can already be iterating the old copy-on-write list when the last owner exits.
      // Its captured proxy must become inert even though that old list remains reachable.
      Client staleClient = new Client();
      cometBootstrap.doBootstrap(staleClient);
      check(
          !Proxy.isProxyClass(staleClient.channel.getClass()),
          "a released bootstrap snapshot must not wrap later channels");
      check(
          staleClient.handler.outstandingPushes.getClass() == ConcurrentHashMap.class,
          "a released bootstrap snapshot must not wrap later request maps");
      staleClient.underlying.close();

      ExecutorShufflePushAdmission reboundAdmission =
          ExecutorShufflePushAdmission.forClient(first, 64);
      CelebornTransportCallbackTracker.Push reboundPush =
          reboundAdmission.transportCallbacks(first).beginPush();
      check(reboundPush != null, "a surviving client must reuse its compatible transport hook");
      reboundPush.close();
      check(
          factory.clientBootstraps.size() == 3 && factory.clientBootstraps.get(2) == cometBootstrap,
          "reacquiring a surviving client must restore the same bootstrap proxy");
      check(
          Proxy.isProxyClass(gapClient.channel.getClass()),
          "reacquisition must instrument clients opened while the hook was absent");
      check(
          gapClient.handler.outstandingPushes.getClass() != ConcurrentHashMap.class,
          "reacquisition must instrument request maps opened while the hook was absent");
      ExecutorShufflePushAdmission.releaseClient(first);
      check(factory.clientBootstraps.size() == 2, "the rebound hook must remain releasable");
      gapClient.underlying.close();
    } finally {
      ExecutorShufflePushAdmission.releaseClient(first);
      ExecutorShufflePushAdmission.releaseClient(second);
      first.pushDataRetryPool.shutdownNow();
      second.pushDataRetryPool.shutdownNow();
    }
  }

  public static void assertFatalBootstrapErrorsDoNotLeakRegistrations() throws Exception {
    Factory initializationFactory = new Factory();
    Client existing = initializationFactory.createClient();
    ShuffleClient initializationShuffle =
        new ShuffleClient(initializationFactory, Executors.newSingleThreadExecutor());
    AssertionError initializationError = new AssertionError("fatal existing-client mismatch");
    existing.handlerFailure = initializationError;
    try {
      CelebornTransportCallbackTracker tracker =
          ExecutorShufflePushAdmission.forClient(initializationShuffle, 64)
              .transportCallbacks(initializationShuffle);
      try {
        tracker.beginPush();
        throw new AssertionError("a fatal reflected bootstrap error must escape unchanged");
      } catch (AssertionError actual) {
        check(actual == initializationError, "the reflected fatal error must stay unchanged");
      }
      check(
          initializationFactory.clientBootstraps.isEmpty(),
          "fatal initialization must remove the unowned bootstrap registration");
      existing.handlerFailure = null;
      Client ordinary = initializationFactory.createClient();
      ordinary.underlying.close();
    } finally {
      existing.handlerFailure = null;
      existing.underlying.close();
      ExecutorShufflePushAdmission.releaseClient(initializationShuffle);
      initializationShuffle.pushDataRetryPool.shutdownNow();
    }

    ShuffleClient activeShuffle = new ShuffleClient();
    try {
      CelebornTransportCallbackTracker tracker =
          ExecutorShufflePushAdmission.forClient(activeShuffle, 64)
              .transportCallbacks(activeShuffle);
      CelebornTransportCallbackTracker.Push push = tracker.beginPush();
      push.close();
      Bootstrap bootstrap = activeShuffle.factory.clientBootstraps.get(0);
      Client fatalClient = new Client();
      AssertionError callbackError = new AssertionError("fatal new-client mismatch");
      fatalClient.handlerFailure = callbackError;
      try {
        bootstrap.doBootstrap(fatalClient);
        throw new AssertionError("a fatal bootstrap callback error must escape unchanged");
      } catch (AssertionError actual) {
        check(actual == callbackError, "the callback fatal error must stay unchanged");
      } finally {
        fatalClient.underlying.close();
      }
      Client ordinary = activeShuffle.factory.createClient();
      ordinary.underlying.close();
      check(tracker.beginPush() == null, "fatal callback errors must leave the shared hook inert");
    } finally {
      ExecutorShufflePushAdmission.releaseClient(activeShuffle);
      activeShuffle.pushDataRetryPool.shutdownNow();
    }
  }

  public static void assertBootstrapReleaseWaitsForActiveInvocation() throws Exception {
    ShuffleClient shuffle = new ShuffleClient();
    Thread bootstrapThread = null;
    Thread releaseThread = null;
    Client racingClient = new Client();
    Client staleClient = new Client();
    CountDownLatch handlerEntered = new CountDownLatch(1);
    CountDownLatch resumeHandler = new CountDownLatch(1);
    AtomicReference<Throwable> bootstrapFailure = new AtomicReference<>();
    racingClient.handlerEntered = handlerEntered;
    racingClient.resumeHandler = resumeHandler;
    try {
      CelebornTransportCallbackTracker tracker =
          ExecutorShufflePushAdmission.forClient(shuffle, 64).transportCallbacks(shuffle);
      CelebornTransportCallbackTracker.Push push = tracker.beginPush();
      push.close();
      Bootstrap bootstrap = shuffle.factory.clientBootstraps.get(0);

      bootstrapThread =
          new Thread(
              () -> {
                try {
                  bootstrap.doBootstrap(racingClient);
                } catch (Throwable failure) {
                  bootstrapFailure.set(failure);
                }
              });
      bootstrapThread.start();
      check(handlerEntered.await(5, TimeUnit.SECONDS), "the bootstrap must enter client setup");

      releaseThread = new Thread(() -> ExecutorShufflePushAdmission.releaseClient(shuffle));
      releaseThread.start();
      releaseThread.join(100);
      check(releaseThread.isAlive(), "last-owner release must wait for an active bootstrap");

      resumeHandler.countDown();
      bootstrapThread.join(5000);
      releaseThread.join(5000);
      check(!bootstrapThread.isAlive(), "the active bootstrap must finish");
      check(!releaseThread.isAlive(), "last-owner release must finish after the bootstrap");
      check(bootstrapFailure.get() == null, "the active bootstrap must remain valid");
      check(
          Proxy.isProxyClass(racingClient.channel.getClass()),
          "a bootstrap started before release may finish instrumentation");
      check(
          shuffle.factory.clientBootstraps.isEmpty(),
          "last-owner release must remove the bootstrap before returning");

      bootstrap.doBootstrap(staleClient);
      check(
          !Proxy.isProxyClass(staleClient.channel.getClass()),
          "a stale bootstrap must remain inert after release returns");
      check(
          staleClient.handler.outstandingPushes.getClass() == ConcurrentHashMap.class,
          "a stale bootstrap must not instrument request maps after release returns");
    } finally {
      resumeHandler.countDown();
      if (bootstrapThread != null) {
        bootstrapThread.interrupt();
        bootstrapThread.join(5000);
      }
      if (releaseThread != null) {
        releaseThread.interrupt();
        releaseThread.join(5000);
      }
      ExecutorShufflePushAdmission.releaseClient(shuffle);
      racingClient.underlying.close();
      staleClient.underlying.close();
      shuffle.pushDataRetryPool.shutdownNow();
    }
  }

  public static void assertTimedOutWriteRetainsOwnership(boolean existingClient) throws Exception {
    ShuffleClient shuffle = new ShuffleClient();
    Client client = existingClient ? shuffle.factory.createClient() : null;
    CelebornTransportCallbackTracker.Push push =
        CelebornTransportCallbackTracker.tryCreate(shuffle).beginPush();
    if (client == null) {
      client = shuffle.factory.createClient();
    }
    ByteBuf body = Unpooled.wrappedBuffer(new byte[8860]);
    try {
      Request request = new Request();
      client.handler.outstandingPushes.put(1L, request);
      // Stock TransportClient stores the result of this fluent addListener call.
      ChannelFuture write = client.getChannel().writeAndFlush(body).addListener(ignored -> {});
      push.close();
      check(!write.cancel(true), "owned writes must not complete via timeout cancellation");
      client.handler.outstandingPushes.remove(1L);
      request.callback.onFailure(new IOException("Cleaned Up"));
      check(!write.isDone(), "the blocked outbound write must still be pending");
      check(body.refCnt() == 1, "the actual outbound buffer must still own the body");
      check(!push.isComplete(), "callback completion must not release a pending outbound write");

      client.underlying.retireWrite();
      check(write.isDone(), "write retirement must complete its real future");
      check(body.refCnt() == 0, "write retirement must release the actual body");
      check(push.isComplete(), "admission may finish after both callback and write retire");
    } finally {
      push.close();
      client.underlying.close();
      shuffle.pushDataRetryPool.shutdownNow();
    }
  }

  public static void assertUnflushedWriteCannotBeCancelledEarly() throws Exception {
    ShuffleClient shuffle = new ShuffleClient();
    Client client = shuffle.factory.createClient();
    HoldingWriteHandler queued = new HoldingWriteHandler();
    client.underlying.pipeline().addLast(queued);
    CelebornTransportCallbackTracker.Push push =
        CelebornTransportCallbackTracker.tryCreate(shuffle).beginPush();
    ByteBuf body = Unpooled.wrappedBuffer(new byte[8860]);
    try {
      Request request = new Request();
      client.handler.outstandingPushes.put(1L, request);
      ChannelFuture write = client.getChannel().writeAndFlush(body).addListener(ignored -> {});
      push.close();
      check(!write.isCancellable(), "the exposed future must consistently refuse cancellation");
      check(!write.cancel(true), "timeout must not cancel a still-owned queued write");
      client.handler.outstandingPushes.remove(1L);
      request.callback.onFailure(new IOException("Cleaned Up"));
      check(body.refCnt() == 1, "the queued write still owns its payload");
      check(!push.isComplete(), "a queued body must stay charged after callback failure");

      queued.failWrite();
      check(body.refCnt() == 0, "failing the queued write must release its body");
      check(push.isComplete(), "the failed write and callback have both returned ownership");
    } finally {
      push.close();
      queued.failWrite();
      client.underlying.close();
      shuffle.pushDataRetryPool.shutdownNow();
    }
  }

  public static void assertUnownedWritesPreserveCancellation() throws Exception {
    ShuffleClient shuffle = new ShuffleClient();
    Client client = shuffle.factory.createClient();
    CelebornTransportCallbackTracker.Push push =
        CelebornTransportCallbackTracker.tryCreate(shuffle).beginPush();
    push.close();
    HoldingWriteHandler queued = new HoldingWriteHandler();
    client.underlying.pipeline().addLast(queued);
    ByteBuf body = Unpooled.wrappedBuffer(new byte[16]);
    try {
      ChannelFuture write = client.getChannel().writeAndFlush(body);
      check(write.cancel(true), "ordinary Spark writes must keep their normal cancellation API");
    } finally {
      queued.failWrite();
      client.underlying.close();
      shuffle.pushDataRetryPool.shutdownNow();
    }
  }

  public static void assertCompletedCallbacksForgetPayloads() throws Exception {
    for (boolean throwFromCallback : new boolean[] {false, true}) {
      ShuffleClient shuffle = new ShuffleClient();
      Client client = shuffle.factory.createClient();
      CelebornTransportCallbackTracker.Push push =
          CelebornTransportCallbackTracker.tryCreate(shuffle).beginPush();
      final byte[] payload = new byte[8860];
      final RuntimeException callbackFailure = new RuntimeException("callback test failure");
      Request retained = new Request();
      retained.callback =
          new Callback() {
            @Override
            public void onSuccess(ByteBuffer response) {
              check(payload.length == 8860, "the active callback must still own its payload");
              check(!push.isComplete(), "the callback must stay charged while running");
              if (throwFromCallback) {
                throw callbackFailure;
              }
            }

            @Override
            public void onFailure(Throwable failure) {
              onSuccess(null);
            }
          };
      try {
        client.handler.outstandingPushes.put(1L, retained);
        push.close();
        client.handler.outstandingPushes.remove(1L);
        try {
          retained.callback.onFailure(new IOException("Cleaned Up"));
          check(!throwFromCallback, "the callback exception must be preserved");
        } catch (RuntimeException failure) {
          check(failure == callbackFailure && throwFromCallback, "unexpected callback exception");
        }
        // Keep the request and proxy reachable, modeling a handler paused after invocation.
        checkDelegateCleared(Proxy.getInvocationHandler(retained.callback), "callback");
        check(push.isComplete(), "only a payload-free callback wrapper may release admission");
      } finally {
        push.close();
        client.underlying.close();
        shuffle.pushDataRetryPool.shutdownNow();
      }
    }
  }

  public static void assertCompletedRetriesForgetPayloads() throws Exception {
    for (int mode = 0; mode < 3; mode++) {
      QueuedExecutor executor = new QueuedExecutor();
      ShuffleClient shuffle = new ShuffleClient(executor);
      CelebornTransportCallbackTracker.Push push =
          CelebornTransportCallbackTracker.tryCreate(shuffle).beginPush();
      final byte[] payload = new byte[8860];
      final boolean throwFromRetry = mode == 1;
      final RuntimeException retryFailure = new RuntimeException("retry test failure");
      try {
        shuffle.pushDataRetryPool.execute(
            () -> {
              check(payload.length == 8860, "the active retry must still own its payload");
              check(!push.isComplete(), "the retry must stay charged while running");
              if (throwFromRetry) {
                throw retryFailure;
              }
            });
        Runnable retained = executor.tasks.get(0);
        push.close();
        check(!push.isComplete(), "queued retries must retain admission");
        if (mode == 2) {
          List<Runnable> discarded = shuffle.pushDataRetryPool.shutdownNow();
          check(discarded.get(0) == retained, "shutdown must return the discarded wrapper");
          retained.run();
        } else {
          executor.tasks.clear();
          try {
            retained.run();
            check(!throwFromRetry, "the retry exception must be preserved");
          } catch (RuntimeException failure) {
            check(failure == retryFailure && throwFromRetry, "unexpected retry exception");
          }
        }
        // A queue, executor worker, or shutdown caller may keep the wrapper after completion.
        checkDelegateCleared(retained, "delegate");
        check(push.isComplete(), "only a payload-free retry wrapper may release admission");
      } finally {
        push.close();
        shuffle.pushDataRetryPool.shutdownNow();
      }
    }
  }

  private static void checkDelegateCleared(Object owner, String name) throws Exception {
    Field delegate = owner.getClass().getDeclaredField(name);
    delegate.setAccessible(true);
    check(delegate.get(owner) == null, "completed ownership must not retain its " + name);
  }

  private static void check(boolean condition, String message) {
    if (!condition) {
      throw new AssertionError(message);
    }
  }

  public interface Bootstrap {
    void doBootstrap(Client client);
  }

  public interface Callback {
    void onSuccess(ByteBuffer response);

    void onFailure(Throwable failure);
  }

  public static final class Request {
    public Callback callback =
        new Callback() {
          @Override
          public void onSuccess(ByteBuffer response) {}

          @Override
          public void onFailure(Throwable failure) {}
        };
  }

  public static final class ResponseHandler {
    private final ConcurrentHashMap<Long, Request> outstandingPushes = new ConcurrentHashMap<>();
  }

  public static final class Client {
    private final HoldingChannel underlying = new HoldingChannel();
    private final Channel channel;
    private final ResponseHandler handler = new ResponseHandler();
    private Error handlerFailure;
    private CountDownLatch handlerEntered;
    private CountDownLatch resumeHandler;

    private Client() {
      this(true);
    }

    private Client(boolean compatible) {
      channel = compatible ? underlying : null;
    }

    public ResponseHandler getHandler() {
      if (handlerEntered != null) {
        handlerEntered.countDown();
        try {
          if (!resumeHandler.await(5, TimeUnit.SECONDS)) {
            throw new IllegalStateException("timed out waiting to resume client setup");
          }
        } catch (InterruptedException failure) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException("client setup was interrupted", failure);
        }
      }
      if (handlerFailure != null) {
        throw handlerFailure;
      }
      return handler;
    }

    public Channel getChannel() {
      return channel;
    }
  }

  public static final class Pool {
    private final Client[] clients = new Client[1];
    private final Object[] locks = {new Object()};
  }

  public static final class Factory {
    private final List<Bootstrap> clientBootstraps = new ArrayList<>();
    private final ConcurrentHashMap<String, Pool> connectionPool = new ConcurrentHashMap<>();

    private Client createClient() {
      return createClient(true);
    }

    private Client createClient(boolean compatible) {
      Pool pool = connectionPool.computeIfAbsent("worker", ignored -> new Pool());
      synchronized (pool.locks[0]) {
        Client client = new Client(compatible);
        for (Bootstrap bootstrap : clientBootstraps) {
          bootstrap.doBootstrap(client);
        }
        pool.clients[0] = client;
        return client;
      }
    }
  }

  public static final class ShuffleClient {
    private final Factory factory;
    private final ExecutorService pushDataRetryPool;

    private ShuffleClient() {
      this(new Factory(), Executors.newSingleThreadExecutor());
    }

    private ShuffleClient(ExecutorService retryPool) {
      this(new Factory(), retryPool);
    }

    private ShuffleClient(Factory factory, ExecutorService retryPool) {
      this.factory = factory;
      pushDataRetryPool = retryPool;
    }

    public Factory getDataClientFactory() {
      return factory;
    }
  }

  private static final class QueuedExecutor extends AbstractExecutorService {
    private final List<Runnable> tasks = new ArrayList<>();
    private boolean shutdown;

    @Override
    public void execute(Runnable command) {
      if (shutdown) {
        throw new RejectedExecutionException("test executor is shut down");
      }
      tasks.add(command);
    }

    @Override
    public void shutdown() {
      shutdown = true;
    }

    @Override
    public List<Runnable> shutdownNow() {
      shutdown = true;
      List<Runnable> discarded = new ArrayList<>(tasks);
      tasks.clear();
      return discarded;
    }

    @Override
    public boolean isShutdown() {
      return shutdown;
    }

    @Override
    public boolean isTerminated() {
      return shutdown && tasks.isEmpty();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {
      return isTerminated();
    }
  }

  private static final class HoldingChannel extends EmbeddedChannel {
    @Override
    protected void doWrite(ChannelOutboundBuffer outbound) {
      // Leave the real, flushed Netty outbound buffer blocked until the test retires it.
    }

    private void retireWrite() {
      unsafe().outboundBuffer().remove(new IOException("write retired by test"));
    }
  }

  private static final class HoldingWriteHandler extends ChannelOutboundHandlerAdapter {
    private ByteBuf body;
    private ChannelPromise promise;

    @Override
    public void write(ChannelHandlerContext context, Object message, ChannelPromise writePromise) {
      body = (ByteBuf) message;
      promise = writePromise;
    }

    private void failWrite() {
      if (body != null) {
        body.release();
        body = null;
        promise.tryFailure(new IOException("queued write retired by test"));
      }
    }
  }
}
