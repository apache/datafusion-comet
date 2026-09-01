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
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Proxy;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tracks payload ownership across stock Celeborn transport callbacks and retry tasks. */
final class CelebornTransportCallbackTracker {
  private static final Logger LOG = LoggerFactory.getLogger(CelebornTransportCallbackTracker.class);

  private final Object shuffleClient;
  private final Method getDataClientFactory;
  private FactoryHook factoryHook;
  private boolean initialized;
  private boolean closed;

  private CelebornTransportCallbackTracker(Object shuffleClient, Method getDataClientFactory) {
    this.shuffleClient = shuffleClient;
    this.getDataClientFactory = getDataClientFactory;
  }

  static CelebornTransportCallbackTracker tryCreate(Object shuffleClient) {
    try {
      return new CelebornTransportCallbackTracker(
          shuffleClient, shuffleClient.getClass().getMethod("getDataClientFactory"));
    } catch (NoSuchMethodException ignored) {
      // Compatibility clients without a transport factory retain their own completion tracking.
      return null;
    }
  }

  synchronized Push beginPush() throws IOException {
    if (closed) {
      return null;
    }
    if (!initialized) {
      initialized = true;
      factoryHook = installFactoryHook();
    }
    return factoryHook == null ? null : factoryHook.beginPush();
  }

  synchronized void close() {
    if (closed) {
      return;
    }
    closed = true;
    if (factoryHook != null) {
      factoryHook.releaseRegistration();
    }
  }

  private FactoryHook installFactoryHook() {
    try {
      Object factory = getDataClientFactory.invoke(shuffleClient);
      if (factory == null) {
        throw new IOException("Celeborn returned a null data transport factory");
      }
      synchronized (factory) {
        Field bootstrapsField = field(factory.getClass(), "clientBootstraps");
        Object bootstrapValue = bootstrapsField.get(factory);
        if (!(bootstrapValue instanceof List<?>)) {
          throw new IOException("Celeborn transport factory has no bootstrap list");
        }
        List<?> bootstraps = (List<?>) bootstrapValue;
        FactoryHook hook = findHook(bootstraps);
        boolean addBootstrap = false;
        if (hook == null) {
          hook = findRetryHook(shuffleClient, factory);
          addBootstrap = true;
        }
        if (hook == null) {
          Class<?> bootstrapInterface = bootstrapInterface(bootstrapsField, factory);
          hook = new FactoryHook(factory, bootstrapsField);
          hook.bootstrap =
              Proxy.newProxyInstance(
                  bootstrapInterface.getClassLoader(), new Class<?>[] {bootstrapInterface}, hook);
        }
        hook.retainRegistration();
        try {
          if (addBootstrap) {
            ArrayList<Object> augmentedBootstraps = new ArrayList<>(bootstraps);
            augmentedBootstraps.add(hook.bootstrap);
            // A client may already be iterating the old list. Do not mutate that iterator; the
            // pool locks below wait for that client's construction before instrumenting it.
            bootstrapsField.set(factory, augmentedBootstraps);
          }
          try {
            if (hook.acceptsTransportOwnership() && !hook.installed) {
              hook.installExistingClients(factory);
              hook.installed = true;
            }
            if (hook.acceptsTransportOwnership()) {
              hook.installRetryPool(shuffleClient);
            }
          } catch (ReflectiveOperationException | IOException | RuntimeException failure) {
            Error invocationError = invocationError(failure);
            if (invocationError != null) {
              throw invocationError;
            }
            hook.disableTransportOwnership(failure);
          }
          return hook;
        } catch (ReflectiveOperationException | RuntimeException failure) {
          hook.releaseRegistration();
          throw failure;
        } catch (Error failure) {
          rollbackFatalInitialization(hook, failure);
          throw failure;
        }
      }
    } catch (ReflectiveOperationException | IOException | RuntimeException failure) {
      Error invocationError = invocationError(failure);
      if (invocationError != null) {
        throw invocationError;
      }
      LOG.warn(
          "Cannot install Celeborn transport ownership tracking; falling back to push-state "
              + "completion",
          failure);
      return null;
    }
  }

  private static FactoryHook findHook(List<?> bootstraps) {
    for (Object bootstrap : bootstraps) {
      if (bootstrap != null && Proxy.isProxyClass(bootstrap.getClass())) {
        InvocationHandler handler = Proxy.getInvocationHandler(bootstrap);
        if (handler instanceof FactoryHook) {
          return (FactoryHook) handler;
        }
      }
    }
    return null;
  }

  private static Error invocationError(Throwable failure) {
    if (failure instanceof InvocationTargetException
        && ((InvocationTargetException) failure).getCause() instanceof Error) {
      return (Error) ((InvocationTargetException) failure).getCause();
    }
    return failure instanceof Error ? (Error) failure : null;
  }

  private static void rollbackFatalInitialization(FactoryHook hook, Error failure) {
    try {
      hook.disableTransportOwnership(failure);
    } catch (Throwable cleanupFailure) {
      if (cleanupFailure != failure) {
        failure.addSuppressed(cleanupFailure);
      }
    }
    try {
      hook.releaseRegistration();
    } catch (Throwable cleanupFailure) {
      if (cleanupFailure != failure) {
        failure.addSuppressed(cleanupFailure);
      }
    }
  }

  private static void disableAfterFatalBootstrap(FactoryHook hook, Error failure) {
    try {
      hook.disableTransportOwnership(failure);
    } catch (Throwable cleanupFailure) {
      if (cleanupFailure != failure) {
        failure.addSuppressed(cleanupFailure);
      }
    }
  }

  private static FactoryHook findRetryHook(Object shuffleClient, Object factory)
      throws ReflectiveOperationException {
    synchronized (shuffleClient) {
      Object retryPool = field(shuffleClient.getClass(), "pushDataRetryPool").get(shuffleClient);
      if (retryPool instanceof TrackingRetryExecutor) {
        FactoryHook hook = ((TrackingRetryExecutor) retryPool).hook;
        return hook.factory == factory ? hook : null;
      }
      return null;
    }
  }

  private static Class<?> bootstrapInterface(Field bootstrapsField, Object factory)
      throws ReflectiveOperationException {
    Type genericType = bootstrapsField.getGenericType();
    if (genericType instanceof ParameterizedType) {
      Type elementType = ((ParameterizedType) genericType).getActualTypeArguments()[0];
      if (elementType instanceof Class<?> && ((Class<?>) elementType).isInterface()) {
        return (Class<?>) elementType;
      }
    }
    return Class.forName(
        "org.apache.celeborn.common.network.client.TransportClientBootstrap",
        false,
        factory.getClass().getClassLoader());
  }

  private static Field field(Class<?> type, String name) throws NoSuchFieldException {
    for (Class<?> owner = type; owner != null; owner = owner.getSuperclass()) {
      try {
        Field found = owner.getDeclaredField(name);
        found.setAccessible(true);
        return found;
      } catch (NoSuchFieldException ignored) {
        // Stock clients and compatibility test clients can inherit these fields.
      }
    }
    throw new NoSuchFieldException(type.getName() + "." + name);
  }

  /** A raw push plus every asynchronous owner created while handling that push. */
  static final class Push implements AutoCloseable {
    private final FactoryHook hook;
    private final Push previous;
    private final AtomicInteger owners = new AtomicInteger(1);
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean transportOwnershipAvailable = new AtomicBoolean(true);
    private final ConcurrentLinkedQueue<WriteOwnership> writes = new ConcurrentLinkedQueue<>();

    private Push(FactoryHook hook) {
      this.hook = hook;
      this.previous = hook.current.get();
      hook.current.set(this);
    }

    boolean isComplete() {
      if (!usesTransportOwnership()) {
        return false;
      }
      return retainedTransportOwnershipComplete() && usesTransportOwnership();
    }

    boolean retainedTransportOwnershipComplete() {
      // A shutting-down event loop can reject listener notification after completing a promise.
      // Reconciliation can still observe that write's completion without depending on notification.
      for (WriteOwnership write : writes) {
        write.completeIfDone();
      }
      return owners.get() == 0;
    }

    boolean usesTransportOwnership() {
      return transportOwnershipAvailable.get();
    }

    private void fallBackToPushState() {
      transportOwnershipAvailable.set(false);
    }

    private void releaseOwner() {
      if (owners.decrementAndGet() == 0) {
        hook.activePushes.remove(this);
      }
    }

    private Lease retain() {
      int currentOwners;
      do {
        currentOwners = owners.get();
        if (currentOwners == 0) {
          throw new IllegalStateException("Cannot retain a completed Celeborn push");
        }
      } while (!owners.compareAndSet(currentOwners, currentOwners + 1));
      return new Lease(this);
    }

    private Activation activate() {
      return new Activation(this);
    }

    @Override
    public void close() {
      if (closed.compareAndSet(false, true)) {
        if (hook.current.get() != this) {
          throw new IllegalStateException(
              "Celeborn raw push scopes must close in invocation order");
        }
        hook.restore(previous);
        releaseOwner();
      }
    }
  }

  private static final class Lease implements AutoCloseable {
    private final Push push;
    private final AtomicBoolean closed = new AtomicBoolean();

    private Lease(Push push) {
      this.push = push;
    }

    @Override
    public void close() {
      if (closed.compareAndSet(false, true)) {
        push.releaseOwner();
      }
    }
  }

  private static final class Activation implements AutoCloseable {
    private final Push push;
    private final Push previous;

    private Activation(Push push) {
      this.push = push;
      previous = push.hook.current.get();
      push.hook.current.set(push);
    }

    @Override
    public void close() {
      push.hook.restore(previous);
    }
  }

  /** The factory owns its context; it is explicitly propagated at every asynchronous boundary. */
  private static final class FactoryHook implements InvocationHandler {
    // This is an invocation scope, not Spark TaskContext or native worker identity. A callback or
    // retry can run on any thread; its wrapper restores this scope only while invoking its
    // delegate.
    private final ThreadLocal<Push> current = new ThreadLocal<>();
    private final Set<Push> activePushes = ConcurrentHashMap.newKeySet();
    private final Object factory;
    private final Field bootstrapsField;
    private final AtomicBoolean acceptingTransportOwnership = new AtomicBoolean(true);
    private volatile boolean active;
    private Object bootstrap;
    // Read and written only while holding the owning transport factory's monitor.
    private int registrations;
    // Read and written only while holding the owning transport factory's monitor.
    private boolean installed;

    private FactoryHook(Object factory, Field bootstrapsField) {
      this.factory = factory;
      this.bootstrapsField = bootstrapsField;
    }

    private synchronized Push beginPush() {
      if (!active || !acceptingTransportOwnership.get()) {
        return null;
      }
      Push push = new Push(this);
      activePushes.add(push);
      return push;
    }

    private boolean acceptsTransportOwnership() {
      return active && acceptingTransportOwnership.get();
    }

    private Push currentPush() {
      Push push = currentObservedPush();
      return push != null && push.usesTransportOwnership() ? push : null;
    }

    private Push currentObservedPush() {
      Push push = current.get();
      return active ? push : null;
    }

    private synchronized void disableTransportOwnership(Throwable failure) {
      for (Push push : activePushes) {
        push.fallBackToPushState();
      }
      if (!acceptingTransportOwnership.compareAndSet(true, false)) {
        return;
      }
      LOG.warn(
          "Cannot instrument a Celeborn transport client; falling back to push-state completion",
          failure);
    }

    private synchronized void retainRegistration() {
      registrations++;
      active = true;
    }

    private void releaseRegistration() {
      synchronized (factory) {
        if (registrations == 0) {
          return;
        }
        registrations--;
        if (registrations != 0) {
          return;
        }
        installed = false;
        synchronized (this) {
          active = false;
          for (Push push : activePushes) {
            push.fallBackToPushState();
          }
        }
        try {
          Object value = bootstrapsField.get(factory);
          if (!(value instanceof List<?>)) {
            LOG.warn("Cannot remove the released Celeborn transport ownership hook");
            return;
          }
          ArrayList<Object> retained = new ArrayList<>();
          boolean removed = false;
          for (Object candidate : (List<?>) value) {
            if (candidate == bootstrap) {
              removed = true;
            } else {
              retained.add(candidate);
            }
          }
          if (removed) {
            bootstrapsField.set(factory, retained);
          }
        } catch (ReflectiveOperationException | RuntimeException failure) {
          LOG.warn("Cannot remove the released Celeborn transport ownership hook", failure);
        }
      }
    }

    private void restore(Push previous) {
      if (previous == null) {
        current.remove();
      } else {
        current.set(previous);
      }
    }

    private void installRetryPool(Object shuffleClient)
        throws ReflectiveOperationException, IOException {
      synchronized (shuffleClient) {
        Field poolField = field(shuffleClient.getClass(), "pushDataRetryPool");
        Object pool = poolField.get(shuffleClient);
        if (pool instanceof TrackingRetryExecutor) {
          if (((TrackingRetryExecutor) pool).hook != this) {
            throw new IOException("Celeborn retry executor has a different callback tracker");
          }
        } else if (pool instanceof ExecutorService) {
          poolField.set(shuffleClient, new TrackingRetryExecutor((ExecutorService) pool, this));
        } else {
          throw new IOException("Celeborn shuffle client has no compatible retry executor");
        }
      }
    }

    private void installExistingClients(Object factory)
        throws ReflectiveOperationException, IOException {
      Object pools = field(factory.getClass(), "connectionPool").get(factory);
      if (!(pools instanceof Map<?, ?>)) {
        throw new IOException("Celeborn transport factory has no connection pool");
      }
      for (Object pool : ((Map<?, ?>) pools).values()) {
        Object clients = field(pool.getClass(), "clients").get(pool);
        Object locks = field(pool.getClass(), "locks").get(pool);
        if (clients == null
            || locks == null
            || !clients.getClass().isArray()
            || !locks.getClass().isArray()
            || Array.getLength(clients) != Array.getLength(locks)) {
          throw new IOException("Celeborn connection pool has incompatible client slots");
        }
        for (int index = 0; index < Array.getLength(clients); index++) {
          Object lock = Array.get(locks, index);
          if (lock == null) {
            throw new IOException("Celeborn connection pool has a null client lock");
          }
          synchronized (lock) {
            Object client = Array.get(clients, index);
            if (client != null) {
              installClient(client);
            }
          }
        }
      }
    }

    private void installClient(Object client) throws ReflectiveOperationException, IOException {
      Object handler = client.getClass().getMethod("getHandler").invoke(client);
      if (handler == null) {
        throw new IOException("Celeborn transport client has a null response handler");
      }
      synchronized (handler) {
        installChannel(client);
        Field requestsField = field(handler.getClass(), "outstandingPushes");
        Object requests = requestsField.get(handler);
        if (requests instanceof CallbackTrackingRequests) {
          if (((CallbackTrackingRequests) requests).hook != this) {
            throw new IOException("Celeborn response handler has a different callback tracker");
          }
          return;
        }
        if (!(requests instanceof ConcurrentHashMap<?, ?>)) {
          throw new IOException("Celeborn response handler has incompatible push request storage");
        }
        @SuppressWarnings("unchecked")
        ConcurrentHashMap<Object, Object> original = (ConcurrentHashMap<Object, Object>) requests;
        // Keep the original map as the backing store: an unrelated task may already be using it.
        requestsField.set(handler, new CallbackTrackingRequests(original, this));
      }
    }

    private void installChannel(Object client) throws ReflectiveOperationException, IOException {
      Field channelField = field(client.getClass(), "channel");
      Object channel = channelField.get(client);
      if (channel == null || !channelField.getType().isInterface()) {
        throw new IOException("Celeborn transport client has no compatible channel interface");
      }
      if (Proxy.isProxyClass(channel.getClass())
          && Proxy.getInvocationHandler(channel) instanceof TrackingChannel) {
        if (((TrackingChannel) Proxy.getInvocationHandler(channel)).hook != this) {
          throw new IOException("Celeborn channel has a different callback tracker");
        }
        return;
      }
      Class<?> channelInterface = channelField.getType();
      TrackingChannel tracking = new TrackingChannel(channel, channelInterface, this);
      channelField.set(
          client,
          Proxy.newProxyInstance(
              channelInterface.getClassLoader(), new Class<?>[] {channelInterface}, tracking));
    }

    private PreparedRequest prepareRequest(Object requestInfo) {
      Push push = currentPush();
      if (push == null) {
        return null;
      }
      try {
        Field callbackField = field(requestInfo.getClass(), "callback");
        Object callback = callbackField.get(requestInfo);
        if (callback == null) {
          return null;
        }
        if (Proxy.isProxyClass(callback.getClass())
            && Proxy.getInvocationHandler(callback) instanceof CompletionCallback) {
          return null;
        }
        Class<?> callbackInterface = callbackField.getType();
        if (!callbackInterface.isInterface()) {
          throw new IllegalStateException("Celeborn push callback is not an interface");
        }
        CompletionCallback completion = new CompletionCallback(callback, push);
        try {
          Object wrapped =
              Proxy.newProxyInstance(
                  callbackInterface.getClassLoader(),
                  new Class<?>[] {callbackInterface},
                  completion);
          callbackField.set(requestInfo, wrapped);
          return new PreparedRequest(requestInfo, callbackField, callback, wrapped, completion);
        } catch (ReflectiveOperationException | RuntimeException | Error failure) {
          completion.lease.close();
          throw failure;
        }
      } catch (ReflectiveOperationException failure) {
        throw new IllegalStateException("Cannot observe a Celeborn push callback", failure);
      }
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      if (method.getDeclaringClass() == Object.class) {
        return objectMethod(proxy, method, args);
      }
      if ("doBootstrap".equals(method.getName()) && args != null && args.length == 1) {
        synchronized (this) {
          // Serialize the active check with the zero-registration transition. Once the last
          // owner has returned from releaseRegistration, no stale copy-on-write bootstrap
          // snapshot may instrument or reject a later ordinary client.
          if (!active) {
            return null;
          }
          if (!acceptingTransportOwnership.get()) {
            Push push = current.get();
            if (push != null) {
              push.fallBackToPushState();
            }
            return null;
          }
          try {
            installClient(args[0]);
          } catch (ReflectiveOperationException | IOException | RuntimeException failure) {
            Error invocationError = invocationError(failure);
            if (invocationError != null) {
              disableAfterFatalBootstrap(this, invocationError);
              throw invocationError;
            }
            disableTransportOwnership(failure);
          } catch (Error failure) {
            disableAfterFatalBootstrap(this, failure);
            throw failure;
          }
          return null;
        }
      }
      throw new UnsupportedOperationException(
          "Unsupported Celeborn transport bootstrap: " + method);
    }
  }

  /** Pins the actual write future independently of the response handler's timeout bookkeeping. */
  private static final class WriteOwnership implements InvocationHandler {
    private final Push push;
    private final Lease lease;
    private final Method isDone;
    private volatile Object future;

    private WriteOwnership(Push push, Method isDone) {
      this.push = push;
      this.lease = push.retain();
      this.isDone = isDone;
      push.writes.add(this);
    }

    private void finish() {
      lease.close();
      push.writes.remove(this);
    }

    private void completeIfDone() {
      Object actual = future;
      if (actual != null) {
        try {
          if ((boolean) isDone.invoke(actual)) {
            finish();
          }
        } catch (ReflectiveOperationException failure) {
          throw new IllegalStateException(
              "Cannot inspect Celeborn outbound write completion", failure);
        }
      }
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) {
      if (method.getDeclaringClass() == Object.class) {
        return objectMethod(proxy, method, args);
      }
      if ("operationComplete".equals(method.getName())) {
        finish();
        return null;
      }
      throw new UnsupportedOperationException("Unsupported Celeborn write listener: " + method);
    }
  }

  private static final class TrackingChannel implements InvocationHandler {
    private final Object channel;
    private final FactoryHook hook;
    private final Class<?> futureInterface;
    private final Method addListener;
    private final Method isDone;

    private TrackingChannel(Object channel, Class<?> channelInterface, FactoryHook hook)
        throws NoSuchMethodException {
      this.channel = channel;
      this.hook = hook;
      futureInterface = channelInterface.getMethod("writeAndFlush", Object.class).getReturnType();
      if (!futureInterface.isInterface()) {
        throw new NoSuchMethodException("Celeborn channel writes must return a future interface");
      }
      isDone = futureInterface.getMethod("isDone");
      Method listenerMethod = null;
      for (Method candidate : futureInterface.getMethods()) {
        if ("addListener".equals(candidate.getName())
            && candidate.getParameterCount() == 1
            && candidate.getParameterTypes()[0].isInterface()) {
          listenerMethod = candidate;
          break;
        }
      }
      if (listenerMethod == null) {
        throw new NoSuchMethodException(
            "Celeborn channel futures must support completion listeners");
      }
      addListener = listenerMethod;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      if (method.getDeclaringClass() == Object.class) {
        return objectMethod(proxy, method, args);
      }
      Push push = hook.currentObservedPush();
      if (push == null || !"writeAndFlush".equals(method.getName())) {
        try {
          Object result = method.invoke(channel, args);
          return result == channel ? proxy : result;
        } catch (InvocationTargetException failure) {
          throw failure.getCause();
        }
      }
      if (args == null || args.length != 1) {
        throw new IOException("Celeborn owned writes must create their own transport promise");
      }
      WriteOwnership ownership = new WriteOwnership(push, isDone);
      final Object future;
      try {
        future = method.invoke(channel, args);
      } catch (InvocationTargetException failure) {
        ownership.finish();
        throw failure.getCause();
      } catch (ReflectiveOperationException | RuntimeException | Error failure) {
        ownership.finish();
        throw failure;
      }
      ownership.future = future;
      Class<?> listenerInterface = addListener.getParameterTypes()[0];
      Object listener =
          Proxy.newProxyInstance(
              listenerInterface.getClassLoader(), new Class<?>[] {listenerInterface}, ownership);
      try {
        addListener.invoke(future, listener);
      } catch (InvocationTargetException failure) {
        ownership.completeIfDone();
        throw failure.getCause();
      }
      ownership.completeIfDone();
      return Proxy.newProxyInstance(
          futureInterface.getClassLoader(),
          new Class<?>[] {futureInterface},
          new UncancellableWriteFuture(future));
    }
  }

  /** A timeout may finish the response callback, but must not mark an unwritten body complete. */
  private static final class UncancellableWriteFuture implements InvocationHandler {
    private final Object future;

    private UncancellableWriteFuture(Object future) {
      this.future = future;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      if (method.getDeclaringClass() == Object.class) {
        return objectMethod(proxy, method, args);
      }
      if ("cancel".equals(method.getName()) || "isCancellable".equals(method.getName())) {
        // Celeborn already tolerates cancellation failing once Netty has flushed a write. Keep
        // that behavior for queued Comet writes too, so only actual write retirement releases it.
        return false;
      }
      try {
        Object result = method.invoke(future, args);
        // Stock TransportClient stores writeAndFlush(...).addListener(...), not the initial
        // future. Preserve this wrapper through all fluent future operations.
        return result == future ? proxy : result;
      } catch (InvocationTargetException failure) {
        throw failure.getCause();
      }
    }
  }

  private static final class PreparedRequest {
    private final Object requestInfo;
    private final Field callbackField;
    private final Object original;
    private final Object wrapped;
    private final CompletionCallback completion;

    private PreparedRequest(
        Object requestInfo,
        Field callbackField,
        Object original,
        Object wrapped,
        CompletionCallback completion) {
      this.requestInfo = requestInfo;
      this.callbackField = callbackField;
      this.original = original;
      this.wrapped = wrapped;
      this.completion = completion;
    }

    private void discard() {
      if (completion.delivered.compareAndSet(false, true)) {
        try {
          if (callbackField.get(requestInfo) == wrapped) {
            callbackField.set(requestInfo, original);
          }
        } catch (IllegalAccessException failure) {
          throw new IllegalStateException(
              "Cannot restore an unpublished Celeborn callback", failure);
        } finally {
          completion.callback = null;
          completion.lease.close();
        }
      }
    }
  }

  private static final class CompletionCallback implements InvocationHandler {
    private volatile Object callback;
    private final Push push;
    private final Lease lease;
    private final AtomicBoolean delivered = new AtomicBoolean();

    private CompletionCallback(Object callback, Push push) {
      this.callback = callback;
      this.push = push;
      this.lease = push.retain();
    }

    private Object invokeCallback(Method method, Object[] args) throws Throwable {
      try {
        return method.invoke(callback, args);
      } catch (InvocationTargetException failure) {
        throw failure.getCause();
      }
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      if (method.getDeclaringClass() == Object.class) {
        return objectMethod(proxy, method, args);
      }
      if (!delivered.compareAndSet(false, true)) {
        return null;
      }
      try (Activation ignored = push.activate()) {
        return invokeCallback(method, args);
      } finally {
        // Request-map removal is not completion. Retry submission during the original callback
        // has already retained its own lease before this transport owner's lease is released.
        // The handler may retain RequestInfo after invoking it. Clear its payload-owning delegate
        // only after the invocation frame has returned, and before publishing completion.
        callback = null;
        lease.close();
      }
    }
  }

  private interface OwnedRetry {
    void discard();
  }

  /** A running retry retains its payload even if its Future is cancelled or reports completion. */
  private static final class RetryOwnership {
    private static final int QUEUED = 0;
    private static final int RUNNING = 1;
    private static final int FINISHED = 2;

    private final Push push;
    private final Lease lease;
    private final AtomicInteger state = new AtomicInteger(QUEUED);

    private RetryOwnership(Push push) {
      this.push = push;
      lease = push.retain();
    }

    private boolean start() {
      return state.compareAndSet(QUEUED, RUNNING);
    }

    private void finish() {
      state.set(FINISHED);
      lease.close();
    }

    private boolean discard() {
      return state.compareAndSet(QUEUED, FINISHED);
    }
  }

  private static final class TrackingRetryRunnable implements Runnable, OwnedRetry {
    private volatile Runnable delegate;
    private final RetryOwnership ownership;

    private TrackingRetryRunnable(Runnable delegate, Push push) {
      this.delegate = delegate;
      ownership = new RetryOwnership(push);
    }

    private void invokeDelegate() {
      delegate.run();
    }

    @Override
    public void run() {
      if (!ownership.start()) {
        return;
      }
      try (Activation ignored = ownership.push.activate()) {
        invokeDelegate();
      } finally {
        // An executor or shutdown caller can retain the completed wrapper. The delegate's own
        // invocation frame must have returned before dropping its payload reference and lease.
        delegate = null;
        ownership.finish();
      }
    }

    @Override
    public void discard() {
      if (ownership.discard()) {
        delegate = null;
        ownership.finish();
      }
    }
  }

  private static final class TrackingRetryFuture<T> extends FutureTask<T> implements OwnedRetry {
    private final RetryOwnership ownership;

    private TrackingRetryFuture(Callable<T> callable, Push push) {
      super(callable);
      ownership = new RetryOwnership(push);
    }

    private TrackingRetryFuture(Runnable runnable, T result, Push push) {
      super(runnable, result);
      ownership = new RetryOwnership(push);
    }

    @Override
    public void run() {
      if (!ownership.start()) {
        return;
      }
      try (Activation ignored = ownership.push.activate()) {
        super.run();
      } finally {
        ownership.finish();
      }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      boolean cancelled = super.cancel(mayInterruptIfRunning);
      if (cancelled) {
        // FutureTask can become cancelled while user code is still running. Only a queued task
        // may release here; a running task releases from run's finally block after it returns.
        if (ownership.discard()) {
          ownership.finish();
        }
      }
      return cancelled;
    }

    @Override
    public void discard() {
      cancel(false);
    }
  }

  private static final class TrackingRetryExecutor extends AbstractExecutorService {
    private final ExecutorService delegate;
    private final FactoryHook hook;

    private TrackingRetryExecutor(ExecutorService delegate, FactoryHook hook) {
      this.delegate = delegate;
      this.hook = hook;
    }

    @Override
    public void execute(Runnable command) {
      if (command == null) {
        throw new NullPointerException("command");
      }
      Push push = hook.currentPush();
      if (push == null) {
        delegate.execute(command);
      } else {
        submitOwned(new TrackingRetryRunnable(command, push));
      }
    }

    @Override
    public Future<?> submit(Runnable task) {
      return submit(task, null);
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
      Push push = hook.currentPush();
      if (push == null) {
        return delegate.submit(task, result);
      }
      TrackingRetryFuture<T> future = new TrackingRetryFuture<>(task, result, push);
      submitOwned(future);
      return future;
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
      Push push = hook.currentPush();
      if (push == null) {
        return delegate.submit(task);
      }
      TrackingRetryFuture<T> future = new TrackingRetryFuture<>(task, push);
      submitOwned(future);
      return future;
    }

    private <T extends Runnable & OwnedRetry> void submitOwned(T task) {
      try {
        delegate.execute(task);
      } catch (RuntimeException | Error failure) {
        task.discard();
        throw failure;
      }
    }

    @Override
    public void shutdown() {
      // Stock Celeborn uses graceful shutdown: queued retries retain ownership until they drain.
      delegate.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
      List<Runnable> queued = delegate.shutdownNow();
      for (Runnable task : queued) {
        if (task instanceof OwnedRetry) {
          ((OwnedRetry) task).discard();
        }
      }
      // Return cancelled wrappers, as with FutureTasks, without reviving the payload-owning
      // original Runnable after its reservation was released. Unowned tasks are unchanged.
      return queued;
    }

    @Override
    public boolean isShutdown() {
      return delegate.isShutdown();
    }

    @Override
    public boolean isTerminated() {
      return delegate.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
      return delegate.awaitTermination(timeout, unit);
    }
  }

  private static Object objectMethod(Object proxy, Method method, Object[] args) {
    switch (method.getName()) {
      case "equals":
        return proxy == args[0];
      case "hashCode":
        return System.identityHashCode(proxy);
      case "toString":
        return "Comet Celeborn transport callback observer";
      default:
        throw new UnsupportedOperationException(method.toString());
    }
  }

  /**
   * Intercepts stock transport's put-based request publication. Other map operations delegate to
   * the original backing store without introducing ownership for values that might not be inserted.
   */
  private static final class CallbackTrackingRequests extends ConcurrentHashMap<Object, Object> {
    private final ConcurrentHashMap<Object, Object> delegate;
    private final FactoryHook hook;

    private CallbackTrackingRequests(ConcurrentHashMap<Object, Object> delegate, FactoryHook hook) {
      this.delegate = delegate;
      this.hook = hook;
    }

    @Override
    public Object put(Object key, Object value) {
      PreparedRequest prepared = hook.prepareRequest(value);
      boolean published = false;
      try {
        Object previous = delegate.put(key, value);
        published = true;
        return previous;
      } finally {
        if (!published && prepared != null) {
          prepared.discard();
        }
      }
    }

    @Override
    public Object putIfAbsent(Object key, Object value) {
      PreparedRequest prepared = hook.prepareRequest(value);
      boolean published = false;
      try {
        Object previous = delegate.putIfAbsent(key, value);
        published = previous == null;
        return previous;
      } finally {
        if (!published && prepared != null) {
          prepared.discard();
        }
      }
    }

    @Override
    public void putAll(Map<?, ?> values) {
      values.forEach(this::put);
    }

    @Override
    public Object get(Object key) {
      return delegate.get(key);
    }

    @Override
    public Object getOrDefault(Object key, Object defaultValue) {
      return delegate.getOrDefault(key, defaultValue);
    }

    @Override
    public boolean containsKey(Object key) {
      return delegate.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
      return delegate.containsValue(value);
    }

    @Override
    public Object remove(Object key) {
      return delegate.remove(key);
    }

    @Override
    public boolean remove(Object key, Object value) {
      return delegate.remove(key, value);
    }

    @Override
    public Object replace(Object key, Object value) {
      return delegate.replace(key, value);
    }

    @Override
    public boolean replace(Object key, Object oldValue, Object newValue) {
      return delegate.replace(key, oldValue, newValue);
    }

    @Override
    public Object computeIfAbsent(Object key, Function<? super Object, ?> mappingFunction) {
      return delegate.computeIfAbsent(key, mappingFunction);
    }

    @Override
    public Object computeIfPresent(
        Object key, BiFunction<? super Object, ? super Object, ?> remappingFunction) {
      return delegate.computeIfPresent(key, remappingFunction);
    }

    @Override
    public Object compute(
        Object key, BiFunction<? super Object, ? super Object, ?> remappingFunction) {
      return delegate.compute(key, remappingFunction);
    }

    @Override
    public Object merge(
        Object key, Object value, BiFunction<? super Object, ? super Object, ?> remappingFunction) {
      return delegate.merge(key, value, remappingFunction);
    }

    @Override
    public void replaceAll(BiFunction<? super Object, ? super Object, ?> function) {
      delegate.replaceAll(function);
    }

    @Override
    public void clear() {
      delegate.clear();
    }

    @Override
    public int size() {
      return delegate.size();
    }

    @Override
    public boolean isEmpty() {
      return delegate.isEmpty();
    }

    @Override
    public long mappingCount() {
      return delegate.mappingCount();
    }

    @Override
    public KeySetView<Object, Object> keySet() {
      return delegate.keySet();
    }

    @Override
    public Set<Map.Entry<Object, Object>> entrySet() {
      return delegate.entrySet();
    }

    @Override
    public Collection<Object> values() {
      return delegate.values();
    }

    @Override
    public void forEach(BiConsumer<? super Object, ? super Object> action) {
      delegate.forEach(action);
    }
  }
}
