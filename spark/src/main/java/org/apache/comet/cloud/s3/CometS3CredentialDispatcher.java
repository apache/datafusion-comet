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

package org.apache.comet.cloud.s3;

import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.comet.util.ClassLoaders;

/**
 * JNI entry point that resolves a {@link CometS3CredentialProvider} for native code.
 *
 * <p>{@link #ensureInitialized} reflects the named class, runs {@code initialize(Map)} once, and
 * returns a {@code long} handle. {@link #getCredentialsForPath} takes that handle on every
 * per-request call. See the design notes in the contributor guide for why the SPI is shaped this
 * way (keying, multi-tenant isolation, lifecycle).
 *
 * <p>Each {@link #ensureInitialized} call takes a reference to the provider, and {@link #release}
 * gives one back. When the last reference is released, the dispatcher closes the provider and
 * forgets its key, so a later {@link #ensureInitialized} for the same key creates a new one. This
 * is what retires a provider whose catalog properties were superseded, such as when an Iceberg REST
 * catalog vends new credentials for a table: native code releases the old handle when it drops the
 * last reader built on it. Providers still referenced at JVM shutdown are closed then.
 *
 * <p>A release can come from any thread that drops a native reader, including a Tokio worker or a
 * task thread inside a scan, so the dispatcher closes released providers on its own daemon thread.
 * A {@code close()} that blocks, such as Iceberg's {@code VendedCredentialsProvider} waiting for a
 * token refresh in flight, then delays only the closes queued behind it.
 */
public final class CometS3CredentialDispatcher {

  private static final Logger LOG = LoggerFactory.getLogger(CometS3CredentialDispatcher.class);

  private static final ConcurrentHashMap<InstanceKey, Long> KEY_TO_HANDLE =
      new ConcurrentHashMap<>();
  private static final ConcurrentHashMap<Long, RegisteredProvider> INSTANCES =
      new ConcurrentHashMap<>();
  private static final AtomicLong HANDLE_SEQ = new AtomicLong(1L);
  private static final CometS3AccessMode[] MODES = CometS3AccessMode.values();

  private static final ExecutorService DEFAULT_CLOSER =
      Executors.newSingleThreadExecutor(
          r -> {
            Thread thread = new Thread(r, "comet-s3-credential-close");
            thread.setDaemon(true);
            return thread;
          });
  private static volatile Executor closer = DEFAULT_CLOSER;

  static {
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(CometS3CredentialDispatcher::closeAll, "comet-s3-credential-shutdown"));
  }

  private CometS3CredentialDispatcher() {}

  /**
   * Reflects and initializes the named provider on first call for the {@code (FQCN, dispatchKey,
   * catalogProperties)} triple, and returns a handle reused by subsequent {@link
   * #getCredentialsForPath} calls. Each call takes a reference that the caller gives back with
   * {@link #release}.
   */
  public static long ensureInitialized(
      String providerClassName, String dispatchKey, Map<String, String> catalogProperties) {
    if (providerClassName == null || providerClassName.isEmpty()) {
      throw new IllegalArgumentException(
          "providerClassName is empty; native side should not call without a configured class");
    }
    Map<String, String> snapshot =
        catalogProperties == null
            ? Collections.emptyMap()
            : Collections.unmodifiableMap(new HashMap<>(catalogProperties));
    InstanceKey key =
        new InstanceKey(providerClassName, dispatchKey == null ? "" : dispatchKey, snapshot);
    while (true) {
      long handle =
          KEY_TO_HANDLE.computeIfAbsent(
              key,
              k -> {
                CometS3CredentialProvider provider = instantiate(k.providerClassName);
                provider.initialize(k.catalogProperties);
                long h = HANDLE_SEQ.getAndIncrement();
                INSTANCES.put(h, new RegisteredProvider(provider, k));
                return h;
              });
      RegisteredProvider registered = INSTANCES.get(handle);
      if (registered != null && registered.acquire()) {
        return handle;
      }
      // The provider was retired after the lookup; drop its stale mapping and register anew.
      KEY_TO_HANDLE.remove(key, handle);
    }
  }

  /**
   * Invoked by native code when it drops the last reader built on {@code handle}. Gives back one
   * reference taken by {@link #ensureInitialized}, and closes the provider when it was the last. An
   * unknown handle, such as one already closed at shutdown, is ignored.
   */
  public static void release(long handle) {
    RegisteredProvider registered = INSTANCES.get(handle);
    if (registered == null || !registered.releaseLast()) {
      return;
    }
    KEY_TO_HANDLE.remove(registered.key, handle);
    INSTANCES.remove(handle, registered);
    try {
      closer.execute(() -> close(registered, "after its last reference was released"));
    } catch (RejectedExecutionException e) {
      close(registered, "after its last reference was released");
    }
  }

  /**
   * Invoked by native code on every per-request credential fetch. {@code handle} must be a value
   * returned by a prior {@link #ensureInitialized} call; otherwise this throws. {@code mode} is the
   * {@link CometS3AccessMode} ordinal.
   */
  public static CometS3Credentials getCredentialsForPath(
      long handle, String bucket, String path, int mode) throws Exception {
    if (mode < 0 || mode >= MODES.length) {
      throw new IllegalArgumentException("Invalid CometS3AccessMode ordinal: " + mode);
    }
    RegisteredProvider registered = INSTANCES.get(handle);
    if (registered == null) {
      throw new IllegalStateException(
          "CometS3CredentialProvider handle "
              + handle
              + " was not initialized; "
              + "ensureInitialized must be called before getCredentialsForPath");
    }
    CometS3AccessMode accessMode = MODES[mode];
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Fetching credentials via {} (dispatchKey={}, handle={}) for bucket={} path={} mode={}",
          registered.key.providerClassName,
          registered.key.dispatchKey,
          handle,
          bucket,
          path,
          accessMode);
    }
    return registered.provider.getCredentialsForPath(
        new CometS3CredentialContext(bucket, path, accessMode));
  }

  /**
   * Invoked by native code when it creates the object store for {@code bucket} (Parquet), or builds
   * an Iceberg {@code FileIO} for catalog properties whose locations it does not have yet or first
   * uses {@code bucket} through one (Iceberg), and again after a request fails with 403. Returns
   * {@code null} when the provider behind {@code handle} does not implement {@link
   * CometS3LocationScopedCredentialProvider}, which leaves it with one credential per bucket, or
   * per table on the Iceberg path. Otherwise returns a copy of the provider's locations.
   *
   * <p>Copying the list here runs any lazy {@code List} code inside this call, so its exceptions
   * reach native code as ordinary Java exceptions, and a non-{@code String} element fails with
   * {@link ArrayStoreException}. A {@code null} list or location is a contract violation and
   * throws, so the read fails instead of using a broader credential.
   */
  public static String[] getPolicyLocations(long handle, String bucket) throws Exception {
    RegisteredProvider registered = INSTANCES.get(handle);
    if (registered == null) {
      throw new IllegalStateException(
          "CometS3CredentialProvider handle "
              + handle
              + " was not initialized; "
              + "ensureInitialized must be called before getPolicyLocations");
    }
    if (!(registered.provider instanceof CometS3LocationScopedCredentialProvider)) {
      return null;
    }
    List<String> locations =
        ((CometS3LocationScopedCredentialProvider) registered.provider).getPolicyLocations(bucket);
    if (locations == null) {
      throw new IllegalStateException(
          registered.key.providerClassName
              + ".getPolicyLocations returned null for bucket "
              + bucket
              + "; return an empty list when the bucket has no locations");
    }
    String[] copy = locations.toArray(new String[0]);
    for (String location : copy) {
      if (location == null) {
        throw new IllegalStateException(
            registered.key.providerClassName
                + ".getPolicyLocations returned a null location for bucket "
                + bucket);
      }
    }
    return copy;
  }

  private static CometS3CredentialProvider instantiate(String providerClassName) {
    Class<?> clazz;
    try {
      clazz = ClassLoaders.loadClass(providerClassName);
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException(
          "CometS3CredentialProvider class not found: "
              + providerClassName
              + ". Ensure the vendor JAR is on the executor classpath.",
          e);
    }
    if (!CometS3CredentialProvider.class.isAssignableFrom(clazz)) {
      throw new IllegalStateException(
          providerClassName + " does not implement " + CometS3CredentialProvider.class.getName());
    }
    try {
      Object instance = clazz.getDeclaredConstructor().newInstance();
      LOG.info("Instantiated CometS3CredentialProvider {}", providerClassName);
      return (CometS3CredentialProvider) instance;
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException(
          providerClassName + " must declare a public no-arg constructor", e);
    } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new IllegalStateException(
          "Failed to instantiate CometS3CredentialProvider " + providerClassName, e);
    }
  }

  /** Visible for tests; otherwise invoked from the JVM shutdown hook. */
  static void closeAll() {
    for (RegisteredProvider registered : INSTANCES.values()) {
      // A concurrent last release may have retired the provider already, and closes it itself.
      if (registered.retire()) {
        close(registered, "during shutdown");
      }
    }
    INSTANCES.clear();
    KEY_TO_HANDLE.clear();
  }

  /**
   * Visible for tests: runs closes of released providers on {@code executor}, or on the
   * dispatcher's own thread when it is null.
   */
  static void setCloserForTesting(Executor executor) {
    closer = executor == null ? DEFAULT_CLOSER : executor;
  }

  /**
   * Visible for tests: waits until the dispatcher's own thread has run every close queued before
   * this call. Its executor runs one task at a time, in order, so a no-op queued now runs last.
   */
  static void awaitQueuedClosesForTesting() throws Exception {
    DEFAULT_CLOSER.submit(() -> {}).get(10, TimeUnit.SECONDS);
  }

  /** Visible for tests: the number of providers the dispatcher holds. */
  static int registeredCount() {
    return INSTANCES.size();
  }

  private static void close(RegisteredProvider registered, String when) {
    try {
      registered.provider.close();
    } catch (Throwable t) {
      LOG.warn(
          "Exception from {} (dispatchKey={}).close() {}",
          registered.key.providerClassName,
          registered.key.dispatchKey,
          when,
          t);
    }
  }

  private static final class InstanceKey {
    final String providerClassName;
    final String dispatchKey;
    final Map<String, String> catalogProperties;

    InstanceKey(
        String providerClassName, String dispatchKey, Map<String, String> catalogProperties) {
      this.providerClassName = providerClassName;
      this.dispatchKey = dispatchKey;
      this.catalogProperties = catalogProperties;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof InstanceKey)) return false;
      InstanceKey other = (InstanceKey) o;
      return providerClassName.equals(other.providerClassName)
          && dispatchKey.equals(other.dispatchKey)
          && catalogProperties.equals(other.catalogProperties);
    }

    @Override
    public int hashCode() {
      return Objects.hash(providerClassName, dispatchKey, catalogProperties);
    }
  }

  private static final class RegisteredProvider {
    /** {@link #refs} once the provider is retired; it then takes no more references. */
    private static final int RETIRED = -1;

    final CometS3CredentialProvider provider;
    final InstanceKey key;
    private final AtomicInteger refs = new AtomicInteger();

    RegisteredProvider(CometS3CredentialProvider provider, InstanceKey key) {
      this.provider = provider;
      this.key = key;
    }

    /** Takes a reference, unless the provider is already retired. */
    boolean acquire() {
      while (true) {
        int n = refs.get();
        if (n == RETIRED) {
          return false;
        }
        if (refs.compareAndSet(n, n + 1)) {
          return true;
        }
      }
    }

    /** Retires the provider outright. Returns false when it was already retired. */
    boolean retire() {
      return refs.getAndSet(RETIRED) != RETIRED;
    }

    /**
     * Gives back a reference. Returns true when it was the last, in which case the provider is now
     * retired and the caller closes it. A concurrent {@link #acquire} that wins the race keeps it.
     */
    boolean releaseLast() {
      while (true) {
        int n = refs.get();
        if (n <= 0) {
          return false;
        }
        if (n == 1 ? refs.compareAndSet(1, RETIRED) : refs.compareAndSet(n, n - 1)) {
          return n == 1;
        }
      }
    }
  }
}
