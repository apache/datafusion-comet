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
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
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
 * way (keying, multi-tenant isolation, shutdown lifecycle).
 */
public final class CometS3CredentialDispatcher {

  private static final Logger LOG = LoggerFactory.getLogger(CometS3CredentialDispatcher.class);

  private static final ConcurrentHashMap<InstanceKey, Long> KEY_TO_HANDLE =
      new ConcurrentHashMap<>();
  private static final ConcurrentHashMap<Long, RegisteredProvider> INSTANCES =
      new ConcurrentHashMap<>();
  private static final AtomicLong HANDLE_SEQ = new AtomicLong(1L);
  private static final CometS3AccessMode[] MODES = CometS3AccessMode.values();

  static {
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(CometS3CredentialDispatcher::closeAll, "comet-s3-credential-shutdown"));
  }

  private CometS3CredentialDispatcher() {}

  /**
   * Reflects and initializes the named provider on first call for the {@code (FQCN, dispatchKey,
   * catalogProperties)} triple, and returns a handle reused by subsequent {@link
   * #getCredentialsForPath} calls.
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
    return KEY_TO_HANDLE.computeIfAbsent(
        key,
        k -> {
          CometS3CredentialProvider provider = instantiate(k.providerClassName);
          provider.initialize(k.catalogProperties);
          long handle = HANDLE_SEQ.getAndIncrement();
          INSTANCES.put(handle, new RegisteredProvider(provider, k));
          return handle;
        });
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
      try {
        registered.provider.close();
      } catch (Throwable t) {
        LOG.warn(
            "Exception from {} (dispatchKey={}).close() during shutdown",
            registered.key.providerClassName,
            registered.key.dispatchKey,
            t);
      }
    }
    INSTANCES.clear();
    KEY_TO_HANDLE.clear();
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
    final CometS3CredentialProvider provider;
    final InstanceKey key;

    RegisteredProvider(CometS3CredentialProvider provider, InstanceKey key) {
      this.provider = provider;
      this.key = key;
    }
  }
}
