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
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JNI entry point invoked from native code to resolve a {@link CometS3CredentialProvider}.
 *
 * <p>Native code names a vendor class via the activation knob ({@code
 * fs.s3a.comet.credential.provider.class} for the Parquet path, {@code
 * s3.comet.credential.provider.class} on a Spark catalog property for the Iceberg path) and a
 * {@code dispatchKey} that scopes the instance: catalog name on the Iceberg path, bucket name on
 * the Parquet path. Each {@code (FQCN, dispatchKey)} key gets its own instance, so two catalogs
 * sharing one provider class get isolated state.
 */
public final class CometS3CredentialDispatcher {

  private static final Logger LOG = LoggerFactory.getLogger(CometS3CredentialDispatcher.class);

  private static final ConcurrentHashMap<InstanceKey, CometS3CredentialProvider> INSTANCES =
      new ConcurrentHashMap<>();
  private static final CometS3AccessMode[] MODES = CometS3AccessMode.values();

  private CometS3CredentialDispatcher() {}

  /**
   * Reflects and initializes the named provider for {@code (FQCN, dispatchKey)} exactly once per
   * JVM. Subsequent calls with the same key are no-ops. Native code invokes this synchronously when
   * {@code CometS3CredentialBridge} is constructed at plan time, before any per-request {@link
   * #getCredentialsForPath} call. {@code catalogProperties} carries the unfiltered FileIO property
   * bag on the Iceberg path and is empty on the Parquet path.
   */
  public static void ensureInitialized(
      String providerClassName, String dispatchKey, Map<String, String> catalogProperties) {
    if (providerClassName == null || providerClassName.isEmpty()) {
      throw new IllegalArgumentException(
          "providerClassName is empty; native side should not call without a configured class");
    }
    InstanceKey key = new InstanceKey(providerClassName, dispatchKey == null ? "" : dispatchKey);
    Map<String, String> props =
        catalogProperties == null ? Collections.emptyMap() : catalogProperties;
    INSTANCES.computeIfAbsent(
        key,
        k -> {
          CometS3CredentialProvider provider = instantiate(k.providerClassName);
          provider.initialize(props);
          return provider;
        });
  }

  /**
   * Invoked by native code on every per-request credential fetch. The instance must have been
   * created by a prior {@link #ensureInitialized} call; otherwise this throws. {@code mode} is the
   * {@link CometS3AccessMode} ordinal.
   */
  public static CometS3Credentials getCredentialsForPath(
      String providerClassName, String dispatchKey, String bucket, String path, int mode)
      throws Exception {
    if (providerClassName == null || providerClassName.isEmpty()) {
      throw new IllegalArgumentException(
          "providerClassName is empty; native side should not call without a configured class");
    }
    if (mode < 0 || mode >= MODES.length) {
      throw new IllegalArgumentException("Invalid CometS3AccessMode ordinal: " + mode);
    }
    InstanceKey key = new InstanceKey(providerClassName, dispatchKey == null ? "" : dispatchKey);
    CometS3CredentialProvider provider = INSTANCES.get(key);
    if (provider == null) {
      throw new IllegalStateException(
          "CometS3CredentialProvider "
              + providerClassName
              + " (dispatchKey="
              + key.dispatchKey
              + ") was not initialized; ensureInitialized must be called before"
              + " getCredentialsForPath");
    }
    CometS3AccessMode accessMode = MODES[mode];
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Fetching credentials via {} (dispatchKey={}) for bucket={} path={} mode={}",
          providerClassName,
          key.dispatchKey,
          bucket,
          path,
          accessMode);
    }
    return provider.getCredentialsForPath(bucket, path, accessMode);
  }

  private static CometS3CredentialProvider instantiate(String providerClassName) {
    Class<?> clazz;
    try {
      clazz = Class.forName(providerClassName);
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

  private static final class InstanceKey {
    final String providerClassName;
    final String dispatchKey;

    InstanceKey(String providerClassName, String dispatchKey) {
      this.providerClassName = providerClassName;
      this.dispatchKey = dispatchKey;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof InstanceKey)) return false;
      InstanceKey other = (InstanceKey) o;
      return providerClassName.equals(other.providerClassName)
          && dispatchKey.equals(other.dispatchKey);
    }

    @Override
    public int hashCode() {
      return Objects.hash(providerClassName, dispatchKey);
    }
  }
}
