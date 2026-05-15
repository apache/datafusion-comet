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
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JNI entry point invoked from native code to resolve a {@link CometS3CredentialProvider}.
 *
 * <p>Native code passes the FQCN named in {@code fs.s3a.comet.credential.provider.class} (or its
 * per-bucket / Iceberg-namespaced variants). Each named class is instantiated once via reflection
 * and cached, so a single executor JVM can serve multiple providers (e.g. one per bucket).
 */
public final class CometS3CredentialDispatcher {

  private static final Logger LOG = LoggerFactory.getLogger(CometS3CredentialDispatcher.class);

  private static final ConcurrentHashMap<String, CometS3CredentialProvider> INSTANCES =
      new ConcurrentHashMap<>();
  private static final CometS3AccessMode[] MODES = CometS3AccessMode.values();

  private CometS3CredentialDispatcher() {}

  /**
   * Invoked by native code. {@code mode} is the {@link CometS3AccessMode} ordinal.
   *
   * @param providerClassName FQCN configured in {@code fs.s3a.comet.credential.provider.class}
   */
  public static CometS3Credentials getCredentialsForPath(
      String providerClassName, String bucket, String path, int mode) throws Exception {
    if (providerClassName == null || providerClassName.isEmpty()) {
      throw new IllegalArgumentException(
          "providerClassName is empty; native side should not call without a configured class");
    }
    if (mode < 0 || mode >= MODES.length) {
      throw new IllegalArgumentException("Invalid CometS3AccessMode ordinal: " + mode);
    }
    CometS3CredentialProvider provider = resolve(providerClassName);
    CometS3AccessMode accessMode = MODES[mode];
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Fetching credentials via {} for bucket={} path={} mode={}",
          providerClassName,
          bucket,
          path,
          accessMode);
    }
    return provider.getCredentialsForPath(bucket, path, accessMode);
  }

  private static CometS3CredentialProvider resolve(String providerClassName) {
    return INSTANCES.computeIfAbsent(providerClassName, CometS3CredentialDispatcher::instantiate);
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
}
