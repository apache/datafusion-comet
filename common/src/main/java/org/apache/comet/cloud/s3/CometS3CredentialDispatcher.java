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

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JNI entry point invoked from native code to resolve {@link CometS3CredentialProvider}.
 *
 * <p>The provider is resolved once via {@link ServiceLoader} and cached in a {@code static final}
 * field. A query falling back from Comet to Spark mid-execution therefore sees identical
 * credentials, since both paths resolve from the same executor classpath.
 *
 * <p>Multiple registered impls fail fast at class-load; chaining is a vendor-side concern.
 */
public final class CometS3CredentialDispatcher {

  private static final Logger LOG = LoggerFactory.getLogger(CometS3CredentialDispatcher.class);

  private static final CometS3CredentialProvider PROVIDER = resolve();
  private static final CometS3AccessMode[] MODES = CometS3AccessMode.values();

  private CometS3CredentialDispatcher() {}

  public static boolean isProviderRegistered() {
    return PROVIDER != null;
  }

  /** Invoked by native code. {@code mode} is the {@link CometS3AccessMode} ordinal. */
  public static CometS3Credentials getCredentialsForPath(String bucket, String path, int mode)
      throws Exception {
    if (PROVIDER == null) {
      throw new IllegalStateException(
          "No CometS3CredentialProvider registered; check META-INF/services on the classpath");
    }
    if (mode < 0 || mode >= MODES.length) {
      throw new IllegalArgumentException("Invalid CometS3AccessMode ordinal: " + mode);
    }
    CometS3AccessMode accessMode = MODES[mode];
    if (LOG.isDebugEnabled()) {
      LOG.debug("Fetching credentials for bucket={} path={} mode={}", bucket, path, accessMode);
    }
    return PROVIDER.getCredentialsForPath(bucket, path, accessMode);
  }

  private static CometS3CredentialProvider resolve() {
    List<CometS3CredentialProvider> impls = new ArrayList<>();
    for (CometS3CredentialProvider impl : ServiceLoader.load(CometS3CredentialProvider.class)) {
      impls.add(impl);
    }
    if (impls.isEmpty()) {
      LOG.info(
          "No CometS3CredentialProvider registered; native S3 readers will use the default "
              + "AWS credential chain");
      return null;
    }
    if (impls.size() > 1) {
      List<String> names =
          impls.stream().map(p -> p.getClass().getName()).collect(Collectors.toList());
      throw new IllegalStateException(
          "Multiple CometS3CredentialProvider impls on classpath: " + names);
    }
    CometS3CredentialProvider provider = impls.get(0);
    LOG.info("Registered CometS3CredentialProvider: {}", provider.getClass().getName());
    return provider;
  }
}
