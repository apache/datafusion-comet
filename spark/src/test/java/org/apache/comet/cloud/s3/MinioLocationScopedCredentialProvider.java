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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Test {@link CometS3LocationScopedCredentialProvider} for suites that run against Minio. It
 * returns the locations installed with {@link #installLocations} and Minio's static credentials,
 * and records the path of every credential request, so a suite can assert which location served
 * each read. Minio does not enforce per-prefix policies here, so the recorded paths, not a 403,
 * show that routing worked.
 */
public final class MinioLocationScopedCredentialProvider
    implements CometS3LocationScopedCredentialProvider {

  private static final AtomicReference<CometS3Credentials> CREDS = new AtomicReference<>();
  private static final AtomicReference<List<String>> LOCATIONS =
      new AtomicReference<>(Collections.emptyList());
  private static final AtomicInteger LOCATION_CALL_COUNT = new AtomicInteger(0);

  /**
   * Counts provider instances. Not cleared by {@link #resetCounters}: the dispatcher keeps one
   * instance per registration for the life of the JVM.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  private static final Set<String> CREDENTIAL_PATHS = ConcurrentHashMap.newKeySet();

  public static void installCredentials(String accessKeyId, String secretAccessKey) {
    CREDS.set(new CometS3Credentials(accessKeyId, secretAccessKey, null, 0L));
  }

  public static void installLocations(List<String> locations) {
    LOCATIONS.set(List.copyOf(locations));
  }

  public static int locationCallCount() {
    return LOCATION_CALL_COUNT.get();
  }

  public static int initCount() {
    return INIT_COUNT.get();
  }

  /** The paths passed to {@link #getCredentialsForPath} since the last reset. */
  public static Set<String> credentialPaths() {
    return Set.copyOf(CREDENTIAL_PATHS);
  }

  public static void resetCounters() {
    LOCATION_CALL_COUNT.set(0);
    CREDENTIAL_PATHS.clear();
  }

  @Override
  public void initialize(Map<String, String> catalogProperties) {
    INIT_COUNT.incrementAndGet();
  }

  @Override
  public CometS3Credentials getCredentialsForPath(CometS3CredentialContext context) {
    CREDENTIAL_PATHS.add(context.getPath());
    CometS3Credentials creds = CREDS.get();
    if (creds == null) {
      throw new IllegalStateException(
          "MinioLocationScopedCredentialProvider.installCredentials was not called");
    }
    return creds;
  }

  @Override
  public List<String> getPolicyLocations(String bucket) {
    LOCATION_CALL_COUNT.incrementAndGet();
    return LOCATIONS.get();
  }
}
