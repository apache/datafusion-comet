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

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Test-only {@link CometS3ScopedCredentialProvider} paired with {@link
 * CometS3ScopedCredentialProviderTest}. State is static because the dispatcher caches one instance
 * per (FQCN, dispatchKey) for the JVM lifetime, so per-test observation must survive across handle
 * lookups.
 */
public class TestCometS3ScopedCredentialProvider implements CometS3ScopedCredentialProvider {

  static final AtomicInteger callCount = new AtomicInteger(0);
  static final AtomicInteger policyCallCount = new AtomicInteger(0);
  static final AtomicReference<List<String>> nextPolicyLocations = new AtomicReference<>(List.of());
  static volatile String lastBucket;
  static volatile String lastPath;
  static volatile CometS3AccessMode lastMode;
  static volatile Exception throwOnNextPolicyCall;

  static void reset() {
    callCount.set(0);
    policyCallCount.set(0);
    nextPolicyLocations.set(List.of());
    lastBucket = null;
    lastPath = null;
    lastMode = null;
    throwOnNextPolicyCall = null;
  }

  @Override
  public void initialize(Map<String, String> catalogProperties) {}

  @Override
  public CometS3Credentials getCredentialsForPath(CometS3CredentialContext context) {
    callCount.incrementAndGet();
    return new CometS3Credentials("AKIASCOPED", "secret", "session-tok", 0L);
  }

  @Override
  public List<String> getPolicyLocationsFor(CometS3CredentialContext context) throws Exception {
    policyCallCount.incrementAndGet();
    lastBucket = context.getBucket();
    lastPath = context.getPath();
    lastMode = context.getMode();
    Exception toThrow = throwOnNextPolicyCall;
    if (toThrow != null) {
      throwOnNextPolicyCall = null;
      throw toThrow;
    }
    return nextPolicyLocations.get();
  }
}
