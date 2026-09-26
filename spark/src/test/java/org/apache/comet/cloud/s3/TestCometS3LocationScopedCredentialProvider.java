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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Test-only {@link CometS3LocationScopedCredentialProvider}. State is static because the dispatcher
 * caches one instance per (FQCN, dispatchKey) for the JVM lifetime.
 */
public class TestCometS3LocationScopedCredentialProvider
    implements CometS3LocationScopedCredentialProvider {

  static final AtomicInteger callCount = new AtomicInteger(0);
  static final AtomicInteger locationCallCount = new AtomicInteger(0);
  static final AtomicReference<List<String>> nextLocations =
      new AtomicReference<>(Collections.emptyList());
  static volatile String lastBucket;
  static volatile Exception throwOnNextLocationCall;

  static void reset() {
    callCount.set(0);
    locationCallCount.set(0);
    nextLocations.set(Collections.emptyList());
    lastBucket = null;
    throwOnNextLocationCall = null;
  }

  @Override
  public void initialize(Map<String, String> catalogProperties) {}

  @Override
  public CometS3Credentials getCredentialsForPath(CometS3CredentialContext context) {
    callCount.incrementAndGet();
    return new CometS3Credentials("AKIASCOPED", "secret", "session-tok", 0L);
  }

  @Override
  public List<String> getPolicyLocations(String bucket) throws Exception {
    locationCallCount.incrementAndGet();
    lastBucket = bucket;
    Exception toThrow = throwOnNextLocationCall;
    if (toThrow != null) {
      throwOnNextLocationCall = null;
      throw toThrow;
    }
    return nextLocations.get();
  }
}
