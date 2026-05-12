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

package org.apache.comet.cloud;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test-only provider registered via {@code META-INF/services} in the test classpath. State is
 * static because {@link CometCloudCredentialDispatcher} caches a single instance in a {@code static
 * final} field for the JVM lifetime; tests reset state via {@link #reset()}.
 */
public class TestCometCloudCredentialProvider implements CometCloudCredentialProvider {

  static final AtomicInteger callCount = new AtomicInteger(0);
  static volatile String lastBucket;
  static volatile String lastPath;
  static volatile RuntimeException throwOnNext;
  static volatile CometCredentials nextResult =
      new CometCredentials("AKIATEST", "secret", "session-tok", "us-east-1", 0L);

  static void reset() {
    callCount.set(0);
    lastBucket = null;
    lastPath = null;
    throwOnNext = null;
    nextResult = new CometCredentials("AKIATEST", "secret", "session-tok", "us-east-1", 0L);
  }

  @Override
  public CometCredentials getCredentialsForPath(String bucket, String path) {
    callCount.incrementAndGet();
    lastBucket = bucket;
    lastPath = path;
    RuntimeException toThrow = throwOnNext;
    if (toThrow != null) {
      throwOnNext = null;
      throw toThrow;
    }
    return nextResult;
  }
}
