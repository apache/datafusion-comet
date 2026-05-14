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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test-only provider registered via {@code META-INF/services}. State is static because the
 * dispatcher caches a single provider instance for the JVM lifetime.
 */
public class TestCometS3CredentialProvider implements CometS3CredentialProvider {

  static final AtomicInteger callCount = new AtomicInteger(0);
  static volatile String lastBucket;
  static volatile String lastPath;
  static volatile CometS3AccessMode lastMode;
  static volatile RuntimeException throwOnNext;
  static volatile CometS3Credentials nextResult =
      new CometS3Credentials("AKIATEST", "secret", "session-tok", 0L);

  static void reset() {
    callCount.set(0);
    lastBucket = null;
    lastPath = null;
    lastMode = null;
    throwOnNext = null;
    nextResult = new CometS3Credentials("AKIATEST", "secret", "session-tok", 0L);
  }

  @Override
  public CometS3Credentials getCredentialsForPath(
      String bucket, String path, CometS3AccessMode mode) {
    callCount.incrementAndGet();
    lastBucket = bucket;
    lastPath = path;
    lastMode = mode;
    RuntimeException toThrow = throwOnNext;
    if (toThrow != null) {
      throwOnNext = null;
      throw toThrow;
    }
    return nextResult;
  }
}
