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
import java.util.concurrent.atomic.AtomicReference;

/**
 * Test-only {@link CometCloudCredentialProvider} that returns credentials previously installed by
 * {@link #installCredentials} and counts every invocation. Registered via {@code
 * META-INF/services/org.apache.comet.cloud.CometCloudCredentialProvider} on the test classpath.
 *
 * <p>State is held in static fields because {@link CometCloudCredentialDispatcher} caches a single
 * provider instance for the JVM lifetime; tests interact with that instance via these statics
 * rather than constructing their own.
 */
public final class MinioCometCredentialProvider implements CometCloudCredentialProvider {

  private static final AtomicReference<Credentials> CREDS = new AtomicReference<>();
  private static final AtomicInteger CALL_COUNT = new AtomicInteger(0);
  private static final AtomicReference<String> LAST_BUCKET = new AtomicReference<>();
  private static final AtomicReference<String> LAST_PATH = new AtomicReference<>();

  /** Install the credentials this provider should return. Called from test {@code beforeAll}. */
  public static void installCredentials(String accessKeyId, String secretAccessKey) {
    CREDS.set(new Credentials(accessKeyId, secretAccessKey));
  }

  public static int callCount() {
    return CALL_COUNT.get();
  }

  public static String lastBucket() {
    return LAST_BUCKET.get();
  }

  public static String lastPath() {
    return LAST_PATH.get();
  }

  /** Reset call tracking. Tests use this to take a clean snapshot before a query. */
  public static void resetCounters() {
    CALL_COUNT.set(0);
    LAST_BUCKET.set(null);
    LAST_PATH.set(null);
  }

  @Override
  public CometCredentials getCredentialsForPath(String bucket, String path) {
    CALL_COUNT.incrementAndGet();
    LAST_BUCKET.set(bucket);
    LAST_PATH.set(path);
    Credentials c = CREDS.get();
    if (c == null) {
      throw new IllegalStateException(
          "MinioCometCredentialProvider used before installCredentials() was called");
    }
    return new CometCredentials(c.accessKeyId, c.secretAccessKey, null, null, 0L);
  }

  private static final class Credentials {
    final String accessKeyId;
    final String secretAccessKey;

    Credentials(String accessKeyId, String secretAccessKey) {
      this.accessKeyId = accessKeyId;
      this.secretAccessKey = secretAccessKey;
    }
  }
}
