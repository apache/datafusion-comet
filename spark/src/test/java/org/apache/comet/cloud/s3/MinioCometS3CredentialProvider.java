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
import java.util.concurrent.atomic.AtomicReference;

/**
 * Test-only {@link CometS3CredentialProvider} registered via {@code META-INF/services}. Returns
 * Minio's static credentials once {@link #installCredentials} has been called and counts every
 * invocation so suites can assert the bridge was actually used.
 */
public final class MinioCometS3CredentialProvider implements CometS3CredentialProvider {

  private static final AtomicReference<Credentials> CREDS = new AtomicReference<>();
  private static final AtomicInteger CALL_COUNT = new AtomicInteger(0);
  private static final AtomicReference<String> LAST_BUCKET = new AtomicReference<>();
  private static final AtomicReference<String> LAST_PATH = new AtomicReference<>();

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

  public static void resetCounters() {
    CALL_COUNT.set(0);
    LAST_BUCKET.set(null);
    LAST_PATH.set(null);
  }

  @Override
  public CometS3Credentials getCredentialsForPath(
      String bucket, String path, CometS3AccessMode mode) {
    CALL_COUNT.incrementAndGet();
    LAST_BUCKET.set(bucket);
    LAST_PATH.set(path);
    Credentials c = CREDS.get();
    if (c == null) {
      throw new IllegalStateException(
          "MinioCometS3CredentialProvider used before installCredentials() was called");
    }
    return new CometS3Credentials(c.accessKeyId, c.secretAccessKey, null, 0L);
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
