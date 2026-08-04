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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Test-only {@link CometS3CredentialProvider} named via {@code
 * spark.hadoop.fs.s3a.comet.credential.provider.class} (and the per-catalog Iceberg form). Returns
 * Minio's static credentials once {@link #installCredentials} has been called and counts every
 * invocation so suites can assert the bridge was actually used.
 */
public final class MinioCometS3CredentialProvider implements CometS3CredentialProvider {

  /**
   * Property key used by tests to tag a per-catalog provider instance. The dispatcher key alone is
   * not visible to {@code initialize(Map)}, so suites that want to identify which catalog's
   * instance they are inspecting put a discriminator under this key on the catalog config.
   */
  public static final String TEST_INSTANCE_TAG = "comet.test-instance-tag";

  private static final AtomicReference<Credentials> CREDS = new AtomicReference<>();
  private static final AtomicInteger CALL_COUNT = new AtomicInteger(0);
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);
  private static final AtomicReference<String> LAST_BUCKET = new AtomicReference<>();
  private static final AtomicReference<String> LAST_PATH = new AtomicReference<>();
  private static final AtomicReference<Map<String, String>> LAST_INIT_PROPS =
      new AtomicReference<>();

  /**
   * Captures one entry per `initialize(Map)` invocation, keyed by the value of {@link
   * #TEST_INSTANCE_TAG} in the supplied map. Lets multi-catalog tests look up the per-instance init
   * bag without relying on the most-recent-wins {@link #LAST_INIT_PROPS}.
   */
  private static final ConcurrentHashMap<String, Map<String, String>> INIT_BY_TAG =
      new ConcurrentHashMap<>();

  public static void installCredentials(String accessKeyId, String secretAccessKey) {
    CREDS.set(new Credentials(accessKeyId, secretAccessKey));
  }

  public static int callCount() {
    return CALL_COUNT.get();
  }

  public static int initCount() {
    return INIT_COUNT.get();
  }

  public static String lastBucket() {
    return LAST_BUCKET.get();
  }

  public static String lastPath() {
    return LAST_PATH.get();
  }

  public static Map<String, String> lastInitProperties() {
    return LAST_INIT_PROPS.get();
  }

  public static Map<String, String> initPropertiesForTag(String tag) {
    return INIT_BY_TAG.get(tag);
  }

  public static void resetCounters() {
    CALL_COUNT.set(0);
    INIT_COUNT.set(0);
    LAST_BUCKET.set(null);
    LAST_PATH.set(null);
    LAST_INIT_PROPS.set(null);
    INIT_BY_TAG.clear();
  }

  @Override
  public void initialize(Map<String, String> catalogProperties) {
    INIT_COUNT.incrementAndGet();
    Map<String, String> snapshot =
        Collections.unmodifiableMap(new java.util.HashMap<>(catalogProperties));
    LAST_INIT_PROPS.set(snapshot);
    String tag = catalogProperties.get(TEST_INSTANCE_TAG);
    if (tag != null) {
      INIT_BY_TAG.put(tag, snapshot);
    }
  }

  @Override
  public CometS3Credentials getCredentialsForPath(CometS3CredentialContext context) {
    CALL_COUNT.incrementAndGet();
    LAST_BUCKET.set(context.getBucket());
    LAST_PATH.set(context.getPath());
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
