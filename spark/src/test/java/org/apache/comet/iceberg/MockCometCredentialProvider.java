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

package org.apache.comet.iceberg;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Mock catalog-scoped credential provider for testing. Tracks initialization and call counts.
 * Returns configurable credentials that can simulate rotation.
 */
public class MockCometCredentialProvider implements CometCredentialProvider {

  private static final long serialVersionUID = 1L;

  private static final AtomicInteger initCount = new AtomicInteger(0);
  private static final AtomicInteger resolveCount = new AtomicInteger(0);
  private static volatile ResolveContext lastContext;
  private static volatile Map<String, String> lastCatalogProperties;

  private String accessKeyId = "test-access-key";
  private String secretAccessKey = "test-secret-key";
  private String sessionToken = "test-session-token";

  @Override
  public void initialize(Map<String, String> catalogProperties) {
    initCount.incrementAndGet();
    lastCatalogProperties = catalogProperties;

    if (catalogProperties.containsKey("s3.access-key-id")) {
      accessKeyId = catalogProperties.get("s3.access-key-id");
    }
    if (catalogProperties.containsKey("s3.secret-access-key")) {
      secretAccessKey = catalogProperties.get("s3.secret-access-key");
    }
    if (catalogProperties.containsKey("s3.session-token")) {
      sessionToken = catalogProperties.get("s3.session-token");
    }
  }

  @Override
  public String[] resolveCredentials(ResolveContext context) {
    resolveCount.incrementAndGet();
    lastContext = context;
    long expiresAt = System.currentTimeMillis() + 3600_000;
    return new String[] {accessKeyId, secretAccessKey, sessionToken, String.valueOf(expiresAt)};
  }

  public static int getInitCount() {
    return initCount.get();
  }

  public static int getResolveCount() {
    return resolveCount.get();
  }

  public static String getLastTableLocation() {
    ResolveContext ctx = lastContext;
    return ctx == null ? null : ctx.tableLocation();
  }

  public static ResolveContext getLastContext() {
    return lastContext;
  }

  public static Map<String, String> getLastCatalogProperties() {
    return lastCatalogProperties;
  }

  public static void reset() {
    initCount.set(0);
    resolveCount.set(0);
    lastContext = null;
    lastCatalogProperties = null;
  }
}
