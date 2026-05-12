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

import java.util.Objects;

/**
 * Credentials returned by a {@link CometCloudCredentialProvider}, consumed by Comet's native code
 * via JNI field accessors.
 *
 * <p>{@code sessionToken} is null for non-STS credentials; {@code region} is null when the provider
 * has no opinion (the native side falls back to its configured region). {@code
 * expirationEpochMillis} is {@code 0} when the provider does not track expiration; in that case
 * Comet will not pre-emptively refresh and relies on the provider to return fresh credentials on
 * each call.
 */
public final class CometCredentials {

  private final String accessKeyId;
  private final String secretAccessKey;
  private final String sessionToken;
  private final String region;
  private final long expirationEpochMillis;

  public CometCredentials(
      String accessKeyId,
      String secretAccessKey,
      String sessionToken,
      String region,
      long expirationEpochMillis) {
    this.accessKeyId = Objects.requireNonNull(accessKeyId, "accessKeyId");
    this.secretAccessKey = Objects.requireNonNull(secretAccessKey, "secretAccessKey");
    this.sessionToken = sessionToken;
    this.region = region;
    this.expirationEpochMillis = expirationEpochMillis;
  }

  public String getAccessKeyId() {
    return accessKeyId;
  }

  public String getSecretAccessKey() {
    return secretAccessKey;
  }

  public String getSessionToken() {
    return sessionToken;
  }

  public String getRegion() {
    return region;
  }

  public long getExpirationEpochMillis() {
    return expirationEpochMillis;
  }
}
