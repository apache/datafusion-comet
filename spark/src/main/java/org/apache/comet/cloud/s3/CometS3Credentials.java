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

import java.util.Objects;

import org.apache.comet.annotation.Public;

/**
 * Credentials returned by a {@link CometS3CredentialProvider}. Field names are read back over JNI
 * by name and are part of the cross-language contract. {@code sessionToken} is null for non-STS
 * credentials.
 *
 * <p>{@code expirationEpochMillis} is when the credential stops working, in milliseconds since the
 * epoch. Comet reuses the credential until five minutes before it, then asks the provider again.
 * {@code 0} means unknown and {@code Long.MAX_VALUE} means the credential does not expire. Comet
 * does not reuse either, and asks again for every request that does not overlap a call already in
 * flight. A value before 2000, almost always seconds sent as milliseconds, is treated as unknown. A
 * provider whose credentials can be revoked before the expiry it reports should report an earlier
 * one, or {@code 0}.
 */
@Public
public final class CometS3Credentials {

  private final String accessKeyId;
  private final String secretAccessKey;
  private final String sessionToken;
  private final long expirationEpochMillis;

  public CometS3Credentials(
      String accessKeyId, String secretAccessKey, String sessionToken, long expirationEpochMillis) {
    this.accessKeyId = Objects.requireNonNull(accessKeyId, "accessKeyId");
    this.secretAccessKey = Objects.requireNonNull(secretAccessKey, "secretAccessKey");
    this.sessionToken = sessionToken;
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

  public long getExpirationEpochMillis() {
    return expirationEpochMillis;
  }
}
