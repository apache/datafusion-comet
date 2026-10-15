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

import java.time.Instant;
import java.util.Optional;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

/**
 * Maps AWS SDK v2 {@link AwsCredentials} onto {@link CometS3Credentials}. Compiled only into
 * spark-4.0+ builds (spark-4.x source set), directly against SDK v2.
 */
final class SdkCredentialExtraction {

  private SdkCredentialExtraction() {}

  static CometS3Credentials toCometCredentials(String bucket, AwsCredentials creds) {
    String accessKeyId = creds.accessKeyId();
    String secretAccessKey = creds.secretAccessKey();
    if (accessKeyId == null
        || accessKeyId.isEmpty()
        || secretAccessKey == null
        || secretAccessKey.isEmpty()) {
      // The chain resolved anonymous or empty credentials (e.g. AnonymousCredentialsProvider, used
      // for public buckets, or a provider that returns blank keys). The SPI has no way to express
      // "anonymous", and the native reader would otherwise sign with blank keys and 403, so fail
      // with a clear cause instead. To read this bucket anonymously, opt it out with an empty
      // fs.s3a.bucket.<bucket>.comet.credential.provider.class.
      throw new IllegalStateException(
          "The credential provider chain resolved anonymous or empty credentials for bucket "
              + bucket
              + ". The Comet S3 credential adapters cannot serve anonymous access; opt this bucket"
              + " out with an empty fs.s3a.bucket."
              + bucket
              + ".comet.credential.provider.class so the native reader accesses it unsigned.");
    }
    String sessionToken = null;
    long expirationEpochMillis = 0L;
    if (creds instanceof AwsSessionCredentials) {
      AwsSessionCredentials session = (AwsSessionCredentials) creds;
      sessionToken = session.sessionToken();
      Optional<Instant> expiration = session.expirationTime();
      if (expiration.isPresent()) {
        expirationEpochMillis = expiration.get().toEpochMilli();
      }
    }
    return new CometS3Credentials(
        accessKeyId, secretAccessKey, sessionToken, expirationEpochMillis);
  }
}
