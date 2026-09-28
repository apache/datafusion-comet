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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSSessionCredentials;

/**
 * Maps AWS SDK v1 {@link AWSCredentials} onto {@link CometS3Credentials}. Compiled only into
 * spark-3.4 / 3.5 builds (spark-3.x source set), directly against SDK v1.
 */
final class SdkCredentialExtraction {

  private SdkCredentialExtraction() {}

  static CometS3Credentials toCometCredentials(String bucket, AWSCredentials creds) {
    String accessKeyId = creds.getAWSAccessKeyId();
    String secretKey = creds.getAWSSecretKey();
    if (accessKeyId == null || accessKeyId.isEmpty() || secretKey == null || secretKey.isEmpty()) {
      // The chain resolved anonymous or empty credentials (e.g. AnonymousAWSCredentialsProvider,
      // used for public buckets, or a provider that returns blank keys). The SPI has no way to
      // express "anonymous", and the native reader would otherwise sign with blank keys and 403,
      // so fail with a clear cause instead. To read this bucket anonymously, opt it out with an
      // empty fs.s3a.bucket.<bucket>.comet.credential.provider.class.
      throw new IllegalStateException(
          "The credential provider chain resolved anonymous or empty credentials for bucket "
              + bucket
              + ". The Comet S3 credential adapters cannot serve anonymous access; opt this bucket"
              + " out with an empty fs.s3a.bucket."
              + bucket
              + ".comet.credential.provider.class so the native reader accesses it unsigned.");
    }
    String sessionToken = null;
    if (creds instanceof AWSSessionCredentials) {
      sessionToken = ((AWSSessionCredentials) creds).getSessionToken();
    }
    // The v1 base interface exposes no expiration; report 0 (unknown). Safe: the Parquet path
    // ignores expiration and the Iceberg path applies a bounded default TTL.
    return new CometS3Credentials(accessKeyId, secretKey, sessionToken, 0L);
  }
}
