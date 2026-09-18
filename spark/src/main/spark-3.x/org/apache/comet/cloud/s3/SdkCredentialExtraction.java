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

  static CometS3Credentials toCometCredentials(AWSCredentials creds) {
    String sessionToken = null;
    if (creds instanceof AWSSessionCredentials) {
      sessionToken = ((AWSSessionCredentials) creds).getSessionToken();
    }
    // The v1 base interface exposes no expiration; report 0 (unknown). Safe: the Parquet path
    // ignores expiration and the Iceberg path applies a bounded default TTL.
    return new CometS3Credentials(creds.getAWSAccessKeyId(), creds.getAWSSecretKey(), sessionToken, 0L);
  }
}
