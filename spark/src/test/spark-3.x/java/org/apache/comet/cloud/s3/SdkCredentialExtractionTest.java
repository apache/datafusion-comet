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

import org.junit.Test;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/** AWS SDK v1 credential extraction (spark-3.x source set). */
public class SdkCredentialExtractionTest {

  @Test
  public void basicCredentialsHaveNoSessionOrExpiry() {
    CometS3Credentials creds =
        SdkCredentialExtraction.toCometCredentials(new BasicAWSCredentials("AK", "SK"));
    assertEquals("AK", creds.getAccessKeyId());
    assertEquals("SK", creds.getSecretAccessKey());
    assertNull(creds.getSessionToken());
    assertEquals(0L, creds.getExpirationEpochMillis());
  }

  @Test
  public void sessionCredentialsCarryTokenAndUnknownExpiry() {
    CometS3Credentials creds =
        SdkCredentialExtraction.toCometCredentials(
            new BasicSessionCredentials("AK", "SK", "TOKEN"));
    assertEquals("TOKEN", creds.getSessionToken());
    // v1 exposes no expiration; always unknown (0).
    assertEquals(0L, creds.getExpirationEpochMillis());
  }
}
