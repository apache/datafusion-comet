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

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/**
 * Unit tests for {@link IcebergRESTVendedS3Provider}. Feeds the wrapped {@code
 * VendedCredentialsProvider} a fully-populated {@code s3.*} bag so its {@code CachedSupplier}
 * short-circuits without calling the REST {@code credentials.uri} endpoint. Refresh-via-REST
 * coverage lives upstream in Iceberg.
 */
public class IcebergRESTVendedS3ProviderTest {

  private static Map<String, String> staticVendedProps() {
    Map<String, String> props = new HashMap<>();
    // Required by VendedCredentialsProvider.create; not actually contacted in this test.
    props.put("uri", "http://localhost:0/catalog");
    props.put("credentials.uri", "http://localhost:0/credentials");
    // Required four keys for the in-properties short-circuit path.
    props.put("s3.access-key-id", "AKIA_TEST");
    props.put("s3.secret-access-key", "secret_TEST");
    props.put("s3.session-token", "token_TEST");
    props.put(
        "s3.session-token-expires-at-ms", Long.toString(System.currentTimeMillis() + 3_600_000L));
    return props;
  }

  @Test
  public void initializeThenGetReturnsVendedCredentials() {
    IcebergRESTVendedS3Provider p = new IcebergRESTVendedS3Provider();
    p.initialize(staticVendedProps());

    CometS3Credentials c =
        p.getCredentialsForPath(
            new CometS3CredentialContext("bucket", "/k", CometS3AccessMode.READ));

    assertEquals("AKIA_TEST", c.getAccessKeyId());
    assertEquals("secret_TEST", c.getSecretAccessKey());
    assertEquals("token_TEST", c.getSessionToken());
    // Wrapper publishes 0 since VendedCredentialsProvider's CachedSupplier owns the expiry.
    assertEquals(0L, c.getExpirationEpochMillis());
  }

  @Test
  public void getBeforeInitializeThrows() {
    IcebergRESTVendedS3Provider p = new IcebergRESTVendedS3Provider();
    assertThrows(
        IllegalStateException.class,
        () ->
            p.getCredentialsForPath(
                new CometS3CredentialContext("bucket", "/k", CometS3AccessMode.READ)));
  }

  @Test
  public void multipleCallsServedByCache() {
    IcebergRESTVendedS3Provider p = new IcebergRESTVendedS3Provider();
    p.initialize(staticVendedProps());

    CometS3Credentials a =
        p.getCredentialsForPath(new CometS3CredentialContext("b", "/k", CometS3AccessMode.READ));
    CometS3Credentials b =
        p.getCredentialsForPath(new CometS3CredentialContext("b", "/k2", CometS3AccessMode.READ));
    CometS3Credentials c =
        p.getCredentialsForPath(new CometS3CredentialContext("b", "/k3", CometS3AccessMode.READ));

    // CachedSupplier hands out the same identity-equal credential until expiry; we assert by
    // value so the test does not depend on AWS SDK internal caching semantics.
    assertEquals(a.getAccessKeyId(), b.getAccessKeyId());
    assertEquals(a.getAccessKeyId(), c.getAccessKeyId());
    assertEquals(a.getSecretAccessKey(), b.getSecretAccessKey());
    assertEquals(a.getSessionToken(), b.getSessionToken());
  }
}
