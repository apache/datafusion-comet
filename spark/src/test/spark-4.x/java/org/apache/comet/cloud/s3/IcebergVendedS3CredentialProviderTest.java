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

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sun.net.httpserver.HttpServer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

/**
 * Unit tests for {@link IcebergVendedS3CredentialProvider}. A local HTTP server stands in for the
 * REST catalog's credentials endpoint, so the refresh test exercises Iceberg's real {@code
 * VendedCredentialsProvider}.
 */
public class IcebergVendedS3CredentialProviderTest {

  private static final CometS3CredentialContext CONTEXT =
      new CometS3CredentialContext("bucket", "/warehouse/t/data/f.parquet", CometS3AccessMode.READ);

  private HttpServer server;
  private final AtomicInteger refreshCalls = new AtomicInteger();
  private long refreshedExpiryMillis;

  @Before
  public void startServer() throws IOException {
    refreshedExpiryMillis = System.currentTimeMillis() + 3_600_000L;
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/v1/credentials",
        exchange -> {
          refreshCalls.incrementAndGet();
          byte[] body =
              ("{\"storage-credentials\":[{\"prefix\":\"s3://bucket\",\"config\":{"
                      + "\"s3.access-key-id\":\"AKIA_REFRESHED\","
                      + "\"s3.secret-access-key\":\"secret_REFRESHED\","
                      + "\"s3.session-token\":\"token_REFRESHED\","
                      + "\"s3.session-token-expires-at-ms\":\""
                      + refreshedExpiryMillis
                      + "\"}}]}")
                  .getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().add("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, body.length);
          try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
          }
        });
    server.start();
  }

  @After
  public void stopServer() {
    server.stop(0);
  }

  private String catalogUri() {
    return "http://127.0.0.1:" + server.getAddress().getPort();
  }

  private static Map<String, String> vendedProps(long expiresAtMillis) {
    Map<String, String> props = new HashMap<>();
    props.put("s3.access-key-id", "AKIA_INITIAL");
    props.put("s3.secret-access-key", "secret_INITIAL");
    props.put("s3.session-token", "token_INITIAL");
    props.put("s3.session-token-expires-at-ms", Long.toString(expiresAtMillis));
    return props;
  }

  @Test
  public void refreshesExpiredVendedCredentialsThroughTheCatalogEndpoint() {
    Map<String, String> props = vendedProps(System.currentTimeMillis() - 60_000L);
    props.put("uri", catalogUri());
    props.put("client.refresh-credentials-endpoint", catalogUri() + "/v1/credentials");

    IcebergVendedS3CredentialProvider p = new IcebergVendedS3CredentialProvider();
    p.initialize(props);
    CometS3Credentials c = p.getCredentialsForPath(CONTEXT);

    assertEquals("AKIA_REFRESHED", c.getAccessKeyId());
    assertEquals("secret_REFRESHED", c.getSecretAccessKey());
    assertEquals("token_REFRESHED", c.getSessionToken());
    assertEquals(refreshedExpiryMillis, c.getExpirationEpochMillis());
    assertEquals(1, refreshCalls.get());
  }

  @Test
  public void reusesUnexpiredVendedCredentialsWithoutCallingTheEndpoint() {
    long expiresAt = System.currentTimeMillis() + 3_600_000L;
    Map<String, String> props = vendedProps(expiresAt);
    props.put("uri", catalogUri());
    props.put("client.refresh-credentials-endpoint", catalogUri() + "/v1/credentials");

    IcebergVendedS3CredentialProvider p = new IcebergVendedS3CredentialProvider();
    p.initialize(props);
    CometS3Credentials c = p.getCredentialsForPath(CONTEXT);

    assertEquals("AKIA_INITIAL", c.getAccessKeyId());
    assertEquals("token_INITIAL", c.getSessionToken());
    assertEquals(expiresAt, c.getExpirationEpochMillis());
    assertEquals(0, refreshCalls.get());
  }

  @Test
  public void usesStaticVendedCredentialsWhenTheCatalogReturnsNoRefreshEndpoint() {
    IcebergVendedS3CredentialProvider p = new IcebergVendedS3CredentialProvider();
    p.initialize(vendedProps(System.currentTimeMillis() + 3_600_000L));
    CometS3Credentials c = p.getCredentialsForPath(CONTEXT);

    assertEquals("AKIA_INITIAL", c.getAccessKeyId());
    assertEquals("secret_INITIAL", c.getSecretAccessKey());
    assertEquals("token_INITIAL", c.getSessionToken());
    assertEquals(0, refreshCalls.get());
  }

  @Test
  public void honorsRefreshDisabled() {
    Map<String, String> props = vendedProps(System.currentTimeMillis() - 60_000L);
    props.put("uri", catalogUri());
    props.put("client.refresh-credentials-endpoint", catalogUri() + "/v1/credentials");
    props.put("client.refresh-credentials-enabled", "false");

    IcebergVendedS3CredentialProvider p = new IcebergVendedS3CredentialProvider();
    p.initialize(props);
    CometS3Credentials c = p.getCredentialsForPath(CONTEXT);

    assertEquals("AKIA_INITIAL", c.getAccessKeyId());
    assertEquals(0, refreshCalls.get());
  }

  @Test
  public void staticKeysWithoutSessionTokenReportNoToken() {
    Map<String, String> props = new HashMap<>();
    props.put("s3.access-key-id", "AKIA_STATIC");
    props.put("s3.secret-access-key", "secret_STATIC");

    IcebergVendedS3CredentialProvider p = new IcebergVendedS3CredentialProvider();
    p.initialize(props);
    CometS3Credentials c = p.getCredentialsForPath(CONTEXT);

    assertEquals("AKIA_STATIC", c.getAccessKeyId());
    assertNull(c.getSessionToken());
    assertEquals(0L, c.getExpirationEpochMillis());
  }

  @Test
  public void getBeforeInitializeThrows() {
    IcebergVendedS3CredentialProvider p = new IcebergVendedS3CredentialProvider();
    assertThrows(IllegalStateException.class, () -> p.getCredentialsForPath(CONTEXT));
  }
}
