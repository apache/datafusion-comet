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
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class CometS3CredentialDispatcherTest {

  private static final String TEST_PROVIDER = TestCometS3CredentialProvider.class.getName();
  private static final String DK = "test-dispatch-key";
  private static final int READ = CometS3AccessMode.READ.ordinal();
  private static final int WRITE = CometS3AccessMode.WRITE.ordinal();

  @Before
  public void resetTestProvider() {
    TestCometS3CredentialProvider.reset();
  }

  private void init() {
    CometS3CredentialDispatcher.ensureInitialized(TEST_PROVIDER, DK, Collections.emptyMap());
  }

  @Test
  public void getCredentialsRoundTripsThroughProvider() throws Exception {
    init();
    CometS3Credentials creds =
        CometS3CredentialDispatcher.getCredentialsForPath(
            TEST_PROVIDER, DK, "my-bucket", "path/to/object", READ);

    assertNotNull(creds);
    assertEquals("AKIATEST", creds.getAccessKeyId());
    assertEquals("secret", creds.getSecretAccessKey());
    assertEquals("session-tok", creds.getSessionToken());
    assertEquals(0L, creds.getExpirationEpochMillis());

    assertEquals(1, TestCometS3CredentialProvider.callCount.get());
    assertEquals("my-bucket", TestCometS3CredentialProvider.lastBucket);
    assertEquals("path/to/object", TestCometS3CredentialProvider.lastPath);
    assertEquals(CometS3AccessMode.READ, TestCometS3CredentialProvider.lastMode);
  }

  @Test
  public void writeModeIsForwarded() throws Exception {
    init();
    CometS3CredentialDispatcher.getCredentialsForPath(TEST_PROVIDER, DK, "b", "k", WRITE);
    assertEquals(CometS3AccessMode.WRITE, TestCometS3CredentialProvider.lastMode);
  }

  @Test
  public void unknownModeRejected() {
    init();
    assertThrows(
        IllegalArgumentException.class,
        () -> CometS3CredentialDispatcher.getCredentialsForPath(TEST_PROVIDER, DK, "b", "k", 99));
  }

  @Test
  public void emptyClassNameRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> CometS3CredentialDispatcher.ensureInitialized("", DK, Collections.emptyMap()));
    assertThrows(
        IllegalArgumentException.class,
        () -> CometS3CredentialDispatcher.ensureInitialized(null, DK, Collections.emptyMap()));
    assertThrows(
        IllegalArgumentException.class,
        () -> CometS3CredentialDispatcher.getCredentialsForPath("", DK, "b", "k", READ));
  }

  @Test
  public void missingClassReportsActionableError() {
    Exception thrown =
        assertThrows(
            IllegalStateException.class,
            () ->
                CometS3CredentialDispatcher.ensureInitialized(
                    "com.does.not.Exist", DK, Collections.emptyMap()));
    assertTrue(thrown.getMessage().contains("not found"));
  }

  @Test
  public void classNotImplementingInterfaceRejected() {
    Exception thrown =
        assertThrows(
            IllegalStateException.class,
            () ->
                CometS3CredentialDispatcher.ensureInitialized(
                    NotACredentialProvider.class.getName(), DK, Collections.emptyMap()));
    assertTrue(thrown.getMessage().contains("does not implement"));
  }

  @Test
  public void classWithoutNoArgCtorRejected() {
    Exception thrown =
        assertThrows(
            IllegalStateException.class,
            () ->
                CometS3CredentialDispatcher.ensureInitialized(
                    NoNoArgCtorProvider.class.getName(), DK, Collections.emptyMap()));
    assertTrue(thrown.getMessage().contains("no-arg constructor"));
  }

  @Test
  public void getWithoutEnsureInitializedThrows() {
    Exception thrown =
        assertThrows(
            IllegalStateException.class,
            () ->
                CometS3CredentialDispatcher.getCredentialsForPath(
                    TEST_PROVIDER, "never-initialized", "b", "k", READ));
    assertTrue(thrown.getMessage().contains("not initialized"));
  }

  @Test
  public void initializeCalledExactlyOncePerKey() {
    Map<String, String> props = new HashMap<>();
    props.put("tenant-id", "T1");

    CometS3CredentialDispatcher.ensureInitialized(TEST_PROVIDER, "cat-a", props);
    CometS3CredentialDispatcher.ensureInitialized(TEST_PROVIDER, "cat-a", props);
    CometS3CredentialDispatcher.ensureInitialized(TEST_PROVIDER, "cat-a", props);

    assertEquals(1, TestCometS3CredentialProvider.initCount.get());
  }

  @Test
  public void distinctDispatchKeysIsolateInstances() throws Exception {
    Map<String, String> propsA = new HashMap<>();
    propsA.put("tenant-id", "T-A");
    Map<String, String> propsB = new HashMap<>();
    propsB.put("tenant-id", "T-B");

    CometS3CredentialDispatcher.ensureInitialized(TEST_PROVIDER, "iso-cat-a", propsA);
    CometS3CredentialDispatcher.ensureInitialized(TEST_PROVIDER, "iso-cat-b", propsB);

    assertEquals(2, TestCometS3CredentialProvider.initCount.get());

    CometS3CredentialDispatcher.getCredentialsForPath(TEST_PROVIDER, "iso-cat-a", "b", "k", READ);
    assertEquals("T-A", TestCometS3CredentialProvider.lastTenantSeen);

    CometS3CredentialDispatcher.getCredentialsForPath(TEST_PROVIDER, "iso-cat-b", "b", "k", READ);
    assertEquals("T-B", TestCometS3CredentialProvider.lastTenantSeen);
  }

  @Test
  public void providerExceptionsPropagate() {
    init();
    IllegalStateException boom = new IllegalStateException("simulated STS failure");
    TestCometS3CredentialProvider.throwOnNext = boom;

    Exception thrown =
        assertThrows(
            Exception.class,
            () ->
                CometS3CredentialDispatcher.getCredentialsForPath(
                    TEST_PROVIDER, DK, "b", "k", READ));
    assertSame(boom, thrown);
  }

  @Test
  public void nullSessionTokenAllowed() throws Exception {
    init();
    TestCometS3CredentialProvider.nextResult = new CometS3Credentials("AKIA", "sec", null, 0L);

    CometS3Credentials creds =
        CometS3CredentialDispatcher.getCredentialsForPath(TEST_PROVIDER, DK, "b", "k", READ);

    assertNull(creds.getSessionToken());
  }

  @Test
  public void providerReceivesEachCallSeparately() throws Exception {
    init();
    CometS3CredentialDispatcher.getCredentialsForPath(TEST_PROVIDER, DK, "b1", "k1", READ);
    CometS3CredentialDispatcher.getCredentialsForPath(TEST_PROVIDER, DK, "b2", "k2", READ);
    CometS3CredentialDispatcher.getCredentialsForPath(TEST_PROVIDER, DK, "b3", "k3", READ);

    assertEquals(3, TestCometS3CredentialProvider.callCount.get());
    assertEquals("b3", TestCometS3CredentialProvider.lastBucket);
    assertEquals("k3", TestCometS3CredentialProvider.lastPath);
  }
}
