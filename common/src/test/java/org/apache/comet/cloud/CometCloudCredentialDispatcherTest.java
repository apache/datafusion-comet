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

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class CometCloudCredentialDispatcherTest {

  @Before
  public void resetTestProvider() {
    TestCometCloudCredentialProvider.reset();
  }

  @Test
  public void providerIsRegisteredFromTestClasspath() {
    assertTrue(CometCloudCredentialDispatcher.isProviderRegistered());
  }

  @Test
  public void getCredentialsRoundTripsThroughProvider() throws Exception {
    CometCredentials creds =
        CometCloudCredentialDispatcher.getCredentialsForPath("my-bucket", "path/to/object", "READ");

    assertNotNull(creds);
    assertEquals("AKIATEST", creds.getAccessKeyId());
    assertEquals("secret", creds.getSecretAccessKey());
    assertEquals("session-tok", creds.getSessionToken());
    assertEquals("us-east-1", creds.getRegion());
    assertEquals(0L, creds.getExpirationEpochMillis());

    assertEquals(1, TestCometCloudCredentialProvider.callCount.get());
    assertEquals("my-bucket", TestCometCloudCredentialProvider.lastBucket);
    assertEquals("path/to/object", TestCometCloudCredentialProvider.lastPath);
    assertEquals(CometAccessMode.READ, TestCometCloudCredentialProvider.lastMode);
  }

  @Test
  public void writeModeIsForwarded() throws Exception {
    CometCloudCredentialDispatcher.getCredentialsForPath("b", "k", "WRITE");
    assertEquals(CometAccessMode.WRITE, TestCometCloudCredentialProvider.lastMode);
  }

  @Test
  public void unknownModeRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> CometCloudCredentialDispatcher.getCredentialsForPath("b", "k", "BOGUS"));
  }

  @Test
  public void providerExceptionsPropagate() {
    IllegalStateException boom = new IllegalStateException("simulated STS failure");
    TestCometCloudCredentialProvider.throwOnNext = boom;

    Exception thrown =
        assertThrows(
            Exception.class,
            () -> CometCloudCredentialDispatcher.getCredentialsForPath("b", "k", "READ"));
    assertSame(boom, thrown);
  }

  @Test
  public void nullSessionTokenAndRegionAreAllowed() throws Exception {
    TestCometCloudCredentialProvider.nextResult =
        new CometCredentials("AKIA", "sec", null, null, 0L);

    CometCredentials creds = CometCloudCredentialDispatcher.getCredentialsForPath("b", "k", "READ");

    assertNull(creds.getSessionToken());
    assertNull(creds.getRegion());
  }

  @Test
  public void providerReceivesEachCallSeparately() throws Exception {
    CometCloudCredentialDispatcher.getCredentialsForPath("b1", "k1", "READ");
    CometCloudCredentialDispatcher.getCredentialsForPath("b2", "k2", "READ");
    CometCloudCredentialDispatcher.getCredentialsForPath("b3", "k3", "READ");

    assertEquals(3, TestCometCloudCredentialProvider.callCount.get());
    assertEquals("b3", TestCometCloudCredentialProvider.lastBucket);
    assertEquals("k3", TestCometCloudCredentialProvider.lastPath);
  }
}
