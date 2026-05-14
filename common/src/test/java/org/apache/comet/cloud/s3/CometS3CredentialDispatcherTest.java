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

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class CometS3CredentialDispatcherTest {

  private static final int READ = CometS3AccessMode.READ.ordinal();
  private static final int WRITE = CometS3AccessMode.WRITE.ordinal();

  @Before
  public void resetTestProvider() {
    TestCometS3CredentialProvider.reset();
  }

  @Test
  public void providerIsRegisteredFromTestClasspath() {
    assertTrue(CometS3CredentialDispatcher.isProviderRegistered());
  }

  @Test
  public void getCredentialsRoundTripsThroughProvider() throws Exception {
    CometS3Credentials creds =
        CometS3CredentialDispatcher.getCredentialsForPath("my-bucket", "path/to/object", READ);

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
    CometS3CredentialDispatcher.getCredentialsForPath("b", "k", WRITE);
    assertEquals(CometS3AccessMode.WRITE, TestCometS3CredentialProvider.lastMode);
  }

  @Test
  public void unknownModeRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> CometS3CredentialDispatcher.getCredentialsForPath("b", "k", 99));
  }

  @Test
  public void providerExceptionsPropagate() {
    IllegalStateException boom = new IllegalStateException("simulated STS failure");
    TestCometS3CredentialProvider.throwOnNext = boom;

    Exception thrown =
        assertThrows(
            Exception.class,
            () -> CometS3CredentialDispatcher.getCredentialsForPath("b", "k", READ));
    assertSame(boom, thrown);
  }

  @Test
  public void nullSessionTokenAllowed() throws Exception {
    TestCometS3CredentialProvider.nextResult = new CometS3Credentials("AKIA", "sec", null, 0L);

    CometS3Credentials creds = CometS3CredentialDispatcher.getCredentialsForPath("b", "k", READ);

    assertNull(creds.getSessionToken());
  }

  @Test
  public void providerReceivesEachCallSeparately() throws Exception {
    CometS3CredentialDispatcher.getCredentialsForPath("b1", "k1", READ);
    CometS3CredentialDispatcher.getCredentialsForPath("b2", "k2", READ);
    CometS3CredentialDispatcher.getCredentialsForPath("b3", "k3", READ);

    assertEquals(3, TestCometS3CredentialProvider.callCount.get());
    assertEquals("b3", TestCometS3CredentialProvider.lastBucket);
    assertEquals("k3", TestCometS3CredentialProvider.lastPath);
  }
}
