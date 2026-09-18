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
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Covers {@link CometS3CredentialDispatcher#getPolicyLocationsFor(long, String, String, int)}: it
 * dispatches to the scoped sub-interface when present and returns an empty list without touching
 * the provider when only the base interface is implemented.
 */
public class CometS3ScopedCredentialProviderTest {

  private static final String BASE_PROVIDER = TestCometS3CredentialProvider.class.getName();
  private static final String SCOPED_PROVIDER = TestCometS3ScopedCredentialProvider.class.getName();
  private static final String DK = "scoped-test-dispatch-key";
  private static final int READ = CometS3AccessMode.READ.ordinal();
  private static final int WRITE = CometS3AccessMode.WRITE.ordinal();

  @Before
  public void resetTestProviders() {
    CometS3CredentialDispatcher.closeAll();
    TestCometS3CredentialProvider.reset();
    TestCometS3ScopedCredentialProvider.reset();
  }

  @Test
  public void baseInterfaceProviderReturnsEmptyList() throws Exception {
    long handle =
        CometS3CredentialDispatcher.ensureInitialized(BASE_PROVIDER, DK, Collections.emptyMap());

    List<String> hint =
        CometS3CredentialDispatcher.getPolicyLocationsFor(handle, "b", "path/x", READ);

    assertNotNull(hint);
    assertTrue("expected empty hint for a base-only provider", hint.isEmpty());
    // The base interface has no getPolicyLocationsFor, so no dispatch should have hit the
    // provider's getCredentialsForPath either.
    assertEquals(0, TestCometS3CredentialProvider.callCount.get());
  }

  @Test
  public void scopedInterfaceProviderReceivesContext() throws Exception {
    long handle =
        CometS3CredentialDispatcher.ensureInitialized(SCOPED_PROVIDER, DK, Collections.emptyMap());
    TestCometS3ScopedCredentialProvider.nextPolicyLocations.set(
        List.of("warehouse/allowed/", "warehouse/other/"));

    List<String> hint =
        CometS3CredentialDispatcher.getPolicyLocationsFor(
            handle, "my-bucket", "warehouse/allowed/foo", WRITE);

    assertEquals(List.of("warehouse/allowed/", "warehouse/other/"), hint);
    assertEquals(1, TestCometS3ScopedCredentialProvider.policyCallCount.get());
    assertEquals("my-bucket", TestCometS3ScopedCredentialProvider.lastBucket);
    assertEquals("warehouse/allowed/foo", TestCometS3ScopedCredentialProvider.lastPath);
    assertEquals(CometS3AccessMode.WRITE, TestCometS3ScopedCredentialProvider.lastMode);
    // getPolicyLocationsFor must not accidentally invoke getCredentialsForPath.
    assertEquals(0, TestCometS3ScopedCredentialProvider.callCount.get());
  }

  @Test
  public void nullReturnFromScopedProviderNormalizedToEmpty() throws Exception {
    long handle =
        CometS3CredentialDispatcher.ensureInitialized(SCOPED_PROVIDER, DK, Collections.emptyMap());
    TestCometS3ScopedCredentialProvider.nextPolicyLocations.set(null);

    List<String> hint = CometS3CredentialDispatcher.getPolicyLocationsFor(handle, "b", "k", READ);

    assertNotNull(hint);
    assertTrue(hint.isEmpty());
  }

  @Test
  public void scopedProviderExceptionsPropagate() {
    long handle =
        CometS3CredentialDispatcher.ensureInitialized(SCOPED_PROVIDER, DK, Collections.emptyMap());
    IllegalStateException boom = new IllegalStateException("simulated policy source failure");
    TestCometS3ScopedCredentialProvider.throwOnNextPolicyCall = boom;

    Exception thrown =
        assertThrows(
            Exception.class,
            () -> CometS3CredentialDispatcher.getPolicyLocationsFor(handle, "b", "k", READ));
    assertSame(boom, thrown);
  }

  @Test
  public void unknownModeRejected() {
    long handle =
        CometS3CredentialDispatcher.ensureInitialized(SCOPED_PROVIDER, DK, Collections.emptyMap());
    assertThrows(
        IllegalArgumentException.class,
        () -> CometS3CredentialDispatcher.getPolicyLocationsFor(handle, "b", "k", 99));
  }

  @Test
  public void unknownHandleRejected() {
    Exception thrown =
        assertThrows(
            IllegalStateException.class,
            () ->
                CometS3CredentialDispatcher.getPolicyLocationsFor(Long.MAX_VALUE, "b", "k", READ));
    assertTrue(thrown.getMessage().contains("not initialized"));
  }
}
