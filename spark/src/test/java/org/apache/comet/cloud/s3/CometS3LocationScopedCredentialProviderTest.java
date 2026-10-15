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

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Covers {@link CometS3CredentialDispatcher#getPolicyLocations(long, String)}, the entry point
 * native code uses to read a {@link CometS3LocationScopedCredentialProvider}'s locations.
 */
public class CometS3LocationScopedCredentialProviderTest {

  private static final String BASE_PROVIDER = TestCometS3CredentialProvider.class.getName();
  private static final String SCOPED_PROVIDER =
      TestCometS3LocationScopedCredentialProvider.class.getName();
  private static final String DK = "location-scoped-test-dispatch-key";

  @Before
  public void resetTestProviders() {
    CometS3CredentialDispatcher.closeAll();
    TestCometS3CredentialProvider.reset();
    TestCometS3LocationScopedCredentialProvider.reset();
  }

  private static long scopedHandle() {
    return CometS3CredentialDispatcher.ensureInitialized(
        SCOPED_PROVIDER, DK, Collections.emptyMap());
  }

  @Test
  public void baseProviderReturnsNullWithoutCallingIt() throws Exception {
    long handle =
        CometS3CredentialDispatcher.ensureInitialized(BASE_PROVIDER, DK, Collections.emptyMap());

    assertNull(CometS3CredentialDispatcher.getPolicyLocations(handle, "b"));
    assertEquals(0, TestCometS3CredentialProvider.callCount.get());
  }

  @Test
  public void returnsTheProvidersLocationsForTheBucket() throws Exception {
    long handle = scopedHandle();
    TestCometS3LocationScopedCredentialProvider.nextLocations.set(
        List.of("warehouse/sales", "/warehouse/finance/"));

    String[] locations = CometS3CredentialDispatcher.getPolicyLocations(handle, "my-bucket");

    assertArrayEquals(new String[] {"warehouse/sales", "/warehouse/finance/"}, locations);
    assertEquals("my-bucket", TestCometS3LocationScopedCredentialProvider.lastBucket);
    assertEquals(1, TestCometS3LocationScopedCredentialProvider.locationCallCount.get());
    assertEquals(
        "getPolicyLocations must not fetch credentials",
        0,
        TestCometS3LocationScopedCredentialProvider.callCount.get());
  }

  @Test
  public void emptyListReturnsEmptyArray() throws Exception {
    String[] locations = CometS3CredentialDispatcher.getPolicyLocations(scopedHandle(), "b");

    assertEquals(0, locations.length);
  }

  @Test
  public void nullListIsRejected() {
    long handle = scopedHandle();
    TestCometS3LocationScopedCredentialProvider.nextLocations.set(null);

    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class,
            () -> CometS3CredentialDispatcher.getPolicyLocations(handle, "b"));
    assertTrue(thrown.getMessage(), thrown.getMessage().contains("returned null"));
  }

  @Test
  public void nullLocationIsRejected() {
    long handle = scopedHandle();
    TestCometS3LocationScopedCredentialProvider.nextLocations.set(Arrays.asList("a", null));

    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class,
            () -> CometS3CredentialDispatcher.getPolicyLocations(handle, "b"));
    assertTrue(thrown.getMessage(), thrown.getMessage().contains("null location"));
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void nonStringLocationIsRejected() {
    long handle = scopedHandle();
    List raw = new ArrayList();
    raw.add("a");
    raw.add(42);
    TestCometS3LocationScopedCredentialProvider.nextLocations.set(raw);

    assertThrows(
        ArrayStoreException.class,
        () -> CometS3CredentialDispatcher.getPolicyLocations(handle, "b"));
  }

  /** A lazy list's exception surfaces from the dispatcher call, not later in native code. */
  @Test
  public void lazyListExceptionsPropagate() {
    long handle = scopedHandle();
    IllegalStateException boom = new IllegalStateException("policy source unavailable");
    TestCometS3LocationScopedCredentialProvider.nextLocations.set(
        new AbstractList<String>() {
          @Override
          public String get(int index) {
            throw boom;
          }

          @Override
          public int size() {
            throw boom;
          }
        });

    Exception thrown =
        assertThrows(
            Exception.class, () -> CometS3CredentialDispatcher.getPolicyLocations(handle, "b"));
    assertSame(boom, thrown);
  }

  @Test
  public void providerExceptionsPropagate() {
    long handle = scopedHandle();
    IllegalStateException boom = new IllegalStateException("simulated policy source failure");
    TestCometS3LocationScopedCredentialProvider.throwOnNextLocationCall = boom;

    Exception thrown =
        assertThrows(
            Exception.class, () -> CometS3CredentialDispatcher.getPolicyLocations(handle, "b"));
    assertSame(boom, thrown);
  }

  @Test
  public void unknownHandleRejected() {
    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class,
            () -> CometS3CredentialDispatcher.getPolicyLocations(Long.MAX_VALUE, "b"));
    assertTrue(thrown.getMessage().contains("not initialized"));
  }
}
