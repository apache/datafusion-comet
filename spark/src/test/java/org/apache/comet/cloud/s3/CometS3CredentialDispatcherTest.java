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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
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
    // Each test starts with an empty dispatcher cache so providers and counters from prior tests
    // do not leak through closeAll() / initCount / closeCount assertions.
    CometS3CredentialDispatcher.closeAll();
    TestCometS3CredentialProvider.reset();
    // Close released providers on the releasing thread, so counts are checked right after release.
    CometS3CredentialDispatcher.setCloserForTesting(Runnable::run);
  }

  @After
  public void restoreCloser() {
    CometS3CredentialDispatcher.setCloserForTesting(null);
  }

  private long init() {
    return CometS3CredentialDispatcher.ensureInitialized(TEST_PROVIDER, DK, Collections.emptyMap());
  }

  @Test
  public void getCredentialsRoundTripsThroughProvider() throws Exception {
    long handle = init();
    CometS3Credentials creds =
        CometS3CredentialDispatcher.getCredentialsForPath(
            handle, "my-bucket", "path/to/object", READ);

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
    long handle = init();
    CometS3CredentialDispatcher.getCredentialsForPath(handle, "b", "k", WRITE);
    assertEquals(CometS3AccessMode.WRITE, TestCometS3CredentialProvider.lastMode);
  }

  @Test
  public void unknownModeRejected() {
    long handle = init();
    assertThrows(
        IllegalArgumentException.class,
        () -> CometS3CredentialDispatcher.getCredentialsForPath(handle, "b", "k", 99));
  }

  @Test
  public void emptyClassNameRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> CometS3CredentialDispatcher.ensureInitialized("", DK, Collections.emptyMap()));
    assertThrows(
        IllegalArgumentException.class,
        () -> CometS3CredentialDispatcher.ensureInitialized(null, DK, Collections.emptyMap()));
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
                CometS3CredentialDispatcher.getCredentialsForPath(Long.MAX_VALUE, "b", "k", READ));
    assertTrue(thrown.getMessage().contains("not initialized"));
  }

  @Test
  public void initializeCalledExactlyOncePerKey() {
    Map<String, String> props = new HashMap<>();
    props.put("tenant-id", "T1");

    long h1 = CometS3CredentialDispatcher.ensureInitialized(TEST_PROVIDER, "cat-a", props);
    long h2 = CometS3CredentialDispatcher.ensureInitialized(TEST_PROVIDER, "cat-a", props);
    long h3 = CometS3CredentialDispatcher.ensureInitialized(TEST_PROVIDER, "cat-a", props);

    assertEquals(1, TestCometS3CredentialProvider.initCount.get());
    assertEquals(h1, h2);
    assertEquals(h1, h3);
  }

  @Test
  public void distinctDispatchKeysIsolateInstances() throws Exception {
    Map<String, String> propsA = new HashMap<>();
    propsA.put("tenant-id", "T-A");
    Map<String, String> propsB = new HashMap<>();
    propsB.put("tenant-id", "T-B");

    long ha = CometS3CredentialDispatcher.ensureInitialized(TEST_PROVIDER, "iso-cat-a", propsA);
    long hb = CometS3CredentialDispatcher.ensureInitialized(TEST_PROVIDER, "iso-cat-b", propsB);

    assertNotEquals(ha, hb);
    assertEquals(2, TestCometS3CredentialProvider.initCount.get());

    CometS3CredentialDispatcher.getCredentialsForPath(ha, "b", "k", READ);
    assertEquals("T-A", TestCometS3CredentialProvider.lastTenantSeen);

    CometS3CredentialDispatcher.getCredentialsForPath(hb, "b", "k", READ);
    assertEquals("T-B", TestCometS3CredentialProvider.lastTenantSeen);
  }

  /** Multi-tenant collision: same FQCN and dispatchKey, different catalogProperties; isolated. */
  @Test
  public void distinctCatalogPropertiesIsolateInstances() throws Exception {
    Map<String, String> propsA = new HashMap<>();
    propsA.put("tenant-id", "T-A");
    propsA.put("credentials.uri", "https://rest-a.example/credentials");
    Map<String, String> propsB = new HashMap<>();
    propsB.put("tenant-id", "T-B");
    propsB.put("credentials.uri", "https://rest-b.example/credentials");

    long ha = CometS3CredentialDispatcher.ensureInitialized(TEST_PROVIDER, "shared-cat", propsA);
    long hb = CometS3CredentialDispatcher.ensureInitialized(TEST_PROVIDER, "shared-cat", propsB);

    assertNotEquals(ha, hb);
    assertEquals(2, TestCometS3CredentialProvider.initCount.get());

    CometS3CredentialDispatcher.getCredentialsForPath(ha, "b", "k", READ);
    assertEquals("T-A", TestCometS3CredentialProvider.lastTenantSeen);

    CometS3CredentialDispatcher.getCredentialsForPath(hb, "b", "k", READ);
    assertEquals("T-B", TestCometS3CredentialProvider.lastTenantSeen);
  }

  @Test
  public void providerExceptionsPropagate() {
    long handle = init();
    IllegalStateException boom = new IllegalStateException("simulated STS failure");
    TestCometS3CredentialProvider.throwOnNext = boom;

    Exception thrown =
        assertThrows(
            Exception.class,
            () -> CometS3CredentialDispatcher.getCredentialsForPath(handle, "b", "k", READ));
    assertSame(boom, thrown);
  }

  @Test
  public void nullSessionTokenAllowed() throws Exception {
    long handle = init();
    TestCometS3CredentialProvider.nextResult = new CometS3Credentials("AKIA", "sec", null, 0L);

    CometS3Credentials creds =
        CometS3CredentialDispatcher.getCredentialsForPath(handle, "b", "k", READ);

    assertNull(creds.getSessionToken());
  }

  @Test
  public void providerReceivesEachCallSeparately() throws Exception {
    long handle = init();
    CometS3CredentialDispatcher.getCredentialsForPath(handle, "b1", "k1", READ);
    CometS3CredentialDispatcher.getCredentialsForPath(handle, "b2", "k2", READ);
    CometS3CredentialDispatcher.getCredentialsForPath(handle, "b3", "k3", READ);

    assertEquals(3, TestCometS3CredentialProvider.callCount.get());
    assertEquals("b3", TestCometS3CredentialProvider.lastBucket);
    assertEquals("k3", TestCometS3CredentialProvider.lastPath);
  }

  @Test
  public void closeAllInvokesEveryProvider() {
    Map<String, String> propsA = new HashMap<>();
    propsA.put("tenant-id", "T-A");
    Map<String, String> propsB = new HashMap<>();
    propsB.put("tenant-id", "T-B");

    CometS3CredentialDispatcher.ensureInitialized(TEST_PROVIDER, "close-a", propsA);
    CometS3CredentialDispatcher.ensureInitialized(TEST_PROVIDER, "close-b", propsB);

    CometS3CredentialDispatcher.closeAll();

    assertEquals(2, TestCometS3CredentialProvider.closeCount.get());

    long fresh = CometS3CredentialDispatcher.ensureInitialized(TEST_PROVIDER, "close-a", propsA);
    assertEquals(3, TestCometS3CredentialProvider.initCount.get());
    assertNotNull(fresh);
  }

  /** A failing vendor close() must not block other providers from being closed. */
  @Test
  public void closeAllSwallowsProviderExceptions() {
    CometS3CredentialDispatcher.ensureInitialized(
        TEST_PROVIDER, "close-throws-a", Collections.emptyMap());
    CometS3CredentialDispatcher.ensureInitialized(
        TEST_PROVIDER, "close-throws-b", Collections.emptyMap());

    TestCometS3CredentialProvider.throwOnClose = new IllegalStateException("simulated cleanup");

    CometS3CredentialDispatcher.closeAll();

    // Both close() invocations ran even though one (the first to be visited) threw.
    assertEquals(2, TestCometS3CredentialProvider.closeCount.get());
  }

  @Test
  public void releasingTheLastReferenceClosesTheProvider() throws Exception {
    long h1 = init();
    long h2 = init();
    assertEquals(h1, h2);

    CometS3CredentialDispatcher.release(h1);
    assertEquals(0, TestCometS3CredentialProvider.closeCount.get());
    CometS3CredentialDispatcher.getCredentialsForPath(h2, "b", "k", READ);

    CometS3CredentialDispatcher.release(h2);
    assertEquals(1, TestCometS3CredentialProvider.closeCount.get());
    assertEquals(0, CometS3CredentialDispatcher.registeredCount());
    Exception thrown =
        assertThrows(
            IllegalStateException.class,
            () -> CometS3CredentialDispatcher.getCredentialsForPath(h2, "b", "k", READ));
    assertTrue(thrown.getMessage().contains("not initialized"));
  }

  @Test
  public void aRetiredKeyGetsANewProvider() {
    long retired = init();
    CometS3CredentialDispatcher.release(retired);

    long fresh = init();

    assertNotEquals(retired, fresh);
    assertEquals(2, TestCometS3CredentialProvider.initCount.get());
    assertEquals(1, CometS3CredentialDispatcher.registeredCount());
  }

  /** An extra release, or one for a handle closed at shutdown, changes nothing. */
  @Test
  public void unknownOrRepeatedReleasesAreIgnored() {
    CometS3CredentialDispatcher.release(Long.MAX_VALUE);
    long handle = init();
    CometS3CredentialDispatcher.release(handle);
    CometS3CredentialDispatcher.release(handle);

    assertEquals(1, TestCometS3CredentialProvider.closeCount.get());
    long other = init();
    CometS3CredentialDispatcher.release(handle);
    assertEquals(1, CometS3CredentialDispatcher.registeredCount());
    CometS3CredentialDispatcher.release(other);
    assertEquals(2, TestCometS3CredentialProvider.closeCount.get());
  }

  @Test
  public void concurrentAcquireAndReleaseNeverUseAClosedProvider() throws Exception {
    int threads = 8;
    int rounds = 2000;
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      List<Future<?>> futures = new ArrayList<>();
      for (int t = 0; t < threads; t++) {
        futures.add(
            pool.submit(
                () -> {
                  for (int i = 0; i < rounds; i++) {
                    long handle = init();
                    CometS3CredentialDispatcher.getCredentialsForPath(handle, "b", "k", READ);
                    CometS3CredentialDispatcher.release(handle);
                  }
                  return null;
                }));
      }
      for (Future<?> f : futures) {
        f.get();
      }
    } finally {
      pool.shutdownNow();
    }
    assertEquals(0, CometS3CredentialDispatcher.registeredCount());
    assertEquals(
        TestCometS3CredentialProvider.initCount.get(),
        TestCometS3CredentialProvider.closeCount.get());
  }

  /** The releasing thread may be a native reader's, so the close runs on the dispatcher's own. */
  @Test
  public void releaseClosesOnTheDispatcherThread() throws Exception {
    CometS3CredentialDispatcher.setCloserForTesting(null);
    TestCometS3CredentialProvider.closeEntered = new CountDownLatch(1);
    long handle = init();

    CometS3CredentialDispatcher.release(handle);

    assertTrue(TestCometS3CredentialProvider.closeEntered.await(10, TimeUnit.SECONDS));
    assertEquals("comet-s3-credential-close", TestCometS3CredentialProvider.lastCloseThread);
    assertEquals(0, CometS3CredentialDispatcher.registeredCount());
  }

  /** A close that blocks holds up neither the releasing thread nor a new reader for the key. */
  @Test
  public void aBlockingCloseDoesNotHoldUpRelease() throws Exception {
    CometS3CredentialDispatcher.setCloserForTesting(null);
    CountDownLatch block = new CountDownLatch(1);
    TestCometS3CredentialProvider.blockClose = block;
    TestCometS3CredentialProvider.closeEntered = new CountDownLatch(1);
    try {
      long retired = init();
      CometS3CredentialDispatcher.release(retired);
      assertTrue(TestCometS3CredentialProvider.closeEntered.await(10, TimeUnit.SECONDS));

      long fresh = init();
      CometS3CredentialDispatcher.getCredentialsForPath(fresh, "b", "k", READ);
      assertNotEquals(retired, fresh);
      assertEquals(0, TestCometS3CredentialProvider.closeCount.get());
    } finally {
      block.countDown();
    }
  }

  /**
   * The shutdown hook and a concurrent last release can reach one provider together. Only one of
   * them closes it.
   */
  @Test
  public void shutdownAndAConcurrentLastReleaseCloseOnce() throws Exception {
    long handle = init();
    CountDownLatch block = new CountDownLatch(1);
    TestCometS3CredentialProvider.blockClose = block;
    TestCometS3CredentialProvider.closeEntered = new CountDownLatch(1);
    Thread shutdown = new Thread(CometS3CredentialDispatcher::closeAll);
    Thread releaser = new Thread(() -> CometS3CredentialDispatcher.release(handle));
    shutdown.start();
    try {
      assertTrue(TestCometS3CredentialProvider.closeEntered.await(10, TimeUnit.SECONDS));
      // Without the shared claim, this release would close the provider a second time.
      releaser.start();
      releaser.join(2_000);
    } finally {
      block.countDown();
    }
    shutdown.join(10_000);
    releaser.join(10_000);

    assertEquals(1, TestCometS3CredentialProvider.closeCount.get());
  }
}
