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

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test-only provider instantiated by the dispatcher when its FQCN is passed across JNI. State is
 * static because the dispatcher keeps one instance per (FQCN, dispatchKey, catalogProperties) until
 * its last reference is released or the JVM shuts down.
 */
public class TestCometS3CredentialProvider implements CometS3CredentialProvider {

  static final AtomicInteger initCount = new AtomicInteger(0);
  static final AtomicInteger callCount = new AtomicInteger(0);
  static final AtomicInteger closeCount = new AtomicInteger(0);
  static volatile String lastBucket;
  static volatile String lastPath;
  static volatile CometS3AccessMode lastMode;
  static volatile String lastTenantSeen;
  static volatile RuntimeException throwOnNext;
  static volatile Exception throwOnClose;
  static volatile CometS3Credentials nextResult =
      new CometS3Credentials("AKIATEST", "secret", "session-tok", 0L);

  /** When set, {@code close()} counts down {@link #closeEntered} and waits for this latch. */
  static volatile CountDownLatch blockClose;

  static volatile CountDownLatch closeEntered;

  /** When set, {@code close()} counts this down once it has counted the close. */
  static volatile CountDownLatch closeFinished;

  static volatile String lastCloseThread;

  private volatile String tenantId;
  private volatile boolean closed;

  static void reset() {
    initCount.set(0);
    callCount.set(0);
    closeCount.set(0);
    lastBucket = null;
    lastPath = null;
    lastMode = null;
    lastTenantSeen = null;
    throwOnNext = null;
    throwOnClose = null;
    nextResult = new CometS3Credentials("AKIATEST", "secret", "session-tok", 0L);
    blockClose = null;
    closeEntered = null;
    closeFinished = null;
    lastCloseThread = null;
  }

  @Override
  public void initialize(Map<String, String> catalogProperties) {
    initCount.incrementAndGet();
    this.tenantId = catalogProperties.get("tenant-id");
  }

  @Override
  public CometS3Credentials getCredentialsForPath(CometS3CredentialContext context) {
    if (closed) {
      throw new IllegalStateException("TestCometS3CredentialProvider used after close()");
    }
    callCount.incrementAndGet();
    lastBucket = context.getBucket();
    lastPath = context.getPath();
    lastMode = context.getMode();
    lastTenantSeen = tenantId;
    RuntimeException toThrow = throwOnNext;
    if (toThrow != null) {
      throwOnNext = null;
      throw toThrow;
    }
    return nextResult;
  }

  @Override
  public void close() throws Exception {
    closed = true;
    lastCloseThread = Thread.currentThread().getName();
    CountDownLatch entered = closeEntered;
    if (entered != null) {
      entered.countDown();
    }
    try {
      CountDownLatch block = blockClose;
      if (block != null) {
        block.await();
      }
      closeCount.incrementAndGet();
      Exception toThrow = throwOnClose;
      if (toThrow != null) {
        throwOnClose = null;
        throw toThrow;
      }
    } finally {
      CountDownLatch finished = closeFinished;
      if (finished != null) {
        finished.countDown();
      }
    }
  }
}
