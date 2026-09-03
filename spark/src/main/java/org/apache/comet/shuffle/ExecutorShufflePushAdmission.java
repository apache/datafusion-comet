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

package org.apache.comet.shuffle;

import java.io.IOException;
import java.util.IdentityHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/** Fair, executor-wide byte admission shared by tasks using the same Celeborn client. */
final class ExecutorShufflePushAdmission {
  // At most one entry exists for each application-owned Celeborn client. The shuffle manager
  // explicitly removes its client on shutdown, so this registry never owns an application after
  // the application's own client has been released.
  private static final IdentityHashMap<Object, ExecutorShufflePushAdmission> CLIENTS =
      new IdentityHashMap<>();

  private final int limit;
  private final Semaphore available;
  private CelebornTransportCallbackTracker transportCallbacks;
  private boolean transportCallbacksInitialized;
  private boolean closed;

  private ExecutorShufflePushAdmission(int limit) {
    this.limit = limit;
    this.available = new Semaphore(limit, true);
  }

  static ExecutorShufflePushAdmission forClient(Object client, int limit) {
    synchronized (CLIENTS) {
      ExecutorShufflePushAdmission admission = CLIENTS.get(client);
      if (admission == null) {
        admission = new ExecutorShufflePushAdmission(limit);
        CLIENTS.put(client, admission);
      } else if (admission.limit != limit) {
        throw new IllegalArgumentException(
            "Tasks sharing a Celeborn client must use the same in-flight byte limit");
      }
      return admission;
    }
  }

  static void releaseClient(Object client) {
    ExecutorShufflePushAdmission admission;
    synchronized (CLIENTS) {
      admission = CLIENTS.remove(client);
    }
    if (admission != null) {
      admission.close();
    }
  }

  synchronized CelebornTransportCallbackTracker transportCallbacks(Object client) {
    if (closed) {
      return null;
    }
    if (!transportCallbacksInitialized) {
      transportCallbacks = CelebornTransportCallbackTracker.tryCreate(client);
      transportCallbacksInitialized = true;
    }
    return transportCallbacks;
  }

  private synchronized void close() {
    if (closed) {
      return;
    }
    closed = true;
    if (transportCallbacks != null) {
      transportCallbacks.close();
    }
  }

  void acquire(int bytes, BooleanSupplier cancelled) throws IOException {
    if (bytes <= 0 || bytes > limit) {
      throw new IOException(
          "Celeborn push requires "
              + bytes
              + " bytes, exceeding the executor in-flight byte limit of "
              + limit);
    }
    try {
      while (!available.tryAcquire(bytes, 25, TimeUnit.MILLISECONDS)) {
        if (cancelled.getAsBoolean()) {
          throw new IOException("Celeborn shuffle map attempt was cancelled during admission");
        }
      }
      if (cancelled.getAsBoolean()) {
        available.release(bytes);
        throw new IOException("Celeborn shuffle map attempt was cancelled during admission");
      }
    } catch (InterruptedException failure) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while waiting for Celeborn push admission", failure);
    }
  }

  void release(int bytes) {
    if (bytes > 0) {
      available.release(bytes);
    }
  }
}
