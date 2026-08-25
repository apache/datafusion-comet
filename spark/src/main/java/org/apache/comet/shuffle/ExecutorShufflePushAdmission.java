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

/** Completion-backed byte admission shared by all map attempts using one Celeborn client. */
final class ExecutorShufflePushAdmission {
  private static final IdentityHashMap<Object, ExecutorShufflePushAdmission> BY_CLIENT =
      new IdentityHashMap<>();

  private final int limit;
  private final Semaphore available;

  private ExecutorShufflePushAdmission(int limit) {
    this.limit = limit;
    this.available = new Semaphore(limit, true);
  }

  static ExecutorShufflePushAdmission forClient(Object client, int limit) {
    synchronized (BY_CLIENT) {
      ExecutorShufflePushAdmission admission = BY_CLIENT.get(client);
      if (admission == null) {
        admission = new ExecutorShufflePushAdmission(limit);
        BY_CLIENT.put(client, admission);
      } else if (admission.limit != limit) {
        throw new IllegalArgumentException(
            "All map attempts sharing a Celeborn client must use the same in-flight byte limit");
      }
      return admission;
    }
  }

  static void releaseClient(Object client) {
    synchronized (BY_CLIENT) {
      BY_CLIENT.remove(client);
    }
  }

  void acquire(int bytes, BooleanSupplier cancelled) throws IOException {
    if (bytes > limit) {
      throw new IOException(
          "Celeborn request requires "
              + bytes
              + " bytes, exceeding the executor in-flight byte limit of "
              + limit);
    }

    try {
      while (true) {
        if (cancelled.getAsBoolean()) {
          throw new IOException("Celeborn shuffle map attempt was cancelled during admission");
        }
        if (available.tryAcquire(bytes, 25, TimeUnit.MILLISECONDS)) {
          if (cancelled.getAsBoolean()) {
            available.release(bytes);
            throw new IOException("Celeborn shuffle map attempt was cancelled during admission");
          }
          return;
        }
      }
    } catch (InterruptedException failure) {
      Thread.currentThread().interrupt();
      throw new IOException(
          "Interrupted while waiting for Celeborn shuffle push admission", failure);
    }
  }

  void release(int bytes) {
    available.release(bytes);
  }
}
