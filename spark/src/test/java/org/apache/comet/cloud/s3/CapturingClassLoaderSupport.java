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

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Test support for the adapters' class-loader capture. The adapters capture the context loader in
 * {@code initialize()} (which the dispatcher calls on a thread that has Spark's user-jar loader)
 * and must use it to load the delegate later, on native worker threads whose context loader is
 * null. These helpers let a test capture a recording loader and then fetch on a null-context
 * thread, asserting the captured loader was the one asked for the delegate class.
 */
final class CapturingClassLoaderSupport {

  private CapturingClassLoaderSupport() {}

  /**
   * A loader that records whether it was asked to load {@code watched}, then delegates normally.
   */
  static final class RecordingClassLoader extends ClassLoader {
    private final String watched;
    final Set<String> loaded = ConcurrentHashMap.newKeySet();

    RecordingClassLoader(String watched, ClassLoader parent) {
      super(parent);
      this.watched = watched;
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
      if (watched.equals(name)) {
        loaded.add(name);
      }
      return super.loadClass(name, resolve);
    }
  }

  /** Runs {@code body} on a fresh thread whose context ClassLoader is {@code cl} (may be null). */
  static <T> T onThread(ClassLoader cl, Callable<T> body) throws Exception {
    AtomicReference<T> result = new AtomicReference<>();
    AtomicReference<Throwable> error = new AtomicReference<>();
    Thread t =
        new Thread(
            () -> {
              try {
                result.set(body.call());
              } catch (Throwable e) {
                error.set(e);
              }
            });
    t.setContextClassLoader(cl);
    t.start();
    t.join();
    Throwable e = error.get();
    if (e instanceof Exception) {
      throw (Exception) e;
    }
    if (e != null) {
      throw new RuntimeException(e);
    }
    return result.get();
  }
}
