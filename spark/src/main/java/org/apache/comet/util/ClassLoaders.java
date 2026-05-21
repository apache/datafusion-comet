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

package org.apache.comet.util;

/** ClassLoader helpers shared across Comet modules. */
public final class ClassLoaders {

  private ClassLoaders() {}

  /**
   * Loads a class using the thread context ClassLoader if available, falling back to the system
   * ClassLoader. Spark wires user JARs onto the context ClassLoader, so vendor classes named in
   * Spark configs are reachable through this path.
   */
  public static Class<?> loadClass(String className) throws ClassNotFoundException {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader != null) {
      // scalastyle:off classforname
      return Class.forName(className, true, classLoader);
      // scalastyle:on classforname
    }
    // scalastyle:off classforname
    return Class.forName(className);
    // scalastyle:on classforname
  }
}
