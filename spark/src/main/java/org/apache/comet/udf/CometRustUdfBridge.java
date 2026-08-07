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

package org.apache.comet.udf;

import org.apache.comet.NativeBase;

/** JNI bridge for driver-side Rust UDF library validation. */
public final class CometRustUdfBridge extends NativeBase {
  private CometRustUdfBridge() {}

  /**
   * Validate that {@code libraryPath} loads and exposes a UDF named {@code expectedName}. Returns
   * normally when it does, and throws RuntimeException otherwise.
   */
  public static native void validateLibrary(String libraryPath, String expectedName);
}
