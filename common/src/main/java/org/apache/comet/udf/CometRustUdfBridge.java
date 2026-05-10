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
   * Validate that {@code libraryPath} loads, exposes a UDF named {@code expectedName}, and return a
   * JSON description of that UDF. Throws RuntimeException on any error.
   *
   * <p>The returned JSON has the form: {@code
   * {"name":"add_one","args":["Int64"],"return_type":"Int64","volatility":0}}
   */
  public static native String validateLibrary(String libraryPath, String expectedName);

  /**
   * Return a JSON array describing every UDF exposed by {@code libraryPath}. Each element has the
   * same shape as the return value of {@link #validateLibrary}.
   */
  public static native String listUdfs(String libraryPath);
}
