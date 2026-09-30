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

package org.apache.comet;

import java.util.Locale;

/** Helpers invoked from native code so it can match Spark's JVM behavior exactly. */
public class CometSchemaUtils {
  private CometSchemaUtils() {}

  /**
   * Folds a field name to lower case using {@link Locale#ROOT}. This mirrors Spark's {@code
   * ParquetReadSupport}, which resolves Parquet fields case-insensitively by grouping on {@code
   * name.toLowerCase(Locale.ROOT)}. The native Parquet schema adapter calls this so it folds field
   * names byte-for-byte identically to the JVM, including non-ASCII characters.
   */
  public static String toLowerCaseRoot(String name) {
    return name.toLowerCase(Locale.ROOT);
  }
}
