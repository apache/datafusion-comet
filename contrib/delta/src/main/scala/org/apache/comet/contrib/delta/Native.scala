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

package org.apache.comet.contrib.delta

import org.apache.comet.NativeBase

/**
 * Contrib-local JVM handle to the Delta-specific native entry point.
 *
 * Extends `NativeBase` so the libcomet load triggers on first use of any subclass -- the contrib
 * doesn't reload the library itself (there is exactly one libcomet at runtime), but inheriting
 * from `NativeBase` ensures the static initializer ordering works the same way as core's
 * `org.apache.comet.Native`. The `@native` method below binds to
 * `Java_org_apache_comet_contrib_delta_Native_planDeltaScan` exported by the contrib's Rust crate
 * (compiled INTO libcomet via the `contrib-delta` Cargo feature on `native/core`).
 */
class Native extends NativeBase {

  /**
   * Driver-side Delta log replay. Returns a prost-encoded `DeltaScanTaskList` proto (raw bytes)
   * which the caller decodes via `DeltaScanTaskList.parseFrom(...)`.
   *
   * @param tableUrl
   *   absolute URL or bare path of the Delta table root
   * @param snapshotVersion
   *   `-1` for the latest snapshot, otherwise an exact version
   * @param storageOptions
   *   cloud credentials / endpoint overrides (Hadoop-style keys)
   * @param predicateBytes
   *   prost-encoded Catalyst data filter for kernel-side stats-based file pruning, or an empty
   *   array for no predicate
   * @param columnNames
   *   logical column names the caller requires (kernel uses this for column-mapping resolution
   *   before stats-based file pruning).
   * @return
   *   `byte[]` containing the encoded DeltaScanTaskList
   */
  @native def planDeltaScan(
      tableUrl: String,
      snapshotVersion: Long,
      storageOptions: java.util.Map[String, String],
      predicateBytes: Array[Byte],
      columnNames: Array[String]): Array[Byte]
}
