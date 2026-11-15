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

package org.apache.comet.iceberg

import java.util.Locale

import org.apache.spark.internal.Logging

import org.apache.comet.NativeBase

/**
 * The storage schemes the native Iceberg storage factory publishes over JNI, so the JVM scan and
 * write gates decline what native cannot open instead of failing at execution. Native is the only
 * source of these lists. Every caller sits behind `isCometLoaded`, so a JVM where the library is
 * not loaded never plans a native scan or write, and both sets are simply empty there.
 */
private[comet] object IcebergStorageSchemes extends Logging {

  lazy val read: Set[String] = load(forWrite = false)
  lazy val write: Set[String] = load(forWrite = true)

  /**
   * Splits the comma-joined native list, trimming and lowercasing each entry. A null or blank
   * list throws: `builtin_storage_schemes` is a fixed non-empty constant natively, so a loaded
   * library that publishes nothing can only be a build or marshalling bug, and a warning here
   * would silently disable every native Iceberg scan and write.
   */
  private[comet] def parse(joined: String, forWrite: Boolean): Set[String] = {
    val schemes = Option(joined).toSeq
      .flatMap(_.split(","))
      .map(_.trim.toLowerCase(Locale.ROOT))
      .filter(_.nonEmpty)
      .toSet
    if (schemes.isEmpty) {
      val path = if (forWrite) "write" else "read"
      throw new IllegalStateException(
        s"Comet native library published no Iceberg $path scheme list")
    }
    schemes
  }

  /**
   * Loads one list over JNI, or answers the empty set with a warning when the library is not
   * loaded; that branch never runs a plan, because every caller sits behind `isCometLoaded`.
   * `isLoaded` is a parameter so the unloaded answer can be tested without unloading the library.
   * Once loaded, anything but a non-empty list is a build bug that must fail loudly: a native
   * fault while answering the probe propagates, and a blank answer throws in `parse`.
   */
  private[comet] def load(
      forWrite: Boolean,
      isLoaded: Boolean = NativeBase.isLoaded): Set[String] = {
    if (isLoaded) {
      parse(NativeBase.icebergStorageSchemes(forWrite), forWrite)
    } else {
      val path = if (forWrite) "write" else "read"
      logWarning(s"Comet native library is not loaded; the Iceberg $path scheme list is empty")
      Set.empty
    }
  }
}
