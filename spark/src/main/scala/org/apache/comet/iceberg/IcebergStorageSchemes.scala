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
 * write gates decline what native cannot open instead of failing at execution. Every caller sits
 * behind `isCometLoaded`, so the fallback constants are only consulted in a JVM where nothing
 * runs natively; the pinning test in `CometScanSchemeFallbackSuite` keeps them equal to the
 * native lists.
 */
private[comet] object IcebergStorageSchemes extends Logging {

  private[comet] val FallbackRead: Set[String] = Set("file", "s3", "s3a", "gs", "oss")
  private[comet] val FallbackWrite: Set[String] = Set("file", "memory", "s3", "s3a", "gs")

  lazy val read: Set[String] = load(forWrite = false, FallbackRead)
  lazy val write: Set[String] = load(forWrite = true, FallbackWrite)

  /** Splits the comma-joined native list; a null or blank list yields `fallback`. */
  private[comet] def parse(joined: String, fallback: Set[String]): Set[String] = {
    val schemes = Option(joined).toSeq
      .flatMap(_.split(","))
      .map(_.trim.toLowerCase(Locale.ROOT))
      .filter(_.nonEmpty)
      .toSet
    if (schemes.isEmpty) {
      logWarning(
        "Comet native library published an empty Iceberg scheme list; using the fallback list " +
          fallback.toSeq.sorted.mkString(", "))
      fallback
    } else {
      schemes
    }
  }

  // A native fault while answering the probe propagates: `isLoaded` true means the symbol
  // resolves, and anything else is a build bug that must fail loudly rather than fall back.
  private def load(forWrite: Boolean, fallback: Set[String]): Set[String] = {
    if (NativeBase.isLoaded) {
      parse(NativeBase.icebergStorageSchemes(forWrite), fallback)
    } else {
      val path = if (forWrite) "write" else "read"
      logWarning(
        s"Comet native library is not loaded; using the fallback Iceberg $path scheme list " +
          fallback.toSeq.sorted.mkString(", "))
      fallback
    }
  }
}
