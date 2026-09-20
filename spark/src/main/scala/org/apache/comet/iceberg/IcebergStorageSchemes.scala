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

import scala.util.control.NonFatal

import org.apache.spark.internal.Logging

import org.apache.comet.NativeBase

/**
 * The storage schemes the native Iceberg storage factory publishes over JNI, so the JVM scan and
 * write gates decline what native cannot open instead of failing at execution. The fallback
 * constants are used only when the library cannot be loaded: assuming a scheme is supported would
 * recreate the execution-time failure, and without the library nothing runs natively anyway.
 */
private[comet] object IcebergStorageSchemes extends Logging {

  private[comet] val FallbackRead: Set[String] = Set("file", "memory", "s3", "s3a", "gs", "oss")
  private[comet] val FallbackWrite: Set[String] = Set("file", "memory", "s3", "s3a", "gs")

  lazy val read: Set[String] = load(forWrite = false, FallbackRead)
  lazy val write: Set[String] = load(forWrite = true, FallbackWrite)

  private def load(forWrite: Boolean, fallback: Set[String]): Set[String] = {
    val path = if (forWrite) "write" else "read"
    val joined =
      try NativeBase.icebergStorageSchemes(forWrite)
      catch {
        case e: UnsatisfiedLinkError =>
          logWarning(
            s"Comet native library is not loaded; using the fallback Iceberg $path scheme list " +
              s"${fallback.toSeq.sorted.mkString(", ")}: ${e.getMessage}")
          return fallback
        case NonFatal(e) =>
          logWarning(
            s"Failed to load the Iceberg $path scheme list from the Comet native library; using " +
              s"the fallback list ${fallback.toSeq.sorted.mkString(", ")}",
            e)
          return fallback
      }
    val schemes = Option(joined).toSeq
      .flatMap(_.split(","))
      .map(_.trim.toLowerCase(Locale.ROOT))
      .filter(_.nonEmpty)
      .toSet
    if (schemes.isEmpty) {
      logWarning(
        s"Comet native library published an empty Iceberg $path scheme list; using the fallback " +
          s"list ${fallback.toSeq.sorted.mkString(", ")}")
      fallback
    } else {
      schemes
    }
  }
}
