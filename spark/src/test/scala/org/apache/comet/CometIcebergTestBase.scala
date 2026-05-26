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

package org.apache.comet

import java.io.File
import java.nio.file.Files

import org.apache.comet.CometSparkSessionExtensions.isSpark41Plus
import org.apache.comet.iceberg.IcebergReflection

/**
 * Shared fixtures for Iceberg-backed test suites: classpath probe and per-test temp directory.
 */
trait CometIcebergTestBase {

  /**
   * True only when Iceberg is on the classpath AND usable on the running Spark version. Apache
   * Iceberg does not yet publish a Spark 4.1+ runtime (tracked in apache/iceberg#15238); Comet's
   * spark-4.1 / spark-4.2 profiles pin `iceberg-spark-runtime-4.0` as a build-only stopgap, which
   * is binary-incompatible with Spark 4.1's Catalyst (e.g. `trees.Origin`) and aborts the moment
   * an Iceberg SQL-extensions statement is parsed. Gate on Spark version so these suites skip
   * cleanly instead of aborting. Remove the version check once the pom adopts a real
   * `iceberg-spark-runtime-4.1` (Iceberg 1.11.0+).
   */
  protected def icebergAvailable: Boolean =
    !isSpark41Plus &&
      (try {
        IcebergReflection.loadClass("org.apache.iceberg.catalog.Catalog")
        true
      } catch {
        case _: ClassNotFoundException => false
      })

  protected def withTempIcebergDir(f: File => Unit): Unit = {
    val dir = Files.createTempDirectory("comet-iceberg-test").toFile
    try f(dir)
    finally deleteRecursively(dir)
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) file.listFiles().foreach(deleteRecursively)
    file.delete()
  }
}
