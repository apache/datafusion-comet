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

package org.apache.comet.ballista

import java.io.File
import java.nio.file.{Files, Paths}

import org.apache.comet.NativeBase

/**
 * JNI binding to the native driver-side Ballista submission entry, implemented in the
 * `datafusion-comet-ballista` crate (`libdatafusion_comet_ballista`).
 *
 * EXPERIMENTAL (R1): used by [[org.apache.spark.sql.comet.CometExec.executeCollectViaBallista]]
 * to offload a single-stage Comet query to an in-process Ballista engine on the Spark driver.
 */
class NativeBallista {

  // Ensure the native library is loaded before any native method is invoked.
  NativeBallista.ensureLoaded()

  /**
   * Build the fixed spike test proto (a single `NativeScan` over a Parquet file with one int32
   * column `a` = [1..5]) native-side and return its serialized bytes. Lets tests exercise the
   * proto boundary without depending on the generated proto Java classes.
   */
  @native def buildTestProto(): Array[Byte]

  /**
   * Run a serialized Comet `Operator` proto on an in-process standalone Ballista engine (no Spark
   * executors) and export the single (concatenated) result batch into the caller-allocated Arrow
   * C Data structs.
   *
   * @param proto
   *   serialized Comet `Operator` proto
   * @param arrayAddrs
   *   memory addresses of one `ArrowArray` struct per output column
   * @param schemaAddrs
   *   memory addresses of one `ArrowSchema` struct per output column
   * @return
   *   the number of rows exported
   */
  @native def executeQuery(
      proto: Array[Byte],
      arrayAddrs: Array[Long],
      schemaAddrs: Array[Long]): Long
}

object NativeBallista {

  @volatile private var loaded = false
  @volatile private var loadError: Option[Throwable] = None

  /**
   * Load `libdatafusion_comet_ballista`.
   *
   * Symbol ownership: the ballista cdylib statically links Comet core and therefore re-exports
   * core's `Java_org_apache_comet_Native_*` JNI symbols in addition to its own distinct
   * `Java_org_apache_comet_ballista_NativeBallista_*` entries. We force `libcomet` (via
   * [[NativeBase]]) to load FIRST so the JVM binds every core native method to `libcomet`;
   * loading the ballista library afterwards contributes only the distinct `NativeBallista_*`
   * symbols. This keeps all Comet core state in a single library and avoids two divergent copies.
   *
   * The library is not on `java.library.path`, so we resolve it by absolute path: the
   * `COMET_BALLISTA_LIB` env var first, then the debug/release build outputs relative to the
   * module working directory.
   */
  private def load(): Unit = synchronized {
    if (loaded || loadError.isDefined) return

    // Load libcomet first so core JNI symbols bind to it, not to the ballista cdylib's re-exports.
    try {
      NativeBase.isLoaded()
    } catch {
      case t: Throwable =>
        loadError = Some(t)
        return
    }

    val libName = System.mapLibraryName("datafusion_comet_ballista")
    val moduleDir = new File(System.getProperty("user.dir"))
    val candidates = Seq(
      sys.env.get("COMET_BALLISTA_LIB"),
      Some(new File(moduleDir, s"../native/target/debug/$libName").getPath),
      Some(new File(moduleDir, s"../native/target/release/$libName").getPath),
      Some(new File(moduleDir, s"native/target/debug/$libName").getPath),
      Some(new File(moduleDir, s"native/target/release/$libName").getPath)).flatten
    candidates.find(p => Files.exists(Paths.get(p))) match {
      case Some(path) =>
        try {
          System.load(new File(path).getAbsolutePath)
          loaded = true
        } catch {
          case t: Throwable => loadError = Some(t)
        }
      case None =>
        loadError = Some(
          new UnsatisfiedLinkError(
            s"could not find $libName in any of: ${candidates.mkString(", ")}"))
    }
  }

  /** Load the library, throwing if it cannot be loaded. */
  def ensureLoaded(): Unit = {
    load()
    loadError.foreach { t =>
      throw new IllegalStateException(
        s"failed to load native ballista library: ${t.getMessage}",
        t)
    }
  }

  /** True if the native ballista library is available (loads it on first call). */
  def isAvailable: Boolean = {
    load()
    loaded
  }

  /** The load failure, if any (loads the library on first call). */
  def loadFailure: Option[Throwable] = {
    load()
    loadError
  }
}
