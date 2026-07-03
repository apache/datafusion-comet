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

import org.apache.comet.NativeBase

/**
 * JNI binding to the native driver-side Ballista submission entry.
 *
 * The offload code (the `execution::ballista` module and its
 * `Java_org_apache_comet_ballista_NativeBallista_*` JNI entries) is compiled into the single
 * `libcomet` cdylib when Comet's native crate is built with the default-off `ballista` Cargo
 * feature (`cd native && cargo build --features ballista`). There is no separate
 * `libdatafusion_comet_ballista` library anymore: folding the offload into core means it shares
 * Comet core's single `JAVA_VM` static, so a Comet-on-executor query and an in-process offload
 * can coexist in one JVM without the "JAVA_VM not initialized" panic.
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

  /**
   * EXPERIMENTAL (R2): run a distributed two-stage GROUP BY offload on an in-process standalone
   * Ballista cluster (no Spark executors).
   *
   * `block1` is the serialized partial-aggregate block (self-contained `NativeScan` leaf);
   * `block2` is the serialized final-aggregate block (whose input leaf is a `Scan` fed by the
   * shuffle). The native side assembles `CometFragmentExec(block2,
   * [Hash-Repartition(CometFragmentExec(block1))])`, which Ballista splits at the hash
   * repartition into a partial-agg stage and a final-agg stage across a shuffle, then exports the
   * concatenated result batch into the caller-allocated Arrow C Data structs.
   *
   * @param block1
   *   serialized partial-aggregate `Operator` proto (with file partitions injected)
   * @param block2
   *   serialized final-aggregate `Operator` proto (leaf is a `Scan`, not a `ShuffleScan`)
   * @param numGroupKeys
   *   number of grouping columns (the leading columns of block1's output to hash on)
   * @param numPartitions
   *   number of shuffle partitions
   * @param arrayAddrs
   *   memory addresses of one `ArrowArray` struct per output column of `block2`
   * @param schemaAddrs
   *   memory addresses of one `ArrowSchema` struct per output column of `block2`
   * @return
   *   the number of rows exported
   */
  @native def executeQueryDistributed(
      block1: Array[Byte],
      block2: Array[Byte],
      numGroupKeys: Int,
      numPartitions: Int,
      arrayAddrs: Array[Long],
      schemaAddrs: Array[Long]): Long
}

object NativeBallista {

  @volatile private var probed = false
  @volatile private var available = false
  @volatile private var loadError: Option[Throwable] = None

  /**
   * Ensure the single `libcomet` cdylib is loaded. [[NativeBase]] already loads it for every
   * Comet native method; because the offload JNI entries are now compiled into `libcomet` (behind
   * the `ballista` feature), loading libcomet also binds the `NativeBallista_*` entries. There is
   * no separate library to `System.load`.
   */
  private def ensureCometLoaded(): Unit = synchronized {
    if (loadError.isDefined) return
    try {
      NativeBase.isLoaded()
    } catch {
      case t: Throwable => loadError = Some(t)
    }
  }

  /** Load libcomet, throwing if it cannot be loaded. */
  def ensureLoaded(): Unit = {
    ensureCometLoaded()
    loadError.foreach { t =>
      throw new IllegalStateException(s"failed to load native comet library: ${t.getMessage}", t)
    }
  }

  /**
   * True if the offload native entries are present - i.e. `libcomet` loaded AND was built with
   * the `ballista` Cargo feature. Detected once by resolving a `NativeBallista_*` JNI symbol; a
   * feature-less `libcomet` has no such symbol and yields `false`, so the offload suites
   * `assume`-skip instead of hard-failing with an `UnsatisfiedLinkError`.
   */
  def isAvailable: Boolean = synchronized {
    if (probed) return available
    probed = true
    ensureCometLoaded()
    if (loadError.isDefined) {
      available = false
    } else {
      available =
        try {
          // Resolve a NativeBallista JNI entry; only a `--features ballista` libcomet has it.
          new NativeBallista().buildTestProto()
          true
        } catch {
          case t: Throwable =>
            loadError = Some(t)
            false
        }
    }
    available
  }

  /** The load/availability failure, if any (probes on first call). */
  def loadFailure: Option[Throwable] = {
    if (!probed) isAvailable
    loadError
  }
}
