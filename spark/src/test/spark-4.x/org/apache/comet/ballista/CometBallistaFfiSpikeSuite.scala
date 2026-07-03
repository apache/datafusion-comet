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

import java.nio.file.{Files, Paths}

import org.scalatest.funsuite.AnyFunSuite

import org.apache.arrow.c.{ArrowArray, ArrowSchema, CDataDictionaryProvider, Data}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.IntVector

/**
 * SPIKE: proves the driver-side "offload to Ballista" round trip across the JVM boundary.
 *
 * The JVM asks native code to build a fixed Comet `Operator` proto, hands those proto bytes back
 * to native code, which runs them on an in-process standalone Ballista engine (no Spark
 * executors) and exports the result batch back to the JVM over the Arrow C Data Interface. The
 * JVM imports the result and asserts 5 rows come back.
 *
 * The native entry points live in the single `libcomet` cdylib, compiled in when Comet's native
 * crate is built with the default-off `ballista` Cargo feature (`cd native && cargo build
 * --features ballista`, or `make core-ballista`). There is no separate offload library.
 */
class CometBallistaFfiSpikeSuite extends AnyFunSuite {

  test("JVM -> native -> in-process Ballista -> JVM returns 5 rows over Arrow FFI") {
    CometBallistaFfiSpikeSuite.assumeLibraryLoaded()
    // Arrow's C Data JNI helper (arrow_cdata_jni) extracts itself into java.io.tmpdir; the surefire
    // config points that at target/tmp, which may not exist yet when this suite runs alone.
    Files.createDirectories(Paths.get(System.getProperty("java.io.tmpdir")))
    val native = new NativeBallista

    // 1. Native builds the fixed test proto (single NativeScan over a = [1..5]) and returns bytes.
    val proto: Array[Byte] = native.buildTestProto()
    assert(proto.nonEmpty, "native buildTestProto returned no bytes")

    val allocator = new RootAllocator(Long.MaxValue)
    val provider = new CDataDictionaryProvider()
    // One output column (`a`): allocate the C Data structs the JVM owns.
    val arrowArray = ArrowArray.allocateNew(allocator)
    val arrowSchema = ArrowSchema.allocateNew(allocator)
    try {
      // 2. JVM hands proto + struct addresses back to native, which runs Ballista and exports.
      val numRows = native.executeQuery(
        proto,
        Array(arrowArray.memoryAddress()),
        Array(arrowSchema.memoryAddress()))
      assert(numRows == 5, s"expected 5 rows from Ballista, got $numRows")

      // 3. JVM imports the exported column over the Arrow C Data Interface.
      val vector = Data.importVector(allocator, arrowArray, arrowSchema, provider)
      try {
        assert(
          vector.getValueCount == 5,
          s"expected 5 imported values, got ${vector.getValueCount}")
        val ints = vector.asInstanceOf[IntVector]
        val values = (0 until ints.getValueCount).map(ints.get)
        assert(values == Seq(1, 2, 3, 4, 5), s"unexpected values: $values")
      } finally {
        vector.close()
      }
    } finally {
      arrowArray.close()
      arrowSchema.close()
      provider.close()
      allocator.close()
    }
  }
}

object CometBallistaFfiSpikeSuite {

  def assumeLibraryLoaded(): Unit = {
    NativeBallista.loadFailure.foreach { t =>
      org.scalatest.Assertions
        .cancel(s"native ballista library not available: ${t.getMessage}", t)
    }
  }
}
