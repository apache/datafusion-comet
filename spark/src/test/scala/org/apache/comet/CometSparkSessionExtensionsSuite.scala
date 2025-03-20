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

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.internal.SQLConf

class CometSparkSessionExtensionsSuite extends CometTestBase {

  import CometSparkSessionExtensions._

  test("isCometEnabled") {
    val conf = new SQLConf

    conf.setConfString(CometConf.COMET_ENABLED.key, "false")
    assert(!isCometEnabled(conf))

    // Since the native lib is probably already loaded due to previous tests, we reset it here
    NativeBase.setLoaded(false)

    conf.setConfString(CometConf.COMET_ENABLED.key, "true")
    val oldProperty = System.getProperty("os.name")
    System.setProperty("os.name", "foo")
    assert(!isCometEnabled(conf))

    System.setProperty("os.name", oldProperty)

    conf.setConf(SQLConf.PARQUET_INT96_TIMESTAMP_CONVERSION, true)
    assert(!isCometEnabled(conf))

    // Restore the original state
    NativeBase.setLoaded(true)
  }

  test("Arrow properties") {
    NativeBase.setLoaded(false)
    NativeBase.load()

    assert(System.getProperty(NativeBase.ARROW_UNSAFE_MEMORY_ACCESS) == "true")
    assert(System.getProperty(NativeBase.ARROW_NULL_CHECK_FOR_GET) == "false")

    System.setProperty(NativeBase.ARROW_UNSAFE_MEMORY_ACCESS, "false")
    NativeBase.setLoaded(false)
    NativeBase.load()
    assert(System.getProperty(NativeBase.ARROW_UNSAFE_MEMORY_ACCESS) == "false")

    // Should not enable when debug mode is on
    System.clearProperty(NativeBase.ARROW_UNSAFE_MEMORY_ACCESS)
    SQLConf.get.setConfString(CometConf.COMET_DEBUG_ENABLED.key, "true")
    NativeBase.setLoaded(false)
    NativeBase.load()
    assert(System.getProperty(NativeBase.ARROW_UNSAFE_MEMORY_ACCESS) == null)

    // Restore the original state
    NativeBase.setLoaded(true)
    SQLConf.get.setConfString(CometConf.COMET_DEBUG_ENABLED.key, "false")
  }

  def getBytesFromMib(mib: Long): Long = mib * 1024 * 1024

  test("Minimum Comet memory overhead") {
    val conf = new SparkConf()
    assert(getCometMemoryOverhead(conf) == getBytesFromMib(384))
  }

  test("Comet memory overhead factor with executor memory") {
    val sparkConf = new SparkConf()
    sparkConf.set("spark.executor.memory", "16g")
    sparkConf.set(CometConf.COMET_MEMORY_OVERHEAD_FACTOR.key, "0.5")

    assert(getCometMemoryOverhead(sparkConf) == getBytesFromMib(8 * 1024))
  }

  test("Comet memory overhead factor with default executor memory") {
    val sparkConf = new SparkConf()
    sparkConf.set(CometConf.COMET_MEMORY_OVERHEAD_FACTOR.key, "0.5")
    assert(getCometMemoryOverhead(sparkConf) == getBytesFromMib(512))
  }

  test("Comet memory overhead") {
    val sparkConf = new SparkConf()
    sparkConf.set(CometConf.COMET_MEMORY_OVERHEAD.key, "10g")
    assert(getCometMemoryOverhead(sparkConf) == getBytesFromMib(1024 * 10))
    assert(shouldOverrideMemoryConf(sparkConf))
  }

  test("Comet memory overhead (min)") {
    val sparkConf = new SparkConf()
    sparkConf.set(CometConf.COMET_MEMORY_OVERHEAD_MIN_MIB.key, "2g")
    assert(getCometMemoryOverhead(sparkConf) == getBytesFromMib(1024 * 2))
  }

  test("Comet memory overhead (factor)") {
    val sparkConf = new SparkConf()
    sparkConf.set(CometConf.COMET_MEMORY_OVERHEAD_FACTOR.key, "0.5")
    assert(getCometMemoryOverhead(sparkConf) == getBytesFromMib(512))
  }

  test("Comet memory overhead (off heap)") {
    val sparkConf = new SparkConf()
    sparkConf.set(CometConf.COMET_MEMORY_OVERHEAD.key, "64g")
    sparkConf.set("spark.memory.offHeap.enabled", "true")
    sparkConf.set("spark.memory.offHeap.size", "10g")
    assert(getCometMemoryOverhead(sparkConf) == 0)
    assert(!shouldOverrideMemoryConf(sparkConf))
  }

  test("Comet shuffle memory factor") {
    val conf = new SparkConf()

    val sqlConf = new SQLConf
    sqlConf.setConfString(CometConf.COMET_COLUMNAR_SHUFFLE_MEMORY_FACTOR.key, "0.2")

    // Minimum Comet memory overhead is 384MB
    assert(
      getCometShuffleMemorySize(conf, sqlConf) ==
        getBytesFromMib((384 * 0.2).toLong))

    conf.set(CometConf.COMET_MEMORY_OVERHEAD_FACTOR.key, "0.5")
    assert(
      getCometShuffleMemorySize(conf, sqlConf) ==
        getBytesFromMib((1024 * 0.5 * 0.2).toLong))
  }

  test("Comet shuffle memory") {
    val conf = new SparkConf()
    val sqlConf = new SQLConf
    conf.set(CometConf.COMET_MEMORY_OVERHEAD.key, "1g")
    sqlConf.setConfString(CometConf.COMET_COLUMNAR_SHUFFLE_MEMORY_SIZE.key, "512m")

    assert(getCometShuffleMemorySize(conf, sqlConf) == getBytesFromMib(512))
  }

  test("Comet shuffle memory (off-heap)") {
    val conf = new SparkConf()
    val sqlConf = new SQLConf
    conf.set(CometConf.COMET_MEMORY_OVERHEAD.key, "1g")
    conf.set("spark.memory.offHeap.enabled", "true")
    conf.set("spark.memory.offHeap.size", "10g")
    sqlConf.setConfString(CometConf.COMET_COLUMNAR_SHUFFLE_MEMORY_SIZE.key, "512m")

    assertThrows[AssertionError] {
      getCometShuffleMemorySize(conf, sqlConf)
    }
  }

  test("Comet shuffle memory cannot be larger than Comet memory overhead") {
    val conf = new SparkConf()
    val sqlConf = new SQLConf
    conf.set(CometConf.COMET_MEMORY_OVERHEAD.key, "1g")
    sqlConf.setConfString(CometConf.COMET_COLUMNAR_SHUFFLE_MEMORY_SIZE.key, "10g")
    assert(getCometShuffleMemorySize(conf, sqlConf) == getBytesFromMib(1024))
  }
}
