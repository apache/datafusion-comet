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

  test("isCometLoaded") {
    val conf = new SQLConf

    conf.setConfString(CometConf.COMET_ENABLED.key, "false")
    assert(!isCometLoaded(conf))

    // Since the native lib is probably already loaded due to previous tests, we reset it here
    NativeBase.setLoaded(false)

    conf.setConfString(CometConf.COMET_ENABLED.key, "true")
    val oldProperty = System.getProperty("os.name")
    System.setProperty("os.name", "foo")
    assert(!isCometLoaded(conf))

    System.setProperty("os.name", oldProperty)

    conf.setConf(SQLConf.PARQUET_INT96_TIMESTAMP_CONVERSION, true)
    assert(!isCometLoaded(conf))

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

  test("Default Comet memory overhead") {
    val conf = new SparkConf()
    assert(getCometMemoryOverhead(conf) == getBytesFromMib(1024))
  }

  test("Comet memory overhead") {
    val sparkConf = new SparkConf()
    sparkConf.set(CometConf.COMET_ONHEAP_MEMORY_OVERHEAD.key, "10g")
    assert(getCometMemoryOverhead(sparkConf) == getBytesFromMib(1024 * 10))
    assert(shouldOverrideMemoryConf(sparkConf))
  }

  test("Comet memory overhead (off heap)") {
    val sparkConf = new SparkConf()
    sparkConf.set(CometConf.COMET_ONHEAP_MEMORY_OVERHEAD.key, "64g")
    sparkConf.set("spark.memory.offHeap.enabled", "true")
    sparkConf.set("spark.memory.offHeap.size", "10g")
    assert(getCometMemoryOverhead(sparkConf) == 0)
    assert(!shouldOverrideMemoryConf(sparkConf))
  }

  test("Comet shuffle memory factor") {
    val conf = new SparkConf()

    val sqlConf = new SQLConf
    sqlConf.setConfString(CometConf.COMET_ONHEAP_SHUFFLE_MEMORY_FACTOR.key, "0.2")

    // Minimum Comet memory overhead is 384MB
    assert(
      getCometShuffleMemorySize(conf, sqlConf) ==
        getBytesFromMib((1024 * 0.2).toLong))
  }
}
