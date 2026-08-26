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
import org.apache.spark.sql.catalyst.expressions.{Ascending, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, RangePartitioning, SinglePartition}
import org.apache.spark.sql.comet.CometSinkPlaceHolder
import org.apache.spark.sql.comet.execution.shuffle.{CometColumnarShuffle, CometNativeShuffle, CometShuffleExchangeExec, CometShuffleManager}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.serde.OperatorOuterClass

class CometSparkSessionExtensionsSuite extends CometTestBase {

  import CometSparkSessionExtensions._

  test("isCometLoaded") {
    val conf = new SQLConf
    // Disable Comet shuffle so this test can focus on other checks without needing
    // spark.shuffle.manager to be set.
    conf.setConfString(CometConf.COMET_SHUFFLE_ENABLED.key, "false")

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

  test("isCometLoaded requires CometShuffleManager when shuffle.enabled=true") {
    val conf = new SQLConf
    conf.setConfString(CometConf.COMET_ENABLED.key, "true")

    // Default: shuffle.enabled=true. Without spark.shuffle.manager set, Comet must be disabled.
    assert(!isCometLoaded(conf))

    // Opt out: shuffle.enabled=false. Comet should load (assumes native lib is available).
    conf.setConfString(CometConf.COMET_SHUFFLE_ENABLED.key, "false")
    assert(isCometLoaded(conf))

    // shuffle.enabled=true with the Comet shuffle manager registered: Comet should load.
    conf.setConfString(CometConf.COMET_SHUFFLE_ENABLED.key, "true")
    conf.setConfString(
      "spark.shuffle.manager",
      "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager")
    assert(isCometLoaded(conf))
  }

  test("wide decimal hash keys use Spark-compatible shuffle partitioning") {
    for {
      mode <- Seq("native", "auto", "jvm")
      precision <- Seq(18, 19, 38)
    } {
      withSQLConf(
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_SHUFFLE_ENABLED.key -> "true",
        CometConf.COMET_SHUFFLE_MODE.key -> mode,
        CometConf.COMET_SHUFFLE_NATIVE_HASH_PARTITIONING_ENABLED.key -> "true",
        CometConf.COMET_SHUFFLE_NATIVE_RANGE_PARTITIONING_ENABLED.key -> "true",
        "spark.shuffle.manager" -> classOf[CometShuffleManager].getName) {
        val originalChild = spark
          .range(1)
          .selectExpr(s"CAST(id AS DECIMAL($precision, 0)) AS d", "id")
          .queryExecution
          .executedPlan
        val child = CometSinkPlaceHolder(
          OperatorOuterClass.Operator.getDefaultInstance,
          originalChild,
          originalChild)
        val shuffle = ShuffleExchangeExec(HashPartitioning(Seq(child.output.head), 2), child)
        val expected = if (mode == "jvm" || (mode == "auto" && precision > 18)) {
          Some(CometColumnarShuffle)
        } else if (precision <= 18) {
          Some(CometNativeShuffle)
        } else {
          None
        }

        withClue(s"mode=$mode, precision=$precision: ") {
          assert(CometShuffleExchangeExec.shuffleSupported(shuffle) == expected)
          if (expected.isEmpty) {
            assert(
              shuffle
                .getTagValue(CometExplainInfo.FALLBACK_REASONS)
                .getOrElse(Set.empty[String])
                .exists(_.contains("unsupported hash partitioning data type for native shuffle")))
          } else {
            // A native failure must not tag an exchange that can use the columnar path.
            assert(shuffle.getTagValue(CometExplainInfo.FALLBACK_REASONS).isEmpty)
          }

          if (mode == "native" && precision == 38) {
            // Wide decimals remain supported as payloads, range keys, and in a single partition.
            Seq(
              HashPartitioning(Seq(child.output(1)), 2),
              RangePartitioning(Seq(SortOrder(child.output.head, Ascending)), 2),
              SinglePartition).foreach { partitioning =>
              val supported = ShuffleExchangeExec(partitioning, child)
              assert(
                CometShuffleExchangeExec.shuffleSupported(supported).contains(CometNativeShuffle))
            }
          }
        }
      }
    }
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
    sqlConf.setConfString(CometConf.COMET_SHUFFLE_JVM_MEMORY_FACTOR.key, "0.2")

    // Minimum Comet memory overhead is 384MB
    assert(
      getCometShuffleMemorySize(conf, sqlConf) ==
        getBytesFromMib((1024 * 0.2).toLong))
  }
}
