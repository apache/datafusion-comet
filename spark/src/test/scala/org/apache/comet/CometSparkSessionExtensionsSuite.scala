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

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.plans.physical.{RoundRobinPartitioning, SinglePartition}
import org.apache.spark.sql.comet.CometScanWrapper
import org.apache.spark.sql.comet.execution.shuffle.{CometCelebornShuffleManager, CometColumnarShuffle, CometShuffleExchangeExec}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.LongType

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

  test("isCometLoaded follows the application's shuffle manager, not the session conf") {
    // This suite's SparkContext runs CometShuffleManager. A session conf naming another manager,
    // which SparkSession.Builder can leave behind, does not change what runs the shuffle.
    Seq(
      "org.apache.spark.shuffle.sort.SortShuffleManager",
      "org.apache.spark.shuffle.celeborn.SparkShuffleManager").foreach { manager =>
      val conf = new SQLConf
      conf.setConfString(CometConf.COMET_ENABLED.key, "true")
      conf.setConfString(CometConf.COMET_SHUFFLE_ENABLED.key, "true")
      conf.setConfString("spark.shuffle.manager", manager)
      assert(isCometShuffleEnabled(conf), manager)
      assert(isCometLoaded(conf), manager)
    }
  }

  test("the composite manager is recognized without requiring the optional Celeborn client") {
    val conf = new SQLConf
    conf.setConfString(CometConf.COMET_ENABLED.key, "true")
    conf.setConfString(CometConf.COMET_SHUFFLE_ENABLED.key, "true")
    conf.setConfString(CometConf.COMET_SHUFFLE_MODE.key, "native")
    conf.setConfString("spark.shuffle.manager", classOf[CometCelebornShuffleManager].getName)
    assert(isCometShuffleManagerEnabled)
    assert(isCometLoaded(conf))
    // A session-only setting must not replace this suite's actual local shuffle manager.
    assert(!isCometShuffleEnabled(conf))
  }

  test("local auto mode retains Comet columnar fallback for unsupported native partitioning") {
    withSQLConf(
      CometConf.COMET_SHUFFLE_MODE.key -> "auto",
      CometConf.COMET_SHUFFLE_NATIVE_ROUND_ROBIN_PARTITIONING_ENABLED.key -> "false") {
      val leaf = spark.sessionState.planner
        .plan(LocalRelation(Seq(AttributeReference("value", LongType)())))
        .next()
      val child = CometScanWrapper(OperatorOuterClass.Operator.getDefaultInstance, leaf)
      val exchange = ShuffleExchangeExec(RoundRobinPartitioning(2), child)
      assert(CometShuffleExchangeExec.shuffleSupported(exchange).contains(CometColumnarShuffle))
    }
  }

  test("local JVM shuffle remains available when native execution is disabled") {
    withSQLConf(
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm",
      CometConf.COMET_EXEC_ENABLED.key -> "false") {
      val child = spark.sessionState.planner
        .plan(LocalRelation(Seq(AttributeReference("value", LongType)())))
        .next()
      val exchange = ShuffleExchangeExec(SinglePartition, child)
      assert(isCometShuffleEnabled(spark.sessionState.conf))
      assert(CometShuffleExchangeExec.shuffleSupported(exchange).contains(CometColumnarShuffle))
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
}
