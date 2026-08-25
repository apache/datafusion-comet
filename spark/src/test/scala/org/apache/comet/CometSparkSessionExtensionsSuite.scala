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
import org.apache.spark.sql.catalyst.plans.physical.{RoundRobinPartitioning, SinglePartition}
import org.apache.spark.sql.comet.{CometNativeExec, CometSinkPlaceHolder}
import org.apache.spark.sql.comet.execution.shuffle.{CometCelebornShuffleManager, CometColumnarShuffle, CometNativeShuffle, CometShuffleExchangeExec, CometShuffleManager}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.serde.OperatorOuterClass

class CometSparkSessionExtensionsSuite extends CometTestBase {

  import CometSparkSessionExtensions._

  private def withShuffleManagerSession(
      manager: String,
      mode: String = "auto",
      nativeExecution: Boolean = true)(f: SQLConf => Unit): Unit = {
    val session = spark.newSession()
    val conf = session.sessionState.conf
    conf.setConfString(CometConf.COMET_ENABLED.key, "true")
    conf.setConfString(CometConf.COMET_SHUFFLE_ENABLED.key, "true")
    conf.setConfString(CometConf.COMET_SHUFFLE_MODE.key, mode)
    conf.setConfString(CometConf.COMET_EXEC_ENABLED.key, nativeExecution.toString)
    conf.setConfString("spark.shuffle.manager", manager)

    val previousActiveSession = SparkSession.getActiveSession
    try {
      SparkSession.setActiveSession(session)
      f(conf)
    } finally {
      previousActiveSession match {
        case Some(previousSession) => SparkSession.setActiveSession(previousSession)
        case None => SparkSession.clearActiveSession()
      }
    }
  }

  private def nativeShuffleChild(): CometNativeExec = {
    val child = spark.emptyDataFrame.queryExecution.executedPlan
    CometSinkPlaceHolder(OperatorOuterClass.Operator.getDefaultInstance, child, child)
  }

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

  test("isCometLoaded requires a supported Comet shuffle manager when shuffle.enabled=true") {
    val conf = new SQLConf
    conf.setConfString(CometConf.COMET_ENABLED.key, "true")

    // Default: shuffle.enabled=true. Without spark.shuffle.manager set, Comet must be disabled.
    assert(!isCometLoaded(conf))

    // Opt out: shuffle.enabled=false. Comet should load (assumes native lib is available).
    conf.setConfString(CometConf.COMET_SHUFFLE_ENABLED.key, "false")
    assert(isCometLoaded(conf))

    // shuffle.enabled=true with the Comet shuffle manager registered: Comet should load.
    conf.setConfString(CometConf.COMET_SHUFFLE_ENABLED.key, "true")
    conf.setConfString("spark.shuffle.manager", classOf[CometShuffleManager].getName)
    assert(isCometLoaded(conf))
    assert(isCometShuffleManagerEnabled(conf))
    assert(isCometShuffleEnabled(conf))
  }

  test("Celeborn manager enables Comet shuffle only with explicit native opt-in") {
    val conf = new SQLConf
    conf.setConfString(CometConf.COMET_ENABLED.key, "true")
    conf.setConfString(CometConf.COMET_SHUFFLE_ENABLED.key, "true")
    conf.setConfString(CometConf.COMET_EXEC_ENABLED.key, "true")
    conf.setConfString("spark.shuffle.manager", classOf[CometCelebornShuffleManager].getName)

    assert(isCometShuffleManagerEnabled(conf))
    assert(isCometLoaded(conf))
    assert(!isCometShuffleEnabled(conf), "default auto mode must preserve Spark shuffle")

    Seq("auto", "jvm").foreach { mode =>
      conf.setConfString(CometConf.COMET_SHUFFLE_MODE.key, mode)
      assert(
        !isCometShuffleEnabled(conf),
        s"Celeborn native shuffle must not be enabled for mode=$mode")
    }

    conf.setConfString(CometConf.COMET_SHUFFLE_MODE.key, "native")
    assert(isCometShuffleEnabled(conf))

    conf.setConfString(CometConf.COMET_SHUFFLE_CELEBORN_ENABLED.key, "false")
    assert(!isCometShuffleEnabled(conf))
    conf.setConfString(CometConf.COMET_SHUFFLE_CELEBORN_ENABLED.key, "true")

    conf.setConfString("spark.io.encryption.enabled", "true")
    assert(!isCometShuffleEnabled(conf))
    conf.setConfString("spark.io.encryption.enabled", "false")
    assert(isCometShuffleEnabled(conf))

    conf.setConfString(CometConf.COMET_EXEC_ENABLED.key, "false")
    assert(!isCometShuffleEnabled(conf))

    conf.setConfString(CometConf.COMET_EXEC_ENABLED.key, "true")
    conf.setConfString(CometConf.COMET_SHUFFLE_ENABLED.key, "false")
    assert(!isCometShuffleEnabled(conf))
  }

  test("Celeborn manager selects native shuffle for supported Comet native children") {
    val child = nativeShuffleChild()

    withShuffleManagerSession(classOf[CometCelebornShuffleManager].getName, "native") { _ =>
      val shuffle = ShuffleExchangeExec(SinglePartition, child)

      assert(CometShuffleExchangeExec.shuffleSupported(shuffle).contains(CometNativeShuffle))
      assert(shuffle.getTagValue(CometExplainInfo.FALLBACK_REASONS).isEmpty)
    }
  }

  test("Celeborn manager keeps encrypted Spark shuffle on the existing Celeborn path") {
    val child = nativeShuffleChild()

    withShuffleManagerSession(classOf[CometCelebornShuffleManager].getName, "native") { conf =>
      conf.setConfString("spark.io.encryption.enabled", "true")
      assert(!isCometShuffleEnabled(conf))

      val shuffle = ShuffleExchangeExec(SinglePartition, child)
      assert(CometShuffleExchangeExec.shuffleSupported(shuffle).isEmpty)
      assert(
        shuffle
          .getTagValue(CometExplainInfo.FALLBACK_REASONS)
          .getOrElse(Set.empty[String])
          .exists(_.contains("spark.io.encryption.enabled=true")))
    }
  }

  test("Celeborn manager preserves Spark shuffle for default and explicit auto mode") {
    val child = nativeShuffleChild()

    Seq(false, true).foreach { explicitAutoMode =>
      withShuffleManagerSession(classOf[CometCelebornShuffleManager].getName) { conf =>
        if (!explicitAutoMode) {
          conf.unsetConf(CometConf.COMET_SHUFFLE_MODE.key)
        }
        assert(CometConf.COMET_SHUFFLE_MODE.get(conf) == "auto")
        assert(!isCometShuffleEnabled(conf))

        val shuffle = ShuffleExchangeExec(SinglePartition, child)
        assert(CometShuffleExchangeExec.shuffleSupported(shuffle).isEmpty)
        assert(
          shuffle
            .getTagValue(CometExplainInfo.FALLBACK_REASONS)
            .getOrElse(Set.empty[String])
            .exists(_.contains("requires spark.comet.shuffle.mode=native")))
      }
    }
  }

  test("Celeborn manager leaves non-native Spark children on the Spark shuffle path") {
    val sparkChild = spark.emptyDataFrame.queryExecution.executedPlan
    val nativeChild = nativeShuffleChild()

    withShuffleManagerSession(classOf[CometCelebornShuffleManager].getName, "native") { _ =>
      val shuffle = ShuffleExchangeExec(SinglePartition, sparkChild)

      assert(CometShuffleExchangeExec.shuffleSupported(shuffle).isEmpty)
      val reasons = shuffle
        .getTagValue(CometExplainInfo.FALLBACK_REASONS)
        .getOrElse(Set.empty[String])
      assert(reasons.exists(_.contains("requires a Comet child")))
      assert(reasons.exists(_.contains("columnar shuffle is not supported")))

      // AQE may reshape the child later; a prior Spark-fallback decision must remain sticky.
      val reshaped = shuffle.withNewChildren(Seq(nativeChild)).asInstanceOf[ShuffleExchangeExec]
      assert(CometShuffleExchangeExec.shuffleSupported(reshaped).isEmpty)
    }
  }

  test("unsupported Celeborn native partitioning falls back to Spark instead of Comet columnar") {
    val child = nativeShuffleChild()

    withShuffleManagerSession(classOf[CometCelebornShuffleManager].getName, "native") { _ =>
      val shuffle = ShuffleExchangeExec(RoundRobinPartitioning(2), child)

      assert(CometShuffleExchangeExec.shuffleSupported(shuffle).isEmpty)
      val reasons = shuffle
        .getTagValue(CometExplainInfo.FALLBACK_REASONS)
        .getOrElse(Set.empty[String])
      assert(
        reasons.exists(
          _.contains(CometConf.COMET_SHUFFLE_NATIVE_ROUND_ROBIN_PARTITIONING_ENABLED.key)))
      assert(reasons.exists(_.contains("columnar shuffle is not supported")))
    }

    withShuffleManagerSession(classOf[CometShuffleManager].getName) { _ =>
      val shuffle = ShuffleExchangeExec(RoundRobinPartitioning(2), child)
      assert(CometShuffleExchangeExec.shuffleSupported(shuffle).contains(CometColumnarShuffle))
    }
  }

  test("Celeborn manager explains JVM mode and disabled native execution fallback") {
    val child = nativeShuffleChild()
    val scenarios = Seq(
      ("jvm", true, "does not support spark.comet.shuffle.mode=jvm"),
      ("auto", false, "requires Comet native execution to be enabled"))

    scenarios.foreach { case (mode, nativeExecution, expectedReason) =>
      withShuffleManagerSession(
        classOf[CometCelebornShuffleManager].getName,
        mode,
        nativeExecution) { conf =>
        assert(!isCometShuffleEnabled(conf))

        val shuffle = ShuffleExchangeExec(SinglePartition, child)
        assert(CometShuffleExchangeExec.shuffleSupported(shuffle).isEmpty)
        assert(
          shuffle
            .getTagValue(CometExplainInfo.FALLBACK_REASONS)
            .getOrElse(Set.empty[String])
            .exists(_.contains(expectedReason)))
      }
    }
  }

  test("local Comet manager still supports columnar shuffle without native execution") {
    val child = spark.emptyDataFrame.queryExecution.executedPlan

    withShuffleManagerSession(
      classOf[CometShuffleManager].getName,
      mode = "jvm",
      nativeExecution = false) { conf =>
      assert(isCometShuffleEnabled(conf))
      val shuffle = ShuffleExchangeExec(SinglePartition, child)
      assert(CometShuffleExchangeExec.shuffleSupported(shuffle).contains(CometColumnarShuffle))
    }
  }

  test("stock Celeborn manager does not satisfy Comet shuffle-manager requirements") {
    val conf = new SQLConf
    conf.setConfString(CometConf.COMET_ENABLED.key, "true")
    conf.setConfString(CometConf.COMET_SHUFFLE_ENABLED.key, "true")
    conf.setConfString(
      "spark.shuffle.manager",
      "org.apache.spark.shuffle.celeborn.SparkShuffleManager")

    assert(!isCometShuffleManagerEnabled(conf))
    assert(!isCometShuffleEnabled(conf))
    assert(!isCometLoaded(conf))
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
