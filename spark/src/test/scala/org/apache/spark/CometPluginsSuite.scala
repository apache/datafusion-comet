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

package org.apache.spark

import java.io.File

import org.apache.logging.log4j.Level
import org.apache.spark.sql.{CometTestBase, SaveMode}
import org.apache.spark.sql.comet.CometPlan
import org.apache.spark.sql.internal.StaticSQLConf

import org.apache.comet.{COMET_VERSION, CometConf, CometSparkSessionExtensions}

class CometPluginsSuite extends CometTestBase {
  override protected def sparkConf: SparkConf = {
    val conf = new SparkConf()
    conf.set("spark.driver.memory", "1G")
    conf.set("spark.executor.memory", "1G")
    conf.set("spark.executor.memoryOverhead", "2G")
    conf.set("spark.plugins", "org.apache.spark.CometPlugin")
    conf.set(
      "spark.shuffle.manager",
      "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager")
    conf.set("spark.comet.enabled", "true")
    conf.set("spark.comet.exec.enabled", "true")
    conf.set("spark.comet.exec.onHeap.enabled", "true")
    conf.set("spark.comet.metrics.enabled", "true")
    conf
  }

  test("Register Comet extension") {
    // test common case where no extensions are previously registered
    {
      val conf = new SparkConf()
      CometDriverPlugin.registerCometSessionExtension(conf)
      assert(
        "org.apache.comet.CometSparkSessionExtensions" == conf.get(
          StaticSQLConf.SPARK_SESSION_EXTENSIONS.key))
    }
    // test case where Comet is already registered
    {
      val conf = new SparkConf()
      conf.set(
        StaticSQLConf.SPARK_SESSION_EXTENSIONS.key,
        "org.apache.comet.CometSparkSessionExtensions")
      CometDriverPlugin.registerCometSessionExtension(conf)
      assert(
        "org.apache.comet.CometSparkSessionExtensions" == conf.get(
          StaticSQLConf.SPARK_SESSION_EXTENSIONS.key))
    }
    // test case where other extensions are already registered
    {
      val conf = new SparkConf()
      conf.set(StaticSQLConf.SPARK_SESSION_EXTENSIONS.key, "foo,bar")
      CometDriverPlugin.registerCometSessionExtension(conf)
      assert(
        "foo,bar,org.apache.comet.CometSparkSessionExtensions" == conf.get(
          StaticSQLConf.SPARK_SESSION_EXTENSIONS.key))
    }
    // test case where other extensions, including Comet, are already registered
    {
      val conf = new SparkConf()
      conf.set(
        StaticSQLConf.SPARK_SESSION_EXTENSIONS.key,
        "foo,bar,org.apache.comet.CometSparkSessionExtensions")
      CometDriverPlugin.registerCometSessionExtension(conf)
      assert(
        "foo,bar,org.apache.comet.CometSparkSessionExtensions" == conf.get(
          StaticSQLConf.SPARK_SESSION_EXTENSIONS.key))
    }
  }

  test("Comet version is exposed as a Spark config") {
    // The driver plugin sets spark.comet.version, which is then visible both on the SparkContext
    // conf and through the session runtime config (SET / spark.conf.get).
    assert(spark.sparkContext.conf.get(CometDriverPlugin.COMET_VERSION_CONFIG) == COMET_VERSION)
    assert(spark.conf.get(CometDriverPlugin.COMET_VERSION_CONFIG) == COMET_VERSION)
  }

  test("CometSource metrics are recorded") {
    val nativeBefore = CometSource.NATIVE_OPERATORS.getCount
    val queriesBefore = CometSource.QUERIES_PLANNED.getCount

    withTempPath { dir =>
      val path = new File(dir, "test.parquet").toString
      spark.range(1000).toDF("id").write.mode(SaveMode.Overwrite).parquet(path)
      spark.read.parquet(path).filter("id > 500").collect()
    }
    spark.sparkContext.listenerBus.waitUntilEmpty()
    assert(
      CometSource.QUERIES_PLANNED.getCount > queriesBefore,
      "queries.planned should increment after query")
    assert(
      CometSource.NATIVE_OPERATORS.getCount > nativeBefore,
      "operators.native should increment for native execution")
  }

  test("metrics not double counted with AQE") {
    withSQLConf("spark.sql.adaptive.enabled" -> "true") {
      withTempPath { dir =>
        val path = new File(dir, "test.parquet").toString
        spark.range(10000).toDF("id").write.mode(SaveMode.Overwrite).parquet(path)

        spark.sparkContext.listenerBus.waitUntilEmpty()
        val queriesBefore = CometSource.QUERIES_PLANNED.getCount
        spark.read.parquet(path).filter("id > 100").collect()
        spark.read.parquet(path).filter("id > 200").collect()
        spark.sparkContext.listenerBus.waitUntilEmpty()
        val queriesAfter = CometSource.QUERIES_PLANNED.getCount
        assert(
          queriesAfter == queriesBefore + 2,
          s"Expected 2 queries, got ${queriesAfter - queriesBefore}")
      }
    }
  }

  test("executor memory overhead is left alone") {
    // Comet does not adjust spark.executor.memoryOverhead. A driver plugin runs too late to
    // influence the executor container on Spark 3.4, 3.5 and 4.0, so the value the application
    // set is the value that is used.
    val execMemOverhead1 = spark.conf.get("spark.executor.memoryOverhead")
    val execMemOverhead2 = spark.sessionState.conf.getConfString("spark.executor.memoryOverhead")
    val execMemOverhead3 = spark.sparkContext.getConf.get("spark.executor.memoryOverhead")
    val execMemOverhead4 = spark.sparkContext.conf.get("spark.executor.memoryOverhead")

    assert(execMemOverhead1 == "2G")
    assert(execMemOverhead2 == "2G")
    assert(execMemOverhead3 == "2G")
    assert(execMemOverhead4 == "2G")
  }
}

class CometPluginsDefaultSuite extends CometTestBase {
  override protected def sparkConf: SparkConf = {
    val conf = new SparkConf()
    conf.set("spark.driver.memory", "1G")
    conf.set("spark.executor.memory", "1G")
    conf.set("spark.executor.memoryOverheadFactor", "0.5")
    conf.set("spark.plugins", "org.apache.spark.CometPlugin")
    conf.set("spark.comet.enabled", "true")
    conf.set("spark.comet.shuffle.enabled", "true")
    conf.set("spark.comet.exec.onHeap.enabled", "true")
    conf
  }

  test("unset executor memory overhead is left unset") {
    assert(!spark.sparkContext.conf.contains("spark.executor.memoryOverhead"))
  }
}

class CometPluginsMemoryOverheadWarningSuite extends CometTestBase {

  private val warning = "spark.executor.memoryOverhead is not set"

  private def warningsFor(conf: SparkConf): Seq[String] = {
    // Logging derives the logger name by stripping the object's trailing '$'
    val logger = CometDriverPlugin.getClass.getName.stripSuffix("$")
    val appender = new LogAppender("executor memory overhead warning")
    withLogAppender(appender, Seq(logger), Some(Level.WARN)) {
      CometDriverPlugin.warnIfExecutorMemoryOverheadUnset(conf)
    }
    appender.loggingEvents.map(_.getMessage.getFormattedMessage).toSeq
  }

  test("warns when executor memory overhead is unset and Comet is active") {
    val conf = new SparkConf()
    conf.set("spark.comet.enabled", "true")
    conf.set("spark.comet.exec.enabled", "true")
    assert(warningsFor(conf).exists(_.contains(warning)))
  }

  test("does not warn when executor memory overhead is set") {
    val conf = new SparkConf()
    conf.set("spark.comet.enabled", "true")
    conf.set("spark.comet.exec.enabled", "true")
    conf.set("spark.executor.memoryOverhead", "2g")
    assert(!warningsFor(conf).exists(_.contains(warning)))
  }

  test("does not warn when Comet is not executing anything") {
    val conf = new SparkConf()
    conf.set("spark.comet.enabled", "true")
    conf.set("spark.comet.exec.enabled", "false")
    conf.set("spark.comet.shuffle.enabled", "false")
    assert(!warningsFor(conf).exists(_.contains(warning)))
  }
}

class CometPluginsMemoryPoolFractionWarningSuite extends CometTestBase {

  private val warning = "spark.comet.exec.memoryPool.fraction=0.8 is deprecated"

  private def warningsFor(conf: SparkConf): Seq[String] = {
    // Logging derives the logger name by stripping the object's trailing '$'
    val logger = CometDriverPlugin.getClass.getName.stripSuffix("$")
    val appender = new LogAppender("memory pool fraction warning")
    withLogAppender(appender, Seq(logger), Some(Level.WARN)) {
      CometDriverPlugin.warnIfMemoryPoolFractionSet(conf)
    }
    appender.loggingEvents.map(_.getMessage.getFormattedMessage).toSeq
  }

  test("warns when the memory pool fraction is set") {
    val conf = new SparkConf().set("spark.comet.exec.memoryPool.fraction", "0.8")
    assert(warningsFor(conf).exists(_.contains(warning)))
  }

  test("does not warn when the memory pool fraction is unset") {
    assert(!warningsFor(new SparkConf()).exists(_.contains("memoryPool.fraction")))
  }
}

class CometPluginsUnifiedModeSuite extends CometTestBase {
  override protected def sparkConf: SparkConf = {
    val conf = new SparkConf()
    conf.set("spark.driver.memory", "1G")
    conf.set("spark.executor.memory", "1G")
    conf.set("spark.executor.memoryOverhead", "1G")
    conf.set("spark.plugins", "org.apache.spark.CometPlugin")
    conf.set("spark.comet.enabled", "true")
    conf.set("spark.memory.offHeap.enabled", "true")
    conf.set("spark.memory.offHeap.size", "2G")
    conf.set("spark.comet.shuffle.enabled", "true")
    conf.set("spark.comet.exec.enabled", "true")
    conf
  }

  test("executor memory overhead is left alone in off-heap mode") {
    val execMemOverhead1 = spark.conf.get("spark.executor.memoryOverhead")
    val execMemOverhead2 = spark.sessionState.conf.getConfString("spark.executor.memoryOverhead")
    val execMemOverhead3 = spark.sparkContext.getConf.get("spark.executor.memoryOverhead")
    val execMemOverhead4 = spark.sparkContext.conf.get("spark.executor.memoryOverhead")

    assert(execMemOverhead1 == "1G")
    assert(execMemOverhead2 == "1G")
    assert(execMemOverhead3 == "1G")
    assert(execMemOverhead4 == "1G")
  }
}

class CometPluginsExtensionOnlySuite extends CometTestBase {
  // No plugin and no off-heap memory. CometTestBase registers CometSparkSessionExtensions
  // directly, as an application can with spark.sql.extensions.
  override protected def sparkConf: SparkConf = {
    val conf = new SparkConf()
    conf.set(
      "spark.shuffle.manager",
      "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager")
    conf.set("spark.comet.enabled", "true")
    conf.set("spark.comet.exec.enabled", "true")
    // Set explicitly, since ENABLE_COMET_ONHEAP in the environment changes the default.
    conf.set(CometConf.COMET_ONHEAP_ENABLED.key, "false")
    conf
  }

  private val query = "SELECT _1 FROM tbl WHERE _1 > 5"

  test("Comet is disabled when off-heap memory is disabled") {
    // Logging derives the logger name by stripping the object's trailing '$'
    val logger = CometSparkSessionExtensions.getClass.getName.stripSuffix("$")
    val appender = new LogAppender("off-heap mode warning")
    withParquetTable((0 until 10).map(i => (i, i.toString)), "tbl") {
      withLogAppender(appender, Seq(logger), Some(Level.WARN)) {
        val (_, plan) = checkSparkAnswer(query)
        assert(collect(plan) { case op: CometPlan => op }.isEmpty, plan)
      }
    }
    assert(
      appender.loggingEvents
        .exists(_.getMessage.getFormattedMessage.contains("not running in off-heap mode")))
  }

  test("spark.comet.exec.onHeap.enabled enables Comet without off-heap memory") {
    withParquetTable((0 until 10).map(i => (i, i.toString)), "tbl") {
      withSQLConf(CometConf.COMET_ONHEAP_ENABLED.key -> "true") {
        val (_, plan) = checkSparkAnswer(query)
        assert(collect(plan) { case op: CometPlan => op }.nonEmpty, plan)
      }
    }
  }
}
