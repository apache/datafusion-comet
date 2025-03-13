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

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.internal.StaticSQLConf

class CometPluginsSuite extends CometTestBase {
  override protected def sparkConf: SparkConf = {
    val conf = new SparkConf()
    conf.set("spark.driver.memory", "1G")
    conf.set("spark.executor.memory", "1G")
    conf.set("spark.executor.memoryOverhead", "2G")
    conf.set("spark.plugins", "org.apache.spark.CometPlugin")
    conf.set("spark.comet.enabled", "true")
    conf.set("spark.comet.exec.enabled", "true")
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

  test("Default Comet memory overhead") {
    val execMemOverhead1 = spark.conf.get("spark.executor.memoryOverhead")
    val execMemOverhead2 = spark.sessionState.conf.getConfString("spark.executor.memoryOverhead")
    val execMemOverhead3 = spark.sparkContext.getConf.get("spark.executor.memoryOverhead")
    val execMemOverhead4 = spark.sparkContext.conf.get("spark.executor.memoryOverhead")

    // 2GB + 384MB (default Comet memory overhead)
    assert(execMemOverhead1 == "2432M")
    assert(execMemOverhead2 == "2432M")
    assert(execMemOverhead3 == "2432M")
    assert(execMemOverhead4 == "2432M")
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
    conf.set("spark.comet.exec.shuffle.enabled", "true")
    conf
  }

  test("Default executor memory overhead + Comet memory overhead") {
    val execMemOverhead1 = spark.conf.get("spark.executor.memoryOverhead")
    val execMemOverhead2 = spark.sessionState.conf.getConfString("spark.executor.memoryOverhead")
    val execMemOverhead3 = spark.sparkContext.getConf.get("spark.executor.memoryOverhead")
    val execMemOverhead4 = spark.sparkContext.conf.get("spark.executor.memoryOverhead")

    // Spark executor memory overhead = executor memory (1G) * memoryOverheadFactor (0.5) = 512MB
    // 512MB + 384MB (default Comet memory overhead)
    assert(execMemOverhead1 == "896M")
    assert(execMemOverhead2 == "896M")
    assert(execMemOverhead3 == "896M")
    assert(execMemOverhead4 == "896M")
  }
}

class CometPluginsNonOverrideSuite extends CometTestBase {
  override protected def sparkConf: SparkConf = {
    val conf = new SparkConf()
    conf.set("spark.driver.memory", "1G")
    conf.set("spark.executor.memory", "1G")
    conf.set("spark.executor.memoryOverhead", "2G")
    conf.set("spark.executor.memoryOverheadFactor", "0.5")
    conf.set("spark.plugins", "org.apache.spark.CometPlugin")
    conf.set("spark.comet.enabled", "true")
    conf.set("spark.comet.exec.shuffle.enabled", "false")
    conf.set("spark.comet.exec.enabled", "false")
    conf
  }

  test("executor memory overhead is not overridden") {
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

class CometPluginsUnifiedModeOverrideSuite extends CometTestBase {
  override protected def sparkConf: SparkConf = {
    val conf = new SparkConf()
    conf.set("spark.driver.memory", "1G")
    conf.set("spark.executor.memory", "1G")
    conf.set("spark.executor.memoryOverhead", "1G")
    conf.set("spark.plugins", "org.apache.spark.CometPlugin")
    conf.set("spark.comet.enabled", "true")
    conf.set("spark.memory.offHeap.enabled", "true")
    conf.set("spark.memory.offHeap.size", "2G")
    conf.set("spark.comet.exec.shuffle.enabled", "true")
    conf.set("spark.comet.exec.enabled", "true")
    conf.set("spark.comet.memory.overhead.factor", "0.5")
    conf
  }
}
