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
import java.lang.management.ManagementFactory
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.concurrent.TimeUnit

import scala.jdk.CollectionConverters._

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{expr, sum}
import org.apache.spark.util.Utils

/**
 * Entry point run in a child JVM by [[CometRuntimeShutdownSuite]]. Executes a native Comet query
 * and then returns from `main` without calling `SparkContext.stop()`.
 */
object CometRuntimeShutdownMain {
  val Marker = "comet native query executed"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .config("spark.plugins", "org.apache.spark.CometPlugin")
      .config(
        "spark.shuffle.manager",
        "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager")
      .config("spark.sql.extensions", "org.apache.comet.CometSparkSessionExtensions")
      .config("spark.comet.enabled", "true")
      .config("spark.comet.exec.enabled", "true")
      .config("spark.memory.offHeap.enabled", "true")
      .config("spark.memory.offHeap.size", "256m")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    val dir = Files.createTempDirectory("comet-runtime-shutdown").toFile
    try {
      val path = dir.getAbsolutePath
      spark.range(100000).write.mode("overwrite").parquet(path)
      // The aggregate reserves memory through the unified pool, which calls back into the JVM
      // from a tokio worker thread.
      val df = spark.read.parquet(path).groupBy(expr("id % 10")).agg(sum("id"))
      df.collect()
      val plan = df.queryExecution.executedPlan.toString
      require(plan.contains("CometHashAggregate"), s"Expected a native plan but got:\n$plan")
    } finally {
      Utils.deleteRecursively(dir)
    }
    // scalastyle:off println
    println(Marker)
    // scalastyle:on println
  }
}

/**
 * Runs Comet in a child JVM because the failure mode under test, the JVM being unable to exit,
 * cannot be observed from inside the affected JVM.
 */
class CometRuntimeShutdownSuite extends AnyFunSuite {

  test("JVM exits when main returns without SparkContext.stop()") {
    val javaBin = new File(System.getProperty("java.home"), "bin/java").getAbsolutePath
    val jvmArgs = ManagementFactory.getRuntimeMXBean.getInputArguments.asScala
      .filter(a => a.startsWith("--add-opens") || a.startsWith("--add-exports"))
    val cmd = Seq(javaBin, "-Xmx1g") ++ jvmArgs ++ Seq(
      "-cp",
      System.getProperty("java.class.path"),
      CometRuntimeShutdownMain.getClass.getName.stripSuffix("$"))

    // java.io.tmpdir is pinned to target/tmp by the pom and may not exist yet
    val tmpDir = Files.createDirectories(Paths.get(System.getProperty("java.io.tmpdir")))
    val output = Files.createTempFile(tmpDir, "comet-runtime-shutdown", ".log").toFile
    try {
      val process =
        new ProcessBuilder(cmd.asJava).redirectErrorStream(true).redirectOutput(output).start()
      val exited = process.waitFor(2, TimeUnit.MINUTES)
      if (!exited) {
        process.destroyForcibly().waitFor()
      }
      val log = new String(Files.readAllBytes(output.toPath), StandardCharsets.UTF_8)
      val tail = log.linesIterator.toSeq.takeRight(30).mkString("\n")
      assert(
        log.contains(CometRuntimeShutdownMain.Marker),
        s"Child JVM did not run query:\n$tail")
      assert(exited, s"Child JVM did not exit after main returned:\n$tail")
      assert(process.exitValue() == 0, s"Child JVM exited with ${process.exitValue()}:\n$tail")
    } finally {
      output.delete()
    }
  }
}
