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

package org.apache.comet.contrib.delta

import java.nio.file.Files

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.CometDeltaNativeScanExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

import org.apache.comet.CometSparkSessionExtensions

/**
 * Base trait for unit-testing the contrib-delta JVM layer.
 *
 * Wires up Spark+Delta in local mode with the contrib enabled (Comet + Delta session
 * extensions, AQE forced on so Comet's query-stage-prep rules fire) and provides both the
 * native-read assertions (`assertDeltaNativeMatches` / `assertKernelReadEngaged` /
 * `assertDeltaFallback` / `assertNativePlanContains` -- a claimed scan engages
 * `CometDeltaNativeScanExec`) and the decline assertion (`assertNoMarker` -- a declined scan plants
 * no `CometDeltaScanMarker` and runs on vanilla Spark).
 */
trait CometDeltaTestBase extends CometTestBase with AdaptiveSparkPlanHelper {

  /**
   * True iff the io.delta.spark classes are on the test classpath. When false, the test
   * harness can `assume(deltaSparkAvailable, ...)` to skip tests rather than throw.
   * Useful for builds without `-Pcontrib-delta` that still want the test classes to
   * compile (the contrib's reflective bridge means we don't strictly need delta-spark
   * at compile time even when we do need it at test runtime).
   */
  protected def deltaSparkAvailable: Boolean =
    try {
      Class.forName("org.apache.spark.sql.delta.DeltaParquetFileFormat")
      true
    } catch {
      case _: ClassNotFoundException => false
    }

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set("spark.comet.scan.deltaNative.enabled", "true")
    conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    conf.set("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    // Comet registers via injectQueryStagePrepRule, which only fires for plans wrapped
    // in AdaptiveSparkPlanExec. AQE skips simple plans without exchanges; forcing it on
    // here ensures every test plan goes through query-stage prep and Comet's rules see
    // every scan. (The regression-script sbt run gets AQE-wrapped plans naturally
    // because Delta's own queries always include joins/exchanges.)
    conf.set("spark.sql.adaptive.enabled", "true")
    // Pin Spark to loopback so the test JVM doesn't try to reach a remote executor at
    // the host's LAN IP (which may be unreachable when Wi-Fi state is off-network).
    conf.set("spark.driver.bindAddress", "127.0.0.1")
    conf.set("spark.driver.host", "localhost")
    conf
  }

  /**
   * Override to chain Delta's session extension after Comet's. `withExtensions` is
   * additive, so the chain becomes: Comet rules + Delta rules. Setting
   * `spark.sql.extensions` via config would also work but interacts unpredictably
   * with Spark's own `WITH_EXTENSIONS` env wiring in test JVMs.
   */
  override protected def createSparkSession: SparkSessionType = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()

    val deltaExt: org.apache.spark.sql.SparkSessionExtensions => Unit =
      try {
        val cls = Class.forName("io.delta.sql.DeltaSparkSessionExtension")
        val instance = cls.getDeclaredConstructor().newInstance()
        instance.asInstanceOf[org.apache.spark.sql.SparkSessionExtensions => Unit]
      } catch {
        case _: ClassNotFoundException =>
          (_: org.apache.spark.sql.SparkSessionExtensions) => ()
      }

    // Use the standard SparkSession builder (works on Spark 3.5 and 4.x; on Spark 4
    // `org.apache.spark.sql.SparkSession.builder()` returns the classic builder by
    // default, same as `org.apache.spark.sql.classic.SparkSession.builder()`).
    SparkSession
      .builder()
      .config(sparkContext.getConf)
      .withExtensions(new CometSparkSessionExtensions)
      .withExtensions(deltaExt)
      .getOrCreate()
      .asInstanceOf[SparkSessionType]
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark.sparkContext.hadoopConfiguration
      .set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    spark.sparkContext.hadoopConfiguration
      .setBoolean("fs.file.impl.disable.cache", true)
  }

  /** Run `body` with a fresh temp directory and a Delta table path under it. */
  protected def withDeltaTable(testName: String)(body: String => Unit): Unit = {
    val tempDir = Files.createTempDirectory(s"comet-delta-$testName").toFile
    try {
      val tablePath = new java.io.File(tempDir, "t").getAbsolutePath
      body(tablePath)
    } finally {
      deleteRecursively(tempDir)
    }
  }

  /**
   * FQN of the contrib's scan marker, matched by class NAME so the test base stays free of any
   * compile-time dependency on the contrib exec classes (it must also compile on builds where the
   * serde/exec unit is not present).
   */
  private val MarkerClass = "org.apache.spark.sql.comet.CometDeltaScanMarker"

  /** Collect operators whose class is the contrib scan marker, descending into AQE wrappers. */
  private def markersIn(df: DataFrame): Seq[org.apache.spark.sql.execution.SparkPlan] = {
    df.collect() // force AQE to materialise so Comet's query-stage-prep rules fire
    collect(df.queryExecution.executedPlan) { case p if p.getClass.getName == MarkerClass => p }
  }

  /**
   * Assert the rule DECLINED the scan (no `CometDeltaScanMarker` planted) -- the read runs as a
   * vanilla Spark Delta scan. Used for the decline-path cases (unsupported projection, encryption,
   * etc.).
   */
  protected def assertNoMarker(df: DataFrame): Unit = {
    val markers = markersIn(df)
    assert(
      markers.isEmpty,
      s"expected NO CometDeltaScanMarker (declined) but plan was:\n${df.queryExecution.executedPlan}")
  }

  /**
   * Assert that `df`'s executed plan (after a forced `.collect()` so AQE materialises Comet's rules)
   * contains an operator whose simple class name matches each name in `expectedExecs`. Uses the
   * AQE-aware `collect` (from `AdaptiveSparkPlanHelper`) so it descends into the
   * `AdaptiveSparkPlanExec` wrapper that every real exec lives inside under the AQE-forced-on config.
   */
  protected def assertNativePlanContains(df: DataFrame, expectedExecs: String*): Unit = {
    df.collect()
    val plan = df.queryExecution.executedPlan
    val present = collect(plan) { case p => p.getClass.getSimpleName }.toSet
    val missing = expectedExecs.filterNot(present.contains)
    assert(
      missing.isEmpty,
      s"expected execs missing from plan: ${missing.mkString(", ")}\n" +
        s"present execs: ${present.mkString(", ")}\nfull plan:\n$plan")
  }

  /**
   * Run `query` against the Delta table at `tablePath` with the native scan engaged, assert the
   * executed plan contains a `CometDeltaNativeScanExec` (the read went native), and that the rows
   * match vanilla Spark's (order-independent).
   */
  protected def assertDeltaNativeMatches(
      tablePath: String,
      query: DataFrame => DataFrame): Unit = {
    val native = query(spark.read.format("delta").load(tablePath))
    // Materialise first so AQE runs its query-stage prep rules (Comet's CometScanRule fires lazily
    // when AQE materialises a stage); after collect, executedPlan reflects the finalized plan.
    val nativeRows = native.collect().toSeq.map(normalizeRow)
    val plan = native.queryExecution.executedPlan
    val deltaScans = collect(plan) { case s: CometDeltaNativeScanExec => s }
    assert(deltaScans.nonEmpty, s"expected CometDeltaNativeScanExec in plan, got:\n$plan")

    withSQLConf("spark.comet.scan.deltaNative.enabled" -> "false") {
      val vanillaRows = query(spark.read.format("delta").load(tablePath))
        .collect()
        .toSeq
        .map(normalizeRow)
      assert(
        nativeRows.sortBy(_.mkString("|")) == vanillaRows.sortBy(_.mkString("|")),
        s"native result did not match vanilla Spark result\nnative=$nativeRows\nvanilla=$vanillaRows")
    }
  }

  /**
   * Like `assertDeltaNativeMatches` but asserts the native plan SHOULD fall back: no
   * `CometDeltaNativeScanExec` appears (the read ran on vanilla Spark).
   */
  protected def assertDeltaFallback(
      tablePath: String,
      query: DataFrame => DataFrame): Unit = {
    val attempt = query(spark.read.format("delta").load(tablePath))
    attempt.collect()
    val plan = attempt.queryExecution.executedPlan
    val deltaScans = collect(plan) { case s: CometDeltaNativeScanExec => s }
    assert(
      deltaScans.isEmpty,
      s"expected fallback (no CometDeltaNativeScanExec) but plan was:\n$plan")
  }

  /**
   * Assert the native kernel-read path engaged: the plan carries a `CometDeltaNativeScanExec` rather
   * than falling back to vanilla Spark.
   */
  protected def assertKernelReadEngaged(tablePath: String): Unit = {
    val df = spark.read.format("delta").load(tablePath)
    df.collect() // materialize so AQE / Comet rules finalize the plan
    val plan = df.queryExecution.executedPlan
    val scans = collect(plan) { case s: CometDeltaNativeScanExec => s }
    assert(scans.nonEmpty, s"expected CometDeltaNativeScanExec in plan, got:\n$plan")
  }

  protected def normalizeRow(row: Row): Seq[Any] =
    row.toSeq.map(normalizeValue)

  protected def normalizeValue(v: Any): Any = v match {
    case null => null
    case arr: Array[_] => arr.toList.map(normalizeValue)
    case seq: scala.collection.Seq[_] => seq.toList.map(normalizeValue)
    case m: scala.collection.Map[_, _] =>
      m.toList
        .map { case (k, vv) => (normalizeValue(k), normalizeValue(vv)) }
        .sortBy(_._1.toString)
    case r: Row => normalizeRow(r).toList
    case other => other
  }

  protected def deleteRecursively(file: java.io.File): Unit = {
    if (file.isDirectory) {
      Option(file.listFiles()).foreach(_.foreach(deleteRecursively))
    }
    file.delete()
  }
}
