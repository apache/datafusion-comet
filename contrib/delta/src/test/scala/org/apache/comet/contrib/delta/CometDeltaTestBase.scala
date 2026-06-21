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
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

import org.apache.comet.CometSparkSessionExtensions

/**
 * Base trait for unit-testing the contrib-delta JVM layer.
 *
 * Wires up Spark+Delta in local mode with the contrib enabled (Comet + Delta session
 * extensions, AQE forced on so Comet's query-stage-prep rules fire) and provides the
 * claim/decline assertions this unit needs: `assertMarkerPlanted` / `assertNoMarker`
 * (did `DeltaScanRule` claim the scan?) and `assertResultsMatchVanilla` (does the
 * marker's fallback / a decline stay result-correct?). The native-read assertions
 * (`assertDeltaNativeMatches` etc.) land with the serde/exec unit, since they need
 * `CometDeltaNativeScanExec`.
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
   * Assert that `DeltaScanRule` CLAIMED the scan: the executed plan contains a `CometDeltaScanMarker`.
   * On a build without the serde (this unit) the marker does not yet mix in `CometContribScanMarker`,
   * so `CometExecRule` matches nothing, leaves it in the plan, and it executes as a vanilla Delta
   * fallback -- which is exactly the claim signal we assert here. (On the A.2 build, where no
   * `CometScanContrib` service is registered at all, no marker is planted, so this is red there /
   * green here.)
   */
  protected def assertMarkerPlanted(df: DataFrame): Unit = {
    val markers = markersIn(df)
    assert(
      markers.nonEmpty,
      s"expected a CometDeltaScanMarker in the plan, got:\n${df.queryExecution.executedPlan}")
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
   * Assert the Delta read at `tablePath` returns the same rows whether the native claim path is on
   * or off -- i.e. the marker's vanilla fallback (and any decline) is result-correct. Order-independent.
   */
  protected def assertResultsMatchVanilla(
      tablePath: String,
      query: DataFrame => DataFrame): Unit = {
    val withClaim = query(spark.read.format("delta").load(tablePath))
      .collect()
      .toSeq
      .map(normalizeRow)
    withSQLConf("spark.comet.scan.deltaNative.enabled" -> "false") {
      val vanilla = query(spark.read.format("delta").load(tablePath))
        .collect()
        .toSeq
        .map(normalizeRow)
      assert(
        withClaim.sortBy(_.mkString("|")) == vanilla.sortBy(_.mkString("|")),
        s"claim-path result did not match vanilla Spark result\n" +
          s"withClaim=$withClaim\nvanilla=$vanilla")
    }
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
