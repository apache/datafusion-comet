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

package org.apache.spark.sql.comet

import scala.concurrent.duration.DurationInt

import org.apache.spark.sql.{CometTestBase, DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.util.Utils

import org.apache.comet.CometConf

/**
 * End-to-end lifecycle of the executor-side plan data held by [[PlanDataInjector]]. A map-only
 * stage parses its plan once into the base plan cache; a scan fused into a native shuffle map
 * stage prepares under the shuffle id instead. Spark's shuffle cleanup releases one shuffle's
 * entry, and stopping the SparkContext releases both stores, so a context recreated in the same
 * JVM starts empty even though its shuffle ids restart at zero.
 *
 * The recreated-context test stops the suite's SparkContext, so it runs last and nothing else may
 * follow it.
 */
class PlanDataInjectorShuffleLifecycleSuite extends CometTestBase {

  private def withNativeShuffle[T](session: SparkSession)(f: => T): T = {
    val keys = Seq(
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_MODE.key -> "native")
    val previous = keys.map { case (k, _) => k -> session.conf.getOption(k) }
    keys.foreach { case (k, v) => session.conf.set(k, v) }
    try f
    finally
      previous.foreach {
        case (k, Some(v)) => session.conf.set(k, v)
        case (k, None) => session.conf.unset(k)
      }
  }

  private def writeInput(session: SparkSession, path: String): Unit =
    session
      .range(0, 1000, 1, numPartitions = 4)
      .selectExpr("id AS _1", "CAST(id AS STRING) AS _2")
      .write
      .parquet(path)

  /**
   * Collects a native Parquet scan straight back, a map-only stage of four tasks, and returns the
   * DataFrame with the fingerprint of the base plan entry the stage added. The shuffle store is
   * checked for additions only: the ContextCleaner may remove an earlier test's shuffle at any
   * time.
   */
  private def runMapOnlyScan(session: SparkSession, path: String): (DataFrame, Long) = {
    writeInput(session, path)
    val before = PlanDataInjector.basePlanSnapshot
    val shufflesBefore = PlanDataInjector.preparedShuffleSnapshot.keySet
    val df = session.read.parquet(path)
    assert(df.collect().length == 1000)
    val added = PlanDataInjector.basePlanSnapshot -- before.keySet
    assert(added.size == 1, s"four tasks of one stage should add one base plan entry, saw $added")
    assert(added.values.head.nonEmpty, "the scan must have been prepared under the plan entry")
    val shufflesAdded = PlanDataInjector.preparedShuffleSnapshot.keySet -- shufflesBefore
    assert(
      shufflesAdded.isEmpty,
      s"a map-only stage must not touch the shuffle store: $shufflesAdded")
    (df, added.keys.head)
  }

  /** Runs a native shuffle fed directly by a native Parquet scan and returns its scan keys. */
  private def runScanFusedShuffle(session: SparkSession, path: String): Set[String] = {
    writeInput(session, path)
    withNativeShuffle(session) {
      val before = PlanDataInjector.preparedShuffleSnapshot
      val df = session.read.parquet(path).repartition(5, col("_1"))
      assert(df.count() == 1000)
      val added = PlanDataInjector.preparedShuffleSnapshot.filterNot { case (id, keys) =>
        before.get(id).contains(keys)
      }
      assert(added.size == 1, s"expected one new shuffle entry, saw $added")
      val keys = added.values.head
      assert(keys.nonEmpty, "the native scan must have been prepared under the shuffle's id")
      keys
    }
  }

  test("a map-only stage hits the base plan cache on every task after the first") {
    PlanDataInjector.releaseAll()
    withTempDir { dir =>
      val (df, fingerprint) =
        runMapOnlyScan(spark, new java.io.File(dir, "map-only.parquet").toString)
      // Re-executing the same plan ships the same bytes, so nothing new is parsed either.
      assert(df.collect().length == 1000)
      assert(
        PlanDataInjector.basePlanSnapshot.keySet == Set(fingerprint),
        "rerunning the stage must hit the cached entry, not add another")
    }
  }

  test("a scan fused into a native shuffle prepares under the shuffle id, not the base cache") {
    PlanDataInjector.releaseAll()
    withTempDir { dir =>
      val before = PlanDataInjector.basePlanSnapshot
      val keys = runScanFusedShuffle(spark, new java.io.File(dir, "fused.parquet").toString)
      assert(keys.nonEmpty)
      assert(
        PlanDataInjector.basePlanSnapshot == before,
        "the shuffle writer's per-task plan never enters the base plan cache")
    }
  }

  test("Spark's shuffle cleanup releases the shuffle's prepared scan data") {
    PlanDataInjector.releaseAll()
    withTempDir { dir =>
      val keys = runScanFusedShuffle(spark, new java.io.File(dir, "cleanup.parquet").toString)
      // The DataFrame is out of scope here; once the ShuffleDependency is collected, the
      // ContextCleaner asks every block manager to remove the shuffle, which reaches
      // CometShuffleManager.unregisterShuffle.
      eventually(timeout(30.seconds), interval(1.second)) {
        System.gc()
        val live = PlanDataInjector.preparedShuffleSnapshot.values.flatten.toSet
        assert(
          (live & keys).isEmpty,
          s"the collected shuffle's scan data should be gone, still holds ${live & keys}")
      }
    }
  }

  test("a recreated SparkContext starts from empty plan data stores") {
    PlanDataInjector.releaseAll()
    // Not withTempDir: its task-drain check needs the suite's context, which this test stops.
    val dir = Utils.createTempDir()
    try {
      val firstKeys =
        runScanFusedShuffle(spark, new java.io.File(dir, "first-context.parquet").toString)
      assert(PlanDataInjector.preparedShuffleSnapshot.values.flatten.toSet == firstKeys)
      val (_, firstPlan) =
        runMapOnlyScan(spark, new java.io.File(dir, "first-context-map.parquet").toString)
      assert(PlanDataInjector.basePlanSnapshot.contains(firstPlan))

      spark.stop()
      assert(
        PlanDataInjector.preparedShuffleSnapshot.isEmpty,
        "stopping the context must release every shuffle's prepared data")
      assert(
        PlanDataInjector.basePlanSnapshot.isEmpty,
        "stopping the context must release the base plan cache as well")

      val second = createSparkSession
      try {
        val secondKeys =
          runScanFusedShuffle(second, new java.io.File(dir, "second-context.parquet").toString)
        val snapshot = PlanDataInjector.preparedShuffleSnapshot
        assert(snapshot.size == 1, s"the new context should own the only entry, saw $snapshot")
        assert(
          snapshot.values.head == secondKeys,
          "a shuffle id reused by the new context must not carry the old context's scans")
      } finally {
        second.stop()
      }
    } finally {
      Utils.deleteRecursively(dir)
    }
  }
}
