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

package org.apache.comet.exec

import java.io.File
import java.nio.file.{Files, Paths, StandardCopyOption}

import org.scalatest.funsuite.AnyFunSuite

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession

/**
 * The native Parquet scan shares a footer cache between the tasks of a stage in a registry that
 * lives as long as the JVM. Stage ids restart at zero in every SparkContext, so a context started
 * after another one stopped must not be handed the footers the old one left in it.
 *
 * Runs its own contexts, which is why it is not part of a suite that shares a session.
 */
class CometSharedMetadataCacheRestartSuite extends AnyFunSuite {

  private def newSession(): SparkSession =
    SparkSession
      .builder()
      .master("local[2]")
      .config("spark.plugins", "org.apache.spark.CometPlugin")
      .config(
        "spark.shuffle.manager",
        "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager")
      .config("spark.sql.extensions", "org.apache.comet.CometSparkSessionExtensions")
      .config("spark.comet.enabled", "true")
      .config("spark.comet.exec.enabled", "true")
      .config("spark.comet.parquet.metadataCache.sharedWithinStage.enabled", "true")
      .config("spark.memory.offHeap.enabled", "true")
      .config("spark.memory.offHeap.size", "256m")
      // Equal sizes for different values need an encoding whose size does not follow the values.
      .config("spark.sql.parquet.compression.codec", "uncompressed")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

  /**
   * Writes the ids in `[from, until)` as a single file named `part.parquet` in `table`, replacing
   * the file already there, then counts the rows with an id of at least 1000 in a fresh context.
   * The jobs run are the same ones each time, so the scan has the same stage id in every context.
   */
  private def countFrom1000(scratch: File, table: File, from: Long, until: Long): (Long, Long) = {
    val spark = newSession()
    try {
      val written = new File(scratch, s"written-$from")
      spark.range(from, until).coalesce(1).write.parquet(written.getAbsolutePath)
      val source = written
        .listFiles()
        .filter(_.getName.endsWith(".parquet"))
        .head
        .toPath
      Files.copy(
        source,
        new File(table, "part.parquet").toPath,
        StandardCopyOption.REPLACE_EXISTING)

      val df = spark.read.parquet(table.getAbsolutePath).where("id >= 1000")
      val count = df.count()
      val plan = df.queryExecution.executedPlan.toString
      require(plan.contains("CometNativeScan"), s"Expected a native scan but got:\n$plan")
      (count, new File(table, "part.parquet").length())
    } finally {
      spark.stop()
    }
  }

  test("a restarted SparkContext does not read the footers of the previous one") {
    assume(SparkSession.getDefaultSession.isEmpty, "another SparkSession is running in this JVM")
    // java.io.tmpdir is pinned to target/tmp by the pom and may not exist yet
    val tmpDir = Files.createDirectories(Paths.get(System.getProperty("java.io.tmpdir")))
    val scratch = Files.createTempDirectory(tmpDir, "comet-footer-restart").toFile
    try {
      val table = new File(scratch, "table")
      assert(table.mkdirs())

      // The first file holds ids 0 to 999, so its footer says nothing is at least 1000.
      val (before, sizeBefore) = countFrom1000(scratch, table, 0, 1000)
      assert(before == 0)

      // The second holds ids 1000 to 1999 at the same path with the same size. Its footer is
      // the one the scan must read, not the cached one that would prune every row.
      val (after, sizeAfter) = countFrom1000(scratch, table, 1000, 2000)
      assert(sizeAfter == sizeBefore, "the replacement must have the size of the original")
      assert(after == 1000)
    } finally {
      FileUtils.deleteDirectory(scratch)
    }
  }
}
