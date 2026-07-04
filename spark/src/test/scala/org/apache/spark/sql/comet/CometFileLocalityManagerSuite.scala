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

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

/**
 * Unit tests for [[CometFileLocalityManager]] cache-affinity semantics (section 2.10). Pure
 * driver logic - no SparkContext required.
 */
class CometFileLocalityManagerSuite extends AnyFunSuite with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
    super.beforeEach()
    CometFileLocalityManager.clear()
  }

  private def files(n: Int): Seq[String] = (0 until n).map(i => s"s3://bucket/f$i.parquet")

  test("assignments are sticky across calls") {
    val fs = files(6)
    CometFileLocalityManager.assignFilesForQuery(fs, Seq("h1", "h2", "h3"))
    val first = fs.map(f => CometFileLocalityManager.assignedHost(f))
    // Same hosts still available -> every file keeps its owner.
    CometFileLocalityManager.assignFilesForQuery(fs, Seq("h1", "h2", "h3"))
    val second = fs.map(f => CometFileLocalityManager.assignedHost(f))
    assert(first == second)
    assert(first.forall(_.isDefined))
  }

  test("new files are assigned least-loaded per query") {
    val hosts = Seq("h1", "h2", "h3")
    CometFileLocalityManager.assignFilesForQuery(files(9), hosts)
    // 9 files over 3 hosts -> perfectly balanced (3 each).
    val counts = files(9)
      .flatMap(f => CometFileLocalityManager.assignedHost(f))
      .groupBy(identity)
      .view
      .mapValues(_.size)
      .toMap
    assert(counts.values.forall(_ == 3), s"expected 3 files per host, got $counts")
  }

  test("lost host reassigns its files, survivors untouched") {
    val fs = files(6)
    CometFileLocalityManager.assignFilesForQuery(fs, Seq("h1", "h2", "h3"))
    val before = fs.map(f => CometFileLocalityManager.assignedHost(f).get)

    // h3 disappears. Files owned by h3 must move; files on h1/h2 must stay.
    CometFileLocalityManager.assignFilesForQuery(fs, Seq("h1", "h2"))
    val after = fs.map(f => CometFileLocalityManager.assignedHost(f).get)

    for ((f, i) <- fs.zipWithIndex) {
      if (before(i) == "h3") {
        assert(after(i) != "h3", s"$f should have been reassigned off the lost host")
        assert(Set("h1", "h2").contains(after(i)))
      } else {
        assert(after(i) == before(i), s"$f on a surviving host should not move")
      }
    }
    assert(!after.contains("h3"))
  }

  test("new host triggers fair-share rebalance moving only the excess") {
    val fs = files(6)
    // Start with two hosts: 3 files each.
    CometFileLocalityManager.assignFilesForQuery(fs, Seq("h1", "h2"))
    assert(hostCounts(fs).values.forall(_ == 3))

    // A third host appears. fairShare = ceil(6/3) = 2. Each old host should shed 1 file to
    // the newcomer; the newcomer ends with 2.
    CometFileLocalityManager.assignFilesForQuery(fs, Seq("h1", "h2", "h3"))
    val counts = hostCounts(fs)
    assert(counts.getOrElse("h3", 0) == 2, s"newcomer should reach fair share: $counts")
    assert(counts.getOrElse("h1", 0) == 2, s"$counts")
    assert(counts.getOrElse("h2", 0) == 2, s"$counts")
  }

  test("preferredHostsForPartition orders hosts by majority ownership") {
    val fs = files(3)
    // Force a known layout: f0,f1 -> hA ; f2 -> hB by assigning on a single host first then
    // reassigning is awkward; instead assign across two hosts and read back the mapping.
    CometFileLocalityManager.assignFilesForQuery(fs, Seq("hA", "hB"))
    val prefs = CometFileLocalityManager.preferredHostsForPartition(fs)
    // The most-owning host for this partition must come first.
    val counts = hostCounts(fs)
    val expectedFirst = counts.maxBy(_._2)._1
    assert(prefs.headOption.contains(expectedFirst))
    assert(prefs.toSet == counts.keySet)
  }

  test("preferredHostsForPartition returns Nil for unassigned files") {
    assert(CometFileLocalityManager.preferredHostsForPartition(files(2)).isEmpty)
  }

  test("prune drops only unreferenced paths") {
    val fs = files(4)
    CometFileLocalityManager.assignFilesForQuery(fs, Seq("h1", "h2"))
    assert(CometFileLocalityManager.size == 4)
    CometFileLocalityManager.pruneStaleAssignments(Set(fs(0), fs(1)))
    assert(CometFileLocalityManager.size == 2)
    assert(CometFileLocalityManager.assignedHost(fs(0)).isDefined)
    assert(CometFileLocalityManager.assignedHost(fs(2)).isEmpty)
  }

  test("empty inputs are no-ops") {
    CometFileLocalityManager.assignFilesForQuery(Seq.empty, Seq("h1"))
    CometFileLocalityManager.assignFilesForQuery(files(1), Seq.empty)
    assert(CometFileLocalityManager.size == 0)
  }

  private def hostCounts(fs: Seq[String]): Map[String, Int] =
    fs.flatMap(f => CometFileLocalityManager.assignedHost(f))
      .groupBy(identity)
      .view
      .mapValues(_.size)
      .toMap
}
