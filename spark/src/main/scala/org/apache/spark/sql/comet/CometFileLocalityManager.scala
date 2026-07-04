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

import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging

/**
 * Driver-side cache-affinity scheduler for the object-store data cache
 * (OBJECT_STORE_CACHE_DESIGN.md section 2.10).
 *
 * Without scheduler affinity a private per-executor cache is ~`1/K` effective at K executors,
 * because Spark schedules S3-backed scan tasks with no locality. This manager assigns each
 * Parquet file a sticky owner host and exposes it through `RDD.getPreferredLocations`, so repeat
 * reads of a file route back to the executor that already cached it - turning K private caches
 * into one coherent cluster cache.
 *
 * The driver *decides* placement rather than *observing* it (assignment-follows-scheduling):
 * there is no executor->driver reporting channel and no broadcast; executor caches simply fill
 * where tasks run. Modeled on `indextables_spark`'s `DriverSplitLocalityManager`.
 *
 * Preferred locations are hints: if the owner host is busy past `spark.locality.wait` the task
 * runs elsewhere, reads through that host's cache, and is still correct (worst case a cache
 * miss). Locality can never fail or stall a query.
 */
object CometFileLocalityManager extends Logging {

  /** Driver-lifetime sticky assignment: file path -> owner host. */
  private val assignments = new ConcurrentHashMap[String, String]()

  /** Hosts observed on a previous query, to detect newly-added executors for rebalance. */
  @volatile private var knownHosts: Set[String] = Set.empty

  /** Guards the multi-step assign/rebalance so concurrent queries don't interleave it. */
  private val lock = new Object

  /**
   * Assign a sticky owner host to each file referenced by a query, balancing this query's work
   * across `availableHosts`.
   *
   *   1. Files whose assigned host is still available keep it (sticky locality). 2. Unassigned
   *      files, and files orphaned by a lost host, go to the host with the fewest files *in this
   *      query* (least-loaded, balancing current work not history). 3. When new executors have
   *      appeared since the last query, a fair-share rebalance moves just enough files from the
   *      most-overloaded hosts to bring newcomers up to `ceil(filesInQuery / hosts)`.
   */
  def assignFilesForQuery(paths: Seq[String], availableHosts: Seq[String]): Unit = {
    if (paths.isEmpty || availableHosts.isEmpty) {
      return
    }
    lock.synchronized {
      val hosts = availableHosts.distinct
      val hostSet = hosts.toSet
      val hadNewHosts = hostSet.exists(h => !knownHosts.contains(h))
      knownHosts = hostSet

      // Per-query load: how many of THIS query's files each host owns.
      val queryLoad = mutable.HashMap[String, Int]()
      hosts.foreach(h => queryLoad(h) = 0)

      // Step 1: keep valid existing assignments; collect files needing (re)assignment.
      val needsAssignment = mutable.ArrayBuffer[String]()
      for (p <- paths.distinct) {
        val cur = assignments.get(p)
        if (cur != null && hostSet.contains(cur)) {
          queryLoad(cur) += 1
        } else {
          needsAssignment += p
        }
      }

      // Step 2: assign the rest to the least-loaded host (by current query load).
      for (p <- needsAssignment) {
        val host = leastLoaded(hosts, queryLoad)
        assignments.put(p, host)
        queryLoad(host) += 1
      }

      // Step 3: only rebalance to newcomers when the host set actually grew.
      if (hadNewHosts) {
        rebalanceForNewHosts(paths.distinct, hosts, queryLoad)
      }
    }
  }

  /**
   * Preferred hosts for a partition, most-preferred first. A partition whose files map to several
   * hosts returns them ordered by how many of the partition's files each host owns (majority
   * preference). Spark treats the result as an ordered preference list.
   */
  def preferredHostsForPartition(filePaths: Seq[String]): Seq[String] = {
    if (filePaths.isEmpty) {
      return Nil
    }
    val counts = mutable.HashMap[String, Int]()
    for (p <- filePaths) {
      val h = assignments.get(p)
      if (h != null) {
        counts(h) = counts.getOrElse(h, 0) + 1
      }
    }
    counts.toSeq.sortBy { case (_, c) => -c }.map { case (host, _) => host }
  }

  /**
   * Available executor hosts: `SparkContext.getExecutorMemoryStatus` minus the driver, with a
   * localhost fallback for local mode / before any executor has registered.
   */
  def getAvailableHosts(sc: SparkContext): Seq[String] = {
    val driverHost = sc.getConf.get("spark.driver.host", "localhost")
    val hosts = sc.getExecutorMemoryStatus.keys.iterator
      .map(blockManagerId => blockManagerId.split(":").head)
      .filter(_ != driverHost)
      .toSeq
      .distinct
    if (hosts.nonEmpty) hosts else Seq("localhost")
  }

  /**
   * Drop assignments for files no longer referenced. Pure hygiene to bound the map (`O(distinct
   * files seen)`); not required for correctness.
   */
  def pruneStaleAssignments(referencedPaths: Set[String]): Unit = {
    val it = assignments.keySet.iterator
    while (it.hasNext) {
      val path = it.next()
      if (!referencedPaths.contains(path)) {
        it.remove()
      }
    }
  }

  /** Current assignment for a file, if any. Exposed for tests. */
  private[comet] def assignedHost(path: String): Option[String] =
    Option(assignments.get(path))

  /** Number of live assignments. Exposed for tests. */
  private[comet] def size: Int = assignments.size

  /** Reset all state. Exposed for tests. */
  private[comet] def clear(): Unit = lock.synchronized {
    assignments.clear()
    knownHosts = Set.empty
  }

  private def leastLoaded(hosts: Seq[String], load: mutable.HashMap[String, Int]): String =
    hosts.minBy(h => load.getOrElse(h, 0))

  /**
   * Move files from over-fair-share hosts to under-fair-share hosts (the newcomers) until every
   * under-loaded host reaches fair share or no overloaded host has spare files. Only this query's
   * files move; untouched assignments keep their locality.
   */
  private def rebalanceForNewHosts(
      paths: Seq[String],
      hosts: Seq[String],
      queryLoad: mutable.HashMap[String, Int]): Unit = {
    val fairShare = math.ceil(paths.size.toDouble / hosts.size).toInt.max(1)

    // Files of this query grouped by their (already-assigned) owner host.
    val byHost = mutable.HashMap[String, mutable.ArrayBuffer[String]]()
    hosts.foreach(h => byHost(h) = mutable.ArrayBuffer[String]())
    for (p <- paths) {
      val h = assignments.get(p)
      if (h != null && byHost.contains(h)) {
        byHost(h) += p
      }
    }

    val underloaded = mutable.Queue[String]()
    hosts.foreach(h => if (byHost(h).size < fairShare) underloaded.enqueue(h))

    for (host <- hosts) {
      while (byHost(host).size > fairShare && underloaded.nonEmpty) {
        val target = underloaded.head
        if (byHost(target).size >= fairShare) {
          underloaded.dequeue()
        } else {
          val moved = byHost(host).remove(byHost(host).size - 1)
          assignments.put(moved, target)
          byHost(target) += moved
          queryLoad(host) = (queryLoad.getOrElse(host, 1) - 1).max(0)
          queryLoad(target) = queryLoad.getOrElse(target, 0) + 1
          if (byHost(target).size >= fairShare) {
            underloaded.dequeue()
          }
        }
      }
    }
  }
}
