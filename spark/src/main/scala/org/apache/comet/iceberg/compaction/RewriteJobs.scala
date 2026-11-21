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

package org.apache.comet.iceberg.compaction

import scala.collection.mutable

/** How the file groups of a rewrite are split into the Spark jobs that rewrite them. */
private[compaction] object RewriteJobs {

  /**
   * The number of groups a commit of `partial-progress.enabled` takes, as iceberg-java's action
   * counts it: the groups the plan had before `max-files-to-rewrite`, over the commits allowed.
   */
  def groupsPerCommit(totalGroupCount: Int, maxCommits: Int): Int =
    math.max(1, (totalGroupCount + maxCommits - 1) / maxCommits)

  /**
   * Splits the groups of a rewrite into the jobs that run them, in order. A job takes groups
   * until it has `maxGroups` of them, or until the next one would take it past `maxWeight`, which
   * is counted in bins. A group heavier than `maxWeight` still gets a job of its own.
   */
  def chunk[T](entries: Seq[T], maxGroups: Int, maxWeight: Int)(
      weightOf: T => Int): Seq[Seq[T]] = {
    val chunks = mutable.ArrayBuffer.empty[Seq[T]]
    var current = mutable.ArrayBuffer.empty[T]
    var weight = 0
    entries.foreach { entry =>
      val entryWeight = weightOf(entry)
      val full = current.size >= maxGroups || weight.toLong + entryWeight > maxWeight
      if (current.nonEmpty && full) {
        chunks.append(current.toVector)
        current = mutable.ArrayBuffer.empty[T]
        weight = 0
      }
      current.append(entry)
      weight += entryWeight
    }
    if (current.nonEmpty) {
      chunks.append(current.toVector)
    }
    chunks.toSeq
  }
}
