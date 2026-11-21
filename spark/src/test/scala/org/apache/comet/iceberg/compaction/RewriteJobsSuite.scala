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

import org.scalatest.funsuite.AnyFunSuite

/**
 * Unit tests for how the file groups of a rewrite are split into Spark jobs. There is no Iceberg
 * dependency, so these run on every Spark profile.
 */
class RewriteJobsSuite extends AnyFunSuite {

  test("a commit takes the groups of the plan over the commits allowed, rounded up") {
    assert(RewriteJobs.groupsPerCommit(totalGroupCount = 10, maxCommits = 3) == 4)
    assert(RewriteJobs.groupsPerCommit(totalGroupCount = 9, maxCommits = 3) == 3)
    // More commits than groups is a group per commit.
    assert(RewriteJobs.groupsPerCommit(totalGroupCount = 3, maxCommits = 20) == 1)
  }

  test("a job takes at most the number of groups asked for") {
    val jobs = RewriteJobs.chunk(Seq("a", "b", "c", "d", "e"), 2, Int.MaxValue)(_ => 1)
    assert(jobs == Seq(Seq("a", "b"), Seq("c", "d"), Seq("e")))
  }

  test("a job stops before it passes the bin limit") {
    val jobs = RewriteJobs.chunk(Seq("a" -> 6, "b" -> 4, "c" -> 1, "d" -> 9), 10, 10)(_._2)
    assert(jobs == Seq(Seq("a" -> 6, "b" -> 4), Seq("c" -> 1, "d" -> 9)))
  }

  test("a group over the bin limit gets a job of its own") {
    val jobs = RewriteJobs.chunk(Seq("a" -> 1, "huge" -> 50, "b" -> 1), 10, 10)(_._2)
    assert(jobs == Seq(Seq("a" -> 1), Seq("huge" -> 50), Seq("b" -> 1)))
  }

  test("without limits all the groups are one job, and none is no job") {
    assert(
      RewriteJobs.chunk(Seq(1, 2, 3), Int.MaxValue, Int.MaxValue)(_ => 1) == Seq(Seq(1, 2, 3)))
    assert(RewriteJobs.chunk(Seq.empty[Int], 2, 10)(_ => 1).isEmpty)
  }
}
