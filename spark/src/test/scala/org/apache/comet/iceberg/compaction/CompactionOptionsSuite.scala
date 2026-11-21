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
 * Unit tests for the options of a bin-pack `rewrite_data_files` that Comet acts on. The defaults
 * are those of iceberg-java's action, and anything Comet does not reproduce has to come back as a
 * reason so that the call runs Iceberg's procedure. Iceberg's planner reads and checks the other
 * options, so these tests stand in for its list of them.
 */
class CompactionOptionsSuite extends AnyFunSuite {

  private val plannerOptions = Set("target-file-size-bytes", "min-input-files", "rewrite-all")

  private def parse(options: Map[String, String]): Either[String, CompactionOptions] =
    CompactionOptions.parse(options, plannerOptions)

  private def parsed(options: Map[String, String] = Map.empty): CompactionOptions =
    parse(options) match {
      case Right(o) => o
      case Left(reason) => fail(s"Expected the options to parse, got: $reason")
    }

  test("defaults follow iceberg-java's action") {
    val o = parsed()
    assert(o.maxConcurrentGroupRewrites == 5)
    assert(!o.partialProgress)
    assert(o.maxCommits == 10)
    assert(o.maxFailedCommits == 10)
    assert(o.useStartingSequenceNumber)
    assert(!o.removeDanglingDeletes)
  }

  test("every option of the action is read") {
    val o = parsed(
      Map(
        "max-concurrent-file-group-rewrites" -> "8",
        "partial-progress.enabled" -> "true",
        "partial-progress.max-commits" -> "4",
        "partial-progress.max-failed-commits" -> "2",
        "use-starting-sequence-number" -> "false",
        "remove-dangling-deletes" -> "true",
        "output-spec-id" -> "3"))
    assert(o.maxConcurrentGroupRewrites == 8)
    assert(o.partialProgress && o.maxCommits == 4 && o.maxFailedCommits == 2)
    assert(!o.useStartingSequenceNumber)
    assert(o.removeDanglingDeletes)
  }

  test("the options of the planner are accepted and left to it") {
    assert(parse(Map("target-file-size-bytes" -> "1024", "rewrite-all" -> "true")).isRight)
  }

  test("the failed commit limit defaults to the commit limit") {
    val o =
      parsed(Map("partial-progress.enabled" -> "true", "partial-progress.max-commits" -> "7"))
    assert(o.maxFailedCommits == 7)
  }

  test("booleans parse the way Boolean.parseBoolean does") {
    assert(parsed(Map("partial-progress.enabled" -> "TRUE")).partialProgress)
    assert(!parsed(Map("remove-dangling-deletes" -> "yes")).removeDanglingDeletes)
    assert(!parsed(Map("use-starting-sequence-number" -> "1")).useStartingSequenceNumber)
  }

  test("an option neither the action nor the planner reads is reported") {
    // `cache-delete-files` is an option of Iceberg 1.12's action that tunes Iceberg's own jobs.
    val reason = parse(
      Map(
        "shuffle-partitions-per-file" -> "2",
        "compression-factor" -> "1",
        "cache-delete-files" -> "true"))
    assert(reason.isLeft)
    Seq("cache-delete-files", "compression-factor", "shuffle-partitions-per-file").foreach {
      option => assert(reason.left.toOption.get.contains(option))
    }
  }

  test("values Iceberg's action rejects are reported instead of being corrected") {
    val bad = Seq(
      Map("max-concurrent-file-group-rewrites" -> "0"),
      Map("max-concurrent-file-group-rewrites" -> "abc"),
      Map("partial-progress.max-commits" -> "x"),
      Map("partial-progress.enabled" -> "true", "partial-progress.max-commits" -> "0"))
    bad.foreach { options =>
      assert(parse(options).isLeft, s"Expected $options to be reported")
    }
  }

  test("the commit limit is only checked when partial progress is on") {
    assert(parse(Map("partial-progress.max-commits" -> "0")).isRight)
  }
}
