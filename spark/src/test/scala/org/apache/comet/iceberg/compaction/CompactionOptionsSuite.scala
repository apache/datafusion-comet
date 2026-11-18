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
 * Unit tests for the option parsing of a bin-pack `rewrite_data_files`. The defaults are
 * iceberg-java's, and anything Comet does not reproduce has to come back as a reason so that the
 * call runs Iceberg's procedure.
 */
class CompactionOptionsSuite extends AnyFunSuite {

  private val MB = 1024L * 1024

  private def parse(
      options: Map[String, String],
      properties: Map[String, String] = Map.empty,
      specId: Int = 3): Either[String, CompactionOptions] =
    CompactionOptions.parse(options, properties, specId)

  private def parsed(
      options: Map[String, String] = Map.empty,
      properties: Map[String, String] = Map.empty): CompactionOptions =
    parse(options, properties) match {
      case Right(o) => o
      case Left(reason) => fail(s"Expected the options to parse, got: $reason")
    }

  test("defaults follow iceberg-java") {
    val o = parsed()
    assert(o.targetFileSize == 512 * MB)
    assert(o.minFileSize == (512 * MB * 0.75).toLong)
    assert(o.maxFileSize == (512 * MB * 1.8).toLong)
    assert(o.minInputFiles == 5)
    assert(!o.rewriteAll)
    assert(o.maxGroupSize == 100L * 1024 * MB)
    assert(o.outputSpecId == 3)
    assert(o.deleteFileThreshold == Int.MaxValue)
    assert(o.deleteRatioThreshold == 0.3)
    assert(o.jobOrder == RewriteJobOrder.NoOrder)
    assert(o.maxFilesToRewrite.isEmpty)
    assert(o.maxConcurrentGroupRewrites == 5)
    assert(!o.partialProgress)
    assert(o.maxCommits == 10)
    assert(o.maxFailedCommits == 10)
    assert(o.useStartingSequenceNumber)
  }

  test("the target size defaults to the table property") {
    val o = parsed(properties = Map("write.target-file-size-bytes" -> (64 * MB).toString))
    assert(o.targetFileSize == 64 * MB)
    assert(o.minFileSize == (64 * MB * 0.75).toLong)
    assert(o.maxFileSize == (64 * MB * 1.8).toLong)
    // The option wins over the table.
    val explicit = parsed(
      Map("target-file-size-bytes" -> (32 * MB).toString),
      Map("write.target-file-size-bytes" -> (64 * MB).toString))
    assert(explicit.targetFileSize == 32 * MB)
  }

  test("the rolling size sits halfway between the target and the maximum") {
    val o = parsed(Map("target-file-size-bytes" -> "100"))
    assert(o.maxFileSize == 180L)
    assert(o.writeMaxFileSize == 140L)
  }

  test("every supported option is read") {
    val o = parsed(
      Map(
        "target-file-size-bytes" -> "1000",
        "min-file-size-bytes" -> "500",
        "max-file-size-bytes" -> "2000",
        "min-input-files" -> "2",
        "rewrite-all" -> "true",
        "max-file-group-size-bytes" -> "5000",
        "delete-file-threshold" -> "3",
        "delete-ratio-threshold" -> "0.5",
        "rewrite-job-order" -> "Bytes-Desc",
        "max-files-to-rewrite" -> "20",
        "max-concurrent-file-group-rewrites" -> "8",
        "partial-progress.enabled" -> "true",
        "partial-progress.max-commits" -> "4",
        "partial-progress.max-failed-commits" -> "2",
        "use-starting-sequence-number" -> "false",
        "remove-dangling-deletes" -> "false",
        "output-spec-id" -> "3"))
    assert(o.targetFileSize == 1000 && o.minFileSize == 500 && o.maxFileSize == 2000)
    assert(o.minInputFiles == 2 && o.rewriteAll && o.maxGroupSize == 5000)
    assert(o.deleteFileThreshold == 3 && o.deleteRatioThreshold == 0.5)
    assert(o.jobOrder == RewriteJobOrder.BytesDesc && o.maxFilesToRewrite.contains(20))
    assert(o.maxConcurrentGroupRewrites == 8)
    assert(o.partialProgress && o.maxCommits == 4 && o.maxFailedCommits == 2)
    assert(!o.useStartingSequenceNumber)
  }

  test("the failed commit limit defaults to the commit limit") {
    val o =
      parsed(Map("partial-progress.enabled" -> "true", "partial-progress.max-commits" -> "7"))
    assert(o.maxFailedCommits == 7)
  }

  test("booleans parse the way Boolean.parseBoolean does") {
    assert(parsed(Map("rewrite-all" -> "TRUE")).rewriteAll)
    assert(!parsed(Map("rewrite-all" -> "yes")).rewriteAll)
    assert(!parsed(Map("use-starting-sequence-number" -> "1")).useStartingSequenceNumber)
  }

  test("an option Comet does not reproduce is reported") {
    val reason = parse(Map("shuffle-partitions-per-file" -> "2", "compression-factor" -> "1"))
    assert(reason.isLeft)
    assert(reason.left.toOption.get.contains("compression-factor"))
    assert(reason.left.toOption.get.contains("shuffle-partitions-per-file"))
  }

  test("remove-dangling-deletes is only accepted when it is off") {
    assert(parse(Map("remove-dangling-deletes" -> "true")).isLeft)
    assert(parse(Map("remove-dangling-deletes" -> "false")).isRight)
  }

  test("an output spec other than the current one is reported") {
    assert(parse(Map("output-spec-id" -> "2")).isLeft)
  }

  test("values Iceberg rejects are reported instead of being corrected") {
    val bad = Seq(
      Map("target-file-size-bytes" -> "abc"),
      Map("target-file-size-bytes" -> "0"),
      Map("target-file-size-bytes" -> "100", "min-file-size-bytes" -> "100"),
      Map("target-file-size-bytes" -> "100", "max-file-size-bytes" -> "100"),
      Map("min-file-size-bytes" -> "-1"),
      Map("min-input-files" -> "0"),
      Map("max-file-group-size-bytes" -> "0"),
      Map("delete-file-threshold" -> "-1"),
      Map("delete-ratio-threshold" -> "0"),
      Map("delete-ratio-threshold" -> "1.5"),
      Map("rewrite-job-order" -> "sideways"),
      Map("max-files-to-rewrite" -> "0"),
      Map("max-concurrent-file-group-rewrites" -> "0"),
      Map("partial-progress.enabled" -> "true", "partial-progress.max-commits" -> "0"))
    bad.foreach { options =>
      assert(parse(options).isLeft, s"Expected $options to be reported")
    }
  }

  test("the commit limit is only checked when partial progress is on") {
    assert(parse(Map("partial-progress.max-commits" -> "0")).isRight)
  }
}
