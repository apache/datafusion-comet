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

package org.apache.comet.rules

import org.apache.spark.sql.CometTestBase

import org.apache.comet.CometConf

/**
 * Unit tests for the SortMergeJoin -> ShuffledHashJoin rewrite rule's build-size-budget
 * computation. End-to-end rewrite behavior is covered by CometJoinSuite and CometExecSuite.
 */
class RewriteJoinSuite extends CometTestBase {

  test("computeMaxBuildSize returns None when maxBuildSize=-1") {
    withSQLConf(CometConf.COMET_REPLACE_SMJ_MAX_BUILD_SIZE.key -> "-1") {
      assert(RewriteJoin.computeMaxBuildSize().isEmpty)
    }
  }

  test("computeMaxBuildSize uses explicit positive value directly") {
    withSQLConf(CometConf.COMET_REPLACE_SMJ_MAX_BUILD_SIZE.key -> "12345") {
      assert(RewriteJoin.computeMaxBuildSize().contains(12345L))
    }
  }

  test("computeMaxBuildSize returns a positive derived value when maxBuildSize=0") {
    withSQLConf(
      CometConf.COMET_REPLACE_SMJ_MAX_BUILD_SIZE.key -> "0",
      CometConf.COMET_REPLACE_SMJ_MEMORY_FRACTION.key -> "0.25",
      CometConf.COMET_REPLACE_SMJ_HASH_TABLE_OVERHEAD.key -> "3.0") {
      val budget = RewriteJoin.computeMaxBuildSize()
      assert(budget.isDefined, "auto-derived budget should be defined")
      assert(budget.get > 0L, s"derived budget should be positive, got ${budget.get}")
    }
  }

  test("derived budget scales with memoryFraction") {
    def budgetWith(fraction: String): Long = {
      var result = 0L
      withSQLConf(
        CometConf.COMET_REPLACE_SMJ_MAX_BUILD_SIZE.key -> "0",
        CometConf.COMET_REPLACE_SMJ_MEMORY_FRACTION.key -> fraction,
        CometConf.COMET_REPLACE_SMJ_HASH_TABLE_OVERHEAD.key -> "3.0") {
        result = RewriteJoin.computeMaxBuildSize().get
      }
      result
    }
    val small = budgetWith("0.1")
    val large = budgetWith("0.5")
    assert(large >= small, s"larger fraction should yield larger budget ($small vs $large)")
  }

  test("derived budget scales inversely with hashTableOverhead") {
    def budgetWith(overhead: String): Long = {
      var result = 0L
      withSQLConf(
        CometConf.COMET_REPLACE_SMJ_MAX_BUILD_SIZE.key -> "0",
        CometConf.COMET_REPLACE_SMJ_MEMORY_FRACTION.key -> "0.25",
        CometConf.COMET_REPLACE_SMJ_HASH_TABLE_OVERHEAD.key -> overhead) {
        result = RewriteJoin.computeMaxBuildSize().get
      }
      result
    }
    val low = budgetWith("2.0")
    val high = budgetWith("6.0")
    assert(low >= high, s"lower overhead should yield larger budget ($low vs $high)")
  }
}
