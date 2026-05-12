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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.comet.{CometBroadcastHashJoinExec, CometHashJoinExec, CometSortMergeJoinExec}
import org.apache.spark.sql.execution.LocalTableScanExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.types.StringType

import org.apache.comet.{CometConf, CometExplainInfo}
import org.apache.comet.serde.OperatorOuterClass

class CometCollationSuite extends CometTestBase {

  // Queries that group, sort, or shuffle on a non-default collated string must fall back to
  // Spark because Comet's shuffle/sort/aggregate compare raw bytes rather than collation-aware
  // keys. The shuffle-exchange rule is the primary line of defense (see #1947), so these tests
  // pin down the fallback reason it emits.
  private val hashShuffleCollationReason =
    "unsupported hash partitioning data type for columnar shuffle"
  private val rangeShuffleCollationReason =
    "unsupported range partitioning data type for columnar shuffle"
  private val joinKeyCollationReason =
    "unsupported non-default collated string join keys"

  test("listagg DISTINCT with utf8_lcase collation (issue #1947)") {
    checkSparkAnswerAndFallbackReason(
      "SELECT lower(listagg(DISTINCT c1 COLLATE utf8_lcase) " +
        "WITHIN GROUP (ORDER BY c1 COLLATE utf8_lcase)) " +
        "FROM (VALUES ('a'), ('B'), ('b'), ('A')) AS t(c1)",
      hashShuffleCollationReason)
  }

  test("DISTINCT on utf8_lcase collated string groups case-insensitively") {
    checkSparkAnswerAndFallbackReason(
      "SELECT DISTINCT c1 COLLATE utf8_lcase AS c " +
        "FROM (VALUES ('a'), ('A'), ('b'), ('B')) AS t(c1) ORDER BY c",
      hashShuffleCollationReason)
  }

  test("GROUP BY utf8_lcase collated string groups case-insensitively") {
    checkSparkAnswerAndFallbackReason(
      "SELECT lower(c1 COLLATE utf8_lcase) AS k, count(*) " +
        "FROM (VALUES ('a'), ('A'), ('b'), ('B')) AS t(c1) " +
        "GROUP BY c1 COLLATE utf8_lcase ORDER BY k",
      hashShuffleCollationReason)
  }

  test("ORDER BY utf8_lcase collated string sorts case-insensitively") {
    checkSparkAnswerAndFallbackReason(
      "SELECT c1 COLLATE utf8_lcase AS c " +
        "FROM (VALUES ('A'), ('b'), ('a'), ('B')) AS t(c1) ORDER BY c",
      rangeShuffleCollationReason)
  }

  test("default UTF8_BINARY string still runs through Comet") {
    // Sanity check that the collation fallback does not over-block the default string type.
    withParquetTable(Seq(("a", 1), ("b", 2), ("a", 3)), "tbl") {
      checkSparkAnswerAndOperator("SELECT DISTINCT _1 FROM tbl ORDER BY _1")
    }
  }

  // ---- Join collation guards (issue #4051) ----------------------------------------
  //
  // Comet's native join compares keys byte-by-byte, so 'a' and 'A' would not match
  // under utf8_lcase, producing wrong results. The converters must reject any join
  // whose keys carry a non-default collation.
  //
  // End-to-end SQL cannot reach the join converter today: higher-level guards
  // (CometScanRule, Collate-expression serialization, #4035 shuffle guard) short-circuit
  // first. The tests below bypass those guards by constructing physical-plan operators
  // directly and calling convert() — the contract is that convert() returns None for
  // collated keys.

  private def collatedKey(name: String): AttributeReference =
    AttributeReference(name, StringType("UTF8_LCASE"), nullable = false)()

  private def placeholderChildOp(): OperatorOuterClass.Operator =
    OperatorOuterClass.Operator.newBuilder().build()

  // Ensure converters are on so that None from convert() means the collation guard fired,
  // not that the join type is disabled.
  private def withJoinConvertersEnabled(f: => Unit): Unit =
    withSQLConf(
      CometConf.COMET_EXEC_HASH_JOIN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_BROADCAST_HASH_JOIN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_SORT_MERGE_JOIN_ENABLED.key -> "true") {
      f
    }

  private def assertFallbackReason(plan: SparkPlan, expectedReason: String): Unit = {
    val reasons = plan.getTagValue(CometExplainInfo.EXTENSION_INFO).getOrElse(Set.empty[String])
    assert(
      reasons.contains(expectedReason),
      s"Expected fallback reason '$expectedReason' on ${plan.nodeName}, got: $reasons")
  }

  test("CometBroadcastHashJoinExec rejects non-default collated join keys") {
    withJoinConvertersEnabled {
      val left = collatedKey("l")
      val right = collatedKey("r")
      val join = BroadcastHashJoinExec(
        leftKeys = Seq(left),
        rightKeys = Seq(right),
        joinType = Inner,
        buildSide = BuildRight,
        condition = None,
        left = LocalTableScanExec(Seq(left), Nil, None),
        right = LocalTableScanExec(Seq(right), Nil, None))

      val builder = OperatorOuterClass.Operator.newBuilder()
      val result =
        CometBroadcastHashJoinExec.convert(
          join,
          builder,
          placeholderChildOp(),
          placeholderChildOp())

      assert(
        result.isEmpty,
        "CometBroadcastHashJoinExec.convert must reject non-default collated join keys " +
          "(issue #4051): native byte equality cannot match values that compare equal " +
          "under utf8_lcase. Got a non-empty proto: " + result)
      assertFallbackReason(join, joinKeyCollationReason)
    }
  }

  test("CometHashJoinExec rejects non-default collated join keys") {
    withJoinConvertersEnabled {
      val left = collatedKey("l")
      val right = collatedKey("r")
      val join = ShuffledHashJoinExec(
        leftKeys = Seq(left),
        rightKeys = Seq(right),
        joinType = Inner,
        buildSide = BuildLeft,
        condition = None,
        left = LocalTableScanExec(Seq(left), Nil, None),
        right = LocalTableScanExec(Seq(right), Nil, None))

      val builder = OperatorOuterClass.Operator.newBuilder()
      val result =
        CometHashJoinExec.convert(join, builder, placeholderChildOp(), placeholderChildOp())

      assert(
        result.isEmpty,
        "CometHashJoinExec.convert must reject non-default collated join keys (issue " +
          "#4051): native byte equality cannot match values that compare equal under " +
          "utf8_lcase. Got a non-empty proto: " + result)
      assertFallbackReason(join, joinKeyCollationReason)
    }
  }

  test("CometBroadcastHashJoinExec still accepts default UTF8_BINARY string keys") {
    withJoinConvertersEnabled {
      val left = AttributeReference("l", StringType, nullable = false)()
      val right = AttributeReference("r", StringType, nullable = false)()
      val join = BroadcastHashJoinExec(
        leftKeys = Seq(left),
        rightKeys = Seq(right),
        joinType = Inner,
        buildSide = BuildRight,
        condition = None,
        left = LocalTableScanExec(Seq(left), Nil, None),
        right = LocalTableScanExec(Seq(right), Nil, None))

      val builder = OperatorOuterClass.Operator.newBuilder()
      val result =
        CometBroadcastHashJoinExec.convert(
          join,
          builder,
          placeholderChildOp(),
          placeholderChildOp())

      assert(
        result.isDefined,
        "CometBroadcastHashJoinExec.convert must continue to accept default UTF8_BINARY " +
          "string keys; the collation guard for #4051 must not over-block.")
    }
  }

  test("CometSortMergeJoinExec rejects non-default collated join keys") {
    withJoinConvertersEnabled {
      val left = collatedKey("l")
      val right = collatedKey("r")
      val join = SortMergeJoinExec(
        leftKeys = Seq(left),
        rightKeys = Seq(right),
        joinType = Inner,
        condition = None,
        left = LocalTableScanExec(Seq(left), Nil, None),
        right = LocalTableScanExec(Seq(right), Nil, None))

      val builder = OperatorOuterClass.Operator.newBuilder()
      val result =
        CometSortMergeJoinExec.convert(join, builder, placeholderChildOp(), placeholderChildOp())

      assert(
        result.isEmpty,
        "CometSortMergeJoinExec.convert must reject non-default collated join keys " +
          "(issue #4051): supportedSortMergeJoinEqualType must check collation. Got a " +
          "non-empty proto: " + result)
      assertFallbackReason(join, joinKeyCollationReason)
    }
  }

  test("CometSortMergeJoinExec still accepts default UTF8_BINARY string keys") {
    withJoinConvertersEnabled {
      val left = AttributeReference("l", StringType, nullable = false)()
      val right = AttributeReference("r", StringType, nullable = false)()
      val join = SortMergeJoinExec(
        leftKeys = Seq(left),
        rightKeys = Seq(right),
        joinType = Inner,
        condition = None,
        left = LocalTableScanExec(Seq(left), Nil, None),
        right = LocalTableScanExec(Seq(right), Nil, None))

      val builder = OperatorOuterClass.Operator.newBuilder()
      val result =
        CometSortMergeJoinExec.convert(join, builder, placeholderChildOp(), placeholderChildOp())

      assert(
        result.isDefined,
        "CometSortMergeJoinExec.convert must continue to accept default UTF8_BINARY " +
          "string keys; the collation guard for #4051 must not over-block.")
    }
  }
}
