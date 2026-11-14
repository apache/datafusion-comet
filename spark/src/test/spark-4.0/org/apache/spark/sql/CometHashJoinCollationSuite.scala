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
import org.apache.spark.sql.comet.{CometBroadcastHashJoinExec, CometHashJoinExec}
import org.apache.spark.sql.execution.{LocalTableScanExec, SparkPlan}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, ShuffledHashJoinExec}
import org.apache.spark.sql.types.StringType

import org.apache.comet.{CometConf, CometExplainInfo}
import org.apache.comet.serde.OperatorOuterClass

// Spark 4.1 and later normalize hash join keys with CollationKey before constructing the
// physical join, so its converters receive BinaryType keys instead of collated strings.
// These tests exercise the Spark 4.0 path, where the converters see the original collation.
class CometHashJoinCollationSuite extends CometTestBase {

  private val joinKeyCollationReason =
    "unsupported non-default collated string join keys"

  private def collatedKey(name: String): AttributeReference =
    AttributeReference(name, StringType("UTF8_LCASE"), nullable = false)()

  private def placeholderChildOp(): OperatorOuterClass.Operator =
    OperatorOuterClass.Operator.newBuilder().build()

  // Ensure converters are on so that None from convert() means the collation guard fired,
  // not that the join type is disabled.
  private def withJoinConvertersEnabled(f: => Unit): Unit =
    withSQLConf(
      CometConf.COMET_EXEC_HASH_JOIN_ENABLED.key -> "true",
      CometConf.COMET_EXEC_BROADCAST_HASH_JOIN_ENABLED.key -> "true") {
      f
    }

  private def assertFallbackReason(plan: SparkPlan, expectedReason: String): Unit = {
    val reasons = plan.getTagValue(CometExplainInfo.FALLBACK_REASONS).getOrElse(Set.empty[String])
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

}
