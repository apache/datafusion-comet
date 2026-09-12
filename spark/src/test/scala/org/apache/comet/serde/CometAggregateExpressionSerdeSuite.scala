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

package org.apache.comet.serde

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.catalyst.expressions.{Attribute, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average, BitAndAgg, BitOrAgg, BitXorAgg, BloomFilterAggregate, Count, HyperLogLogPlusPlus, Max, Min, Sum}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DecimalType

class CometAggregateExpressionSerdeSuite extends CometTestBase {

  test("opting into Spark partial consumption does not enable native partial production") {
    val reverseOnly = new CometAggregateExpressionSerde[Min] {
      override def supportsSparkPartialToNativeFinal(fn: Min): Boolean = true

      // This policy-only handler deliberately cannot serialize an aggregate.
      override def convert(
          aggExpr: AggregateExpression,
          expr: Min,
          inputs: Seq[Attribute],
          binding: Boolean,
          conf: SQLConf): Option[ExprOuterClass.AggExpr] = None
    }
    val fn = Min(Literal(1L))
    assert(reverseOnly.supportsSparkPartialToNativeFinal(fn))
    assert(!reverseOnly.supportsNativePartialToSparkFinal(fn))
  }

  test("existing native partial policies preserve safe opt-ins and COUNT AVG asymmetry") {
    val input = Literal(1L)
    assert(CometMin.supportsNativePartialToSparkFinal(Min(input)))
    assert(CometMax.supportsNativePartialToSparkFinal(Max(input)))
    assert(CometBitAndAgg.supportsNativePartialToSparkFinal(BitAndAgg(input)))
    assert(CometBitOrAgg.supportsNativePartialToSparkFinal(BitOrAgg(input)))
    assert(CometBitXOrAgg.supportsNativePartialToSparkFinal(BitXorAgg(input)))
    assert(
      CometBloomFilterAggregate.supportsNativePartialToSparkFinal(
        new BloomFilterAggregate(input)))
    assert(
      CometApproxCountDistinct.supportsNativePartialToSparkFinal(new HyperLogLogPlusPlus(input)))

    val count = Count(Seq(input))
    assert(CometCount.supportsNativePartialToSparkFinal(count))
    assert(!CometCount.supportsSparkPartialToNativeFinal(count))
    val avg = Average(input)
    assert(!CometAverage.supportsNativePartialToSparkFinal(avg))
    assert(CometAverage.supportsSparkPartialToNativeFinal(avg))
    val decimalAvg = Average(Literal.create(null, DecimalType(20, 2)))
    assert(!CometAverage.supportsNativePartialToSparkFinal(decimalAvg))
    assert(!CometAverage.supportsSparkPartialToNativeFinal(decimalAvg))
  }

  test("SUM policies retain decimal and TRY exclusions in both directions") {
    // Resolve through Spark so TRY_SUM uses the version's own Sum evaluation-mode constructor.
    for {
      ansi <- Seq("true", "false")
      dataType <- Seq("BIGINT", "DOUBLE", "DECIMAL(20, 2)")
      fn <- Seq("sum", "try_sum")
    } {
      withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi) {
        val sums =
          sql(s"SELECT $fn(CAST(id AS $dataType)) FROM range(1)").queryExecution.optimizedPlan
            .flatMap(_.expressions.flatMap(_.collect { case sum: Sum =>
              sum
            }))
        assert(sums.size == 1)
        val expected = fn == "sum" && !dataType.startsWith("DECIMAL")
        assert(CometSum.supportsNativePartialToSparkFinal(sums.head) == expected)
        assert(CometSum.supportsSparkPartialToNativeFinal(sums.head) == expected)
      }
    }
  }
}
