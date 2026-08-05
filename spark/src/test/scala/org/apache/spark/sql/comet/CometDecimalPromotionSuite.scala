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

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.catalyst.expressions.{ArrayContains, AttributeReference, Divide, EvalMode}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DecimalType, IntegerType}

import org.apache.comet.serde.{CometDivide, ExprOuterClass, QueryPlanSerde, Unsupported}

class CometDecimalPromotionSuite extends CometTestBase {

  private case class Operation(
      name: String,
      symbol: String,
      hasProto: ExprOuterClass.Expr => Boolean)

  private case class TestMode(name: String, ansiEnabled: Boolean, failOnError: Boolean)

  test("issue #5190: decimal promotion is idempotent during recursive serialization") {
    val left = "CAST(id AS DECIMAL(10, 0))"
    val right = "CAST(id + 1 AS DECIMAL(10, 0))"
    val divide = Operation(name = "divide", symbol = "/", hasProto = _.hasDivide)
    val operations = Seq(
      Operation(name = "add", symbol = "+", hasProto = _.hasAdd),
      Operation(name = "subtract", symbol = "-", hasProto = _.hasSubtract),
      Operation(name = "multiply", symbol = "*", hasProto = _.hasMultiply),
      divide,
      Operation(name = "remainder", symbol = "%", hasProto = _.hasRemainder))
    val legacy = TestMode(name = "LEGACY", ansiEnabled = false, failOnError = false)
    val ansi = TestMode(name = "ANSI", ansiEnabled = true, failOnError = true)

    def check(operation: Operation, mode: TestMode, arithmetic: String): Unit = {
      val name = s"${operation.name} ${mode.name}"
      withSQLConf(SQLConf.ANSI_ENABLED.key -> mode.ansiEnabled.toString) {
        val plan = spark
          .sql(s"SELECT array_contains(array($arithmetic), $arithmetic) FROM range(1, 4)")
          .queryExecution
          .optimizedPlan
        val expression = plan.expressions.head
        val arrayContains = expression.collectFirst { case e: ArrayContains => e }.get
        val promoted = DecimalPrecision.promote(expression)
        assert(
          DecimalPrecision.promote(promoted) == promoted,
          s"$name promotion is not idempotent: $promoted")

        // This proto-shape check relies on CometArrayContains re-entering exprToProto for its
        // children. If https://github.com/apache/datafusion-comet/issues/5248 changes that, 
        // re-point it to another recursively serializing serde.
        val arithmeticProto = QueryPlanSerde
          .exprToProto(expression, plan.children.head.output)
          .get
          .getScalarFunc
          .getArgs(1)
        assert(arithmeticProto.hasCheckOverflow, s"$name: $arithmeticProto")
        val overflow = arithmeticProto.getCheckOverflow
        assert(
          overflow.getDatatype === QueryPlanSerde
            .serializeDataType(arrayContains.right.dataType)
            .get,
          s"$name has the wrong CheckOverflow datatype: $arithmeticProto")
        assert(overflow.getFailOnError === mode.failOnError, s"$name: $arithmeticProto")
        assert(
          operation.hasProto(overflow.getChild),
          s"$name has duplicate CheckOverflow: $arithmeticProto")
      }
    }

    operations.foreach { operation =>
      Seq(legacy, ansi).foreach { mode =>
        check(operation, mode, s"$left ${operation.symbol} $right")
      }
    }
    check(
      divide,
      TestMode(name = "TRY", ansiEnabled = true, failOnError = false),
      s"try_divide($left, $right)")
  }

  test("decimal Divide with a non-decimal operand is unsupported") {
    // This is only a sanity check; Spark's type coercion should prevent this case.
    val decimal = AttributeReference("decimal", DecimalType(10, 0))()
    val integer = AttributeReference("integer", IntegerType)()
    val divide = Divide(decimal, integer, EvalMode.LEGACY)

    assert(divide.dataType === DecimalType(10, 0))
    assert(CometDivide.getSupportLevel(divide).isInstanceOf[Unsupported])
  }
}
