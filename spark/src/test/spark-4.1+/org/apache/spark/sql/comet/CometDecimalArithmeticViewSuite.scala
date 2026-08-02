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
import org.apache.spark.sql.catalyst.expressions.{Add, AttributeReference, BinaryArithmetic, CheckOverflow, Divide, EvalMode, Expression, Multiply, NumericEvalContext, Remainder, Subtract}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DecimalType

import org.apache.comet.serde.{ExprOuterClass, QueryPlanSerde}

class CometDecimalArithmeticViewSuite extends CometTestBase {

  // Spark 4.1+ (SPARK-53968) stores `spark.sql.decimalOperations.allowPrecisionLoss` per
  // arithmetic expression so a view's analyzed plan keeps a stable result type across config
  // changes. Comet's DecimalPrecision rule used to recompute the result type from the current
  // SQLConf, producing a CheckOverflow target that disagreed with the stored Add.dataType and
  // re-labelling the Decimal128 buffer at the wrong scale (issue #4124). The repro requires a
  // mismatch between the stored evalContext and the live SQLConf, which is why we construct
  // Add directly rather than going through SQL parsing.
  test("issue #4124: DecimalPrecision.promote honours per-expression allowPrecisionLoss") {
    val left = AttributeReference("a", DecimalType(38, 18))()
    val right = AttributeReference("b", DecimalType(38, 18))()
    val storedTrue =
      Add(left, right, NumericEvalContext(EvalMode.LEGACY, allowDecimalPrecisionLoss = true))
    val storedFalse =
      Add(left, right, NumericEvalContext(EvalMode.LEGACY, allowDecimalPrecisionLoss = false))
    assert(storedTrue.dataType === DecimalType(38, 17))
    assert(storedFalse.dataType === DecimalType(38, 18))

    // Current SQLConf disagrees with the stored evalContext on each Add. The promoted
    // CheckOverflow's target type must come from Add.dataType (which honours the stored
    // evalContext), not from the current SQLConf. Otherwise the native CheckOverflow
    // re-labels the Decimal128 buffer at the wrong scale and values come out 10x off.
    Seq((true, storedFalse), (false, storedTrue)).foreach { case (currentConf, add) =>
      withSQLConf(SQLConf.DECIMAL_OPERATIONS_ALLOW_PREC_LOSS.key -> currentConf.toString) {
        val promoted = org.apache.spark.sql.comet.DecimalPrecision
          .promote(add)
        promoted match {
          case CheckOverflow(_, dt, _) =>
            assert(
              dt === add.dataType,
              s"CheckOverflow target $dt must match Add.dataType ${add.dataType}; mismatch " +
                "causes the decimal buffer to be re-labelled at the wrong scale.")
          case other =>
            fail(s"Expected DecimalPrecision.promote to wrap Add in CheckOverflow, got: $other")
        }
      }
    }
  }

  test("issue #5075: DecimalPrecision.promote honours per-expression eval mode") {
    val left = AttributeReference("a", DecimalType(10, 0))()
    val right = AttributeReference("b", DecimalType(10, 0))()
    val third = AttributeReference("c", DecimalType(10, 0))()

    val operations: Seq[(
        String,
        (Expression, Expression, NumericEvalContext) => BinaryArithmetic,
        ExprOuterClass.Expr => ExprOuterClass.MathExpr)] = Seq(
      ("add", Add.apply, _.getAdd),
      ("subtract", Subtract.apply, _.getSubtract),
      ("multiply", Multiply.apply, _.getMultiply),
      ("divide", Divide.apply, _.getDivide),
      ("remainder", Remainder.apply, _.getRemainder))
    val tryContext = NumericEvalContext(EvalMode.TRY, allowDecimalPrecisionLoss = true)
    val ansiContext = NumericEvalContext(EvalMode.ANSI, allowDecimalPrecisionLoss = true)
    val expressionTrees = operations.map { case (name, operation, getMathExpr) =>
      (name, operation(operation(left, right, tryContext), third, ansiContext), getMathExpr)
    }

    Seq(false, true).foreach { sessionAnsiEnabled =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> sessionAnsiEnabled.toString) {
        expressionTrees.foreach { case (name, expression, getMathExpr) =>
          val proto = QueryPlanSerde.exprToProto(expression, Seq(left, right, third)).get
          assert(proto.hasCheckOverflow, s"$name under session ANSI=$sessionAnsiEnabled")
          val ansiOverflow = proto.getCheckOverflow
          assert(ansiOverflow.getFailOnError, s"$name under session ANSI=$sessionAnsiEnabled")

          val mathExprProto = ansiOverflow.getChild
          val tryExprProto = getMathExpr(mathExprProto).getLeft
          assert(tryExprProto.hasCheckOverflow, s"$name under session ANSI=$sessionAnsiEnabled")
          assert(
            !tryExprProto.getCheckOverflow.getFailOnError,
            s"$name under session ANSI=$sessionAnsiEnabled")
        }
      }
    }
  }

  test("issue #5190: recursive serialization does not duplicate decimal CheckOverflow") {
    val left = "CAST(id AS DECIMAL(10, 0))"
    val right = "CAST(id + 1 AS DECIMAL(10, 0))"
    val operations: Seq[(String, Boolean, String, ExprOuterClass.Expr => Boolean, Boolean)] = Seq(
      ("add", false, s"$left + $right", _.hasAdd, false),
      ("subtract", false, s"$left - $right", _.hasSubtract, false),
      ("multiply", false, s"$left * $right", _.hasMultiply, false),
      ("remainder", false, s"$left % $right", _.hasRemainder, false),
      ("divide LEGACY", false, s"$left / $right", _.hasDivide, false),
      ("divide TRY", true, s"try_divide($left, $right)", _.hasDivide, false),
      ("divide ANSI", true, s"$left / $right", _.hasDivide, true))

    operations.foreach { case (name, ansiEnabled, arithmetic, hasOperation, failOnError) =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> ansiEnabled.toString) {
        val plan = spark
          .sql(s"SELECT array_contains(array($arithmetic), $arithmetic) FROM range(1, 4)")
          .queryExecution
          .optimizedPlan
        val arithmeticProto = QueryPlanSerde
          .exprToProto(plan.expressions.head, plan.children.head.output)
          .get
          .getScalarFunc
          .getArgs(1)

        assert(arithmeticProto.hasCheckOverflow, s"$name: $arithmeticProto")
        val overflow = arithmeticProto.getCheckOverflow
        assert(overflow.getFailOnError === failOnError, s"$name: $arithmeticProto")
        assert(
          hasOperation(overflow.getChild),
          s"$name has duplicate CheckOverflow: $arithmeticProto")
      }
    }
  }
}
