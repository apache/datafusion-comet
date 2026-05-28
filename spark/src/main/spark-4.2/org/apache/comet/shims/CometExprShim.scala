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

package org.apache.comet.shims

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, StaticInvoke}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.TimeType

import org.apache.comet.expressions.CometEvalMode
import org.apache.comet.serde.ExprOuterClass.{BinaryOutputStyle, Expr}
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, optExprWithInfo, scalarFunctionExprToProtoWithReturnType}

/**
 * `CometExprShim` acts as a shim for parsing expressions from different Spark versions.
 */
trait CometExprShim extends Spark4xCometExprShim {

  def binaryOutputStyle: BinaryOutputStyle = {
    // In Spark 4.2, BINARY_OUTPUT_STYLE is an enumConf so getConf already returns the enum value.
    SQLConf.get.getConf(SQLConf.BINARY_OUTPUT_STYLE) match {
      case Some(SQLConf.BinaryOutputStyle.UTF8) => BinaryOutputStyle.UTF8
      case Some(SQLConf.BinaryOutputStyle.BASIC) => BinaryOutputStyle.BASIC
      case Some(SQLConf.BinaryOutputStyle.BASE64) => BinaryOutputStyle.BASE64
      case Some(SQLConf.BinaryOutputStyle.HEX) => BinaryOutputStyle.HEX
      case _ => BinaryOutputStyle.HEX_DISCRETE
    }
  }

  // Spark 4.2 inherits the make_time / to_time / try_to_time planner forms from 4.1, which
  // differ from the shared 4.x patterns. They live here rather than in the shared
  // Spark4xCometExprShim parent so that 4.0 (which lacks TimeType) is unaffected.
  override def sparkVersionSpecificExprToProtoInternal(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    expr match {
      case s: StaticInvoke
          if s.staticObject == classOf[DateTimeUtils.type] &&
            s.functionName == "makeTime" &&
            s.arguments.size == 3 &&
            s.dataType.isInstanceOf[TimeType] =>
        val childExprs = s.arguments.map(exprToProtoInternal(_, inputs, binding))
        val optExpr =
          scalarFunctionExprToProtoWithReturnType("make_time", s.dataType, true, childExprs: _*)
        optExprWithInfo(optExpr, expr, s.arguments: _*)

      case i: Invoke =>
        (i.targetObject, i.functionName, i.arguments) match {
          case (Literal(parser: ToTimeParser, _), "parse", args)
              if i.dataType.isInstanceOf[TimeType] && parser.fmt.isEmpty && args.size == 1 =>
            val childExprs = args.map(exprToProtoInternal(_, inputs, binding))
            val optExpr =
              scalarFunctionExprToProtoWithReturnType("to_time", i.dataType, true, childExprs: _*)
            optExprWithInfo(optExpr, i, args: _*)
          case _ =>
            super.sparkVersionSpecificExprToProtoInternal(expr, inputs, binding)
        }

      // try_to_time resolves to TryEval(Invoke(Literal(ToTimeParser), "parse", ...))
      case TryEval(i: Invoke) =>
        (i.targetObject, i.functionName, i.arguments) match {
          case (Literal(parser: ToTimeParser, _), "parse", args)
              if i.dataType.isInstanceOf[TimeType] && parser.fmt.isEmpty && args.size == 1 =>
            val childExprs = args.map(exprToProtoInternal(_, inputs, binding))
            val optExpr = scalarFunctionExprToProtoWithReturnType(
              "to_time",
              i.dataType,
              false,
              childExprs: _*)
            optExprWithInfo(optExpr, expr, args: _*)
          case _ =>
            super.sparkVersionSpecificExprToProtoInternal(expr, inputs, binding)
        }

      case _ => super.sparkVersionSpecificExprToProtoInternal(expr, inputs, binding)
    }
  }
}

object CometEvalModeUtil {
  def fromSparkEvalMode(evalMode: EvalMode.Value): CometEvalMode.Value = evalMode match {
    case EvalMode.LEGACY => CometEvalMode.LEGACY
    case EvalMode.TRY => CometEvalMode.TRY
    case EvalMode.ANSI => CometEvalMode.ANSI
  }

  // In Spark 4.2, Sum carries a NumericEvalContext rather than a direct EvalMode.
  def sumEvalMode(s: Sum): EvalMode.Value = s.evalContext.evalMode
}
