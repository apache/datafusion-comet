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
import org.apache.spark.sql.catalyst.expressions.json.StructsToJsonEvaluator
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, StaticInvoke}
import org.apache.spark.sql.internal.types.StringTypeWithCollation
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, StringType}

import org.apache.comet.expressions.CometEvalMode
import org.apache.comet.serde.{CometExpressionSerde, CometMapSort, CometToPrettyString, CometWidthBucket, CommonStringExprs}
import org.apache.comet.serde.ExprOuterClass.Expr
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, optExprWithInfo, scalarFunctionExprToProtoWithReturnType}

/**
 * Shared trait body for the Spark 4.x `CometExprShim` traits (4.0/4.1/4.2). Holds the parts that
 * are identical across minor versions; per-version traits override only `binaryOutputStyle` and
 * supply the matching `CometEvalModeUtil.sumEvalMode`.
 */
trait Spark4xCometExprShim extends CommonStringExprs {
  protected def evalMode(c: Cast): CometEvalMode.Value =
    CometEvalModeUtil.fromSparkEvalMode(c.evalMode)

  def versionSpecificStringExpressions: Map[Class[_ <: Expression], CometExpressionSerde[_]] =
    Map.empty
  def versionSpecificMathExpressions: Map[Class[_ <: Expression], CometExpressionSerde[_]] =
    Map(classOf[WidthBucket] -> CometWidthBucket)
  def versionSpecificMiscExpressions: Map[Class[_ <: Expression], CometExpressionSerde[_]] =
    Map(classOf[ToPrettyString] -> CometToPrettyString)
  def versionSpecificMapExpressions: Map[Class[_ <: Expression], CometExpressionSerde[_]] =
    Map(classOf[MapSort] -> CometMapSort)

  def versionSpecificExprToProtoInternal(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    expr match {
      case knc: KnownNotContainsNull =>
        // On Spark 4.0+, array_compact rewrites to KnownNotContainsNull(ArrayFilter(IsNotNull)).
        // Strip the wrapper and serialize the inner ArrayFilter as spark_array_compact.
        knc.child match {
          case filter: ArrayFilter =>
            filter.function.children.headOption match {
              case Some(_: IsNotNull) =>
                val arrayChild = filter.left
                val elementType = arrayChild.dataType.asInstanceOf[ArrayType].elementType
                val arrayExprProto = exprToProtoInternal(arrayChild, inputs, binding)
                val returnType = ArrayType(elementType)
                val scalarExpr = scalarFunctionExprToProtoWithReturnType(
                  "spark_array_compact",
                  returnType,
                  false,
                  arrayExprProto)
                optExprWithInfo(scalarExpr, knc, arrayChild)
              case _ => exprToProtoInternal(knc.child, inputs, binding)
            }
          case _ => exprToProtoInternal(knc.child, inputs, binding)
        }

      case s: StaticInvoke
          if s.staticObject == classOf[StringDecode] &&
            s.dataType.isInstanceOf[StringType] &&
            s.functionName == "decode" &&
            s.arguments.size == 4 &&
            s.inputTypes == Seq(
              BinaryType,
              StringTypeWithCollation(supportsTrimCollation = true),
              BooleanType,
              BooleanType) =>
        val Seq(bin, charset, _, _) = s.arguments
        stringDecode(expr, charset, bin, inputs, binding)

      // On Spark 4.0+, StructsToJson is a RuntimeReplaceable whose replacement is
      // Invoke(Literal(StructsToJsonEvaluator), "evaluate", ...). Reconstruct the
      // original StructsToJson and recurse so support-level checks apply.
      case i: Invoke =>
        (i.targetObject, i.functionName, i.arguments) match {
          case (Literal(evaluator: StructsToJsonEvaluator, _), "evaluate", Seq(child)) =>
            exprToProtoInternal(
              StructsToJson(evaluator.options, child, evaluator.timeZoneId),
              inputs,
              binding)
          case _ => None
        }

      case _ => None
    }
  }
}
