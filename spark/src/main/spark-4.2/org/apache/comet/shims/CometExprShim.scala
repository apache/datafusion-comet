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
import org.apache.spark.sql.catalyst.expressions.json.StructsToJsonEvaluator
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, StaticInvoke}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.types.StringTypeWithCollation
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, DataTypes, StringType}

import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.expressions.{CometCast, CometEvalMode}
import org.apache.comet.serde.{CometExpressionSerde, CometWidthBucket, CommonStringExprs, Compatible, ExprOuterClass, Incompatible}
import org.apache.comet.serde.ExprOuterClass.{BinaryOutputStyle, Expr}
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, optExprWithInfo, scalarFunctionExprToProtoWithReturnType}

/**
 * `CometExprShim` acts as a shim for parsing expressions from different Spark versions.
 */
trait CometExprShim extends CommonStringExprs {
  protected def evalMode(c: Cast): CometEvalMode.Value =
    CometEvalModeUtil.fromSparkEvalMode(c.evalMode)

  def binaryOutputStyle: BinaryOutputStyle = {
    // In Spark 4.1, BINARY_OUTPUT_STYLE is an enumConf so getConf already returns the enum value.
    SQLConf.get.getConf(SQLConf.BINARY_OUTPUT_STYLE) match {
      case Some(SQLConf.BinaryOutputStyle.UTF8) => BinaryOutputStyle.UTF8
      case Some(SQLConf.BinaryOutputStyle.BASIC) => BinaryOutputStyle.BASIC
      case Some(SQLConf.BinaryOutputStyle.BASE64) => BinaryOutputStyle.BASE64
      case Some(SQLConf.BinaryOutputStyle.HEX) => BinaryOutputStyle.HEX
      case _ => BinaryOutputStyle.HEX_DISCRETE
    }
  }

  def versionSpecificStringExpressions: Map[Class[_ <: Expression], CometExpressionSerde[_]] =
    Map.empty
  def versionSpecificMathExpressions: Map[Class[_ <: Expression], CometExpressionSerde[_]] =
    Map(classOf[WidthBucket] -> CometWidthBucket)
  def versionSpecificMiscExpressions: Map[Class[_ <: Expression], CometExpressionSerde[_]] =
    Map.empty

  def versionSpecificExprToProtoInternal(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    expr match {
      case knc: KnownNotContainsNull =>
        // On Spark 4.0, array_compact rewrites to KnownNotContainsNull(ArrayFilter(IsNotNull)).
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

      case expr @ ToPrettyString(child, timeZoneId) =>
        val castSupported = CometCast.isSupported(
          child.dataType,
          DataTypes.StringType,
          timeZoneId,
          CometEvalMode.TRY)

        val isCastSupported = castSupported match {
          case Compatible(_) => true
          case Incompatible(_) => true
          case _ => false
        }

        if (isCastSupported) {
          exprToProtoInternal(child, inputs, binding) match {
            case Some(p) =>
              val toPrettyString = ExprOuterClass.ToPrettyString
                .newBuilder()
                .setChild(p)
                .setTimezone(timeZoneId.getOrElse("UTC"))
                .setBinaryOutputStyle(binaryOutputStyle)
                .build()
              Some(
                ExprOuterClass.Expr
                  .newBuilder()
                  .setToPrettyString(toPrettyString)
                  .build())
            case _ =>
              withInfo(expr, child)
              None
          }
        } else {
          None
        }

      // In Spark 4.0, StructsToJson is a RuntimeReplaceable whose replacement is
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

object CometEvalModeUtil {
  def fromSparkEvalMode(evalMode: EvalMode.Value): CometEvalMode.Value = evalMode match {
    case EvalMode.LEGACY => CometEvalMode.LEGACY
    case EvalMode.TRY => CometEvalMode.TRY
    case EvalMode.ANSI => CometEvalMode.ANSI
  }

  // In Spark 4.1, Sum carries a NumericEvalContext rather than a direct EvalMode.
  def sumEvalMode(s: Sum): EvalMode.Value = s.evalContext.evalMode
}
