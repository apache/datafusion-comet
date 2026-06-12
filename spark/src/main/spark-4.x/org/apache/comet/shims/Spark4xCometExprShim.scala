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
import org.apache.spark.sql.catalyst.expressions.json.{JsonExpressionUtils, StructsToJsonEvaluator}
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, StaticInvoke}
import org.apache.spark.sql.catalyst.expressions.url.ParseUrlEvaluator
import org.apache.spark.sql.internal.types.StringTypeWithCollation
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, StringType}

import org.apache.comet.CometExplainInfo
import org.apache.comet.expressions.CometEvalMode
import org.apache.comet.serde.{CometExpressionSerde, CometMapSort, CometScalaUDF, CometToPrettyString, CometWidthBucket}
import org.apache.comet.serde.ExprOuterClass.Expr
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, optExprWithFallbackReason, scalarFunctionExprToProtoWithReturnType}

/**
 * Shared trait body for the Spark 4.x `CometExprShim` traits (4.0/4.1/4.2). Holds the parts that
 * are identical across minor versions; per-version traits override only `binaryOutputStyle` and
 * supply the matching `CometEvalModeUtil.sumEvalMode`.
 */
trait Spark4xCometExprShim extends CometExprShim4x {
  protected def evalMode(c: Cast): CometEvalMode.Value =
    CometEvalModeUtil.fromSparkEvalMode(c.evalMode)

  def sparkVersionSpecificStringExpressions
      : Map[Class[_ <: Expression], CometExpressionSerde[_]] =
    Map.empty
  def sparkVersionSpecificMathExpressions: Map[Class[_ <: Expression], CometExpressionSerde[_]] =
    Map(classOf[WidthBucket] -> CometWidthBucket)
  def sparkVersionSpecificMiscExpressions: Map[Class[_ <: Expression], CometExpressionSerde[_]] =
    Map(classOf[ToPrettyString] -> CometToPrettyString)
  def sparkVersionSpecificMapExpressions: Map[Class[_ <: Expression], CometExpressionSerde[_]] =
    Map(classOf[MapSort] -> CometMapSort)

  def sparkVersionSpecificExprToProtoInternal(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    expr match {
      // RuntimeReplaceable structured-text functions (schema_of_csv/json, json_object_keys,
      // xpath_*, schema_of_xml) and from_xml/to_xml route through the codegen dispatcher; see
      // CometExprShim4x.convertStructuredText. Guarded so non-structured-text Invoke/StaticInvoke
      // nodes still reach their existing handlers below.
      case e if isStructuredTextDispatch(e) =>
        convertStructuredText(e, inputs, binding)

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
                optExprWithFallbackReason(scalarExpr, knc, arrayChild)
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
        // Spark 4.0 lowers `decode(bin, charset)` to a `StaticInvoke` carrying the
        // `legacyCharsets` / `legacyErrorAction` flags. Native lowering to a TRY-mode cast would
        // ignore those flags and produce wrong output on invalid byte sequences, so we route the
        // entire `StaticInvoke` through the codegen dispatcher: Spark's own `doGenCode` runs
        // inside the Comet pipeline and honors both flags exactly. Falls back to Spark when the
        // dispatcher is disabled. See https://github.com/apache/datafusion-comet/issues/4465.
        CometScalaUDF.emitJvmCodegenDispatch(s, inputs, binding)

      // On Spark 4.0+, RuntimeReplaceable expressions (StructsToJson, ParseUrl) become
      // Invoke(Literal(Evaluator), "evaluate", ...). Reconstruct the original expression and
      // recurse so support-level checks apply, propagating any explain info back onto the
      // outer Invoke so fallback reasons surface to the user.
      case i: Invoke =>
        (i.targetObject, i.functionName, i.arguments) match {
          case (Literal(evaluator: StructsToJsonEvaluator, _), "evaluate", Seq(child)) =>
            val toJson = StructsToJson(evaluator.options, child, evaluator.timeZoneId)
            val exprProto = exprToProtoInternal(toJson, inputs, binding)
            if (exprProto.isEmpty) {
              toJson
                .getTagValue(CometExplainInfo.FALLBACK_REASONS)
                .foreach(reasons => i.setTagValue(CometExplainInfo.FALLBACK_REASONS, reasons))
            }
            exprProto
          case (Literal(evaluator: ParseUrlEvaluator, _), "evaluate", args) =>
            val parseUrl = ParseUrl(args, evaluator.failOnError)
            val result = exprToProtoInternal(parseUrl, inputs, binding)
            if (result.isEmpty) {
              parseUrl
                .getTagValue(CometExplainInfo.FALLBACK_REASONS)
                .foreach(reasons => i.setTagValue(CometExplainInfo.FALLBACK_REASONS, reasons))
            }
            result
          case _ => None
        }

      case s: StaticInvoke =>
        (s.staticObject, s.functionName, s.arguments) match {
          case (cls, "lengthOfJsonArray", Seq(child)) if cls == classOf[JsonExpressionUtils] =>
            val lengthOfJsonArray = LengthOfJsonArray(child)
            val exprProto = exprToProtoInternal(lengthOfJsonArray, inputs, binding)
            if (exprProto.isEmpty) {
              lengthOfJsonArray
                .getTagValue(CometExplainInfo.FALLBACK_REASONS)
                .foreach(reasons => s.setTagValue(CometExplainInfo.FALLBACK_REASONS, reasons))
            }
            exprProto
          case _ => None
        }

      // dayname / monthname (Spark 4.0+) are shared across all 4.x minor versions; see
      // CometExprShim4x.convertDayMonthName.
      case _: DayName | _: MonthName =>
        convertDayMonthName(expr, inputs, binding)

      case _ => None
    }
  }
}
