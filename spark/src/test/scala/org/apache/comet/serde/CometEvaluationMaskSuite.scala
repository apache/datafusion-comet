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
import org.apache.spark.sql.catalyst.expressions.{Add, AttributeReference, Cast, Concat, Expression, FormatString, Like, Literal, UnBase64}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StringType}

import org.apache.comet.CometConf

class CometEvaluationMaskSuite extends CometTestBase {

  private class DerivedUnBase64(input: Expression, strict: Boolean)
      extends UnBase64(input, strict) {
    override protected def withNewChildInternal(newChild: Expression): UnBase64 =
      new DerivedUnBase64(newChild, failOnError)
  }

  test("only unbase64 serdes opt into Spark evaluation masks") {
    val enrolled = QueryPlanSerde.exprSerdeMap.collect {
      case (expressionClass, _: RequiresSparkEvaluationMask[_]) => expressionClass
    }.toSet
    assert(enrolled == Set(classOf[UnBase64]))
  }

  test("unbase64 evaluation masks are independent of native support and the dispatcher") {
    for (dispatch <- Seq(false, true)) {
      withSQLConf(CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> dispatch.toString) {
        val input = AttributeReference("encoded", StringType)()
        val children: Seq[(String, Expression, Boolean)] = Seq(
          ("column", input, true),
          ("literal", Literal("YWJj"), true),
          ("compound", Concat(Seq(input, Literal(""))), false))
        for ((name, child, nativeChild) <- children; strict <- Seq(false, true)) {
          withClue(s"dispatch=$dispatch, child=$name, failOnError=$strict: ") {
            val expr = UnBase64(child, failOnError = strict)
            assert(QueryPlanSerde.evaluationMaskName(expr).contains("unbase64"))

            val native = nativeChild && !strict
            val support = CometUnBase64.getSupportLevel(expr)
            if (native) {
              assert(support.isInstanceOf[Compatible])
            } else {
              assert(support.isInstanceOf[Unsupported])
            }

            val proto = QueryPlanSerde.exprToProtoInternal(expr, Seq(input), binding = false)
            if (native) {
              assert(proto.exists(p => p.hasScalarFunc && p.getScalarFunc.getFunc == "unbase64"))
            } else if (dispatch) {
              assert(proto.exists(_.hasJvmScalarUdf))
            } else {
              assert(proto.isEmpty)
            }
          }
        }
      }
    }
  }

  test("evaluation mask lookup preserves unregistered unbase64 subclasses") {
    val input = AttributeReference("encoded", StringType)()
    for (strict <- Seq(false, true)) {
      val expr: Expression = new DerivedUnBase64(input, strict)
      assert(!QueryPlanSerde.exprSerdeMap.contains(expr.getClass))
      assert(QueryPlanSerde.evaluationMaskName(expr).contains("unbase64"))
    }
  }

  test("ANSI arithmetic and other dispatcher serdes are not enrolled in evaluation masks") {
    for (ansi <- Seq(false, true); dispatch <- Seq(false, true)) {
      withSQLConf(
        SQLConf.ANSI_ENABLED.key -> ansi.toString,
        CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> dispatch.toString) {
        val input = AttributeReference("input", StringType)()
        val expressions: Seq[Expression] = Seq(
          Add(Literal(Int.MaxValue), Literal(1)),
          Cast(input, IntegerType),
          Concat(Seq(input, Literal(""))),
          FormatString(Literal("%s"), input),
          Like(input, Literal("%"), escapeChar = '\\'))
        expressions.foreach { expr =>
          withClue(s"ansi=$ansi, dispatch=$dispatch, expression=${expr.nodeName}: ") {
            assert(QueryPlanSerde.exprSerdeMap.contains(expr.getClass))
            // None means not enrolled, even for expressions that can throw on some inputs.
            assert(QueryPlanSerde.evaluationMaskName(expr).isEmpty)
          }
        }
      }
    }
  }
}
