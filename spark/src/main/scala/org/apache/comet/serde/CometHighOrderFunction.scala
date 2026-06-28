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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.expressions.{Attribute, HigherOrderFunction, LambdaFunction => SparkLambdaFunction, NamedLambdaVariable => SparkNamedLambdaVariable}

import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.serde.CometHighOrderFunction.nlv2Proto
import org.apache.comet.serde.ExprOuterClass.{HigherOrderFunc, LambdaFunction, NamedLambdaVariable}
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, serializeDataType}

case class CometHighOrderFunction[T <: HigherOrderFunction](name: String)
    extends CometExpressionSerde[T] {

  private val UNSUPPORTED_LAMBDA_TYPE = "lambda functions must be LambdaFunction"
  private val UNSUPPORTED_LAMBDA_PARAM_TYPE = "lambda arguments must be NamedLambdaVariables"
  private val UNARY_FUNCTION_EXPECTED =
    "DataFusion higher-order functions support only 1 argument"

  override def getIncompatibleReasons(): Seq[String] =
    Seq(UNSUPPORTED_LAMBDA_TYPE, UNSUPPORTED_LAMBDA_PARAM_TYPE)

  override def getSupportLevel(expr: T): SupportLevel = {
    if (!expr.functions.forall(_.isInstanceOf[SparkLambdaFunction])) {
      return Unsupported(Some(UNSUPPORTED_LAMBDA_TYPE))
    }
    val lambdaFunctions = expr.functions.map(_.asInstanceOf[SparkLambdaFunction])
    if (!lambdaFunctions.forall(_.arguments.length == 1)) {
      return Unsupported(Some(UNARY_FUNCTION_EXPECTED))
    }
    if (!expr.functions
        .flatMap(_.asInstanceOf[SparkLambdaFunction].arguments)
        .forall(_.isInstanceOf[SparkNamedLambdaVariable])) {
      return Unsupported(Some(UNSUPPORTED_LAMBDA_PARAM_TYPE))
    }
    Compatible()
  }

  def convert(expr: T, inputs: Seq[Attribute], binding: Boolean): Option[ExprOuterClass.Expr] = {
    val argumentsProto = expr.arguments.map(exprToProtoInternal(_, inputs, binding))
    val functionsProto = expr.functions
      .map(_.asInstanceOf[SparkLambdaFunction])
      .map { slf =>
        val maybeExpr = exprToProtoInternal(slf.function, inputs, binding)
        maybeExpr
          .map { bodyProto =>
            val nlvProto = slf.arguments
              .map(_.asInstanceOf[SparkNamedLambdaVariable])
              .map(nlv2Proto)
            LambdaFunction
              .newBuilder()
              .addAllArgs(nlvProto.asJava)
              .setBody(bodyProto)
              .build()
          }
      }
    if (functionsProto.forall(_.isDefined) && argumentsProto.forall(_.isDefined)) {
      val hof = HigherOrderFunc
        .newBuilder()
        .setFuncName(name)
        .addAllValueArgs(argumentsProto.map(_.get).asJava)
        .addAllLambdas(functionsProto.map(_.get).asJava)
        .build()
      Some(ExprOuterClass.Expr.newBuilder().setHighOrderFunc(hof).build())
    } else {
      withFallbackReason(expr, expr.children: _*)
      None
    }
  }
}

object CometHighOrderFunction {
  def nlv2Proto(nlv: SparkNamedLambdaVariable): NamedLambdaVariable = {
    NamedLambdaVariable
      .newBuilder()
      .setName(nlv.name)
      .setNullable(nlv.nullable)
      .setDataType(serializeDataType(nlv.dataType).get)
      .build()
  }
}

object CometNamedLambdaVariable extends CometExpressionSerde[SparkNamedLambdaVariable] {
  def convert(
      expr: SparkNamedLambdaVariable,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val nlvProto = CometHighOrderFunction.nlv2Proto(expr)
    Some(
      ExprOuterClass.Expr
        .newBuilder()
        .setNamedLambdaVariable(nlvProto)
        .build())
  }
}
