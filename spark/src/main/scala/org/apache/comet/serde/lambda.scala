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

import org.apache.spark.sql.catalyst.expressions.{ArrayExists, Attribute, LambdaFunction, NamedLambdaVariable}

import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.serde.QueryPlanSerde._

object CometArrayExists extends CometExpressionSerde[ArrayExists] {

  override def getSupportLevel(expr: ArrayExists): SupportLevel = {
    if (!expr.followThreeValuedLogic) {
      Unsupported(
        Some(
          "legacy ArrayExists two-valued logic is not supported; " +
            "set spark.sql.legacy.followThreeValuedLogicInArrayExists=true"))
    } else {
      Compatible()
    }
  }

  override def convert(
      expr: ArrayExists,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val argumentExpr = exprToProto(expr.argument, inputs, binding)
    val functionExpr = exprToProtoInternal(expr.function, inputs, binding)

    if (argumentExpr.isEmpty || functionExpr.isEmpty) {
      withInfo(expr, expr.argument, expr.function)
      return None
    }

    val builder = ExprOuterClass.HigherOrderFunc
      .newBuilder()
      .setFunc("array_exists")
      .addArgs(argumentExpr.get)
      .addArgs(functionExpr.get)

    Some(ExprOuterClass.Expr.newBuilder().setHigherOrderFunc(builder).build())
  }
}

object CometLambdaFunction extends CometExpressionSerde[LambdaFunction] {

  override def convert(
      expr: LambdaFunction,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val params = expr.arguments.collect { case v: NamedLambdaVariable => v.name }
    if (params.length != expr.arguments.length) {
      withInfo(expr, "lambda arguments must be NamedLambdaVariables")
      return None
    }

    val bodyExpr = exprToProtoInternal(expr.function, inputs, binding)
    if (bodyExpr.isEmpty) {
      withInfo(expr, expr.function)
      return None
    }

    val builder = ExprOuterClass.LambdaFunc
      .newBuilder()
      .setBody(bodyExpr.get)
      .addAllParamNames(params.asJava)

    Some(ExprOuterClass.Expr.newBuilder().setLambdaFunc(builder).build())
  }
}

object CometNamedLambdaVariable extends CometExpressionSerde[NamedLambdaVariable] {

  override def convert(
      expr: NamedLambdaVariable,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    serializeDataType(expr.dataType).map { dt =>
      val builder = ExprOuterClass.LambdaVar
        .newBuilder()
        .setName(expr.name)
        .setDatatype(dt)
        .setNullable(expr.nullable)
      ExprOuterClass.Expr.newBuilder().setLambdaVar(builder).build()
    }
  }
}
