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

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, HigherOrderFunction, LambdaFunction => SparkLambdaFunction, NamedLambdaVariable => SparkNamedLambdaVariable}

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.serde.CometHighOrderFunction.namedLambdaVariable2Proto
import org.apache.comet.serde.ExprOuterClass.{HigherOrderFunc, LambdaFunction, NamedLambdaVariable}
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, serializeDataType}

/**
 * Serializer that converts Spark higher-order functions (e.g. `filter`, `transform`, `exists`)
 * into Comet's protobuf representation.
 *
 * Depending on the available configuration and on whether the expression satisfies the native
 * constraints, [[convert]] produces one of two representations:
 *   - a native higher-order function proto (executed by the DataFusion engine), used when
 *     `COMET_EXEC_HIGHER_ORDER_FUNCTION_NATIVE_ENABLED` is set and the expression is natively
 *     supported (see [[nativeUnsupportedReason]] / [[getSupportLevel]]); or
 *   - a JVM codegen dispatch (Scala UDF fallback via `CometScalaUDF.emitJvmCodegenDispatch`),
 *     used when the native path is unavailable but `COMET_SCALA_UDF_CODEGEN_ENABLED` is enabled.
 */

case class CometHighOrderFunction[T <: HigherOrderFunction](name: String)
    extends CometExpressionSerde[T] {

  private val UNSUPPORTED_LAMBDA_TYPE = "lambda functions must be LambdaFunction"
  private val UNSUPPORTED_LAMBDA_PARAM_TYPE = "lambda arguments must be NamedLambdaVariables"

  override def getUnsupportedReasons(): Seq[String] =
    Seq(UNSUPPORTED_LAMBDA_TYPE, UNSUPPORTED_LAMBDA_PARAM_TYPE)

  private def nativeUnsupportedReason(expr: T): Option[String] = {
    if (!expr.functions.forall(_.isInstanceOf[SparkLambdaFunction])) {
      return Some(UNSUPPORTED_LAMBDA_TYPE)
    }
    if (!expr.functions
        .flatMap(_.asInstanceOf[SparkLambdaFunction].arguments)
        .forall(_.isInstanceOf[SparkNamedLambdaVariable])) {
      return Some(UNSUPPORTED_LAMBDA_PARAM_TYPE)
    }
    None
  }

  override def getSupportLevel(expr: T): SupportLevel = {
    val unsupportedReason = nativeUnsupportedReason(expr)
    val nativeAvailable =
      unsupportedReason.isEmpty && CometConf.COMET_EXEC_HIGHER_ORDER_FUNCTION_NATIVE_ENABLED.get()
    val codegenEnabled = CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.get()
    if (nativeAvailable || codegenEnabled) {
      Compatible()
    } else {
      Unsupported(unsupportedReason)
    }
  }

  def convert(expr: T, inputs: Seq[Attribute], binding: Boolean): Option[ExprOuterClass.Expr] = {
    val nativeAvailable =
      nativeUnsupportedReason(
        expr).isEmpty && CometConf.COMET_EXEC_HIGHER_ORDER_FUNCTION_NATIVE_ENABLED.get()
    val hofProto = highOrderFunction2Proto(expr, inputs, binding)
    if (nativeAvailable && hofProto.isDefined) {
      hofProto
    } else {
      CometScalaUDF.emitJvmCodegenDispatch(expr, inputs, binding)
    }
  }

  private def highOrderFunction2Proto(
      expr: T,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val argumentsProto = expr.arguments.map(exprToProtoInternal(_, inputs, binding))
    val functionsProto = expr.functions
      .map { func =>
        val sparkLambdaFunction = func.asInstanceOf[SparkLambdaFunction]
        exprToProtoInternal(sparkLambdaFunction.function, inputs, binding)
          .flatMap { bodyProto =>
            val namedLambdaVariablesProto = sparkLambdaFunction.arguments
              .map { arg =>
                val sparkNamedLambdaVariable = arg.asInstanceOf[SparkNamedLambdaVariable]
                namedLambdaVariable2Proto(sparkNamedLambdaVariable)
              }
            if (namedLambdaVariablesProto.forall(_.isDefined)) {
              Some(
                LambdaFunction
                  .newBuilder()
                  .addAllArgs(namedLambdaVariablesProto.map(_.get).asJava)
                  .setBody(bodyProto)
                  .build())
            } else {
              None
            }
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
  def namedLambdaVariable2Proto(nlv: SparkNamedLambdaVariable): Option[NamedLambdaVariable] = {
    val dataTypeProto = serializeDataType(nlv.dataType)
    if (dataTypeProto.isEmpty) {
      withFallbackReason(nlv, s"Unsupported datatype: ${nlv.dataType}")
      return None
    }
    Some(
      NamedLambdaVariable
        .newBuilder()
        .setName(nlv.name)
        .setNullable(nlv.nullable)
        .setDataType(dataTypeProto.get)
        .build())
  }
}

object CometNamedLambdaVariable extends CometExpressionSerde[SparkNamedLambdaVariable] {
  def convert(
      expr: SparkNamedLambdaVariable,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    CometHighOrderFunction
      .namedLambdaVariable2Proto(expr)
      .map { nlvProto =>
        ExprOuterClass.Expr
          .newBuilder()
          .setNamedLambdaVariable(nlvProto)
          .build()
      }
  }
}
