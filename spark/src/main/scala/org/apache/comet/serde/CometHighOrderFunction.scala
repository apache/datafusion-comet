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

import org.apache.comet.CometConf
import org.apache.comet.serde.CometHighOrderFunction.{containsJvmDispatch, namedLambdaVariable2Proto}
import org.apache.comet.serde.ExprOuterClass.{HigherOrderFunc, LambdaFunction, NamedLambdaVariable}
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, serializeDataType}

/**
 * Serializer that converts Spark higher-order functions (e.g. `filter`, `transform`, `exists`)
 * into Comet's protobuf representation.
 *
 * Path selection happens in [[convert]] and has exactly two outcomes, chosen in order:
 *
 * Native HOF proto - when `spark.comet.exec.higherOrderFunction.native.enabled` is set and
 * [[highOrderFunction2Proto]] serializes the whole expression natively. The method returns `None`
 * (and the HOF falls to the next path) if the HOF is structurally invalid (lambda functions must
 * be `LambdaFunction`, lambda arguments must be `NamedLambdaVariable`) or if a lambda body
 * contains a dispatch-only subexpression (regex, JSON, ...). Such subexpressions are serialized
 * as JVM codegen dispatch nodes, which cannot bind `NamedLambdaVariable`s, so the lambda cannot
 * be executed natively. A dispatch-only expression among the *value* arguments is fine and stays
 * native. JVM codegen dispatch - `CometScalaUDF.emitJvmCodegenDispatch` runs the whole HOF
 * (lambda included) on the JVM; `NamedLambdaVariable`s never cross this boundary because the
 * lambda is evaluated by Spark's own implementation. Returns `None` when the dispatcher cannot
 * handle the expression, which falls back to Spark entirely.
 *
 * Whether a lambda body requires dispatch is decided from the already-built body proto by
 * [[CometHighOrderFunction.containsJvmDispatch]] - the body is serialized exactly once, and the
 * verdict comes from the same proto that native execution would use.
 */
case class CometHighOrderFunction[T <: HigherOrderFunction](name: String)
    extends CometExpressionSerde[T] {

  def convert(expr: T, inputs: Seq[Attribute], binding: Boolean): Option[ExprOuterClass.Expr] = {
    if (!CometConf.COMET_EXEC_HIGHER_ORDER_FUNCTION_NATIVE_ENABLED.get()) {
      return CometScalaUDF.emitJvmCodegenDispatch(expr, inputs, binding)
    }
    highOrderFunction2Proto(expr, inputs, binding)
      .orElse {
        CometScalaUDF.emitJvmCodegenDispatch(expr, inputs, binding)
      }
  }

  private def highOrderFunction2Proto(
      expr: T,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val argumentsProto = expr.arguments.map(exprToProtoInternal(_, inputs, binding))
    val functionsProto = expr.functions
      .map {
        case slf: SparkLambdaFunction =>
          exprToProtoInternal(slf.function, inputs, binding)
            .flatMap { bodyProto =>
              if (containsJvmDispatch(bodyProto)) {
                return None
              }
              val namedLambdaVariablesProto = slf.arguments
                .map {
                  case arg: SparkNamedLambdaVariable =>
                    namedLambdaVariable2Proto(arg)
                  case _ => None
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
        case _ => None
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
      None
    }
  }
}

object CometHighOrderFunction {
  private def containsJvmDispatch(e: ExprOuterClass.Expr): Boolean =
    containsJvmDispatch(e.asInstanceOf[com.google.protobuf.Message])

  private def containsJvmDispatch(m: com.google.protobuf.Message): Boolean =
    m match {
      case e: ExprOuterClass.Expr if e.hasJvmScalarUdf => true
      case _ =>
        m.getAllFields.values().asScala.exists {
          case v: com.google.protobuf.Message => containsJvmDispatch(v)
          case vs: java.util.List[_] =>
            vs.asScala.exists {
              case v: com.google.protobuf.Message => containsJvmDispatch(v)
              case _ => false
            }
          case _ => false
        }
    }

  def namedLambdaVariable2Proto(nlv: SparkNamedLambdaVariable): Option[NamedLambdaVariable] = {
    val dataTypeProto = serializeDataType(nlv.dataType)
    if (dataTypeProto.isEmpty) {
      return None
    }
    Some(
      NamedLambdaVariable
        .newBuilder()
        .setName(nlv.name)
        .setExprId(nlv.exprId.id)
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
