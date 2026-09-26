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
import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.expressions.{Abs, Add, AssertTrue, Attribute, CaseWhen, Cast, Coalesce, Divide, ElementAt, Expression, GetArrayItem, HigherOrderFunction, If, IntegralDivide, LambdaFunction => SparkLambdaFunction, Multiply, NamedLambdaVariable => SparkNamedLambdaVariable, RaiseError, Remainder, Subtract, UnaryMinus}
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf
import org.apache.comet.serde.CometHighOrderFunction.{containsJvmDispatch, hasGuardedFallibleBranch, namedLambdaVariable2Proto}
import org.apache.comet.serde.ExprOuterClass.{HigherOrderFunc, LambdaFunction, NamedLambdaVariable}
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, serializeDataType}

/**
 * Generic expression serializer for Spark higher-order functions (e.g., `filter`, `transform`).
 *
 * This class implements a three-tier execution hierarchy:
 *   1. '''Native DataFusion execution:''' Produced when
 *      `COMET_EXEC_HIGHER_ORDER_FUNCTION_NATIVE_ENABLED` is enabled and the lambda structure
 *      meets native runtime constraints. 2. '''JVM codegen dispatch:''' Emits a `JvmScalarUdf`
 *      fallback via `CometScalaUDF.emitJvmCodegenDispatch` if the native path cannot be taken,
 *      provided `COMET_SCALA_UDF_CODEGEN_ENABLED` is enabled. 3. '''Vanilla Spark:''' Final
 *      fallback if neither native DataFusion nor codegen dispatch is available.
 *
 * ===Short-Circuiting and Safety Guarantees===
 *   - '''Boolean short-circuiting (AND / OR):''' Handled natively in Rust via
 *     `ShortCircuitBinaryExpr`. It enforces strict SQL Three-Valued Logic (3VL) per-element
 *     masking via `evaluate_selection`, ensuring that stateful functions
 *     (`monotonically_increasing_id`, `rand`) and fallible operations (`DIV`, `abs`,
 *     `element_at`) are never evaluated on skipped elements.
 *   - '''Conditional expressions (CASE WHEN, IF, COALESCE):''' Guarded branches containing
 *     fallible operations are checked via [[hasGuardedFallibleBranch]] and safely routed to JVM
 *     codegen dispatch.
 *   - '''Speculative serialization:''' Lambda traversal is wrapped in a `NonFatal` catch to
 *     decline the native path if eager expression evaluation (e.g. `CometCast` evaluating literal
 *     arguments) fails during plan generation under ANSI mode.
 */
case class CometHighOrderFunction[T <: HigherOrderFunction](name: String)
    extends CometExpressionSerde[T] {

  def convert(expr: T, inputs: Seq[Attribute], binding: Boolean): Option[ExprOuterClass.Expr] = {
    if (!CometConf.COMET_EXEC_HIGHER_ORDER_FUNCTION_NATIVE_ENABLED.get()) {
      return CometScalaUDF.emitJvmCodegenDispatch(expr, inputs, binding)
    }
    val hofProto =
      try {
        highOrderFunction2Proto(expr, inputs, binding)
      } catch {
        // Speculative serialization traverses the lambda body where certain expressions
        // eagerly evaluate literal arguments (e.g., CometCast calling cast.eval()).
        // In ANSI mode, guarded branches of conditional expressions (e.g., CASE WHEN)
        // may throw during planning even though they are never reached at runtime.
        // Decline the native path cleanly and let execution fall back to JVM codegen dispatch.
        case NonFatal(_) => None
      }
    hofProto.orElse {
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
          if (hasGuardedFallibleBranch(slf.function)) {
            return None
          }
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

  def containsJvmDispatch(e: ExprOuterClass.Expr): Boolean =
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

  /**
   * Checks whether an expression can throw a runtime exception during evaluation.
   */
  private def isFallibleExpr(expr: Expression): Boolean = {
    val ansi = SQLConf.get.ansiEnabled
    expr.exists {
      case _: Divide | _: IntegralDivide | _: Remainder => true
      case c: Cast if ansi => !c.evalMode.toString.contains("TRY")
      case _: Add | _: Subtract | _: Multiply | _: UnaryMinus | _: Abs if ansi => true
      case _: GetArrayItem | _: ElementAt => true
      case _: RaiseError | _: AssertTrue => true
      case _ => false
    }
  }

  /**
   * Checks whether conditional expressions (CASE WHEN, IF, COALESCE) contain guarded fallible
   * branches that require JVM codegen fallback.
   *
   * Note: AND and OR are handled natively with strict per-element masking in Rust (via
   * StrictBooleanExpr) and do not require fallback.
   */
  def hasGuardedFallibleBranch(expr: Expression): Boolean = {
    expr.exists {
      // CASE WHEN: THEN and ELSE branches
      case CaseWhen(branches, elseValue) =>
        branches.map(_._2).exists(isFallibleExpr) || elseValue.exists(isFallibleExpr)

      // IF: true and false branches
      case If(_, trueValue, falseValue) =>
        isFallibleExpr(trueValue) || isFallibleExpr(falseValue)

      // COALESCE: tail arguments
      case Coalesce(children) if children.length > 1 =>
        children.tail.exists(isFallibleExpr)

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
