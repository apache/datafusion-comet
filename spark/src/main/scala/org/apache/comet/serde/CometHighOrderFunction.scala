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

import org.apache.spark.sql.catalyst.expressions.{Add, And, AssertTrue, Attribute, CaseWhen, Cast, Coalesce, Divide, ElementAt, Expression, GetArrayItem, HigherOrderFunction, If, IntegralDivide, LambdaFunction => SparkLambdaFunction, Multiply, NamedLambdaVariable => SparkNamedLambdaVariable, Or, RaiseError, Remainder, Subtract, UnaryMinus}
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf
import org.apache.comet.serde.CometHighOrderFunction.{containsJvmDispatch, hasGuardedFallibleBranch, namedLambdaVariable2Proto}
import org.apache.comet.serde.ExprOuterClass.{HigherOrderFunc, LambdaFunction, NamedLambdaVariable}
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, serializeDataType}

/**
 * Serializer that converts Spark higher-order functions (e.g. `filter`, `transform`, `exists`)
 * into Comet's protobuf representation.
 *
 * [[convert]] follows a three-tier execution model:
 *   1. '''Native DataFusion execution:''' Attempted when
 *      `COMET_EXEC_HIGHER_ORDER_FUNCTION_NATIVE_ENABLED` is enabled and the expression meets
 *      native constraints. 2. '''JVM codegen dispatch:''' Fallback via
 *      `CometScalaUDF.emitJvmCodegenDispatch` when the native path cannot be used, provided
 *      `COMET_SCALA_UDF_CODEGEN_ENABLED` is enabled. 3. '''Spark execution:''' Final fallback to
 *      vanilla Spark if neither native nor codegen path is viable.
 *
 * The native path is declined in favor of JVM codegen dispatch under the following conditions:
 *   - The native HOF feature flag is disabled.
 *   - In ANSI mode, the lambda body contains guarded conditional branches (`AND`, `OR`, `CASE
 *     WHEN`, `IF`, `COALESCE`) with fallible operations (division, arithmetic overflow, non-try
 *     casts, indexing). Because DataFusion's vectorized execution can evaluate unmasked batches
 *     above its selectivity threshold, it cannot guarantee Spark's strict per-element
 *     short-circuiting.
 *   - Unsupported lambda shapes (e.g. multi-argument lambdas with indices) or subexpressions that
 *     cannot be evaluated natively.
 *   - Speculative serialization of the lambda body throws a non-fatal exception during planning
 *     (e.g., eager constant evaluation of unreachable branches in `CometCast`).
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
   *
   * In ANSI mode, Spark raises runtime exceptions on operations such as integer division by zero,
   * arithmetic overflow, malformed string casts, or out-of-bounds array indexing, whereas in
   * non-ANSI mode these operations typically produce NULL.
   */
  private def isFallibleExpr(expr: Expression): Boolean = {
    val ansi = SQLConf.get.ansiEnabled
    expr.exists {
      // 1. Division by zero and remainder operations
      case _: Divide | _: IntegralDivide | _: Remainder => true

      // 2. Malformed string-to-type casts (TRY_CAST produces NULL safely)
      case c: Cast if ansi => !c.evalMode.toString.contains("TRY")

      // 3. Arithmetic operations subject to overflow in ANSI mode
      case _: Add | _: Subtract | _: Multiply | _: UnaryMinus if ansi => true

      // 4. Array indexing that throws ArrayIndexOutOfBoundsException in ANSI mode
      case _: GetArrayItem | _: ElementAt if ansi => true

      // 5. Explicit error-raising expressions
      case _: RaiseError | _: AssertTrue => true

      case _ => false
    }
  }

  /**
   * Checks whether a conditional expression contains guarded branches with fallible operations
   * that could throw runtime exceptions if evaluated natively under ANSI mode.
   *
   * Spark guarantees strict per-element short-circuiting for conditional operators (e.g., `AND`,
   * `OR`, `CASE WHEN`, `IF`, `COALESCE`). However, DataFusion uses vectorized batch evaluation
   * where binary expressions may evaluate operands unmasked over the entire batch if the
   * selectivity threshold (e.g. 20% in `BinaryExpr`) is exceeded.
   *
   * In lambda functions where batches represent individual arrays (often containing very few
   * elements, such as `[0, 1]`), a single element exceeds this threshold. Consequently,
   * DataFusion evaluates skipped branches speculatively, triggering runtime errors on elements
   * that Spark's per-element short-circuiting would protect.
   *
   * When such guarded fallible branches are detected, we decline the native path to allow
   * fallback to JVM codegen dispatch, preserving Spark's execution semantics.
   */
  def hasGuardedFallibleBranch(expr: Expression): Boolean = {
    if (!SQLConf.get.ansiEnabled) {
      false
    } else {
      expr.exists {
        // AND / OR: the right-hand branch is guarded by the left-hand condition
        case And(_, right) => isFallibleExpr(right)
        case Or(_, right) => isFallibleExpr(right)

        // CASE WHEN: all THEN branches and the ELSE branch are conditionally guarded
        case CaseWhen(branches, elseValue) =>
          branches.map(_._2).exists(isFallibleExpr) || elseValue.exists(isFallibleExpr)

        // IF(cond, trueValue, falseValue): both branches are conditionally guarded
        case If(_, trueValue, falseValue) =>
          isFallibleExpr(trueValue) || isFallibleExpr(falseValue)

        // COALESCE: arguments after the first are evaluated only when preceding ones are NULL
        case Coalesce(children) if children.length > 1 =>
          children.tail.exists(isFallibleExpr)

        case _ => false
      }
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
