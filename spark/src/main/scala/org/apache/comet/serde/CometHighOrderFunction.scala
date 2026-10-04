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

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, CaseWhen, Coalesce, EqualNullSafe, Expression, HigherOrderFunction, If, LambdaFunction => SparkLambdaFunction, NamedLambdaVariable => SparkNamedLambdaVariable}
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}

import org.apache.comet.CometConf
import org.apache.comet.serde.CometHighOrderFunction.{capturesComplexOuterVariable, containsJvmDispatch, hasUnsupportedNativeExpressions, namedLambdaVariable2Proto}
import org.apache.comet.serde.ExprOuterClass.{HigherOrderFunc, LambdaFunction, NamedLambdaVariable}
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, serializeDataType}

/**
 * Generic expression serializer for Spark higher-order functions (e.g. `filter`, `transform`).
 *
 * This class implements a three-tier execution hierarchy: '''Native DataFusion execution:'''
 * Attempted when `COMET_EXEC_HIGHER_ORDER_FUNCTION_NATIVE_ENABLED` is enabled and the expression
 * satisfies native execution constraints. 2. '''JVM codegen dispatch:''' Emits a `JvmScalarUdf`
 * fallback via `CometScalaUDF.emitJvmCodegenDispatch` when the native path cannot be taken,
 * provided `COMET_SCALA_UDF_CODEGEN_ENABLED` is enabled. 3. '''Vanilla Spark:''' Final fallback
 * to vanilla Spark if neither native execution nor codegen dispatch is available.
 *
 * ===Native Execution & Safety Guarantees===
 *   - '''Boolean Short-Circuiting (AND / OR):''' Handled fully natively in Rust via
 *     `ShortCircuitBinaryExpr`. It enforces strict SQL Three-Valued Logic (3VL) per-element
 *     masking via `evaluate_selection`, ensuring that stateful functions
 *     (`monotonically_increasing_id`, `rand`) and fallible operations (e.g., division by zero or
 *     out-of-bounds indexing under ANSI) are never evaluated on skipped elements.
 *   - '''Empty Batch Protection:''' The native lambda body is wrapped in `EmptyBatchGuardExpr` to
 *     short-circuit on zero-row inputs (`[]`, `NULL`), preventing scalar runtime evaluation.
 *   - '''Speculative Serialization:''' AST traversal is wrapped in a `NonFatal` catch to safely
 *     decline the native path if eager expression evaluation (e.g., `CometCast` evaluating
 *     literal arguments in unreachable branches under ANSI mode) throws an exception during plan
 *     generation.
 *
 * ===Degradation to JVM Codegen Dispatch===
 * The serializer gracefully degrades to JVM codegen dispatch under the following conditions:
 *   - '''Complex Outer Captures:''' When the lambda body captures outer attributes of nested
 *     types (`ArrayType`, `MapType`, `StructType`). This avoids quadratic memory replication in
 *     DataFusion's `take_arrays` broadcast mechanism. Scalar captures (`Int`, `String`, etc.)
 *     remain fully native.
 *   - '''Boolean Conditionals:''' When conditional expressions (`CASE WHEN`, `IF`, `COALESCE`)
 *     return `BooleanType` and serve as predicates, due to known native evaluation discrepancies
 *     in DataFusion. Scalar conditionals (e.g., `coalesce(x, 0) > 0`) remain fully native.
 *   - '''Unsupported Shapes:''' When the expression uses multi-argument lambdas with indices
 *     (e.g., `(x, i) -> ...`) or subexpressions requiring JVM dispatch (e.g., `rlike`).
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
          if (hasUnsupportedNativeExpressions(slf.function) || capturesComplexOuterVariable(
              slf)) {
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

  private def isComplexType(dt: DataType): Boolean = dt match {
    case _: ArrayType | _: MapType | _: StructType => true
    case _ => false
  }

  /**
   * Checks whether the lambda body captures any outer attributes or outer lambda variables of
   * complex types (Array, Map, Struct).
   *
   * Replicating captured complex columns via DataFusion's `take_arrays` causes quadratic memory
   * amplification. Both outer table attributes (AttributeReference) and enclosing lambda
   * variables (SparkNamedLambdaVariable) must degrade to JVM codegen dispatch.
   */
  private def capturesComplexOuterVariable(lambda: SparkLambdaFunction): Boolean = {
    val definedParamIds = lambda
      .collect { case l: SparkLambdaFunction =>
        l.arguments.map(_.exprId)
      }
      .flatten
      .toSet

    lambda.function.exists {
      case attr: AttributeReference =>
        isComplexType(attr.dataType)
      case v: SparkNamedLambdaVariable if !definedParamIds.contains(v.exprId) =>
        isComplexType(v.dataType)
      case _ => false
    }
  }

  /**
   * Checks whether the lambda body contains expressions that produce incorrect results in native
   * DataFusion execution and must be routed to JVM codegen dispatch:
   *   - Conditional expressions (CaseWhen, If, Coalesce), regardless of return type.
   *   - Null-safe equality comparisons (EqualNullSafe).
   */
  private def hasUnsupportedNativeExpressions(expr: Expression): Boolean = {
    expr.exists {
      case _: CaseWhen | _: If | _: Coalesce => true
      case _: EqualNullSafe => true
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
