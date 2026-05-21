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

import org.apache.spark.SparkEnv
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSeq, BindReferences, KnownNotNull, Literal, ScalaUDF}
import org.apache.spark.sql.types.BinaryType

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.codegen.CometBatchKernelCodegen
import org.apache.comet.serde.ExprOuterClass.Expr
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, serializeDataType}
import org.apache.comet.udf.CometUDFRegistry
import org.apache.comet.udf.codegen.CometScalaUDFCodegen

/**
 * Routes scalar `ScalaUDF` (Scala and Java UDFs) through one of two native paths.
 *
 * If the UDF name has a user-supplied [[org.apache.comet.udf.CometUDF]] registered in
 * [[CometUDFRegistry]], emit a `JvmScalarUdf` that targets the registered class directly. The
 * native side passes each argument as an Arrow vector and the user implementation produces the
 * output vector.
 *
 * Otherwise, route through the Janino codegen dispatcher. `ScalaUDF.doGenCode` emits compilable
 * Java that invokes the user function via `ctx.addReferenceObj`; the dispatcher serializes the
 * bound tree, the closure serializer carries the function reference across the wire, and the
 * Janino-compiled kernel invokes it in a tight batch loop. Gated by
 * [[CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED]]; when disabled, unregistered ScalaUDFs fall back
 * to Spark.
 *
 * Not covered by either path:
 *   - Aggregate UDFs (`ScalaAggregator`, `TypedImperativeAggregate`, legacy UDAF).
 *   - Table UDFs and generators.
 *   - Python / Pandas UDFs.
 *   - Hive `GenericUDF` / `SimpleUDF`.
 */
object CometScalaUDF extends CometExpressionSerde[ScalaUDF] {

  override def convert(expr: ScalaUDF, inputs: Seq[Attribute], binding: Boolean): Option[Expr] = {
    expr.udfName.flatMap(CometUDFRegistry.get) match {
      case Some(udfClass) =>
        return convertRegistered(expr, inputs, binding, udfClass.getName)
      case None =>
    }

    if (!CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.get()) {
      withInfo(
        expr,
        s"${CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key}=false; ScalaUDF has no native path " +
          "so the plan falls back to Spark")
      return None
    }

    // Bind against only the AttributeReferences the tree actually reads, so ordinals align with
    // the data args we ship.
    val attrs = expr.collect { case a: AttributeReference => a }.distinct
    val boundExpr = BindReferences.bindReference(expr, AttributeSeq(attrs))

    // Gate at plan time. Surface the reason via withInfo rather than crashing Janino at execute.
    CometBatchKernelCodegen.canHandle(boundExpr) match {
      case Some(reason) =>
        withInfo(expr, reason)
        return None
      case None =>
    }

    // Serialize via Spark's closure serializer: respects the task context classloader (so user
    // UDF jars are visible) and matches Spark's wire format. The bytes become arg 0 of the
    // JvmScalarUdf proto and self-describe the expression so this works in cluster mode without
    // executor-side driver registry state.
    val serializer = SparkEnv.get.closureSerializer.newInstance()
    val buffer = serializer.serialize(boundExpr)
    val bytes = new Array[Byte](buffer.remaining())
    buffer.get(bytes)
    val exprArg = exprToProtoInternal(Literal(bytes, BinaryType), inputs, binding)
      .getOrElse(return None)

    val dataArgs =
      attrs.map(a => exprToProtoInternal(a, inputs, binding).getOrElse(return None))
    val returnTypeProto = serializeDataType(expr.dataType).getOrElse(return None)

    val udfBuilder = ExprOuterClass.JvmScalarUdf
      .newBuilder()
      .setClassName(classOf[CometScalaUDFCodegen].getName)
      .addArgs(exprArg)
    dataArgs.foreach(udfBuilder.addArgs)
    udfBuilder
      .setReturnType(returnTypeProto)
      .setReturnNullable(expr.nullable)
    Some(
      ExprOuterClass.Expr
        .newBuilder()
        .setJvmScalarUdf(udfBuilder.build())
        .build())
  }

  /**
   * Build a `JvmScalarUdf` proto that targets a user-registered [[org.apache.comet.udf.CometUDF]]
   * class directly. Each ScalaUDF child becomes an `args` entry; the native side evaluates them
   * to Arrow vectors before invoking `evaluate(inputs, numRows)` on a per-task instance.
   *
   * Spark wraps UDF arguments in `KnownNotNull` when the UDF declares its inputs non-nullable;
   * strip those so the underlying expression has a Comet serde mapping.
   */
  private def convertRegistered(
      expr: ScalaUDF,
      inputs: Seq[Attribute],
      binding: Boolean,
      className: String): Option[Expr] = {
    val argProtos = expr.children.map {
      case KnownNotNull(child) => exprToProtoInternal(child, inputs, binding)
      case other => exprToProtoInternal(other, inputs, binding)
    }
    if (argProtos.exists(_.isEmpty)) {
      return None
    }
    val returnTypeProto = serializeDataType(expr.dataType).getOrElse(return None)
    val udfBuilder = ExprOuterClass.JvmScalarUdf
      .newBuilder()
      .setClassName(className)
      .setReturnType(returnTypeProto)
      .setReturnNullable(expr.nullable)
    argProtos.foreach(p => udfBuilder.addArgs(p.get))
    Some(
      ExprOuterClass.Expr
        .newBuilder()
        .setJvmScalarUdf(udfBuilder.build())
        .build())
  }
}
