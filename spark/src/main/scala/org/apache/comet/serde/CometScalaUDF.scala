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
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSeq, BindReferences, Literal, ScalaUDF}
import org.apache.spark.sql.types.BinaryType

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.codegen.CometBatchKernelCodegen
import org.apache.comet.serde.ExprOuterClass.Expr
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, serializeDataType}
import org.apache.comet.udf.codegen.CometScalaUDFCodegen

/**
 * Routes scalar `ScalaUDF` expressions (user-registered Scala and Java UDFs) through the
 * Arrow-direct codegen dispatcher. `ScalaUDF.doGenCode` emits compilable Java that invokes the
 * user function via `ctx.addReferenceObj`, so the codegen path picks it up unchanged: we
 * serialize the bound tree, the closure serializer carries the function reference across the
 * wire, and the Janino-compiled kernel loads the function and invokes it in a tight batch loop.
 *
 * Not covered here:
 *   - Aggregate UDFs (`ScalaAggregator`, `TypedImperativeAggregate`, old UDAF API) - different
 *     bridge contract.
 *   - Table UDFs (`UserDefinedTableFunction`) - generator shape; `canHandle` rejects.
 *   - Python / Pandas UDFs - different runtime.
 *   - Hive UDFs (`HiveGenericUDF` / `HiveSimpleUDF`) - separate expression classes; would need
 *     their own serde.
 *
 * Gated by [[CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED]]. When disabled, the plan falls back to
 * Spark for the enclosing operator; `ScalaUDF` has no native path so there is no in-between
 * option.
 */
object CometScalaUDF extends CometExpressionSerde[ScalaUDF] {

  override def convert(expr: ScalaUDF, inputs: Seq[Attribute], binding: Boolean): Option[Expr] = {
    if (!CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.get()) {
      withInfo(
        expr,
        s"${CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key}=false; ScalaUDF has no native path " +
          "so the plan falls back to Spark")
      return None
    }

    // Bind the tree against the set of AttributeReferences it actually reads, so the compiled
    // kernel's Spark-codegen path resolves ordinals relative to the data args we send as inputs
    // rather than the full input schema.
    val attrs = expr.collect { case a: AttributeReference => a }.distinct
    val boundExpr = BindReferences.bindReference(expr, AttributeSeq(attrs))

    // Gate on canHandle before serializing: prevents unsupported input / output shapes from
    // reaching the Janino compiler at execute time and surfaces the reason via withInfo.
    CometBatchKernelCodegen.canHandle(boundExpr) match {
      case Some(reason) =>
        withInfo(expr, reason)
        return None
      case None =>
    }

    // Serialize the bound tree via Spark's closure serializer. The serializer respects the task
    // context classloader (so user UDF jars are visible) and matches the machinery Spark uses to
    // ship closures across the wire. The bytes become arg 0 of the JvmScalarUdf proto; the
    // dispatcher identifies the expression to compile from them, which makes the path work in
    // cluster mode without executor-side driver registry state.
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
}
