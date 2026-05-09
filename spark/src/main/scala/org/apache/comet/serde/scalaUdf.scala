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

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSeq, BindReferences, ScalaUDF}

import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.serde.ExprOuterClass.Expr
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, serializeDataType}
import org.apache.comet.udf.CometCodegenDispatchUDF

/**
 * Route scalar `ScalaUDF` expressions (user-registered Scala and Java UDFs) through the
 * Arrow-direct codegen dispatcher. `ScalaUDF.doGenCode` already emits compilable Java that calls
 * the user function via `ctx.addReferenceObj`, so the codegen path reuses Spark's own machinery:
 * we serialize the bound tree, the closure serializer carries the function reference across the
 * wire, and on the executor the Janino-compiled kernel loads the function and invokes it in a
 * tight batch loop.
 *
 * Before this serde, any `ScalaUDF` in a plan forced Comet to fall back to Spark in full. Now,
 * scalar UDFs in the supported type surface keep the surrounding operators on Comet's native side
 * and replace row-by-row interpreted evaluation with batch-processed JVM execution behind a
 * single JNI hop.
 *
 * Not covered here:
 *   - Aggregate UDFs (`ScalaAggregator`, `TypedImperativeAggregate`, old UDAF API) - require a
 *     different bridge contract.
 *   - Table UDFs (`UserDefinedTableFunction`) - generator shape; `canHandle` rejects.
 *   - Python / Pandas UDFs - different runtime.
 *   - Hive UDFs (`HiveGenericUDF` / `HiveSimpleUDF`) - separate expression classes; would need
 *     their own serde.
 *
 * Mode knob: always prefer codegen in `auto`. `ScalaUDF` has no native fallback path in Comet, so
 * `mode=disabled` returns `None` and the plan falls back to Spark.
 */
object CometScalaUDF extends CometExpressionSerde[ScalaUDF] {

  override def convert(expr: ScalaUDF, inputs: Seq[Attribute], binding: Boolean): Option[Expr] = {
    CodegenDispatchSerdeHelpers.pickWithMode(
      viaCodegen = () => convertViaCodegen(expr, inputs, binding),
      // No non-codegen path exists. Disabled mode means "don't route through our dispatcher".
      // Return None so the converting caller falls back to Spark for the whole plan.
      viaNonCodegen = () => {
        withInfo(
          expr,
          "codegen dispatch disabled; ScalaUDF has no native path so the plan falls back to Spark")
        None
      },
      preferCodegenInAuto = true)
  }

  private def convertViaCodegen(
      expr: ScalaUDF,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    val attrs = expr.collect { case a: AttributeReference => a }.distinct
    val boundExpr = BindReferences.bindReference(expr, AttributeSeq(attrs))

    val exprArg = CodegenDispatchSerdeHelpers
      .serializedExpressionArg(expr, boundExpr, inputs, binding)
      .getOrElse(return None)
    val dataArgs =
      attrs.map(a => exprToProtoInternal(a, inputs, binding).getOrElse(return None))

    val returnType = serializeDataType(expr.dataType).getOrElse(return None)
    val udfBuilder = ExprOuterClass.JvmScalarUdf
      .newBuilder()
      .setClassName(classOf[CometCodegenDispatchUDF].getName)
      .addArgs(exprArg)
    dataArgs.foreach(udfBuilder.addArgs)
    udfBuilder
      .setReturnType(returnType)
      .setReturnNullable(expr.nullable)
    Some(
      ExprOuterClass.Expr
        .newBuilder()
        .setJvmScalarUdf(udfBuilder.build())
        .build())
  }
}
