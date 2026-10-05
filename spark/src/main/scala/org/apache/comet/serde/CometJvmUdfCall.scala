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

import org.apache.spark.sql.catalyst.expressions.Attribute

import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.serde.ExprOuterClass.Expr
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, serializeDataType}
import org.apache.comet.udf.JvmUdfCall

/**
 * Emits a `JvmScalarUdf` naming the registered [[org.apache.comet.udf.CometUDF]] class, with each
 * argument serialized as its own native expression. Unlike the codegen dispatcher, which compiles
 * the whole argument tree into its JVM kernel, only the arguments' values cross into the JVM.
 */
object CometJvmUdfCall extends CometExpressionSerde[JvmUdfCall] {

  override def convert(
      expr: JvmUdfCall,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    val args = expr.children.map(exprToProtoInternal(_, inputs, binding))
    // An argument that could not be converted has already recorded why.
    if (args.contains(None)) {
      return None
    }
    val returnType = serializeDataType(expr.dataType).getOrElse {
      withFallbackReason(expr, s"UDF '${expr.name}': unsupported return type ${expr.dataType}")
      return None
    }
    val udf = ExprOuterClass.JvmScalarUdf
      .newBuilder()
      .setClassName(expr.className)
      .setReturnType(returnType)
      .setReturnNullable(expr.nullable)
    args.flatten.foreach(udf.addArgs)
    Some(Expr.newBuilder().setJvmScalarUdf(udf.build()).build())
  }
}
