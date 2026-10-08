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
import org.apache.comet.udf.NativeUdfCall

/**
 * Emits a `NativeScalarUdf` naming the registered function and the library it lives in, with each
 * argument serialized as its own native expression. Executors load the library from that path on
 * first use, so they need no registration of their own.
 */
object CometNativeUdfCall extends CometExpressionSerde[NativeUdfCall] {

  override def convert(
      expr: NativeUdfCall,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    val args = expr.children.map(exprToProtoInternal(_, inputs, binding))
    // An argument that could not be converted has already recorded why.
    if (args.contains(None)) {
      return None
    }
    val returnType = serializeDataType(expr.dataType).getOrElse {
      withFallbackReason(
        expr,
        s"native UDF '${expr.name}': unsupported return type ${expr.dataType}")
      return None
    }
    val udf = ExprOuterClass.NativeScalarUdf
      .newBuilder()
      .setName(expr.name)
      .setLibraryPath(expr.libraryPath)
      .setReturnType(returnType)
      .setDeterministic(expr.deterministic)
    args.flatten.foreach(udf.addArgs)
    Some(Expr.newBuilder().setNativeScalarUdf(udf.build()).build())
  }
}
