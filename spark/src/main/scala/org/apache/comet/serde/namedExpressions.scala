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

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, BindReferences, BoundReference}

import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, serializeDataType}

object CometAlias extends CometExpressionSerde[Alias] {
  override def convert(
      a: Alias,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val r = exprToProtoInternal(a.child, inputs, binding)
    if (r.isEmpty) {
      withInfo(a, a.child)
    }
    r
  }
}

object CometAttributeReference extends CometExpressionSerde[AttributeReference] {
  override def convert(
      attr: AttributeReference,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val dataType = serializeDataType(attr.dataType)

    if (dataType.isDefined) {
      if (binding) {
        // Spark may produce unresolvable attributes in some cases,
        // for example https://github.com/apache/datafusion-comet/issues/925.
        // So, we allow the binding to fail.
        val boundRef: Any = BindReferences
          .bindReference(attr, inputs, allowFailures = true)

        if (boundRef.isInstanceOf[AttributeReference]) {
          withInfo(attr, s"cannot resolve $attr among ${inputs.mkString(", ")}")
          return None
        }

        val boundExpr = ExprOuterClass.BoundReference
          .newBuilder()
          .setIndex(boundRef.asInstanceOf[BoundReference].ordinal)
          .setDatatype(dataType.get)
          .build()

        Some(
          ExprOuterClass.Expr
            .newBuilder()
            .setBound(boundExpr)
            .build())
      } else {
        val unboundRef = ExprOuterClass.UnboundReference
          .newBuilder()
          .setName(attr.name)
          .setDatatype(dataType.get)
          .build()

        Some(
          ExprOuterClass.Expr
            .newBuilder()
            .setUnbound(unboundRef)
            .build())
      }
    } else {
      withInfo(attr, s"unsupported datatype: ${attr.dataType}")
      None
    }

  }
}
