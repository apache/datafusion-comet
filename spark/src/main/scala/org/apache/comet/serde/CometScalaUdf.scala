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

import org.apache.spark.sql.catalyst.expressions.{Attribute, KnownNotNull, ScalaUDF}

import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, serializeDataType}
import org.apache.comet.udf.CometUdfRegistry

/**
 * Handles serialization of Spark ScalaUDF expressions when a matching CometUDF implementation is
 * registered in [[CometUdfRegistry]]. If the UDF is not registered, falls back to Spark.
 */
object CometScalaUdf {

  def convert(
      expr: ScalaUDF,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val name = expr.udfName.getOrElse {
      withInfo(expr, "ScalaUDF has no name, cannot look up CometUDF registration")
      return None
    }

    val entry = CometUdfRegistry.get(name).getOrElse {
      withInfo(expr, s"ScalaUDF '$name' is not registered in CometUdfRegistry")
      return None
    }

    // Spark wraps UDF arguments in KnownNotNull when the UDF is declared non-nullable.
    // Unwrap these since the CometUDF handles nullability itself.
    val unwrappedChildren = expr.children.map {
      case KnownNotNull(child) => child
      case other => other
    }

    val argProtos = unwrappedChildren.map { child =>
      exprToProtoInternal(child, inputs, binding)
    }
    if (argProtos.exists(_.isEmpty)) {
      withInfo(expr, s"Failed to serialize one or more arguments for CometUDF '$name'")
      return None
    }

    val returnType = serializeDataType(entry.returnType).getOrElse {
      withInfo(expr, s"Failed to serialize return type for CometUDF '$name'")
      return None
    }

    val udfBuilder = ExprOuterClass.JvmScalarUdf
      .newBuilder()
      .setClassName(entry.className)
      .setReturnType(returnType)
      .setReturnNullable(entry.nullable)

    argProtos.foreach(proto => udfBuilder.addArgs(proto.get))

    Some(
      ExprOuterClass.Expr
        .newBuilder()
        .setJvmScalarUdf(udfBuilder.build())
        .build())
  }
}
