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
import org.apache.spark.sql.execution.ScalarSubquery

import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.serde.QueryPlanSerde.{serializeDataType, supportedDataType}

object CometScalarSubquery extends CometExpressionSerde[ScalarSubquery] {

  override def getUnsupportedReasons(): Seq[String] = Seq(
    "Not all data types are supported for scalar subquery results")

  override def getSupportLevel(expr: ScalarSubquery): SupportLevel =
    if (supportedDataType(expr.dataType)) {
      Compatible()
    } else {
      Unsupported(Some(s"Unsupported data type: ${expr.dataType}"))
    }

  override def convert(
      expr: ScalarSubquery,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    // getSupportLevel has already screened the data type with `supportedDataType`. That is a
    // different predicate from `serializeDataType`, which can still decline, so keep this check.
    val dataType = serializeDataType(expr.dataType)
    if (dataType.isEmpty) {
      withFallbackReason(
        expr,
        s"Failed to serialize datatype ${expr.dataType} for scalar subquery")
      return None
    }

    val builder = ExprOuterClass.Subquery
      .newBuilder()
      .setId(expr.exprId.id)
      .setDatatype(dataType.get)
    Some(ExprOuterClass.Expr.newBuilder().setSubquery(builder).build())
  }
}
