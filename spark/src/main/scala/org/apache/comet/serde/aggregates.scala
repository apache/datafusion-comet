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

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.types.{BooleanType, DataType, DateType, NumericType, TimestampType}

import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.serde.QueryPlanSerde.{exprToProto, serializeDataType}

object CometMin extends CometAggregateExpressionSerde {

  override def convert(
      aggExpr: AggregateExpression,
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.AggExpr] = {
    if (!AggSerde.minMaxDataTypeSupported(expr.dataType)) {
      withInfo(aggExpr, s"Unsupported data type: ${expr.dataType}")
      return None
    }
    val child = expr.children.head
    val childExpr = exprToProto(child, inputs, binding)
    val dataType = serializeDataType(expr.dataType)

    if (childExpr.isDefined && dataType.isDefined) {
      val minBuilder = ExprOuterClass.Min.newBuilder()
      minBuilder.setChild(childExpr.get)
      minBuilder.setDatatype(dataType.get)

      Some(
        ExprOuterClass.AggExpr
          .newBuilder()
          .setMin(minBuilder)
          .build())
    } else if (dataType.isEmpty) {
      withInfo(aggExpr, s"datatype ${expr.dataType} is not supported", child)
      None
    } else {
      withInfo(aggExpr, child)
      None
    }
  }
}

object CometMax extends CometAggregateExpressionSerde {

  override def convert(
      aggExpr: AggregateExpression,
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.AggExpr] = {
    if (!AggSerde.minMaxDataTypeSupported(expr.dataType)) {
      withInfo(aggExpr, s"Unsupported data type: ${expr.dataType}")
      return None
    }
    val child = expr.children.head
    val childExpr = exprToProto(child, inputs, binding)
    val dataType = serializeDataType(expr.dataType)

    if (childExpr.isDefined && dataType.isDefined) {
      val MaxBuilder = ExprOuterClass.Max.newBuilder()
      MaxBuilder.setChild(childExpr.get)
      MaxBuilder.setDatatype(dataType.get)

      Some(
        ExprOuterClass.AggExpr
          .newBuilder()
          .setMax(MaxBuilder)
          .build())
    } else if (dataType.isEmpty) {
      withInfo(aggExpr, s"datatype ${expr.dataType} is not supported", child)
      None
    } else {
      withInfo(aggExpr, child)
      None
    }
  }
}

object AggSerde {
  def minMaxDataTypeSupported(dt: DataType): Boolean = {
    dt match {
      case _: NumericType | DateType | TimestampType | BooleanType => true
      case _ => false
    }
  }
}
