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

import scala.collection.JavaConverters.asJavaIterableConverter

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average}
import org.apache.spark.sql.types.{BooleanType, DataType, DateType, DecimalType, NumericType, TimestampType}

import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.serde.QueryPlanSerde.{exprToProto, serializeDataType}
import org.apache.comet.shims.ShimQueryPlanSerde

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

object CometCount extends CometAggregateExpressionSerde {
  override def convert(
      aggExpr: AggregateExpression,
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.AggExpr] = {
    val exprChildren = expr.children.map(exprToProto(_, inputs, binding))
    if (exprChildren.forall(_.isDefined)) {
      val countBuilder = ExprOuterClass.Count.newBuilder()
      countBuilder.addAllChildren(exprChildren.map(_.get).asJava)
      Some(
        ExprOuterClass.AggExpr
          .newBuilder()
          .setCount(countBuilder)
          .build())
    } else {
      withInfo(aggExpr, expr.children: _*)
      None
    }
  }
}

object CometAverage extends CometAggregateExpressionSerde with ShimQueryPlanSerde {
  override def convert(
      aggExpr: AggregateExpression,
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.AggExpr] = {
    val avg = expr.asInstanceOf[Average]

    if (!AggSerde.avgDataTypeSupported(expr.dataType)) {
      withInfo(aggExpr, s"Unsupported data type: ${expr.dataType}")
      return None
    }

    if (!isLegacyMode(avg)) {
      withInfo(aggExpr, "Average is only supported in legacy mode")
      return None
    }

    val child = avg.child
    val childExpr = exprToProto(child, inputs, binding)
    val dataType = serializeDataType(expr.dataType)

    val sumDataType = child.dataType match {
      case decimalType: DecimalType =>
        // This is input precision + 10 to be consistent with Spark
        val precision = Math.min(DecimalType.MAX_PRECISION, decimalType.precision + 10)
        val newType =
          DecimalType.apply(precision, decimalType.scale)
        serializeDataType(newType)
      case _ =>
        serializeDataType(child.dataType)
    }

    if (childExpr.isDefined && dataType.isDefined) {
      val builder = ExprOuterClass.Avg.newBuilder()
      builder.setChild(childExpr.get)
      builder.setDatatype(dataType.get)
      builder.setFailOnError(getFailOnError(avg))
      builder.setSumDatatype(sumDataType.get)

      Some(
        ExprOuterClass.AggExpr
          .newBuilder()
          .setAvg(builder)
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
      case _: NumericType =>
        // TODO: implement support for interval types
        true
      case DateType | TimestampType | BooleanType => true
      case _ => false
    }
  }

  def avgDataTypeSupported(dt: DataType): Boolean = {
    dt match {
      case _: NumericType =>
        // TODO: implement support for interval types
        true
      case _ => false
    }
  }

}
