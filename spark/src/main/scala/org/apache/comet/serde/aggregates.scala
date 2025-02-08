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
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average, BitAndAgg, BitOrAgg, BitXorAgg, Covariance, CovPopulation, CovSample, First, Last, Sum}
import org.apache.spark.sql.types.DecimalType

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
      val builder = ExprOuterClass.Min.newBuilder()
      builder.setChild(childExpr.get)
      builder.setDatatype(dataType.get)

      Some(
        ExprOuterClass.AggExpr
          .newBuilder()
          .setMin(builder)
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
      val builder = ExprOuterClass.Max.newBuilder()
      builder.setChild(childExpr.get)
      builder.setDatatype(dataType.get)

      Some(
        ExprOuterClass.AggExpr
          .newBuilder()
          .setMax(builder)
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
      val builder = ExprOuterClass.Count.newBuilder()
      builder.addAllChildren(exprChildren.map(_.get).asJava)
      Some(
        ExprOuterClass.AggExpr
          .newBuilder()
          .setCount(builder)
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
object CometSum extends CometAggregateExpressionSerde with ShimQueryPlanSerde {
  override def convert(
      aggExpr: AggregateExpression,
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.AggExpr] = {
    val sum = expr.asInstanceOf[Sum]
    val childExpr = exprToProto(sum.child, inputs, binding)
    val dataType = serializeDataType(sum.dataType)

    if (childExpr.isDefined && dataType.isDefined) {
      val builder = ExprOuterClass.Sum.newBuilder()
      builder.setChild(childExpr.get)
      builder.setDatatype(dataType.get)
      builder.setFailOnError(getFailOnError(sum))

      Some(
        ExprOuterClass.AggExpr
          .newBuilder()
          .setSum(builder)
          .build())
    } else {
      if (dataType.isEmpty) {
        withInfo(aggExpr, s"datatype ${sum.dataType} is not supported", sum.child)
      } else {
        withInfo(aggExpr, sum.child)
      }
      None
    }
  }
}

object CometFirst extends CometAggregateExpressionSerde {
  override def convert(
      aggExpr: AggregateExpression,
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.AggExpr] = {
    val first = expr.asInstanceOf[First]
    if (first.ignoreNulls) {
      // DataFusion doesn't support ignoreNulls true
      withInfo(aggExpr, "First not supported when ignoreNulls=true")
      return None
    }
    val child = first.children.head
    val childExpr = exprToProto(child, inputs, binding)
    val dataType = serializeDataType(first.dataType)

    if (childExpr.isDefined && dataType.isDefined) {
      val builder = ExprOuterClass.First.newBuilder()
      builder.setChild(childExpr.get)
      builder.setDatatype(dataType.get)

      Some(
        ExprOuterClass.AggExpr
          .newBuilder()
          .setFirst(builder)
          .build())
    } else if (dataType.isEmpty) {
      withInfo(aggExpr, s"datatype ${first.dataType} is not supported", child)
      None
    } else {
      withInfo(aggExpr, child)
      None
    }
  }
}

object CometLast extends CometAggregateExpressionSerde {
  override def convert(
      aggExpr: AggregateExpression,
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.AggExpr] = {
    val last = expr.asInstanceOf[Last]
    if (last.ignoreNulls) {
      // DataFusion doesn't support ignoreNulls true
      withInfo(aggExpr, "Last not supported when ignoreNulls=true")
      return None
    }
    val child = last.children.head
    val childExpr = exprToProto(child, inputs, binding)
    val dataType = serializeDataType(last.dataType)

    if (childExpr.isDefined && dataType.isDefined) {
      val builder = ExprOuterClass.Last.newBuilder()
      builder.setChild(childExpr.get)
      builder.setDatatype(dataType.get)

      Some(
        ExprOuterClass.AggExpr
          .newBuilder()
          .setLast(builder)
          .build())
    } else if (dataType.isEmpty) {
      withInfo(aggExpr, s"datatype ${last.dataType} is not supported", child)
      None
    } else {
      withInfo(aggExpr, child)
      None
    }
  }
}

object CometBitAndAgg extends CometAggregateExpressionSerde {
  override def convert(
      aggExpr: AggregateExpression,
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.AggExpr] = {
    val bitAnd = expr.asInstanceOf[BitAndAgg]
    if (!AggSerde.bitwiseAggTypeSupported(bitAnd.dataType)) {
      withInfo(aggExpr, s"Unsupported data type: ${expr.dataType}")
      return None
    }
    val child = bitAnd.child
    val childExpr = exprToProto(child, inputs, binding)
    val dataType = serializeDataType(bitAnd.dataType)

    if (childExpr.isDefined && dataType.isDefined) {
      val builder = ExprOuterClass.BitAndAgg.newBuilder()
      builder.setChild(childExpr.get)
      builder.setDatatype(dataType.get)
      Some(
        ExprOuterClass.AggExpr
          .newBuilder()
          .setBitAndAgg(builder)
          .build())
    } else if (dataType.isEmpty) {
      withInfo(aggExpr, s"datatype ${bitAnd.dataType} is not supported", child)
      None
    } else {
      withInfo(aggExpr, child)
      None
    }
  }
}

object CometBitOrAgg extends CometAggregateExpressionSerde {
  override def convert(
      aggExpr: AggregateExpression,
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.AggExpr] = {
    val bitOr = expr.asInstanceOf[BitOrAgg]
    if (!AggSerde.bitwiseAggTypeSupported(bitOr.dataType)) {
      withInfo(aggExpr, s"Unsupported data type: ${expr.dataType}")
      return None
    }
    val child = bitOr.child
    val childExpr = exprToProto(child, inputs, binding)
    val dataType = serializeDataType(bitOr.dataType)

    if (childExpr.isDefined && dataType.isDefined) {
      val builder = ExprOuterClass.BitOrAgg.newBuilder()
      builder.setChild(childExpr.get)
      builder.setDatatype(dataType.get)
      Some(
        ExprOuterClass.AggExpr
          .newBuilder()
          .setBitOrAgg(builder)
          .build())
    } else if (dataType.isEmpty) {
      withInfo(aggExpr, s"datatype ${bitOr.dataType} is not supported", child)
      None
    } else {
      withInfo(aggExpr, child)
      None
    }
  }
}

object CometBitXOrAgg extends CometAggregateExpressionSerde {
  override def convert(
      aggExpr: AggregateExpression,
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.AggExpr] = {
    val bitXor = expr.asInstanceOf[BitXorAgg]
    if (!AggSerde.bitwiseAggTypeSupported(bitXor.dataType)) {
      withInfo(aggExpr, s"Unsupported data type: ${expr.dataType}")
      return None
    }
    val child = bitXor.child
    val childExpr = exprToProto(child, inputs, binding)
    val dataType = serializeDataType(bitXor.dataType)

    if (childExpr.isDefined && dataType.isDefined) {
      val builder = ExprOuterClass.BitXorAgg.newBuilder()
      builder.setChild(childExpr.get)
      builder.setDatatype(dataType.get)
      Some(
        ExprOuterClass.AggExpr
          .newBuilder()
          .setBitXorAgg(builder)
          .build())
    } else if (dataType.isEmpty) {
      withInfo(aggExpr, s"datatype ${bitXor.dataType} is not supported", child)
      None
    } else {
      withInfo(aggExpr, child)
      None
    }
  }
}

trait CometCovBase extends CometAggregateExpressionSerde {
  def convertCov(
      aggExpr: AggregateExpression,
      cov: Covariance,
      nullOnDivideByZero: Boolean,
      statsType: Int,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.AggExpr] = {
    val child1Expr = exprToProto(cov.left, inputs, binding)
    val child2Expr = exprToProto(cov.right, inputs, binding)
    val dataType = serializeDataType(cov.dataType)

    if (child1Expr.isDefined && child2Expr.isDefined && dataType.isDefined) {
      val builder = ExprOuterClass.Covariance.newBuilder()
      builder.setChild1(child1Expr.get)
      builder.setChild2(child2Expr.get)
      builder.setNullOnDivideByZero(nullOnDivideByZero)
      builder.setDatatype(dataType.get)
      builder.setStatsTypeValue(statsType)

      Some(
        ExprOuterClass.AggExpr
          .newBuilder()
          .setCovariance(builder)
          .build())
    } else {
      withInfo(aggExpr, "Child expression or data type not supported")
      None
    }
  }
}

object CometCovSampleAgg extends CometCovBase {
  override def convert(
      aggExpr: AggregateExpression,
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.AggExpr] = {
    val covSample = expr.asInstanceOf[CovSample]
    convertCov(aggExpr, covSample, covSample.nullOnDivideByZero, 0, inputs, binding)
  }
}

object CometCovPopulationAgg extends CometCovBase {
  override def convert(
      aggExpr: AggregateExpression,
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.AggExpr] = {
    val covSample = expr.asInstanceOf[CovPopulation]
    convertCov(aggExpr, covSample, covSample.nullOnDivideByZero, 1, inputs, binding)
  }
}

object AggSerde {
  import org.apache.spark.sql.types._

  def minMaxDataTypeSupported(dt: DataType): Boolean = {
    dt match {
      case BooleanType => true
      case ByteType | ShortType | IntegerType | LongType => true
      case FloatType | DoubleType => true
      case _: DecimalType => true
      case DateType | TimestampType => true
      case _ => false
    }
  }

  def avgDataTypeSupported(dt: DataType): Boolean = {
    dt match {
      case ByteType | ShortType | IntegerType | LongType => true
      case FloatType | DoubleType => true
      case _: DecimalType => true
      case _ => false
    }
  }

  def sumDataTypeSupported(dt: DataType): Boolean = {
    dt match {
      case ByteType | ShortType | IntegerType | LongType => true
      case FloatType | DoubleType => true
      case _: DecimalType => true
      case _ => false
    }
  }

  def bitwiseAggTypeSupported(dt: DataType): Boolean = {
    dt match {
      case ByteType | ShortType | IntegerType | LongType => true
      case _ => false
    }
  }

}
