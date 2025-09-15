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

import org.apache.spark.sql.catalyst.expressions.{Attribute, EvalMode}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average, BitAndAgg, BitOrAgg, BitXorAgg, BloomFilterAggregate, CentralMomentAgg, Corr, Count, Covariance, CovPopulation, CovSample, First, Last, Max, Min, StddevPop, StddevSamp, Sum, VariancePop, VarianceSamp}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ByteType, DecimalType, IntegerType, LongType, ShortType, StringType}

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.serde.QueryPlanSerde.{exprToProto, serializeDataType}

object CometMin extends CometAggregateExpressionSerde[Min] {

  override def convert(
      aggExpr: AggregateExpression,
      expr: Min,
      inputs: Seq[Attribute],
      binding: Boolean,
      conf: SQLConf): Option[ExprOuterClass.AggExpr] = {
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

object CometMax extends CometAggregateExpressionSerde[Max] {

  override def convert(
      aggExpr: AggregateExpression,
      expr: Max,
      inputs: Seq[Attribute],
      binding: Boolean,
      conf: SQLConf): Option[ExprOuterClass.AggExpr] = {
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

object CometCount extends CometAggregateExpressionSerde[Count] {
  override def convert(
      aggExpr: AggregateExpression,
      expr: Count,
      inputs: Seq[Attribute],
      binding: Boolean,
      conf: SQLConf): Option[ExprOuterClass.AggExpr] = {
    val exprChildren = expr.children.map(exprToProto(_, inputs, binding))

    // scalastyle:off
    println(exprChildren.head.getClass)

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

object CometAverage extends CometAggregateExpressionSerde[Average] {
  override def convert(
      aggExpr: AggregateExpression,
      avg: Average,
      inputs: Seq[Attribute],
      binding: Boolean,
      conf: SQLConf): Option[ExprOuterClass.AggExpr] = {

    if (!AggSerde.avgDataTypeSupported(avg.dataType)) {
      withInfo(aggExpr, s"Unsupported data type: ${avg.dataType}")
      return None
    }

    avg.evalMode match {
      case EvalMode.ANSI if !CometConf.COMET_EXPR_ALLOW_INCOMPATIBLE.get() =>
        withInfo(
          aggExpr,
          "ANSI mode is not supported. Set " +
            s"${CometConf.COMET_EXPR_ALLOW_INCOMPATIBLE.key}=true to allow it anyway")
        return None
      case EvalMode.TRY =>
        withInfo(aggExpr, "TRY mode is not supported")
        return None
      case _ =>
      // supported
    }

    val child = avg.child
    val childExpr = exprToProto(child, inputs, binding)
    val dataType = serializeDataType(avg.dataType)

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
      builder.setFailOnError(avg.evalMode == EvalMode.ANSI)
      builder.setSumDatatype(sumDataType.get)

      Some(
        ExprOuterClass.AggExpr
          .newBuilder()
          .setAvg(builder)
          .build())
    } else if (dataType.isEmpty) {
      withInfo(aggExpr, s"datatype ${avg.dataType} is not supported", child)
      None
    } else {
      withInfo(aggExpr, child)
      None
    }
  }
}
object CometSum extends CometAggregateExpressionSerde[Sum] {
  override def convert(
      aggExpr: AggregateExpression,
      sum: Sum,
      inputs: Seq[Attribute],
      binding: Boolean,
      conf: SQLConf): Option[ExprOuterClass.AggExpr] = {

    if (!AggSerde.sumDataTypeSupported(sum.dataType)) {
      withInfo(aggExpr, s"Unsupported data type: ${sum.dataType}")
      return None
    }

    sum.evalMode match {
      case EvalMode.ANSI if !CometConf.COMET_EXPR_ALLOW_INCOMPATIBLE.get() =>
        withInfo(
          aggExpr,
          "ANSI mode is not supported. Set " +
            s"${CometConf.COMET_EXPR_ALLOW_INCOMPATIBLE.key}=true to allow it anyway")
        return None
      case EvalMode.TRY =>
        withInfo(aggExpr, "TRY mode is not supported")
        return None
      case _ =>
      // supported
    }

    val childExpr = exprToProto(sum.child, inputs, binding)
    val dataType = serializeDataType(sum.dataType)

    if (childExpr.isDefined && dataType.isDefined) {
      val builder = ExprOuterClass.Sum.newBuilder()
      builder.setChild(childExpr.get)
      builder.setDatatype(dataType.get)
      builder.setFailOnError(sum.evalMode == EvalMode.ANSI)

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

object CometFirst extends CometAggregateExpressionSerde[First] {
  override def convert(
      aggExpr: AggregateExpression,
      first: First,
      inputs: Seq[Attribute],
      binding: Boolean,
      conf: SQLConf): Option[ExprOuterClass.AggExpr] = {
    val child = first.children.head
    val childExpr = exprToProto(child, inputs, binding)
    val dataType = serializeDataType(first.dataType)

    if (childExpr.isDefined && dataType.isDefined) {
      val builder = ExprOuterClass.First.newBuilder()
      builder.setChild(childExpr.get)
      builder.setDatatype(dataType.get)
      builder.setIgnoreNulls(first.ignoreNulls)

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

object CometLast extends CometAggregateExpressionSerde[Last] {
  override def convert(
      aggExpr: AggregateExpression,
      last: Last,
      inputs: Seq[Attribute],
      binding: Boolean,
      conf: SQLConf): Option[ExprOuterClass.AggExpr] = {
    val child = last.children.head
    val childExpr = exprToProto(child, inputs, binding)
    val dataType = serializeDataType(last.dataType)

    if (childExpr.isDefined && dataType.isDefined) {
      val builder = ExprOuterClass.Last.newBuilder()
      builder.setChild(childExpr.get)
      builder.setDatatype(dataType.get)
      builder.setIgnoreNulls(last.ignoreNulls)

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

object CometBitAndAgg extends CometAggregateExpressionSerde[BitAndAgg] {
  override def convert(
      aggExpr: AggregateExpression,
      bitAnd: BitAndAgg,
      inputs: Seq[Attribute],
      binding: Boolean,
      conf: SQLConf): Option[ExprOuterClass.AggExpr] = {
    if (!AggSerde.bitwiseAggTypeSupported(bitAnd.dataType)) {
      withInfo(aggExpr, s"Unsupported data type: ${bitAnd.dataType}")
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

object CometBitOrAgg extends CometAggregateExpressionSerde[BitOrAgg] {
  override def convert(
      aggExpr: AggregateExpression,
      bitOr: BitOrAgg,
      inputs: Seq[Attribute],
      binding: Boolean,
      conf: SQLConf): Option[ExprOuterClass.AggExpr] = {
    if (!AggSerde.bitwiseAggTypeSupported(bitOr.dataType)) {
      withInfo(aggExpr, s"Unsupported data type: ${bitOr.dataType}")
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

object CometBitXOrAgg extends CometAggregateExpressionSerde[BitXorAgg] {
  override def convert(
      aggExpr: AggregateExpression,
      bitXor: BitXorAgg,
      inputs: Seq[Attribute],
      binding: Boolean,
      conf: SQLConf): Option[ExprOuterClass.AggExpr] = {
    if (!AggSerde.bitwiseAggTypeSupported(bitXor.dataType)) {
      withInfo(aggExpr, s"Unsupported data type: ${bitXor.dataType}")
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

trait CometCovBase {
  def convertCov(
      aggExpr: AggregateExpression,
      cov: Covariance,
      nullOnDivideByZero: Boolean,
      statsType: Int,
      inputs: Seq[Attribute],
      binding: Boolean,
      conf: SQLConf): Option[ExprOuterClass.AggExpr] = {
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

object CometCovSample extends CometAggregateExpressionSerde[CovSample] with CometCovBase {
  override def convert(
      aggExpr: AggregateExpression,
      covSample: CovSample,
      inputs: Seq[Attribute],
      binding: Boolean,
      conf: SQLConf): Option[ExprOuterClass.AggExpr] = {
    convertCov(
      aggExpr,
      covSample,
      covSample.nullOnDivideByZero,
      0,
      inputs,
      binding,
      conf: SQLConf)
  }
}

object CometCovPopulation extends CometAggregateExpressionSerde[CovPopulation] with CometCovBase {
  override def convert(
      aggExpr: AggregateExpression,
      covPopulation: CovPopulation,
      inputs: Seq[Attribute],
      binding: Boolean,
      conf: SQLConf): Option[ExprOuterClass.AggExpr] = {
    convertCov(
      aggExpr,
      covPopulation,
      covPopulation.nullOnDivideByZero,
      1,
      inputs,
      binding,
      conf: SQLConf)
  }
}

trait CometVariance {
  def convertVariance(
      aggExpr: AggregateExpression,
      expr: CentralMomentAgg,
      nullOnDivideByZero: Boolean,
      statsType: Int,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.AggExpr] = {
    val childExpr = exprToProto(expr.child, inputs, binding)
    val dataType = serializeDataType(expr.dataType)

    if (childExpr.isDefined && dataType.isDefined) {
      val builder = ExprOuterClass.Variance.newBuilder()
      builder.setChild(childExpr.get)
      builder.setNullOnDivideByZero(nullOnDivideByZero)
      builder.setDatatype(dataType.get)
      builder.setStatsTypeValue(statsType)

      Some(
        ExprOuterClass.AggExpr
          .newBuilder()
          .setVariance(builder)
          .build())
    } else {
      withInfo(aggExpr, expr.child)
      None
    }
  }

}

object CometVarianceSamp extends CometAggregateExpressionSerde[VarianceSamp] with CometVariance {
  override def convert(
      aggExpr: AggregateExpression,
      variance: VarianceSamp,
      inputs: Seq[Attribute],
      binding: Boolean,
      conf: SQLConf): Option[ExprOuterClass.AggExpr] = {
    convertVariance(aggExpr, variance, variance.nullOnDivideByZero, 0, inputs, binding)
  }
}

object CometVariancePop extends CometAggregateExpressionSerde[VariancePop] with CometVariance {
  override def convert(
      aggExpr: AggregateExpression,
      variance: VariancePop,
      inputs: Seq[Attribute],
      binding: Boolean,
      conf: SQLConf): Option[ExprOuterClass.AggExpr] = {
    convertVariance(aggExpr, variance, variance.nullOnDivideByZero, 1, inputs, binding)
  }
}

trait CometStddev {
  def convertStddev(
      aggExpr: AggregateExpression,
      stddev: CentralMomentAgg,
      nullOnDivideByZero: Boolean,
      statsType: Int,
      inputs: Seq[Attribute],
      binding: Boolean,
      conf: SQLConf): Option[ExprOuterClass.AggExpr] = {
    val child = stddev.child
    if (CometConf.COMET_EXPR_STDDEV_ENABLED.get(conf)) {
      val childExpr = exprToProto(child, inputs, binding)
      val dataType = serializeDataType(stddev.dataType)

      if (childExpr.isDefined && dataType.isDefined) {
        val builder = ExprOuterClass.Stddev.newBuilder()
        builder.setChild(childExpr.get)
        builder.setNullOnDivideByZero(nullOnDivideByZero)
        builder.setDatatype(dataType.get)
        builder.setStatsTypeValue(statsType)

        Some(
          ExprOuterClass.AggExpr
            .newBuilder()
            .setStddev(builder)
            .build())
      } else {
        withInfo(aggExpr, child)
        None
      }
    } else {
      withInfo(
        aggExpr,
        "stddev disabled by default because it can be slower than Spark. " +
          s"Set ${CometConf.COMET_EXPR_STDDEV_ENABLED}=true to enable it.",
        child)
      None
    }
  }
}

object CometStddevSamp extends CometAggregateExpressionSerde[StddevSamp] with CometStddev {
  override def convert(
      aggExpr: AggregateExpression,
      stddev: StddevSamp,
      inputs: Seq[Attribute],
      binding: Boolean,
      conf: SQLConf): Option[ExprOuterClass.AggExpr] = {
    convertStddev(aggExpr, stddev, stddev.nullOnDivideByZero, 0, inputs, binding, conf: SQLConf)
  }
}

object CometStddevPop extends CometAggregateExpressionSerde[StddevPop] with CometStddev {
  override def convert(
      aggExpr: AggregateExpression,
      stddev: StddevPop,
      inputs: Seq[Attribute],
      binding: Boolean,
      conf: SQLConf): Option[ExprOuterClass.AggExpr] = {
    convertStddev(aggExpr, stddev, stddev.nullOnDivideByZero, 1, inputs, binding, conf: SQLConf)
  }
}

object CometCorr extends CometAggregateExpressionSerde[Corr] {
  override def convert(
      aggExpr: AggregateExpression,
      corr: Corr,
      inputs: Seq[Attribute],
      binding: Boolean,
      conf: SQLConf): Option[ExprOuterClass.AggExpr] = {
    val child1Expr = exprToProto(corr.x, inputs, binding)
    val child2Expr = exprToProto(corr.y, inputs, binding)
    val dataType = serializeDataType(corr.dataType)

    if (child1Expr.isDefined && child2Expr.isDefined && dataType.isDefined) {
      val builder = ExprOuterClass.Correlation.newBuilder()
      builder.setChild1(child1Expr.get)
      builder.setChild2(child2Expr.get)
      builder.setNullOnDivideByZero(corr.nullOnDivideByZero)
      builder.setDatatype(dataType.get)

      Some(
        ExprOuterClass.AggExpr
          .newBuilder()
          .setCorrelation(builder)
          .build())
    } else {
      withInfo(aggExpr, corr.x, corr.y)
      None
    }
  }
}

object CometBloomFilterAggregate extends CometAggregateExpressionSerde[BloomFilterAggregate] {

  override def convert(
      aggExpr: AggregateExpression,
      bloomFilter: BloomFilterAggregate,
      inputs: Seq[Attribute],
      binding: Boolean,
      conf: SQLConf): Option[ExprOuterClass.AggExpr] = {
    // We ignore mutableAggBufferOffset and inputAggBufferOffset because they are
    // implementation details for Spark's ObjectHashAggregate.
    val childExpr = exprToProto(bloomFilter.child, inputs, binding)
    val numItemsExpr = exprToProto(bloomFilter.estimatedNumItemsExpression, inputs, binding)
    val numBitsExpr = exprToProto(bloomFilter.numBitsExpression, inputs, binding)
    val dataType = serializeDataType(bloomFilter.dataType)

    if (childExpr.isDefined &&
      (bloomFilter.child.dataType
        .isInstanceOf[ByteType] ||
        bloomFilter.child.dataType
          .isInstanceOf[ShortType] ||
        bloomFilter.child.dataType
          .isInstanceOf[IntegerType] ||
        bloomFilter.child.dataType
          .isInstanceOf[LongType] ||
        bloomFilter.child.dataType
          .isInstanceOf[StringType]) &&
      numItemsExpr.isDefined &&
      numBitsExpr.isDefined &&
      dataType.isDefined) {
      val builder = ExprOuterClass.BloomFilterAgg.newBuilder()
      builder.setChild(childExpr.get)
      builder.setNumItems(numItemsExpr.get)
      builder.setNumBits(numBitsExpr.get)
      builder.setDatatype(dataType.get)

      Some(
        ExprOuterClass.AggExpr
          .newBuilder()
          .setBloomFilterAgg(builder)
          .build())
    } else {
      withInfo(
        aggExpr,
        bloomFilter.child,
        bloomFilter.estimatedNumItemsExpression,
        bloomFilter.numBitsExpression)
      None
    }
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
