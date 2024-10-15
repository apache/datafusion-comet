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

import scala.collection.JavaConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average, BitAndAgg, BitOrAgg, BitXorAgg, Complete, Corr, Count, CovPopulation, CovSample, Final, First, Last, Max, Min, Partial, StddevPop, StddevSamp, Sum, VariancePop, VarianceSamp}
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, NormalizeNaNAndZero}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, RangePartitioning, RoundRobinPartitioning, SinglePartition}
import org.apache.spark.sql.catalyst.util.CharVarcharCodegenUtils
import org.apache.spark.sql.comet.{CometBroadcastExchangeExec, CometSinkPlaceHolder, CometSparkToColumnarExec, DecimalPrecision}
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{BroadcastQueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.aggregate.{BaseAggregateExec, HashAggregateExec, ObjectHashAggregateExec}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, HashJoin, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.{isCometScan, isSpark34Plus, withInfo}
import org.apache.comet.expressions.{CometCast, CometEvalMode, Compatible, Incompatible, RegExp, Unsupported}
import org.apache.comet.serde.ExprOuterClass.{AggExpr, DataType => ProtoDataType, Expr, ScalarFunc}
import org.apache.comet.serde.ExprOuterClass.DataType.{DataTypeInfo, DecimalInfo, ListInfo, MapInfo, StructInfo}
import org.apache.comet.serde.OperatorOuterClass.{AggregateMode => CometAggregateMode, BuildSide, JoinType, Operator}
import org.apache.comet.shims.CometExprShim
import org.apache.comet.shims.ShimQueryPlanSerde

/**
 * An utility object for query plan and expression serialization.
 */
object QueryPlanSerde extends Logging with ShimQueryPlanSerde with CometExprShim {
  def emitWarning(reason: String): Unit = {
    logWarning(s"Comet native execution is disabled due to: $reason")
  }

  def supportedDataType(dt: DataType, allowStruct: Boolean = false): Boolean = dt match {
    case _: ByteType | _: ShortType | _: IntegerType | _: LongType | _: FloatType |
        _: DoubleType | _: StringType | _: BinaryType | _: TimestampType | _: DecimalType |
        _: DateType | _: BooleanType | _: NullType =>
      true
    case dt if isTimestampNTZType(dt) => true
    case s: StructType if allowStruct =>
      s.fields.map(_.dataType).forall(supportedDataType(_, allowStruct))
    case dt =>
      emitWarning(s"unsupported Spark data type: $dt")
      false
  }

  /**
   * Serializes Spark datatype to protobuf. Note that, a datatype can be serialized by this method
   * doesn't mean it is supported by Comet native execution, i.e., `supportedDataType` may return
   * false for it.
   */
  def serializeDataType(dt: DataType): Option[ExprOuterClass.DataType] = {
    val typeId = dt match {
      case _: BooleanType => 0
      case _: ByteType => 1
      case _: ShortType => 2
      case _: IntegerType => 3
      case _: LongType => 4
      case _: FloatType => 5
      case _: DoubleType => 6
      case _: StringType => 7
      case _: BinaryType => 8
      case _: TimestampType => 9
      case _: DecimalType => 10
      case dt if isTimestampNTZType(dt) => 11
      case _: DateType => 12
      case _: NullType => 13
      case _: ArrayType => 14
      case _: MapType => 15
      case _: StructType => 16
      case dt =>
        emitWarning(s"Cannot serialize Spark data type: $dt")
        return None
    }

    val builder = ProtoDataType.newBuilder()
    builder.setTypeIdValue(typeId)

    // Decimal
    val dataType = dt match {
      case t: DecimalType =>
        val info = DataTypeInfo.newBuilder()
        val decimal = DecimalInfo.newBuilder()
        decimal.setPrecision(t.precision)
        decimal.setScale(t.scale)
        info.setDecimal(decimal)
        builder.setTypeInfo(info.build()).build()

      case a: ArrayType =>
        val elementType = serializeDataType(a.elementType)

        if (elementType.isEmpty) {
          return None
        }

        val info = DataTypeInfo.newBuilder()
        val list = ListInfo.newBuilder()
        list.setElementType(elementType.get)
        list.setContainsNull(a.containsNull)

        info.setList(list)
        builder.setTypeInfo(info.build()).build()

      case m: MapType =>
        val keyType = serializeDataType(m.keyType)
        if (keyType.isEmpty) {
          return None
        }

        val valueType = serializeDataType(m.valueType)
        if (valueType.isEmpty) {
          return None
        }

        val info = DataTypeInfo.newBuilder()
        val map = MapInfo.newBuilder()
        map.setKeyType(keyType.get)
        map.setValueType(valueType.get)
        map.setValueContainsNull(m.valueContainsNull)

        info.setMap(map)
        builder.setTypeInfo(info.build()).build()

      case s: StructType =>
        val info = DataTypeInfo.newBuilder()
        val struct = StructInfo.newBuilder()

        val fieldNames = s.fields.map(_.name).toIterable.asJava
        val fieldDatatypes = s.fields.map(f => serializeDataType(f.dataType)).toSeq
        val fieldNullable = s.fields.map(f => Boolean.box(f.nullable)).toIterable.asJava

        if (fieldDatatypes.exists(_.isEmpty)) {
          return None
        }

        struct.addAllFieldNames(fieldNames)
        struct.addAllFieldDatatypes(fieldDatatypes.map(_.get).asJava)
        struct.addAllFieldNullable(fieldNullable)

        info.setStruct(struct)
        builder.setTypeInfo(info.build()).build()
      case _ => builder.build()
    }

    Some(dataType)
  }

  private def sumDataTypeSupported(dt: DataType): Boolean = {
    dt match {
      case _: NumericType => true
      case _ => false
    }
  }

  private def avgDataTypeSupported(dt: DataType): Boolean = {
    dt match {
      case _: NumericType => true
      // TODO: implement support for interval types
      case _ => false
    }
  }

  private def minMaxDataTypeSupported(dt: DataType): Boolean = {
    dt match {
      case _: NumericType | DateType | TimestampType | BooleanType => true
      case _ => false
    }
  }

  private def bitwiseAggTypeSupported(dt: DataType): Boolean = {
    dt match {
      case _: IntegerType | LongType | ShortType | ByteType => true
      case _ => false
    }
  }

  def windowExprToProto(
      windowExpr: WindowExpression,
      output: Seq[Attribute],
      conf: SQLConf): Option[OperatorOuterClass.WindowExpr] = {

    val aggregateExpressions: Array[AggregateExpression] = windowExpr.flatMap { expr =>
      expr match {
        case agg: AggregateExpression =>
          agg.aggregateFunction match {
            case _: Count =>
              Some(agg)
            case min: Min =>
              if (minMaxDataTypeSupported(min.dataType)) {
                Some(agg)
              } else {
                withInfo(windowExpr, s"datatype ${min.dataType} is not supported", expr)
                None
              }
            case max: Max =>
              if (minMaxDataTypeSupported(max.dataType)) {
                Some(agg)
              } else {
                withInfo(windowExpr, s"datatype ${max.dataType} is not supported", expr)
                None
              }
            case s: Sum =>
              if (sumDataTypeSupported(s.dataType) && !s.dataType.isInstanceOf[DecimalType]) {
                Some(agg)
              } else {
                withInfo(windowExpr, s"datatype ${s.dataType} is not supported", expr)
                None
              }
            case _ =>
              withInfo(
                windowExpr,
                s"aggregate ${agg.aggregateFunction}" +
                  " is not supported for window function",
                expr)
              None
          }
        case _ =>
          None
      }
    }.toArray

    val (aggExpr, builtinFunc) = if (aggregateExpressions.nonEmpty) {
      val modes = aggregateExpressions.map(_.mode).distinct
      assert(modes.size == 1 && modes.head == Complete)
      (aggExprToProto(aggregateExpressions.head, output, true, conf), None)
    } else {
      (None, exprToProto(windowExpr.windowFunction, output))
    }

    if (aggExpr.isEmpty && builtinFunc.isEmpty) {
      return None
    }

    val f = windowExpr.windowSpec.frameSpecification

    val (frameType, lowerBound, upperBound) = f match {
      case SpecifiedWindowFrame(frameType, lBound, uBound) =>
        val frameProto = frameType match {
          case RowFrame => OperatorOuterClass.WindowFrameType.Rows
          case RangeFrame => OperatorOuterClass.WindowFrameType.Range
        }

        val lBoundProto = lBound match {
          case UnboundedPreceding =>
            OperatorOuterClass.LowerWindowFrameBound
              .newBuilder()
              .setUnboundedPreceding(OperatorOuterClass.UnboundedPreceding.newBuilder().build())
              .build()
          case CurrentRow =>
            OperatorOuterClass.LowerWindowFrameBound
              .newBuilder()
              .setCurrentRow(OperatorOuterClass.CurrentRow.newBuilder().build())
              .build()
          case e =>
            val offset = e.eval() match {
              case i: Integer => i.toLong
              case l: Long => l
              case _ => return None
            }
            OperatorOuterClass.LowerWindowFrameBound
              .newBuilder()
              .setPreceding(
                OperatorOuterClass.Preceding
                  .newBuilder()
                  .setOffset(offset)
                  .build())
              .build()
        }

        val uBoundProto = uBound match {
          case UnboundedFollowing =>
            OperatorOuterClass.UpperWindowFrameBound
              .newBuilder()
              .setUnboundedFollowing(OperatorOuterClass.UnboundedFollowing.newBuilder().build())
              .build()
          case CurrentRow =>
            OperatorOuterClass.UpperWindowFrameBound
              .newBuilder()
              .setCurrentRow(OperatorOuterClass.CurrentRow.newBuilder().build())
              .build()
          case e =>
            val offset = e.eval() match {
              case i: Integer => i.toLong
              case l: Long => l
              case _ => return None
            }

            OperatorOuterClass.UpperWindowFrameBound
              .newBuilder()
              .setFollowing(
                OperatorOuterClass.Following
                  .newBuilder()
                  .setOffset(offset)
                  .build())
              .build()
        }

        (frameProto, lBoundProto, uBoundProto)
      case _ =>
        (
          OperatorOuterClass.WindowFrameType.Rows,
          OperatorOuterClass.LowerWindowFrameBound
            .newBuilder()
            .setUnboundedPreceding(OperatorOuterClass.UnboundedPreceding.newBuilder().build())
            .build(),
          OperatorOuterClass.UpperWindowFrameBound
            .newBuilder()
            .setUnboundedFollowing(OperatorOuterClass.UnboundedFollowing.newBuilder().build())
            .build())
    }

    val frame = OperatorOuterClass.WindowFrame
      .newBuilder()
      .setFrameType(frameType)
      .setLowerBound(lowerBound)
      .setUpperBound(upperBound)
      .build()

    val spec =
      OperatorOuterClass.WindowSpecDefinition.newBuilder().setFrameSpecification(frame).build()

    if (builtinFunc.isDefined) {
      Some(
        OperatorOuterClass.WindowExpr
          .newBuilder()
          .setBuiltInWindowFunction(builtinFunc.get)
          .setSpec(spec)
          .build())
    } else if (aggExpr.isDefined) {
      Some(
        OperatorOuterClass.WindowExpr
          .newBuilder()
          .setAggFunc(aggExpr.get)
          .setSpec(spec)
          .build())
    } else {
      None
    }
  }

  def aggExprToProto(
      aggExpr: AggregateExpression,
      inputs: Seq[Attribute],
      binding: Boolean,
      conf: SQLConf): Option[AggExpr] = {
    aggExpr.aggregateFunction match {
      case s @ Sum(child, _) if sumDataTypeSupported(s.dataType) && isLegacyMode(s) =>
        val childExpr = exprToProto(child, inputs, binding)
        val dataType = serializeDataType(s.dataType)

        if (childExpr.isDefined && dataType.isDefined) {
          val sumBuilder = ExprOuterClass.Sum.newBuilder()
          sumBuilder.setChild(childExpr.get)
          sumBuilder.setDatatype(dataType.get)
          sumBuilder.setFailOnError(getFailOnError(s))

          Some(
            ExprOuterClass.AggExpr
              .newBuilder()
              .setSum(sumBuilder)
              .build())
        } else {
          if (dataType.isEmpty) {
            withInfo(aggExpr, s"datatype ${s.dataType} is not supported", child)
          } else {
            withInfo(aggExpr, child)
          }
          None
        }
      case s @ Average(child, _) if avgDataTypeSupported(s.dataType) && isLegacyMode(s) =>
        val childExpr = exprToProto(child, inputs, binding)
        val dataType = serializeDataType(s.dataType)

        val sumDataType = if (child.dataType.isInstanceOf[DecimalType]) {

          // This is input precision + 10 to be consistent with Spark
          val precision = Math.min(
            DecimalType.MAX_PRECISION,
            child.dataType.asInstanceOf[DecimalType].precision + 10)
          val newType =
            DecimalType.apply(precision, child.dataType.asInstanceOf[DecimalType].scale)
          serializeDataType(newType)
        } else {
          serializeDataType(child.dataType)
        }

        if (childExpr.isDefined && dataType.isDefined) {
          val builder = ExprOuterClass.Avg.newBuilder()
          builder.setChild(childExpr.get)
          builder.setDatatype(dataType.get)
          builder.setFailOnError(getFailOnError(s))
          builder.setSumDatatype(sumDataType.get)

          Some(
            ExprOuterClass.AggExpr
              .newBuilder()
              .setAvg(builder)
              .build())
        } else if (dataType.isEmpty) {
          withInfo(aggExpr, s"datatype ${s.dataType} is not supported", child)
          None
        } else {
          withInfo(aggExpr, child)
          None
        }
      case Count(children) =>
        val exprChildren = children.map(exprToProto(_, inputs, binding))

        if (exprChildren.forall(_.isDefined)) {
          val countBuilder = ExprOuterClass.Count.newBuilder()
          countBuilder.addAllChildren(exprChildren.map(_.get).asJava)

          Some(
            ExprOuterClass.AggExpr
              .newBuilder()
              .setCount(countBuilder)
              .build())
        } else {
          withInfo(aggExpr, children: _*)
          None
        }
      case min @ Min(child) if minMaxDataTypeSupported(min.dataType) =>
        val childExpr = exprToProto(child, inputs, binding)
        val dataType = serializeDataType(min.dataType)

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
          withInfo(aggExpr, s"datatype ${min.dataType} is not supported", child)
          None
        } else {
          withInfo(aggExpr, child)
          None
        }
      case max @ Max(child) if minMaxDataTypeSupported(max.dataType) =>
        val childExpr = exprToProto(child, inputs, binding)
        val dataType = serializeDataType(max.dataType)

        if (childExpr.isDefined && dataType.isDefined) {
          val maxBuilder = ExprOuterClass.Max.newBuilder()
          maxBuilder.setChild(childExpr.get)
          maxBuilder.setDatatype(dataType.get)

          Some(
            ExprOuterClass.AggExpr
              .newBuilder()
              .setMax(maxBuilder)
              .build())
        } else if (dataType.isEmpty) {
          withInfo(aggExpr, s"datatype ${max.dataType} is not supported", child)
          None
        } else {
          withInfo(aggExpr, child)
          None
        }
      case first @ First(child, ignoreNulls)
          if !ignoreNulls => // DataFusion doesn't support ignoreNulls true
        val childExpr = exprToProto(child, inputs, binding)
        val dataType = serializeDataType(first.dataType)

        if (childExpr.isDefined && dataType.isDefined) {
          val firstBuilder = ExprOuterClass.First.newBuilder()
          firstBuilder.setChild(childExpr.get)
          firstBuilder.setDatatype(dataType.get)

          Some(
            ExprOuterClass.AggExpr
              .newBuilder()
              .setFirst(firstBuilder)
              .build())
        } else if (dataType.isEmpty) {
          withInfo(aggExpr, s"datatype ${first.dataType} is not supported", child)
          None
        } else {
          withInfo(aggExpr, child)
          None
        }
      case last @ Last(child, ignoreNulls)
          if !ignoreNulls => // DataFusion doesn't support ignoreNulls true
        val childExpr = exprToProto(child, inputs, binding)
        val dataType = serializeDataType(last.dataType)

        if (childExpr.isDefined && dataType.isDefined) {
          val lastBuilder = ExprOuterClass.Last.newBuilder()
          lastBuilder.setChild(childExpr.get)
          lastBuilder.setDatatype(dataType.get)

          Some(
            ExprOuterClass.AggExpr
              .newBuilder()
              .setLast(lastBuilder)
              .build())
        } else if (dataType.isEmpty) {
          withInfo(aggExpr, s"datatype ${last.dataType} is not supported", child)
          None
        } else {
          withInfo(aggExpr, child)
          None
        }
      case bitAnd @ BitAndAgg(child) if bitwiseAggTypeSupported(bitAnd.dataType) =>
        val childExpr = exprToProto(child, inputs, binding)
        val dataType = serializeDataType(bitAnd.dataType)

        if (childExpr.isDefined && dataType.isDefined) {
          val bitAndBuilder = ExprOuterClass.BitAndAgg.newBuilder()
          bitAndBuilder.setChild(childExpr.get)
          bitAndBuilder.setDatatype(dataType.get)

          Some(
            ExprOuterClass.AggExpr
              .newBuilder()
              .setBitAndAgg(bitAndBuilder)
              .build())
        } else if (dataType.isEmpty) {
          withInfo(aggExpr, s"datatype ${bitAnd.dataType} is not supported", child)
          None
        } else {
          withInfo(aggExpr, child)
          None
        }
      case bitOr @ BitOrAgg(child) if bitwiseAggTypeSupported(bitOr.dataType) =>
        val childExpr = exprToProto(child, inputs, binding)
        val dataType = serializeDataType(bitOr.dataType)

        if (childExpr.isDefined && dataType.isDefined) {
          val bitOrBuilder = ExprOuterClass.BitOrAgg.newBuilder()
          bitOrBuilder.setChild(childExpr.get)
          bitOrBuilder.setDatatype(dataType.get)

          Some(
            ExprOuterClass.AggExpr
              .newBuilder()
              .setBitOrAgg(bitOrBuilder)
              .build())
        } else if (dataType.isEmpty) {
          withInfo(aggExpr, s"datatype ${bitOr.dataType} is not supported", child)
          None
        } else {
          withInfo(aggExpr, child)
          None
        }
      case bitXor @ BitXorAgg(child) if bitwiseAggTypeSupported(bitXor.dataType) =>
        val childExpr = exprToProto(child, inputs, binding)
        val dataType = serializeDataType(bitXor.dataType)

        if (childExpr.isDefined && dataType.isDefined) {
          val bitXorBuilder = ExprOuterClass.BitXorAgg.newBuilder()
          bitXorBuilder.setChild(childExpr.get)
          bitXorBuilder.setDatatype(dataType.get)

          Some(
            ExprOuterClass.AggExpr
              .newBuilder()
              .setBitXorAgg(bitXorBuilder)
              .build())
        } else if (dataType.isEmpty) {
          withInfo(aggExpr, s"datatype ${bitXor.dataType} is not supported", child)
          None
        } else {
          withInfo(aggExpr, child)
          None
        }
      case cov @ CovSample(child1, child2, nullOnDivideByZero) =>
        val child1Expr = exprToProto(child1, inputs, binding)
        val child2Expr = exprToProto(child2, inputs, binding)
        val dataType = serializeDataType(cov.dataType)

        if (child1Expr.isDefined && child2Expr.isDefined && dataType.isDefined) {
          val covBuilder = ExprOuterClass.Covariance.newBuilder()
          covBuilder.setChild1(child1Expr.get)
          covBuilder.setChild2(child2Expr.get)
          covBuilder.setNullOnDivideByZero(nullOnDivideByZero)
          covBuilder.setDatatype(dataType.get)
          covBuilder.setStatsTypeValue(0)

          Some(
            ExprOuterClass.AggExpr
              .newBuilder()
              .setCovariance(covBuilder)
              .build())
        } else {
          None
        }
      case cov @ CovPopulation(child1, child2, nullOnDivideByZero) =>
        val child1Expr = exprToProto(child1, inputs, binding)
        val child2Expr = exprToProto(child2, inputs, binding)
        val dataType = serializeDataType(cov.dataType)

        if (child1Expr.isDefined && child2Expr.isDefined && dataType.isDefined) {
          val covBuilder = ExprOuterClass.Covariance.newBuilder()
          covBuilder.setChild1(child1Expr.get)
          covBuilder.setChild2(child2Expr.get)
          covBuilder.setNullOnDivideByZero(nullOnDivideByZero)
          covBuilder.setDatatype(dataType.get)
          covBuilder.setStatsTypeValue(1)

          Some(
            ExprOuterClass.AggExpr
              .newBuilder()
              .setCovariance(covBuilder)
              .build())
        } else {
          None
        }
      case variance @ VarianceSamp(child, nullOnDivideByZero) =>
        val childExpr = exprToProto(child, inputs, binding)
        val dataType = serializeDataType(variance.dataType)

        if (childExpr.isDefined && dataType.isDefined) {
          val varBuilder = ExprOuterClass.Variance.newBuilder()
          varBuilder.setChild(childExpr.get)
          varBuilder.setNullOnDivideByZero(nullOnDivideByZero)
          varBuilder.setDatatype(dataType.get)
          varBuilder.setStatsTypeValue(0)

          Some(
            ExprOuterClass.AggExpr
              .newBuilder()
              .setVariance(varBuilder)
              .build())
        } else {
          withInfo(aggExpr, child)
          None
        }
      case variancePop @ VariancePop(child, nullOnDivideByZero) =>
        val childExpr = exprToProto(child, inputs, binding)
        val dataType = serializeDataType(variancePop.dataType)

        if (childExpr.isDefined && dataType.isDefined) {
          val varBuilder = ExprOuterClass.Variance.newBuilder()
          varBuilder.setChild(childExpr.get)
          varBuilder.setNullOnDivideByZero(nullOnDivideByZero)
          varBuilder.setDatatype(dataType.get)
          varBuilder.setStatsTypeValue(1)

          Some(
            ExprOuterClass.AggExpr
              .newBuilder()
              .setVariance(varBuilder)
              .build())
        } else {
          withInfo(aggExpr, child)
          None
        }

      case std @ StddevSamp(child, nullOnDivideByZero) =>
        if (CometConf.COMET_EXPR_STDDEV_ENABLED.get(conf)) {
          val childExpr = exprToProto(child, inputs, binding)
          val dataType = serializeDataType(std.dataType)

          if (childExpr.isDefined && dataType.isDefined) {
            val stdBuilder = ExprOuterClass.Stddev.newBuilder()
            stdBuilder.setChild(childExpr.get)
            stdBuilder.setNullOnDivideByZero(nullOnDivideByZero)
            stdBuilder.setDatatype(dataType.get)
            stdBuilder.setStatsTypeValue(0)

            Some(
              ExprOuterClass.AggExpr
                .newBuilder()
                .setStddev(stdBuilder)
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

      case std @ StddevPop(child, nullOnDivideByZero) =>
        if (CometConf.COMET_EXPR_STDDEV_ENABLED.get(conf)) {
          val childExpr = exprToProto(child, inputs, binding)
          val dataType = serializeDataType(std.dataType)

          if (childExpr.isDefined && dataType.isDefined) {
            val stdBuilder = ExprOuterClass.Stddev.newBuilder()
            stdBuilder.setChild(childExpr.get)
            stdBuilder.setNullOnDivideByZero(nullOnDivideByZero)
            stdBuilder.setDatatype(dataType.get)
            stdBuilder.setStatsTypeValue(1)

            Some(
              ExprOuterClass.AggExpr
                .newBuilder()
                .setStddev(stdBuilder)
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

      case corr @ Corr(child1, child2, nullOnDivideByZero) =>
        val child1Expr = exprToProto(child1, inputs, binding)
        val child2Expr = exprToProto(child2, inputs, binding)
        val dataType = serializeDataType(corr.dataType)

        if (child1Expr.isDefined && child2Expr.isDefined && dataType.isDefined) {
          val corrBuilder = ExprOuterClass.Correlation.newBuilder()
          corrBuilder.setChild1(child1Expr.get)
          corrBuilder.setChild2(child2Expr.get)
          corrBuilder.setNullOnDivideByZero(nullOnDivideByZero)
          corrBuilder.setDatatype(dataType.get)

          Some(
            ExprOuterClass.AggExpr
              .newBuilder()
              .setCorrelation(corrBuilder)
              .build())
        } else {
          withInfo(aggExpr, child1, child2)
          None
        }
      case fn =>
        val msg = s"unsupported Spark aggregate function: ${fn.prettyName}"
        emitWarning(msg)
        withInfo(aggExpr, msg, fn.children: _*)
        None
    }
  }

  def evalModeToProto(evalMode: CometEvalMode.Value): ExprOuterClass.EvalMode = {
    evalMode match {
      case CometEvalMode.LEGACY => ExprOuterClass.EvalMode.LEGACY
      case CometEvalMode.TRY => ExprOuterClass.EvalMode.TRY
      case CometEvalMode.ANSI => ExprOuterClass.EvalMode.ANSI
      case _ => throw new IllegalStateException(s"Invalid evalMode $evalMode")
    }
  }

  /**
   * Convert a Spark expression to protobuf.
   *
   * @param expr
   *   The input expression
   * @param inputs
   *   The input attributes
   * @param binding
   *   Whether to bind the expression to the input attributes
   * @return
   *   The protobuf representation of the expression, or None if the expression is not supported
   */
  def exprToProto(
      expr: Expression,
      input: Seq[Attribute],
      binding: Boolean = true): Option[Expr] = {
    def castToProto(
        timeZoneId: Option[String],
        dt: DataType,
        childExpr: Option[Expr],
        evalMode: CometEvalMode.Value): Option[Expr] = {
      val dataType = serializeDataType(dt)

      if (childExpr.isDefined && dataType.isDefined) {
        val castBuilder = ExprOuterClass.Cast.newBuilder()
        castBuilder.setChild(childExpr.get)
        castBuilder.setDatatype(dataType.get)
        castBuilder.setEvalMode(evalModeToProto(evalMode))
        castBuilder.setAllowIncompat(CometConf.COMET_CAST_ALLOW_INCOMPATIBLE.get())
        val timeZone = timeZoneId.getOrElse("UTC")
        castBuilder.setTimezone(timeZone)

        Some(
          ExprOuterClass.Expr
            .newBuilder()
            .setCast(castBuilder)
            .build())
      } else {
        if (!dataType.isDefined) {
          withInfo(expr, s"Unsupported datatype ${dt}")
        } else {
          withInfo(expr, s"Unsupported expression $childExpr")
        }
        None
      }
    }

    def exprToProtoInternal(expr: Expression, inputs: Seq[Attribute]): Option[Expr] = {
      SQLConf.get

      def handleCast(
          child: Expression,
          inputs: Seq[Attribute],
          dt: DataType,
          timeZoneId: Option[String],
          evalMode: CometEvalMode.Value): Option[Expr] = {

        val childExpr = exprToProtoInternal(child, inputs)
        if (childExpr.isDefined) {
          val castSupport =
            CometCast.isSupported(child.dataType, dt, timeZoneId, evalMode)

          def getIncompatMessage(reason: Option[String]): String =
            "Comet does not guarantee correct results for cast " +
              s"from ${child.dataType} to $dt " +
              s"with timezone $timeZoneId and evalMode $evalMode" +
              reason.map(str => s" ($str)").getOrElse("")

          castSupport match {
            case Compatible(_) =>
              castToProto(timeZoneId, dt, childExpr, evalMode)
            case Incompatible(reason) =>
              if (CometConf.COMET_CAST_ALLOW_INCOMPATIBLE.get()) {
                logWarning(getIncompatMessage(reason))
                castToProto(timeZoneId, dt, childExpr, evalMode)
              } else {
                withInfo(
                  expr,
                  s"${getIncompatMessage(reason)}. To enable all incompatible casts, set " +
                    s"${CometConf.COMET_CAST_ALLOW_INCOMPATIBLE.key}=true")
                None
              }
            case Unsupported =>
              withInfo(
                expr,
                s"Unsupported cast from ${child.dataType} to $dt " +
                  s"with timezone $timeZoneId and evalMode $evalMode")
              None
          }
        } else {
          withInfo(expr, child)
          None
        }
      }

      expr match {
        case a @ Alias(_, _) =>
          val r = exprToProtoInternal(a.child, inputs)
          if (r.isEmpty) {
            withInfo(expr, a.child)
          }
          r

        case cast @ Cast(_: Literal, dataType, _, _) =>
          // This can happen after promoting decimal precisions
          val value = cast.eval()
          exprToProtoInternal(Literal(value, dataType), inputs)

        case UnaryExpression(child) if expr.prettyName == "trycast" =>
          val timeZoneId = SQLConf.get.sessionLocalTimeZone
          handleCast(child, inputs, expr.dataType, Some(timeZoneId), CometEvalMode.TRY)

        case c @ Cast(child, dt, timeZoneId, _) =>
          handleCast(child, inputs, dt, timeZoneId, evalMode(c))

        case add @ Add(left, right, _) if supportedDataType(left.dataType) =>
          val leftExpr = exprToProtoInternal(left, inputs)
          val rightExpr = exprToProtoInternal(right, inputs)

          if (leftExpr.isDefined && rightExpr.isDefined) {
            val addBuilder = ExprOuterClass.Add.newBuilder()
            addBuilder.setLeft(leftExpr.get)
            addBuilder.setRight(rightExpr.get)
            addBuilder.setFailOnError(getFailOnError(add))
            serializeDataType(add.dataType).foreach { t =>
              addBuilder.setReturnType(t)
            }

            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setAdd(addBuilder)
                .build())
          } else {
            withInfo(add, left, right)
            None
          }

        case add @ Add(left, _, _) if !supportedDataType(left.dataType) =>
          withInfo(add, s"Unsupported datatype ${left.dataType}")
          None

        case sub @ Subtract(left, right, _) if supportedDataType(left.dataType) =>
          val leftExpr = exprToProtoInternal(left, inputs)
          val rightExpr = exprToProtoInternal(right, inputs)

          if (leftExpr.isDefined && rightExpr.isDefined) {
            val builder = ExprOuterClass.Subtract.newBuilder()
            builder.setLeft(leftExpr.get)
            builder.setRight(rightExpr.get)
            builder.setFailOnError(getFailOnError(sub))
            serializeDataType(sub.dataType).foreach { t =>
              builder.setReturnType(t)
            }

            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setSubtract(builder)
                .build())
          } else {
            withInfo(sub, left, right)
            None
          }

        case sub @ Subtract(left, _, _) if !supportedDataType(left.dataType) =>
          withInfo(sub, s"Unsupported datatype ${left.dataType}")
          None

        case mul @ Multiply(left, right, _)
            if supportedDataType(left.dataType) && !decimalBeforeSpark34(left.dataType) =>
          val leftExpr = exprToProtoInternal(left, inputs)
          val rightExpr = exprToProtoInternal(right, inputs)

          if (leftExpr.isDefined && rightExpr.isDefined) {
            val builder = ExprOuterClass.Multiply.newBuilder()
            builder.setLeft(leftExpr.get)
            builder.setRight(rightExpr.get)
            builder.setFailOnError(getFailOnError(mul))
            serializeDataType(mul.dataType).foreach { t =>
              builder.setReturnType(t)
            }

            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setMultiply(builder)
                .build())
          } else {
            withInfo(mul, left, right)
            None
          }

        case mul @ Multiply(left, _, _) =>
          if (!supportedDataType(left.dataType)) {
            withInfo(mul, s"Unsupported datatype ${left.dataType}")
          }
          if (decimalBeforeSpark34(left.dataType)) {
            withInfo(mul, "Decimal support requires Spark 3.4 or later")
          }
          None

        case div @ Divide(left, right, _)
            if supportedDataType(left.dataType) && !decimalBeforeSpark34(left.dataType) =>
          val leftExpr = exprToProtoInternal(left, inputs)
          // Datafusion now throws an exception for dividing by zero
          // See https://github.com/apache/arrow-datafusion/pull/6792
          // For now, use NullIf to swap zeros with nulls.
          val rightExpr = exprToProtoInternal(nullIfWhenPrimitive(right), inputs)

          if (leftExpr.isDefined && rightExpr.isDefined) {
            val builder = ExprOuterClass.Divide.newBuilder()
            builder.setLeft(leftExpr.get)
            builder.setRight(rightExpr.get)
            builder.setFailOnError(getFailOnError(div))
            serializeDataType(div.dataType).foreach { t =>
              builder.setReturnType(t)
            }

            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setDivide(builder)
                .build())
          } else {
            withInfo(div, left, right)
            None
          }
        case div @ Divide(left, _, _) =>
          if (!supportedDataType(left.dataType)) {
            withInfo(div, s"Unsupported datatype ${left.dataType}")
          }
          if (decimalBeforeSpark34(left.dataType)) {
            withInfo(div, "Decimal support requires Spark 3.4 or later")
          }
          None

        case rem @ Remainder(left, right, _)
            if supportedDataType(left.dataType) && !decimalBeforeSpark34(left.dataType) =>
          val leftExpr = exprToProtoInternal(left, inputs)
          val rightExpr = exprToProtoInternal(nullIfWhenPrimitive(right), inputs)

          if (leftExpr.isDefined && rightExpr.isDefined) {
            val builder = ExprOuterClass.Remainder.newBuilder()
            builder.setLeft(leftExpr.get)
            builder.setRight(rightExpr.get)
            builder.setFailOnError(getFailOnError(rem))
            serializeDataType(rem.dataType).foreach { t =>
              builder.setReturnType(t)
            }

            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setRemainder(builder)
                .build())
          } else {
            withInfo(rem, left, right)
            None
          }
        case rem @ Remainder(left, _, _) =>
          if (!supportedDataType(left.dataType)) {
            withInfo(rem, s"Unsupported datatype ${left.dataType}")
          }
          if (decimalBeforeSpark34(left.dataType)) {
            withInfo(rem, "Decimal support requires Spark 3.4 or later")
          }
          None

        case EqualTo(left, right) =>
          val leftExpr = exprToProtoInternal(left, inputs)
          val rightExpr = exprToProtoInternal(right, inputs)

          if (leftExpr.isDefined && rightExpr.isDefined) {
            val builder = ExprOuterClass.Equal.newBuilder()
            builder.setLeft(leftExpr.get)
            builder.setRight(rightExpr.get)

            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setEq(builder)
                .build())
          } else {
            withInfo(expr, left, right)
            None
          }

        case Not(EqualTo(left, right)) =>
          val leftExpr = exprToProtoInternal(left, inputs)
          val rightExpr = exprToProtoInternal(right, inputs)

          if (leftExpr.isDefined && rightExpr.isDefined) {
            val builder = ExprOuterClass.NotEqual.newBuilder()
            builder.setLeft(leftExpr.get)
            builder.setRight(rightExpr.get)

            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setNeq(builder)
                .build())
          } else {
            withInfo(expr, left, right)
            None
          }

        case EqualNullSafe(left, right) =>
          val leftExpr = exprToProtoInternal(left, inputs)
          val rightExpr = exprToProtoInternal(right, inputs)

          if (leftExpr.isDefined && rightExpr.isDefined) {
            val builder = ExprOuterClass.EqualNullSafe.newBuilder()
            builder.setLeft(leftExpr.get)
            builder.setRight(rightExpr.get)

            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setEqNullSafe(builder)
                .build())
          } else {
            withInfo(expr, left, right)
            None
          }

        case Not(EqualNullSafe(left, right)) =>
          val leftExpr = exprToProtoInternal(left, inputs)
          val rightExpr = exprToProtoInternal(right, inputs)

          if (leftExpr.isDefined && rightExpr.isDefined) {
            val builder = ExprOuterClass.NotEqualNullSafe.newBuilder()
            builder.setLeft(leftExpr.get)
            builder.setRight(rightExpr.get)

            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setNeqNullSafe(builder)
                .build())
          } else {
            withInfo(expr, left, right)
            None
          }

        case GreaterThan(left, right) =>
          val leftExpr = exprToProtoInternal(left, inputs)
          val rightExpr = exprToProtoInternal(right, inputs)

          if (leftExpr.isDefined && rightExpr.isDefined) {
            val builder = ExprOuterClass.GreaterThan.newBuilder()
            builder.setLeft(leftExpr.get)
            builder.setRight(rightExpr.get)

            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setGt(builder)
                .build())
          } else {
            withInfo(expr, left, right)
            None
          }

        case GreaterThanOrEqual(left, right) =>
          val leftExpr = exprToProtoInternal(left, inputs)
          val rightExpr = exprToProtoInternal(right, inputs)

          if (leftExpr.isDefined && rightExpr.isDefined) {
            val builder = ExprOuterClass.GreaterThanEqual.newBuilder()
            builder.setLeft(leftExpr.get)
            builder.setRight(rightExpr.get)

            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setGtEq(builder)
                .build())
          } else {
            withInfo(expr, left, right)
            None
          }

        case LessThan(left, right) =>
          val leftExpr = exprToProtoInternal(left, inputs)
          val rightExpr = exprToProtoInternal(right, inputs)

          if (leftExpr.isDefined && rightExpr.isDefined) {
            val builder = ExprOuterClass.LessThan.newBuilder()
            builder.setLeft(leftExpr.get)
            builder.setRight(rightExpr.get)

            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setLt(builder)
                .build())
          } else {
            withInfo(expr, left, right)
            None
          }

        case LessThanOrEqual(left, right) =>
          val leftExpr = exprToProtoInternal(left, inputs)
          val rightExpr = exprToProtoInternal(right, inputs)

          if (leftExpr.isDefined && rightExpr.isDefined) {
            val builder = ExprOuterClass.LessThanEqual.newBuilder()
            builder.setLeft(leftExpr.get)
            builder.setRight(rightExpr.get)

            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setLtEq(builder)
                .build())
          } else {
            withInfo(expr, left, right)
            None
          }

        case Literal(value, dataType)
            if supportedDataType(dataType, allowStruct = value == null) =>
          val exprBuilder = ExprOuterClass.Literal.newBuilder()

          if (value == null) {
            exprBuilder.setIsNull(true)
          } else {
            exprBuilder.setIsNull(false)
            dataType match {
              case _: BooleanType => exprBuilder.setBoolVal(value.asInstanceOf[Boolean])
              case _: ByteType => exprBuilder.setByteVal(value.asInstanceOf[Byte])
              case _: ShortType => exprBuilder.setShortVal(value.asInstanceOf[Short])
              case _: IntegerType => exprBuilder.setIntVal(value.asInstanceOf[Int])
              case _: LongType => exprBuilder.setLongVal(value.asInstanceOf[Long])
              case _: FloatType => exprBuilder.setFloatVal(value.asInstanceOf[Float])
              case _: DoubleType => exprBuilder.setDoubleVal(value.asInstanceOf[Double])
              case _: StringType =>
                exprBuilder.setStringVal(value.asInstanceOf[UTF8String].toString)
              case _: TimestampType => exprBuilder.setLongVal(value.asInstanceOf[Long])
              case _: DecimalType =>
                // Pass decimal literal as bytes.
                val unscaled = value.asInstanceOf[Decimal].toBigDecimal.underlying.unscaledValue
                exprBuilder.setDecimalVal(
                  com.google.protobuf.ByteString.copyFrom(unscaled.toByteArray))
              case _: BinaryType =>
                val byteStr =
                  com.google.protobuf.ByteString.copyFrom(value.asInstanceOf[Array[Byte]])
                exprBuilder.setBytesVal(byteStr)
              case _: DateType => exprBuilder.setIntVal(value.asInstanceOf[Int])
              case dt if isTimestampNTZType(dt) =>
                exprBuilder.setLongVal(value.asInstanceOf[Long])
              case dt =>
                logWarning(s"Unexpected date type '$dt' for literal value '$value'")
            }
          }

          val dt = serializeDataType(dataType)

          if (dt.isDefined) {
            exprBuilder.setDatatype(dt.get)

            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setLiteral(exprBuilder)
                .build())
          } else {
            withInfo(expr, s"Unsupported datatype $dataType")
            None
          }
        case Literal(_, dataType) if !supportedDataType(dataType) =>
          withInfo(expr, s"Unsupported datatype $dataType")
          None

        case Substring(str, Literal(pos, _), Literal(len, _)) =>
          val strExpr = exprToProtoInternal(str, inputs)

          if (strExpr.isDefined) {
            val builder = ExprOuterClass.Substring.newBuilder()
            builder.setChild(strExpr.get)
            builder.setStart(pos.asInstanceOf[Int])
            builder.setLen(len.asInstanceOf[Int])

            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setSubstring(builder)
                .build())
          } else {
            withInfo(expr, str)
            None
          }

        case StructsToJson(options, child, timezoneId) =>
          if (options.nonEmpty) {
            withInfo(expr, "StructsToJson with options is not supported")
            None
          } else {

            def isSupportedType(dt: DataType): Boolean = {
              dt match {
                case StructType(fields) =>
                  fields.forall(f => isSupportedType(f.dataType))
                case DataTypes.BooleanType | DataTypes.ByteType | DataTypes.ShortType |
                    DataTypes.IntegerType | DataTypes.LongType | DataTypes.FloatType |
                    DataTypes.DoubleType | DataTypes.StringType =>
                  true
                case DataTypes.DateType | DataTypes.TimestampType =>
                  // TODO implement these types with tests for formatting options and timezone
                  false
                case _: MapType | _: ArrayType =>
                  // Spark supports map and array in StructsToJson but this is not yet
                  // implemented in Comet
                  false
                case _ => false
              }
            }

            val isSupported = child.dataType match {
              case s: StructType =>
                s.fields.forall(f => isSupportedType(f.dataType))
              case _: MapType | _: ArrayType =>
                // Spark supports map and array in StructsToJson but this is not yet
                // implemented in Comet
                false
              case _ =>
                false
            }

            if (isSupported) {
              exprToProto(child, input, binding) match {
                case Some(p) =>
                  val toJson = ExprOuterClass.ToJson
                    .newBuilder()
                    .setChild(p)
                    .setTimezone(timezoneId.getOrElse("UTC"))
                    .setIgnoreNullFields(true)
                    .build()
                  Some(
                    ExprOuterClass.Expr
                      .newBuilder()
                      .setToJson(toJson)
                      .build())
                case _ =>
                  withInfo(expr, child)
                  None
              }
            } else {
              withInfo(expr, "Unsupported data type", child)
              None
            }
          }

        case Like(left, right, escapeChar) =>
          if (escapeChar == '\\') {
            val leftExpr = exprToProtoInternal(left, inputs)
            val rightExpr = exprToProtoInternal(right, inputs)

            if (leftExpr.isDefined && rightExpr.isDefined) {
              val builder = ExprOuterClass.Like.newBuilder()
              builder.setLeft(leftExpr.get)
              builder.setRight(rightExpr.get)

              Some(
                ExprOuterClass.Expr
                  .newBuilder()
                  .setLike(builder)
                  .build())
            } else {
              withInfo(expr, left, right)
              None
            }
          } else {
            // TODO custom escape char
            withInfo(expr, s"custom escape character $escapeChar not supported in LIKE")
            None
          }

        case RLike(left, right) =>
          // we currently only support scalar regex patterns
          right match {
            case Literal(pattern, DataTypes.StringType) =>
              if (!RegExp.isSupportedPattern(pattern.toString) &&
                !CometConf.COMET_REGEXP_ALLOW_INCOMPATIBLE.get()) {
                withInfo(
                  expr,
                  s"Regexp pattern $pattern is not compatible with Spark. " +
                    s"Set ${CometConf.COMET_REGEXP_ALLOW_INCOMPATIBLE.key}=true " +
                    "to allow it anyway.")
                return None
              }
            case _ =>
              withInfo(expr, "Only scalar regexp patterns are supported")
              return None
          }

          val leftExpr = exprToProtoInternal(left, inputs)
          val rightExpr = exprToProtoInternal(right, inputs)

          if (leftExpr.isDefined && rightExpr.isDefined) {
            val builder = ExprOuterClass.RLike.newBuilder()
            builder.setLeft(leftExpr.get)
            builder.setRight(rightExpr.get)

            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setRlike(builder)
                .build())
          } else {
            withInfo(expr, left, right)
            None
          }
        case StartsWith(left, right) =>
          val leftExpr = exprToProtoInternal(left, inputs)
          val rightExpr = exprToProtoInternal(right, inputs)

          if (leftExpr.isDefined && rightExpr.isDefined) {
            val builder = ExprOuterClass.StartsWith.newBuilder()
            builder.setLeft(leftExpr.get)
            builder.setRight(rightExpr.get)

            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setStartsWith(builder)
                .build())
          } else {
            withInfo(expr, left, right)
            None
          }

        case EndsWith(left, right) =>
          val leftExpr = exprToProtoInternal(left, inputs)
          val rightExpr = exprToProtoInternal(right, inputs)

          if (leftExpr.isDefined && rightExpr.isDefined) {
            val builder = ExprOuterClass.EndsWith.newBuilder()
            builder.setLeft(leftExpr.get)
            builder.setRight(rightExpr.get)

            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setEndsWith(builder)
                .build())
          } else {
            withInfo(expr, left, right)
            None
          }

        case Contains(left, right) =>
          val leftExpr = exprToProtoInternal(left, inputs)
          val rightExpr = exprToProtoInternal(right, inputs)

          if (leftExpr.isDefined && rightExpr.isDefined) {
            val builder = ExprOuterClass.Contains.newBuilder()
            builder.setLeft(leftExpr.get)
            builder.setRight(rightExpr.get)

            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setContains(builder)
                .build())
          } else {
            withInfo(expr, left, right)
            None
          }

        case StringSpace(child) =>
          val childExpr = exprToProtoInternal(child, inputs)

          if (childExpr.isDefined) {
            val builder = ExprOuterClass.StringSpace.newBuilder()
            builder.setChild(childExpr.get)

            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setStringSpace(builder)
                .build())
          } else {
            withInfo(expr, child)
            None
          }

        case Hour(child, timeZoneId) =>
          val childExpr = exprToProtoInternal(child, inputs)

          if (childExpr.isDefined) {
            val builder = ExprOuterClass.Hour.newBuilder()
            builder.setChild(childExpr.get)

            val timeZone = timeZoneId.getOrElse("UTC")
            builder.setTimezone(timeZone)

            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setHour(builder)
                .build())
          } else {
            withInfo(expr, child)
            None
          }

        case Minute(child, timeZoneId) =>
          val childExpr = exprToProtoInternal(child, inputs)

          if (childExpr.isDefined) {
            val builder = ExprOuterClass.Minute.newBuilder()
            builder.setChild(childExpr.get)

            val timeZone = timeZoneId.getOrElse("UTC")
            builder.setTimezone(timeZone)

            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setMinute(builder)
                .build())
          } else {
            withInfo(expr, child)
            None
          }

        case DateAdd(left, right) =>
          val leftExpr = exprToProtoInternal(left, inputs)
          val rightExpr = exprToProtoInternal(right, inputs)
          val optExpr = scalarExprToProtoWithReturnType("date_add", DateType, leftExpr, rightExpr)
          optExprWithInfo(optExpr, expr, left, right)

        case DateSub(left, right) =>
          val leftExpr = exprToProtoInternal(left, inputs)
          val rightExpr = exprToProtoInternal(right, inputs)
          val optExpr = scalarExprToProtoWithReturnType("date_sub", DateType, leftExpr, rightExpr)
          optExprWithInfo(optExpr, expr, left, right)

        case TruncDate(child, format) =>
          val childExpr = exprToProtoInternal(child, inputs)
          val formatExpr = exprToProtoInternal(format, inputs)

          if (childExpr.isDefined && formatExpr.isDefined) {
            val builder = ExprOuterClass.TruncDate.newBuilder()
            builder.setChild(childExpr.get)
            builder.setFormat(formatExpr.get)

            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setTruncDate(builder)
                .build())
          } else {
            withInfo(expr, child, format)
            None
          }

        case TruncTimestamp(format, child, timeZoneId) =>
          val childExpr = exprToProtoInternal(child, inputs)
          val formatExpr = exprToProtoInternal(format, inputs)

          if (childExpr.isDefined && formatExpr.isDefined) {
            val builder = ExprOuterClass.TruncTimestamp.newBuilder()
            builder.setChild(childExpr.get)
            builder.setFormat(formatExpr.get)

            val timeZone = timeZoneId.getOrElse("UTC")
            builder.setTimezone(timeZone)

            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setTruncTimestamp(builder)
                .build())
          } else {
            withInfo(expr, child, format)
            None
          }

        case Second(child, timeZoneId) =>
          val childExpr = exprToProtoInternal(child, inputs)

          if (childExpr.isDefined) {
            val builder = ExprOuterClass.Second.newBuilder()
            builder.setChild(childExpr.get)

            val timeZone = timeZoneId.getOrElse("UTC")
            builder.setTimezone(timeZone)

            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setSecond(builder)
                .build())
          } else {
            withInfo(expr, child)
            None
          }

        case Year(child) =>
          val periodType = exprToProtoInternal(Literal("year"), inputs)
          val childExpr = exprToProtoInternal(child, inputs)
          val optExpr = scalarExprToProto("datepart", Seq(periodType, childExpr): _*)
            .map(e => {
              Expr
                .newBuilder()
                .setCast(
                  ExprOuterClass.Cast
                    .newBuilder()
                    .setChild(e)
                    .setDatatype(serializeDataType(IntegerType).get)
                    .setEvalMode(ExprOuterClass.EvalMode.LEGACY)
                    .setAllowIncompat(false)
                    .build())
                .build()
            })
          optExprWithInfo(optExpr, expr, child)

        case IsNull(child) =>
          val childExpr = exprToProtoInternal(child, inputs)

          if (childExpr.isDefined) {
            val castBuilder = ExprOuterClass.IsNull.newBuilder()
            castBuilder.setChild(childExpr.get)

            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setIsNull(castBuilder)
                .build())
          } else {
            withInfo(expr, child)
            None
          }

        case IsNotNull(child) =>
          val childExpr = exprToProtoInternal(child, inputs)

          if (childExpr.isDefined) {
            val castBuilder = ExprOuterClass.IsNotNull.newBuilder()
            castBuilder.setChild(childExpr.get)

            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setIsNotNull(castBuilder)
                .build())
          } else {
            withInfo(expr, child)
            None
          }

        case IsNaN(child) =>
          val childExpr = exprToProtoInternal(child, inputs)
          val optExpr =
            scalarExprToProtoWithReturnType("isnan", BooleanType, childExpr)

          optExprWithInfo(optExpr, expr, child)

        case SortOrder(child, direction, nullOrdering, _) =>
          val childExpr = exprToProtoInternal(child, inputs)

          if (childExpr.isDefined) {
            val sortOrderBuilder = ExprOuterClass.SortOrder.newBuilder()
            sortOrderBuilder.setChild(childExpr.get)

            direction match {
              case Ascending => sortOrderBuilder.setDirectionValue(0)
              case Descending => sortOrderBuilder.setDirectionValue(1)
            }

            nullOrdering match {
              case NullsFirst => sortOrderBuilder.setNullOrderingValue(0)
              case NullsLast => sortOrderBuilder.setNullOrderingValue(1)
            }

            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setSortOrder(sortOrderBuilder)
                .build())
          } else {
            withInfo(expr, child)
            None
          }

        case And(left, right) =>
          val leftExpr = exprToProtoInternal(left, inputs)
          val rightExpr = exprToProtoInternal(right, inputs)

          if (leftExpr.isDefined && rightExpr.isDefined) {
            val builder = ExprOuterClass.And.newBuilder()
            builder.setLeft(leftExpr.get)
            builder.setRight(rightExpr.get)

            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setAnd(builder)
                .build())
          } else {
            withInfo(expr, left, right)
            None
          }

        case Or(left, right) =>
          val leftExpr = exprToProtoInternal(left, inputs)
          val rightExpr = exprToProtoInternal(right, inputs)

          if (leftExpr.isDefined && rightExpr.isDefined) {
            val builder = ExprOuterClass.Or.newBuilder()
            builder.setLeft(leftExpr.get)
            builder.setRight(rightExpr.get)

            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setOr(builder)
                .build())
          } else {
            withInfo(expr, left, right)
            None
          }

        case UnaryExpression(child) if expr.prettyName == "promote_precision" =>
          // `UnaryExpression` includes `PromotePrecision` for Spark 3.3
          // `PromotePrecision` is just a wrapper, don't need to serialize it.
          exprToProtoInternal(child, inputs)

        case CheckOverflow(child, dt, nullOnOverflow) =>
          val childExpr = exprToProtoInternal(child, inputs)

          if (childExpr.isDefined) {
            val builder = ExprOuterClass.CheckOverflow.newBuilder()
            builder.setChild(childExpr.get)
            builder.setFailOnError(!nullOnOverflow)

            // `dataType` must be decimal type
            val dataType = serializeDataType(dt)
            builder.setDatatype(dataType.get)

            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setCheckOverflow(builder)
                .build())
          } else {
            withInfo(expr, child)
            None
          }

        case attr: AttributeReference =>
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

        // abs implementation is not correct
        // https://github.com/apache/datafusion-comet/issues/666
//        case Abs(child, failOnErr) =>
//          val childExpr = exprToProtoInternal(child, inputs)
//          if (childExpr.isDefined) {
//            val evalModeStr =
//              if (failOnErr) ExprOuterClass.EvalMode.ANSI else ExprOuterClass.EvalMode.LEGACY
//            val absBuilder = ExprOuterClass.Abs.newBuilder()
//            absBuilder.setChild(childExpr.get)
//            absBuilder.setEvalMode(evalModeStr)
//            Some(Expr.newBuilder().setAbs(absBuilder).build())
//          } else {
//            withInfo(expr, child)
//            None
//          }

        case Acos(child) =>
          val childExpr = exprToProtoInternal(child, inputs)
          val optExpr = scalarExprToProto("acos", childExpr)
          optExprWithInfo(optExpr, expr, child)

        case Asin(child) =>
          val childExpr = exprToProtoInternal(child, inputs)
          val optExpr = scalarExprToProto("asin", childExpr)
          optExprWithInfo(optExpr, expr, child)

        case Atan(child) =>
          val childExpr = exprToProtoInternal(child, inputs)
          val optExpr = scalarExprToProto("atan", childExpr)
          optExprWithInfo(optExpr, expr, child)

        case Atan2(left, right) =>
          val leftExpr = exprToProtoInternal(left, inputs)
          val rightExpr = exprToProtoInternal(right, inputs)
          val optExpr = scalarExprToProto("atan2", leftExpr, rightExpr)
          optExprWithInfo(optExpr, expr, left, right)

        case Hex(child) =>
          val childExpr = exprToProtoInternal(child, inputs)
          val optExpr =
            scalarExprToProtoWithReturnType("hex", StringType, childExpr)

          optExprWithInfo(optExpr, expr, child)

        case e: Unhex =>
          val unHex = unhexSerde(e)

          val childExpr = exprToProtoInternal(unHex._1, inputs)
          val failOnErrorExpr = exprToProtoInternal(unHex._2, inputs)

          val optExpr =
            scalarExprToProtoWithReturnType("unhex", e.dataType, childExpr, failOnErrorExpr)
          optExprWithInfo(optExpr, expr, unHex._1)

        case e @ Ceil(child) =>
          val childExpr = exprToProtoInternal(child, inputs)
          child.dataType match {
            case t: DecimalType if t.scale == 0 => // zero scale is no-op
              childExpr
            case t: DecimalType if t.scale < 0 => // Spark disallows negative scale SPARK-30252
              withInfo(e, s"Decimal type $t has negative scale")
              None
            case _ =>
              val optExpr = scalarExprToProtoWithReturnType("ceil", e.dataType, childExpr)
              optExprWithInfo(optExpr, expr, child)
          }

        case Cos(child) =>
          val childExpr = exprToProtoInternal(child, inputs)
          val optExpr = scalarExprToProto("cos", childExpr)
          optExprWithInfo(optExpr, expr, child)

        case Exp(child) =>
          val childExpr = exprToProtoInternal(child, inputs)
          val optExpr = scalarExprToProto("exp", childExpr)
          optExprWithInfo(optExpr, expr, child)

        case e @ Floor(child) =>
          val childExpr = exprToProtoInternal(child, inputs)
          child.dataType match {
            case t: DecimalType if t.scale == 0 => // zero scale is no-op
              childExpr
            case t: DecimalType if t.scale < 0 => // Spark disallows negative scale SPARK-30252
              withInfo(e, s"Decimal type $t has negative scale")
              None
            case _ =>
              val optExpr = scalarExprToProtoWithReturnType("floor", e.dataType, childExpr)
              optExprWithInfo(optExpr, expr, child)
          }

        // The expression for `log` functions is defined as null on numbers less than or equal
        // to 0. This matches Spark and Hive behavior, where non positive values eval to null
        // instead of NaN or -Infinity.
        case Log(child) =>
          val childExpr = exprToProtoInternal(nullIfNegative(child), inputs)
          val optExpr = scalarExprToProto("ln", childExpr)
          optExprWithInfo(optExpr, expr, child)

        case Log10(child) =>
          val childExpr = exprToProtoInternal(nullIfNegative(child), inputs)
          val optExpr = scalarExprToProto("log10", childExpr)
          optExprWithInfo(optExpr, expr, child)

        case Log2(child) =>
          val childExpr = exprToProtoInternal(nullIfNegative(child), inputs)
          val optExpr = scalarExprToProto("log2", childExpr)
          optExprWithInfo(optExpr, expr, child)

        case Pow(left, right) =>
          val leftExpr = exprToProtoInternal(left, inputs)
          val rightExpr = exprToProtoInternal(right, inputs)
          val optExpr = scalarExprToProto("pow", leftExpr, rightExpr)
          optExprWithInfo(optExpr, expr, left, right)

        case r: Round =>
          // _scale s a constant, copied from Spark's RoundBase because it is a protected val
          val scaleV: Any = r.scale.eval(EmptyRow)
          val _scale: Int = scaleV.asInstanceOf[Int]

          lazy val childExpr = exprToProtoInternal(r.child, inputs)
          r.child.dataType match {
            case t: DecimalType if t.scale < 0 => // Spark disallows negative scale SPARK-30252
              withInfo(r, "Decimal type has negative scale")
              None
            case _ if scaleV == null =>
              exprToProtoInternal(Literal(null), inputs)
            case _: ByteType | ShortType | IntegerType | LongType if _scale >= 0 =>
              childExpr // _scale(I.e. decimal place) >= 0 is a no-op for integer types in Spark
            case _: FloatType | DoubleType =>
              // We cannot properly match with the Spark behavior for floating-point numbers.
              // Spark uses BigDecimal for rounding float/double, and BigDecimal fist converts a
              // double to string internally in order to create its own internal representation.
              // The problem is BigDecimal uses java.lang.Double.toString() and it has complicated
              // rounding algorithm. E.g. -5.81855622136895E8 is actually
              // -581855622.13689494132995605468750. Note the 5th fractional digit is 4 instead of
              // 5. Java(Scala)'s toString() rounds it up to -581855622.136895. This makes a
              // difference when rounding at 5th digit, I.e. round(-5.81855622136895E8, 5) should be
              // -5.818556221369E8, instead of -5.8185562213689E8. There is also an example that
              // toString() does NOT round up. 6.1317116247283497E18 is 6131711624728349696. It can
              // be rounded up to 6.13171162472835E18 that still represents the same double number.
              // I.e. 6.13171162472835E18 == 6.1317116247283497E18. However, toString() does not.
              // That results in round(6.1317116247283497E18, -5) == 6.1317116247282995E18 instead
              // of 6.1317116247283999E18.
              withInfo(r, "Comet does not support Spark's BigDecimal rounding")
              None
            case _ =>
              // `scale` must be Int64 type in DataFusion
              val scaleExpr = exprToProtoInternal(Literal(_scale.toLong, LongType), inputs)
              val optExpr =
                scalarExprToProtoWithReturnType("round", r.dataType, childExpr, scaleExpr)
              optExprWithInfo(optExpr, expr, r.child)
          }

        // TODO enable once https://github.com/apache/datafusion/issues/11557 is fixed or
        // when we have a Spark-compatible version implemented in Comet
//        case Signum(child) =>
//          val childExpr = exprToProtoInternal(child, inputs)
//          val optExpr = scalarExprToProto("signum", childExpr)
//          optExprWithInfo(optExpr, expr, child)

        case Sin(child) =>
          val childExpr = exprToProtoInternal(child, inputs)
          val optExpr = scalarExprToProto("sin", childExpr)
          optExprWithInfo(optExpr, expr, child)

        case Sqrt(child) =>
          val childExpr = exprToProtoInternal(child, inputs)
          val optExpr = scalarExprToProto("sqrt", childExpr)
          optExprWithInfo(optExpr, expr, child)

        case Tan(child) =>
          val childExpr = exprToProtoInternal(child, inputs)
          val optExpr = scalarExprToProto("tan", childExpr)
          optExprWithInfo(optExpr, expr, child)

        case Ascii(child) =>
          val castExpr = Cast(child, StringType)
          val childExpr = exprToProtoInternal(castExpr, inputs)
          val optExpr = scalarExprToProto("ascii", childExpr)
          optExprWithInfo(optExpr, expr, castExpr)

        case BitLength(child) =>
          val castExpr = Cast(child, StringType)
          val childExpr = exprToProtoInternal(castExpr, inputs)
          val optExpr = scalarExprToProto("bit_length", childExpr)
          optExprWithInfo(optExpr, expr, castExpr)

        case If(predicate, trueValue, falseValue) =>
          val predicateExpr = exprToProtoInternal(predicate, inputs)
          val trueExpr = exprToProtoInternal(trueValue, inputs)
          val falseExpr = exprToProtoInternal(falseValue, inputs)
          if (predicateExpr.isDefined && trueExpr.isDefined && falseExpr.isDefined) {
            val builder = ExprOuterClass.IfExpr.newBuilder()
            builder.setIfExpr(predicateExpr.get)
            builder.setTrueExpr(trueExpr.get)
            builder.setFalseExpr(falseExpr.get)
            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setIf(builder)
                .build())
          } else {
            withInfo(expr, predicate, trueValue, falseValue)
            None
          }

        case CaseWhen(branches, elseValue) =>
          var allBranches: Seq[Expression] = Seq()
          val whenSeq = branches.map(elements => {
            allBranches = allBranches :+ elements._1
            exprToProtoInternal(elements._1, inputs)
          })
          val thenSeq = branches.map(elements => {
            allBranches = allBranches :+ elements._1
            exprToProtoInternal(elements._2, inputs)
          })
          assert(whenSeq.length == thenSeq.length)
          if (whenSeq.forall(_.isDefined) && thenSeq.forall(_.isDefined)) {
            val builder = ExprOuterClass.CaseWhen.newBuilder()
            builder.addAllWhen(whenSeq.map(_.get).asJava)
            builder.addAllThen(thenSeq.map(_.get).asJava)
            if (elseValue.isDefined) {
              val elseValueExpr =
                exprToProtoInternal(elseValue.get, inputs)
              if (elseValueExpr.isDefined) {
                builder.setElseExpr(elseValueExpr.get)
              } else {
                withInfo(expr, elseValue.get)
                return None
              }
            }
            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setCaseWhen(builder)
                .build())
          } else {
            withInfo(expr, allBranches: _*)
            None
          }
        case ConcatWs(children) =>
          var childExprs: Seq[Expression] = Seq()
          val exprs = children.map(e => {
            val castExpr = Cast(e, StringType)
            childExprs = childExprs :+ castExpr
            exprToProtoInternal(castExpr, inputs)
          })
          val optExpr = scalarExprToProto("concat_ws", exprs: _*)
          optExprWithInfo(optExpr, expr, childExprs: _*)

        case Chr(child) =>
          val childExpr = exprToProtoInternal(child, inputs)
          val optExpr = scalarExprToProto("chr", childExpr)
          optExprWithInfo(optExpr, expr, child)

        case InitCap(child) =>
          val castExpr = Cast(child, StringType)
          val childExpr = exprToProtoInternal(castExpr, inputs)
          val optExpr = scalarExprToProto("initcap", childExpr)
          optExprWithInfo(optExpr, expr, castExpr)

        case Length(child) =>
          val castExpr = Cast(child, StringType)
          val childExpr = exprToProtoInternal(castExpr, inputs)
          val optExpr = scalarExprToProto("length", childExpr)
          optExprWithInfo(optExpr, expr, castExpr)

        case Md5(child) =>
          val childExpr = exprToProtoInternal(child, inputs)
          val optExpr = scalarExprToProto("md5", childExpr)
          optExprWithInfo(optExpr, expr, child)

        case OctetLength(child) =>
          val castExpr = Cast(child, StringType)
          val childExpr = exprToProtoInternal(castExpr, inputs)
          val optExpr = scalarExprToProto("octet_length", childExpr)
          optExprWithInfo(optExpr, expr, castExpr)

        case Reverse(child) =>
          val castExpr = Cast(child, StringType)
          val childExpr = exprToProtoInternal(castExpr, inputs)
          val optExpr = scalarExprToProto("reverse", childExpr)
          optExprWithInfo(optExpr, expr, castExpr)

        case StringInstr(str, substr) =>
          val leftCast = Cast(str, StringType)
          val rightCast = Cast(substr, StringType)
          val leftExpr = exprToProtoInternal(leftCast, inputs)
          val rightExpr = exprToProtoInternal(rightCast, inputs)
          val optExpr = scalarExprToProto("strpos", leftExpr, rightExpr)
          optExprWithInfo(optExpr, expr, leftCast, rightCast)

        case StringRepeat(str, times) =>
          val leftCast = Cast(str, StringType)
          val rightCast = Cast(times, LongType)
          val leftExpr = exprToProtoInternal(leftCast, inputs)
          val rightExpr = exprToProtoInternal(rightCast, inputs)
          val optExpr = scalarExprToProto("repeat", leftExpr, rightExpr)
          optExprWithInfo(optExpr, expr, leftCast, rightCast)

        case StringReplace(src, search, replace) =>
          val srcCast = Cast(src, StringType)
          val searchCast = Cast(search, StringType)
          val replaceCast = Cast(replace, StringType)
          val srcExpr = exprToProtoInternal(srcCast, inputs)
          val searchExpr = exprToProtoInternal(searchCast, inputs)
          val replaceExpr = exprToProtoInternal(replaceCast, inputs)
          val optExpr = scalarExprToProto("replace", srcExpr, searchExpr, replaceExpr)
          optExprWithInfo(optExpr, expr, srcCast, searchCast, replaceCast)

        case StringTranslate(src, matching, replace) =>
          val srcCast = Cast(src, StringType)
          val matchingCast = Cast(matching, StringType)
          val replaceCast = Cast(replace, StringType)
          val srcExpr = exprToProtoInternal(srcCast, inputs)
          val matchingExpr = exprToProtoInternal(matchingCast, inputs)
          val replaceExpr = exprToProtoInternal(replaceCast, inputs)
          val optExpr = scalarExprToProto("translate", srcExpr, matchingExpr, replaceExpr)
          optExprWithInfo(optExpr, expr, srcCast, matchingCast, replaceCast)

        case StringTrim(srcStr, trimStr) =>
          trim(expr, srcStr, trimStr, inputs, "trim")

        case StringTrimLeft(srcStr, trimStr) =>
          trim(expr, srcStr, trimStr, inputs, "ltrim")

        case StringTrimRight(srcStr, trimStr) =>
          trim(expr, srcStr, trimStr, inputs, "rtrim")

        case StringTrimBoth(srcStr, trimStr, _) =>
          trim(expr, srcStr, trimStr, inputs, "btrim")

        case Upper(child) =>
          if (CometConf.COMET_CASE_CONVERSION_ENABLED.get()) {
            val castExpr = Cast(child, StringType)
            val childExpr = exprToProtoInternal(castExpr, inputs)
            val optExpr = scalarExprToProto("upper", childExpr)
            optExprWithInfo(optExpr, expr, castExpr)
          } else {
            withInfo(
              expr,
              "Comet is not compatible with Spark for case conversion in " +
                s"locale-specific cases. Set ${CometConf.COMET_CASE_CONVERSION_ENABLED.key}=true " +
                "to enable it anyway.")
            None
          }

        case Lower(child) =>
          if (CometConf.COMET_CASE_CONVERSION_ENABLED.get()) {
            val castExpr = Cast(child, StringType)
            val childExpr = exprToProtoInternal(castExpr, inputs)
            val optExpr = scalarExprToProto("lower", childExpr)
            optExprWithInfo(optExpr, expr, castExpr)
          } else {
            withInfo(
              expr,
              "Comet is not compatible with Spark for case conversion in " +
                s"locale-specific cases. Set ${CometConf.COMET_CASE_CONVERSION_ENABLED.key}=true " +
                "to enable it anyway.")
            None
          }

        case BitwiseAnd(left, right) =>
          val leftExpr = exprToProtoInternal(left, inputs)
          val rightExpr = exprToProtoInternal(right, inputs)

          if (leftExpr.isDefined && rightExpr.isDefined) {
            val builder = ExprOuterClass.BitwiseAnd.newBuilder()
            builder.setLeft(leftExpr.get)
            builder.setRight(rightExpr.get)

            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setBitwiseAnd(builder)
                .build())
          } else {
            withInfo(expr, left, right)
            None
          }

        case BitwiseNot(child) =>
          val childExpr = exprToProtoInternal(child, inputs)

          if (childExpr.isDefined) {
            val builder = ExprOuterClass.BitwiseNot.newBuilder()
            builder.setChild(childExpr.get)

            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setBitwiseNot(builder)
                .build())
          } else {
            withInfo(expr, child)
            None
          }

        case BitwiseOr(left, right) =>
          val leftExpr = exprToProtoInternal(left, inputs)
          val rightExpr = exprToProtoInternal(right, inputs)

          if (leftExpr.isDefined && rightExpr.isDefined) {
            val builder = ExprOuterClass.BitwiseOr.newBuilder()
            builder.setLeft(leftExpr.get)
            builder.setRight(rightExpr.get)

            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setBitwiseOr(builder)
                .build())
          } else {
            withInfo(expr, left, right)
            None
          }

        case BitwiseXor(left, right) =>
          val leftExpr = exprToProtoInternal(left, inputs)
          val rightExpr = exprToProtoInternal(right, inputs)

          if (leftExpr.isDefined && rightExpr.isDefined) {
            val builder = ExprOuterClass.BitwiseXor.newBuilder()
            builder.setLeft(leftExpr.get)
            builder.setRight(rightExpr.get)

            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setBitwiseXor(builder)
                .build())
          } else {
            withInfo(expr, left, right)
            None
          }

        case ShiftRight(left, right) =>
          val leftExpr = exprToProtoInternal(left, inputs)
          // DataFusion bitwise shift right expression requires
          // same data type between left and right side
          val rightExpression = if (left.dataType == LongType) {
            Cast(right, LongType)
          } else {
            right
          }
          val rightExpr = exprToProtoInternal(rightExpression, inputs)

          if (leftExpr.isDefined && rightExpr.isDefined) {
            val builder = ExprOuterClass.BitwiseShiftRight.newBuilder()
            builder.setLeft(leftExpr.get)
            builder.setRight(rightExpr.get)

            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setBitwiseShiftRight(builder)
                .build())
          } else {
            withInfo(expr, left, rightExpression)
            None
          }

        case ShiftLeft(left, right) =>
          val leftExpr = exprToProtoInternal(left, inputs)
          // DataFusion bitwise shift right expression requires
          // same data type between left and right side
          val rightExpression = if (left.dataType == LongType) {
            Cast(right, LongType)
          } else {
            right
          }
          val rightExpr = exprToProtoInternal(rightExpression, inputs)

          if (leftExpr.isDefined && rightExpr.isDefined) {
            val builder = ExprOuterClass.BitwiseShiftLeft.newBuilder()
            builder.setLeft(leftExpr.get)
            builder.setRight(rightExpr.get)

            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setBitwiseShiftLeft(builder)
                .build())
          } else {
            withInfo(expr, left, rightExpression)
            None
          }

        case In(value, list) =>
          in(expr, value, list, inputs, false)

        case InSet(value, hset) =>
          val valueDataType = value.dataType
          val list = hset.map { setVal =>
            Literal(setVal, valueDataType)
          }.toSeq
          // Change `InSet` to `In` expression
          // We do Spark `InSet` optimization in native (DataFusion) side.
          in(expr, value, list, inputs, false)

        case Not(In(value, list)) =>
          in(expr, value, list, inputs, true)

        case Not(child) =>
          val childExpr = exprToProtoInternal(child, inputs)
          if (childExpr.isDefined) {
            val builder = ExprOuterClass.Not.newBuilder()
            builder.setChild(childExpr.get)
            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setNot(builder)
                .build())
          } else {
            withInfo(expr, child)
            None
          }

        case UnaryMinus(child, failOnError) =>
          val childExpr = exprToProtoInternal(child, inputs)
          if (childExpr.isDefined) {
            val builder = ExprOuterClass.UnaryMinus.newBuilder()
            builder.setChild(childExpr.get)
            builder.setFailOnError(failOnError)
            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setUnaryMinus(builder)
                .build())
          } else {
            withInfo(expr, child)
            None
          }

        case a @ Coalesce(_) =>
          val exprChildren = a.children.map(exprToProtoInternal(_, inputs))
          scalarExprToProto("coalesce", exprChildren: _*)

        // With Spark 3.4, CharVarcharCodegenUtils.readSidePadding gets called to pad spaces for
        // char types.
        // See https://github.com/apache/spark/pull/38151
        case s: StaticInvoke
            if s.staticObject.isInstanceOf[Class[CharVarcharCodegenUtils]] &&
              s.dataType.isInstanceOf[StringType] &&
              s.functionName == "readSidePadding" &&
              s.arguments.size == 2 &&
              s.propagateNull &&
              !s.returnNullable &&
              s.isDeterministic =>
          val argsExpr = Seq(
            exprToProtoInternal(Cast(s.arguments(0), StringType), inputs),
            exprToProtoInternal(s.arguments(1), inputs))

          if (argsExpr.forall(_.isDefined)) {
            val builder = ExprOuterClass.ScalarFunc.newBuilder()
            builder.setFunc("read_side_padding")
            argsExpr.foreach(arg => builder.addArgs(arg.get))

            Some(ExprOuterClass.Expr.newBuilder().setScalarFunc(builder).build())
          } else {
            withInfo(expr, s.arguments: _*)
            None
          }

        case KnownFloatingPointNormalized(NormalizeNaNAndZero(expr)) =>
          val dataType = serializeDataType(expr.dataType)
          if (dataType.isEmpty) {
            withInfo(expr, s"Unsupported datatype ${expr.dataType}")
            return None
          }
          val ex = exprToProtoInternal(expr, inputs)
          ex.map { child =>
            val builder = ExprOuterClass.NormalizeNaNAndZero
              .newBuilder()
              .setChild(child)
              .setDatatype(dataType.get)
            ExprOuterClass.Expr.newBuilder().setNormalizeNanAndZero(builder).build()
          }

        case s @ execution.ScalarSubquery(_, _) if supportedDataType(s.dataType) =>
          val dataType = serializeDataType(s.dataType)
          if (dataType.isEmpty) {
            withInfo(s, s"Scalar subquery returns unsupported datatype ${s.dataType}")
            return None
          }

          val builder = ExprOuterClass.Subquery
            .newBuilder()
            .setId(s.exprId.id)
            .setDatatype(dataType.get)
          Some(ExprOuterClass.Expr.newBuilder().setSubquery(builder).build())

        case UnscaledValue(child) =>
          val childExpr = exprToProtoInternal(child, inputs)
          val optExpr = scalarExprToProtoWithReturnType("unscaled_value", LongType, childExpr)
          optExprWithInfo(optExpr, expr, child)

        case MakeDecimal(child, precision, scale, true) =>
          val childExpr = exprToProtoInternal(child, inputs)
          val optExpr = scalarExprToProtoWithReturnType(
            "make_decimal",
            DecimalType(precision, scale),
            childExpr)
          optExprWithInfo(optExpr, expr, child)

        case b @ BloomFilterMightContain(_, _) =>
          val bloomFilter = b.left
          val value = b.right
          val bloomFilterExpr = exprToProtoInternal(bloomFilter, inputs)
          val valueExpr = exprToProtoInternal(value, inputs)
          if (bloomFilterExpr.isDefined && valueExpr.isDefined) {
            val builder = ExprOuterClass.BloomFilterMightContain.newBuilder()
            builder.setBloomFilter(bloomFilterExpr.get)
            builder.setValue(valueExpr.get)
            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setBloomFilterMightContain(builder)
                .build())
          } else {
            withInfo(expr, bloomFilter, value)
            None
          }

        case Murmur3Hash(children, seed) =>
          val firstUnSupportedInput = children.find(c => !supportedDataType(c.dataType))
          if (firstUnSupportedInput.isDefined) {
            withInfo(expr, s"Unsupported datatype ${firstUnSupportedInput.get.dataType}")
            return None
          }
          val exprs = children.map(exprToProtoInternal(_, inputs))
          val seedBuilder = ExprOuterClass.Literal
            .newBuilder()
            .setDatatype(serializeDataType(IntegerType).get)
            .setIntVal(seed)
          val seedExpr = Some(ExprOuterClass.Expr.newBuilder().setLiteral(seedBuilder).build())
          // the seed is put at the end of the arguments
          scalarExprToProtoWithReturnType("murmur3_hash", IntegerType, exprs :+ seedExpr: _*)

        case XxHash64(children, seed) =>
          val firstUnSupportedInput = children.find(c => !supportedDataType(c.dataType))
          if (firstUnSupportedInput.isDefined) {
            withInfo(expr, s"Unsupported datatype ${firstUnSupportedInput.get.dataType}")
            return None
          }
          val exprs = children.map(exprToProtoInternal(_, inputs))
          val seedBuilder = ExprOuterClass.Literal
            .newBuilder()
            .setDatatype(serializeDataType(LongType).get)
            .setLongVal(seed)
          val seedExpr = Some(ExprOuterClass.Expr.newBuilder().setLiteral(seedBuilder).build())
          // the seed is put at the end of the arguments
          scalarExprToProtoWithReturnType("xxhash64", LongType, exprs :+ seedExpr: _*)

        case Sha2(left, numBits) =>
          if (!numBits.foldable) {
            withInfo(expr, "non literal numBits is not supported")
            return None
          }
          // it's possible for spark to dynamically compute the number of bits from input
          // expression, however DataFusion does not support that yet.
          val childExpr = exprToProtoInternal(left, inputs)
          val bits = numBits.eval().asInstanceOf[Int]
          val algorithm = bits match {
            case 224 => "sha224"
            case 256 | 0 => "sha256"
            case 384 => "sha384"
            case 512 => "sha512"
            case _ =>
              null
          }
          if (algorithm == null) {
            exprToProtoInternal(Literal(null, StringType), inputs)
          } else {
            scalarExprToProtoWithReturnType(algorithm, StringType, childExpr)
          }

        case struct @ CreateNamedStruct(_) =>
          if (struct.names.length != struct.names.distinct.length) {
            withInfo(expr, "CreateNamedStruct with duplicate field names are not supported")
            return None
          }

          val valExprs = struct.valExprs.map(exprToProto(_, inputs, binding))

          if (valExprs.forall(_.isDefined)) {
            val structBuilder = ExprOuterClass.CreateNamedStruct.newBuilder()
            structBuilder.addAllValues(valExprs.map(_.get).asJava)
            structBuilder.addAllNames(struct.names.map(_.toString).asJava)

            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setCreateNamedStruct(structBuilder)
                .build())
          } else {
            withInfo(expr, "unsupported arguments for CreateNamedStruct", struct.valExprs: _*)
            None
          }

        case GetStructField(child, ordinal, _) =>
          exprToProto(child, inputs, binding).map { childExpr =>
            val getStructFieldBuilder = ExprOuterClass.GetStructField
              .newBuilder()
              .setChild(childExpr)
              .setOrdinal(ordinal)

            ExprOuterClass.Expr
              .newBuilder()
              .setGetStructField(getStructFieldBuilder)
              .build()
          }

        case CreateArray(children, _) =>
          val childExprs = children.map(exprToProto(_, inputs, binding))

          if (childExprs.forall(_.isDefined)) {
            scalarExprToProto("make_array", childExprs: _*)
          } else {
            withInfo(expr, "unsupported arguments for CreateArray", children: _*)
            None
          }

        case GetArrayItem(child, ordinal, failOnError) =>
          val childExpr = exprToProto(child, inputs, binding)
          val ordinalExpr = exprToProto(ordinal, inputs, binding)

          if (childExpr.isDefined && ordinalExpr.isDefined) {
            val listExtractBuilder = ExprOuterClass.ListExtract
              .newBuilder()
              .setChild(childExpr.get)
              .setOrdinal(ordinalExpr.get)
              .setOneBased(false)
              .setFailOnError(failOnError)

            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setListExtract(listExtractBuilder)
                .build())
          } else {
            withInfo(expr, "unsupported arguments for GetArrayItem", child, ordinal)
            None
          }

        case ElementAt(child, ordinal, defaultValue, failOnError)
            if child.dataType.isInstanceOf[ArrayType] =>
          val childExpr = exprToProto(child, inputs, binding)
          val ordinalExpr = exprToProto(ordinal, inputs, binding)
          val defaultExpr = defaultValue.flatMap(exprToProto(_, inputs, binding))

          if (childExpr.isDefined && ordinalExpr.isDefined &&
            defaultExpr.isDefined == defaultValue.isDefined) {
            val arrayExtractBuilder = ExprOuterClass.ListExtract
              .newBuilder()
              .setChild(childExpr.get)
              .setOrdinal(ordinalExpr.get)
              .setOneBased(true)
              .setFailOnError(failOnError)

            defaultExpr.foreach(arrayExtractBuilder.setDefaultValue(_))

            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setListExtract(arrayExtractBuilder)
                .build())
          } else {
            withInfo(expr, "unsupported arguments for ElementAt", child, ordinal)
            None
          }

        case GetArrayStructFields(child, _, ordinal, _, _) =>
          val childExpr = exprToProto(child, inputs, binding)

          if (childExpr.isDefined) {
            val arrayStructFieldsBuilder = ExprOuterClass.GetArrayStructFields
              .newBuilder()
              .setChild(childExpr.get)
              .setOrdinal(ordinal)

            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setGetArrayStructFields(arrayStructFieldsBuilder)
                .build())
          } else {
            withInfo(expr, "unsupported arguments for GetArrayStructFields", child)
            None
          }

        case _ =>
          withInfo(expr, s"${expr.prettyName} is not supported", expr.children: _*)
          None
      }
    }

    def trim(
        expr: Expression, // parent expression
        srcStr: Expression,
        trimStr: Option[Expression],
        inputs: Seq[Attribute],
        trimType: String): Option[Expr] = {
      val srcCast = Cast(srcStr, StringType)
      val srcExpr = exprToProtoInternal(srcCast, inputs)
      if (trimStr.isDefined) {
        val trimCast = Cast(trimStr.get, StringType)
        val trimExpr = exprToProtoInternal(trimCast, inputs)
        val optExpr = scalarExprToProto(trimType, srcExpr, trimExpr)
        optExprWithInfo(optExpr, expr, srcCast, trimCast)
      } else {
        val optExpr = scalarExprToProto(trimType, srcExpr)
        optExprWithInfo(optExpr, expr, srcCast)
      }
    }

    def in(
        expr: Expression,
        value: Expression,
        list: Seq[Expression],
        inputs: Seq[Attribute],
        negate: Boolean): Option[Expr] = {
      val valueExpr = exprToProtoInternal(value, inputs)
      val listExprs = list.map(exprToProtoInternal(_, inputs))
      if (valueExpr.isDefined && listExprs.forall(_.isDefined)) {
        val builder = ExprOuterClass.In.newBuilder()
        builder.setInValue(valueExpr.get)
        builder.addAllLists(listExprs.map(_.get).asJava)
        builder.setNegated(negate)
        Some(
          ExprOuterClass.Expr
            .newBuilder()
            .setIn(builder)
            .build())
      } else {
        val allExprs = list ++ Seq(value)
        withInfo(expr, allExprs: _*)
        None
      }
    }

    val conf = SQLConf.get
    val newExpr =
      DecimalPrecision.promote(conf.decimalOperationsAllowPrecisionLoss, expr, !conf.ansiEnabled)
    exprToProtoInternal(newExpr, input)
  }

  def scalarExprToProtoWithReturnType(
      funcName: String,
      returnType: DataType,
      args: Option[Expr]*): Option[Expr] = {
    val builder = ExprOuterClass.ScalarFunc.newBuilder()
    builder.setFunc(funcName)
    serializeDataType(returnType).flatMap { t =>
      builder.setReturnType(t)
      scalarExprToProto0(builder, args: _*)
    }
  }

  def scalarExprToProto(funcName: String, args: Option[Expr]*): Option[Expr] = {
    val builder = ExprOuterClass.ScalarFunc.newBuilder()
    builder.setFunc(funcName)
    scalarExprToProto0(builder, args: _*)
  }

  private def scalarExprToProto0(
      builder: ScalarFunc.Builder,
      args: Option[Expr]*): Option[Expr] = {
    args.foreach {
      case Some(a) => builder.addArgs(a)
      case _ =>
        return None
    }
    Some(ExprOuterClass.Expr.newBuilder().setScalarFunc(builder).build())
  }

  def isPrimitive(expression: Expression): Boolean = expression.dataType match {
    case _: ByteType | _: ShortType | _: IntegerType | _: LongType | _: FloatType |
        _: DoubleType | _: TimestampType | _: DateType | _: BooleanType | _: DecimalType =>
      true
    case _ => false
  }

  def nullIfWhenPrimitive(expression: Expression): Expression = if (isPrimitive(expression)) {
    val zero = Literal.default(expression.dataType)
    expression match {
      case _: Literal if expression != zero => expression
      case _ =>
        If(EqualTo(expression, zero), Literal.create(null, expression.dataType), expression)
    }
  } else {
    expression
  }

  def nullIfNegative(expression: Expression): Expression = {
    val zero = Literal.default(expression.dataType)
    If(LessThanOrEqual(expression, zero), Literal.create(null, expression.dataType), expression)
  }

  /**
   * Returns true if given datatype is supported as a key in DataFusion sort merge join.
   */
  def supportedSortMergeJoinEqualType(dataType: DataType): Boolean = dataType match {
    case _: ByteType | _: ShortType | _: IntegerType | _: LongType | _: FloatType |
        _: DoubleType | _: StringType | _: DateType | _: DecimalType | _: BooleanType =>
      true
    case dt if isTimestampNTZType(dt) => true
    case _ => false
  }

  /**
   * Convert a Spark plan operator to a protobuf Comet operator.
   *
   * @param op
   *   Spark plan operator
   * @param childOp
   *   previously converted protobuf Comet operators, which will be consumed by the Spark plan
   *   operator as its children
   * @return
   *   The converted Comet native operator for the input `op`, or `None` if the `op` cannot be
   *   converted to a native operator.
   */
  def operator2Proto(op: SparkPlan, childOp: Operator*): Option[Operator] = {
    val conf = op.conf
    val result = OperatorOuterClass.Operator.newBuilder()
    childOp.foreach(result.addChildren)

    op match {
      case ProjectExec(projectList, child) if CometConf.COMET_EXEC_PROJECT_ENABLED.get(conf) =>
        val exprs = projectList.map(exprToProto(_, child.output))

        if (exprs.forall(_.isDefined) && childOp.nonEmpty) {
          val projectBuilder = OperatorOuterClass.Projection
            .newBuilder()
            .addAllProjectList(exprs.map(_.get).asJava)
          Some(result.setProjection(projectBuilder).build())
        } else {
          withInfo(op, projectList: _*)
          None
        }

      case FilterExec(condition, child) if CometConf.COMET_EXEC_FILTER_ENABLED.get(conf) =>
        val cond = exprToProto(condition, child.output)

        if (cond.isDefined && childOp.nonEmpty) {
          val filterBuilder = OperatorOuterClass.Filter.newBuilder().setPredicate(cond.get)
          Some(result.setFilter(filterBuilder).build())
        } else {
          withInfo(op, condition, child)
          None
        }

      case SortExec(sortOrder, _, child, _) if CometConf.COMET_EXEC_SORT_ENABLED.get(conf) =>
        if (!supportedSortType(op, sortOrder)) {
          return None
        }

        val sortOrders = sortOrder.map(exprToProto(_, child.output))

        if (sortOrders.forall(_.isDefined) && childOp.nonEmpty) {
          val sortBuilder = OperatorOuterClass.Sort
            .newBuilder()
            .addAllSortOrders(sortOrders.map(_.get).asJava)
          Some(result.setSort(sortBuilder).build())
        } else {
          withInfo(op, "sort order not supported", sortOrder: _*)
          None
        }

      case LocalLimitExec(limit, _) if CometConf.COMET_EXEC_LOCAL_LIMIT_ENABLED.get(conf) =>
        if (childOp.nonEmpty) {
          // LocalLimit doesn't use offset, but it shares same operator serde class.
          // Just set it to zero.
          val limitBuilder = OperatorOuterClass.Limit
            .newBuilder()
            .setLimit(limit)
            .setOffset(0)
          Some(result.setLimit(limitBuilder).build())
        } else {
          withInfo(op, "No child operator")
          None
        }

      case globalLimitExec: GlobalLimitExec
          if CometConf.COMET_EXEC_GLOBAL_LIMIT_ENABLED.get(conf) =>
        // TODO: We don't support negative limit for now.
        if (childOp.nonEmpty && globalLimitExec.limit >= 0) {
          val limitBuilder = OperatorOuterClass.Limit.newBuilder()

          // TODO: Spark 3.3 might have negative limit (-1) for Offset usage.
          // When we upgrade to Spark 3.3., we need to address it here.
          limitBuilder.setLimit(globalLimitExec.limit)

          Some(result.setLimit(limitBuilder).build())
        } else {
          withInfo(op, "No child operator")
          None
        }

      case ExpandExec(projections, _, child) if CometConf.COMET_EXEC_EXPAND_ENABLED.get(conf) =>
        var allProjExprs: Seq[Expression] = Seq()
        val projExprs = projections.flatMap(_.map(e => {
          allProjExprs = allProjExprs :+ e
          exprToProto(e, child.output)
        }))

        if (projExprs.forall(_.isDefined) && childOp.nonEmpty) {
          val expandBuilder = OperatorOuterClass.Expand
            .newBuilder()
            .addAllProjectList(projExprs.map(_.get).asJava)
            .setNumExprPerProject(projections.head.size)
          Some(result.setExpand(expandBuilder).build())
        } else {
          withInfo(op, allProjExprs: _*)
          None
        }

      case WindowExec(windowExpression, partitionSpec, orderSpec, child)
          if CometConf.COMET_EXEC_WINDOW_ENABLED.get(conf) =>
        val output = child.output

        val winExprs: Array[WindowExpression] = windowExpression.flatMap { expr =>
          expr match {
            case alias: Alias =>
              alias.child match {
                case winExpr: WindowExpression =>
                  Some(winExpr)
                case _ =>
                  None
              }
            case _ =>
              None
          }
        }.toArray

        if (winExprs.length != windowExpression.length) {
          withInfo(op, "Unsupported window expression(s)")
          return None
        }

        if (partitionSpec.nonEmpty && orderSpec.nonEmpty &&
          !validatePartitionAndSortSpecsForWindowFunc(partitionSpec, orderSpec, op)) {
          return None
        }

        val windowExprProto = winExprs.map(windowExprToProto(_, output, op.conf))
        val partitionExprs = partitionSpec.map(exprToProto(_, child.output))

        val sortOrders = orderSpec.map(exprToProto(_, child.output))

        if (windowExprProto.forall(_.isDefined) && partitionExprs.forall(_.isDefined)
          && sortOrders.forall(_.isDefined)) {
          val windowBuilder = OperatorOuterClass.Window.newBuilder()
          windowBuilder.addAllWindowExpr(windowExprProto.map(_.get).toIterable.asJava)
          windowBuilder.addAllPartitionByList(partitionExprs.map(_.get).asJava)
          windowBuilder.addAllOrderByList(sortOrders.map(_.get).asJava)
          Some(result.setWindow(windowBuilder).build())
        } else {
          None
        }

      case aggregate: BaseAggregateExec
          if (aggregate.isInstanceOf[HashAggregateExec] ||
            aggregate.isInstanceOf[ObjectHashAggregateExec]) &&
            CometConf.COMET_EXEC_AGGREGATE_ENABLED.get(conf) =>
        val groupingExpressions = aggregate.groupingExpressions
        val aggregateExpressions = aggregate.aggregateExpressions
        val aggregateAttributes = aggregate.aggregateAttributes
        val resultExpressions = aggregate.resultExpressions
        val child = aggregate.child

        if (groupingExpressions.isEmpty && aggregateExpressions.isEmpty) {
          withInfo(op, "No group by or aggregation")
          return None
        }

        // Aggregate expressions with filter are not supported yet.
        if (aggregateExpressions.exists(_.filter.isDefined)) {
          withInfo(op, "Aggregate expression with filter is not supported")
          return None
        }

        val groupingExprs = groupingExpressions.map(exprToProto(_, child.output))

        // In some of the cases, the aggregateExpressions could be empty.
        // For example, if the aggregate functions only have group by or if the aggregate
        // functions only have distinct aggregate functions:
        //
        // SELECT COUNT(distinct col2), col1 FROM test group by col1
        //  +- HashAggregate (keys =[col1# 6], functions =[count (distinct col2#7)] )
        //    +- Exchange hashpartitioning (col1#6, 10), ENSURE_REQUIREMENTS, [plan_id = 36]
        //      +- HashAggregate (keys =[col1#6], functions =[partial_count (distinct col2#7)] )
        //        +- HashAggregate (keys =[col1#6, col2#7], functions =[] )
        //          +- Exchange hashpartitioning (col1#6, col2#7, 10), ENSURE_REQUIREMENTS, ...
        //            +- HashAggregate (keys =[col1#6, col2#7], functions =[] )
        //              +- FileScan parquet spark_catalog.default.test[col1#6, col2#7] ......
        // If the aggregateExpressions is empty, we only want to build groupingExpressions,
        // and skip processing of aggregateExpressions.
        if (aggregateExpressions.isEmpty) {
          val hashAggBuilder = OperatorOuterClass.HashAggregate.newBuilder()
          hashAggBuilder.addAllGroupingExprs(groupingExprs.map(_.get).asJava)
          val attributes = groupingExpressions.map(_.toAttribute) ++ aggregateAttributes
          val resultExprs = resultExpressions.map(exprToProto(_, attributes))
          if (resultExprs.exists(_.isEmpty)) {
            val msg = s"Unsupported result expressions found in: ${resultExpressions}"
            emitWarning(msg)
            withInfo(op, msg, resultExpressions: _*)
            return None
          }
          hashAggBuilder.addAllResultExprs(resultExprs.map(_.get).asJava)
          Some(result.setHashAgg(hashAggBuilder).build())
        } else {
          val modes = aggregateExpressions.map(_.mode).distinct

          if (modes.size != 1) {
            // This shouldn't happen as all aggregation expressions should share the same mode.
            // Fallback to Spark nevertheless here.
            withInfo(op, "All aggregate expressions do not have the same mode")
            return None
          }

          val mode = modes.head match {
            case Partial => CometAggregateMode.Partial
            case Final => CometAggregateMode.Final
            case _ =>
              withInfo(op, s"Unsupported aggregation mode ${modes.head}")
              return None
          }

          // In final mode, the aggregate expressions are bound to the output of the
          // child and partial aggregate expressions buffer attributes produced by partial
          // aggregation. This is done in Spark `HashAggregateExec` internally. In Comet,
          // we don't have to do this because we don't use the merging expression.
          val binding = mode != CometAggregateMode.Final
          // `output` is only used when `binding` is true (i.e., non-Final)
          val output = child.output

          val aggExprs =
            aggregateExpressions.map(aggExprToProto(_, output, binding, op.conf))
          if (childOp.nonEmpty && groupingExprs.forall(_.isDefined) &&
            aggExprs.forall(_.isDefined)) {
            val hashAggBuilder = OperatorOuterClass.HashAggregate.newBuilder()
            hashAggBuilder.addAllGroupingExprs(groupingExprs.map(_.get).asJava)
            hashAggBuilder.addAllAggExprs(aggExprs.map(_.get).asJava)
            if (mode == CometAggregateMode.Final) {
              val attributes = groupingExpressions.map(_.toAttribute) ++ aggregateAttributes
              val resultExprs = resultExpressions.map(exprToProto(_, attributes))
              if (resultExprs.exists(_.isEmpty)) {
                val msg = s"Unsupported result expressions found in: ${resultExpressions}"
                emitWarning(msg)
                withInfo(op, msg, resultExpressions: _*)
                return None
              }
              hashAggBuilder.addAllResultExprs(resultExprs.map(_.get).asJava)
            }
            hashAggBuilder.setModeValue(mode.getNumber)
            Some(result.setHashAgg(hashAggBuilder).build())
          } else {
            val allChildren: Seq[Expression] =
              groupingExpressions ++ aggregateExpressions ++ aggregateAttributes
            withInfo(op, allChildren: _*)
            None
          }
        }

      case join: HashJoin =>
        // `HashJoin` has only two implementations in Spark, but we check the type of the join to
        // make sure we are handling the correct join type.
        if (!(CometConf.COMET_EXEC_HASH_JOIN_ENABLED.get(conf) &&
            join.isInstanceOf[ShuffledHashJoinExec]) &&
          !(CometConf.COMET_EXEC_BROADCAST_HASH_JOIN_ENABLED.get(conf) &&
            join.isInstanceOf[BroadcastHashJoinExec])) {
          withInfo(join, s"Invalid hash join type ${join.nodeName}")
          return None
        }

        if (join.buildSide == BuildRight && join.joinType == LeftAnti) {
          withInfo(join, "BuildRight with LeftAnti is not supported")
          return None
        }

        val condition = join.condition.map { cond =>
          val condProto = exprToProto(cond, join.left.output ++ join.right.output)
          if (condProto.isEmpty) {
            withInfo(join, cond)
            return None
          }
          condProto.get
        }

        val joinType = join.joinType match {
          case Inner => JoinType.Inner
          case LeftOuter => JoinType.LeftOuter
          case RightOuter => JoinType.RightOuter
          case FullOuter => JoinType.FullOuter
          case LeftSemi => JoinType.LeftSemi
          case LeftAnti => JoinType.LeftAnti
          case _ =>
            // Spark doesn't support other join types
            withInfo(join, s"Unsupported join type ${join.joinType}")
            return None
        }

        val leftKeys = join.leftKeys.map(exprToProto(_, join.left.output))
        val rightKeys = join.rightKeys.map(exprToProto(_, join.right.output))

        if (leftKeys.forall(_.isDefined) &&
          rightKeys.forall(_.isDefined) &&
          childOp.nonEmpty) {
          val joinBuilder = OperatorOuterClass.HashJoin
            .newBuilder()
            .setJoinType(joinType)
            .addAllLeftJoinKeys(leftKeys.map(_.get).asJava)
            .addAllRightJoinKeys(rightKeys.map(_.get).asJava)
            .setBuildSide(
              if (join.buildSide == BuildLeft) BuildSide.BuildLeft else BuildSide.BuildRight)
          condition.foreach(joinBuilder.setCondition)
          Some(result.setHashJoin(joinBuilder).build())
        } else {
          val allExprs: Seq[Expression] = join.leftKeys ++ join.rightKeys
          withInfo(join, allExprs: _*)
          None
        }

      case join: SortMergeJoinExec if CometConf.COMET_EXEC_SORT_MERGE_JOIN_ENABLED.get(conf) =>
        // `requiredOrders` and `getKeyOrdering` are copied from Spark's SortMergeJoinExec.
        def requiredOrders(keys: Seq[Expression]): Seq[SortOrder] = {
          keys.map(SortOrder(_, Ascending))
        }

        def getKeyOrdering(
            keys: Seq[Expression],
            childOutputOrdering: Seq[SortOrder]): Seq[SortOrder] = {
          val requiredOrdering = requiredOrders(keys)
          if (SortOrder.orderingSatisfies(childOutputOrdering, requiredOrdering)) {
            keys.zip(childOutputOrdering).map { case (key, childOrder) =>
              val sameOrderExpressionsSet = ExpressionSet(childOrder.children) - key
              SortOrder(key, Ascending, sameOrderExpressionsSet.toSeq)
            }
          } else {
            requiredOrdering
          }
        }

        if (join.condition.isDefined &&
          !CometConf.COMET_EXEC_SORT_MERGE_JOIN_WITH_JOIN_FILTER_ENABLED
            .get(conf)) {
          withInfo(
            join,
            s"${CometConf.COMET_EXEC_SORT_MERGE_JOIN_WITH_JOIN_FILTER_ENABLED.key} is not enabled",
            join.condition.get)
          return None
        }

        val condition = join.condition.map { cond =>
          val condProto = exprToProto(cond, join.left.output ++ join.right.output)
          if (condProto.isEmpty) {
            withInfo(join, cond)
            return None
          }
          condProto.get
        }

        val joinType = join.joinType match {
          case Inner => JoinType.Inner
          case LeftOuter => JoinType.LeftOuter
          case RightOuter => JoinType.RightOuter
          case FullOuter => JoinType.FullOuter
          case LeftSemi => JoinType.LeftSemi
          // TODO: DF SMJ with join condition fails TPCH q21
          case LeftAnti if condition.isEmpty => JoinType.LeftAnti
          case LeftAnti =>
            withInfo(join, "LeftAnti SMJ join with condition is not supported")
            return None
          case _ =>
            // Spark doesn't support other join types
            withInfo(op, s"Unsupported join type ${join.joinType}")
            return None
        }

        // Checks if the join keys are supported by DataFusion SortMergeJoin.
        val errorMsgs = join.leftKeys.flatMap { key =>
          if (!supportedSortMergeJoinEqualType(key.dataType)) {
            Some(s"Unsupported join key type ${key.dataType} on key: ${key.sql}")
          } else {
            None
          }
        }

        if (errorMsgs.nonEmpty) {
          withInfo(op, errorMsgs.flatten.mkString("\n"))
          return None
        }

        val leftKeys = join.leftKeys.map(exprToProto(_, join.left.output))
        val rightKeys = join.rightKeys.map(exprToProto(_, join.right.output))

        val sortOptions = getKeyOrdering(join.leftKeys, join.left.outputOrdering)
          .map(exprToProto(_, join.left.output))

        if (sortOptions.forall(_.isDefined) &&
          leftKeys.forall(_.isDefined) &&
          rightKeys.forall(_.isDefined) &&
          childOp.nonEmpty) {
          val joinBuilder = OperatorOuterClass.SortMergeJoin
            .newBuilder()
            .setJoinType(joinType)
            .addAllSortOptions(sortOptions.map(_.get).asJava)
            .addAllLeftJoinKeys(leftKeys.map(_.get).asJava)
            .addAllRightJoinKeys(rightKeys.map(_.get).asJava)
          condition.map(joinBuilder.setCondition)
          Some(result.setSortMergeJoin(joinBuilder).build())
        } else {
          val allExprs: Seq[Expression] = join.leftKeys ++ join.rightKeys
          withInfo(join, allExprs: _*)
          None
        }

      case join: SortMergeJoinExec if !CometConf.COMET_EXEC_SORT_MERGE_JOIN_ENABLED.get(conf) =>
        withInfo(join, "SortMergeJoin is not enabled")
        None

      case op if isCometSink(op) && op.output.forall(a => supportedDataType(a.dataType, true)) =>
        // These operators are source of Comet native execution chain
        val scanBuilder = OperatorOuterClass.Scan.newBuilder()
        scanBuilder.setSource(op.simpleStringWithNodeId())

        val scanTypes = op.output.flatten { attr =>
          serializeDataType(attr.dataType)
        }

        if (scanTypes.length == op.output.length) {
          scanBuilder.addAllFields(scanTypes.asJava)

          // Sink operators don't have children
          result.clearChildren()

          Some(result.setScan(scanBuilder).build())
        } else {
          // There are unsupported scan type
          val msg =
            s"unsupported Comet operator: ${op.nodeName}, due to unsupported data types above"
          emitWarning(msg)
          withInfo(op, msg)
          None
        }

      case op =>
        // Emit warning if:
        //  1. it is not Spark shuffle operator, which is handled separately
        //  2. it is not a Comet operator
        if (!op.nodeName.contains("Comet") && !op.isInstanceOf[ShuffleExchangeExec]) {
          val msg = s"unsupported Spark operator: ${op.nodeName}"
          emitWarning(msg)
          withInfo(op, msg)
        }
        None
    }
  }

  /**
   * Whether the input Spark operator `op` can be considered as a Comet sink, i.e., the start of
   * native execution. If it is true, we'll wrap `op` with `CometScanWrapper` or
   * `CometSinkPlaceHolder` later in `CometSparkSessionExtensions` after `operator2proto` is
   * called.
   */
  private def isCometSink(op: SparkPlan): Boolean = {
    op match {
      case s if isCometScan(s) => true
      case _: CometSparkToColumnarExec => true
      case _: CometSinkPlaceHolder => true
      case _: CoalesceExec => true
      case _: CollectLimitExec => true
      case _: UnionExec => true
      case _: ShuffleExchangeExec => true
      case ShuffleQueryStageExec(_, _: CometShuffleExchangeExec, _) => true
      case ShuffleQueryStageExec(_, ReusedExchangeExec(_, _: CometShuffleExchangeExec), _) => true
      case _: TakeOrderedAndProjectExec => true
      case BroadcastQueryStageExec(_, _: CometBroadcastExchangeExec, _) => true
      case _: BroadcastExchangeExec => true
      case _: WindowExec => true
      case _ => false
    }
  }

  /**
   * Checks whether `dt` is a decimal type AND whether Spark version is before 3.4
   */
  private def decimalBeforeSpark34(dt: DataType): Boolean = {
    !isSpark34Plus && (dt match {
      case _: DecimalType => true
      case _ => false
    })
  }

  /**
   * Check if the datatypes of shuffle input are supported. This is used for Columnar shuffle
   * which supports struct/array.
   */
  def supportPartitioningTypes(
      inputs: Seq[Attribute],
      partitioning: Partitioning): (Boolean, String) = {
    def supportedDataType(dt: DataType): Boolean = dt match {
      case _: ByteType | _: ShortType | _: IntegerType | _: LongType | _: FloatType |
          _: DoubleType | _: StringType | _: BinaryType | _: TimestampType | _: DecimalType |
          _: DateType | _: BooleanType =>
        true
      case StructType(fields) =>
        fields.forall(f => supportedDataType(f.dataType)) &&
        // Java Arrow stream reader cannot work on duplicate field name
        fields.map(f => f.name).distinct.length == fields.length
      case ArrayType(ArrayType(_, _), _) => false // TODO: nested array is not supported
      case ArrayType(MapType(_, _, _), _) => false // TODO: map array element is not supported
      case ArrayType(elementType, _) =>
        supportedDataType(elementType)
      case MapType(MapType(_, _, _), _, _) => false // TODO: nested map is not supported
      case MapType(_, MapType(_, _, _), _) => false
      case MapType(StructType(_), _, _) => false // TODO: struct map key/value is not supported
      case MapType(_, StructType(_), _) => false
      case MapType(ArrayType(_, _), _, _) => false // TODO: array map key/value is not supported
      case MapType(_, ArrayType(_, _), _) => false
      case MapType(keyType, valueType, _) =>
        supportedDataType(keyType) && supportedDataType(valueType)
      case _ =>
        false
    }

    var msg = ""
    val supported = partitioning match {
      case HashPartitioning(expressions, _) =>
        val supported =
          expressions.map(QueryPlanSerde.exprToProto(_, inputs)).forall(_.isDefined) &&
            expressions.forall(e => supportedDataType(e.dataType)) &&
            inputs.forall(attr => supportedDataType(attr.dataType))
        if (!supported) {
          msg = s"unsupported Spark partitioning expressions: $expressions"
        }
        supported
      case SinglePartition => inputs.forall(attr => supportedDataType(attr.dataType))
      case RoundRobinPartitioning(_) => inputs.forall(attr => supportedDataType(attr.dataType))
      case RangePartitioning(orderings, _) =>
        val supported =
          orderings.map(QueryPlanSerde.exprToProto(_, inputs)).forall(_.isDefined) &&
            orderings.forall(e => supportedDataType(e.dataType)) &&
            inputs.forall(attr => supportedDataType(attr.dataType))
        if (!supported) {
          msg = s"unsupported Spark partitioning expressions: $orderings"
        }
        supported
      case _ =>
        msg = s"unsupported Spark partitioning: ${partitioning.getClass.getName}"
        false
    }

    if (!supported) {
      emitWarning(msg)
      (false, msg)
    } else {
      (true, null)
    }
  }

  /**
   * Whether the given Spark partitioning is supported by Comet.
   */
  def supportPartitioning(
      inputs: Seq[Attribute],
      partitioning: Partitioning): (Boolean, String) = {
    def supportedDataType(dt: DataType): Boolean = dt match {
      case _: ByteType | _: ShortType | _: IntegerType | _: LongType | _: FloatType |
          _: DoubleType | _: StringType | _: BinaryType | _: TimestampType | _: DecimalType |
          _: DateType | _: BooleanType =>
        true
      case _ =>
        // Native shuffle doesn't support struct/array yet
        false
    }

    var msg = ""
    val supported = partitioning match {
      case HashPartitioning(expressions, _) =>
        val supported =
          expressions.map(QueryPlanSerde.exprToProto(_, inputs)).forall(_.isDefined) &&
            expressions.forall(e => supportedDataType(e.dataType)) &&
            inputs.forall(attr => supportedDataType(attr.dataType))
        if (!supported) {
          msg = s"unsupported Spark partitioning expressions: $expressions"
        }
        supported
      case SinglePartition => inputs.forall(attr => supportedDataType(attr.dataType))
      case _ =>
        msg = s"unsupported Spark partitioning: ${partitioning.getClass.getName}"
        false
    }

    if (!supported) {
      emitWarning(msg)
      (false, msg)
    } else {
      (true, null)
    }
  }

  // Utility method. Adds explain info if the result of calling exprToProto is None
  private def optExprWithInfo(
      optExpr: Option[Expr],
      expr: Expression,
      childExpr: Expression*): Option[Expr] = {
    optExpr match {
      case None =>
        withInfo(expr, childExpr: _*)
        None
      case o => o
    }

  }

  // TODO: Remove this constraint when we upgrade to new arrow-rs including
  // https://github.com/apache/arrow-rs/pull/6225
  def supportedSortType(op: SparkPlan, sortOrder: Seq[SortOrder]): Boolean = {
    def canRank(dt: DataType): Boolean = {
      dt match {
        case _: ByteType | _: ShortType | _: IntegerType | _: LongType | _: FloatType |
            _: DoubleType | _: TimestampType | _: DecimalType | _: DateType =>
          true
        case _: BinaryType | _: StringType => true
        case _ => false
      }
    }

    if (sortOrder.length == 1) {
      val canSort = sortOrder.head.dataType match {
        case _: BooleanType => true
        case _: ByteType | _: ShortType | _: IntegerType | _: LongType | _: FloatType |
            _: DoubleType | _: TimestampType | _: DecimalType | _: DateType =>
          true
        case dt if isTimestampNTZType(dt) => true
        case _: BinaryType | _: StringType => true
        case ArrayType(elementType, _) => canRank(elementType)
        case _ => false
      }
      if (!canSort) {
        withInfo(op, s"Sort on single column of type ${sortOrder.head.dataType} is not supported")
        false
      } else {
        true
      }
    } else {
      true
    }
  }

  private def validatePartitionAndSortSpecsForWindowFunc(
      partitionSpec: Seq[Expression],
      orderSpec: Seq[SortOrder],
      op: SparkPlan): Boolean = {
    if (partitionSpec.length != orderSpec.length) {
      withInfo(op, "Partitioning and sorting specifications do not match")
      return false
    }

    val partitionColumnNames = partitionSpec.collect { case a: AttributeReference =>
      a.name
    }

    val orderColumnNames = orderSpec.collect { case s: SortOrder =>
      s.child match {
        case a: AttributeReference => a.name
      }
    }

    if (partitionColumnNames.zip(orderColumnNames).exists { case (partCol, orderCol) =>
        partCol != orderCol
      }) {
      withInfo(op, "Partitioning and sorting specifications must be the same.")
      return false
    }

    true
  }
}
