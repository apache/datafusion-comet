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
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average, BitAndAgg, BitOrAgg, BitXorAgg, Count, CovPopulation, CovSample, Final, First, Last, Max, Min, Partial, StddevPop, StddevSamp, Sum, VariancePop, VarianceSamp}
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, NormalizeNaNAndZero}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, SinglePartition}
import org.apache.spark.sql.catalyst.util.CharVarcharCodegenUtils
import org.apache.spark.sql.comet.{CometBroadcastExchangeExec, CometRowToColumnarExec, CometSinkPlaceHolder, DecimalPrecision}
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{BroadcastQueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, HashJoin, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.{isCometOperatorEnabled, isCometScan, isSpark32, isSpark34Plus, withInfo}
import org.apache.comet.expressions.{CometCast, Compatible, Incompatible, Unsupported}
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

  def supportedDataType(dt: DataType): Boolean = dt match {
    case _: ByteType | _: ShortType | _: IntegerType | _: LongType | _: FloatType |
        _: DoubleType | _: StringType | _: BinaryType | _: TimestampType | _: DecimalType |
        _: DateType | _: BooleanType | _: NullType =>
      true
    // `TimestampNTZType` is private in Spark 3.2.
    case dt if dt.typeName == "timestamp_ntz" => true
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
      case dt if dt.typeName == "timestamp_ntz" => 11
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

  def aggExprToProto(
      aggExpr: AggregateExpression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[AggExpr] = {
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
      case cov @ CovSample(child1, child2, _) =>
        val child1Expr = exprToProto(child1, inputs, binding)
        val child2Expr = exprToProto(child2, inputs, binding)
        val dataType = serializeDataType(cov.dataType)

        if (child1Expr.isDefined && child2Expr.isDefined && dataType.isDefined) {
          val covBuilder = ExprOuterClass.CovSample.newBuilder()
          covBuilder.setChild1(child1Expr.get)
          covBuilder.setChild2(child2Expr.get)
          covBuilder.setDatatype(dataType.get)

          Some(
            ExprOuterClass.AggExpr
              .newBuilder()
              .setCovSample(covBuilder)
              .build())
        } else {
          None
        }
      case cov @ CovPopulation(child1, child2, _) =>
        val child1Expr = exprToProto(child1, inputs, binding)
        val child2Expr = exprToProto(child2, inputs, binding)
        val dataType = serializeDataType(cov.dataType)

        if (child1Expr.isDefined && child2Expr.isDefined && dataType.isDefined) {
          val covBuilder = ExprOuterClass.CovPopulation.newBuilder()
          covBuilder.setChild1(child1Expr.get)
          covBuilder.setChild2(child2Expr.get)
          covBuilder.setDatatype(dataType.get)

          Some(
            ExprOuterClass.AggExpr
              .newBuilder()
              .setCovPopulation(covBuilder)
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
      case std @ StddevPop(child, nullOnDivideByZero) =>
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
      case fn =>
        val msg = s"unsupported Spark aggregate function: ${fn.prettyName}"
        emitWarning(msg)
        withInfo(aggExpr, msg, fn.children: _*)
        None
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
        evalMode: String): Option[Expr] = {
      val dataType = serializeDataType(dt)

      if (childExpr.isDefined && dataType.isDefined) {
        val castBuilder = ExprOuterClass.Cast.newBuilder()
        castBuilder.setChild(childExpr.get)
        castBuilder.setDatatype(dataType.get)
        castBuilder.setEvalMode(evalMode)

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
          actualEvalModeStr: String): Option[Expr] = {

        val childExpr = exprToProtoInternal(child, inputs)
        if (childExpr.isDefined) {
          val castSupport =
            CometCast.isSupported(child.dataType, dt, timeZoneId, actualEvalModeStr)

          def getIncompatMessage(reason: Option[String]): String =
            "Comet does not guarantee correct results for cast " +
              s"from ${child.dataType} to $dt " +
              s"with timezone $timeZoneId and evalMode $actualEvalModeStr" +
              reason.map(str => s" ($str)").getOrElse("")

          castSupport match {
            case Compatible(_) =>
              castToProto(timeZoneId, dt, childExpr, actualEvalModeStr)
            case Incompatible(reason) =>
              if (CometConf.COMET_CAST_ALLOW_INCOMPATIBLE.get()) {
                logWarning(getIncompatMessage(reason))
                castToProto(timeZoneId, dt, childExpr, actualEvalModeStr)
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
                  s"with timezone $timeZoneId and evalMode $actualEvalModeStr")
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
          handleCast(child, inputs, expr.dataType, Some(timeZoneId), "TRY")

        case Cast(child, dt, timeZoneId, evalMode) =>
          val evalModeStr = if (evalMode.isInstanceOf[Boolean]) {
            // Spark 3.2 & 3.3 has ansiEnabled boolean
            if (evalMode.asInstanceOf[Boolean]) "ANSI" else "LEGACY"
          } else {
            // Spark 3.4+ has EvalMode enum with values LEGACY, ANSI, and TRY
            evalMode.toString
          }
          handleCast(child, inputs, dt, timeZoneId, evalModeStr)

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

        case Literal(value, dataType) if supportedDataType(dataType) =>
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

        case Like(left, right, _) =>
          // TODO escapeChar
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

        // TODO waiting for arrow-rs update
//      case RLike(left, right) =>
//        val leftExpr = exprToProtoInternal(left, inputs)
//        val rightExpr = exprToProtoInternal(right, inputs)
//
//        if (leftExpr.isDefined && rightExpr.isDefined) {
//          val builder = ExprOuterClass.RLike.newBuilder()
//          builder.setLeft(leftExpr.get)
//          builder.setRight(rightExpr.get)
//
//          Some(
//            ExprOuterClass.Expr
//              .newBuilder()
//              .setRlike(builder)
//              .build())
//        } else {
//          None
//        }

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
                    .setEvalMode("LEGACY") // year is not affected by ANSI mode
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
          // `UnaryExpression` includes `PromotePrecision` for Spark 3.2 & 3.3
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
              val boundRef = BindReferences
                .bindReference(attr, inputs, allowFailures = false)
                .asInstanceOf[BoundReference]
              val boundExpr = ExprOuterClass.BoundReference
                .newBuilder()
                .setIndex(boundRef.ordinal)
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

        case Abs(child, _) =>
          val childExpr = exprToProtoInternal(child, inputs)
          if (childExpr.isDefined) {
            val abs =
              ExprOuterClass.Abs
                .newBuilder()
                .setChild(childExpr.get)
                .build()
            Some(Expr.newBuilder().setAbs(abs).build())
          } else {
            withInfo(expr, child)
            None
          }

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

        case e: Unhex if !isSpark32 =>
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

        case Log(child) =>
          val childExpr = exprToProtoInternal(child, inputs)
          val optExpr = scalarExprToProto("ln", childExpr)
          optExprWithInfo(optExpr, expr, child)

        case Log10(child) =>
          val childExpr = exprToProtoInternal(child, inputs)
          val optExpr = scalarExprToProto("log10", childExpr)
          optExprWithInfo(optExpr, expr, child)

        case Log2(child) =>
          val childExpr = exprToProtoInternal(child, inputs)
          val optExpr = scalarExprToProto("log2", childExpr)
          optExprWithInfo(optExpr, expr, child)

        case Pow(left, right) =>
          val leftExpr = exprToProtoInternal(left, inputs)
          val rightExpr = exprToProtoInternal(right, inputs)
          val optExpr = scalarExprToProto("pow", leftExpr, rightExpr)
          optExprWithInfo(optExpr, expr, left, right)

        // round function for Spark 3.2 does not allow negative round target scale. In addition,
        // it has different result precision/scale for decimals. Supporting only 3.3 and above.
        case r: Round if !isSpark32 =>
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

        case Signum(child) =>
          val childExpr = exprToProtoInternal(child, inputs)
          val optExpr = scalarExprToProto("signum", childExpr)
          optExprWithInfo(optExpr, expr, child)

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

        case Lower(child) =>
          val castExpr = Cast(child, StringType)
          val childExpr = exprToProtoInternal(castExpr, inputs)
          val optExpr = scalarExprToProto("lower", childExpr)
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
          val castExpr = Cast(child, StringType)
          val childExpr = exprToProtoInternal(castExpr, inputs)
          val optExpr = scalarExprToProto("upper", childExpr)
          optExprWithInfo(optExpr, expr, castExpr)

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

        case UnaryMinus(child, _) =>
          val childExpr = exprToProtoInternal(child, inputs)
          if (childExpr.isDefined) {
            val builder = ExprOuterClass.Negative.newBuilder()
            builder.setChild(childExpr.get)
            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setNegative(builder)
                .build())
          } else {
            withInfo(expr, child)
            None
          }

        case a @ Coalesce(_) =>
          val exprChildren = a.children.map(exprToProtoInternal(_, inputs))
          val childExpr = scalarExprToProto("coalesce", exprChildren: _*)
          // TODO: Remove this once we have new DataFusion release which includes
          // the fix: https://github.com/apache/arrow-datafusion/pull/9459
          if (childExpr.isDefined) {
            castToProto(None, a.dataType, childExpr, "LEGACY")
          } else {
            withInfo(expr, a.children: _*)
            None
          }

        // With Spark 3.4, CharVarcharCodegenUtils.readSidePadding gets called to pad spaces for
        // char types. Use rpad to achieve the behavior.
        // See https://github.com/apache/spark/pull/38151
        case StaticInvoke(
              _: Class[CharVarcharCodegenUtils],
              _: StringType,
              "readSidePadding",
              arguments,
              _,
              true,
              false,
              true) if arguments.size == 2 =>
          val argsExpr = Seq(
            exprToProtoInternal(Cast(arguments(0), StringType), inputs),
            exprToProtoInternal(arguments(1), inputs))

          if (argsExpr.forall(_.isDefined)) {
            val builder = ExprOuterClass.ScalarFunc.newBuilder()
            builder.setFunc("rpad")
            argsExpr.foreach(arg => builder.addArgs(arg.get))

            Some(ExprOuterClass.Expr.newBuilder().setScalarFunc(builder).build())
          } else {
            withInfo(expr, arguments: _*)
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

        case s @ execution.ScalarSubquery(_, _) =>
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

        case b @ BinaryExpression(_, _) if isBloomFilterMightContain(b) =>
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
        optExprWithInfo(optExpr, expr, null, srcCast, trimCast)
      } else {
        val optExpr = scalarExprToProto(trimType, srcExpr)
        optExprWithInfo(optExpr, expr, null, srcCast)
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
    new NullIf(expression, Literal.default(expression.dataType)).child
  } else {
    expression
  }

  /**
   * Returns true if given datatype is supported as a key in DataFusion sort merge join.
   */
  def supportedSortMergeJoinEqualType(dataType: DataType): Boolean = dataType match {
    case _: ByteType | _: ShortType | _: IntegerType | _: LongType | _: FloatType |
        _: DoubleType | _: StringType | _: DateType | _: DecimalType | _: BooleanType =>
      true
    // `TimestampNTZType` is private in Spark 3.2/3.3.
    case dt if dt.typeName == "timestamp_ntz" => true
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
    val result = OperatorOuterClass.Operator.newBuilder()
    childOp.foreach(result.addChildren)

    op match {
      case ProjectExec(projectList, child) if isCometOperatorEnabled(op.conf, "project") =>
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

      case FilterExec(condition, child) if isCometOperatorEnabled(op.conf, "filter") =>
        val cond = exprToProto(condition, child.output)

        if (cond.isDefined && childOp.nonEmpty) {
          val filterBuilder = OperatorOuterClass.Filter.newBuilder().setPredicate(cond.get)
          Some(result.setFilter(filterBuilder).build())
        } else {
          withInfo(op, condition, child)
          None
        }

      case SortExec(sortOrder, _, child, _) if isCometOperatorEnabled(op.conf, "sort") =>
        val sortOrders = sortOrder.map(exprToProto(_, child.output))

        if (sortOrders.forall(_.isDefined) && childOp.nonEmpty) {
          val sortBuilder = OperatorOuterClass.Sort
            .newBuilder()
            .addAllSortOrders(sortOrders.map(_.get).asJava)
          Some(result.setSort(sortBuilder).build())
        } else {
          withInfo(op, sortOrder: _*)
          None
        }

      case LocalLimitExec(limit, _) if isCometOperatorEnabled(op.conf, "local_limit") =>
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

      case globalLimitExec: GlobalLimitExec if isCometOperatorEnabled(op.conf, "global_limit") =>
        // TODO: We don't support negative limit for now.
        if (childOp.nonEmpty && globalLimitExec.limit >= 0) {
          val limitBuilder = OperatorOuterClass.Limit.newBuilder()

          // Spark 3.2 doesn't support offset for GlobalLimit, but newer Spark versions
          // support it. Before we upgrade to Spark 3.3, just set it zero.
          // TODO: Spark 3.3 might have negative limit (-1) for Offset usage.
          // When we upgrade to Spark 3.3., we need to address it here.
          limitBuilder.setLimit(globalLimitExec.limit)
          limitBuilder.setOffset(0)

          Some(result.setLimit(limitBuilder).build())
        } else {
          withInfo(op, "No child operator")
          None
        }

      case ExpandExec(projections, _, child) if isCometOperatorEnabled(op.conf, "expand") =>
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

      case HashAggregateExec(
            _,
            _,
            _,
            groupingExpressions,
            aggregateExpressions,
            aggregateAttributes,
            _,
            resultExpressions,
            child) if isCometOperatorEnabled(op.conf, "aggregate") =>
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
            aggregateExpressions.map(aggExprToProto(_, output, binding))
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
        if (!(isCometOperatorEnabled(op.conf, "hash_join") &&
            join.isInstanceOf[ShuffledHashJoinExec]) &&
          !(isCometOperatorEnabled(op.conf, "broadcast_hash_join") &&
            join.isInstanceOf[BroadcastHashJoinExec])) {
          withInfo(join, s"Invalid hash join type ${join.nodeName}")
          return None
        }

        if (join.buildSide == BuildRight && join.joinType == LeftAnti) {
          // DataFusion HashJoin LeftAnti has bugs on null keys.
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

      case join: SortMergeJoinExec if isCometOperatorEnabled(op.conf, "sort_merge_join") =>
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

        // TODO: Support SortMergeJoin with join condition after new DataFusion release
        if (join.condition.isDefined) {
          withInfo(op, "Sort merge join with a join condition is not supported")
          return None
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
          Some(result.setSortMergeJoin(joinBuilder).build())
        } else {
          val allExprs: Seq[Expression] = join.leftKeys ++ join.rightKeys
          withInfo(join, allExprs: _*)
          None
        }

      case join: SortMergeJoinExec if !isCometOperatorEnabled(op.conf, "sort_merge_join") =>
        withInfo(join, "SortMergeJoin is not enabled")
        None

      case op if isCometSink(op) && op.output.forall(a => supportedDataType(a.dataType)) =>
        // These operators are source of Comet native execution chain
        val scanBuilder = OperatorOuterClass.Scan.newBuilder()

        val scanTypes = op.output.flatten { attr =>
          serializeDataType(attr.dataType)
        }

        if (scanTypes.length == op.output.length) {
          scanBuilder.addAllFields(scanTypes.asJava)

          // Sink operators don't have children
          result.clearChildren()

          scanBuilder.setName(op.nodeName)

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
      case _: CometRowToColumnarExec => true
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
  def supportPartitioningTypes(inputs: Seq[Attribute]): (Boolean, String) = {
    def supportedDataType(dt: DataType): Boolean = dt match {
      case _: ByteType | _: ShortType | _: IntegerType | _: LongType | _: FloatType |
          _: DoubleType | _: StringType | _: BinaryType | _: TimestampType | _: DecimalType |
          _: DateType | _: BooleanType =>
        true
      case StructType(fields) =>
        fields.forall(f => supportedDataType(f.dataType))
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

    // Check if the datatypes of shuffle input are supported.
    var msg = ""
    val supported = inputs.forall(attr => supportedDataType(attr.dataType))
    if (!supported) {
      msg = s"unsupported Spark partitioning: ${inputs.map(_.dataType)}"
      emitWarning(msg)
    }
    (supported, msg)
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

    // Check if the datatypes of shuffle input are supported.
    val supported = inputs.forall(attr => supportedDataType(attr.dataType))

    if (!supported) {
      val msg = s"unsupported Spark partitioning: ${inputs.map(_.dataType)}"
      emitWarning(msg)
      (false, msg)
    } else {
      partitioning match {
        case HashPartitioning(expressions, _) =>
          (expressions.map(QueryPlanSerde.exprToProto(_, inputs)).forall(_.isDefined), null)
        case SinglePartition => (true, null)
        case other =>
          val msg = s"unsupported Spark partitioning: ${other.getClass.getName}"
          emitWarning(msg)
          (false, msg)
      }
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
}
