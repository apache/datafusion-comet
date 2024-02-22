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
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average, Count, Final, First, Last, Max, Min, Partial, Sum}
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.optimizer.NormalizeNaNAndZero
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, SinglePartition}
import org.apache.spark.sql.catalyst.util.CharVarcharCodegenUtils
import org.apache.spark.sql.comet.{CometHashAggregateExec, CometPlan, CometSinkPlaceHolder, DecimalPrecision}
import org.apache.spark.sql.execution
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import org.apache.comet.CometSparkSessionExtensions.{isCometOperatorEnabled, isCometScan, isSpark32, isSpark34Plus}
import org.apache.comet.serde.ExprOuterClass.{AggExpr, DataType => ProtoDataType, Expr, ScalarFunc}
import org.apache.comet.serde.ExprOuterClass.DataType.{DataTypeInfo, DecimalInfo, ListInfo, MapInfo, StructInfo}
import org.apache.comet.serde.OperatorOuterClass.{AggregateMode => CometAggregateMode, Operator}
import org.apache.comet.shims.ShimQueryPlanSerde

/**
 * An utility object for query plan and expression serialization.
 */
object QueryPlanSerde extends Logging with ShimQueryPlanSerde {
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
      case _: NumericType | DateType | TimestampType => true
      case _ => false
    }
  }

  def aggExprToProto(aggExpr: AggregateExpression, inputs: Seq[Attribute]): Option[AggExpr] = {
    aggExpr.aggregateFunction match {
      case s @ Sum(child, _) if sumDataTypeSupported(s.dataType) =>
        val childExpr = exprToProto(child, inputs)
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
          None
        }
      case s @ Average(child, _) if avgDataTypeSupported(s.dataType) =>
        val childExpr = exprToProto(child, inputs)
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
        } else {
          None
        }
      case Count(children) =>
        val exprChildren = children.map(exprToProto(_, inputs))

        if (exprChildren.forall(_.isDefined)) {
          val countBuilder = ExprOuterClass.Count.newBuilder()
          countBuilder.addAllChildren(exprChildren.map(_.get).asJava)

          Some(
            ExprOuterClass.AggExpr
              .newBuilder()
              .setCount(countBuilder)
              .build())
        } else {
          None
        }
      case min @ Min(child) if minMaxDataTypeSupported(min.dataType) =>
        val childExpr = exprToProto(child, inputs)
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
        } else {
          None
        }
      case max @ Max(child) if minMaxDataTypeSupported(max.dataType) =>
        val childExpr = exprToProto(child, inputs)
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
        } else {
          None
        }
      case first @ First(child, ignoreNulls)
          if !ignoreNulls => // DataFusion doesn't support ignoreNulls true
        val childExpr = exprToProto(child, inputs)
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
        } else {
          None
        }
      case last @ Last(child, ignoreNulls)
          if !ignoreNulls => // DataFusion doesn't support ignoreNulls true
        val childExpr = exprToProto(child, inputs)
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
        } else {
          None
        }

      case fn =>
        emitWarning(s"unsupported Spark aggregate function: $fn")
        None
    }
  }

  def exprToProto(expr: Expression, input: Seq[Attribute]): Option[Expr] = {
    val conf = SQLConf.get
    val newExpr =
      DecimalPrecision.promote(conf.decimalOperationsAllowPrecisionLoss, expr, !conf.ansiEnabled)
    exprToProtoInternal(newExpr, input)
  }

  def exprToProtoInternal(expr: Expression, inputs: Seq[Attribute]): Option[Expr] = {
    SQLConf.get
    expr match {
      case a @ Alias(_, _) =>
        exprToProtoInternal(a.child, inputs)

      case cast @ Cast(_: Literal, dataType, _, _) =>
        // This can happen after promoting decimal precisions
        val value = cast.eval()
        exprToProtoInternal(Literal(value, dataType), inputs)

      case Cast(child, dt, timeZoneId, _) =>
        val childExpr = exprToProtoInternal(child, inputs)
        val dataType = serializeDataType(dt)

        if (childExpr.isDefined && dataType.isDefined) {
          val castBuilder = ExprOuterClass.Cast.newBuilder()
          castBuilder.setChild(childExpr.get)
          castBuilder.setDatatype(dataType.get)

          val timeZone = timeZoneId.getOrElse("UTC")
          castBuilder.setTimezone(timeZone)

          Some(
            ExprOuterClass.Expr
              .newBuilder()
              .setCast(castBuilder)
              .build())
        } else {
          None
        }

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
          None
        }

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
          None
        }

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
          None
        }

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
          None
        }

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
          None
        }

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
          None
        }

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
          None
        }

      case Year(child) =>
        val periodType = exprToProtoInternal(Literal("year"), inputs)
        val childExpr = exprToProtoInternal(child, inputs)
        scalarExprToProto("datepart", Seq(periodType, childExpr): _*)
          .map(e => {
            Expr
              .newBuilder()
              .setCast(
                ExprOuterClass.Cast
                  .newBuilder()
                  .setChild(e)
                  .setDatatype(serializeDataType(IntegerType).get)
                  .build())
              .build()
          })

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
          None
        }

      case attr: AttributeReference =>
        val dataType = serializeDataType(attr.dataType)

        if (dataType.isDefined) {
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
          None
        }

      case Abs(child, _) =>
        exprToProtoInternal(child, inputs).map(childExpr => {
          val abs =
            ExprOuterClass.Abs
              .newBuilder()
              .setChild(childExpr)
              .build()
          Expr.newBuilder().setAbs(abs).build()
        })

      case Acos(child) =>
        val childExpr = exprToProtoInternal(child, inputs)
        scalarExprToProto("acos", childExpr)

      case Asin(child) =>
        val childExpr = exprToProtoInternal(child, inputs)
        scalarExprToProto("asin", childExpr)

      case Atan(child) =>
        val childExpr = exprToProtoInternal(child, inputs)
        scalarExprToProto("atan", childExpr)

      case Atan2(left, right) =>
        val leftExpr = exprToProtoInternal(left, inputs)
        val rightExpr = exprToProtoInternal(right, inputs)
        scalarExprToProto("atan2", leftExpr, rightExpr)

      case e @ Ceil(child) =>
        val childExpr = exprToProtoInternal(child, inputs)
        child.dataType match {
          case t: DecimalType if t.scale == 0 => // zero scale is no-op
            childExpr
          case t: DecimalType if t.scale < 0 => // Spark disallows negative scale SPARK-30252
            None
          case _ =>
            scalarExprToProtoWithReturnType("ceil", e.dataType, childExpr)
        }

      case Cos(child) =>
        val childExpr = exprToProtoInternal(child, inputs)
        scalarExprToProto("cos", childExpr)

      case Exp(child) =>
        val childExpr = exprToProtoInternal(child, inputs)
        scalarExprToProto("exp", childExpr)

      case e @ Floor(child) =>
        val childExpr = exprToProtoInternal(child, inputs)
        child.dataType match {
          case t: DecimalType if t.scale == 0 => // zero scale is no-op
            childExpr
          case t: DecimalType if t.scale < 0 => // Spark disallows negative scale SPARK-30252
            None
          case _ =>
            scalarExprToProtoWithReturnType("floor", e.dataType, childExpr)
        }

      case Log(child) =>
        val childExpr = exprToProtoInternal(child, inputs)
        scalarExprToProto("ln", childExpr)

      case Log10(child) =>
        val childExpr = exprToProtoInternal(child, inputs)
        scalarExprToProto("log10", childExpr)

      case Log2(child) =>
        val childExpr = exprToProtoInternal(child, inputs)
        scalarExprToProto("log2", childExpr)

      case Pow(left, right) =>
        val leftExpr = exprToProtoInternal(left, inputs)
        val rightExpr = exprToProtoInternal(right, inputs)
        scalarExprToProto("pow", leftExpr, rightExpr)

      // round function for Spark 3.2 does not allow negative round target scale. In addition,
      // it has different result precision/scale for decimals. Supporting only 3.3 and above.
      case r: Round if !isSpark32 =>
        // _scale s a constant, copied from Spark's RoundBase because it is a protected val
        val scaleV: Any = r.scale.eval(EmptyRow)
        val _scale: Int = scaleV.asInstanceOf[Int]

        lazy val childExpr = exprToProtoInternal(r.child, inputs)
        r.child.dataType match {
          case t: DecimalType if t.scale < 0 => // Spark disallows negative scale SPARK-30252
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
            None
          case _ =>
            // `scale` must be Int64 type in DataFusion
            val scaleExpr = exprToProtoInternal(Literal(_scale.toLong, LongType), inputs)
            scalarExprToProtoWithReturnType("round", r.dataType, childExpr, scaleExpr)
        }

      case Signum(child) =>
        val childExpr = exprToProtoInternal(child, inputs)
        scalarExprToProto("signum", childExpr)

      case Sin(child) =>
        val childExpr = exprToProtoInternal(child, inputs)
        scalarExprToProto("sin", childExpr)

      case Sqrt(child) =>
        val childExpr = exprToProtoInternal(child, inputs)
        scalarExprToProto("sqrt", childExpr)

      case Tan(child) =>
        val childExpr = exprToProtoInternal(child, inputs)
        scalarExprToProto("tan", childExpr)

      case Ascii(child) =>
        val childExpr = exprToProtoInternal(Cast(child, StringType), inputs)
        scalarExprToProto("ascii", childExpr)

      case BitLength(child) =>
        val childExpr = exprToProtoInternal(Cast(child, StringType), inputs)
        scalarExprToProto("bit_length", childExpr)

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
          None
        }

      case CaseWhen(branches, elseValue) =>
        val whenSeq = branches.map(elements => exprToProtoInternal(elements._1, inputs))
        val thenSeq = branches.map(elements => exprToProtoInternal(elements._2, inputs))
        assert(whenSeq.length == thenSeq.length)
        if (whenSeq.forall(_.isDefined) && thenSeq.forall(_.isDefined)) {
          val builder = ExprOuterClass.CaseWhen.newBuilder()
          builder.addAllWhen(whenSeq.map(_.get).asJava)
          builder.addAllThen(thenSeq.map(_.get).asJava)
          if (elseValue.isDefined) {
            val elseValueExpr = exprToProtoInternal(elseValue.get, inputs)
            if (elseValueExpr.isDefined) {
              builder.setElseExpr(elseValueExpr.get)
            } else {
              return None
            }
          }
          Some(
            ExprOuterClass.Expr
              .newBuilder()
              .setCaseWhen(builder)
              .build())
        } else {
          None
        }

      case ConcatWs(children) =>
        val exprs = children.map(e => exprToProtoInternal(Cast(e, StringType), inputs))
        scalarExprToProto("concat_ws", exprs: _*)

      case Chr(child) =>
        val childExpr = exprToProtoInternal(child, inputs)
        scalarExprToProto("chr", childExpr)

      case InitCap(child) =>
        val childExpr = exprToProtoInternal(Cast(child, StringType), inputs)
        scalarExprToProto("initcap", childExpr)

      case Length(child) =>
        val childExpr = exprToProtoInternal(Cast(child, StringType), inputs)
        scalarExprToProto("length", childExpr)

      case Lower(child) =>
        val childExpr = exprToProtoInternal(Cast(child, StringType), inputs)
        scalarExprToProto("lower", childExpr)

      case Md5(child) =>
        val childExpr = exprToProtoInternal(Cast(child, StringType), inputs)
        scalarExprToProto("md5", childExpr)

      case OctetLength(child) =>
        val childExpr = exprToProtoInternal(Cast(child, StringType), inputs)
        scalarExprToProto("octet_length", childExpr)

      case Reverse(child) =>
        val childExpr = exprToProtoInternal(Cast(child, StringType), inputs)
        scalarExprToProto("reverse", childExpr)

      case StringInstr(str, substr) =>
        val leftExpr = exprToProtoInternal(Cast(str, StringType), inputs)
        val rightExpr = exprToProtoInternal(Cast(substr, StringType), inputs)
        scalarExprToProto("strpos", leftExpr, rightExpr)

      case StringRepeat(str, times) =>
        val leftExpr = exprToProtoInternal(Cast(str, StringType), inputs)
        val rightExpr = exprToProtoInternal(Cast(times, LongType), inputs)
        scalarExprToProto("repeat", leftExpr, rightExpr)

      case StringReplace(src, search, replace) =>
        val srcExpr = exprToProtoInternal(Cast(src, StringType), inputs)
        val searchExpr = exprToProtoInternal(Cast(search, StringType), inputs)
        val replaceExpr = exprToProtoInternal(Cast(replace, StringType), inputs)
        scalarExprToProto("replace", srcExpr, searchExpr, replaceExpr)

      case StringTranslate(src, matching, replace) =>
        val srcExpr = exprToProtoInternal(Cast(src, StringType), inputs)
        val matchingExpr = exprToProtoInternal(Cast(matching, StringType), inputs)
        val replaceExpr = exprToProtoInternal(Cast(replace, StringType), inputs)
        scalarExprToProto("translate", srcExpr, matchingExpr, replaceExpr)

      case StringTrim(srcStr, trimStr) =>
        trim(srcStr, trimStr, inputs, "trim")

      case StringTrimLeft(srcStr, trimStr) =>
        trim(srcStr, trimStr, inputs, "ltrim")

      case StringTrimRight(srcStr, trimStr) =>
        trim(srcStr, trimStr, inputs, "rtrim")

      case StringTrimBoth(srcStr, trimStr, _) =>
        trim(srcStr, trimStr, inputs, "btrim")

      case Upper(child) =>
        val childExpr = exprToProtoInternal(Cast(child, StringType), inputs)
        scalarExprToProto("upper", childExpr)

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
          None
        }

      case ShiftRight(left, right) =>
        val leftExpr = exprToProtoInternal(left, inputs)
        val rightExpr = exprToProtoInternal(right, inputs)

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
          None
        }

      case ShiftLeft(left, right) =>
        val leftExpr = exprToProtoInternal(left, inputs)
        val rightExpr = exprToProtoInternal(right, inputs)

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
          None
        }

      case In(value, list) =>
        in(value, list, inputs, false)

      case InSet(value, hset) =>
        val valueDataType = value.dataType
        val list = hset.map { setVal =>
          Literal(setVal, valueDataType)
        }.toSeq
        // Change `InSet` to `In` expression
        // We do Spark `InSet` optimization in native (DataFusion) side.
        in(value, list, inputs, false)

      case Not(In(value, list)) =>
        in(value, list, inputs, true)

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
          None
        }

      case a @ Coalesce(_) =>
        val exprChildren = a.children.map(exprToProtoInternal(_, inputs))
        scalarExprToProto("coalesce", exprChildren: _*)

      // With Spark 3.4, CharVarcharCodegenUtils.readSidePadding gets called to pad spaces for char
      // types. Use rpad to achieve the behavior. See https://github.com/apache/spark/pull/38151
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
          None
        }

      case KnownFloatingPointNormalized(NormalizeNaNAndZero(expr)) =>
        val dataType = serializeDataType(expr.dataType)
        if (dataType.isEmpty) {
          return None
        }
        exprToProtoInternal(expr, inputs).map { child =>
          val builder = ExprOuterClass.NormalizeNaNAndZero
            .newBuilder()
            .setChild(child)
            .setDatatype(dataType.get)
          ExprOuterClass.Expr.newBuilder().setNormalizeNanAndZero(builder).build()
        }

      case s @ execution.ScalarSubquery(_, _) =>
        val dataType = serializeDataType(s.dataType)
        if (dataType.isEmpty) {
          return None
        }

        val builder = ExprOuterClass.Subquery
          .newBuilder()
          .setId(s.exprId.id)
          .setDatatype(dataType.get)
        Some(ExprOuterClass.Expr.newBuilder().setSubquery(builder).build())

      case UnscaledValue(child) =>
        val childExpr = exprToProtoInternal(child, inputs)
        scalarExprToProtoWithReturnType("unscaled_value", LongType, childExpr)

      case MakeDecimal(child, precision, scale, true) =>
        val childExpr = exprToProtoInternal(child, inputs)
        scalarExprToProtoWithReturnType("make_decimal", DecimalType(precision, scale), childExpr)

      case e =>
        emitWarning(s"unsupported Spark expression: '$e' of class '${e.getClass.getName}")
        None
    }
  }

  private def trim(
      srcStr: Expression,
      trimStr: Option[Expression],
      inputs: Seq[Attribute],
      trimType: String): Option[Expr] = {
    val srcExpr = exprToProtoInternal(Cast(srcStr, StringType), inputs)
    if (trimStr.isDefined) {
      val trimExpr = exprToProtoInternal(Cast(trimStr.get, StringType), inputs)
      scalarExprToProto(trimType, srcExpr, trimExpr)
    } else {
      scalarExprToProto(trimType, srcExpr)
    }
  }

  private def in(
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
      None
    }
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
      case _ => return None
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
          None
        }

      case FilterExec(condition, child) if isCometOperatorEnabled(op.conf, "filter") =>
        val cond = exprToProto(condition, child.output)

        if (cond.isDefined && childOp.nonEmpty) {
          val filterBuilder = OperatorOuterClass.Filter.newBuilder().setPredicate(cond.get)
          Some(result.setFilter(filterBuilder).build())
        } else {
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
          None
        }

      case globalLimitExec: GlobalLimitExec if isCometOperatorEnabled(op.conf, "global_limit") =>
        if (childOp.nonEmpty) {
          val limitBuilder = OperatorOuterClass.Limit.newBuilder()

          // Spark 3.2 doesn't support offset for GlobalLimit, but newer Spark versions
          // support it. Before we upgrade to Spark 3.3, just set it zero.
          // TODO: Spark 3.3 might have negative limit (-1) for Offset usage.
          // When we upgrade to Spark 3.3., we need to address it here.
          assert(globalLimitExec.limit >= 0, "limit should be greater or equal to zero")
          limitBuilder.setLimit(globalLimitExec.limit)
          limitBuilder.setOffset(0)

          Some(result.setLimit(limitBuilder).build())
        } else {
          None
        }

      case ExpandExec(projections, _, child) if isCometOperatorEnabled(op.conf, "expand") =>
        val projExprs = projections.flatMap(_.map(exprToProto(_, child.output)))

        if (projExprs.forall(_.isDefined) && childOp.nonEmpty) {
          val expandBuilder = OperatorOuterClass.Expand
            .newBuilder()
            .addAllProjectList(projExprs.map(_.get).asJava)
            .setNumExprPerProject(projections.head.size)
          Some(result.setExpand(expandBuilder).build())
        } else {
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
          Some(result.setHashAgg(hashAggBuilder).build())
        } else {
          val modes = aggregateExpressions.map(_.mode).distinct

          if (modes.size != 1) {
            // This shouldn't happen as all aggregation expressions should share the same mode.
            // Fallback to Spark nevertheless here.
            return None
          }

          val mode = modes.head match {
            case Partial => CometAggregateMode.Partial
            case Final => CometAggregateMode.Final
            case _ => return None
          }

          val output = mode match {
            case CometAggregateMode.Partial => child.output
            case CometAggregateMode.Final =>
              // Assuming `Final` always follows `Partial` aggregation, this find the first
              // `Partial` aggregation and get the input attributes from it.
              // During finding partial aggregation, we must ensure all traversed op are
              // native operators. If not, we should fallback to Spark.
              var seenNonNativeOp = false
              var partialAggInput: Option[Seq[Attribute]] = None
              child.transformDown {
                case op if !op.isInstanceOf[CometPlan] =>
                  seenNonNativeOp = true
                  op
                case op @ CometHashAggregateExec(_, _, _, _, input, Some(Partial), _, _) =>
                  if (!seenNonNativeOp && partialAggInput.isEmpty) {
                    partialAggInput = Some(input)
                  }
                  op
              }

              if (partialAggInput.isDefined) {
                partialAggInput.get
              } else {
                return None
              }
            case _ => return None
          }

          val aggExprs = aggregateExpressions.map(aggExprToProto(_, output))
          if (childOp.nonEmpty && groupingExprs.forall(_.isDefined) &&
            aggExprs.forall(_.isDefined)) {
            val hashAggBuilder = OperatorOuterClass.HashAggregate.newBuilder()
            hashAggBuilder.addAllGroupingExprs(groupingExprs.map(_.get).asJava)
            hashAggBuilder.addAllAggExprs(aggExprs.map(_.get).asJava)
            if (mode == CometAggregateMode.Final) {
              val attributes = groupingExpressions.map(_.toAttribute) ++ aggregateAttributes
              val resultExprs = resultExpressions.map(exprToProto(_, attributes))
              if (resultExprs.exists(_.isEmpty)) {
                emitWarning(s"Unsupported result expressions found in: ${resultExpressions}")
                return None
              }
              hashAggBuilder.addAllResultExprs(resultExprs.map(_.get).asJava)
            }
            hashAggBuilder.setModeValue(mode.getNumber)
            Some(result.setHashAgg(hashAggBuilder).build())
          } else {
            None
          }
        }

      case op if isCometSink(op) =>
        // These operators are source of Comet native execution chain
        val scanBuilder = OperatorOuterClass.Scan.newBuilder()

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
          emitWarning(
            s"unsupported Comet operator: ${op.nodeName}, due to unsupported data types above")
          None
        }

      case op =>
        // Emit warning if:
        //  1. it is not Spark shuffle operator, which is handled separately
        //  2. it is not a Comet operator
        if (!op.nodeName.contains("Comet") && !op.isInstanceOf[ShuffleExchangeExec]) {
          emitWarning(s"unsupported Spark operator: ${op.nodeName}")
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
      case _: CometSinkPlaceHolder => true
      case _: CoalesceExec => true
      case _: UnionExec => true
      case _: ShuffleExchangeExec => true
      case _: TakeOrderedAndProjectExec => true
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
  def supportPartitioningTypes(inputs: Seq[Attribute]): Boolean = {
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
    val supported = inputs.forall(attr => supportedDataType(attr.dataType))
    if (!supported) {
      emitWarning(s"unsupported Spark partitioning: ${inputs.map(_.dataType)}")
    }
    supported
  }

  /**
   * Whether the given Spark partitioning is supported by Comet.
   */
  def supportPartitioning(inputs: Seq[Attribute], partitioning: Partitioning): Boolean = {
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
      emitWarning(s"unsupported Spark partitioning: ${inputs.map(_.dataType)}")
      false
    } else {
      partitioning match {
        case HashPartitioning(expressions, _) =>
          expressions.map(QueryPlanSerde.exprToProto(_, inputs)).forall(_.isDefined)
        case SinglePartition => true
        case other =>
          emitWarning(s"unsupported Spark partitioning: ${other.getClass.getName}")
          false
      }
    }
  }
}
