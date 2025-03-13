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
import scala.math.min

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, NormalizeNaNAndZero}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.util.CharVarcharCodegenUtils
import org.apache.spark.sql.comet._
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{BroadcastQueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.aggregate.{BaseAggregateExec, HashAggregateExec, ObjectHashAggregateExec}
import org.apache.spark.sql.execution.datasources.{FilePartition, FileScanRDD}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceRDD, DataSourceRDDPartition}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, HashJoin, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.{isCometScan, isSpark34Plus, withInfo}
import org.apache.comet.expressions._
import org.apache.comet.serde.ExprOuterClass.{AggExpr, DataType => ProtoDataType, Expr, ScalarFunc}
import org.apache.comet.serde.ExprOuterClass.DataType._
import org.apache.comet.serde.OperatorOuterClass.{AggregateMode => CometAggregateMode, BuildSide, JoinType, Operator}
import org.apache.comet.shims.{CometExprShim, ShimQueryPlanSerde}

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
              if (AggSerde.minMaxDataTypeSupported(min.dataType)) {
                Some(agg)
              } else {
                withInfo(windowExpr, s"datatype ${min.dataType} is not supported", expr)
                None
              }
            case max: Max =>
              if (AggSerde.minMaxDataTypeSupported(max.dataType)) {
                Some(agg)
              } else {
                withInfo(windowExpr, s"datatype ${max.dataType} is not supported", expr)
                None
              }
            case s: Sum =>
              if (AggSerde.sumDataTypeSupported(s.dataType) && !s.dataType
                  .isInstanceOf[DecimalType]) {
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

    if (aggExpr.isDistinct) {
      // https://github.com/apache/datafusion-comet/issues/1260
      withInfo(aggExpr, "distinct aggregates are not supported")
      return None
    }

    val cometExpr: CometAggregateExpressionSerde = aggExpr.aggregateFunction match {
      case _: Sum => CometSum
      case _: Average => CometAverage
      case _: Count => CometCount
      case _: Min => CometMin
      case _: Max => CometMax
      case _: First => CometFirst
      case _: Last => CometLast
      case _: BitAndAgg => CometBitAndAgg
      case _: BitOrAgg => CometBitOrAgg
      case _: BitXorAgg => CometBitXOrAgg
      case _: CovSample => CometCovSample
      case _: CovPopulation => CometCovPopulation
      case _: VarianceSamp => CometVarianceSamp
      case _: VariancePop => CometVariancePop
      case _: StddevSamp => CometStddevSamp
      case _: StddevPop => CometStddevPop
      case _: Corr => CometCorr
      case _: BloomFilterAggregate => CometBloomFilterAggregate
      case fn =>
        val msg = s"unsupported Spark aggregate function: ${fn.prettyName}"
        emitWarning(msg)
        withInfo(aggExpr, msg, fn.children: _*)
        return None

    }
    cometExpr.convert(aggExpr, aggExpr.aggregateFunction, inputs, binding, conf)
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
   * Wrap an expression in a cast.
   */
  def castToProto(
      expr: Expression,
      timeZoneId: Option[String],
      dt: DataType,
      childExpr: Expr,
      evalMode: CometEvalMode.Value): Option[Expr] = {
    serializeDataType(dt) match {
      case Some(dataType) =>
        val castBuilder = ExprOuterClass.Cast.newBuilder()
        castBuilder.setChild(childExpr)
        castBuilder.setDatatype(dataType)
        castBuilder.setEvalMode(evalModeToProto(evalMode))
        castBuilder.setAllowIncompat(CometConf.COMET_CAST_ALLOW_INCOMPATIBLE.get())
        castBuilder.setTimezone(timeZoneId.getOrElse("UTC"))
        Some(
          ExprOuterClass.Expr
            .newBuilder()
            .setCast(castBuilder)
            .build())
      case _ =>
        withInfo(expr, s"Unsupported datatype in castToProto: $dt")
        None
    }
  }

  def handleCast(
      expr: Expression,
      child: Expression,
      inputs: Seq[Attribute],
      binding: Boolean,
      dt: DataType,
      timeZoneId: Option[String],
      evalMode: CometEvalMode.Value): Option[Expr] = {

    val childExpr = exprToProtoInternal(child, inputs, binding)
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
          castToProto(expr, timeZoneId, dt, childExpr.get, evalMode)
        case Incompatible(reason) =>
          if (CometConf.COMET_CAST_ALLOW_INCOMPATIBLE.get()) {
            logWarning(getIncompatMessage(reason))
            castToProto(expr, timeZoneId, dt, childExpr.get, evalMode)
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

  /**
   * Convert a Spark expression to a protocol-buffer representation of a native Comet/DataFusion
   * expression.
   *
   * This method performs a transformation on the plan to handle decimal promotion and then calls
   * into the recursive method [[exprToProtoInternal]].
   *
   * @param expr
   *   The input expression
   * @param inputs
   *   The input attributes
   * @param binding
   *   Whether to bind the expression to the input attributes
   * @return
   *   The protobuf representation of the expression, or None if the expression is not supported.
   *   In the case where None is returned, the expression will be tagged with the reason(s) why it
   *   is not supported.
   */
  def exprToProto(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean = true): Option[Expr] = {

    val conf = SQLConf.get
    val newExpr =
      DecimalPrecision.promote(conf.decimalOperationsAllowPrecisionLoss, expr, !conf.ansiEnabled)
    exprToProtoInternal(newExpr, inputs, binding)
  }

  /**
   * Convert a Spark expression to a protocol-buffer representation of a native Comet/DataFusion
   * expression.
   *
   * @param expr
   *   The input expression
   * @param inputs
   *   The input attributes
   * @param binding
   *   Whether to bind the expression to the input attributes
   * @return
   *   The protobuf representation of the expression, or None if the expression is not supported.
   *   In the case where None is returned, the expression will be tagged with the reason(s) why it
   *   is not supported.
   */
  def exprToProtoInternal(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[Expr] = {
    SQLConf.get

    def convert(handler: CometExpressionSerde): Option[Expr] = {
      handler match {
        case _: IncompatExpr if !CometConf.COMET_EXPR_ALLOW_INCOMPATIBLE.get() =>
          withInfo(
            expr,
            s"$expr is not fully compatible with Spark. To enable it anyway, set " +
              s"${CometConf.COMET_EXPR_ALLOW_INCOMPATIBLE.key}=true. ${CometConf.COMPAT_GUIDE}.")
          None
        case _ =>
          handler.convert(expr, inputs, binding)
      }
    }

    expr match {
      case a @ Alias(_, _) =>
        val r = exprToProtoInternal(a.child, inputs, binding)
        if (r.isEmpty) {
          withInfo(expr, a.child)
        }
        r

      case cast @ Cast(_: Literal, dataType, _, _) =>
        // This can happen after promoting decimal precisions
        val value = cast.eval()
        exprToProtoInternal(Literal(value, dataType), inputs, binding)

      case UnaryExpression(child) if expr.prettyName == "trycast" =>
        val timeZoneId = SQLConf.get.sessionLocalTimeZone
        handleCast(
          expr,
          child,
          inputs,
          binding,
          expr.dataType,
          Some(timeZoneId),
          CometEvalMode.TRY)

      case c @ Cast(child, dt, timeZoneId, _) =>
        handleCast(expr, child, inputs, binding, dt, timeZoneId, evalMode(c))

      case add @ Add(left, right, _) if supportedDataType(left.dataType) =>
        createMathExpression(
          expr,
          left,
          right,
          inputs,
          binding,
          add.dataType,
          getFailOnError(add),
          (builder, mathExpr) => builder.setAdd(mathExpr))

      case add @ Add(left, _, _) if !supportedDataType(left.dataType) =>
        withInfo(add, s"Unsupported datatype ${left.dataType}")
        None

      case sub @ Subtract(left, right, _) if supportedDataType(left.dataType) =>
        createMathExpression(
          expr,
          left,
          right,
          inputs,
          binding,
          sub.dataType,
          getFailOnError(sub),
          (builder, mathExpr) => builder.setSubtract(mathExpr))

      case sub @ Subtract(left, _, _) if !supportedDataType(left.dataType) =>
        withInfo(sub, s"Unsupported datatype ${left.dataType}")
        None

      case mul @ Multiply(left, right, _)
          if supportedDataType(left.dataType) && !decimalBeforeSpark34(left.dataType) =>
        createMathExpression(
          expr,
          left,
          right,
          inputs,
          binding,
          mul.dataType,
          getFailOnError(mul),
          (builder, mathExpr) => builder.setMultiply(mathExpr))

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
        // Datafusion now throws an exception for dividing by zero
        // See https://github.com/apache/arrow-datafusion/pull/6792
        // For now, use NullIf to swap zeros with nulls.
        val rightExpr = nullIfWhenPrimitive(right)

        createMathExpression(
          expr,
          left,
          rightExpr,
          inputs,
          binding,
          div.dataType,
          getFailOnError(div),
          (builder, mathExpr) => builder.setDivide(mathExpr))

      case div @ Divide(left, _, _) =>
        if (!supportedDataType(left.dataType)) {
          withInfo(div, s"Unsupported datatype ${left.dataType}")
        }
        if (decimalBeforeSpark34(left.dataType)) {
          withInfo(div, "Decimal support requires Spark 3.4 or later")
        }
        None

      case div @ IntegralDivide(left, right, _)
          if supportedDataType(left.dataType) && !decimalBeforeSpark34(left.dataType) =>
        val rightExpr = nullIfWhenPrimitive(right)

        val dataType = (left.dataType, right.dataType) match {
          case (l: DecimalType, r: DecimalType) =>
            // copy from IntegralDivide.resultDecimalType
            val intDig = l.precision - l.scale + r.scale
            DecimalType(min(if (intDig == 0) 1 else intDig, DecimalType.MAX_PRECISION), 0)
          case _ => left.dataType
        }

        val divideExpr = createMathExpression(
          expr,
          left,
          rightExpr,
          inputs,
          binding,
          dataType,
          getFailOnError(div),
          (builder, mathExpr) => builder.setIntegralDivide(mathExpr))

        if (divideExpr.isDefined) {
          val childExpr = if (dataType.isInstanceOf[DecimalType]) {
            // check overflow for decimal type
            val builder = ExprOuterClass.CheckOverflow.newBuilder()
            builder.setChild(divideExpr.get)
            builder.setFailOnError(getFailOnError(div))
            builder.setDatatype(serializeDataType(dataType).get)
            Some(
              ExprOuterClass.Expr
                .newBuilder()
                .setCheckOverflow(builder)
                .build())
          } else {
            divideExpr
          }

          // cast result to long
          castToProto(expr, None, LongType, childExpr.get, CometEvalMode.LEGACY)
        } else {
          None
        }

      case div @ IntegralDivide(left, _, _) =>
        if (!supportedDataType(left.dataType)) {
          withInfo(div, s"Unsupported datatype ${left.dataType}")
        }
        if (decimalBeforeSpark34(left.dataType)) {
          withInfo(div, "Decimal support requires Spark 3.4 or later")
        }
        None

      case rem @ Remainder(left, right, _)
          if supportedDataType(left.dataType) && !decimalBeforeSpark34(left.dataType) =>
        val rightExpr = nullIfWhenPrimitive(right)

        createMathExpression(
          expr,
          left,
          rightExpr,
          inputs,
          binding,
          rem.dataType,
          getFailOnError(rem),
          (builder, mathExpr) => builder.setRemainder(mathExpr))

      case rem @ Remainder(left, _, _) =>
        if (!supportedDataType(left.dataType)) {
          withInfo(rem, s"Unsupported datatype ${left.dataType}")
        }
        if (decimalBeforeSpark34(left.dataType)) {
          withInfo(rem, "Decimal support requires Spark 3.4 or later")
        }
        None

      case EqualTo(left, right) =>
        createBinaryExpr(
          expr,
          left,
          right,
          inputs,
          binding,
          (builder, binaryExpr) => builder.setEq(binaryExpr))

      case Not(EqualTo(left, right)) =>
        createBinaryExpr(
          expr,
          left,
          right,
          inputs,
          binding,
          (builder, binaryExpr) => builder.setNeq(binaryExpr))

      case EqualNullSafe(left, right) =>
        createBinaryExpr(
          expr,
          left,
          right,
          inputs,
          binding,
          (builder, binaryExpr) => builder.setEqNullSafe(binaryExpr))

      case Not(EqualNullSafe(left, right)) =>
        createBinaryExpr(
          expr,
          left,
          right,
          inputs,
          binding,
          (builder, binaryExpr) => builder.setNeqNullSafe(binaryExpr))

      case GreaterThan(left, right) =>
        createBinaryExpr(
          expr,
          left,
          right,
          inputs,
          binding,
          (builder, binaryExpr) => builder.setGt(binaryExpr))

      case GreaterThanOrEqual(left, right) =>
        createBinaryExpr(
          expr,
          left,
          right,
          inputs,
          binding,
          (builder, binaryExpr) => builder.setGtEq(binaryExpr))

      case LessThan(left, right) =>
        createBinaryExpr(
          expr,
          left,
          right,
          inputs,
          binding,
          (builder, binaryExpr) => builder.setLt(binaryExpr))

      case LessThanOrEqual(left, right) =>
        createBinaryExpr(
          expr,
          left,
          right,
          inputs,
          binding,
          (builder, binaryExpr) => builder.setLtEq(binaryExpr))

      case Literal(value, dataType) if supportedDataType(dataType, allowStruct = value == null) =>
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
        val strExpr = exprToProtoInternal(str, inputs, binding)

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
            exprToProtoInternal(child, inputs, binding) match {
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
          createBinaryExpr(
            expr,
            left,
            right,
            inputs,
            binding,
            (builder, binaryExpr) => builder.setLike(binaryExpr))
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

        createBinaryExpr(
          expr,
          left,
          right,
          inputs,
          binding,
          (builder, binaryExpr) => builder.setRlike(binaryExpr))

      case StartsWith(left, right) =>
        createBinaryExpr(
          expr,
          left,
          right,
          inputs,
          binding,
          (builder, binaryExpr) => builder.setStartsWith(binaryExpr))

      case EndsWith(left, right) =>
        createBinaryExpr(
          expr,
          left,
          right,
          inputs,
          binding,
          (builder, binaryExpr) => builder.setEndsWith(binaryExpr))

      case Contains(left, right) =>
        createBinaryExpr(
          expr,
          left,
          right,
          inputs,
          binding,
          (builder, binaryExpr) => builder.setContains(binaryExpr))

      case StringSpace(child) =>
        createUnaryExpr(
          expr,
          child,
          inputs,
          binding,
          (builder, unaryExpr) => builder.setStringSpace(unaryExpr))

      case Hour(child, timeZoneId) =>
        val childExpr = exprToProtoInternal(child, inputs, binding)

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
        val childExpr = exprToProtoInternal(child, inputs, binding)

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
        val leftExpr = exprToProtoInternal(left, inputs, binding)
        val rightExpr = exprToProtoInternal(right, inputs, binding)
        val optExpr = scalarExprToProtoWithReturnType("date_add", DateType, leftExpr, rightExpr)
        optExprWithInfo(optExpr, expr, left, right)

      case DateSub(left, right) =>
        val leftExpr = exprToProtoInternal(left, inputs, binding)
        val rightExpr = exprToProtoInternal(right, inputs, binding)
        val optExpr = scalarExprToProtoWithReturnType("date_sub", DateType, leftExpr, rightExpr)
        optExprWithInfo(optExpr, expr, left, right)

      case TruncDate(child, format) =>
        val childExpr = exprToProtoInternal(child, inputs, binding)
        val formatExpr = exprToProtoInternal(format, inputs, binding)

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
        val childExpr = exprToProtoInternal(child, inputs, binding)
        val formatExpr = exprToProtoInternal(format, inputs, binding)

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
        val childExpr = exprToProtoInternal(child, inputs, binding)

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
        val periodType = exprToProtoInternal(Literal("year"), inputs, binding)
        val childExpr = exprToProtoInternal(child, inputs, binding)
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
        createUnaryExpr(
          expr,
          child,
          inputs,
          binding,
          (builder, unaryExpr) => builder.setIsNull(unaryExpr))

      case IsNotNull(child) =>
        createUnaryExpr(
          expr,
          child,
          inputs,
          binding,
          (builder, unaryExpr) => builder.setIsNotNull(unaryExpr))

      case IsNaN(child) =>
        val childExpr = exprToProtoInternal(child, inputs, binding)
        val optExpr =
          scalarExprToProtoWithReturnType("isnan", BooleanType, childExpr)

        optExprWithInfo(optExpr, expr, child)

      case SortOrder(child, direction, nullOrdering, _) =>
        val childExpr = exprToProtoInternal(child, inputs, binding)

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
        createBinaryExpr(
          expr,
          left,
          right,
          inputs,
          binding,
          (builder, binaryExpr) => builder.setAnd(binaryExpr))

      case Or(left, right) =>
        createBinaryExpr(
          expr,
          left,
          right,
          inputs,
          binding,
          (builder, binaryExpr) => builder.setOr(binaryExpr))

      case UnaryExpression(child) if expr.prettyName == "promote_precision" =>
        // `UnaryExpression` includes `PromotePrecision` for Spark 3.3
        // `PromotePrecision` is just a wrapper, don't need to serialize it.
        exprToProtoInternal(child, inputs, binding)

      case CheckOverflow(child, dt, nullOnOverflow) =>
        val childExpr = exprToProtoInternal(child, inputs, binding)

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
        val childExpr = exprToProtoInternal(child, inputs, binding)
        val optExpr = scalarExprToProto("acos", childExpr)
        optExprWithInfo(optExpr, expr, child)

      case Asin(child) =>
        val childExpr = exprToProtoInternal(child, inputs, binding)
        val optExpr = scalarExprToProto("asin", childExpr)
        optExprWithInfo(optExpr, expr, child)

      case Atan(child) =>
        val childExpr = exprToProtoInternal(child, inputs, binding)
        val optExpr = scalarExprToProto("atan", childExpr)
        optExprWithInfo(optExpr, expr, child)

      case Atan2(left, right) =>
        val leftExpr = exprToProtoInternal(left, inputs, binding)
        val rightExpr = exprToProtoInternal(right, inputs, binding)
        val optExpr = scalarExprToProto("atan2", leftExpr, rightExpr)
        optExprWithInfo(optExpr, expr, left, right)

      case Hex(child) =>
        val childExpr = exprToProtoInternal(child, inputs, binding)
        val optExpr =
          scalarExprToProtoWithReturnType("hex", StringType, childExpr)

        optExprWithInfo(optExpr, expr, child)

      case e: Unhex =>
        val unHex = unhexSerde(e)

        val childExpr = exprToProtoInternal(unHex._1, inputs, binding)
        val failOnErrorExpr = exprToProtoInternal(unHex._2, inputs, binding)

        val optExpr =
          scalarExprToProtoWithReturnType("unhex", e.dataType, childExpr, failOnErrorExpr)
        optExprWithInfo(optExpr, expr, unHex._1)

      case e @ Ceil(child) =>
        val childExpr = exprToProtoInternal(child, inputs, binding)
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
        val childExpr = exprToProtoInternal(child, inputs, binding)
        val optExpr = scalarExprToProto("cos", childExpr)
        optExprWithInfo(optExpr, expr, child)

      case Exp(child) =>
        val childExpr = exprToProtoInternal(child, inputs, binding)
        val optExpr = scalarExprToProto("exp", childExpr)
        optExprWithInfo(optExpr, expr, child)

      case e @ Floor(child) =>
        val childExpr = exprToProtoInternal(child, inputs, binding)
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
        val childExpr = exprToProtoInternal(nullIfNegative(child), inputs, binding)
        val optExpr = scalarExprToProto("ln", childExpr)
        optExprWithInfo(optExpr, expr, child)

      case Log10(child) =>
        val childExpr = exprToProtoInternal(nullIfNegative(child), inputs, binding)
        val optExpr = scalarExprToProto("log10", childExpr)
        optExprWithInfo(optExpr, expr, child)

      case Log2(child) =>
        val childExpr = exprToProtoInternal(nullIfNegative(child), inputs, binding)
        val optExpr = scalarExprToProto("log2", childExpr)
        optExprWithInfo(optExpr, expr, child)

      case Pow(left, right) =>
        val leftExpr = exprToProtoInternal(left, inputs, binding)
        val rightExpr = exprToProtoInternal(right, inputs, binding)
        val optExpr = scalarExprToProto("pow", leftExpr, rightExpr)
        optExprWithInfo(optExpr, expr, left, right)

      case r: Round =>
        // _scale s a constant, copied from Spark's RoundBase because it is a protected val
        val scaleV: Any = r.scale.eval(EmptyRow)
        val _scale: Int = scaleV.asInstanceOf[Int]

        lazy val childExpr = exprToProtoInternal(r.child, inputs, binding)
        r.child.dataType match {
          case t: DecimalType if t.scale < 0 => // Spark disallows negative scale SPARK-30252
            withInfo(r, "Decimal type has negative scale")
            None
          case _ if scaleV == null =>
            exprToProtoInternal(Literal(null), inputs, binding)
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
            val scaleExpr = exprToProtoInternal(Literal(_scale.toLong, LongType), inputs, binding)
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
        val childExpr = exprToProtoInternal(child, inputs, binding)
        val optExpr = scalarExprToProto("sin", childExpr)
        optExprWithInfo(optExpr, expr, child)

      case Sqrt(child) =>
        val childExpr = exprToProtoInternal(child, inputs, binding)
        val optExpr = scalarExprToProto("sqrt", childExpr)
        optExprWithInfo(optExpr, expr, child)

      case Tan(child) =>
        val childExpr = exprToProtoInternal(child, inputs, binding)
        val optExpr = scalarExprToProto("tan", childExpr)
        optExprWithInfo(optExpr, expr, child)

      case Ascii(child) =>
        val castExpr = Cast(child, StringType)
        val childExpr = exprToProtoInternal(castExpr, inputs, binding)
        val optExpr = scalarExprToProto("ascii", childExpr)
        optExprWithInfo(optExpr, expr, castExpr)

      case BitLength(child) =>
        val castExpr = Cast(child, StringType)
        val childExpr = exprToProtoInternal(castExpr, inputs, binding)
        val optExpr = scalarExprToProto("bit_length", childExpr)
        optExprWithInfo(optExpr, expr, castExpr)

      case If(predicate, trueValue, falseValue) =>
        val predicateExpr = exprToProtoInternal(predicate, inputs, binding)
        val trueExpr = exprToProtoInternal(trueValue, inputs, binding)
        val falseExpr = exprToProtoInternal(falseValue, inputs, binding)
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
          exprToProtoInternal(elements._1, inputs, binding)
        })
        val thenSeq = branches.map(elements => {
          allBranches = allBranches :+ elements._2
          exprToProtoInternal(elements._2, inputs, binding)
        })
        assert(whenSeq.length == thenSeq.length)
        if (whenSeq.forall(_.isDefined) && thenSeq.forall(_.isDefined)) {
          val builder = ExprOuterClass.CaseWhen.newBuilder()
          builder.addAllWhen(whenSeq.map(_.get).asJava)
          builder.addAllThen(thenSeq.map(_.get).asJava)
          if (elseValue.isDefined) {
            val elseValueExpr =
              exprToProtoInternal(elseValue.get, inputs, binding)
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
          exprToProtoInternal(castExpr, inputs, binding)
        })
        val optExpr = scalarExprToProto("concat_ws", exprs: _*)
        optExprWithInfo(optExpr, expr, childExprs: _*)

      case Chr(child) =>
        val childExpr = exprToProtoInternal(child, inputs, binding)
        val optExpr = scalarExprToProto("chr", childExpr)
        optExprWithInfo(optExpr, expr, child)

      case InitCap(child) =>
        if (CometConf.COMET_EXEC_INITCAP_ENABLED.get()) {
          val castExpr = Cast(child, StringType)
          val childExpr = exprToProtoInternal(castExpr, inputs, binding)
          val optExpr = scalarExprToProto("initcap", childExpr)
          optExprWithInfo(optExpr, expr, castExpr)
        } else {
          withInfo(
            expr,
            "Comet initCap is not compatible with Spark yet. " +
              "See https://github.com/apache/datafusion-comet/issues/1052 ." +
              s"Set ${CometConf.COMET_EXEC_INITCAP_ENABLED.key}=true to enable it anyway.")
          None
        }

      case Length(child) =>
        val castExpr = Cast(child, StringType)
        val childExpr = exprToProtoInternal(castExpr, inputs, binding)
        val optExpr = scalarExprToProto("length", childExpr)
        optExprWithInfo(optExpr, expr, castExpr)

      case Md5(child) =>
        val childExpr = exprToProtoInternal(child, inputs, binding)
        val optExpr = scalarExprToProto("md5", childExpr)
        optExprWithInfo(optExpr, expr, child)

      case OctetLength(child) =>
        val castExpr = Cast(child, StringType)
        val childExpr = exprToProtoInternal(castExpr, inputs, binding)
        val optExpr = scalarExprToProto("octet_length", childExpr)
        optExprWithInfo(optExpr, expr, castExpr)

      case Reverse(child) =>
        val castExpr = Cast(child, StringType)
        val childExpr = exprToProtoInternal(castExpr, inputs, binding)
        val optExpr = scalarExprToProto("reverse", childExpr)
        optExprWithInfo(optExpr, expr, castExpr)

      case StringInstr(str, substr) =>
        val leftCast = Cast(str, StringType)
        val rightCast = Cast(substr, StringType)
        val leftExpr = exprToProtoInternal(leftCast, inputs, binding)
        val rightExpr = exprToProtoInternal(rightCast, inputs, binding)
        val optExpr = scalarExprToProto("strpos", leftExpr, rightExpr)
        optExprWithInfo(optExpr, expr, leftCast, rightCast)

      case StringRepeat(str, times) =>
        val leftCast = Cast(str, StringType)
        val rightCast = Cast(times, LongType)
        val leftExpr = exprToProtoInternal(leftCast, inputs, binding)
        val rightExpr = exprToProtoInternal(rightCast, inputs, binding)
        val optExpr = scalarExprToProto("repeat", leftExpr, rightExpr)
        optExprWithInfo(optExpr, expr, leftCast, rightCast)

      case StringReplace(src, search, replace) =>
        val srcCast = Cast(src, StringType)
        val searchCast = Cast(search, StringType)
        val replaceCast = Cast(replace, StringType)
        val srcExpr = exprToProtoInternal(srcCast, inputs, binding)
        val searchExpr = exprToProtoInternal(searchCast, inputs, binding)
        val replaceExpr = exprToProtoInternal(replaceCast, inputs, binding)
        val optExpr = scalarExprToProto("replace", srcExpr, searchExpr, replaceExpr)
        optExprWithInfo(optExpr, expr, srcCast, searchCast, replaceCast)

      case StringTranslate(src, matching, replace) =>
        val srcCast = Cast(src, StringType)
        val matchingCast = Cast(matching, StringType)
        val replaceCast = Cast(replace, StringType)
        val srcExpr = exprToProtoInternal(srcCast, inputs, binding)
        val matchingExpr = exprToProtoInternal(matchingCast, inputs, binding)
        val replaceExpr = exprToProtoInternal(replaceCast, inputs, binding)
        val optExpr = scalarExprToProto("translate", srcExpr, matchingExpr, replaceExpr)
        optExprWithInfo(optExpr, expr, srcCast, matchingCast, replaceCast)

      case StringTrim(srcStr, trimStr) =>
        trim(expr, srcStr, trimStr, inputs, binding, "trim")

      case StringTrimLeft(srcStr, trimStr) =>
        trim(expr, srcStr, trimStr, inputs, binding, "ltrim")

      case StringTrimRight(srcStr, trimStr) =>
        trim(expr, srcStr, trimStr, inputs, binding, "rtrim")

      case StringTrimBoth(srcStr, trimStr, _) =>
        trim(expr, srcStr, trimStr, inputs, binding, "btrim")

      case Upper(child) =>
        if (CometConf.COMET_CASE_CONVERSION_ENABLED.get()) {
          val castExpr = Cast(child, StringType)
          val childExpr = exprToProtoInternal(castExpr, inputs, binding)
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
          val childExpr = exprToProtoInternal(castExpr, inputs, binding)
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
        createBinaryExpr(
          expr,
          left,
          right,
          inputs,
          binding,
          (builder, binaryExpr) => builder.setBitwiseAnd(binaryExpr))

      case BitwiseNot(child) =>
        createUnaryExpr(
          expr,
          child,
          inputs,
          binding,
          (builder, unaryExpr) => builder.setBitwiseNot(unaryExpr))

      case BitwiseOr(left, right) =>
        createBinaryExpr(
          expr,
          left,
          right,
          inputs,
          binding,
          (builder, binaryExpr) => builder.setBitwiseOr(binaryExpr))

      case BitwiseXor(left, right) =>
        createBinaryExpr(
          expr,
          left,
          right,
          inputs,
          binding,
          (builder, binaryExpr) => builder.setBitwiseXor(binaryExpr))

      case ShiftRight(left, right) =>
        // DataFusion bitwise shift right expression requires
        // same data type between left and right side
        val rightExpression = if (left.dataType == LongType) {
          Cast(right, LongType)
        } else {
          right
        }

        createBinaryExpr(
          expr,
          left,
          rightExpression,
          inputs,
          binding,
          (builder, binaryExpr) => builder.setBitwiseShiftRight(binaryExpr))

      case ShiftLeft(left, right) =>
        // DataFusion bitwise shift right expression requires
        // same data type between left and right side
        val rightExpression = if (left.dataType == LongType) {
          Cast(right, LongType)
        } else {
          right
        }

        createBinaryExpr(
          expr,
          left,
          rightExpression,
          inputs,
          binding,
          (builder, binaryExpr) => builder.setBitwiseShiftLeft(binaryExpr))
      case In(value, list) =>
        in(expr, value, list, inputs, binding, negate = false)

      case InSet(value, hset) =>
        val valueDataType = value.dataType
        val list = hset.map { setVal =>
          Literal(setVal, valueDataType)
        }.toSeq
        // Change `InSet` to `In` expression
        // We do Spark `InSet` optimization in native (DataFusion) side.
        in(expr, value, list, inputs, binding, negate = false)

      case Not(In(value, list)) =>
        in(expr, value, list, inputs, binding, negate = true)

      case Not(child) =>
        createUnaryExpr(
          expr,
          child,
          inputs,
          binding,
          (builder, unaryExpr) => builder.setNot(unaryExpr))

      case UnaryMinus(child, failOnError) =>
        val childExpr = exprToProtoInternal(child, inputs, binding)
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
        val exprChildren = a.children.map(exprToProtoInternal(_, inputs, binding))
        scalarExprToProto("coalesce", exprChildren: _*)

      // With Spark 3.4, CharVarcharCodegenUtils.readSidePadding gets called to pad spaces for
      // char types.
      // See https://github.com/apache/spark/pull/38151
      case s: StaticInvoke
          // classOf gets ther runtime class of T, which lets us compare directly
          // Otherwise isInstanceOf[Class[T]] will always evaluate to true for Class[_]
          if s.staticObject == classOf[CharVarcharCodegenUtils] &&
            s.dataType.isInstanceOf[StringType] &&
            s.functionName == "readSidePadding" &&
            s.arguments.size == 2 &&
            s.propagateNull &&
            !s.returnNullable &&
            s.isDeterministic =>
        val argsExpr = Seq(
          exprToProtoInternal(Cast(s.arguments(0), StringType), inputs, binding),
          exprToProtoInternal(s.arguments(1), inputs, binding))

        if (argsExpr.forall(_.isDefined)) {
          scalarExprToProto("read_side_padding", argsExpr: _*)
        } else {
          withInfo(expr, s.arguments: _*)
          None
        }

      // read-side padding in Spark 3.5.2+ is represented by rpad function
      case StringRPad(srcStr, size, chars) =>
        chars match {
          case Literal(str, DataTypes.StringType) if str.toString == " " =>
            val arg0 = exprToProtoInternal(srcStr, inputs, binding)
            val arg1 = exprToProtoInternal(size, inputs, binding)
            if (arg0.isDefined && arg1.isDefined) {
              scalarExprToProto("rpad", arg0, arg1)
            } else {
              withInfo(expr, "rpad unsupported arguments", srcStr, size)
              None
            }

          case _ =>
            withInfo(expr, "rpad only supports padding with spaces")
            None
        }

      case KnownFloatingPointNormalized(NormalizeNaNAndZero(expr)) =>
        val dataType = serializeDataType(expr.dataType)
        if (dataType.isEmpty) {
          withInfo(expr, s"Unsupported datatype ${expr.dataType}")
          return None
        }
        val ex = exprToProtoInternal(expr, inputs, binding)
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
        val childExpr = exprToProtoInternal(child, inputs, binding)
        val optExpr = scalarExprToProtoWithReturnType("unscaled_value", LongType, childExpr)
        optExprWithInfo(optExpr, expr, child)

      case MakeDecimal(child, precision, scale, true) =>
        val childExpr = exprToProtoInternal(child, inputs, binding)
        val optExpr = scalarExprToProtoWithReturnType(
          "make_decimal",
          DecimalType(precision, scale),
          childExpr)
        optExprWithInfo(optExpr, expr, child)

      case b @ BloomFilterMightContain(_, _) =>
        val bloomFilter = b.left
        val value = b.right
        val bloomFilterExpr = exprToProtoInternal(bloomFilter, inputs, binding)
        val valueExpr = exprToProtoInternal(value, inputs, binding)
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

      case _: Murmur3Hash => CometMurmur3Hash.convert(expr, inputs, binding)

      case _: XxHash64 => CometXxHash64.convert(expr, inputs, binding)

      case Sha2(left, numBits) =>
        if (!numBits.foldable) {
          withInfo(expr, "non literal numBits is not supported")
          return None
        }
        // it's possible for spark to dynamically compute the number of bits from input
        // expression, however DataFusion does not support that yet.
        val childExpr = exprToProtoInternal(left, inputs, binding)
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
          exprToProtoInternal(Literal(null, StringType), inputs, binding)
        } else {
          scalarExprToProtoWithReturnType(algorithm, StringType, childExpr)
        }

      case struct @ CreateNamedStruct(_) =>
        if (struct.names.length != struct.names.distinct.length) {
          withInfo(expr, "CreateNamedStruct with duplicate field names are not supported")
          return None
        }

        val valExprs = struct.valExprs.map(exprToProtoInternal(_, inputs, binding))

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
        exprToProtoInternal(child, inputs, binding).map { childExpr =>
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
        val childExprs = children.map(exprToProtoInternal(_, inputs, binding))

        if (childExprs.forall(_.isDefined)) {
          scalarExprToProto("make_array", childExprs: _*)
        } else {
          withInfo(expr, "unsupported arguments for CreateArray", children: _*)
          None
        }

      case GetArrayItem(child, ordinal, failOnError) =>
        val childExpr = exprToProtoInternal(child, inputs, binding)
        val ordinalExpr = exprToProtoInternal(ordinal, inputs, binding)

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

      case expr if expr.prettyName == "array_insert" =>
        val srcExprProto = exprToProtoInternal(expr.children(0), inputs, binding)
        val posExprProto = exprToProtoInternal(expr.children(1), inputs, binding)
        val itemExprProto = exprToProtoInternal(expr.children(2), inputs, binding)
        val legacyNegativeIndex =
          SQLConf.get.getConfString("spark.sql.legacy.negativeIndexInArrayInsert").toBoolean
        if (srcExprProto.isDefined && posExprProto.isDefined && itemExprProto.isDefined) {
          val arrayInsertBuilder = ExprOuterClass.ArrayInsert
            .newBuilder()
            .setSrcArrayExpr(srcExprProto.get)
            .setPosExpr(posExprProto.get)
            .setItemExpr(itemExprProto.get)
            .setLegacyNegativeIndex(legacyNegativeIndex)

          Some(
            ExprOuterClass.Expr
              .newBuilder()
              .setArrayInsert(arrayInsertBuilder)
              .build())
        } else {
          withInfo(
            expr,
            "unsupported arguments for ArrayInsert",
            expr.children(0),
            expr.children(1),
            expr.children(2))
          None
        }

      case ElementAt(child, ordinal, defaultValue, failOnError)
          if child.dataType.isInstanceOf[ArrayType] =>
        val childExpr = exprToProtoInternal(child, inputs, binding)
        val ordinalExpr = exprToProtoInternal(ordinal, inputs, binding)
        val defaultExpr = defaultValue.flatMap(exprToProtoInternal(_, inputs, binding))

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
        val childExpr = exprToProtoInternal(child, inputs, binding)

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
      case _: ArrayRemove => convert(CometArrayRemove)
      case _: ArrayContains => convert(CometArrayContains)
      // Function introduced in 3.4.0. Refer by name to provide compatibility
      // with older Spark builds
      case _ if expr.prettyName == "array_append" => convert(CometArrayAppend)
      case _: ArrayIntersect => convert(CometArrayIntersect)
      case _: ArrayJoin => convert(CometArrayJoin)
      case _: ArraysOverlap => convert(CometArraysOverlap)
      case _ @ArrayFilter(_, func) if func.children.head.isInstanceOf[IsNotNull] =>
        convert(CometArrayCompact)
      case _ =>
        withInfo(expr, s"${expr.prettyName} is not supported", expr.children: _*)
        None
    }

  }

  /**
   * Creates a UnaryExpr by calling exprToProtoInternal for the provided child expression and then
   * invokes the supplied function to wrap this UnaryExpr in a top-level Expr.
   *
   * @param child
   *   Spark expression
   * @param inputs
   *   Inputs to the expression
   * @param f
   *   Function that accepts an Expr.Builder and a UnaryExpr and builds the specific top-level
   *   Expr
   * @return
   *   Some(Expr) or None if not supported
   */
  def createUnaryExpr(
      expr: Expression,
      child: Expression,
      inputs: Seq[Attribute],
      binding: Boolean,
      f: (ExprOuterClass.Expr.Builder, ExprOuterClass.UnaryExpr) => ExprOuterClass.Expr.Builder)
      : Option[ExprOuterClass.Expr] = {
    val childExpr = exprToProtoInternal(child, inputs, binding) // TODO review
    if (childExpr.isDefined) {
      // create the generic UnaryExpr message
      val inner = ExprOuterClass.UnaryExpr
        .newBuilder()
        .setChild(childExpr.get)
        .build()
      // call the user-supplied function to wrap UnaryExpr in a top-level Expr
      // such as Expr.IsNull or Expr.IsNotNull
      Some(
        f(
          ExprOuterClass.Expr
            .newBuilder(),
          inner).build())
    } else {
      withInfo(expr, child)
      None
    }
  }

  def createBinaryExpr(
      expr: Expression,
      left: Expression,
      right: Expression,
      inputs: Seq[Attribute],
      binding: Boolean,
      f: (ExprOuterClass.Expr.Builder, ExprOuterClass.BinaryExpr) => ExprOuterClass.Expr.Builder)
      : Option[ExprOuterClass.Expr] = {
    val leftExpr = exprToProtoInternal(left, inputs, binding)
    val rightExpr = exprToProtoInternal(right, inputs, binding)
    if (leftExpr.isDefined && rightExpr.isDefined) {
      // create the generic BinaryExpr message
      val inner = ExprOuterClass.BinaryExpr
        .newBuilder()
        .setLeft(leftExpr.get)
        .setRight(rightExpr.get)
        .build()
      // call the user-supplied function to wrap BinaryExpr in a top-level Expr
      // such as Expr.And or Expr.Or
      Some(
        f(
          ExprOuterClass.Expr
            .newBuilder(),
          inner).build())
    } else {
      withInfo(expr, left, right)
      None
    }
  }

  def createMathExpression(
      expr: Expression,
      left: Expression,
      right: Expression,
      inputs: Seq[Attribute],
      binding: Boolean,
      dataType: DataType,
      failOnError: Boolean,
      f: (ExprOuterClass.Expr.Builder, ExprOuterClass.MathExpr) => ExprOuterClass.Expr.Builder)
      : Option[ExprOuterClass.Expr] = {
    val leftExpr = exprToProtoInternal(left, inputs, binding)
    val rightExpr = exprToProtoInternal(right, inputs, binding)

    if (leftExpr.isDefined && rightExpr.isDefined) {
      // create the generic MathExpr message
      val builder = ExprOuterClass.MathExpr.newBuilder()
      builder.setLeft(leftExpr.get)
      builder.setRight(rightExpr.get)
      builder.setFailOnError(failOnError)
      serializeDataType(dataType).foreach { t =>
        builder.setReturnType(t)
      }
      val inner = builder.build()
      // call the user-supplied function to wrap MathExpr in a top-level Expr
      // such as Expr.Add or Expr.Divide
      Some(
        f(
          ExprOuterClass.Expr
            .newBuilder(),
          inner).build())
    } else {
      withInfo(expr, left, right)
      None
    }
  }

  def trim(
      expr: Expression, // parent expression
      srcStr: Expression,
      trimStr: Option[Expression],
      inputs: Seq[Attribute],
      binding: Boolean,
      trimType: String): Option[Expr] = {
    val srcCast = Cast(srcStr, StringType)
    val srcExpr = exprToProtoInternal(srcCast, inputs, binding)
    if (trimStr.isDefined) {
      val trimCast = Cast(trimStr.get, StringType)
      val trimExpr = exprToProtoInternal(trimCast, inputs, binding)
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
      binding: Boolean,
      negate: Boolean): Option[Expr] = {
    val valueExpr = exprToProtoInternal(value, inputs, binding)
    val listExprs = list.map(exprToProtoInternal(_, inputs, binding))
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
    val result = OperatorOuterClass.Operator.newBuilder().setPlanId(op.id)
    childOp.foreach(result.addChildren)

    op match {

      // Fully native scan for V1
      case scan: CometScanExec
          if CometConf.COMET_NATIVE_SCAN_IMPL.get(conf) == CometConf.SCAN_NATIVE_DATAFUSION =>
        val nativeScanBuilder = OperatorOuterClass.NativeScan.newBuilder()
        nativeScanBuilder.setSource(op.simpleStringWithNodeId())

        val scanTypes = op.output.flatten { attr =>
          serializeDataType(attr.dataType)
        }

        if (scanTypes.length == op.output.length) {
          nativeScanBuilder.addAllFields(scanTypes.asJava)

          // Sink operators don't have children
          result.clearChildren()

          // TODO remove flatMap and add error handling for unsupported data filters
          val dataFilters = scan.dataFilters.flatMap(exprToProto(_, scan.output))
          nativeScanBuilder.addAllDataFilters(dataFilters.asJava)

          // TODO: modify CometNativeScan to generate the file partitions without instantiating RDD.
          scan.inputRDD match {
            case rdd: DataSourceRDD =>
              val partitions = rdd.partitions
              partitions.foreach(p => {
                val inputPartitions = p.asInstanceOf[DataSourceRDDPartition].inputPartitions
                inputPartitions.foreach(partition => {
                  partition2Proto(
                    partition.asInstanceOf[FilePartition],
                    nativeScanBuilder,
                    scan.relation.partitionSchema)
                })
              })
            case rdd: FileScanRDD =>
              rdd.filePartitions.foreach(partition => {
                partition2Proto(partition, nativeScanBuilder, scan.relation.partitionSchema)
              })
            case _ =>
          }

          val partitionSchema = schema2Proto(scan.relation.partitionSchema.fields)
          val requiredSchema = schema2Proto(scan.requiredSchema.fields)
          val dataSchema = schema2Proto(scan.relation.dataSchema.fields)

          val data_schema_idxs = scan.requiredSchema.fields.map(field => {
            scan.relation.dataSchema.fieldIndex(field.name)
          })
          val partition_schema_idxs = Array
            .range(
              scan.relation.dataSchema.fields.length,
              scan.relation.dataSchema.length + scan.relation.partitionSchema.fields.length)

          val projection_vector = (data_schema_idxs ++ partition_schema_idxs).map(idx =>
            idx.toLong.asInstanceOf[java.lang.Long])

          nativeScanBuilder.addAllProjectionVector(projection_vector.toIterable.asJava)

          // In `CometScanRule`, we ensure partitionSchema is supported.
          assert(partitionSchema.length == scan.relation.partitionSchema.fields.length)

          nativeScanBuilder.addAllDataSchema(dataSchema.toIterable.asJava)
          nativeScanBuilder.addAllRequiredSchema(requiredSchema.toIterable.asJava)
          nativeScanBuilder.addAllPartitionSchema(partitionSchema.toIterable.asJava)
          nativeScanBuilder.setSessionTimezone(conf.getConfString("spark.sql.session.timeZone"))

          Some(result.setNativeScan(nativeScanBuilder).build())

        } else {
          // There are unsupported scan type
          val msg =
            s"unsupported Comet operator: ${op.nodeName}, due to unsupported data types above"
          emitWarning(msg)
          withInfo(op, msg)
          None
        }

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
          val filterBuilder = OperatorOuterClass.Filter
            .newBuilder()
            .setPredicate(cond.get)
            .setUseDatafusionFilter(
              CometConf.COMET_NATIVE_SCAN_IMPL.get() == CometConf.SCAN_NATIVE_DATAFUSION ||
                CometConf.COMET_NATIVE_SCAN_IMPL.get() == CometConf.SCAN_NATIVE_ICEBERG_COMPAT)
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
        val source = op.simpleStringWithNodeId()
        if (source.isEmpty) {
          scanBuilder.setSource(op.getClass.getSimpleName)
        } else {
          scanBuilder.setSource(source)
        }

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
      return false
    }

    val partitionColumnNames = partitionSpec.collect {
      case a: AttributeReference => a.name
      case other =>
        withInfo(op, s"Unsupported partition expression: ${other.getClass.getSimpleName}")
        return false
    }

    val orderColumnNames = orderSpec.collect { case s: SortOrder =>
      s.child match {
        case a: AttributeReference => a.name
        case other =>
          withInfo(op, s"Unsupported sort expression: ${other.getClass.getSimpleName}")
          return false
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

  private def schema2Proto(
      fields: Array[StructField]): Array[OperatorOuterClass.SparkStructField] = {
    val fieldBuilder = OperatorOuterClass.SparkStructField.newBuilder()
    fields.map(field => {
      fieldBuilder.setName(field.name)
      fieldBuilder.setDataType(serializeDataType(field.dataType).get)
      fieldBuilder.setNullable(field.nullable)
      fieldBuilder.build()
    })
  }

  private def partition2Proto(
      partition: FilePartition,
      nativeScanBuilder: OperatorOuterClass.NativeScan.Builder,
      partitionSchema: StructType): Unit = {
    val partitionBuilder = OperatorOuterClass.SparkFilePartition.newBuilder()
    partition.files.foreach(file => {
      // Process the partition values
      val partitionValues = file.partitionValues
      assert(partitionValues.numFields == partitionSchema.length)
      val partitionVals =
        partitionValues.toSeq(partitionSchema).zipWithIndex.map { case (value, i) =>
          val attr = partitionSchema(i)
          val valueProto = exprToProto(Literal(value, attr.dataType), Seq.empty)
          // In `CometScanRule`, we have already checked that all partition values are
          // supported. So, we can safely use `get` here.
          assert(
            valueProto.isDefined,
            s"Unsupported partition value: $value, type: ${attr.dataType}")
          valueProto.get
        }

      val fileBuilder = OperatorOuterClass.SparkPartitionedFile.newBuilder()
      partitionVals.foreach(fileBuilder.addPartitionValues)
      fileBuilder
        .setFilePath(file.filePath.toString)
        .setStart(file.start)
        .setLength(file.length)
        .setFileSize(file.fileSize)
      partitionBuilder.addPartitionedFile(fileBuilder.build())
    })
    nativeScanBuilder.addFilePartitions(partitionBuilder.build())
  }
}

/**
 * Trait for providing serialization logic for expressions.
 */
trait CometExpressionSerde {

  /**
   * Convert a Spark expression into a protocol buffer representation that can be passed into
   * native code.
   *
   * @param expr
   *   The Spark expression.
   * @param inputs
   *   The input attributes.
   * @param binding
   *   Whether the attributes are bound (this is only relevant in aggregate expressions).
   * @return
   *   Protocol buffer representation, or None if the expression could not be converted. In this
   *   case it is expected that the input expression will have been tagged with reasons why it
   *   could not be converted.
   */
  def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr]
}

/**
 * Trait for providing serialization logic for aggregate expressions.
 */
trait CometAggregateExpressionSerde {

  /**
   * Convert a Spark expression into a protocol buffer representation that can be passed into
   * native code.
   *
   * @param expr
   *   The aggregate expression.
   * @param expr
   *   The aggregate function.
   * @param inputs
   *   The input attributes.
   * @param binding
   *   Whether the attributes are bound (this is only relevant in aggregate expressions).
   * @param conf
   *   SQLConf
   * @return
   *   Protocol buffer representation, or None if the expression could not be converted. In this
   *   case it is expected that the input expression will have been tagged with reasons why it
   *   could not be converted.
   */
  def convert(
      aggExpr: AggregateExpression,
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean,
      conf: SQLConf): Option[ExprOuterClass.AggExpr]
}

/** Marker trait for an expression that is not guaranteed to be 100% compatible with Spark */
trait IncompatExpr {}
