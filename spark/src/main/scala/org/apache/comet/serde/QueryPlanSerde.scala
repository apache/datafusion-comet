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
import scala.collection.mutable.ListBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, NormalizeNaNAndZero}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.util.{CharVarcharCodegenUtils, GenericArrayData}
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns.getExistenceDefaultValues
import org.apache.spark.sql.comet._
import org.apache.spark.sql.comet.execution.shuffle.CometShuffleExchangeExec
import org.apache.spark.sql.execution
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{BroadcastQueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.aggregate.{BaseAggregateExec, HashAggregateExec, ObjectHashAggregateExec}
import org.apache.spark.sql.execution.datasources.{FilePartition, FileScanRDD, PartitionedFile}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceRDD, DataSourceRDDPartition}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, HashJoin, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import com.google.protobuf.ByteString

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.CometSparkSessionExtensions.{isCometScan, isSpark40Plus, withInfo}
import org.apache.comet.DataTypeSupport.isComplexType
import org.apache.comet.expressions._
import org.apache.comet.objectstore.NativeConfig
import org.apache.comet.serde.ExprOuterClass.{AggExpr, DataType => ProtoDataType, Expr, ScalarFunc}
import org.apache.comet.serde.ExprOuterClass.DataType._
import org.apache.comet.serde.OperatorOuterClass.{AggregateMode => CometAggregateMode, BuildSide, JoinType, Operator}
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, optExprWithInfo, scalarFunctionExprToProto}
import org.apache.comet.serde.Types.ListLiteral
import org.apache.comet.shims.CometExprShim

/**
 * An utility object for query plan and expression serialization.
 */
object QueryPlanSerde extends Logging with CometExprShim {

  /**
   * Mapping of Spark operator class to Comet operator handler.
   */
  private val opSerdeMap: Map[Class[_ <: SparkPlan], CometOperatorSerde[_]] =
    Map(classOf[ProjectExec] -> CometProject, classOf[SortExec] -> CometSort)

  /**
   * Mapping of Spark expression class to Comet expression handler.
   */
  private val exprSerdeMap: Map[Class[_ <: Expression], CometExpressionSerde[_]] = (Map(
    classOf[Add] -> CometAdd,
    classOf[Subtract] -> CometSubtract,
    classOf[Multiply] -> CometMultiply,
    classOf[Divide] -> CometDivide,
    classOf[IntegralDivide] -> CometIntegralDivide,
    classOf[Remainder] -> CometRemainder,
    classOf[Round] -> CometRound,
    classOf[ArrayAppend] -> CometArrayAppend,
    classOf[ArrayContains] -> CometArrayContains,
    classOf[ArrayDistinct] -> CometArrayDistinct,
    classOf[ArrayExcept] -> CometArrayExcept,
    classOf[ArrayInsert] -> CometArrayInsert,
    classOf[ArrayIntersect] -> CometArrayIntersect,
    classOf[ArrayJoin] -> CometArrayJoin,
    classOf[ArrayMax] -> CometArrayMax,
    classOf[ArrayMin] -> CometArrayMin,
    classOf[ArrayRemove] -> CometArrayRemove,
    classOf[ArrayRepeat] -> CometArrayRepeat,
    classOf[ArraysOverlap] -> CometArraysOverlap,
    classOf[ArrayUnion] -> CometArrayUnion,
    classOf[CreateArray] -> CometCreateArray,
    classOf[Ascii] -> CometScalarFunction("ascii"),
    classOf[ConcatWs] -> CometScalarFunction("concat_ws"),
    classOf[Chr] -> CometScalarFunction("char"),
    classOf[InitCap] -> CometInitCap,
    classOf[BitwiseCount] -> CometBitwiseCount,
    classOf[BitwiseGet] -> CometBitwiseGet,
    classOf[BitwiseNot] -> CometBitwiseNot,
    classOf[BitwiseAnd] -> CometBitwiseAnd,
    classOf[BitwiseOr] -> CometBitwiseOr,
    classOf[BitwiseXor] -> CometBitwiseXor,
    classOf[BitLength] -> CometScalarFunction("bit_length"),
    classOf[FromUnixTime] -> CometFromUnixTime,
    classOf[Length] -> CometScalarFunction("length"),
    classOf[Acos] -> CometScalarFunction("acos"),
    classOf[Cos] -> CometScalarFunction("cos"),
    classOf[Asin] -> CometScalarFunction("asin"),
    classOf[Sin] -> CometScalarFunction("sin"),
    classOf[Atan] -> CometScalarFunction("atan"),
    classOf[Tan] -> CometScalarFunction("tan"),
    classOf[Exp] -> CometScalarFunction("exp"),
    classOf[Expm1] -> CometScalarFunction("expm1"),
    classOf[Sqrt] -> CometScalarFunction("sqrt"),
    classOf[Signum] -> CometScalarFunction("signum"),
    classOf[Md5] -> CometScalarFunction("md5"),
    classOf[ShiftLeft] -> CometShiftLeft,
    classOf[ShiftRight] -> CometShiftRight,
    classOf[StringInstr] -> CometScalarFunction("instr"),
    classOf[StringRepeat] -> CometStringRepeat,
    classOf[StringReplace] -> CometScalarFunction("replace"),
    classOf[StringTranslate] -> CometScalarFunction("translate"),
    classOf[StringTrim] -> CometScalarFunction("trim"),
    classOf[StringTrimLeft] -> CometScalarFunction("ltrim"),
    classOf[StringTrimRight] -> CometScalarFunction("rtrim"),
    classOf[StringTrimBoth] -> CometScalarFunction("btrim"),
    classOf[Upper] -> CometUpper,
    classOf[Lower] -> CometLower,
    classOf[Murmur3Hash] -> CometMurmur3Hash,
    classOf[XxHash64] -> CometXxHash64,
    classOf[Sha2] -> CometSha2,
    classOf[MapKeys] -> CometMapKeys,
    classOf[MapEntries] -> CometMapEntries,
    classOf[MapValues] -> CometMapValues,
    classOf[MapFromArrays] -> CometMapFromArrays,
    classOf[GetMapValue] -> CometMapExtract,
    classOf[GreaterThan] -> CometGreaterThan,
    classOf[GreaterThanOrEqual] -> CometGreaterThanOrEqual,
    classOf[LessThan] -> CometLessThan,
    classOf[LessThanOrEqual] -> CometLessThanOrEqual,
    classOf[IsNull] -> CometIsNull,
    classOf[IsNotNull] -> CometIsNotNull,
    classOf[IsNaN] -> CometIsNaN,
    classOf[In] -> CometIn,
    classOf[InSet] -> CometInSet,
    classOf[Rand] -> CometRand,
    classOf[Randn] -> CometRandn,
    classOf[SparkPartitionID] -> CometSparkPartitionId,
    classOf[MonotonicallyIncreasingID] -> CometMonotonicallyIncreasingId,
    classOf[StringSpace] -> CometScalarFunction("string_space"),
    classOf[StartsWith] -> CometScalarFunction("starts_with"),
    classOf[EndsWith] -> CometScalarFunction("ends_with"),
    classOf[Contains] -> CometScalarFunction("contains"),
    classOf[Substring] -> CometSubstring,
    classOf[Like] -> CometLike,
    classOf[RLike] -> CometRLike,
    classOf[OctetLength] -> CometScalarFunction("octet_length"),
    classOf[Reverse] -> CometScalarFunction("reverse"),
    classOf[StringRPad] -> CometStringRPad,
    classOf[Year] -> CometYear,
    classOf[Hour] -> CometHour,
    classOf[Minute] -> CometMinute,
    classOf[Second] -> CometSecond,
    classOf[DateAdd] -> CometDateAdd,
    classOf[DateSub] -> CometDateSub,
    classOf[TruncDate] -> CometTruncDate,
    classOf[TruncTimestamp] -> CometTruncTimestamp) ++ versionSpecificExprSerdeMap).toMap

  /**
   * Mapping of Spark aggregate expression class to Comet expression handler.
   */
  private val aggrSerdeMap: Map[Class[_], CometAggregateExpressionSerde[_]] = Map(
    classOf[Sum] -> CometSum,
    classOf[Average] -> CometAverage,
    classOf[Count] -> CometCount,
    classOf[Min] -> CometMin,
    classOf[Max] -> CometMax,
    classOf[First] -> CometFirst,
    classOf[Last] -> CometLast,
    classOf[BitAndAgg] -> CometBitAndAgg,
    classOf[BitOrAgg] -> CometBitOrAgg,
    classOf[BitXorAgg] -> CometBitXOrAgg,
    classOf[CovSample] -> CometCovSample,
    classOf[CovPopulation] -> CometCovPopulation,
    classOf[VarianceSamp] -> CometVarianceSamp,
    classOf[VariancePop] -> CometVariancePop,
    classOf[StddevSamp] -> CometStddevSamp,
    classOf[StddevPop] -> CometStddevPop,
    classOf[Corr] -> CometCorr,
    classOf[BloomFilterAggregate] -> CometBloomFilterAggregate)

  def supportedDataType(dt: DataType, allowComplex: Boolean = false): Boolean = dt match {
    case _: ByteType | _: ShortType | _: IntegerType | _: LongType | _: FloatType |
        _: DoubleType | _: StringType | _: BinaryType | _: TimestampType | _: TimestampNTZType |
        _: DecimalType | _: DateType | _: BooleanType | _: NullType =>
      true
    case s: StructType if allowComplex =>
      s.fields.nonEmpty && s.fields.map(_.dataType).forall(supportedDataType(_, allowComplex))
    case a: ArrayType if allowComplex =>
      supportedDataType(a.elementType, allowComplex)
    case m: MapType if allowComplex =>
      supportedDataType(m.keyType, allowComplex) && supportedDataType(m.valueType, allowComplex)
    case _ =>
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
      case _: TimestampNTZType => 11
      case _: DateType => 12
      case _: NullType => 13
      case _: ArrayType => 14
      case _: MapType => 15
      case _: StructType => 16
      case dt =>
        logWarning(s"Cannot serialize Spark data type: $dt")
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
          case e if frameType == RowFrame =>
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
          case _ =>
            // TODO add support for numeric and temporal RANGE BETWEEN expressions
            // see https://github.com/apache/datafusion-comet/issues/1246
            return None
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
          case e if frameType == RowFrame =>
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
          case _ =>
            // TODO add support for numeric and temporal RANGE BETWEEN expressions
            // see https://github.com/apache/datafusion-comet/issues/1246
            return None
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

    val fn = aggExpr.aggregateFunction
    val cometExpr = aggrSerdeMap.get(fn.getClass)
    cometExpr match {
      case Some(handler) =>
        handler
          .asInstanceOf[CometAggregateExpressionSerde[AggregateFunction]]
          .convert(aggExpr, fn, inputs, binding, conf)
      case _ =>
        withInfo(
          aggExpr,
          s"unsupported Spark aggregate function: ${fn.prettyName}",
          fn.children: _*)
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

    def convert[T <: Expression](expr: T, handler: CometExpressionSerde[T]): Option[Expr] = {
      handler.getSupportLevel(expr) match {
        case Unsupported =>
          withInfo(expr, s"$expr is not supported.")
          None
        case Incompatible(notes) =>
          if (CometConf.COMET_EXPR_ALLOW_INCOMPATIBLE.get()) {
            if (notes.isDefined) {
              logWarning(
                s"Comet supports $expr when ${CometConf.COMET_EXPR_ALLOW_INCOMPATIBLE.key}=true " +
                  s"but has notes: ${notes.get}")
            }
            handler.convert(expr, inputs, binding)
          } else {
            val optionalNotes = notes.map(str => s" ($str)").getOrElse("")
            withInfo(
              expr,
              s"$expr is not fully compatible with Spark$optionalNotes. To enable it anyway, " +
                s"set ${CometConf.COMET_EXPR_ALLOW_INCOMPATIBLE.key}=true. " +
                s"${CometConf.COMPAT_GUIDE}.")
            None
          }
        case Compatible(notes) =>
          if (notes.isDefined) {
            logWarning(s"Comet supports $expr but has notes: ${notes.get}")
          }
          handler.convert(expr, inputs, binding)
      }
    }

    versionSpecificExprToProtoInternal(expr, inputs, binding).orElse(expr match {
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

      case Literal(value, dataType)
          if supportedDataType(
            dataType,
            allowComplex = value == null ||
              // Nested literal support for native reader
              // can be tracked https://github.com/apache/datafusion-comet/issues/1937
              // now supports only Array of primitive
              (Seq(CometConf.SCAN_NATIVE_ICEBERG_COMPAT, CometConf.SCAN_NATIVE_DATAFUSION)
                .contains(CometConf.COMET_NATIVE_SCAN_IMPL.get()) && dataType
                .isInstanceOf[ArrayType]) && !isComplexType(
                dataType.asInstanceOf[ArrayType].elementType)) =>
        val exprBuilder = ExprOuterClass.Literal.newBuilder()

        if (value == null) {
          exprBuilder.setIsNull(true)
        } else {
          exprBuilder.setIsNull(false)
          dataType match {
            case _: BooleanType => exprBuilder.setBoolVal(value.asInstanceOf[Boolean])
            case _: ByteType => exprBuilder.setByteVal(value.asInstanceOf[Byte])
            case _: ShortType => exprBuilder.setShortVal(value.asInstanceOf[Short])
            case _: IntegerType | _: DateType => exprBuilder.setIntVal(value.asInstanceOf[Int])
            case _: LongType | _: TimestampType | _: TimestampNTZType =>
              exprBuilder.setLongVal(value.asInstanceOf[Long])
            case _: FloatType => exprBuilder.setFloatVal(value.asInstanceOf[Float])
            case _: DoubleType => exprBuilder.setDoubleVal(value.asInstanceOf[Double])
            case _: StringType =>
              exprBuilder.setStringVal(value.asInstanceOf[UTF8String].toString)
            case _: DecimalType =>
              // Pass decimal literal as bytes.
              val unscaled = value.asInstanceOf[Decimal].toBigDecimal.underlying.unscaledValue
              exprBuilder.setDecimalVal(
                com.google.protobuf.ByteString.copyFrom(unscaled.toByteArray))
            case _: BinaryType =>
              val byteStr =
                com.google.protobuf.ByteString.copyFrom(value.asInstanceOf[Array[Byte]])
              exprBuilder.setBytesVal(byteStr)
            case a: ArrayType =>
              val listLiteralBuilder = ListLiteral.newBuilder()
              val array = value.asInstanceOf[GenericArrayData].array
              a.elementType match {
                case NullType =>
                  array.foreach(_ => listLiteralBuilder.addNullMask(true))
                case BooleanType =>
                  array.foreach(v => {
                    val casted = v.asInstanceOf[java.lang.Boolean]
                    listLiteralBuilder.addBooleanValues(casted)
                    listLiteralBuilder.addNullMask(casted != null)
                  })
                case ByteType =>
                  array.foreach(v => {
                    val casted = v.asInstanceOf[java.lang.Integer]
                    listLiteralBuilder.addByteValues(casted)
                    listLiteralBuilder.addNullMask(casted != null)
                  })
                case ShortType =>
                  array.foreach(v => {
                    val casted = v.asInstanceOf[java.lang.Short]
                    listLiteralBuilder.addShortValues(
                      if (casted != null) casted.intValue()
                      else null.asInstanceOf[java.lang.Integer])
                    listLiteralBuilder.addNullMask(casted != null)
                  })
                case IntegerType | DateType =>
                  array.foreach(v => {
                    val casted = v.asInstanceOf[java.lang.Integer]
                    listLiteralBuilder.addIntValues(casted)
                    listLiteralBuilder.addNullMask(casted != null)
                  })
                case LongType | TimestampType | TimestampNTZType =>
                  array.foreach(v => {
                    val casted = v.asInstanceOf[java.lang.Long]
                    listLiteralBuilder.addLongValues(casted)
                    listLiteralBuilder.addNullMask(casted != null)
                  })
                case FloatType =>
                  array.foreach(v => {
                    val casted = v.asInstanceOf[java.lang.Float]
                    listLiteralBuilder.addFloatValues(casted)
                    listLiteralBuilder.addNullMask(casted != null)
                  })
                case DoubleType =>
                  array.foreach(v => {
                    val casted = v.asInstanceOf[java.lang.Double]
                    listLiteralBuilder.addDoubleValues(casted)
                    listLiteralBuilder.addNullMask(casted != null)
                  })
                case StringType =>
                  array.foreach(v => {
                    val casted = v.asInstanceOf[org.apache.spark.unsafe.types.UTF8String]
                    listLiteralBuilder.addStringValues(
                      if (casted != null) casted.toString else "")
                    listLiteralBuilder.addNullMask(casted != null)
                  })
                case _: DecimalType =>
                  array
                    .foreach(v => {
                      val casted =
                        v.asInstanceOf[Decimal]
                      listLiteralBuilder.addDecimalValues(if (casted != null) {
                        com.google.protobuf.ByteString
                          .copyFrom(casted.toBigDecimal.underlying.unscaledValue.toByteArray)
                      } else ByteString.EMPTY)
                      listLiteralBuilder.addNullMask(casted != null)
                    })
                case _: BinaryType =>
                  array
                    .foreach(v => {
                      val casted =
                        v.asInstanceOf[Array[Byte]]
                      listLiteralBuilder.addBytesValues(if (casted != null) {
                        com.google.protobuf.ByteString.copyFrom(casted)
                      } else ByteString.EMPTY)
                      listLiteralBuilder.addNullMask(casted != null)
                    })
              }
              exprBuilder.setListVal(listLiteralBuilder.build())
              exprBuilder.setDatatype(serializeDataType(dataType).get)
            case dt =>
              logWarning(s"Unexpected datatype '$dt' for literal value '$value'")
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

      // ToPrettyString is new in Spark 3.5
      case _
          if expr.getClass.getSimpleName == "ToPrettyString" && expr
            .isInstanceOf[UnaryExpression] && expr.isInstanceOf[TimeZoneAwareExpression] =>
        val child = expr.asInstanceOf[UnaryExpression].child
        val timezoneId = expr.asInstanceOf[TimeZoneAwareExpression].timeZoneId

        handleCast(
          expr,
          child,
          inputs,
          binding,
          DataTypes.StringType,
          timezoneId,
          CometEvalMode.TRY) match {
          case Some(_) =>
            exprToProtoInternal(child, inputs, binding) match {
              case Some(p) =>
                val toPrettyString = ExprOuterClass.ToPrettyString
                  .newBuilder()
                  .setChild(p)
                  .setTimezone(timezoneId.getOrElse("UTC"))
                  .build()
                Some(
                  ExprOuterClass.Expr
                    .newBuilder()
                    .setToPrettyString(toPrettyString)
                    .build())
              case _ =>
                withInfo(expr, child)
                None
            }
          case None =>
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

      case Atan2(left, right) =>
        val leftExpr = exprToProtoInternal(left, inputs, binding)
        val rightExpr = exprToProtoInternal(right, inputs, binding)
        val optExpr = scalarFunctionExprToProto("atan2", leftExpr, rightExpr)
        optExprWithInfo(optExpr, expr, left, right)

      case Hex(child) =>
        val childExpr = exprToProtoInternal(child, inputs, binding)
        val optExpr =
          scalarFunctionExprToProtoWithReturnType("hex", StringType, childExpr)

        optExprWithInfo(optExpr, expr, child)

      case e: Unhex =>
        val unHex = unhexSerde(e)

        val childExpr = exprToProtoInternal(unHex._1, inputs, binding)
        val failOnErrorExpr = exprToProtoInternal(unHex._2, inputs, binding)

        val optExpr =
          scalarFunctionExprToProtoWithReturnType("unhex", e.dataType, childExpr, failOnErrorExpr)
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
            val optExpr = scalarFunctionExprToProtoWithReturnType("ceil", e.dataType, childExpr)
            optExprWithInfo(optExpr, expr, child)
        }

      case e @ Floor(child) =>
        val childExpr = exprToProtoInternal(child, inputs, binding)
        child.dataType match {
          case t: DecimalType if t.scale == 0 => // zero scale is no-op
            childExpr
          case t: DecimalType if t.scale < 0 => // Spark disallows negative scale SPARK-30252
            withInfo(e, s"Decimal type $t has negative scale")
            None
          case _ =>
            val optExpr = scalarFunctionExprToProtoWithReturnType("floor", e.dataType, childExpr)
            optExprWithInfo(optExpr, expr, child)
        }

      // The expression for `log` functions is defined as null on numbers less than or equal
      // to 0. This matches Spark and Hive behavior, where non positive values eval to null
      // instead of NaN or -Infinity.
      case Log(child) =>
        val childExpr = exprToProtoInternal(nullIfNegative(child), inputs, binding)
        val optExpr = scalarFunctionExprToProto("ln", childExpr)
        optExprWithInfo(optExpr, expr, child)

      case Log10(child) =>
        val childExpr = exprToProtoInternal(nullIfNegative(child), inputs, binding)
        val optExpr = scalarFunctionExprToProto("log10", childExpr)
        optExprWithInfo(optExpr, expr, child)

      case Log2(child) =>
        val childExpr = exprToProtoInternal(nullIfNegative(child), inputs, binding)
        val optExpr = scalarFunctionExprToProto("log2", childExpr)
        optExprWithInfo(optExpr, expr, child)

      case Pow(left, right) =>
        val leftExpr = exprToProtoInternal(left, inputs, binding)
        val rightExpr = exprToProtoInternal(right, inputs, binding)
        val optExpr = scalarFunctionExprToProto("pow", leftExpr, rightExpr)
        optExprWithInfo(optExpr, expr, left, right)

      case RegExpReplace(subject, pattern, replacement, startPosition) =>
        if (!RegExp.isSupportedPattern(pattern.toString) &&
          !CometConf.COMET_REGEXP_ALLOW_INCOMPATIBLE.get()) {
          withInfo(
            expr,
            s"Regexp pattern $pattern is not compatible with Spark. " +
              s"Set ${CometConf.COMET_REGEXP_ALLOW_INCOMPATIBLE.key}=true " +
              "to allow it anyway.")
          return None
        }
        startPosition match {
          case Literal(value, DataTypes.IntegerType) if value == 1 =>
            val subjectExpr = exprToProtoInternal(subject, inputs, binding)
            val patternExpr = exprToProtoInternal(pattern, inputs, binding)
            val replacementExpr = exprToProtoInternal(replacement, inputs, binding)
            // DataFusion's regexp_replace stops at the first match. We need to add the 'g' flag
            // to apply the regex globally to match Spark behavior.
            val flagsExpr = exprToProtoInternal(Literal("g"), inputs, binding)
            val optExpr = scalarFunctionExprToProto(
              "regexp_replace",
              subjectExpr,
              patternExpr,
              replacementExpr,
              flagsExpr)
            optExprWithInfo(optExpr, expr, subject, pattern, replacement, startPosition)
          case _ =>
            withInfo(expr, "Comet only supports regexp_replace with an offset of 1 (no offset).")
            None
        }

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

      case BitwiseAnd(left, right) =>
        createBinaryExpr(
          expr,
          left,
          right,
          inputs,
          binding,
          (builder, binaryExpr) => builder.setBitwiseAnd(binaryExpr))

      case n @ Not(In(_, _)) =>
        CometNotIn.convert(n, inputs, binding)

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
        scalarFunctionExprToProto("coalesce", exprChildren: _*)

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
          scalarFunctionExprToProto("read_side_padding", argsExpr: _*)
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
        val ex = exprToProtoInternal(expr, inputs, binding)
        ex.map { child =>
          val builder = ExprOuterClass.NormalizeNaNAndZero
            .newBuilder()
            .setChild(child)
            .setDatatype(dataType.get)
          ExprOuterClass.Expr.newBuilder().setNormalizeNanAndZero(builder).build()
        }

      case s @ execution.ScalarSubquery(_, _) =>
        if (supportedDataType(s.dataType)) {
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
        } else {
          withInfo(s, s"Unsupported data type: ${s.dataType}")
          None
        }

      case UnscaledValue(child) =>
        val childExpr = exprToProtoInternal(child, inputs, binding)
        val optExpr =
          scalarFunctionExprToProtoWithReturnType("unscaled_value", LongType, childExpr)
        optExprWithInfo(optExpr, expr, child)

      case MakeDecimal(child, precision, scale, true) =>
        val childExpr = exprToProtoInternal(child, inputs, binding)
        val optExpr = scalarFunctionExprToProtoWithReturnType(
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
      case af @ ArrayFilter(_, func) if func.children.head.isInstanceOf[IsNotNull] =>
        convert(af, CometArrayCompact)
      case expr =>
        QueryPlanSerde.exprSerdeMap.get(expr.getClass) match {
          case Some(handler) =>
            convert(expr, handler.asInstanceOf[CometExpressionSerde[Expression]])
          case _ =>
            withInfo(expr, s"${expr.prettyName} is not supported", expr.children: _*)
            None
        }
    })
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

  def scalarFunctionExprToProtoWithReturnType(
      funcName: String,
      returnType: DataType,
      args: Option[Expr]*): Option[Expr] = {
    val builder = ExprOuterClass.ScalarFunc.newBuilder()
    builder.setFunc(funcName)
    serializeDataType(returnType).flatMap { t =>
      builder.setReturnType(t)
      scalarFunctionExprToProto0(builder, args: _*)
    }
  }

  def scalarFunctionExprToProto(funcName: String, args: Option[Expr]*): Option[Expr] = {
    val builder = ExprOuterClass.ScalarFunc.newBuilder()
    builder.setFunc(funcName)
    scalarFunctionExprToProto0(builder, args: _*)
  }

  private def scalarFunctionExprToProto0(
      builder: ScalarFunc.Builder,
      args: Option[Expr]*): Option[Expr] = {
    args.foreach {
      case Some(a) => builder.addArgs(a)
      case _ =>
        return None
    }
    Some(ExprOuterClass.Expr.newBuilder().setScalarFunc(builder).build())
  }

  private def nullIfNegative(expression: Expression): Expression = {
    val zero = Literal.default(expression.dataType)
    If(LessThanOrEqual(expression, zero), Literal.create(null, expression.dataType), expression)
  }

  /**
   * Returns true if given datatype is supported as a key in DataFusion sort merge join.
   */
  private def supportedSortMergeJoinEqualType(dataType: DataType): Boolean = dataType match {
    case _: ByteType | _: ShortType | _: IntegerType | _: LongType | _: FloatType |
        _: DoubleType | _: StringType | _: DateType | _: DecimalType | _: BooleanType =>
      true
    case TimestampNTZType => true
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
    val builder = OperatorOuterClass.Operator.newBuilder().setPlanId(op.id)
    childOp.foreach(builder.addChildren)

    op match {

      // Fully native scan for V1
      case scan: CometScanExec if scan.scanImpl == CometConf.SCAN_NATIVE_DATAFUSION =>
        val nativeScanBuilder = OperatorOuterClass.NativeScan.newBuilder()
        nativeScanBuilder.setSource(op.simpleStringWithNodeId())

        val scanTypes = op.output.flatten { attr =>
          serializeDataType(attr.dataType)
        }

        if (scanTypes.length == op.output.length) {
          nativeScanBuilder.addAllFields(scanTypes.asJava)

          // Sink operators don't have children
          builder.clearChildren()

          if (conf.getConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED) &&
            CometConf.COMET_RESPECT_PARQUET_FILTER_PUSHDOWN.get(conf)) {

            val dataFilters = new ListBuffer[Expr]()
            for (filter <- scan.dataFilters) {
              exprToProto(filter, scan.output) match {
                case Some(proto) => dataFilters += proto
                case _ =>
                  logWarning(s"Unsupported data filter $filter")
              }
            }
            nativeScanBuilder.addAllDataFilters(dataFilters.asJava)
          }

          val possibleDefaultValues = getExistenceDefaultValues(scan.requiredSchema)
          if (possibleDefaultValues.exists(_ != null)) {
            // Our schema has default values. Serialize two lists, one with the default values
            // and another with the indexes in the schema so the native side can map missing
            // columns to these default values.
            val (defaultValues, indexes) = possibleDefaultValues.zipWithIndex
              .filter { case (expr, _) => expr != null }
              .map { case (expr, index) =>
                // ResolveDefaultColumnsUtil.getExistenceDefaultValues has evaluated these
                // expressions and they should now just be literals.
                (Literal(expr), index.toLong.asInstanceOf[java.lang.Long])
              }
              .unzip
            nativeScanBuilder.addAllDefaultValues(
              defaultValues.flatMap(exprToProto(_, scan.output)).toIterable.asJava)
            nativeScanBuilder.addAllDefaultValuesIndexes(indexes.toIterable.asJava)
          }

          // TODO: modify CometNativeScan to generate the file partitions without instantiating RDD.
          var firstPartition: Option[PartitionedFile] = None
          scan.inputRDD match {
            case rdd: DataSourceRDD =>
              val partitions = rdd.partitions
              partitions.foreach(p => {
                val inputPartitions = p.asInstanceOf[DataSourceRDDPartition].inputPartitions
                inputPartitions.foreach(partition => {
                  if (firstPartition.isEmpty) {
                    firstPartition = partition.asInstanceOf[FilePartition].files.headOption
                  }
                  partition2Proto(
                    partition.asInstanceOf[FilePartition],
                    nativeScanBuilder,
                    scan.relation.partitionSchema)
                })
              })
            case rdd: FileScanRDD =>
              rdd.filePartitions.foreach(partition => {
                if (firstPartition.isEmpty) {
                  firstPartition = partition.files.headOption
                }
                partition2Proto(partition, nativeScanBuilder, scan.relation.partitionSchema)
              })
            case _ =>
          }

          val partitionSchema = schema2Proto(scan.relation.partitionSchema.fields)
          val requiredSchema = schema2Proto(scan.requiredSchema.fields)
          val dataSchema = schema2Proto(scan.relation.dataSchema.fields)

          val dataSchemaIndexes = scan.requiredSchema.fields.map(field => {
            scan.relation.dataSchema.fieldIndex(field.name)
          })
          val partitionSchemaIndexes = Array
            .range(
              scan.relation.dataSchema.fields.length,
              scan.relation.dataSchema.length + scan.relation.partitionSchema.fields.length)

          val projectionVector = (dataSchemaIndexes ++ partitionSchemaIndexes).map(idx =>
            idx.toLong.asInstanceOf[java.lang.Long])

          nativeScanBuilder.addAllProjectionVector(projectionVector.toIterable.asJava)

          // In `CometScanRule`, we ensure partitionSchema is supported.
          assert(partitionSchema.length == scan.relation.partitionSchema.fields.length)

          nativeScanBuilder.addAllDataSchema(dataSchema.toIterable.asJava)
          nativeScanBuilder.addAllRequiredSchema(requiredSchema.toIterable.asJava)
          nativeScanBuilder.addAllPartitionSchema(partitionSchema.toIterable.asJava)
          nativeScanBuilder.setSessionTimezone(conf.getConfString("spark.sql.session.timeZone"))
          nativeScanBuilder.setCaseSensitive(conf.getConf[Boolean](SQLConf.CASE_SENSITIVE))

          // Collect S3/cloud storage configurations
          val hadoopConf = scan.relation.sparkSession.sessionState
            .newHadoopConfWithOptions(scan.relation.options)
          firstPartition.foreach { partitionFile =>
            val objectStoreOptions =
              NativeConfig.extractObjectStoreOptions(hadoopConf, partitionFile.pathUri)
            objectStoreOptions.foreach { case (key, value) =>
              nativeScanBuilder.putObjectStoreOptions(key, value)
            }
          }

          Some(builder.setNativeScan(nativeScanBuilder).build())

        } else {
          // There are unsupported scan type
          withInfo(
            op,
            s"unsupported Comet operator: ${op.nodeName}, due to unsupported data types above")
          None
        }

      case FilterExec(condition, child) if CometConf.COMET_EXEC_FILTER_ENABLED.get(conf) =>
        val cond = exprToProto(condition, child.output)

        if (cond.isDefined && childOp.nonEmpty) {
          // Some native expressions do not support operating on dictionary-encoded arrays, so
          // wrap the child in a CopyExec to unpack dictionaries first.
          def wrapChildInCopyExec(condition: Expression): Boolean = {
            condition.exists(expr => {
              expr.isInstanceOf[StartsWith] || expr.isInstanceOf[EndsWith] || expr
                .isInstanceOf[Contains]
            })
          }

          val filterBuilder = OperatorOuterClass.Filter
            .newBuilder()
            .setPredicate(cond.get)
            .setWrapChildInCopyExec(wrapChildInCopyExec(condition))
          Some(builder.setFilter(filterBuilder).build())
        } else {
          withInfo(op, condition, child)
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
          Some(builder.setLimit(limitBuilder).build())
        } else {
          withInfo(op, "No child operator")
          None
        }

      case globalLimitExec: GlobalLimitExec
          if CometConf.COMET_EXEC_GLOBAL_LIMIT_ENABLED.get(conf) =>
        if (childOp.nonEmpty) {
          val limitBuilder = OperatorOuterClass.Limit.newBuilder()

          limitBuilder.setLimit(globalLimitExec.limit).setOffset(globalLimitExec.offset)

          Some(builder.setLimit(limitBuilder).build())
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
          Some(builder.setExpand(expandBuilder).build())
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
          Some(builder.setWindow(windowBuilder).build())
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

        if (groupingExpressions.exists(expr =>
            expr.dataType match {
              case _: MapType =>
                if (isSpark40Plus &&
                  CometConf.COMET_ENABLE_GROUPING_ON_MAP_TYPE.get(conf) &&
                  CometConf.COMET_NATIVE_SCAN_IMPL.get(conf) == CometConf.SCAN_NATIVE_DATAFUSION)
                  false
                else
                  true
              case _ => false
            })) {
          withInfo(op, "Grouping on map types is not supported")
          return None
        }

        val groupingExprs = groupingExpressions.map(exprToProto(_, child.output))
        if (groupingExprs.exists(_.isEmpty)) {
          withInfo(op, "Not all grouping expressions are supported")
          return None
        }

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
            withInfo(
              op,
              s"Unsupported result expressions found in: $resultExpressions",
              resultExpressions: _*)
            return None
          }
          hashAggBuilder.addAllResultExprs(resultExprs.map(_.get).asJava)
          Some(builder.setHashAgg(hashAggBuilder).build())
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
                withInfo(
                  op,
                  s"Unsupported result expressions found in: $resultExpressions",
                  resultExpressions: _*)
                return None
              }
              hashAggBuilder.addAllResultExprs(resultExprs.map(_.get).asJava)
            }
            hashAggBuilder.setModeValue(mode.getNumber)
            Some(builder.setHashAgg(hashAggBuilder).build())
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
          // https://github.com/apache/datafusion-comet/issues/457
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
          Some(builder.setHashJoin(joinBuilder).build())
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
          Some(builder.setSortMergeJoin(joinBuilder).build())
        } else {
          val allExprs: Seq[Expression] = join.leftKeys ++ join.rightKeys
          withInfo(join, allExprs: _*)
          None
        }

      case join: SortMergeJoinExec if !CometConf.COMET_EXEC_SORT_MERGE_JOIN_ENABLED.get(conf) =>
        withInfo(join, "SortMergeJoin is not enabled")
        None

      case op if isCometSink(op) =>
        val supportedTypes =
          op.output.forall(a => supportedDataType(a.dataType, allowComplex = true))

        if (!supportedTypes) {
          withInfo(op, "Unsupported data type")
          return None
        }

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
          builder.clearChildren()

          Some(builder.setScan(scanBuilder).build())
        } else {
          // There are unsupported scan type
          withInfo(
            op,
            s"unsupported Comet operator: ${op.nodeName}, due to unsupported data types above")
          None
        }

      case op =>
        opSerdeMap.get(op.getClass) match {
          case Some(handler) =>
            handler.enabledConfig.foreach { enabledConfig =>
              if (!enabledConfig.get(op.conf)) {
                withInfo(
                  op,
                  s"Native support for operator ${op.getClass.getSimpleName} is disabled. " +
                    s"Set ${enabledConfig.key}=true to enable it.")
                return None
              }
            }
            handler.asInstanceOf[CometOperatorSerde[SparkPlan]].convert(op, builder, childOp: _*)
          case _ =>
            // Emit warning if:
            //  1. it is not Spark shuffle operator, which is handled separately
            //  2. it is not a Comet operator
            if (!op.nodeName.contains("Comet") && !op.isInstanceOf[ShuffleExchangeExec]) {
              withInfo(op, s"unsupported Spark operator: ${op.nodeName}")
            }
            None
        }
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
      case BroadcastQueryStageExec(_, ReusedExchangeExec(_, _: CometBroadcastExchangeExec), _) =>
        true
      case _: BroadcastExchangeExec => true
      case _: WindowExec => true
      case _ => false
    }
  }

  // Utility method. Adds explain info if the result of calling exprToProto is None
  def optExprWithInfo(
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
            _: DoubleType | _: TimestampType | _: TimestampNTZType | _: DecimalType |
            _: DateType =>
          true
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

sealed trait SupportLevel

/**
 * Comet either supports this feature with full compatibility with Spark, or may have known
 * differences in some specific edge cases that are unlikely to be an issue for most users.
 *
 * Any compatibility differences are noted in the
 * [[https://datafusion.apache.org/comet/user-guide/compatibility.html Comet Compatibility Guide]].
 */
case class Compatible(notes: Option[String] = None) extends SupportLevel

/**
 * Comet supports this feature but results can be different from Spark.
 *
 * Any compatibility differences are noted in the
 * [[https://datafusion.apache.org/comet/user-guide/compatibility.html Comet Compatibility Guide]].
 */
case class Incompatible(notes: Option[String] = None) extends SupportLevel

/** Comet does not support this feature */
object Unsupported extends SupportLevel

/**
 * Trait for providing serialization logic for operators.
 */
trait CometOperatorSerde[T <: SparkPlan] {

  /**
   * Convert a Spark operator into a protocol buffer representation that can be passed into native
   * code.
   *
   * @param op
   *   The Spark operator.
   * @param builder
   *   The protobuf builder for the operator.
   * @param childOp
   *   Child operators that have already been converted to Comet.
   * @return
   *   Protocol buffer representation, or None if the operator could not be converted. In this
   *   case it is expected that the input operator will have been tagged with reasons why it could
   *   not be converted.
   */
  def convert(
      op: T,
      builder: Operator.Builder,
      childOp: Operator*): Option[OperatorOuterClass.Operator]

  /**
   * Get the optional Comet configuration entry that is used to enable or disable native support
   * for this operator.
   */
  def enabledConfig: Option[ConfigEntry[Boolean]]
}

/**
 * Trait for providing serialization logic for expressions.
 */
trait CometExpressionSerde[T <: Expression] {

  /**
   * Determine the support level of the expression based on its attributes.
   *
   * @param expr
   *   The Spark expression.
   * @return
   *   Support level (Compatible, Incompatible, or Unsupported).
   */
  def getSupportLevel(expr: T): SupportLevel = Compatible(None)

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
  def convert(expr: T, inputs: Seq[Attribute], binding: Boolean): Option[ExprOuterClass.Expr]
}

/**
 * Trait for providing serialization logic for aggregate expressions.
 */
trait CometAggregateExpressionSerde[T <: AggregateFunction] {

  /**
   * Convert a Spark expression into a protocol buffer representation that can be passed into
   * native code.
   *
   * @param aggExpr
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
      expr: T,
      inputs: Seq[Attribute],
      binding: Boolean,
      conf: SQLConf): Option[ExprOuterClass.AggExpr]
}

/** Serde for scalar function. */
case class CometScalarFunction[T <: Expression](name: String) extends CometExpressionSerde[T] {
  override def convert(expr: T, inputs: Seq[Attribute], binding: Boolean): Option[Expr] = {
    val childExpr = expr.children.map(exprToProtoInternal(_, inputs, binding))
    val optExpr = scalarFunctionExprToProto(name, childExpr: _*)
    optExprWithInfo(optExpr, expr, expr.children: _*)
  }
}
