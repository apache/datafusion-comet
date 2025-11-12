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

import scala.jdk.CollectionConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.optimizer.NormalizeNaNAndZero
import org.apache.spark.sql.catalyst.util.CharVarcharCodegenUtils
import org.apache.spark.sql.comet._
import org.apache.spark.sql.execution
import org.apache.spark.sql.execution._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.expressions._
import org.apache.comet.serde.ExprOuterClass.{AggExpr, Expr, ScalarFunc}
import org.apache.comet.serde.Types.{DataType => ProtoDataType}
import org.apache.comet.serde.Types.DataType._
import org.apache.comet.serde.literals.CometLiteral
import org.apache.comet.serde.operator._
import org.apache.comet.shims.CometExprShim

/**
 * An utility object for query plan and expression serialization.
 */
object QueryPlanSerde extends Logging with CometExprShim {

  private val arrayExpressions: Map[Class[_ <: Expression], CometExpressionSerde[_]] = Map(
    classOf[ArrayAppend] -> CometArrayAppend,
    classOf[ArrayCompact] -> CometArrayCompact,
    classOf[ArrayContains] -> CometArrayContains,
    classOf[ArrayDistinct] -> CometArrayDistinct,
    classOf[ArrayExcept] -> CometArrayExcept,
    classOf[ArrayFilter] -> CometArrayFilter,
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
    classOf[ElementAt] -> CometElementAt,
    classOf[Flatten] -> CometFlatten,
    classOf[GetArrayItem] -> CometGetArrayItem)

  private val conditionalExpressions: Map[Class[_ <: Expression], CometExpressionSerde[_]] =
    Map(classOf[CaseWhen] -> CometCaseWhen, classOf[If] -> CometIf)

  private val predicateExpressions: Map[Class[_ <: Expression], CometExpressionSerde[_]] = Map(
    classOf[And] -> CometAnd,
    classOf[EqualTo] -> CometEqualTo,
    classOf[EqualNullSafe] -> CometEqualNullSafe,
    classOf[GreaterThan] -> CometGreaterThan,
    classOf[GreaterThanOrEqual] -> CometGreaterThanOrEqual,
    classOf[LessThan] -> CometLessThan,
    classOf[LessThanOrEqual] -> CometLessThanOrEqual,
    classOf[In] -> CometIn,
    classOf[IsNotNull] -> CometIsNotNull,
    classOf[IsNull] -> CometIsNull,
    classOf[InSet] -> CometInSet,
    classOf[Not] -> CometNot,
    classOf[Or] -> CometOr)

  private val mathExpressions: Map[Class[_ <: Expression], CometExpressionSerde[_]] = Map(
    classOf[Acos] -> CometScalarFunction("acos"),
    classOf[Add] -> CometAdd,
    classOf[Asin] -> CometScalarFunction("asin"),
    classOf[Atan] -> CometScalarFunction("atan"),
    classOf[Atan2] -> CometAtan2,
    classOf[Ceil] -> CometCeil,
    classOf[Cos] -> CometScalarFunction("cos"),
    classOf[Divide] -> CometDivide,
    classOf[Exp] -> CometScalarFunction("exp"),
    classOf[Expm1] -> CometScalarFunction("expm1"),
    classOf[Floor] -> CometFloor,
    classOf[Hex] -> CometHex,
    classOf[IntegralDivide] -> CometIntegralDivide,
    classOf[IsNaN] -> CometIsNaN,
    classOf[Log] -> CometLog,
    classOf[Log2] -> CometLog2,
    classOf[Log10] -> CometLog10,
    classOf[Multiply] -> CometMultiply,
    classOf[Pow] -> CometScalarFunction("pow"),
    classOf[Rand] -> CometRand,
    classOf[Randn] -> CometRandn,
    classOf[Remainder] -> CometRemainder,
    classOf[Round] -> CometRound,
    classOf[Signum] -> CometScalarFunction("signum"),
    classOf[Sin] -> CometScalarFunction("sin"),
    classOf[Sqrt] -> CometScalarFunction("sqrt"),
    classOf[Subtract] -> CometSubtract,
    classOf[Tan] -> CometScalarFunction("tan"),
    classOf[UnaryMinus] -> CometUnaryMinus,
    classOf[Unhex] -> CometUnhex,
    classOf[Abs] -> CometAbs)

  private val mapExpressions: Map[Class[_ <: Expression], CometExpressionSerde[_]] = Map(
    classOf[GetMapValue] -> CometMapExtract,
    classOf[MapKeys] -> CometMapKeys,
    classOf[MapEntries] -> CometMapEntries,
    classOf[MapValues] -> CometMapValues,
    classOf[MapFromArrays] -> CometMapFromArrays)

  private val structExpressions: Map[Class[_ <: Expression], CometExpressionSerde[_]] = Map(
    classOf[CreateNamedStruct] -> CometCreateNamedStruct,
    classOf[GetArrayStructFields] -> CometGetArrayStructFields,
    classOf[GetStructField] -> CometGetStructField,
    classOf[StructsToJson] -> CometStructsToJson)

  private val hashExpressions: Map[Class[_ <: Expression], CometExpressionSerde[_]] = Map(
    classOf[Md5] -> CometScalarFunction("md5"),
    classOf[Murmur3Hash] -> CometMurmur3Hash,
    classOf[Sha2] -> CometSha2,
    classOf[XxHash64] -> CometXxHash64,
    classOf[Sha1] -> CometSha1)

  private val stringExpressions: Map[Class[_ <: Expression], CometExpressionSerde[_]] = Map(
    classOf[Ascii] -> CometScalarFunction("ascii"),
    classOf[BitLength] -> CometScalarFunction("bit_length"),
    classOf[Chr] -> CometScalarFunction("char"),
    classOf[ConcatWs] -> CometScalarFunction("concat_ws"),
    classOf[Concat] -> CometConcat,
    classOf[Contains] -> CometScalarFunction("contains"),
    classOf[EndsWith] -> CometScalarFunction("ends_with"),
    classOf[InitCap] -> CometInitCap,
    classOf[Length] -> CometLength,
    classOf[Like] -> CometLike,
    classOf[Lower] -> CometLower,
    classOf[OctetLength] -> CometScalarFunction("octet_length"),
    classOf[RegExpReplace] -> CometRegExpReplace,
    classOf[Reverse] -> CometScalarFunction("reverse"),
    classOf[RLike] -> CometRLike,
    classOf[StartsWith] -> CometScalarFunction("starts_with"),
    classOf[StringInstr] -> CometScalarFunction("instr"),
    classOf[StringRepeat] -> CometStringRepeat,
    classOf[StringReplace] -> CometScalarFunction("replace"),
    classOf[StringRPad] -> CometStringRPad,
    classOf[StringLPad] -> CometStringLPad,
    classOf[StringSpace] -> CometScalarFunction("string_space"),
    classOf[StringTranslate] -> CometScalarFunction("translate"),
    classOf[StringTrim] -> CometScalarFunction("trim"),
    classOf[StringTrimBoth] -> CometScalarFunction("btrim"),
    classOf[StringTrimLeft] -> CometScalarFunction("ltrim"),
    classOf[StringTrimRight] -> CometScalarFunction("rtrim"),
    classOf[Substring] -> CometSubstring,
    classOf[Upper] -> CometUpper)

  private val bitwiseExpressions: Map[Class[_ <: Expression], CometExpressionSerde[_]] = Map(
    classOf[BitwiseAnd] -> CometBitwiseAnd,
    classOf[BitwiseCount] -> CometBitwiseCount,
    classOf[BitwiseGet] -> CometBitwiseGet,
    classOf[BitwiseOr] -> CometBitwiseOr,
    classOf[BitwiseNot] -> CometBitwiseNot,
    classOf[BitwiseXor] -> CometBitwiseXor,
    classOf[ShiftLeft] -> CometShiftLeft,
    classOf[ShiftRight] -> CometShiftRight)

  private val temporalExpressions: Map[Class[_ <: Expression], CometExpressionSerde[_]] = Map(
    classOf[DateAdd] -> CometDateAdd,
    classOf[DateSub] -> CometDateSub,
    classOf[FromUnixTime] -> CometFromUnixTime,
    classOf[Hour] -> CometHour,
    classOf[Minute] -> CometMinute,
    classOf[Second] -> CometSecond,
    classOf[TruncDate] -> CometTruncDate,
    classOf[TruncTimestamp] -> CometTruncTimestamp,
    classOf[Year] -> CometYear,
    classOf[Month] -> CometMonth,
    classOf[DayOfMonth] -> CometDayOfMonth,
    classOf[DayOfWeek] -> CometDayOfWeek,
    classOf[WeekDay] -> CometWeekDay,
    classOf[DayOfYear] -> CometDayOfYear,
    classOf[WeekOfYear] -> CometWeekOfYear,
    classOf[Quarter] -> CometQuarter)

  private val conversionExpressions: Map[Class[_ <: Expression], CometExpressionSerde[_]] = Map(
    classOf[Cast] -> CometCast)

  private val miscExpressions: Map[Class[_ <: Expression], CometExpressionSerde[_]] = Map(
    // TODO PromotePrecision
    // TODO KnownFloatingPointNormalized
    // TODO ScalarSubquery
    // TODO UnscaledValue
    // TODO MakeDecimal
    // TODO BloomFilterMightContain
    // TODO RegExpReplace
    classOf[Alias] -> CometAlias,
    classOf[AttributeReference] -> CometAttributeReference,
    classOf[CheckOverflow] -> CometCheckOverflow,
    classOf[Coalesce] -> CometCoalesce,
    classOf[Literal] -> CometLiteral,
    classOf[MonotonicallyIncreasingID] -> CometMonotonicallyIncreasingId,
    classOf[SparkPartitionID] -> CometSparkPartitionId,
    classOf[SortOrder] -> CometSortOrder)

  /**
   * Mapping of Spark expression class to Comet expression handler.
   */
  val exprSerdeMap: Map[Class[_ <: Expression], CometExpressionSerde[_]] =
    mathExpressions ++ hashExpressions ++ stringExpressions ++
      conditionalExpressions ++ mapExpressions ++ predicateExpressions ++
      structExpressions ++ bitwiseExpressions ++ miscExpressions ++ arrayExpressions ++
      temporalExpressions ++ conversionExpressions

  /**
   * Mapping of Spark aggregate expression class to Comet expression handler.
   */
  val aggrSerdeMap: Map[Class[_], CometAggregateExpressionSerde[_]] = Map(
    classOf[Average] -> CometAverage,
    classOf[BitAndAgg] -> CometBitAndAgg,
    classOf[BitOrAgg] -> CometBitOrAgg,
    classOf[BitXorAgg] -> CometBitXOrAgg,
    classOf[BloomFilterAggregate] -> CometBloomFilterAggregate,
    classOf[Corr] -> CometCorr,
    classOf[Count] -> CometCount,
    classOf[CovPopulation] -> CometCovPopulation,
    classOf[CovSample] -> CometCovSample,
    classOf[First] -> CometFirst,
    classOf[Last] -> CometLast,
    classOf[Max] -> CometMax,
    classOf[Min] -> CometMin,
    classOf[StddevPop] -> CometStddevPop,
    classOf[StddevSamp] -> CometStddevSamp,
    classOf[Sum] -> CometSum,
    classOf[VariancePop] -> CometVariancePop,
    classOf[VarianceSamp] -> CometVarianceSamp)

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
  def serializeDataType(dt: org.apache.spark.sql.types.DataType): Option[Types.DataType] = {
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

    // Support Count(distinct single_value)
    // COUNT(DISTINCT x) - supported
    // COUNT(DISTINCT x, x) - supported through transition to COUNT(DISTINCT x)
    // COUNT(DISTINCT x, y) - not supported
    if (aggExpr.isDistinct
      &&
      !(aggExpr.aggregateFunction.prettyName == "count" &&
        aggExpr.aggregateFunction.children.length == 1)) {
      withInfo(aggExpr, s"Distinct aggregate not supported for: $aggExpr")
      return None
    }

    val fn = aggExpr.aggregateFunction
    val cometExpr = aggrSerdeMap.get(fn.getClass)
    cometExpr match {
      case Some(handler) =>
        val aggHandler = handler.asInstanceOf[CometAggregateExpressionSerde[AggregateFunction]]
        val exprConfName = aggHandler.getExprConfigName(fn)
        if (!CometConf.isExprEnabled(exprConfName)) {
          withInfo(
            aggExpr,
            "Expression support is disabled. Set " +
              s"${CometConf.getExprEnabledConfigKey(exprConfName)}=true to enable it.")
          return None
        }
        aggHandler.convert(aggExpr, fn, inputs, binding, conf)
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

    def convert[T <: Expression](expr: T, handler: CometExpressionSerde[T]): Option[Expr] = {
      val exprConfName = handler.getExprConfigName(expr)
      if (!CometConf.isExprEnabled(exprConfName)) {
        withInfo(
          expr,
          "Expression support is disabled. Set " +
            s"${CometConf.getExprEnabledConfigKey(exprConfName)}=true to enable it.")
        return None
      }
      handler.getSupportLevel(expr) match {
        case Unsupported(notes) =>
          withInfo(expr, notes.getOrElse(""))
          None
        case Incompatible(notes) =>
          val exprAllowIncompat = CometConf.isExprAllowIncompat(exprConfName)
          if (exprAllowIncompat || CometConf.COMET_EXPR_ALLOW_INCOMPATIBLE.get()) {
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
                s"set ${CometConf.getExprAllowIncompatConfigKey(exprConfName)}=true, or set " +
                s"${CometConf.COMET_EXPR_ALLOW_INCOMPATIBLE.key}=true to enable all " +
                s"incompatible expressions. ${CometConf.COMPAT_GUIDE}.")
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

      case UnaryExpression(child) if expr.prettyName == "promote_precision" =>
        // `UnaryExpression` includes `PromotePrecision` for Spark 3.3
        // `PromotePrecision` is just a wrapper, don't need to serialize it.
        exprToProtoInternal(child, inputs, binding)

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
          scalarFunctionExprToProtoWithReturnType("unscaled_value", LongType, false, childExpr)
        optExprWithInfo(optExpr, expr, child)

      case MakeDecimal(child, precision, scale, true) =>
        val childExpr = exprToProtoInternal(child, inputs, binding)
        val optExpr = scalarFunctionExprToProtoWithReturnType(
          "make_decimal",
          DecimalType(precision, scale),
          false,
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
      case r @ Reverse(child) if child.dataType.isInstanceOf[ArrayType] =>
        convert(r, CometArrayReverse)
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
      failOnError: Boolean,
      args: Option[Expr]*): Option[Expr] = {
    val builder = ExprOuterClass.ScalarFunc.newBuilder()
    builder.setFunc(funcName)
    builder.setFailOnError(failOnError)
    serializeDataType(returnType).flatMap { t =>
      builder.setReturnType(t)
      scalarFunctionExprToProto0(builder, args: _*)
    }
  }

  def scalarFunctionExprToProto(funcName: String, args: Option[Expr]*): Option[Expr] = {
    val builder = ExprOuterClass.ScalarFunc.newBuilder()
    builder.setFunc(funcName)
    builder.setFailOnError(false)
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

  // scalastyle:off
  /**
   * Align w/ Arrow's
   * [[https://github.com/apache/arrow-rs/blob/55.2.0/arrow-ord/src/rank.rs#L30-L40 can_rank]] and
   * [[https://github.com/apache/arrow-rs/blob/55.2.0/arrow-ord/src/sort.rs#L193-L215 can_sort_to_indices]]
   *
   * TODO: Include SparkSQL's [[YearMonthIntervalType]] and [[DayTimeIntervalType]]
   */
  // scalastyle:on
  def supportedSortType(op: SparkPlan, sortOrder: Seq[SortOrder]): Boolean = {
    def canRank(dt: DataType): Boolean = {
      dt match {
        case _: ByteType | _: ShortType | _: IntegerType | _: LongType | _: FloatType |
            _: DoubleType | _: DecimalType =>
          true
        case _: DateType | _: TimestampType | _: TimestampNTZType =>
          true
        case _: BooleanType | _: BinaryType | _: StringType => true
        case _ => false
      }
    }

    if (sortOrder.length == 1) {
      val canSort = sortOrder.head.dataType match {
        case _: ByteType | _: ShortType | _: IntegerType | _: LongType | _: FloatType |
            _: DoubleType | _: DecimalType =>
          true
        case _: DateType | _: TimestampType | _: TimestampNTZType =>
          true
        case _: BooleanType | _: BinaryType | _: StringType => true
        case ArrayType(elementType, _) => canRank(elementType)
        case MapType(_, valueType, _) => canRank(valueType)
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

}
