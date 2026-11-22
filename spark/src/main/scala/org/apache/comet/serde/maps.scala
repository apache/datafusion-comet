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

import java.util.IdentityHashMap

import scala.util.DynamicVariable

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.comet.{CometNativeExec, CometPlan, MapCodegenStage, MapCodegenStagePolicy => Policy}
import org.apache.spark.sql.execution.{CollectLimitExec, ColumnarToRowExec, DeserializeToObjectExec, ExpandExec, FilterExec, GlobalLimitExec, InputAdapter, LocalLimitExec, ProjectExec, RowToColumnarExec, SortExec, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.exchange.{Exchange, ShuffleExchangeExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import org.apache.comet.CometConf
import org.apache.comet.DataTypeSupport.isComplexType
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, hasNonDefaultStringCollation, scalarFunctionExprToProto, withNullShortCircuit}
import org.apache.comet.serde.literals.CometLiteral
import org.apache.comet.shims.CometTypeShim

/**
 * Shared gate for the native map kernels that compare a lookup key against a map's stored keys
 * (`map_extract`, reached from both `GetMapValue` and `ElementAt`).
 */
private[serde] object MapKeySupport {

  private val floatingPointReason: String =
    "Spark looks a key up in its SQL ordering, so `-0.0` matches a `+0.0` key and all `NaN`s " +
      "match each other; Comet's native map lookup compares the raw Arrow values."

  private val collationReason: String =
    "Comet's native map lookup compares string keys as `UTF8_BINARY` and cannot honour a " +
      "non-default collation."

  private val complexKeyReason: String =
    "Comet's native `map_extract` casts the lookup key to the map's exact Arrow key type, which " +
      "cannot reproduce Spark's equality for a complex key type (for example a `NULL` inside the " +
      "lookup key aborts the cast against a non-nullable nested component)."

  /**
   * The `SupportLevel` for a map-consuming expression whose stored-key type is `keyType`. Spark
   * finds a key with `TypeUtils.getInterpretedOrdering` over the map's stored keys, while native
   * `map_extract` compares the Arrow values as they are, so decline the key types where those
   * disagree:
   *   - `nanSafeCompareDoubles` treats `-0.0` and `+0.0` as equal and all `NaN`s as equal, so
   *     Spark answers a `-0.0` lookup from a `+0.0` key where native finds nothing, on every
   *     version, regardless of how `ArrayBasedMapBuilder` treats the keys during construction.
   *   - a non-default collation compares under rules native applies as `UTF8_BINARY`.
   *   - for a complex key type, `map_extract`'s `coerce_types` returns the map's exact key field
   *     type, so Comet's planner casts the lookup key to it. That cannot reproduce Spark's
   *     interpreted-ordering equality, and a `NULL` inside the lookup key aborts the cast with
   *     `Non-nullable field of ListArray "item" cannot contain nulls` rather than missing the
   *     lookup. Declined for every complex key type, since which of these trips is a runtime
   *     property of the lookup.
   *
   * `BinaryType` keys need no decline: Arrow compares them by content, as Spark's ordering does.
   * The floating-point and collation checks walk every nesting level of the key type.
   */
  def keySupport(keyType: DataType): SupportLevel = {
    if (SupportLevel.containsType(keyType, classOf[FloatType], classOf[DoubleType])) {
      Unsupported(Some(floatingPointReason))
    } else if (hasNonDefaultStringCollation(keyType)) {
      Unsupported(Some(collationReason))
    } else if (isComplexType(keyType)) {
      Unsupported(Some(complexKeyReason))
    } else {
      Compatible()
    }
  }
}

object CometMapKeys extends CometExpressionSerde[MapKeys] {

  override def convert(
      expr: MapKeys,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childExpr = exprToProtoInternal(expr.child, inputs, binding)
    val mapKeysScalarExpr = scalarFunctionExprToProto("map_keys", childExpr)
    mapKeysScalarExpr
  }
}

object CometMapEntries extends CometExpressionSerde[MapEntries] {

  override def convert(
      expr: MapEntries,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childExpr = exprToProtoInternal(expr.child, inputs, binding)
    val mapEntriesScalarExpr = scalarFunctionExprToProto("map_entries", childExpr)
    mapEntriesScalarExpr
  }
}

object CometMapValues extends CometExpressionSerde[MapValues] {

  override def convert(
      expr: MapValues,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val childExpr = exprToProtoInternal(expr.child, inputs, binding)
    val mapValuesScalarExpr = scalarFunctionExprToProto("map_values", childExpr)
    mapValuesScalarExpr
  }
}

object CometMapExtract extends CometExpressionSerde[GetMapValue] {

  override def getSupportLevel(expr: GetMapValue): SupportLevel = expr.child.dataType match {
    case MapType(keyType, _, _) => MapKeySupport.keySupport(keyType)
    case _ => Compatible()
  }

  override def convert(
      expr: GetMapValue,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val mapExpr = exprToProtoInternal(expr.child, inputs, binding)
    val keyExpr = exprToProtoInternal(expr.key, inputs, binding)
    val mapExtractExpr = scalarFunctionExprToProto("map_extract", mapExpr, keyExpr)
    withNullShortCircuit(expr, mapExtractExpr)
  }
}

private[comet] object MapBuilderKeySupport {
  private val taskPolicy = CometScalarFunction[Expression](Policy.Task)
  private val driverPolicy = CometScalarFunction[Expression](Policy.Driver)

  private type Policies = IdentityHashMap[Expression, Option[Policy]]
  private class Context(plan: SparkPlan) {
    var used: Option[Policy] = None
    lazy val policies: Policies = builderPolicies(plan)
  }
  private val context = new DynamicVariable[Option[Context]](None)

  def withPlan(plan: SparkPlan, preparingStages: Boolean)(f: => SparkPlan): SparkPlan =
    if (!preparingStages) context.withValue(Some(new Context(plan)))(f)
    else if (CometConf.COMET_EXEC_ENABLED.get(plan.conf) && deferToStageCreation(plan)) plan
    else context.withValue(None)(f)

  // AQE forms codegen stages after query stage preparation, possibly with different settings.
  // Leave simple pipelines intact for preColumnarTransitions, including their original scans.
  // Other plans need the early pass to coordinate aggregate buffers and join inputs.
  private def deferToStageCreation(plan: SparkPlan): Boolean = {
    def simple(node: SparkPlan): Boolean = node match {
      case _: ProjectExec | _: FilterExec | _: ExpandExec | _: SortExec | _: LocalLimitExec |
          _: GlobalLimitExec | _: CollectLimitExec | _: DeserializeToObjectExec |
          _: ShuffleExchangeExec =>
        true
      case _ => node.children.isEmpty
    }
    def candidate(expr: Expression): Boolean = expr match {
      case m @ (_: MapFromArrays | _: MapFromEntries) =>
        isFloatingPoint(m.dataType.asInstanceOf[MapType].keyType) && m.children.forall(safeInput)
      case _ => false
    }
    !plan.exists(node => !simple(node)) && plan.exists {
      case op @ (_: ProjectExec | _: FilterExec | _: ExpandExec) =>
        op.expressions.exists(_.exists(candidate))
      case _ => false
    }
  }

  private def select(expr: Expression): Option[Policy] =
    context.value.flatMap(_.policies.getOrDefault(expr, None))

  // Every admitted builder belonging to one operator has the same stage and consumers.
  def record(f: => Option[CometNativeExec]): Option[CometNativeExec] = context.value match {
    case None => f
    case Some(ctx) =>
      val outer = ctx.used
      ctx.used = None
      try {
        val result = f
        result.foreach(exec => ctx.used.foreach(_.record(exec)))
        result
      } finally {
        ctx.used = outer
      }
  }

  private def builderPolicies(plan: SparkPlan): Policies = {
    val policies = new Policies
    if (plan.exists(_.isInstanceOf[CometPlan])) return policies
    val conf = SQLConf.get
    if (conf.getConf(SQLConf.CODEGEN_FACTORY_MODE).toString == "NO_CODEGEN") return policies

    // Walk the stages Spark would form before Comet replaces scans or fuses operators.
    // None for consumers means the first insertion can happen in an unknown scope.
    def visit(
        node: SparkPlan,
        stage: Option[MapCodegenStage],
        consumers: Option[List[MapCodegenStage]]): Unit = node match {
      case generated: WholeStageCodegenExec =>
        val probe = MapCodegenStage.probe(generated)
        visit(generated.child, probe, consumers.filter(_ => probe.isDefined))
      case adapter: InputAdapter => visit(adapter.child, None, consumers)
      case _ =>
        val inputConsumers = node match {
          case _: Exchange => Some(Nil)
          case _: ProjectExec | _: FilterExec | _: DeserializeToObjectExec | _: LocalLimitExec |
              _: GlobalLimitExec | _: CollectLimitExec | _: ExpandExec | _: ColumnarToRowExec |
              _: RowToColumnarExec =>
            consumers
          case _: HashAggregateExec | _: SortExec =>
            for (outer <- consumers; current <- stage) yield current :: outer
          case _ => None
        }
        val policy = node match {
          case _: ProjectExec | _: FilterExec => consumers.map(Policy(stage, _))
          case _: ExpandExec if stage.isDefined => consumers.map(Policy(stage, _))
          case _: HashAggregateExec => inputConsumers.map(Policy(stage, _))
          case _ => None
        }
        def recordExpression(expr: Expression, interpreted: Boolean): Unit = {
          expr match {
            case _: MapFromArrays | _: MapFromEntries =>
              val selected = if (interpreted) None else policy
              val previous = policies.getOrDefault(expr, selected)
              val _ = policies.put(expr, if (previous == selected) selected else None)
            case _ =>
          }
          expr.children.foreach(
            recordExpression(_, interpreted || expr.isInstanceOf[CodegenFallback]))
        }
        node.expressions.foreach(recordExpression(_, interpreted = false))
        node match {
          case agg: HashAggregateExec =>
            // A fallback aggregate evaluates result expressions eagerly only for empty input.
            agg.resultExpressions.foreach(recordExpression(_, interpreted = true))
          case _ =>
        }
        node.children.foreach(visit(_, stage, inputConsumers))
    }
    visit(MapCodegenStage.prepare(plan), None, Some(Nil))
    policies
  }

  def convertFloatMap(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val policy = select(expr)
    val native = if (policy.exists(_.stage.isDefined)) driverPolicy else taskPolicy
    val result = withNullShortCircuit(expr, native.convert(expr, inputs, binding))
    if (result.isDefined && policy.isDefined) context.value.foreach(_.used = policy)
    result.orElse {
      CometScalaUDF.emitJvmCodegenDispatch(expr, inputs, binding)
    }
  }

  val nestedFloatingPointReason: String =
    "Spark compares a `FLOAT` or `DOUBLE` inside a struct or array key using SQL ordering, " +
      "where signed zeros are equal and all NaNs are equal. Comet's native map builders " +
      "compare their bits for these nested keys."

  def isFloatingPoint(keyType: DataType): Boolean =
    keyType == FloatType || keyType == DoubleType

  private def isColumn(expr: Expression): Boolean = expr match {
    case _: AttributeReference | _: BoundReference => true
    case field: GetStructField => isColumn(field.child)
    case _ => false
  }

  // Spark codegen emits canonical NaNs for scalar literals. Java serialization does the same
  // for the boxed elements of folded GenericArrayData. Protobuf preserves their raw bits.
  private def sparkPreservesLiteralBits(value: Any): Boolean = value match {
    case f: Float => java.lang.Float.floatToRawIntBits(f) == java.lang.Float.floatToIntBits(f)
    case d: Double =>
      java.lang.Double.doubleToRawLongBits(d) == java.lang.Double.doubleToLongBits(d)
    case a: GenericArrayData => a.array.forall(sparkPreservesLiteralBits)
    case _ => true
  }

  // Constructors evaluate their children across the batch and can broadcast a large value
  // before LAST_WIN discards it. Only build entries from fixed-width, eagerly evaluable fields.
  private def safeElement(expr: Expression): Boolean = expr match {
    case Literal(v, _) if !sparkPreservesLiteralBits(v) => false
    case s: CreateNamedStruct => s.valExprs.nonEmpty && s.valExprs.forall(safeElement)
    case _: UnaryMinus | _: Add | _: Subtract | _: Multiply =>
      isFloatingPoint(expr.dataType) && expr.children.forall(safeElement)
    case c: Cast if isFloatingPoint(c.dataType) =>
      (c.child.dataType == NullType || c.child.dataType.isInstanceOf[NumericType]) &&
      safeElement(c.child)
    case _ =>
      (expr.isInstanceOf[Literal] || isColumn(expr)) &&
      (expr.dataType.isInstanceOf[DecimalType] || UnsafeRow.isFixedLength(expr.dataType))
  }

  private def simpleArray(expr: Expression): Boolean = expr match {
    case array: CreateArray => array.children.forall(safeElement)
    case _ => isColumn(expr)
  }

  private def safeInput(expr: Expression): Boolean = expr match {
    case Literal(value, ArrayType(element, _)) =>
      CometLiteral.listLiteralElementSupported(element) && sparkPreservesLiteralBits(value)
    case zip: ArraysZip => zip.children.nonEmpty && zip.children.forall(simpleArray)
    case _ => simpleArray(expr)
  }

  val computedInputReason: String =
    "Floating-point map builders use Spark to preserve evaluation of computed inputs."

  val policyReason: String =
    "Floating-point map builders use Spark when the operator can change when map settings are read."

  def keySupport(keyType: DataType, expr: Expression): SupportLevel = {
    if (isFloatingPoint(keyType)) {
      if (!expr.children.forall(safeInput)) {
        Incompatible(Some(computedInputReason))
      } else if (select(expr).isEmpty) {
        Incompatible(Some(policyReason))
      } else {
        Compatible()
      }
    } else if (SupportLevel.containsType(keyType, classOf[FloatType], classOf[DoubleType])) {
      Incompatible(Some(nestedFloatingPointReason))
    } else if (MapKeyDedupPolicySupport.isLastWin) {
      Incompatible(Some(MapKeyDedupPolicySupport.incompatibleReason))
    } else {
      Compatible()
    }
  }
}

private object MapKeyDedupPolicySupport {
  val incompatibleReason: String =
    s"Comet's native map builders support `${SQLConf.MAP_KEY_DEDUP_POLICY.key}=" +
      s"${SQLConf.MapKeyDedupPolicy.LAST_WIN}` only for `FLOAT` and `DOUBLE` keys."

  val nullKeyReason: String =
    "Spark rejects a `NULL` map key. For key types other than `FLOAT` and `DOUBLE`, " +
      "Comet's native map builders can produce a map with a `NULL` key " +
      "([#4680](https://github.com/apache/datafusion-comet/issues/4680))."

  def isLastWin: Boolean =
    SQLConf.get
      .getConf(SQLConf.MAP_KEY_DEDUP_POLICY)
      .toString
      .equalsIgnoreCase(SQLConf.MapKeyDedupPolicy.LAST_WIN.toString)
}

object CometMapFromArrays
    extends CometExpressionSerde[MapFromArrays]
    with CodegenDispatchFallback {

  override def getIncompatibleReasons(): Seq[String] =
    Seq(
      MapBuilderKeySupport.computedInputReason,
      MapBuilderKeySupport.policyReason,
      MapBuilderKeySupport.nestedFloatingPointReason,
      MapKeyDedupPolicySupport.incompatibleReason)

  override def getCompatibleNotes(): Seq[String] =
    Seq(MapKeyDedupPolicySupport.nullKeyReason)

  override def getSupportLevel(expr: MapFromArrays): SupportLevel =
    MapBuilderKeySupport.keySupport(expr.dataType.keyType, expr)

  override def convert(
      expr: MapFromArrays,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val keyType = expr.left.dataType.asInstanceOf[ArrayType].elementType
    if (MapBuilderKeySupport.isFloatingPoint(keyType)) {
      return MapBuilderKeySupport.convertFloatMap(expr, inputs, binding)
    }
    val keysExpr = exprToProtoInternal(expr.left, inputs, binding)
    val valuesExpr = exprToProtoInternal(expr.right, inputs, binding)
    val valueType = expr.right.dataType.asInstanceOf[ArrayType].elementType
    val returnType = MapType(keyType = keyType, valueType = valueType)
    for {
      keysNotNullExprProto <- exprToProtoInternal(IsNotNull(expr.left), inputs, binding)
      valuesNotNullExprProto <- exprToProtoInternal(IsNotNull(expr.right), inputs, binding)
      mapFromArraysExprProto <- scalarFunctionExprToProto("map", keysExpr, valuesExpr)
      nullLiteralExprProto <- exprToProtoInternal(Literal(null, returnType), inputs, binding)
    } yield {
      // Spark skips the values expression when keys are null. Nested CASE guards preserve
      // this evaluation order; a native AND may evaluate both operands for the whole batch.
      val valuesCaseWhenExprProto = ExprOuterClass.CaseWhen
        .newBuilder()
        .addWhen(valuesNotNullExprProto)
        .addThen(mapFromArraysExprProto)
        .setElseExpr(nullLiteralExprProto)
        .build()
      val caseWhenExprProto = ExprOuterClass.CaseWhen
        .newBuilder()
        .addWhen(keysNotNullExprProto)
        .addThen(ExprOuterClass.Expr.newBuilder().setCaseWhen(valuesCaseWhenExprProto).build())
        .setElseExpr(nullLiteralExprProto)
        .build()
      ExprOuterClass.Expr
        .newBuilder()
        .setCaseWhen(caseWhenExprProto)
        .build()
    }
  }

}

object CometMapFromEntries
    extends CometScalarFunction[MapFromEntries]("map_from_entries")
    with CodegenDispatchFallback {
  val keyUnsupportedReason =
    "`BinaryType` is not supported as a map key in `map_from_entries`"
  val valueUnsupportedReason =
    "`BinaryType` is not supported as a map value in `map_from_entries`"

  override def getIncompatibleReasons(): Seq[String] =
    Seq(
      keyUnsupportedReason,
      valueUnsupportedReason,
      MapBuilderKeySupport.computedInputReason,
      MapBuilderKeySupport.policyReason,
      MapBuilderKeySupport.nestedFloatingPointReason,
      MapKeyDedupPolicySupport.incompatibleReason)

  override def getCompatibleNotes(): Seq[String] =
    Seq(MapKeyDedupPolicySupport.nullKeyReason)

  override def getSupportLevel(expr: MapFromEntries): SupportLevel = {
    if (SupportLevel.containsType(expr.dataType.keyType, classOf[BinaryType])) {
      Incompatible(Some(keyUnsupportedReason))
    } else if (SupportLevel.containsType(expr.dataType.valueType, classOf[BinaryType])) {
      Incompatible(Some(valueUnsupportedReason))
    } else {
      MapBuilderKeySupport.keySupport(expr.dataType.keyType, expr)
    }
  }

  override def convert(
      expr: MapFromEntries,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    if (MapBuilderKeySupport.isFloatingPoint(expr.dataType.keyType)) {
      MapBuilderKeySupport.convertFloatMap(expr, inputs, binding)
    } else {
      super.convert(expr, inputs, binding)
    }
  }
}

object CometStrToMap
    extends CometScalarFunction[StringToMap]("str_to_map")
    with CometTypeShim
    with CodegenDispatchFallback {

  // Spark 4.1.1+ honours spark.sql.legacy.truncateForEmptyRegexSplit by truncating trailing
  // empty entries from the split result. Comet's native str_to_map always behaves as if the flag
  // were false. When the flag is true, mark this Incompatible so the CodegenDispatchFallback
  // trait routes the expression through the JVM codegen dispatcher (Spark's own doGenCode inside
  // the Comet kernel) rather than falling the entire projection back to Spark. Read by string
  // key so it resolves on older Spark versions where the config is not registered.
  private val legacyTruncateConfig = "spark.sql.legacy.truncateForEmptyRegexSplit"

  private val legacyTruncateReason =
    s"`$legacyTruncateConfig` is enabled, so trailing empty split entries may differ from Spark."

  private val collationReason =
    "`str_to_map` does not support non-UTF8_BINARY collations on the input string or delimiters."

  override def getIncompatibleReasons(): Seq[String] =
    Seq(legacyTruncateReason, collationReason)

  override def getSupportLevel(expr: StringToMap): SupportLevel = {
    if (SQLConf.get.getConfString(legacyTruncateConfig, "false").toBoolean) {
      Incompatible(Some(legacyTruncateReason))
    } else if (expr.children.exists(child => hasNonDefaultStringCollation(child.dataType))) {
      Incompatible(Some(collationReason))
    } else {
      Compatible(None)
    }
  }
}

object CometCreateMap extends CometCodegenDispatch[CreateMap]

object CometMapFilter extends CometCodegenDispatch[MapFilter]

object CometTransformKeys extends CometCodegenDispatch[TransformKeys]

object CometTransformValues extends CometCodegenDispatch[TransformValues]

object CometMapZipWith extends CometCodegenDispatch[MapZipWith]

object CometMapConcat extends CometCodegenDispatch[MapConcat]
