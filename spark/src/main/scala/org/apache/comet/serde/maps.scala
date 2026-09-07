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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import org.apache.comet.DataTypeSupport.isComplexType
import org.apache.comet.serde.QueryPlanSerde.{createBinaryExpr, exprToProtoInternal, hasNonDefaultStringCollation, scalarFunctionExprToProto}
import org.apache.comet.shims.CometTypeShim

/**
 * Shared gate for the native map kernels that compare a lookup key against a map's stored keys
 * (`map_extract`, reached from both `GetMapValue` and `ElementAt`).
 */
private[serde] object MapKeySupport {

  private val floatingPointReason: String =
    "Spark normalizes floating-point map keys, so `-0.0` matches a `+0.0` key and all `NaN`s " +
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
   * finds a key with `TypeUtils.getInterpretedOrdering` over the keys `ArrayBasedMapBuilder`
   * stored, having first normalized them, while native `map_extract` compares the Arrow values as
   * they are, so decline the key types where those disagree:
   *   - `ArrayBasedMapBuilder` rewrites a `-0.0` key to `+0.0` and canonicalises `NaN`, and
   *     `nanSafeCompareDoubles` treats `-0.0` and `+0.0` as equal, so Spark answers a `-0.0`
   *     lookup from a `+0.0` key where native finds nothing.
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
    mapExtractExpr
  }
}

private object MapKeyDedupPolicySupport {
  val incompatibleReason: String =
    s"`${SQLConf.MAP_KEY_DEDUP_POLICY.key}` is set to " +
      s"`${SQLConf.MapKeyDedupPolicy.LAST_WIN}`; Comet's native map construction " +
      "does not implement LAST_WIN dedup semantics."

  val nullKeyReason: String =
    "Spark rejects a `NULL` element inside the keys array with a `RuntimeException`" +
      " (`Cannot use null as map key`); Comet's native `map_from_arrays` / `map_from_entries`" +
      " does not detect a per-element `NULL` key and produces a map with a `NULL` key instead" +
      " ([#4680](https://github.com/apache/datafusion-comet/issues/4680))."

  def isLastWin: Boolean =
    SQLConf.get
      .getConf(SQLConf.MAP_KEY_DEDUP_POLICY)
      .toString
      .equalsIgnoreCase(SQLConf.MapKeyDedupPolicy.LAST_WIN.toString)
}

object CometMapFromArrays extends CometExpressionSerde[MapFromArrays] {

  override def getIncompatibleReasons(): Seq[String] =
    Seq(MapKeyDedupPolicySupport.incompatibleReason)

  override def getCompatibleNotes(): Seq[String] =
    Seq(MapKeyDedupPolicySupport.nullKeyReason)

  override def getSupportLevel(expr: MapFromArrays): SupportLevel = {
    if (MapKeyDedupPolicySupport.isLastWin) {
      Incompatible(Some(MapKeyDedupPolicySupport.incompatibleReason))
    } else {
      Compatible(None)
    }
  }

  override def convert(
      expr: MapFromArrays,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val keysExpr = exprToProtoInternal(expr.left, inputs, binding)
    val valuesExpr = exprToProtoInternal(expr.right, inputs, binding)
    val keyType = expr.left.dataType.asInstanceOf[ArrayType].elementType
    val valueType = expr.right.dataType.asInstanceOf[ArrayType].elementType
    val returnType = MapType(keyType = keyType, valueType = valueType)
    for {
      andBinaryExprProto <- createAndBinaryExpr(expr, inputs, binding)
      mapFromArraysExprProto <- scalarFunctionExprToProto("map", keysExpr, valuesExpr)
      nullLiteralExprProto <- exprToProtoInternal(Literal(null, returnType), inputs, binding)
    } yield {
      val caseWhenExprProto = ExprOuterClass.CaseWhen
        .newBuilder()
        .addWhen(andBinaryExprProto)
        .addThen(mapFromArraysExprProto)
        .setElseExpr(nullLiteralExprProto)
        .build()
      ExprOuterClass.Expr
        .newBuilder()
        .setCaseWhen(caseWhenExprProto)
        .build()
    }
  }

  private def createAndBinaryExpr(
      expr: MapFromArrays,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    createBinaryExpr(
      expr,
      IsNotNull(expr.left),
      IsNotNull(expr.right),
      inputs,
      binding,
      (builder, binaryExpr) => builder.setAnd(binaryExpr))
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
    Seq(keyUnsupportedReason, valueUnsupportedReason, MapKeyDedupPolicySupport.incompatibleReason)

  override def getCompatibleNotes(): Seq[String] =
    Seq(MapKeyDedupPolicySupport.nullKeyReason)

  override def getSupportLevel(expr: MapFromEntries): SupportLevel = {
    if (SupportLevel.containsType(expr.dataType.keyType, classOf[BinaryType])) {
      Incompatible(Some(keyUnsupportedReason))
    } else if (SupportLevel.containsType(expr.dataType.valueType, classOf[BinaryType])) {
      Incompatible(Some(valueUnsupportedReason))
    } else if (MapKeyDedupPolicySupport.isLastWin) {
      Incompatible(Some(MapKeyDedupPolicySupport.incompatibleReason))
    } else {
      Compatible(None)
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
