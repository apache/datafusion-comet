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

import org.apache.comet.CometConf.COMET_EXEC_STRICT_FLOATING_POINT
import org.apache.comet.DataTypeSupport.isComplexType
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, hasNonDefaultStringCollation, scalarFunctionExprToProto}
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

/**
 * Shared gate for the native map constructors (`map_from_arrays`, `map_from_entries`), which
 * reproduce Spark's `ArrayBasedMapBuilder`: they reject a `NULL` key with `NULL_MAP_KEY` and
 * follow `spark.sql.mapKeyDedupPolicy`, whose value Comet forwards to the native session as
 * `datafusion.spark.map_key_dedup_policy`.
 */
private object MapBuilderSupport {

  /**
   * `ArrayBasedMapBuilder` normalizes a floating-point key before storing it, so a `-0.0` key is
   * stored as `+0.0` and every `NaN` collapses to one canonical `NaN`. The native builders
   * compare the raw Arrow values, so a map built from both `-0.0` and `+0.0` keeps two entries
   * where Spark reports a duplicate key. This is a note rather than a decline because a map keyed
   * on `-0.0` or `NaN` is rare; `spark.comet.exec.strictFloatingPoint` declines it for users who
   * want the guarantee.
   */
  val floatingPointKeyNote: String =
    "Spark normalizes a floating-point map key, so a `-0.0` key is stored as `+0.0` and all " +
      "`NaN` keys collapse into one. Comet's native map construction compares the raw Arrow " +
      "values, so `-0.0` and `+0.0` stay distinct keys rather than a duplicate key. Set " +
      s"`${COMET_EXEC_STRICT_FLOATING_POINT.key}=true` to fall back to Spark for a " +
      "floating-point map key."

  /** The support level for a map constructor whose result has key type `keyType`. */
  def keySupport(keyType: DataType): SupportLevel =
    SupportLevel
      .strictFloatingPointReason(keyType, "Map construction on a floating-point key")
      .map(reason => Incompatible(Some(reason)))
      .getOrElse(Compatible(None))
}

object CometMapFromArrays extends CometExpressionSerde[MapFromArrays] {

  override def getCompatibleNotes(): Seq[String] =
    Seq(MapBuilderSupport.floatingPointKeyNote)

  override def getSupportLevel(expr: MapFromArrays): SupportLevel =
    MapBuilderSupport.keySupport(expr.dataType.keyType)

  override def convert(
      expr: MapFromArrays,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val keysExpr = exprToProtoInternal(expr.left, inputs, binding)
    val valuesExpr = exprToProtoInternal(expr.right, inputs, binding)
    // Native `map_from_arrays` is null intolerant like Spark's: a NULL keys or values array
    // yields a NULL map for that row, so no CaseWhen guard is needed here.
    scalarFunctionExprToProto("map_from_arrays", keysExpr, valuesExpr)
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
    Seq(keyUnsupportedReason, valueUnsupportedReason)

  override def getCompatibleNotes(): Seq[String] =
    Seq(MapBuilderSupport.floatingPointKeyNote)

  override def getSupportLevel(expr: MapFromEntries): SupportLevel = {
    if (SupportLevel.containsType(expr.dataType.keyType, classOf[BinaryType])) {
      Incompatible(Some(keyUnsupportedReason))
    } else if (SupportLevel.containsType(expr.dataType.valueType, classOf[BinaryType])) {
      Incompatible(Some(valueUnsupportedReason))
    } else {
      MapBuilderSupport.keySupport(expr.dataType.keyType)
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
