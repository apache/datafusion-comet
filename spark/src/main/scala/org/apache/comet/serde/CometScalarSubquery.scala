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

import scala.util.DynamicVariable

import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, DayOfWeek, Expression, ExprId, GetDateField, Hour, LastDay, Minute, NamedExpression, NextDay, Second, TimeZoneAwareExpression, ToUnixTimestamp, TruncDate, UnixTimestamp, WeekDay}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.trees.TreePattern.PLAN_EXPRESSION
import org.apache.spark.sql.comet.CometExec
import org.apache.spark.sql.execution.{ExecSubqueryExpression, ExpandExec, GenerateExec, ScalarSubquery, SparkPlan, UnionExec}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.types._

import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.serde.QueryPlanSerde.{serializeDataType, supportedDataType}

object CometScalarSubquery extends CometExpressionSerde[ScalarSubquery] {

  private val structSubqueryInputs =
    new TreeNodeTag[Set[ExprId]]("CometStructScalarSubqueryInputs")
  private val structSubqueryContext =
    new TreeNodeTag[Boolean]("CometStructScalarSubqueryContext")
  private val currentStructSubqueryInputs = new DynamicVariable[Set[ExprId]](Set.empty)
  private val currentPlanHasStructSubquery = new DynamicVariable[Boolean](false)

  /**
   * Trace struct scalar values before native conversion replaces their producing projections.
   * Attribute object identity is insufficient: aliases, nullable join outputs and reused stages
   * can create fresh attributes. Keep provenance by ExprId on the owning plan instead.
   *
   * This is recomputed on every rule application, including the plans hidden by AQE/reuse nodes.
   * No provenance is inferred from a column's data type or carried in a global ExprId registry.
   */
  private[comet] def tagPlanInputs(root: SparkPlan): Unit = {
    val visited = new java.util.IdentityHashMap[SparkPlan, Set[ExprId]]()
    def remember(plan: SparkPlan, sources: Set[ExprId]): Unit = {
      val _ = visited.put(plan, sources)
    }

    def visit(plan: SparkPlan): Set[ExprId] = {
      if (visited.containsKey(plan)) return visited.get(plan)
      remember(plan, Set.empty)
      plan.unsetTagValue(structSubqueryInputs)
      plan.unsetTagValue(structSubqueryContext)

      val original = plan match {
        case comet: CometExec => comet.originalPlan
        case other => other
      }
      val children = plan match {
        case reused: ReusedExchangeExec => Seq(reused.child)
        case stage: QueryStageExec => Seq(stage.plan)
        case adaptive: AdaptiveSparkPlanExec => Seq(adaptive.executedPlan)
        case comet: CometExec if comet.children.isEmpty => Seq(comet.originalPlan)
        case other => other.children
      }
      val childSources = children.map(visit)
      var sources = childSources.flatten.toSet
      // A struct subquery can enable native conversion of both its operator and ancestors.
      // Keep that context even when a later projection drops the struct: sibling/ancestor
      // calendar consumers can still receive ordinary columns outside the native range.
      val hasStructContext =
        children.exists(_.getTagValue(structSubqueryContext).contains(true)) ||
          original.expressions.exists(hasStructSubquerySource(_, sources))
      plan.setTagValue(structSubqueryContext, hasStructContext)
      if (sources.isEmpty &&
        !original.expressions.exists(_.containsPattern(PLAN_EXPRESSION))) {
        return Set.empty
      }

      // Union and reused stages can expose different ExprIds for the same output positions.
      original match {
        case _: UnionExec | _: ReusedExchangeExec | _: QueryStageExec |
            _: AdaptiveSparkPlanExec =>
          plan.output.zipWithIndex.foreach { case (output, ordinal) =>
            if (children.zip(childSources).exists { case (child, ids) =>
                child.output.lift(ordinal).exists(a => ids.contains(a.exprId))
              }) {
              sources += output.exprId
            }
          }
        case _ =>
      }

      val named = original.expressions.flatMap(_.collect { case e: NamedExpression => e })
      var changed = true
      while (changed) {
        val previous = sources
        named.foreach { expression =>
          if (hasStructSubquerySource(expression, sources)) sources += expression.exprId
        }
        original match {
          case aggregate: BaseAggregateExec =>
            aggregate.aggregateExpressions.zip(aggregate.aggregateAttributes).foreach {
              case (expression, output) =>
                val function = expression.aggregateFunction
                if (hasStructSubquerySource(expression, sources) ||
                  function.inputAggBufferAttributes.exists(a => sources.contains(a.exprId))) {
                  sources += output.exprId
                  sources ++= function.aggBufferAttributes.map(_.exprId)
                  sources ++= function.inputAggBufferAttributes.map(_.exprId)
                }
            }
          case generate: GenerateExec if hasStructSubquerySource(generate.generator, sources) =>
            // Generators expose fresh attributes rather than NamedExpression aliases.
            sources ++= generate.generatorOutput.map(_.exprId)
          case expand: ExpandExec =>
            expand.output.zipWithIndex.foreach { case (output, ordinal) =>
              if (expand.projections.exists(p => hasStructSubquerySource(p(ordinal), sources))) {
                sources += output.exprId
              }
            }
          case _ =>
        }
        changed = previous != sources
      }

      plan.setTagValue(structSubqueryInputs, sources)
      val outputs = plan.output.iterator.map(_.exprId).filter(sources.contains).toSet
      remember(plan, outputs)
      outputs
    }

    val _ = visit(root)
  }

  /** Keep the lineage local to this conversion, even when an attribute was copied or rebound. */
  private[comet] def withPlanInputs[T](plan: SparkPlan)(f: => T): T = {
    val inputs = plan.getTagValue(structSubqueryInputs).getOrElse(Set.empty)
    currentStructSubqueryInputs.withValue(inputs) {
      currentPlanHasStructSubquery.withValue(
        plan.getTagValue(structSubqueryContext).getOrElse(false))(f)
    }
  }

  private val calendarConsumerFallbackReason =
    "Calendar operations on struct scalar subqueries require Spark because their runtime values " +
      "may exceed the native calendar range"

  private val timezoneConsumerFallbackReason =
    "Timezone-dependent scalar subquery operations require Spark for non-fixed timezones"

  private val dateUnixTimestampFallbackReason =
    "Date scalar subqueries in unix_timestamp require Spark because their runtime values " +
      "may exceed the native calendar range"

  private def containsCalendarType(dt: DataType): Boolean = dt match {
    case DateType | TimestampType | TimestampNTZType => true
    case s: StructType => s.fields.exists(f => containsCalendarType(f.dataType))
    case ArrayType(elementType, _) => containsCalendarType(elementType)
    case MapType(keyType, valueType, _) =>
      containsCalendarType(keyType) || containsCalendarType(valueType)
    case _ => false
  }

  private def isTimestamp(dt: DataType): Boolean =
    dt == TimestampType || dt == TimestampNTZType

  private def isEpochCast(from: DataType, to: DataType): Boolean =
    (isTimestamp(from) && to == LongType) || (from == LongType && isTimestamp(to))

  private def hasStructSubquerySource(expr: Expression, inputs: Set[ExprId]): Boolean =
    expr.references.exists(a => inputs.contains(a.exprId)) ||
      (expr.containsPattern(PLAN_EXPRESSION) && expr.exists {
        case subquery: ExecSubqueryExpression => subquery.dataType.isInstanceOf[StructType]
        case _ => false
      })

  private def hasStructSubquery(expr: Expression): Boolean =
    hasStructSubquerySource(expr, currentStructSubqueryInputs.value)

  private def hasStructSubqueryContext(expr: Expression): Boolean =
    currentPlanHasStructSubquery.value || hasStructSubquery(expr)

  private def clockFallbackReason(
      child: Expression,
      timeZoneId: Option[String]): Option[String] = {
    if (child.dataType == TimestampNTZType || CometTimeZone.isFixedOffset(timeZoneId)) {
      None
    } else {
      Some(timezoneConsumerFallbackReason)
    }
  }

  /**
   * Keep newly supported scalar consumers on Spark when native calendar or timezone rules cannot
   * cover their unknown runtime values. The struct bridge itself remains compatible: transport,
   * field access, comparisons and epoch arithmetic do not interpret a calendar.
   *
   * Check the whole expression before serialization or codegen dispatch, including wrappers and
   * whole-struct casts. A physical subquery may be reused, but it is still an
   * [[ExecSubqueryExpression]]; its result must never be evaluated while planning this check.
   */
  private[serde] def consumerFallbackReason(expr: Expression): Option[String] = {
    // Skip paths without struct subqueries. A newly admitted calendar consumer can be a
    // sibling or ancestor of a struct-producing operator without using its result directly.
    // Aliases still carry value provenance through the owning plan's ExprIds.
    val derivedInput =
      expr.references.exists(a => currentStructSubqueryInputs.value.contains(a.exprId))
    if (!currentPlanHasStructSubquery.value && !derivedInput &&
      (!expr.containsPattern(PLAN_EXPRESSION) || !ExecSubqueryExpression.hasSubquery(expr))) {
      return None
    }

    val reason = expr match {
      case cast: Cast if hasStructSubqueryContext(cast) =>
        val from = cast.child.dataType
        val to = cast.dataType
        if (from != to && !isEpochCast(from, to) &&
          (containsCalendarType(from) || containsCalendarType(to))) {
          Some(calendarConsumerFallbackReason)
        } else {
          None
        }
      // These two date fields use epoch-day arithmetic rather than chrono's calendar.
      case _: DayOfWeek | _: WeekDay => None
      case field: GetDateField if hasStructSubqueryContext(field) =>
        Some(calendarConsumerFallbackReason)
      case lastDay: LastDay if hasStructSubqueryContext(lastDay) =>
        Some(calendarConsumerFallbackReason)
      case nextDay: NextDay if hasStructSubqueryContext(nextDay) =>
        Some(calendarConsumerFallbackReason)
      // TruncDate is a TruncInstant, not a GetDateField or TimeZoneAwareExpression.
      case trunc: TruncDate if hasStructSubqueryContext(trunc.date) =>
        Some(calendarConsumerFallbackReason)
      // These scalar kernels cover the full timestamp range without calendar conversion when
      // the offset is fixed, and NTZ clock fields never apply the session timezone.
      case hour: Hour => clockFallbackReason(hour.child, hour.timeZoneId)
      case minute: Minute => clockFallbackReason(minute.child, minute.timeZoneId)
      case second: Second => clockFallbackReason(second.child, second.timeZoneId)
      // Timestamp-to-epoch conversions are arithmetic; dates first need calendar conversion.
      case unix: UnixTimestamp if isTimestamp(unix.timeExp.dataType) => None
      case unix: ToUnixTimestamp if isTimestamp(unix.timeExp.dataType) => None
      case unix: UnixTimestamp
          if unix.timeExp.dataType == DateType &&
            (ExecSubqueryExpression.hasSubquery(unix.timeExp) || hasStructSubqueryContext(
              unix.timeExp)) =>
        Some(dateUnixTimestampFallbackReason)
      // Other newly admitted timezone-aware consumers can use chrono even in UTC (for example
      // date_format). Unlike the exceptions above, their full runtime range is not compatible.
      case aware: TimeZoneAwareExpression if hasStructSubqueryContext(aware) =>
        Some(calendarConsumerFallbackReason)
      case _ => None
    }

    reason.orElse {
      expr.children.iterator.map(consumerFallbackReason).collectFirst { case Some(childReason) =>
        childReason
      }
    }
  }

  override def getUnsupportedReasons(): Seq[String] = Seq(
    "Not all data types are supported for scalar subquery results",
    "Struct fields must have supported types and distinct names at every nesting level")

  // The shared type gate handles general capabilities. Only the Arrow IPC scalar-subquery
  // bridge's additional struct-only and decimal-scale restrictions belong here.
  private def supportedStructShape(dt: DataType): Boolean = dt match {
    case s: StructType => s.fields.forall(f => supportedStructShape(f.dataType))
    case _: ArrayType | _: MapType => false
    case d: DecimalType => d.scale >= 0 && d.scale <= d.precision
    case _ => true
  }

  override def getSupportLevel(expr: ScalarSubquery): SupportLevel = {
    val supported = expr.dataType match {
      case s: StructType =>
        supportedDataType(
          s,
          allowComplex = true,
          allowIntervals = false,
          allowCalendarInterval = false,
          allowTimeType = false,
          allowAnyStringType = false,
          allowDuplicateStructFieldNames = false) && supportedStructShape(s)
      case dt => supportedDataType(dt)
    }
    if (supported) {
      Compatible()
    } else {
      Unsupported(Some(s"Unsupported data type: ${expr.dataType}"))
    }
  }

  override def convert(
      expr: ScalarSubquery,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    // getSupportLevel has already checked value-transfer support. Type serialization can still
    // decline, so keep this separate check.
    val dataType = serializeDataType(expr.dataType)
    if (dataType.isEmpty) {
      withFallbackReason(
        expr,
        s"Failed to serialize datatype ${expr.dataType} for scalar subquery")
      return None
    }

    val builder = ExprOuterClass.Subquery
      .newBuilder()
      .setId(expr.exprId.id)
      .setDatatype(dataType.get)
    Some(ExprOuterClass.Expr.newBuilder().setSubquery(builder).build())
  }
}
