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

import org.apache.spark.sql.{CometTestBase, Row}
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Cast, Coalesce, CreateArray, CreateNamedStruct, DateAdd, DateDiff, DayOfWeek, EqualNullSafe, EqualTo, GetStructField, GreaterThanOrEqual, Hour, If, In, LessThan, Literal, NamedExpression, Not, OctetLength, Rand, TruncDate, UnixDate, WeekDay}
import org.apache.spark.sql.execution.{ProjectExec, ScalarSubquery, SubqueryExec}
import org.apache.spark.sql.types._

import org.apache.comet.CometSparkSessionExtensions.{isSpark40Plus, isSpark41Plus}
import org.apache.comet.shims.CometTypeShim

/** Direct type-gate tests avoid optimizer folding and exercise types Parquet cannot store. */
class CometScalarSubquerySuite extends CometTestBase with CometTypeShim {

  private def struct(dt: DataType): StructType = StructType(Seq(StructField("value", dt)))

  private lazy val emptyInput = spark.range(0).queryExecution.sparkPlan

  private def subquery(dt: DataType): ScalarSubquery = {
    // Inspect the declared result type without executing or optimizing the typed NULL away.
    val plan = ProjectExec(Seq(Alias(Literal.create(null, dt), "result")()), emptyInput)
    ScalarSubquery(SubqueryExec("type-check", plan), NamedExpression.newExprId)
  }

  private def supported(dt: DataType): Boolean =
    CometScalarSubquery.getSupportLevel(subquery(dt)) == Compatible()

  private def versionSpecificTypes: Seq[DataType] = {
    val strings =
      if (isSpark40Plus) Seq(DataType.fromDDL("STRING COLLATE UTF8_LCASE")) else Seq.empty
    val times = if (isSpark41Plus) Seq(DataType.fromDDL("TIME")) else Seq.empty
    strings ++ times ++ variantType.toSeq
  }

  private val scalarTypes: Seq[DataType] = Seq(
    BooleanType,
    ByteType,
    ShortType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    StringType,
    BinaryType,
    DateType,
    TimestampType,
    TimestampNTZType,
    NullType,
    DecimalType(1, 0),
    DecimalType(10, 2),
    DecimalType(38, 38))

  private def nestedTypes(dt: DataType): Seq[DataType] = Seq(
    dt,
    struct(dt),
    struct(struct(dt)),
    ArrayType(dt),
    MapType(dt, IntegerType),
    MapType(IntegerType, dt),
    struct(ArrayType(MapType(StringType, dt))))

  test("non-struct scalar subqueries use the shared type gate defaults") {
    val types = scalarTypes ++ versionSpecificTypes ++ Seq(
      CalendarIntervalType,
      YearMonthIntervalType(),
      DayTimeIntervalType(),
      CharType(5),
      VarcharType(5),
      StructType(Nil),
      StructType(Seq(StructField("same", IntegerType), StructField("same", LongType))))
    types.flatMap(nestedTypes).foreach { dt =>
      if (!dt.isInstanceOf[StructType]) {
        assert(supported(dt) == QueryPlanSerde.supportedDataType(dt), dt)
      }
    }
  }

  test("struct scalar subqueries retain supported fields and case-distinct names") {
    val distinct =
      StructType(Seq(StructField("a", IntegerType), StructField("A", LongType)))
    (scalarTypes.map(struct) ++ scalarTypes.map(dt => struct(struct(dt))) ++
      Seq(distinct, struct(distinct))).foreach { dt =>
      assert(supported(dt), dt)
      assert(CometScalarSubquery.convert(subquery(dt), Seq.empty, binding = false).isDefined, dt)
    }
  }

  test("struct scalar subqueries reject unsupported fields and shapes recursively") {
    val rejected = versionSpecificTypes ++ Seq(
      ArrayType(IntegerType),
      MapType(StringType, IntegerType),
      StructType(Nil),
      StructType(Seq(StructField("same", IntegerType), StructField("same", LongType))),
      CalendarIntervalType,
      YearMonthIntervalType(),
      DayTimeIntervalType(),
      CharType(5),
      VarcharType(5))
    rejected.foreach { dt =>
      Seq(struct(dt), struct(struct(dt))).foreach { nested =>
        assert(!supported(nested), nested)
      }
    }
    rejected.collect { case s: StructType => s }.foreach(s => assert(!supported(s), s))
  }

  test("struct decimal scale restrictions do not change the legacy non-struct gate") {
    withSQLConf("spark.sql.legacy.allowNegativeScaleOfDecimal" -> "true") {
      val negative = DecimalType(10, -2)
      assert(QueryPlanSerde.supportedDataType(struct(negative), allowComplex = true))
      assert(supported(negative))
      assert(!supported(struct(negative)))
      assert(!supported(struct(struct(negative))))
      Seq(DecimalType(1, 0), DecimalType(38, 0), DecimalType(38, 38)).foreach { dt =>
        assert(supported(struct(dt)), dt)
        assert(supported(struct(struct(dt))), dt)
      }
    }
  }

  test("nondeterministic variable-width consumers retain Spark fallback") {
    val condition = GreaterThanOrEqual(Rand(Literal(42L)), Literal(0.0))
    Seq(StringType, BinaryType, struct(StringType)).foreach { dt =>
      val field = GetStructField(subquery(struct(dt)), 0)
      val expr = If(condition, field, Literal.create(null, dt))
      assert(
        CometScalarSubquery.consumerFallbackReason(expr).exists(_.startsWith("Nondeterministic")))
      assert(QueryPlanSerde.exprToProtoInternal(expr, Seq.empty, binding = false).isEmpty)
    }
    val numeric = GetStructField(subquery(struct(LongType)), 0)
    assert(
      CometScalarSubquery.consumerFallbackReason(If(condition, numeric, Literal(0L))).isEmpty)
    val plain = If(condition, Literal("x"), Literal(""))
    assert(CometScalarSubquery.consumerFallbackReason(plain).isEmpty)
    val deterministic = OctetLength(GetStructField(subquery(struct(StringType)), 0))
    assert(CometScalarSubquery.consumerFallbackReason(deterministic).isEmpty)
  }

  test("row-dependent nested membership retains Spark short-circuit evaluation") {
    val dt = StructType(Seq(StructField("s", StringType), StructField("f", DoubleType)))
    val scalar = subquery(dt)
    val column = AttributeReference("candidate", dt)()
    val literal = Literal.create(Row("x", Double.NaN), dt)
    val foldable =
      CreateNamedStruct(Seq(Literal("s"), Literal("x"), Literal("f"), Literal(Double.NaN)))
    val dynamic = In(scalar, Seq(column))
    Seq(
      dynamic,
      In(column, Seq(scalar)),
      In(column, Seq(foldable, scalar)),
      If(Literal(true), dynamic, Literal(false)),
      Alias(dynamic, "membership")()).foreach { expr =>
      assert(
        CometScalarSubquery
          .consumerFallbackReason(expr)
          .exists(_.startsWith("Nested membership")),
        expr)
      assert(QueryPlanSerde.exprToProtoInternal(expr, Seq(column), binding = false).isEmpty)
    }
    Seq(In(scalar, Seq(literal)), In(scalar, Seq(subquery(dt))), In(column, Seq(literal)))
      .foreach { expr =>
        withSQLConf("spark.sql.subexpressionElimination.enabled" -> "false") {
          QueryPlanSerde.withSharedSubexpressions(Seq(expr)) {
            assert(CometScalarSubquery.consumerFallbackReason(expr).isEmpty, expr)
          }
        }
      }
    val source = Alias(scalar, "derived")()
    val child = ProjectExec(Seq(source), emptyInput)
    val derived = In(source.toAttribute, Seq(column))
    val parent = ProjectExec(Seq(Alias(derived, "result")()), child)
    CometScalarSubquery.tagPlanInputs(parent)
    CometScalarSubquery.withPlanInputs(parent) {
      assert(CometScalarSubquery.consumerFallbackReason(derived).isDefined)
      // The plan context also protects a sibling after a struct-producing conversion.
      assert(CometScalarSubquery.consumerFallbackReason(In(column, Seq(foldable))).isDefined)
      assert(
        CometScalarSubquery.consumerFallbackReason(In(source.toAttribute, Seq(literal))).isEmpty)
    }
    assert(CometScalarSubquery.consumerFallbackReason(In(column, Seq(foldable))).isEmpty)
    Seq(CreateArray(Seq(scalar)), CreateArray(Seq(CreateArray(Seq(scalar))))).foreach { nested =>
      val candidate = AttributeReference("nested", nested.dataType)()
      assert(CometScalarSubquery.consumerFallbackReason(In(nested, Seq(candidate))).isDefined)
      val derived = Alias(nested, "derived_nested")()
      val child = ProjectExec(Seq(derived), emptyInput)
      val expr = In(derived.toAttribute, Seq(candidate))
      val parent = ProjectExec(Seq(Alias(expr, "result")()), child)
      CometScalarSubquery.tagPlanInputs(parent)
      CometScalarSubquery.withPlanInputs(parent) {
        assert(CometScalarSubquery.consumerFallbackReason(expr).isDefined)
      }
    }
  }

  test("constant scalar membership requires shared-expression authorization") {
    val dt = StructType(Seq(StructField("s", StringType), StructField("f", DoubleType)))
    val scalar = subquery(dt)
    val literal = Literal.create(Row("x", 0.0d), dt)
    val bad = CreateNamedStruct(
      Seq(
        Literal("s"),
        Cast(Cast(GetStructField(subquery(struct(StringType)), 0), IntegerType), StringType),
        Literal("f"),
        Literal(0.0d)))
    val first = In(scalar, Seq(literal, bad))
    val second = In(scalar, Seq(scalar, bad))
    assert(first.references.isEmpty && second.references.isEmpty)
    assert(CometScalarSubquery.consumerFallbackReason(first).isDefined)
    Seq(false, true).foreach { shared =>
      withSQLConf("spark.sql.subexpressionElimination.enabled" -> shared.toString) {
        QueryPlanSerde.withSharedSubexpressions(Seq(first, second)) {
          Seq(first, second).foreach { expr =>
            assert(CometScalarSubquery.consumerFallbackReason(expr).isDefined == shared)
            if (shared)
              assert(QueryPlanSerde.exprToProtoInternal(expr, Seq.empty, binding = false).isEmpty)
          }
        }
      }
    }
  }

  test("comparison NULL short-circuit flags require shared-expression authorization") {
    val left = AttributeReference("left", StringType)()
    val bad = AttributeReference("bad", StringType)()
    val right = Cast(Cast(bad, IntegerType), StringType)
    val equal = EqualTo(left, right)
    val less = LessThan(left, right)
    val inputs = Seq(left, bad)
    def proto(expr: org.apache.spark.sql.catalyst.expressions.Expression): ExprOuterClass.Expr =
      QueryPlanSerde.exprToProtoInternal(expr, inputs, binding = false).get
    assert(!proto(equal).getEq.getNullShortCircuit)
    QueryPlanSerde.withSharedSubexpressions(Seq(equal)) {
      assert(proto(equal).getEq.getNullShortCircuit)
      assert(proto(Not(equal)).getNeq.getNullShortCircuit)
      assert(!proto(EqualNullSafe(left, right)).getEqNullSafe.getNullShortCircuit)
    }
    QueryPlanSerde.withSharedSubexpressions(Seq(equal, less), alwaysShared = true) {
      assert(!proto(equal).getEq.getNullShortCircuit)
      assert(!proto(less).getLt.getNullShortCircuit)
    }
  }

  test("opaque serialized consumers retain Spark fallback only for variable-width sources") {
    val text = GetStructField(subquery(struct(StringType)), 0)
    val numeric = GetStructField(subquery(struct(LongType)), 0)
    val opaque = ExprOuterClass.Expr
      .newBuilder()
      .setJvmScalarUdf(ExprOuterClass.JvmScalarUdf.newBuilder().setClassName("opaque"))
      .build()
    val parent = ExprOuterClass.Expr
      .newBuilder()
      .setScalarFunc(ExprOuterClass.ScalarFunc.newBuilder().setFunc("test").addArgs(opaque))
      .build()
    Seq(opaque, parent).foreach { proto =>
      assert(CometScalarSubquery.dispatchedConsumerFallbackReason(text, proto).isDefined)
      assert(CometScalarSubquery.dispatchedConsumerFallbackReason(numeric, proto).isEmpty)
      assert(CometScalarSubquery.dispatchedConsumerFallbackReason(Literal("x"), proto).isEmpty)
    }
    assert(
      CometScalarSubquery
        .dispatchedConsumerFallbackReason(text, ExprOuterClass.Expr.getDefaultInstance)
        .isEmpty)
  }

  test("calendar consumers require an allow-list even when their result is not a date") {
    val date = GetStructField(subquery(struct(DateType)), 0)
    val epoch = Literal.create(0, DateType)
    val difference = DateDiff(date, epoch)
    val unlisted = Seq(
      difference,
      DateAdd(date, Literal(1)),
      TruncDate(date, Literal("YEAR")),
      Cast(date, StringType),
      Coalesce(Seq(difference, Literal(0))),
      If(Literal(true), difference, Literal(0)))
    unlisted.foreach { expr =>
      assert(
        CometScalarSubquery
          .consumerFallbackReason(expr)
          .exists(_.contains("Calendar operations")),
        expr)
    }
    // Ordinary column/literal expressions must not acquire scalar-subquery provenance.
    assert(CometScalarSubquery.consumerFallbackReason(DateDiff(epoch, epoch)).isEmpty)
  }

  test("scalar-subquery guards record reasons in the current conversion scope") {
    val text = GetStructField(subquery(struct(StringType)), 0)
    val nondeterministic =
      If(GreaterThanOrEqual(Rand(Literal(42L)), Literal(0.0)), text, Literal(""))
    val calendar = TruncDate(GetStructField(subquery(struct(DateType)), 0), Literal("YEAR"))
    Seq(nondeterministic -> "Nondeterministic", calendar -> "Calendar operations")
      .foreach { case (expr, prefix) =>
        val (converted, reasons) =
          QueryPlanSerde.exprToProtoWithFallbackReasons(expr, Seq.empty, binding = false)
        assert(converted.isEmpty)
        assert(reasons.exists(_.startsWith(prefix)), reasons)
        assert(org.apache.comet.CometExplainInfo.currentFallbackReasons(expr).isEmpty)
      }
    val (converted, reasons) =
      QueryPlanSerde.exprToProtoWithFallbackReasons(Literal(42), Seq.empty, binding = false)
    assert(converted.nonEmpty)
    assert(reasons.isEmpty)
  }

  test("calendar allow-list retains raw transport comparison epoch and clock consumers") {
    val date = GetStructField(subquery(struct(DateType)), 0)
    val timestamp = GetStructField(subquery(struct(TimestampType)), 0)
    val ntz = GetStructField(subquery(struct(TimestampNTZType)), 0)
    val allowed = Seq(
      date,
      Alias(date, "renamed")(),
      Coalesce(Seq(date, Literal.create(0, DateType))),
      If(Literal(true), date, Literal.create(null, DateType)),
      EqualNullSafe(date, date),
      UnixDate(date),
      DayOfWeek(date),
      WeekDay(date),
      Cast(timestamp, LongType, Some("UTC")),
      Hour(timestamp, Some("+01:00")),
      Hour(ntz, Some("America/Los_Angeles")))
    allowed.foreach { expr =>
      assert(CometScalarSubquery.consumerFallbackReason(expr).isEmpty, expr)
    }
    assert(
      CometScalarSubquery
        .consumerFallbackReason(Hour(timestamp, Some("America/Los_Angeles")))
        .exists(_.contains("non-fixed timezones")))
  }
}
