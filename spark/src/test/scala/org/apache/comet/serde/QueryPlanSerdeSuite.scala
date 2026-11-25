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

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer
import org.apache.spark.sql.catalyst.expressions.{Abs, Add, Alias, ArrayContains, ArrayPosition, AttributeReference, AttributeSeq, BindReferences, Cast, CheckOverflow, Concat, CreateArray, CreateNamedStruct, DayOfMonth, Divide, ElementAt, EqualTo, EvalMode, Expression, GetStructField, If, In, IntegralDivide, IsNull, Length, Literal, Lower, MakeDate, MapFromArrays, Month, Murmur3Hash, Not, Pmod, Remainder, Slice, StartsWith, StringTrim, StringTrimLeft, StringTrimRight, Substring, UnaryMinus, UnsafeProjection, XxHash64, Year}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Project}
import org.apache.spark.sql.comet.DecimalPrecision
import org.apache.spark.sql.comet.util.Utils.equalsIgnoreCompatibleNullability
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import org.apache.comet.CometExplainInfo
import org.apache.comet.CometSparkSessionExtensions.{isSpark40Plus, isSpark41Plus}
import org.apache.comet.serde.QueryPlanSerde.{canEvaluateSkippedEagerly, canShortCircuitNulls, canSkipEvaluation, scalarFunctionExprToProto, skipsSharedSubexpression, supportedDataType, uncertainSharedSubexpressionReason, withNullShortCircuit, withSharedSubexpressions}

class QueryPlanSerdeSuite extends AnyFunSuite {

  test("make_date IN evaluates the probe but can skip list expressions") {
    val year = AttributeReference("y", IntegerType)()
    val month = AttributeReference("m", IntegerType)()
    val date = MakeDate(year, month, Literal(1), failOnError = true)
    val inputs = Seq(year, month)
    val epoch = Literal(0, DateType)
    val probe = In(date, Seq(epoch, Literal(31, DateType)))
    assert(QueryPlanSerde.exprToProto(probe, inputs).exists(_.hasIn))

    // Spark must skip the unevaluable column references in the list in both cases.
    val nullProbe = In(Literal.create(null, DateType), Seq(date))
    val firstMatch = In(epoch, Seq(epoch, date))
    assert(nullProbe.eval() == null)
    assert(firstMatch.eval() == true)
    Seq(nullProbe, firstMatch).foreach { predicate =>
      assert(QueryPlanSerde.exprToProto(predicate, inputs).isEmpty)
    }
  }

  test("make_date IN accepts analyzer casts that only widen candidate nullability") {
    val year = AttributeReference("y", IntegerType)()
    val text = AttributeReference("s", StringType)()
    val inputs = Seq(year, text)
    val date = MakeDate(year, Literal(1), Literal(1), failOnError = true)
    def struct(value: Expression): Expression =
      CreateNamedStruct(Seq(Literal("d"), value, Literal("f"), Literal(0.0d)))
    def analyze(candidate: Expression): In = {
      val predicate = In(struct(date), Seq(struct(candidate)))
      val plan = Project(Seq(Alias(predicate, "matches")()), LocalRelation(inputs))
      SimpleAnalyzer
        .execute(plan)
        .asInstanceOf[Project]
        .projectList
        .head
        .children
        .head
        .asInstanceOf[In]
    }

    val safe = analyze(Literal(java.sql.Date.valueOf("2024-01-01")))
    // The analyzer coerces the literal's nonnullable date field to match the nullable probe.
    val cast = safe.list.head.asInstanceOf[Cast]
    assert(cast.child.dataType != cast.dataType)
    assert(equalsIgnoreCompatibleNullability(cast.child.dataType, cast.dataType))
    assert(QueryPlanSerde.exprToProto(safe, inputs).exists(_.hasIn))

    // Value-changing casts remain unsafe inside otherwise safe struct constructors.
    val unsafe = analyze(Cast(text, DateType, Some("UTC"), ansiEnabled = true))
    assert(QueryPlanSerde.exprToProto(unsafe, inputs).isEmpty)
  }

  test("make_date IN falls back when a candidate can fail on a skipped row") {
    val year = AttributeReference("y", IntegerType)()
    val text = AttributeReference("s", StringType)()
    val inputs = Seq(year, text)
    val date = MakeDate(year, Literal(1), Literal(1), failOnError = true)
    val first = Literal(java.sql.Date.valueOf("2024-01-01"))
    val cast = Cast(text, DateType, Some("UTC"), ansiEnabled = true)
    def struct(value: Expression): Expression =
      CreateNamedStruct(Seq(Literal("d"), value, Literal("f"), Literal(0.0d)))
    val scalar = In(date, Seq(first, cast))
    val nested = In(struct(date), Seq(struct(first), struct(cast)))
    val rows = Seq(
      InternalRow(2024, UTF8String.fromString("bad")),
      InternalRow(2025, UTF8String.fromString("2025-01-01")))

    Seq(scalar, nested).foreach { in =>
      Seq(in -> true, Not(in) -> false).foreach { case (predicate, expected) =>
        val bound = BindReferences.bindReference(predicate, AttributeSeq(inputs))
        val projection = UnsafeProjection.create(Seq(bound))
        rows.foreach { row =>
          assert(bound.eval(row) == expected)
          assert(projection(row).getBoolean(0) == expected)
        }
        val error = intercept[Exception] {
          bound.eval(InternalRow(2026, UTF8String.fromString("bad")))
        }
        assert(error.getMessage.contains("CAST_INVALID_INPUT"))
        assert(QueryPlanSerde.exprToProto(predicate, inputs).isEmpty)
      }
    }

    // A NULL scalar probe skips even the first candidate.
    val nullProbe = In(date, Seq(cast))
    val bound = BindReferences.bindReference(nullProbe, AttributeSeq(inputs))
    assert(bound.eval(InternalRow(null, UTF8String.fromString("bad"))) == null)
    assert(QueryPlanSerde.exprToProto(nullProbe, inputs).isEmpty)

    // Literal/column candidates, including struct constructors, remain native.
    val candidate = AttributeReference("d", DateType)()
    Seq(In(date, Seq(first, candidate)), In(struct(date), Seq(struct(first), struct(candidate))))
      .foreach { predicate =>
        assert(QueryPlanSerde.exprToProto(predicate, inputs :+ candidate).exists(_.hasIn))
      }
  }

  test("supportedDataType matches each caller boundary") {
    val complex = ArrayType(IntegerType)
    val nestedInterval = StructType(
      Seq(
        StructField(
          "i",
          ArrayType(
            MapType(StringType, DayTimeIntervalType(), valueContainsNull = true),
            containsNull = true))))
    val nestedCalendarInterval =
      StructType(Seq(StructField("i", ArrayType(CalendarIntervalType, containsNull = true))))
    val duplicateFields =
      ArrayType(StructType(Seq(StructField("i", IntegerType), StructField("i", IntegerType))))
    val emptyStruct = StructType(Nil)
    val timeTypes = if (isSpark41Plus) Seq(DataType.fromDDL("TIME")) else Seq.empty
    val collatedStrings =
      if (isSpark40Plus) Seq(DataType.fromDDL("STRING COLLATE UTF8_LCASE")) else Seq.empty

    val boundaries: Seq[(String, DataType => Boolean, Seq[DataType], Seq[DataType])] = Seq(
      (
        "expression serde defaults",
        supportedDataType(_),
        Seq(IntegerType, StringType, CalendarIntervalType) ++ timeTypes ++ collatedStrings,
        Seq(complex, YearMonthIntervalType(), DayTimeIntervalType(), emptyStruct)),
      (
        "CometSink",
        supportedDataType(_, allowComplex = true, allowIntervals = true),
        Seq(complex, nestedInterval, nestedCalendarInterval, duplicateFields) ++
          timeTypes ++ collatedStrings,
        Seq(emptyStruct)),
      (
        "CometLocalTableScanExec",
        supportedDataType(
          _,
          allowComplex = true,
          allowIntervals = true,
          allowAnyStringType = false),
        Seq(
          IntegerType,
          StringType,
          complex,
          nestedInterval,
          nestedCalendarInterval,
          duplicateFields) ++ timeTypes,
        Seq(emptyStruct) ++ collatedStrings),
      (
        "native shuffle",
        supportedDataType(_, allowComplex = true, allowIntervals = true),
        Seq(complex, nestedInterval, nestedCalendarInterval, duplicateFields) ++
          timeTypes ++ collatedStrings,
        Seq(emptyStruct)),
      (
        "JVM columnar shuffle",
        supportedDataType(
          _,
          allowComplex = true,
          allowCalendarInterval = false,
          allowDuplicateStructFieldNames = false),
        Seq(IntegerType, StringType, complex) ++ timeTypes ++ collatedStrings,
        Seq(
          YearMonthIntervalType(),
          DayTimeIntervalType(),
          CalendarIntervalType,
          nestedCalendarInterval,
          duplicateFields,
          emptyStruct)))

    boundaries.foreach { case (name, supports, accepted, rejected) =>
      accepted.foreach(dt => assert(supports(dt), s"$name should accept $dt"))
      rejected.foreach(dt => assert(!supports(dt), s"$name should reject $dt"))
    }
  }

  test("canShortCircuitNulls skips an argument only where the shared subexpressions are known") {
    val array = AttributeReference("ai", ArrayType(IntegerType))()
    val value = AttributeReference("i", IntegerType)()
    // element_at with the index 0 raises in every mode, so it is a shared subexpression to keep
    val risky = ElementAt(AttributeReference("other", ArrayType(IntegerType))(), Literal(0))
    val contains = ArrayContains(array, risky)
    val position = ArrayPosition(array, risky)

    // A column is never shared, so it can be skipped anywhere
    assert(canShortCircuitNulls(ArrayContains(array, value)))
    // Something serialized outside an operator's conversion might share the element_at
    assert(!canShortCircuitNulls(contains))
    withSharedSubexpressions(Seq(contains), certain = true) {
      assert(canShortCircuitNulls(contains))
    }
    withSharedSubexpressions(Seq(contains, position), certain = true) {
      assert(!canShortCircuitNulls(contains))
      assert(!canShortCircuitNulls(position))
    }
  }

  test("an uncertain scope sends a function that skips a shared subexpression to Spark") {
    val array = AttributeReference("ai", ArrayType(IntegerType))()
    val cast = Cast(AttributeReference("s", StringType)(), IntegerType, None, ansiEnabled = true)
    val shared = Seq(ArrayContains(array, cast), ArrayPosition(array, cast))
    val proto =
      scalarFunctionExprToProto("array_contains", Some(ExprOuterClass.Expr.newBuilder().build()))
    // The call's NULL short-circuit flag, or the reasons it falls back to Spark
    def convert(scope: Option[(Seq[Expression], Boolean)]): Either[Set[String], Boolean] = {
      val call = ArrayContains(array, cast)
      def run = withNullShortCircuit(call, proto).map(_.getScalarFunc.getNullShortCircuit)
      val result = scope.fold(run) { case (exprs, certain) =>
        withSharedSubexpressions(exprs, certain = certain)(run)
      }
      result.toRight(call.getTagValue(CometExplainInfo.FALLBACK_REASONS).getOrElse(Set.empty))
    }

    assert(convert(Some((shared, false))) == Left(Set(uncertainSharedSubexpressionReason)))
    // Nothing shared, so the call still skips the cast
    assert(convert(Some((shared.take(1), false))) == Right(true))
    // Where Spark shares the cast for every row, so does native execution
    assert(convert(Some((shared, true))) == Right(false))
    assert(convert(None) == Right(false))
  }

  test("canShortCircuitNulls counts only shared subexpressions that can fail") {
    val array = AttributeReference("ai", ArrayType(IntegerType))()
    val value = AttributeReference("i", IntegerType)()
    val isZero = EqualTo(value, Literal(0))
    val failing = Cast(Cast(value, StringType), IntegerType, None, ansiEnabled = true)
    // The shared `i = 0` cannot fail, so skipping it on a NULL array changes nothing
    val safeKey = If(isZero, failing, Literal(1))
    val safeContains = ArrayContains(If(isZero, Literal(null, array.dataType), array), safeKey)
    withSharedSubexpressions(Seq(safeContains), certain = true) {
      assert(canShortCircuitNulls(safeContains))
    }
    // A shared ANSI cast can fail, so it must be evaluated for every row
    val contains = ArrayContains(array, failing)
    val position = ArrayPosition(array, failing)
    withSharedSubexpressions(Seq(contains, position), certain = true) {
      assert(!canShortCircuitNulls(contains))
    }
    // Outside an operator's conversion an argument that cannot fail can still be skipped
    assert(canShortCircuitNulls(ArrayContains(array, Not(isZero))))
    assert(!canShortCircuitNulls(contains))
    // A non-nullable first argument never makes Spark skip the others
    val literalArray = Literal.create(Seq(1), ArrayType(IntegerType))
    assert(canShortCircuitNulls(ArrayContains(literalArray, failing)))
    // After a non-nullable array and a nullable start, Spark skips only the length
    val sliceStart = Slice(literalArray, failing, value)
    val sliceLength = Slice(literalArray, value, failing)
    withSharedSubexpressions(Seq(sliceStart, sliceLength), certain = true) {
      assert(canShortCircuitNulls(sliceStart))
      assert(!canShortCircuitNulls(sliceLength))
    }
  }

  test("cannotRaise follows eval mode and the allowlist") {
    val array = AttributeReference("ai", ArrayType(IntegerType))()
    val text = AttributeReference("s", StringType)()
    val value = AttributeReference("i", IntegerType)()
    val decimal = AttributeReference("d", DecimalType(10, 2))()
    for (ansi <- Seq(false, true)) {
      // Outside ANSI mode a cast between numbers and strings returns NULL instead of failing
      assert(canSkipEvaluation(Seq(Cast(text, IntegerType, None, ansiEnabled = ansi))) == !ansi)
      assert(canSkipEvaluation(Seq(UnaryMinus(value, failOnError = ansi))) == !ansi)
      // A widening cast, such as the one comparing a decimal with an integer column adds, never
      // fails
      val widened = Cast(decimal, DecimalType(12, 2), None, ansiEnabled = ansi)
      assert(canSkipEvaluation(Seq(EqualTo(widened, Literal(Decimal(0), DecimalType(12, 2))))))
    }
    // String functions that cannot fail, but not a concat, which can exceed the size limit
    assert(canSkipEvaluation(Seq(Lower(Substring(text, Literal(1), Literal(2))))))
    assert(!canSkipEvaluation(Seq(Concat(Seq(text, text)))))
    // Outside ANSI mode a cast to an ANSI interval can still fail
    val interval = Cast(text, DayTimeIntervalType(), None, ansiEnabled = false)
    assert(!canSkipEvaluation(Seq(interval)))
    assert(!canShortCircuitNulls(ArrayContains(array, Cast(interval, IntegerType))))
    // Widening a date to a timestamp overflows on extreme dates in any mode
    val date = AttributeReference("dt", DateType)()
    val dates = AttributeReference("dts", ArrayType(DateType))()
    for (ansi <- Seq(false, true); target <- Seq(TimestampType, TimestampNTZType)) {
      assert(!canSkipEvaluation(Seq(Cast(date, target, Some("UTC"), ansiEnabled = ansi))))
      val widened = Cast(dates, ArrayType(target), Some("UTC"), ansiEnabled = ansi)
      assert(!canSkipEvaluation(Seq(widened)))
    }
    // A legacy decimal sum returns NULL on overflow, also once promotion wraps it
    val left = AttributeReference("a", DecimalType(38, 0))()
    val legacySum = DecimalPrecision.promote(Add(left, decimal, EvalMode.LEGACY))
    assert(canSkipEvaluation(Seq(legacySum)))
    assert(!canSkipEvaluation(Seq(DecimalPrecision.promote(Add(left, decimal, EvalMode.ANSI)))))
    // A literal cast fails only if evaluating it fails
    assert(canSkipEvaluation(Seq(Cast(Literal(0), DecimalType(1, 0), None, ansiEnabled = true))))
    assert(!canSkipEvaluation(Seq(Cast(Literal("x"), IntegerType, None, ansiEnabled = true))))
  }

  test("cannotRaise lists each node it clears and keeps each guard") {
    val text = AttributeReference("s", StringType)()
    val value = AttributeReference("i", IntegerType)()
    val long = AttributeReference("l", LongType)()
    val decimal = AttributeReference("d", DecimalType(10, 2))()
    val date = AttributeReference("dt", DateType)()
    val ts = AttributeReference("ts", TimestampType)()
    val interval = AttributeReference("di", DayTimeIntervalType())()
    val nulls = AttributeReference("n", NullType)()
    val wide = AttributeReference("w", DecimalType(38, 0))()
    val fraction = AttributeReference("f", DecimalType(18, 18))()
    def ansiCast(child: Expression, to: DataType): Cast =
      Cast(child, to, Some("UTC"), EvalMode.ANSI)
    def legacyCast(child: Expression, to: DataType): Cast =
      Cast(child, to, Some("UTC"), EvalMode.LEGACY)
    val cannotRaise = Seq(
      CreateArray(Seq(value, value)),
      CreateNamedStruct(Seq(Literal("f"), value)),
      new Murmur3Hash(Seq(value)),
      new XxHash64(Seq(value)),
      StringTrim(text),
      StringTrimLeft(text),
      StringTrimRight(text),
      Year(date),
      Month(date),
      DayOfMonth(date),
      Pmod(value, value, EvalMode.LEGACY),
      IntegralDivide(long, long, EvalMode.LEGACY),
      Remainder(value, value, EvalMode.LEGACY),
      Abs(value, failOnError = false),
      UnaryMinus(value, failOnError = false),
      CheckOverflow(decimal, DecimalType(10, 2), nullOnOverflow = true),
      ansiCast(
        AttributeReference("mi", MapType(IntegerType, IntegerType))(),
        MapType(LongType, LongType)),
      ansiCast(ts, LongType),
      ansiCast(date, StringType),
      ansiCast(nulls, DateType),
      ansiCast(nulls, BooleanType),
      ansiCast(nulls, BinaryType),
      ansiCast(
        AttributeReference("dd", DayTimeIntervalType(DayTimeIntervalType.DAY))(),
        DayTimeIntervalType()))
    val canRaise = Seq(
      Pmod(value, value, EvalMode.ANSI),
      Pmod(value, value, EvalMode.TRY),
      IntegralDivide(long, long, EvalMode.TRY),
      Remainder(value, value, EvalMode.TRY),
      // The integral part of the quotient exceeds the 39 digits Spark's `Decimal` divides to
      Remainder(wide, fraction, EvalMode.LEGACY),
      Pmod(wide, fraction, EvalMode.LEGACY),
      IntegralDivide(wide, fraction, EvalMode.LEGACY),
      Abs(value, failOnError = true),
      UnaryMinus(interval, failOnError = false),
      Abs(interval, failOnError = false),
      CheckOverflow(decimal, DecimalType(10, 2), nullOnOverflow = false),
      ansiCast(
        AttributeReference("md", MapType(IntegerType, DateType))(),
        MapType(IntegerType, TimestampType)),
      ansiCast(
        AttributeReference("dm", MapType(DateType, IntegerType))(),
        MapType(TimestampType, IntegerType)),
      legacyCast(
        AttributeReference("sd", StructType(Seq(StructField("f", DateType))))(),
        StructType(Seq(StructField("f", TimestampType)))),
      ansiCast(long, IntegerType),
      legacyCast(text, VarcharType(1)),
      ansiCast(nulls, VarcharType(1)))
    assert(cannotRaise.filterNot(e => canSkipEvaluation(Seq(e))).isEmpty)
    assert(canRaise.filter(e => canSkipEvaluation(Seq(e))).isEmpty)
  }

  test("canSkipEvaluation and the dispatcher agree on which shared subexpressions can raise") {
    val array = AttributeReference("ai", ArrayType(IntegerType))()
    val text = AttributeReference("s", StringType)()
    val decimal = AttributeReference("d", DecimalType(10, 2))()
    val failing = Cast(text, IntegerType, None, ansiEnabled = true)
    // Whether the key of a lookup can be skipped, and whether the dispatcher declines the lookup,
    // when the key holds `shared`, which the operator also evaluates on its own
    def skipped(shared: Expression): (Boolean, Boolean) = {
      val key = If(shared, failing, Literal(1))
      val contains = ArrayContains(array, key)
      withSharedSubexpressions(Seq(contains, shared), certain = true) {
        (canSkipEvaluation(Seq(key)), skipsSharedSubexpression(contains))
      }
    }
    val prefix = StartsWith(text, Literal("a"))
    val widened = Cast(decimal, DecimalType(12, 2), None, ansiEnabled = true)
    val legacySum = Add(AttributeReference("a", DecimalType(38, 0))(), decimal, EvalMode.LEGACY)
    val subexpressions =
      Seq(prefix, EqualTo(widened, Literal(Decimal(0), DecimalType(12, 2))), IsNull(legacySum))
    assert(subexpressions.map(skipped) == Seq.fill(subexpressions.length)((true, false)))
  }

  test("an uncertain scope still skips a shared subexpression that cannot raise") {
    val array = AttributeReference("ai", ArrayType(IntegerType))()
    val length = Length(AttributeReference("s", StringType)())
    val contains = ArrayContains(array, length)
    val position = ArrayPosition(array, length)
    val proto =
      scalarFunctionExprToProto("array_contains", Some(ExprOuterClass.Expr.newBuilder().build()))
    def shortCircuit: Option[Boolean] =
      withNullShortCircuit(contains, proto).map(_.getScalarFunc.getNullShortCircuit)

    withSharedSubexpressions(Seq(contains, position), certain = false) {
      assert(canShortCircuitNulls(contains))
      assert(shortCircuit.contains(true))
    }
  }

  test("canEvaluateSkippedEagerly needs every failing part of a skipped argument shared") {
    val array = AttributeReference("ai", ArrayType(IntegerType))()
    val other = AttributeReference("aj", ArrayType(IntegerType))()
    val text = AttributeReference("s", StringType)()
    val shared = Cast(text, IntegerType, None, ansiEnabled = true)
    val unshared =
      Cast(AttributeReference("t", StringType)(), IntegerType, None, ansiEnabled = true)
    val covered = Seq(ArrayContains(array, shared), ArrayContains(other, shared))
    val mixed = ArrayContains(array, Add(shared, unshared, EvalMode.LEGACY))
    withSharedSubexpressions(covered, certain = true) {
      assert(!canShortCircuitNulls(covered.head))
      assert(canEvaluateSkippedEagerly(covered.head))
    }
    // Where Spark may not share them, evaluating them for every row is not sure to match
    withSharedSubexpressions(covered, certain = false) {
      assert(!canEvaluateSkippedEagerly(covered.head))
    }
    withSharedSubexpressions(Seq(mixed, ArrayPosition(other, shared)), certain = true) {
      assert(!canShortCircuitNulls(mixed))
      assert(!canEvaluateSkippedEagerly(mixed))
    }
    // Outside an operator's conversion native execution keeps evaluating them for every row
    assert(canEvaluateSkippedEagerly(mixed))
    // A wrapper that cannot fail around the shared cast holds nothing else that can
    val wrapped = ArrayContains(array, Add(shared, Literal(1), EvalMode.LEGACY))
    val ansiWrapped = ArrayContains(array, Add(shared, Literal(1), EvalMode.ANSI))
    withSharedSubexpressions(Seq(wrapped, ansiWrapped, covered.last), certain = true) {
      assert(!canShortCircuitNulls(wrapped))
      assert(canEvaluateSkippedEagerly(wrapped))
      assert(!canShortCircuitNulls(ansiWrapped))
      assert(!canEvaluateSkippedEagerly(ansiWrapped))
    }
  }

  test("element_at keeps its NULL guard outside an operator's scope") {
    val m1 = AttributeReference("m1", MapType(IntegerType, IntegerType))()
    val m2 = AttributeReference("m2", MapType(IntegerType, IntegerType))()
    val text = AttributeReference("s", StringType)()
    val key = Cast(text, IntegerType, None, ansiEnabled = true)
    val lookup = ElementAt(m1, key, None, failOnError = true)
    val inputs = Seq(m1, m2, text)
    def guarded: Option[Boolean] = QueryPlanSerde.exprToProto(lookup, inputs).map(_.hasCaseWhen)
    assert(guarded.contains(true))
    // Where Spark shares the key for every row, the lookup evaluates it for every row too
    val shared = Seq(lookup, ElementAt(m2, key, None, failOnError = true))
    withSharedSubexpressions(shared, certain = true) {
      assert(guarded.contains(false))
    }
  }

  test("map_from_arrays keeps its NULL guards outside an operator's scope") {
    val k1 = AttributeReference("k1", ArrayType(IntegerType))()
    val k2 = AttributeReference("k2", ArrayType(IntegerType))()
    val text = AttributeReference("s", StringType)()
    val values = CreateArray(Seq(Cast(text, IntegerType, None, ansiEnabled = true)))
    val call = MapFromArrays(k1, values)
    def guarded: Option[Boolean] =
      QueryPlanSerde.exprToProto(call, Seq(k1, k2, text)).map(_.hasCaseWhen)
    assert(guarded.contains(true))
    // Where Spark shares the values for every row, the call cannot skip them, so it falls back
    withSharedSubexpressions(Seq(call, MapFromArrays(k2, values)), certain = true) {
      assert(guarded.isEmpty)
    }
  }

  test("canShortCircuitNulls follows subexpression elimination except in aggregates") {
    val risky = ElementAt(AttributeReference("other", ArrayType(IntegerType))(), Literal(0))
    val array = AttributeReference("ai", ArrayType(IntegerType))()
    val contains = ArrayContains(array, risky)
    val shared = Seq(contains, ArrayPosition(array, risky))

    def withSubexpressionElimination(enabled: Boolean)(f: => Unit): Unit = {
      val conf = new SQLConf
      conf.setConf(SQLConf.SUBEXPRESSION_ELIMINATION_ENABLED, enabled)
      SQLConf.withExistingConf(conf) {
        assert(SQLConf.get.subexpressionEliminationEnabled == enabled)
        f
      }
    }

    withSubexpressionElimination(enabled = false) {
      withSharedSubexpressions(shared, certain = true) {
        assert(canShortCircuitNulls(contains))
      }
      withSharedSubexpressions(shared, alwaysShared = true, certain = true) {
        assert(!canShortCircuitNulls(contains))
      }
      // Outside an operator's conversion what they share is still unknown
      assert(!canShortCircuitNulls(contains))
    }
  }

  test("skipsSharedSubexpression exempts a repeat only where Spark eliminates it") {
    val map = AttributeReference("m", MapType(DoubleType, IntegerType))()
    val text = AttributeReference("s", StringType)()
    val cast = Cast(text, DoubleType, Some("UTC"), EvalMode.ANSI)
    val lookup = ElementAt(map, Add(cast, cast))
    // Spark's elimination does not look into a branch, so it does not share the repeat
    val branch = If(EqualTo(text, Literal("abc")), lookup, Literal(0))

    for (enabled <- Seq(true, false)) {
      val conf = new SQLConf
      conf.setConf(SQLConf.SUBEXPRESSION_ELIMINATION_ENABLED, enabled)
      SQLConf.withExistingConf(conf) {
        withSharedSubexpressions(Seq(lookup), certain = true) {
          assert(!skipsSharedSubexpression(lookup))
        }
        withSharedSubexpressions(Seq(lookup), certain = false) {
          assert(skipsSharedSubexpression(lookup) == enabled)
        }
        for (certain <- Seq(true, false)) {
          withSharedSubexpressions(Seq.empty, certain = certain) {
            assert(skipsSharedSubexpression(lookup) == enabled)
          }
        }
        withSharedSubexpressions(Seq(branch), certain = true) {
          assert(skipsSharedSubexpression(lookup) == enabled)
        }
        withSharedSubexpressions(Seq(lookup), alwaysShared = true, certain = false) {
          assert(skipsSharedSubexpression(lookup))
        }
      }
    }
  }

  test("skipsSharedSubexpression ignores a repeat that cannot raise") {
    val map = AttributeReference("m", MapType(IntegerType, IntegerType))()
    val text = AttributeReference("s", StringType)()
    val struct = AttributeReference("st", StructType(Seq(StructField("f", IntegerType))))()
    val failing = Cast(text, IntegerType, None, ansiEnabled = true)
    // Spark shares the repeat in the first scope, nothing in the second, and does not eliminate
    // out of the branch in the third
    def scopes(expr: Expression): Seq[Seq[Expression]] =
      Seq(Seq(expr), Seq.empty, Seq(If(EqualTo(text, Literal("abc")), expr, Literal(0))))

    val conf = new SQLConf
    conf.setConf(SQLConf.SUBEXPRESSION_ELIMINATION_ENABLED, true)
    SQLConf.withExistingConf(conf) {
      for {
        (repeat, raises) <- Seq(
          Length(text) -> false,
          GetStructField(struct, 0) -> false,
          failing -> true)
        certain <- Seq(true, false)
      } {
        val lookup = ElementAt(map, Add(repeat, repeat, EvalMode.LEGACY))
        for ((scope, i) <- scopes(lookup).zipWithIndex) {
          withSharedSubexpressions(scope, certain = certain) {
            assert(skipsSharedSubexpression(lookup) == (raises && !(certain && i == 0)))
          }
        }
      }
    }
  }

  test("canSkipEvaluation ignores shared subexpressions that cannot raise") {
    val array = AttributeReference("ai", ArrayType(IntegerType))()
    val text = AttributeReference("s", StringType)()
    val struct = AttributeReference("st", StructType(Seq(StructField("f", IntegerType))))()
    val months = AttributeReference("m", YearMonthIntervalType())()
    // element_at with the index 0 raises in every mode
    val risky = ElementAt(AttributeReference("other", ArrayType(IntegerType))(), Literal(0))

    // Whether `e` can be skipped when the operator shares it between two expressions
    def skippable(e: Expression): Boolean =
      withSharedSubexpressions(Seq(e, e), alwaysShared = true, certain = true)(
        canSkipEvaluation(Seq(e)))

    assert(skippable(Length(text)))
    // A field of a struct is NULL for a NULL struct and never fails
    assert(skippable(GetStructField(struct, 0)))
    assert(!skippable(risky))
    // Interval arithmetic raises on overflow in every mode, unlike numeric arithmetic
    assert(!skippable(Add(months, months)))
    // An expression that cannot raise but holds one that does is kept as well
    assert(!skippable(Length(Cast(risky, StringType))))
    // Outside ANSI a cast from a string to an int is NULL on bad input, but a cast from a date to
    // a timestamp overflows for a date past about the year 294247 in every eval mode
    val date = AttributeReference("d", DateType)()
    def legacyCast(child: Expression, to: DataType): Cast =
      Cast(child, to, Some("UTC"), EvalMode.LEGACY)
    assert(skippable(legacyCast(text, IntegerType)))
    assert(!skippable(legacyCast(date, TimestampType)))
    assert(!skippable(legacyCast(date, TimestampNTZType)))
    assert(
      !skippable(
        legacyCast(AttributeReference("ad", ArrayType(DateType))(), ArrayType(TimestampType))))

    // A shared subexpression that raises does not keep a consumer of another one from being skipped
    val safe = Length(text)
    val riskyContains = ArrayContains(array, risky)
    val safeContains = ArrayContains(array, safe)
    withSharedSubexpressions(
      Seq(riskyContains, ArrayPosition(array, risky), safeContains, ArrayPosition(array, safe)),
      alwaysShared = true,
      certain = true) {
      assert(!canShortCircuitNulls(riskyContains))
      assert(canShortCircuitNulls(safeContains))
    }
  }

  test("canSkipEvaluation recognizes shared subexpressions across decimal promotion") {
    val left = AttributeReference("a", DecimalType(10, 0))()
    val right = AttributeReference("b", DecimalType(10, 0))()
    val cast = Cast(Divide(left, right), IntegerType, None, ansiEnabled = true)
    val array = AttributeReference("ai", ArrayType(IntegerType))()
    val contains = ArrayContains(array, cast)
    val position = ArrayPosition(array, cast)
    val promoted = DecimalPrecision.promote(cast)
    assert(!cast.semanticEquals(promoted))

    for (enabled <- Seq(false, true); alwaysShared <- Seq(false, true)) {
      val conf = new SQLConf
      conf.setConf(SQLConf.SUBEXPRESSION_ELIMINATION_ENABLED, enabled)
      SQLConf.withExistingConf(conf) {
        withSharedSubexpressions(Seq(contains, position), alwaysShared, certain = true) {
          val canSkip = !(enabled || alwaysShared)
          assert(canSkipEvaluation(Seq(cast)) == canSkip)
          assert(canSkipEvaluation(Seq(promoted)) == canSkip)
          assert(canShortCircuitNulls(contains) == canSkip)
          assert(canShortCircuitNulls(DecimalPrecision.promote(contains)) == canSkip)
        }
        withSharedSubexpressions(Seq(contains), alwaysShared, certain = true) {
          assert(canSkipEvaluation(Seq(cast)))
          assert(canSkipEvaluation(Seq(promoted)))
        }
      }
    }
  }
}
