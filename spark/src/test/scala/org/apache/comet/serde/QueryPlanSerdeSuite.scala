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
import org.apache.spark.sql.catalyst.expressions.{Add, Alias, ArrayContains, ArrayPosition, AttributeReference, AttributeSeq, BindReferences, Cast, Concat, CreateArray, CreateNamedStruct, Divide, ElementAt, EqualTo, EvalMode, Expression, If, In, Literal, Lower, MakeDate, MapFromArrays, Not, Slice, Substring, UnaryMinus, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Project}
import org.apache.spark.sql.comet.DecimalPrecision
import org.apache.spark.sql.comet.util.Utils.equalsIgnoreCompatibleNullability
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import org.apache.comet.CometExplainInfo
import org.apache.comet.CometSparkSessionExtensions.{isSpark40Plus, isSpark41Plus}
import org.apache.comet.serde.QueryPlanSerde.{canEvaluateSkippedEagerly, canShortCircuitNulls, canSkipEvaluation, scalarFunctionExprToProto, supportedDataType, uncertainSharedSubexpressionReason, withNullShortCircuit, withSharedSubexpressions}

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
    val cast = Cast(AttributeReference("s", StringType)(), IntegerType, None, ansiEnabled = true)
    val contains = ArrayContains(array, cast)
    val position = ArrayPosition(array, cast)

    // A column is never shared, so it can be skipped anywhere
    assert(canShortCircuitNulls(ArrayContains(array, value)))
    // Something serialized outside an operator's conversion might share the cast
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

  test("canFail follows eval mode and the allowlist") {
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
    val cast = Cast(AttributeReference("s", StringType)(), IntegerType, None, ansiEnabled = true)
    val array = AttributeReference("ai", ArrayType(IntegerType))()
    val contains = ArrayContains(array, cast)
    val shared = Seq(contains, ArrayPosition(array, cast))

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
