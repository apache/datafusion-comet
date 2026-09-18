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

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.catalyst.expressions.{Alias, Literal, NamedExpression}
import org.apache.spark.sql.execution.{ProjectExec, ScalarSubquery, SubqueryExec}
import org.apache.spark.sql.types._

import org.apache.comet.CometSparkSessionExtensions.{isSpark40Plus, isSpark41Plus}
import org.apache.comet.serde.QueryPlanSerde.supportedDataType
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

  // Freeze the pre-refactor shared predicate: existing callers must retain their accepted types.
  private def legacySupported(dt: DataType, allowComplex: Boolean): Boolean = dt match {
    case _: ByteType | _: ShortType | _: IntegerType | _: LongType | _: FloatType |
        _: DoubleType | _: StringType | _: BinaryType | _: TimestampType | _: TimestampNTZType |
        _: DecimalType | _: DateType | _: BooleanType | _: NullType | CalendarIntervalType =>
      true
    case dt if isTimeType(dt) => true
    case s: StructType if allowComplex =>
      s.nonEmpty && s.fields.forall(f => legacySupported(f.dataType, allowComplex))
    case a: ArrayType if allowComplex => legacySupported(a.elementType, allowComplex)
    case m: MapType if allowComplex =>
      legacySupported(m.keyType, allowComplex) && legacySupported(m.valueType, allowComplex)
    case _ => false
  }

  private def nestedTypes(dt: DataType): Seq[DataType] = Seq(
    dt,
    struct(dt),
    struct(struct(dt)),
    ArrayType(dt),
    MapType(dt, IntegerType),
    MapType(IntegerType, dt),
    struct(ArrayType(MapType(StringType, dt))))

  test("shared type gate preserves existing defaults and non-struct scalar subqueries") {
    val types = scalarTypes ++ versionSpecificTypes ++ Seq(
      CalendarIntervalType,
      YearMonthIntervalType(),
      DayTimeIntervalType(),
      CharType(5),
      VarcharType(5),
      StructType(Nil),
      StructType(Seq(StructField("same", IntegerType), StructField("same", LongType))))
    types.flatMap(nestedTypes).foreach { dt =>
      Seq(false, true).foreach { allowComplex =>
        withClue(s"$dt, allowComplex=$allowComplex: ") {
          assert(supportedDataType(dt, allowComplex) == legacySupported(dt, allowComplex))
        }
      }
      if (!dt.isInstanceOf[StructType]) {
        assert(supported(dt) == legacySupported(dt, allowComplex = false), dt)
      }
    }
  }

  test("shared capability flags apply recursively to structs arrays and map keys and values") {
    val duplicate =
      StructType(Seq(StructField("same", IntegerType), StructField("same", LongType)))
    val cases: Seq[(DataType, DataType => Boolean, DataType => Boolean)] = Seq(
      (
        CalendarIntervalType,
        (t: DataType) => supportedDataType(t, allowComplex = true),
        (t: DataType) =>
          supportedDataType(t, allowComplex = true, allowCalendarInterval = false)),
      (
        YearMonthIntervalType(),
        (t: DataType) => supportedDataType(t, allowComplex = true, allowIntervals = true),
        (t: DataType) => supportedDataType(t, allowComplex = true)),
      (
        DayTimeIntervalType(),
        (t: DataType) => supportedDataType(t, allowComplex = true, allowIntervals = true),
        (t: DataType) => supportedDataType(t, allowComplex = true)),
      (
        duplicate,
        (t: DataType) => supportedDataType(t, allowComplex = true),
        (t: DataType) =>
          supportedDataType(t, allowComplex = true, allowDuplicateStructFieldNames = false))) ++
      versionSpecificTypes
        .filter(isTimeType)
        .map(dt =>
          (
            dt,
            (t: DataType) => supportedDataType(t, allowComplex = true),
            (t: DataType) => supportedDataType(t, allowComplex = true, allowTimeType = false))) ++
      versionSpecificTypes.collect { case dt: StringType =>
        (
          dt,
          (t: DataType) => supportedDataType(t, allowComplex = true),
          (t: DataType) => supportedDataType(t, allowComplex = true, allowAnyStringType = false))
      }
    cases.foreach { case (dt, accepts, rejects) =>
      nestedTypes(dt).foreach { nested =>
        withClue(s"$nested: ") {
          assert(accepts(nested))
          assert(!rejects(nested))
        }
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
      assert(supportedDataType(struct(negative), allowComplex = true))
      assert(supported(negative))
      assert(!supported(struct(negative)))
      assert(!supported(struct(struct(negative))))
      Seq(DecimalType(1, 0), DecimalType(38, 0), DecimalType(38, 38)).foreach { dt =>
        assert(supported(struct(dt)), dt)
        assert(supported(struct(struct(dt))), dt)
      }
    }
  }
}
