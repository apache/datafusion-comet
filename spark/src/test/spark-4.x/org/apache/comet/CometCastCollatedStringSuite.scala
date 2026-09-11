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

package org.apache.comet

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes, IntegerType, MapType, StringType, StructField, StructType}

import org.apache.comet.expressions.{CometCast, CometEvalMode}
import org.apache.comet.serde.{Compatible, Unsupported}

/**
 * https://github.com/apache/datafusion-comet/issues/4489
 *
 * Spark 4.0 carries collation metadata on `StringType`, but `serializeDataType` maps every
 * `StringType` to one proto type id, so the collation is dropped on the way into the native plan
 * with no warning. Nothing used to stop that from happening. `CometCast.isSupported` matched
 * string casts through `case (DataTypes.StringType, _)` and `case (_, DataTypes.StringType)`, and
 * those only failed to match a collated `StringType` because `DataTypes.StringType` is the
 * default-collation singleton and Scala pattern equality compares the whole instance. The right
 * answer fell out of an accident of pattern matching. The `fromType == toType` shortcut let
 * identity casts on collated types through regardless, including nested ones such as
 * `ARRAY<STRING COLLATE UTF8_LCASE>`, which were byte-safe only because the cast was a no-op.
 *
 * `isSupported` now rejects any source or target type carrying a non-default collation before
 * either of those paths runs. This suite pins the resulting matrix down so the behaviour no
 * longer depends on pattern-match semantics staying the way they are.
 *
 * A note on what `Unsupported` means here. It does not mean the query falls back to Spark.
 * `CometCast` mixes in `CodegenDispatchFallback`, so `QueryPlanSerde.exprToProtoInternal` offers
 * the expression to the JVM codegen dispatcher first, which runs Spark's own `doGenCode` inside
 * the Comet pipeline. `spark.comet.exec.scalaUDF.codegen.enabled` defaults to true and
 * `CometBatchKernelCodegen.isSupportedDataType` admits every `StringType` regardless of
 * collation, so under default config a collated cast usually stays inside Comet. That route is
 * result-correct, because the kernel evaluates Spark's own generated code and the `collationId`
 * rides along on the expression. `Unsupported` means there is no native path. The end-to-end
 * tests at the bottom run both settings of that config on both the scalar and the struct case.
 *
 * These are Scala tests rather than `CometSqlFileTestSuite` fixtures because most of the file
 * asserts on `CometCast.isSupported` over type pairs no SQL can construct, such as
 * `ArrayType(NullType) -> ArrayType(STRING COLLATE UTF8_LCASE)`. The end-to-end tests stay here
 * with them because `--Config` is file scoped, so the query that has to assert a fallback reason
 * with the dispatcher off and the query that has to assert native execution with it on could not
 * share a fixture file.
 *
 * This lives under `spark-4.x`, shared by every 4.x profile, rather than `spark-4.1+`, because
 * collation is a Spark 4.0 feature and `StringType(collationName)` already resolves there.
 */
class CometCastCollatedStringSuite extends CometTestBase {

  private val lcase = StringType("UTF8_LCASE")
  private val unicode = StringType("UNICODE")

  private val evalModes = Seq(CometEvalMode.LEGACY, CometEvalMode.TRY, CometEvalMode.ANSI)

  private def structWith(dt: DataType): StructType = StructType(Seq(StructField("s", dt)))

  /** Asserts that Comet reports no native path for this cast under every eval mode. */
  private def assertNoNativePath(fromType: DataType, toType: DataType): Unit = {
    evalModes.foreach { evalMode =>
      CometCast.isSupported(fromType, toType, None, evalMode) match {
        case _: Unsupported => // expected
        case other =>
          fail(s"expected Unsupported for $fromType -> $toType under $evalMode, got $other")
      }
    }
  }

  /** Asserts that the collation guard leaves an uncollated cast alone. */
  private def assertCompatible(fromType: DataType, toType: DataType): Unit = {
    evalModes.foreach { evalMode =>
      CometCast.isSupported(fromType, toType, None, evalMode) match {
        case _: Compatible => // expected
        case other =>
          fail(s"expected Compatible for $fromType -> $toType under $evalMode, got $other")
      }
    }
  }

  // ---- scalar collated strings ----------------------------------------------------

  test("cast collated string to IntegerType has no native path") {
    assertNoNativePath(lcase, IntegerType)
  }

  test("cast IntegerType to collated string has no native path") {
    // This pair leaves through `canCastFromInt`'s catch-all rather than `canCastToString`,
    // because `case (_, DataTypes.StringType)` does not match a collated target. The guard now
    // answers ahead of both.
    assertNoNativePath(IntegerType, lcase)
  }

  test("cast collated string to default-collation StringType has no native path") {
    assertNoNativePath(lcase, DataTypes.StringType)
  }

  test("cast default-collation StringType to collated string has no native path") {
    assertNoNativePath(DataTypes.StringType, lcase)
  }

  test("cast between two different collations has no native path") {
    assertNoNativePath(lcase, unicode)
  }

  test("cast collated string to the same collation has no native path") {
    // The `fromType == toType` shortcut answered `Compatible()` here before the guard existed.
    // The cast is a byte-level no-op so results were right, but the plan reached the native side
    // with the collation stripped and nothing recording that. This is the implicit behaviour
    // #4489 names, so the guard sits above the shortcut.
    assertNoNativePath(lcase, lcase)
  }

  // ---- nested collated strings ----------------------------------------------------

  test("cast array of collated strings to another collation has no native path") {
    assertNoNativePath(ArrayType(lcase), ArrayType(unicode))
  }

  test("cast array of collated strings to the same collation has no native path") {
    // Same identity shortcut as the scalar case, one level down.
    assertNoNativePath(ArrayType(lcase), ArrayType(lcase))
  }

  test("cast array of collated strings to StringType has no native path") {
    // `case (dt: ArrayType, DataTypes.StringType)` recurses on the element type, so the reason
    // used to describe the element rather than the array. The guard answers for the whole type.
    assertNoNativePath(ArrayType(lcase), DataTypes.StringType)
  }

  test("cast struct with a collated field has no native path") {
    assertNoNativePath(structWith(lcase), structWith(unicode))
    assertNoNativePath(structWith(lcase), structWith(lcase))
  }

  test("cast map with a collated key has no native path") {
    assertNoNativePath(MapType(lcase, IntegerType), MapType(unicode, IntegerType))
    assertNoNativePath(MapType(lcase, IntegerType), MapType(lcase, IntegerType))
  }

  test("cast map with a collated value has no native path") {
    assertNoNativePath(MapType(IntegerType, lcase), MapType(IntegerType, unicode))
    assertNoNativePath(MapType(IntegerType, lcase), MapType(IntegerType, lcase))
  }

  test("cast struct whose collated field is unchanged while a sibling field is cast") {
    // The field zip used to answer per field, so the collated field hit the identity shortcut
    // and reported Compatible while the sibling carried the cast. That let a collated field ride
    // into the native plan on another field's back. The guard answers for the whole struct.
    val from = StructType(Seq(StructField("a", IntegerType), StructField("s", lcase)))
    val to = StructType(Seq(StructField("a", DataTypes.StringType), StructField("s", lcase)))
    assertNoNativePath(from, to)
  }

  test("cast map whose collated key is unchanged while the value type is cast") {
    assertNoNativePath(MapType(lcase, IntegerType), MapType(lcase, DataTypes.LongType))
  }

  test("cast array of nulls to array of collated strings has no native path") {
    // `case (dt: ArrayType, _: ArrayType) if dt.elementType == NullType` returns Compatible ahead
    // of every other branch, so this was one more way to reach the native side with a collation
    // attached to the target.
    assertNoNativePath(ArrayType(DataTypes.NullType), ArrayType(lcase))
  }

  // ---- the guard must not over-block ----------------------------------------------

  test("default-collation string casts are untouched by the collation guard") {
    assertCompatible(DataTypes.StringType, DataTypes.StringType)
    assertCompatible(DataTypes.StringType, IntegerType)
    assertCompatible(IntegerType, DataTypes.StringType)
  }

  test("nested default-collation string casts are untouched by the collation guard") {
    assertCompatible(ArrayType(DataTypes.StringType), ArrayType(DataTypes.StringType))
    assertCompatible(structWith(DataTypes.StringType), structWith(DataTypes.StringType))
    assertCompatible(
      MapType(DataTypes.StringType, IntegerType),
      MapType(DataTypes.StringType, IntegerType))
  }

  // ---- end to end -----------------------------------------------------------------
  //
  // The matrix above only exercises `isSupported`. These three run a query and pin down what the
  // planner actually does with the answer, which is what #4489 asked for. A plain-string Parquet
  // column with `COLLATE` applied on top reaches the cast as a collated child, the same shape
  // the datetime tests in `CometCollationSuite` rely on. The cast child has to be a column
  // rather than a literal, since `getSupportLevel` folds literal children before `isSupported`
  // is consulted.

  private def withCollatedTable(f: => Unit): Unit =
    withParquetTable(Seq(("123", 1), ("456", 2)), "collated_cast_tbl")(f)

  // Read from production rather than retyped, so the two cannot drift apart. The guard is the
  // only thing that produces this reason: without it `StringType(UTF8_LCASE) -> IntegerType`
  // exits through `isSupported`'s `case _` catch-all and reports the generic
  // `Cast from ... to ... is not supported` template instead.
  private val collationReason = CometCast.nonDefaultCollationReason

  // A scalar identity pair such as `lcase -> lcase` cannot be reached from SQL. Spark's
  // `SimplifyCasts` drops a cast whose child already carries the target type, so
  // `CAST(_1 COLLATE utf8_lcase AS STRING COLLATE UTF8_LCASE)` arrives at the planner as a bare
  // `Collate` and the only fallback reason on the plan is "collate is not supported", which comes
  // from a different serde. Those pairs stay pinned at the `isSupported` level above, in the same
  // spirit as the join tests in `CometCollationSuite` that no query can reach. A struct is the
  // way in: when a sibling field changes type the cast survives, and the collated field rides
  // along inside it. That case is covered end to end below.

  test("cast from a collated string falls back to Spark when codegen dispatch is off") {
    withCollatedTable {
      withSQLConf(CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "false") {
        checkSparkAnswerAndFallbackReason(
          "SELECT CAST(_1 COLLATE utf8_lcase AS INT) FROM collated_cast_tbl",
          collationReason)
      }
    }
  }

  test("cast from a collated string routes through the codegen dispatcher when it is on") {
    withCollatedTable {
      withSQLConf(CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "true") {
        checkSparkAnswerAndOperator(
          "SELECT CAST(_1 COLLATE utf8_lcase AS INT) FROM collated_cast_tbl")
      }
    }
  }

  test("cast of a struct carrying a collated field has no native path end to end") {
    // A pair the guard changed the answer for, not just the reason string. The sibling field
    // changes type so the cast survives `SimplifyCasts`, and on the old code the field zip
    // answered `Compatible`, since the collated field matched the identity shortcut, so the
    // struct went native with the collation dropped.
    withCollatedTable {
      withSQLConf(CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "false") {
        checkSparkAnswerAndFallbackReason(
          "SELECT CAST(struct(_2 AS a, _1 COLLATE utf8_lcase AS s) AS " +
            "STRUCT<a: STRING, s: STRING COLLATE UTF8_LCASE>) FROM collated_cast_tbl",
          collationReason)
      }
    }
  }

  test("cast of a struct carrying a collated field routes through the codegen dispatcher") {
    // The struct is the one case the guard redirects rather than merely relabels, so it needs the
    // dispatcher-on half too. `isSupportedDataType` recurses into the fields and accepts them, so
    // the cast stays in the Comet pipeline and the kernel runs Spark's own `Cast.doGenCode`.
    withCollatedTable {
      withSQLConf(CometConf.COMET_SCALA_UDF_CODEGEN_ENABLED.key -> "true") {
        checkSparkAnswerAndOperator(
          "SELECT CAST(struct(_2 AS a, _1 COLLATE utf8_lcase AS s) AS " +
            "STRUCT<a: STRING, s: STRING COLLATE UTF8_LCASE>) FROM collated_cast_tbl")
      }
    }
  }
}
