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

package org.apache.comet.serde.operator

import java.lang.reflect.InvocationTargetException

import scala.jdk.CollectionConverters._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.apache.iceberg.{DeleteFile, FileContent, Schema}
import org.apache.iceberg.expressions.{Expression, Expressions}
import org.apache.iceberg.types.Types
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.types.{ArrayType, BooleanType, DoubleType, FloatType, IntegerType, LongType, MapType, StringType, StructType}

import org.apache.comet.iceberg.IcebergReflection

/** Unit tests for Iceberg native-scan serde helpers that do not require a Spark session. */
class CometIcebergNativeScanSuite extends AnyFunSuite with Matchers {

  test("complex type null residuals are not serialized") {
    // Container predicates stay in the post-scan filter, not the native residual pool.
    for (dataType <- Seq(
        ArrayType(IntegerType),
        MapType(StringType, IntegerType),
        new StructType().add("value", IntegerType));
      predicate <- Seq(Expressions.isNull("value"), Expressions.notNull("value"))) {
      withClue(s"$dataType: $predicate") {
        CometIcebergNativeScan
          .icebergExprToProto(
            predicate,
            Seq(AttributeReference("value", dataType)()),
            Set.empty,
            Set.empty,
            partialPushdown = true)
          .isEmpty shouldBe true
      }
    }
    CometIcebergNativeScan
      .icebergExprToProto(
        Expressions.notNull("value"),
        Seq(AttributeReference("value", IntegerType)()),
        Set.empty,
        Set.empty,
        partialPushdown = true)
      .nonEmpty shouldBe true
  }

  /** The scan output the residual tests below convert against. */
  private val intColumn = Seq(AttributeReference("value", IntegerType)())

  private def serialize(residual: Any) =
    CometIcebergNativeScan.serializeResidual(
      residual,
      intColumn,
      Set.empty,
      Set.empty,
      partialPushdown = true)

  private def causes(t: Throwable): Iterator[Throwable] =
    Iterator.iterate(t)(_.getCause).takeWhile(_ != null)

  /**
   * Shaped like an Iceberg `UnboundPredicate` as far as the residual converter is concerned -- it
   * dispatches on the class-name suffix -- but its accessor throws, the way reflection would
   * against an Iceberg whose expression API has moved. It must stay a named class: a local or
   * anonymous one gets a runtime name that no longer ends in `UnboundPredicate`.
   */
  class ThrowingUnboundPredicate {
    def op(): AnyRef = throw new IllegalStateException("residual op() blew up")
  }

  /** Same shape, but the accessor is not declared at all. */
  class AccessorlessUnboundPredicate

  // Serde keeps its two ways of not pushing a residual apart. A reflection failure fails the
  // query: serde runs after CometScanRule has committed the scan to native execution, so there
  // is no fallback left. A residual the converter declines on purpose is left to the post-scan
  // filter. These go through serializeResidual, so a swallowing catch in either it or the
  // converter underneath turns them red.
  test("a residual whose reflection fails is fatal, not a silent drop") {
    val thrown = intercept[RuntimeException](serialize(new ThrowingUnboundPredicate))
    thrown.getMessage should include("Iceberg reflection failure")
    causes(thrown).exists(_.getMessage == "residual op() blew up") shouldBe true
  }

  test("a residual whose accessor is missing is fatal, not a silent drop") {
    val thrown = intercept[RuntimeException](serialize(new AccessorlessUnboundPredicate))
    causes(thrown).exists(_.isInstanceOf[NoSuchMethodException]) shouldBe true
  }

  test("residuals the converter declines are still dropped rather than raised") {
    val declined = Seq(
      // No native predicate models NOT_IN.
      Expressions.notIn("value", Integer.valueOf(1), Integer.valueOf(2)),
      // Not a node type the converter maps.
      Expressions.alwaysTrue(),
      Expressions.alwaysFalse(),
      // Iceberg spells a nested field as a dotted path, which is never a scan output attribute.
      Expressions.equal("outer.value", Integer.valueOf(1)))
    for (expr <- declined) {
      withClue(s"$expr: ") {
        serialize(expr).isEmpty shouldBe true
      }
    }
    // iceberg-rust cannot use these columns in the page index; the post-scan filter has them.
    CometIcebergNativeScan
      .serializeResidual(
        Expressions.equal("value", Integer.valueOf(1)),
        intColumn,
        Set("value"),
        Set.empty,
        partialPushdown = true)
      .isEmpty shouldBe true
    // The same predicate converts when nothing declines it, so the cases above are not vacuous.
    serialize(Expressions.equal("value", Integer.valueOf(1))).nonEmpty shouldBe true
  }

  test("positive conjunctions preserve supported predicates beside complex null checks") {
    val supported = Expressions.greaterThan("value", Integer.valueOf(5))
    val expected = serialize(supported)
    expected.nonEmpty shouldBe true

    for (dataType <- Seq(
        ArrayType(IntegerType),
        MapType(StringType, IntegerType),
        new StructType().add("field", IntegerType));
      unsupported <- Seq(Expressions.isNull("complex"), Expressions.notNull("complex"))) {
      val output = intColumn :+ AttributeReference("complex", dataType)()
      for (residual <- Seq(
          Expressions.and(supported, unsupported),
          Expressions.and(unsupported, supported))) {
        withClue(s"$dataType: $residual: ") {
          CometIcebergNativeScan.serializeResidual(
            residual,
            output,
            Set.empty,
            Set.empty,
            partialPushdown = true) shouldBe expected
        }
      }
    }
  }

  test("floating-point ranges and zero literals are not pushed") {
    val supported = Expressions.greaterThan("value", Integer.valueOf(5))
    val expected = serialize(supported)
    for (dataType <- Seq(FloatType, DoubleType)) {
      val output = intColumn :+ AttributeReference("floating", dataType)()
      def convert(expr: Any) =
        CometIcebergNativeScan.serializeResidual(
          expr,
          output,
          Set.empty,
          Set.empty,
          partialPushdown = true)
      def declined(predicate: Expression): Unit =
        withClue(s"$dataType: $predicate: ") {
          convert(predicate).isEmpty shouldBe true
          convert(Expressions.and(supported, predicate)) shouldBe expected
          convert(Expressions.and(predicate, supported)) shouldBe expected
          convert(Expressions.or(supported, predicate)).isEmpty shouldBe true
          convert(Expressions.not(predicate)).isEmpty shouldBe true
          convert(Expressions.not(Expressions.and(supported, predicate))).isEmpty shouldBe true
        }

      // The native reader orders floats differently from Spark: it separates the signed zeros
      // and sorts negative NaNs below every number. Range comparisons stay out for every literal.
      for (value <- Seq(-0.0, 0.0, 1.0, Double.NegativeInfinity, Double.PositiveInfinity);
        predicate <- Seq(
          Expressions.lessThan("floating", Double.box(value)),
          Expressions.lessThanOrEqual("floating", Double.box(value)),
          Expressions.greaterThan("floating", Double.box(value)),
          Expressions.greaterThanOrEqual("floating", Double.box(value)))) {
        declined(predicate)
      }
      // A zero literal is declined, including as any element of IN.
      for (value <- Seq(-0.0, 0.0);
        predicate <- Seq(
          Expressions.equal("floating", Double.box(value)),
          Expressions.notEqual("floating", Double.box(value)),
          Expressions.in("floating", Double.box(value), Double.box(2.0)),
          Expressions.in("floating", Double.box(2.0), Double.box(value)))) {
        declined(predicate)
      }
      // Other literals compare the same way in both, so != and IN are pushed in any position.
      for (value <- Seq(1.0, -1.0, Double.NegativeInfinity, Double.PositiveInfinity);
        predicate <- Seq(
          Expressions.notEqual("floating", Double.box(value)),
          Expressions.in("floating", Double.box(value), Double.box(2.0)))) {
        withClue(s"$dataType: $predicate: ") {
          convert(predicate).nonEmpty shouldBe true
          convert(Expressions.or(supported, predicate)).nonEmpty shouldBe true
          convert(Expressions.not(predicate)).nonEmpty shouldBe true
        }
      }
      // Equality under NOT still follows the nullable-column rule tested below.
      val eq = Expressions.equal("floating", Double.box(1.0))
      convert(eq).nonEmpty shouldBe true
      convert(Expressions.not(eq)).isEmpty shouldBe true
      for (predicate <- Seq(Expressions.isNull("floating"), Expressions.notNull("floating"))) {
        convert(predicate).nonEmpty shouldBe true
        convert(Expressions.not(predicate)).nonEmpty shouldBe true
      }
    }
  }

  test("NaN checks are not pushed") {
    // Spark's isnan(f), f = NaN and f != NaN reach the serde as IS_NAN and NOT_NAN.
    val supported = Expressions.greaterThan("value", Integer.valueOf(5))
    for (dataType <- Seq(FloatType, DoubleType);
      predicate <- Seq(Expressions.isNaN("floating"), Expressions.notNaN("floating"))) {
      val output = intColumn :+ AttributeReference("floating", dataType)()
      def convert(expr: Any) =
        CometIcebergNativeScan.serializeResidual(
          expr,
          output,
          Set.empty,
          Set.empty,
          partialPushdown = true)
      withClue(s"$dataType: $predicate: ") {
        convert(predicate).isEmpty shouldBe true
        convert(Expressions.not(predicate)).isEmpty shouldBe true
        convert(Expressions.and(supported, predicate)) shouldBe serialize(supported)
      }
    }
  }

  test("a FLOAT literal is pushed only when it narrows exactly to a nonzero float") {
    val output = Seq(AttributeReference("floating", FloatType)())
    def convert(value: AnyRef) =
      CometIcebergNativeScan.serializeResidual(
        Expressions.equal("floating", value),
        output,
        Set.empty,
        Set.empty,
        partialPushdown = true)
    convert(Float.box(0.1f)).nonEmpty shouldBe true
    convert(Double.box(0.5)).nonEmpty shouldBe true
    // 0.1 is not a float, and 1e-50 narrows to 0.0f.
    convert(Double.box(0.1)).isEmpty shouldBe true
    convert(Double.box(1e-50)).isEmpty shouldBe true
  }

  test("promoted columns push only null checks and equality outside NOT") {
    // Required, so that NOT keeps equality for any reason other than the promotion.
    val required = Seq(AttributeReference("value", IntegerType, nullable = false)())
    def unpromoted(expr: Any) =
      CometIcebergNativeScan.serializeResidual(
        expr,
        required,
        Set.empty,
        Set.empty,
        partialPushdown = true)
    def promoted(expr: Any) =
      CometIcebergNativeScan.serializeResidual(
        expr,
        required,
        Set.empty,
        Set("value"),
        partialPushdown = true)
    val equal = Expressions.equal("value", Integer.valueOf(1))
    for (predicate <- Seq(equal, Expressions.isNull("value"), Expressions.notNull("value"))) {
      withClue(s"$predicate: ") {
        promoted(predicate).nonEmpty shouldBe true
        promoted(predicate) shouldBe unpromoted(predicate)
      }
    }
    for (predicate <- Seq(
        Expressions.not(equal),
        Expressions.notEqual("value", Integer.valueOf(1)),
        Expressions.lessThan("value", Integer.valueOf(1)),
        Expressions.greaterThanOrEqual("value", Integer.valueOf(1)),
        Expressions.in("value", Integer.valueOf(1), Integer.valueOf(2)))) {
      withClue(s"$predicate: ") {
        unpromoted(predicate).nonEmpty shouldBe true
        promoted(predicate).isEmpty shouldBe true
      }
    }
  }

  /** Stands in for an Iceberg `Table`, of which promotedColumns reads only `schemas()`. */
  class SchemaHistory(history: (Int, Schema)*) {
    def schemas(): java.util.Map[Integer, Schema] =
      history.map { case (id, schema) => Integer.valueOf(id) -> schema }.toMap.asJava
  }

  test("promoted columns are found by field id across the schema history") {
    val original = new Schema(
      Types.NestedField.optional(1, "id", Types.IntegerType.get()),
      Types.NestedField.optional(2, "f", Types.FloatType.get()),
      Types.NestedField.optional(3, "s", Types.StringType.get()),
      Types.NestedField.optional(4, "dropped", Types.IntegerType.get()))
    val current = new Schema(
      Types.NestedField.optional(1, "id", Types.LongType.get()),
      Types.NestedField.optional(2, "f", Types.DoubleType.get()),
      Types.NestedField.optional(3, "renamed", Types.StringType.get()),
      Types.NestedField.optional(5, "added", Types.IntegerType.get()))
    val table = new SchemaHistory(0 -> original, 1 -> current)
    IcebergReflection.promotedColumns(table, current) shouldBe Set("id", "f")
    IcebergReflection.promotedColumns(table, original) shouldBe Set("id", "f")
    IcebergReflection.promotedColumns(new SchemaHistory(0 -> current), current) shouldBe empty
  }

  test("an incomplete schema history treats every promotable column as promoted") {
    // Without schema 0, as after expiring snapshots with cleanExpiredMetadata, a long, double or
    // decimal column may still hold files written as a narrower type.
    val schema = new Schema(
      Types.NestedField.optional(1, "id", Types.LongType.get()),
      Types.NestedField.optional(2, "f", Types.DoubleType.get()),
      Types.NestedField.optional(3, "amount", Types.DecimalType.of(10, 2)),
      Types.NestedField.optional(4, "i", Types.IntegerType.get()),
      Types.NestedField.optional(5, "s", Types.StringType.get()))
    IcebergReflection.promotedColumns(new SchemaHistory(1 -> schema), schema) shouldBe
      Set("id", "f", "amount")
    IcebergReflection.promotedColumns(
      new SchemaHistory(0 -> schema, 2 -> schema),
      schema) shouldBe
      Set("id", "f", "amount")
    IcebergReflection.promotedColumns(
      new SchemaHistory(0 -> schema, 1 -> schema),
      schema) shouldBe
      empty
  }

  test("positive conjunctions retain only the supported input scope") {
    val supported = Expressions.equal("value", Integer.valueOf(1))
    val unsupported = Expressions.notIn("value", Integer.valueOf(2))
    serialize(Expressions.and(supported, unsupported)) shouldBe serialize(supported)
    serialize(Expressions.and(unsupported, unsupported)).isEmpty shouldBe true

    // The same rule applies to a leaf declined by the page-index gate or output projection.
    val other = Expressions.equal("other", Integer.valueOf(1))
    val output = intColumn :+ AttributeReference("other", IntegerType)()
    CometIcebergNativeScan.serializeResidual(
      Expressions.and(supported, other),
      output,
      Set("other"),
      Set.empty,
      partialPushdown = true) shouldBe serialize(supported)
    serialize(Expressions.and(supported, other)) shouldBe serialize(supported)
  }

  test("without partial pushdown, an AND is pushed only whole") {
    val supported = Expressions.equal("value", Integer.valueOf(1))
    val unsupported = Expressions.notIn("value", Integer.valueOf(2))
    def convert(expr: Any) =
      CometIcebergNativeScan.serializeResidual(
        expr,
        intColumn,
        Set.empty,
        Set.empty,
        partialPushdown = false)
    convert(Expressions.and(supported, unsupported)).isEmpty shouldBe true
    convert(Expressions.and(unsupported, supported)).isEmpty shouldBe true
    val whole = Expressions.and(supported, Expressions.lessThan("value", Integer.valueOf(5)))
    convert(whole).nonEmpty shouldBe true
    convert(whole) shouldBe serialize(whole)
  }

  test("a partial residual compares BIGINT and DOUBLE columns only by equality") {
    // A promotion made before Iceberg 0.12 left no older schema, so a partial residual treats
    // every BIGINT and DOUBLE column as promoted. A whole residual relies on the recorded history.
    val output = intColumn ++ Seq(
      AttributeReference("big", LongType)(),
      AttributeReference("dbl", DoubleType)(),
      AttributeReference("l", ArrayType(IntegerType))())
    def convert(expr: Any, promoted: Set[String] = Set.empty) =
      CometIcebergNativeScan.serializeResidual(
        expr,
        output,
        Set.empty,
        promoted,
        partialPushdown = true)
    val supported = Expressions.greaterThan("value", Integer.valueOf(5))
    val unsupported =
      Seq(Expressions.notIn("value", Integer.valueOf(2)), Expressions.notNull("l"))
    val ranged = Seq(
      Expressions.greaterThan("big", java.lang.Long.valueOf(5)),
      Expressions.lessThan("big", java.lang.Long.valueOf(3000000000L)),
      Expressions.notEqual("big", java.lang.Long.valueOf(5)),
      Expressions.in("big", java.lang.Long.valueOf(1), java.lang.Long.valueOf(3000000000L)),
      Expressions.notEqual("dbl", Double.box(0.1)),
      Expressions.in("dbl", Double.box(1.5), Double.box(2.5)))
    val equalities = Seq(
      Expressions.equal("big", java.lang.Long.valueOf(5)),
      Expressions.equal("dbl", Double.box(1.5)),
      Expressions.notNull("big"),
      Expressions.isNull("dbl"))
    for (predicate <- ranged) {
      withClue(s"$predicate: ") {
        convert(predicate).nonEmpty shouldBe true
        convert(Expressions.and(predicate, supported)).nonEmpty shouldBe true
        convert(Expressions.and(predicate, supported), Set("big", "dbl")) shouldBe
          serialize(supported)
        for (sibling <- unsupported) {
          convert(Expressions.and(predicate, sibling)).isEmpty shouldBe true
          convert(Expressions.and(sibling, predicate)).isEmpty shouldBe true
          convert(Expressions.and(Expressions.and(predicate, supported), sibling)) shouldBe
            serialize(supported)
        }
      }
    }
    for (predicate <- equalities; sibling <- unsupported) {
      withClue(s"$predicate AND $sibling: ") {
        convert(predicate).nonEmpty shouldBe true
        convert(Expressions.and(predicate, sibling)) shouldBe convert(predicate)
        convert(Expressions.and(sibling, predicate)) shouldBe convert(predicate)
      }
    }
  }

  test("positive OR requires a safe predicate from both branches") {
    val left = Expressions.lessThan("value", Integer.valueOf(5))
    val right = Expressions.greaterThan("value", Integer.valueOf(10))
    val unsupported = Expressions.notIn("value", Integer.valueOf(2))

    serialize(Expressions.or(left, unsupported)).isEmpty shouldBe true
    serialize(Expressions.or(unsupported, left)).isEmpty shouldBe true
    serialize(
      Expressions.or(
        Expressions.and(left, unsupported),
        Expressions.and(right, unsupported))) shouldBe serialize(Expressions.or(left, right))
    serialize(Expressions.and(left, Expressions.or(right, unsupported))) shouldBe serialize(left)
  }

  test("NOT requires an exact child even across nested conjunctions and disjunctions") {
    val left = Expressions.lessThan("value", Integer.valueOf(5))
    val right = Expressions.greaterThan("value", Integer.valueOf(10))
    val unsupported = Expressions.notIn("value", Integer.valueOf(2))

    val declined = Seq(
      Expressions.not(Expressions.and(left, unsupported)),
      Expressions.not(Expressions.and(unsupported, left)),
      Expressions.not(Expressions.or(left, Expressions.and(right, unsupported))),
      Expressions.not(Expressions.and(left, Expressions.or(right, unsupported))))
    for (residual <- declined) {
      withClue(s"$residual: ") {
        serialize(residual).isEmpty shouldBe true
      }
    }
    // A declined NOT subtree can still sit beside a supported positive conjunct.
    serialize(Expressions.and(left, declined.head)) shouldBe serialize(left)
    serialize(Expressions.or(left, declined.head)).isEmpty shouldBe true
    serialize(Expressions.not(Expressions.and(left, right))).nonEmpty shouldBe true
    serialize(Expressions.not(Expressions.or(left, right))).nonEmpty shouldBe true
    // Iceberg removes a double NOT before serde, leaving a positive conjunction.
    serialize(Expressions.not(Expressions.not(Expressions.and(left, unsupported)))) shouldBe
      serialize(left)
  }

  test("NOT declines equality on a nullable column, which may be a null-safe comparison") {
    // Iceberg converts Spark's `value <=> 1` to `value = 1`. Negated, Spark keeps NULL rows that
    // the native reader's not-equal drops.
    val eq = Expressions.equal("value", Integer.valueOf(1))
    val lt = Expressions.lessThan("value", Integer.valueOf(5))
    serialize(eq).nonEmpty shouldBe true
    for (residual <- Seq(
        Expressions.not(eq),
        Expressions.not(Expressions.and(eq, lt)),
        Expressions.not(Expressions.or(lt, eq)))) {
      withClue(s"$residual: ") {
        serialize(residual).isEmpty shouldBe true
      }
    }
    serialize(Expressions.and(lt, Expressions.not(eq))) shouldBe serialize(lt)
    // Spark's other comparisons are NULL for a NULL input, as in the native reader.
    serialize(Expressions.not(lt)).nonEmpty shouldBe true
    serialize(Expressions.notEqual("value", Integer.valueOf(1))).nonEmpty shouldBe true
    // A required column holds no NULLs.
    val required = Seq(AttributeReference("value", IntegerType, nullable = false)())
    CometIcebergNativeScan
      .serializeResidual(
        Expressions.not(eq),
        required,
        Set.empty,
        Set.empty,
        partialPushdown = true)
      .nonEmpty shouldBe true
  }

  test("a NOT_NULL conjunct lets equality on the same column convert under NOT") {
    // A NULL value fails the AND in Spark and in the native reader alike, so the negated
    // equality only sees non-NULL values. Spark writes `b = false` as `b IS NOT NULL AND NOT b`.
    val eq = Expressions.equal("value", Integer.valueOf(1))
    val lt = Expressions.lessThan("value", Integer.valueOf(5))
    val notNull = Expressions.notNull("value")
    def convert(residual: Any, output: Seq[Attribute]) =
      CometIcebergNativeScan.serializeResidual(
        residual,
        output,
        Set.empty,
        Set.empty,
        partialPushdown = false)
    for (residual <- Seq(
        Expressions.and(notNull, Expressions.not(eq)),
        Expressions.and(Expressions.and(lt, notNull), Expressions.not(eq)),
        Expressions.and(notNull, Expressions.or(lt, Expressions.not(eq))),
        Expressions.not(Expressions.and(notNull, eq)))) {
      withClue(s"$residual: ") {
        convert(residual, intColumn).nonEmpty shouldBe true
      }
    }
    // The check must be a conjunct of an AND that encloses the equality.
    val twoColumns = intColumn :+ AttributeReference("other", IntegerType)()
    for (residual <- Seq(
        Expressions.or(notNull, Expressions.not(eq)),
        Expressions.and(Expressions.or(notNull, lt), Expressions.not(eq)),
        Expressions.and(Expressions.not(notNull), Expressions.not(eq)),
        Expressions.and(Expressions.notNull("other"), Expressions.not(eq)))) {
      withClue(s"$residual: ") {
        convert(residual, twoColumns).isEmpty shouldBe true
      }
    }
  }

  test("partial residuals retain null-guarded negated boolean equality") {
    val boolean = AttributeReference("b", BooleanType)()
    val output = Seq(boolean, AttributeReference("l", ArrayType(IntegerType))())
    val guarded = Expressions.and(
      Expressions.notNull("b"),
      Expressions.not(Expressions.equal("b", java.lang.Boolean.TRUE)))
    val expected = CometIcebergNativeScan.serializeResidual(
      guarded,
      Seq(boolean.withNullability(false)),
      Set.empty,
      Set.empty,
      partialPushdown = false)
    expected.nonEmpty shouldBe true
    for (residual <- Seq(
        Expressions.and(Expressions.notNull("l"), guarded),
        Expressions.and(guarded, Expressions.notNull("l")))) {
      CometIcebergNativeScan.serializeResidual(
        residual,
        output,
        Set.empty,
        Set.empty,
        partialPushdown = true) shouldBe expected
    }
  }

  test("a partial promotion guard still declines negated equality with a null check") {
    val output = Seq(
      AttributeReference("big", LongType)(),
      AttributeReference("l", ArrayType(IntegerType))())
    def convert(residual: Any) =
      CometIcebergNativeScan.serializeResidual(
        residual,
        output,
        Set.empty,
        Set.empty,
        partialPushdown = true)
    val notNull = Expressions.notNull("big")
    val guarded = Expressions.and(
      notNull,
      Expressions.not(Expressions.equal("big", java.lang.Long.valueOf(5))))
    convert(Expressions.and(Expressions.notNull("l"), guarded)) shouldBe convert(notNull)
    convert(notNull).nonEmpty shouldBe true
  }

  test("a transform residual is declined rather than read as its source column") {
    // UnboundTransform answers ref() with the source column, so converting the term like a bare
    // reference would push bucket(4, value) = 1 as value = 1 and drop matching rows.
    // CometScanRule declines a non-identity transform during planning, but only when the
    // residual is a bare predicate, so a nested one has to be declined here. The end-to-end
    // wrong answer is pinned in CometIcebergResidualPushdownSuite.
    val bucketed =
      Expressions.equal(Expressions.bucket[Integer]("value", 4), Integer.valueOf(1))
    serialize(bucketed).isEmpty shouldBe true
    val nested = Expressions.and(bucketed, Expressions.equal("value", Integer.valueOf(1)))
    serialize(nested) shouldBe serialize(Expressions.equal("value", Integer.valueOf(1)))
  }

  private def translate(
      props: Map[String, String],
      targetBucket: Option[String]): Map[String, String] =
    CometIcebergNativeScan.hadoopToIcebergS3Properties(props, targetBucket)

  /** Every `fs.s3a.*` suffix the translator maps, paired with its global iceberg `s3.*` key. */
  private val suffixToIcebergKey = Seq(
    "access.key" -> "s3.access-key-id",
    "secret.key" -> "s3.secret-access-key",
    "session.token" -> "s3.session-token",
    "endpoint" -> "s3.endpoint",
    "path.style.access" -> "s3.path-style-access",
    "endpoint.region" -> "s3.region")

  test("full fs.s3a.* suffix mapping to global s3.* keys") {
    // Mappings are per-key independent (no cross-key interaction), so this table-driven case also
    // covers the "several global fs.s3a.* keys at once" scenario.
    suffixToIcebergKey.foreach { case (hadoopSuffix, icebergKey) =>
      val out = translate(Map(s"fs.s3a.$hadoopSuffix" -> "v"), None)
      out should contain(icebergKey -> "v")
      // The Hadoop key itself is never passed through untranslated.
      out.keys should not contain s"fs.s3a.$hadoopSuffix"
    }
  }

  test("target bucket per-bucket keys are promoted to global s3.*") {
    val props = suffixToIcebergKey.map { case (suffix, _) =>
      s"fs.s3a.bucket.target.$suffix" -> s"value-$suffix"
    }.toMap

    val out = translate(props, Some("target"))

    suffixToIcebergKey.foreach { case (suffix, icebergKey) =>
      out(icebergKey) shouldBe s"value-$suffix"
    }
    // Per-bucket keys are never emitted in s3.bucket.* form (the pinned parser ignores those).
    out.keys.foreach(k => k should not startWith "s3.bucket.")
  }

  test("target bucket keys coexist with a different non-target bucket") {
    val props = Map(
      "fs.s3a.bucket.target.endpoint" -> "https://target.example.com",
      "fs.s3a.bucket.target.access.key" -> "AKIA-target",
      "fs.s3a.bucket.other.endpoint" -> "https://other.example.com",
      "fs.s3a.bucket.other.access.key" -> "AKIA-other")

    val out = translate(props, Some("target"))

    out("s3.endpoint") shouldBe "https://target.example.com"
    out("s3.access-key-id") shouldBe "AKIA-target"
    // The non-target bucket contributes nothing: not promoted, not leaked into the global keys.
    out.values.toSet should not contain "https://other.example.com"
    out.values.toSet should not contain "AKIA-other"
    out.keys.size shouldBe 2
  }

  test("target bucket per-bucket value overrides a conflicting global value") {
    // targetBucketGlobals is merged last, so the per-bucket endpoint wins over the global one.
    val props = Map(
      "fs.s3a.endpoint" -> "https://global.example.com",
      "fs.s3a.access.key" -> "AKIA-global",
      "fs.s3a.bucket.target.endpoint" -> "https://target.example.com")

    val out = translate(props, Some("target"))

    out("s3.endpoint") shouldBe "https://target.example.com"
    // A global key with no per-bucket override survives.
    out("s3.access-key-id") shouldBe "AKIA-global"
  }

  test("dotted target bucket names survive (prefix match, not split)") {
    val props = Map(
      "fs.s3a.bucket.my.bucket.name.endpoint" -> "https://dotted.example.com",
      "fs.s3a.bucket.my.bucket.name.access.key" -> "AKIA-dotted",
      "fs.s3a.bucket.my.bucket.name.secret.key" -> "secret-dotted")

    val out = translate(props, Some("my.bucket.name"))

    out("s3.endpoint") shouldBe "https://dotted.example.com"
    out("s3.access-key-id") shouldBe "AKIA-dotted"
    out("s3.secret-access-key") shouldBe "secret-dotted"
  }

  test("keys already in iceberg s3.* form pass through; unrelated keys are ignored") {
    val props = Map(
      "s3.endpoint" -> "https://passthrough.example.com",
      "s3.access-key-id" -> "AKIA-passthrough",
      "fs.gs.project.id" -> "gcp-project",
      "fs.azure.account.key.acct.blob.core.windows.net" -> "azure-key",
      "spark.sql.shuffle.partitions" -> "200")

    translate(props, Some("target")) shouldBe Map(
      "s3.endpoint" -> "https://passthrough.example.com",
      "s3.access-key-id" -> "AKIA-passthrough")
  }

  test("no target bucket means no per-bucket promotion") {
    // Required-parameter contract: with None, per-bucket keys drop, only global keys survive.
    val props = Map(
      "fs.s3a.bucket.some.endpoint" -> "https://some.example.com",
      "fs.s3a.endpoint" -> "https://global.example.com")

    val out = translate(props, None)

    out("s3.endpoint") shouldBe "https://global.example.com"
    out.values.toSet should not contain "https://some.example.com"
  }

  private def keyMetadataMethod(clazz: Class[_]) = clazz.getMethod("keyMetadata")

  private def serializeDeleteFile(file: AnyRef) =
    CometIcebergNativeScan.serializeDeleteFile(
      file,
      file.getClass,
      file.getClass,
      keyMetadataMethod(file.getClass),
      _ => 0)

  test("required Iceberg delete-file accessors are present") {
    Seq("content", "specId", "equalityFieldIds").foreach { accessor =>
      IcebergReflection.findMethod(classOf[DeleteFile], accessor).isDefined shouldBe true
    }
  }

  test("Iceberg delete content names match native serde literals") {
    FileContent.POSITION_DELETES.toString shouldBe IcebergReflection.ContentTypes.POSITION_DELETES
    FileContent.EQUALITY_DELETES.toString shouldBe IcebergReflection.ContentTypes.EQUALITY_DELETES
  }

  test("position-delete file with null equality ids serializes without equality ids") {
    val proto = serializeDeleteFile(new PositionDeleteFile)
    proto.getContentType shouldBe "POSITION_DELETES"
    proto.getPartitionSpecId shouldBe 7
    proto.getEqualityIdsCount shouldBe 0
    proto.getFilePathIdx shouldBe 0
  }

  test("equality-delete file serializes declared equality ids") {
    val proto = serializeDeleteFile(new EqualityDeleteFile)
    proto.getContentType shouldBe "EQUALITY_DELETES"
    proto.getEqualityIdsCount shouldBe 2
    proto.getEqualityIds(0) shouldBe 3
    proto.getEqualityIds(1) shouldBe 5
  }

  test("equality-delete file with null equality ids is fatal") {
    val ex =
      intercept[IllegalStateException](serializeDeleteFile(new EqualityDeleteFileWithNullIds))
    ex.getMessage shouldBe
      "Iceberg equality delete file 's3://bucket/eq-null-ids.parquet' has no equality field IDs"
  }

  test("equality-delete file with empty equality ids is fatal") {
    val ex =
      intercept[IllegalStateException](serializeDeleteFile(new EqualityDeleteFileWithEmptyIds))
    ex.getMessage shouldBe
      "Iceberg equality delete file 's3://bucket/eq-empty-ids.parquet' has no equality field IDs"
  }

  test("content invocation failure propagates instead of defaulting to position deletes") {
    val ex =
      intercept[InvocationTargetException](serializeDeleteFile(new ThrowingContentDeleteFile))
    ex.getCause.getMessage shouldBe "content boom"
  }

  test("spec id invocation failure propagates instead of defaulting to zero") {
    val ex =
      intercept[InvocationTargetException](serializeDeleteFile(new ThrowingSpecIdDeleteFile))
    ex.getCause.getMessage shouldBe "spec boom"
  }

  test("equality-id invocation failure propagates instead of dropping ids") {
    val ex =
      intercept[InvocationTargetException](serializeDeleteFile(new ThrowingEqualityIdsDeleteFile))
    ex.getCause.getMessage shouldBe "ids boom"
  }

  test("missing content accessor is fatal") {
    assertThrows[NoSuchMethodException](serializeDeleteFile(new NoContentAccessorDeleteFile))
  }

  test("missing equality-id accessor is fatal") {
    assertThrows[NoSuchMethodException](serializeDeleteFile(new NoEqualityIdsAccessorDeleteFile))
  }

  test("missing delete-file path accessor is fatal") {
    val ex = intercept[RuntimeException](serializeDeleteFile(new NoPathAccessorDeleteFile))
    ex.getMessage should include("Neither location() nor path() is declared")
  }

  private abstract class BaseDeleteFile {
    def location(): String
    def format(): String = "PARQUET"
    def recordCount(): java.lang.Long = java.lang.Long.valueOf(1L)
    def keyMetadata(): java.nio.ByteBuffer = null
  }

  private class PositionDeleteFile extends BaseDeleteFile {
    override def location(): String = "s3://bucket/pos-delete.parquet"
    def content(): String = "POSITION_DELETES"
    def specId(): Int = 7
    def equalityFieldIds(): java.util.List[Integer] = null
  }

  private class EqualityDeleteFile extends BaseDeleteFile {
    override def location(): String = "s3://bucket/eq-delete.parquet"
    def content(): String = "EQUALITY_DELETES"
    def specId(): Int = 0
    def equalityFieldIds(): java.util.List[Integer] =
      java.util.List.of(Integer.valueOf(3), Integer.valueOf(5))
  }

  private class EqualityDeleteFileWithNullIds extends BaseDeleteFile {
    override def location(): String = "s3://bucket/eq-null-ids.parquet"
    def content(): String = "EQUALITY_DELETES"
    def specId(): Int = 0
    def equalityFieldIds(): java.util.List[Integer] = null
  }

  private class EqualityDeleteFileWithEmptyIds extends BaseDeleteFile {
    override def location(): String = "s3://bucket/eq-empty-ids.parquet"
    def content(): String = "EQUALITY_DELETES"
    def specId(): Int = 0
    def equalityFieldIds(): java.util.List[Integer] = java.util.List.of[Integer]()
  }

  private class ThrowingContentDeleteFile extends BaseDeleteFile {
    override def location(): String = "s3://bucket/d.parquet"
    def content(): String = throw new RuntimeException("content boom")
    def specId(): Int = 0
    def equalityFieldIds(): java.util.List[Integer] = null
  }

  private class ThrowingSpecIdDeleteFile extends BaseDeleteFile {
    override def location(): String = "s3://bucket/d.parquet"
    def content(): String = "POSITION_DELETES"
    def specId(): Int = throw new RuntimeException("spec boom")
    def equalityFieldIds(): java.util.List[Integer] = null
  }

  private class ThrowingEqualityIdsDeleteFile extends BaseDeleteFile {
    override def location(): String = "s3://bucket/d.parquet"
    def content(): String = "EQUALITY_DELETES"
    def specId(): Int = 0
    def equalityFieldIds(): java.util.List[Integer] = throw new RuntimeException("ids boom")
  }

  private class NoContentAccessorDeleteFile extends BaseDeleteFile {
    override def location(): String = "s3://bucket/d.parquet"
    def specId(): Int = 0
    def equalityFieldIds(): java.util.List[Integer] = null
  }

  private class NoEqualityIdsAccessorDeleteFile extends BaseDeleteFile {
    override def location(): String = "s3://bucket/d.parquet"
    def content(): String = "EQUALITY_DELETES"
    def specId(): Int = 0
  }

  private class NoPathAccessorDeleteFile {
    def keyMetadata(): java.nio.ByteBuffer = null
  }
}
