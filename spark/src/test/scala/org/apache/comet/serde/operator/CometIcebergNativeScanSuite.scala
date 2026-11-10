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
import java.math.{BigDecimal, BigInteger}

import scala.jdk.CollectionConverters._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.apache.iceberg.{DeleteFile, FileContent, Schema}
import org.apache.iceberg.expressions.{Expression, Expressions, Not}
import org.apache.iceberg.types.Types
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.types.{ArrayType, BinaryType, DecimalType, IntegerType, MapType, StringType, StructType}

import com.google.protobuf.ByteString

import org.apache.comet.iceberg.IcebergReflection
import org.apache.comet.serde.OperatorOuterClass.{IcebergLiteral, IcebergPredicate, IcebergPredicateOperator}

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
          .icebergExprToProto(predicate, Seq(AttributeReference("value", dataType)()), Set.empty)
          .isEmpty shouldBe true
      }
    }
    CometIcebergNativeScan
      .icebergExprToProto(
        Expressions.notNull("value"),
        Seq(AttributeReference("value", IntegerType)()),
        Set.empty)
      .nonEmpty shouldBe true
  }

  /** The scan output the residual tests below convert against. */
  private val intColumn = Seq(AttributeReference("value", IntegerType)())

  private def serialize(residual: Any) =
    CometIcebergNativeScan.serializeResidual(residual, intColumn, Set.empty)

  // Bypass the page-index column gate to test the literal contract independently of enabling
  // decimal pushdown. Going through the residual entry point also catches a conversion failure
  // incorrectly being reported as a fatal reflection error.
  private def serializeDecimal(
      residual: Expression,
      decimalType: DecimalType = DecimalType(10, 2)): Option[IcebergPredicate] =
    CometIcebergNativeScan.serializeResidual(
      residual,
      Seq(AttributeReference("amount", decimalType)(), AttributeReference("id", IntegerType)()),
      Set.empty)

  private def decimalEqual(value: String): Expression =
    Expressions.equal("amount", new BigDecimal(value))

  // Expressions.not(not(child)) returns child, so use the constructor to test a double NOT
  // that actually reaches the converter rather than a positive expression simplified by Iceberg.
  private def notWithoutSimplification(child: Expression): Expression = {
    val constructor = classOf[Not].getDeclaredConstructor(classOf[Expression])
    constructor.setAccessible(true)
    constructor.newInstance(child)
  }

  test("page-index gate allows decimal predicates and retains uuid fixed and binary") {
    val schema = new Schema(
      Types.NestedField.optional(1, "decimal_int", Types.DecimalType.of(9, 2)),
      Types.NestedField.optional(2, "decimal_long", Types.DecimalType.of(18, 2)),
      Types.NestedField.optional(3, "decimal_fixed", Types.DecimalType.of(38, 2)),
      Types.NestedField.optional(4, "uuid", Types.UUIDType.get()),
      Types.NestedField.optional(5, "fixed", Types.FixedType.ofLength(16)),
      Types.NestedField.optional(6, "binary", Types.BinaryType.get()))
    val gated = IcebergReflection.pageIndexUnsupportedColumns(schema)
    gated shouldBe Set("uuid", "fixed", "binary")
    for ((name, precision) <- Seq(
        ("decimal_int", 9),
        ("decimal_long", 18),
        ("decimal_fixed", 38))) {
      val output = Seq(AttributeReference(name, DecimalType(precision, 2))())
      val equal = CometIcebergNativeScan
        .serializeResidual(Expressions.equal(name, new BigDecimal("123.45")), output, gated)
        .getOrElse(fail(s"expected decimal pushdown for $name"))
      equal.getBinary.getColumn shouldBe name
      assertDecimalLiteral(equal.getBinary.getValue, precision, 2, "12345")
      for (expression <- Seq(Expressions.isNull(name), Expressions.notNull(name))) {
        CometIcebergNativeScan.serializeResidual(expression, output, gated).nonEmpty shouldBe true
      }
    }
    for ((name, dataType) <- Seq(
        ("uuid", StringType),
        ("fixed", BinaryType),
        ("binary", BinaryType));
      expression <- Seq(Expressions.isNull(name), Expressions.notNull(name))) {
      val output = Seq(AttributeReference(name, dataType)())
      CometIcebergNativeScan.serializeResidual(expression, output, gated).isEmpty shouldBe true
    }
  }

  private def assertDecimalLiteral(
      literal: IcebergLiteral,
      precision: Int,
      scale: Int,
      unscaled: String): Unit = {
    literal.getIsNull shouldBe false
    literal.hasDecimalVal shouldBe true
    val decimal = literal.getDecimalVal
    decimal.getPrecision shouldBe precision
    decimal.getScale shouldBe scale
    decimal.getUnscaled shouldBe ByteString.copyFrom(new BigInteger(unscaled).toByteArray)
  }

  for (precision <- Seq(9, 18, 38)) {
    for ((description, value, unscaled) <- Seq(
        ("same scale", "123.45", "12345"),
        ("trailing zero removal", "123.450", "12345"),
        ("scale expansion", "123.4", "12340"),
        ("negative value", "-123.45", "-12345"),
        ("zero", "0.000", "0"),
        ("negative literal scale", "1E+2", "10000"))) {
      test(s"decimal($precision,2) residual literal preserves $description exactly") {
        val predicate = serializeDecimal(decimalEqual(value), DecimalType(precision, 2))
          .getOrElse(fail(s"expected decimal pushdown for $value"))
        predicate.hasBinary shouldBe true
        predicate.getBinary.getColumn shouldBe "amount"
        predicate.getBinary.getOp shouldBe IcebergPredicateOperator.Eq
        assertDecimalLiteral(predicate.getBinary.getValue, precision, 2, unscaled)
      }
    }

    for (sign <- Seq("", "-")) {
      test(s"decimal($precision,2) residual literal preserves the ${sign}precision boundary") {
        // At precision 38 these are actual 38-digit coefficients, beyond rust_decimal's old
        // 28-digit limit, rather than small values in a wide declared column.
        val value = sign + "9" * (precision - 2) + ".99"
        val predicate = serializeDecimal(decimalEqual(value), DecimalType(precision, 2))
          .getOrElse(fail(s"expected decimal pushdown for $value"))
        assertDecimalLiteral(predicate.getBinary.getValue, precision, 2, sign + "9" * precision)
      }
    }
  }

  test("decimal residual literal uses signed big-endian two's-complement bytes") {
    for ((value, bytes) <- Seq(
        ("1.28", Array[Byte](0, -128)),
        ("-1.29", Array[Byte](-1, 127)),
        ("-0.01", Array[Byte](-1)))) {
      val predicate = serializeDecimal(decimalEqual(value))
        .getOrElse(fail(s"expected decimal pushdown for $value"))
      predicate.getBinary.getValue.getDecimalVal.getUnscaled shouldBe ByteString.copyFrom(bytes)
    }
  }

  test("decimal residual literals requiring rounding are declined without an exception") {
    for (value <- Seq("123.456", "-123.456", "0.001", "-0.001")) {
      withClue(s"literal $value: ") {
        serializeDecimal(decimalEqual(value)).isEmpty shouldBe true
      }
    }
  }

  test("decimal residual literals exceeding column precision after rescaling are declined") {
    for (value <- Seq("10", "-10", "10.00", "-10.00")) {
      withClue(s"literal $value: ") {
        serializeDecimal(decimalEqual(value), DecimalType(3, 2)).isEmpty shouldBe true
      }
    }
  }

  test("decimal residual null checks remain unary predicates") {
    for ((expression, op) <- Seq(
        (Expressions.isNull("amount"), IcebergPredicateOperator.IsNull),
        (Expressions.notNull("amount"), IcebergPredicateOperator.NotNull))) {
      val predicate = serializeDecimal(expression).getOrElse(fail("expected unary pushdown"))
      predicate.hasUnary shouldBe true
      predicate.getUnary.getColumn shouldBe "amount"
      predicate.getUnary.getOp shouldBe op
    }
  }

  test("decimal residual IN preserves every literal at the column precision and scale") {
    val expression = Expressions.in(
      "amount",
      new BigDecimal("1.0"),
      new BigDecimal("-5.000"),
      new BigDecimal("999999999999999999999999999999999999.99"))
    val predicate = serializeDecimal(expression, DecimalType(38, 2))
      .getOrElse(fail("expected decimal IN pushdown"))
    predicate.hasSet shouldBe true
    predicate.getSet.getColumn shouldBe "amount"
    predicate.getSet.getOp shouldBe IcebergPredicateOperator.In
    val literals = predicate.getSet.getValuesList.asScala.toSeq
    literals.size shouldBe 3
    literals.foreach { literal =>
      literal.hasDecimalVal shouldBe true
      literal.getDecimalVal.getPrecision shouldBe 38
      literal.getDecimalVal.getScale shouldBe 2
    }
    literals
      .map(l => new BigInteger(l.getDecimalVal.getUnscaled.toByteArray).toString)
      .toSet shouldBe
      Set("100", "-500", "99999999999999999999999999999999999999")
  }

  test("decimal residual IN declines the entire set if any literal requires rounding") {
    for (values <- Seq(Seq("1.00", "5.001"), Seq("5.001", "1.00"))) {
      val expression = Expressions.in("amount", values.map(new BigDecimal(_)): _*)
      serializeDecimal(expression).isEmpty shouldBe true
    }
  }

  test("decimal residual positive AND preserves the other conjunct when a literal is declined") {
    val keep = Expressions.equal("id", Integer.valueOf(7))
    val declined = decimalEqual("123.456")
    val expected = serializeDecimal(keep).getOrElse(fail("expected integer pushdown"))
    for (expression <- Seq(Expressions.and(keep, declined), Expressions.and(declined, keep))) {
      withClue(s"$expression: ") {
        serializeDecimal(expression) shouldBe Some(expected)
      }
    }
  }

  test("decimal residual positive AND preserves an exactly rescaled decimal sibling") {
    val keep = decimalEqual("123.450")
    val expected = serializeDecimal(keep).getOrElse(fail("expected exact decimal pushdown"))
    serializeDecimal(Expressions.and(keep, decimalEqual("123.456"))) shouldBe Some(expected)
  }

  test("decimal residual NOT restriction does not prevent pushing its positive AND sibling") {
    val keep = Expressions.equal("id", Integer.valueOf(7))
    val declined = Expressions.not(Expressions.and(keep, decimalEqual("123.456")))
    val expected = serializeDecimal(keep).getOrElse(fail("expected integer pushdown"))
    for (expression <- Seq(Expressions.and(keep, declined), Expressions.and(declined, keep))) {
      serializeDecimal(expression) shouldBe Some(expected)
    }
  }

  test("decimal residual AND with no convertible conjunct is declined") {
    serializeDecimal(
      Expressions.and(decimalEqual("1.001"), decimalEqual("2.001"))).isEmpty shouldBe true
  }

  test("decimal residual OR cannot discard an unconvertible disjunct") {
    val keep = Expressions.equal("id", Integer.valueOf(7))
    val declined = decimalEqual("123.456")
    serializeDecimal(keep).nonEmpty shouldBe true
    for (expression <- Seq(Expressions.or(keep, declined), Expressions.or(declined, keep))) {
      serializeDecimal(expression).isEmpty shouldBe true
    }
  }

  test("decimal residual OR may combine two safely weakened positive AND branches") {
    val left = Expressions.equal("id", Integer.valueOf(7))
    val right = Expressions.equal("id", Integer.valueOf(8))
    val expected = serializeDecimal(Expressions.or(left, right))
      .getOrElse(fail("expected integer OR pushdown"))
    val expression = Expressions.or(
      Expressions.and(left, decimalEqual("1.001")),
      Expressions.and(decimalEqual("2.001"), right))
    serializeDecimal(expression) shouldBe Some(expected)
  }

  test("decimal residual NOT requires complete conversion throughout its subtree") {
    val keep = Expressions.equal("id", Integer.valueOf(7))
    val other = Expressions.equal("id", Integer.valueOf(8))
    val declined = decimalEqual("123.456")
    // Positive controls use only convertible integers, so these guards already exercise NOT
    // traversal before decimal literal support is implemented.
    val complete = serializeDecimal(Expressions.not(Expressions.and(keep, other)))
      .getOrElse(fail("expected complete NOT pushdown"))
    complete.hasNot shouldBe true
    for (child <- Seq(Expressions.and(keep, declined), Expressions.and(declined, keep))) {
      serializeDecimal(Expressions.not(child)).isEmpty shouldBe true
      serializeDecimal(
        notWithoutSimplification(notWithoutSimplification(child))).isEmpty shouldBe true
    }
    serializeDecimal(
      Expressions.not(
        Expressions.or(Expressions.and(keep, declined), other))).isEmpty shouldBe true
  }

  test("decimal residual NOT OR declines the whole subtree when a disjunct is unconvertible") {
    val valid = Expressions.equal("id", Integer.valueOf(7))
    val other = Expressions.equal("id", Integer.valueOf(8))
    val invalid = decimalEqual("123.456")
    val complete = serializeDecimal(Expressions.not(Expressions.or(valid, other)))
      .getOrElse(fail("expected complete NOT OR pushdown"))
    complete.hasNot shouldBe true
    complete.getNot.hasOr shouldBe true
    // A converter aware of negation could weaken NOT(valid OR invalid) to NOT(valid).
    // This optimization is not required: declining the entire NOT subtree is conservative,
    // leaving the exact expression to the post-scan filter.
    for (child <- Seq(Expressions.or(valid, invalid), Expressions.or(invalid, valid))) {
      withClue(s"NOT($child): ") {
        serializeDecimal(Expressions.not(child)).isEmpty shouldBe true
      }
      serializeDecimal(
        notWithoutSimplification(notWithoutSimplification(child))).isEmpty shouldBe true
    }
  }

  test("decimal residual double NOT preserves a completely convertible child") {
    val child = decimalEqual("123.450")
    val expected = serializeDecimal(child).getOrElse(fail("expected decimal pushdown"))
    val predicate = serializeDecimal(notWithoutSimplification(notWithoutSimplification(child)))
      .getOrElse(fail("expected double NOT pushdown"))
    predicate.hasNot shouldBe true
    predicate.getNot.hasNot shouldBe true
    predicate.getNot.getNot shouldBe expected
  }

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
      .serializeResidual(Expressions.equal("value", Integer.valueOf(1)), intColumn, Set("value"))
      .isEmpty shouldBe true
    // The same predicate converts when nothing declines it, so the cases above are not vacuous.
    val supported = Expressions.equal("value", Integer.valueOf(1))
    serialize(supported).nonEmpty shouldBe true
    serialize(Expressions.and(supported, Expressions.notIn("value", Integer.valueOf(2)))) shouldBe
      serialize(supported)
  }

  test("a transform residual is declined rather than read as its source column") {
    // UnboundTransform answers ref() with the source column, so converting the term like a bare
    // reference would push bucket(4, value) = 1 as value = 1 and drop matching rows.
    // CometScanRule declines a bare transform during planning. In a positive AND the serde may
    // keep a supported sibling, but must never turn the transform into a source-column predicate.
    // The end-to-end wrong answer is pinned in CometIcebergResidualPushdownSuite.
    val bucketed =
      Expressions.equal(Expressions.bucket[Integer]("value", 4), Integer.valueOf(1))
    serialize(bucketed).isEmpty shouldBe true
    val supported = Expressions.equal("value", Integer.valueOf(1))
    val nested = Expressions.and(bucketed, supported)
    serialize(nested) shouldBe serialize(supported)
    serialize(Expressions.or(bucketed, supported)).isEmpty shouldBe true
    serialize(Expressions.not(nested)).isEmpty shouldBe true
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
