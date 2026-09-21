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

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.apache.iceberg.{DeleteFile, FileContent}
import org.apache.iceberg.expressions.Expressions
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.types.{ArrayType, IntegerType, MapType, StringType, StructType}

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
