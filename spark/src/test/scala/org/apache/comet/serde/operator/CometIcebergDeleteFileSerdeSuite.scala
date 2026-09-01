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

import org.apache.iceberg.DeleteFile

import org.apache.comet.iceberg.IcebergReflection

/**
 * Locks in the fail-loud behavior of [[CometIcebergNativeScan.serializeDeleteFile]] required by
 * apache/datafusion-comet#5256: on supported Iceberg versions `content()`, `specId()`, and
 * `equalityFieldIds()` are always declared, so a reflective lookup or invocation failure must
 * propagate rather than fall back to a guessed value. A null `equalityFieldIds()` (a
 * position-delete file) stays a legitimate "no equality keys" result.
 */
class CometIcebergDeleteFileSerdeSuite extends AnyFunSuite {

  private def keyMetadataMethod(clazz: Class[_]) = clazz.getMethod("keyMetadata")

  private def serialize(file: AnyRef) =
    CometIcebergNativeScan.serializeDeleteFile(
      file,
      file.getClass,
      file.getClass,
      keyMetadataMethod(file.getClass))

  test("required Iceberg delete-file accessors are present") {
    Seq("content", "specId", "equalityFieldIds").foreach { accessor =>
      assert(
        IcebergReflection.findMethod(classOf[DeleteFile], accessor).isDefined,
        s"DeleteFile.$accessor must be available for native delete-file serde")
    }
  }

  test("position-delete file: null equalityFieldIds() serializes with no equality ids") {
    val proto = serialize(new PositionDeleteFile)
    assert(proto.getContentType == "POSITION_DELETES")
    assert(proto.getPartitionSpecId == 7)
    assert(proto.getEqualityIdsCount == 0)
    assert(proto.getFilePath == "s3://bucket/pos-delete.parquet")
  }

  test("equality-delete file: declared equalityFieldIds() are serialized") {
    val proto = serialize(new EqualityDeleteFile)
    assert(proto.getContentType == "EQUALITY_DELETES")
    assert(proto.getEqualityIdsCount == 2)
    assert(proto.getEqualityIds(0) == 3)
    assert(proto.getEqualityIds(1) == 5)
  }

  test("equality-delete file: null equalityFieldIds() is fatal") {
    val ex = intercept[IllegalStateException](serialize(new EqualityDeleteFileWithNullIds))
    assert(ex.getMessage ==
      "Iceberg equality delete file 's3://bucket/eq-null-ids.parquet' has no equality field IDs")
  }

  test("equality-delete file: empty equalityFieldIds() is fatal") {
    val ex = intercept[IllegalStateException](serialize(new EqualityDeleteFileWithEmptyIds))
    assert(ex.getMessage ==
      "Iceberg equality delete file 's3://bucket/eq-empty-ids.parquet' has no equality field IDs")
  }

  test("content() invocation failure propagates instead of defaulting to POSITION_DELETES") {
    val ex = intercept[InvocationTargetException](serialize(new ThrowingContentDeleteFile))
    assert(ex.getCause.getMessage == "content boom")
  }

  test("specId() invocation failure propagates instead of defaulting to 0") {
    val ex = intercept[InvocationTargetException](serialize(new ThrowingSpecIdDeleteFile))
    assert(ex.getCause.getMessage == "spec boom")
  }

  test("equalityFieldIds() invocation failure propagates instead of dropping equality ids") {
    val ex = intercept[InvocationTargetException](serialize(new ThrowingEqualityIdsDeleteFile))
    assert(ex.getCause.getMessage == "ids boom")
  }

  test("missing content() accessor is fatal, not a default") {
    assertThrows[NoSuchMethodException](serialize(new NoContentAccessorDeleteFile))
  }

  test("missing equalityFieldIds() accessor is fatal, not an empty list") {
    assertThrows[NoSuchMethodException](serialize(new NoEqualityIdsAccessorDeleteFile))
  }

  test("missing delete-file path accessor is fatal") {
    val ex = intercept[RuntimeException](serialize(new NoPathAccessorDeleteFile))
    assert(ex.getMessage.contains("Neither location() nor path() is declared"))
  }

  // -- Synthetic DeleteFile stubs. Each declares the full accessor set serializeDeleteFile
  //    resolves; only the field under test misbehaves. --

  class PositionDeleteFile {
    def location(): String = "s3://bucket/pos-delete.parquet"
    def content(): String = "POSITION_DELETES"
    def specId(): Int = 7
    def equalityFieldIds(): java.util.List[Integer] = null
    def keyMetadata(): java.nio.ByteBuffer = null
  }

  class EqualityDeleteFile {
    def location(): String = "s3://bucket/eq-delete.parquet"
    def content(): String = "EQUALITY_DELETES"
    def specId(): Int = 0
    def equalityFieldIds(): java.util.List[Integer] =
      java.util.List.of(Integer.valueOf(3), Integer.valueOf(5))
    def keyMetadata(): java.nio.ByteBuffer = null
  }

  class EqualityDeleteFileWithNullIds {
    def location(): String = "s3://bucket/eq-null-ids.parquet"
    def content(): String = "EQUALITY_DELETES"
    def specId(): Int = 0
    def equalityFieldIds(): java.util.List[Integer] = null
    def keyMetadata(): java.nio.ByteBuffer = null
  }

  class EqualityDeleteFileWithEmptyIds {
    def location(): String = "s3://bucket/eq-empty-ids.parquet"
    def content(): String = "EQUALITY_DELETES"
    def specId(): Int = 0
    def equalityFieldIds(): java.util.List[Integer] = java.util.List.of[Integer]()
    def keyMetadata(): java.nio.ByteBuffer = null
  }

  class ThrowingContentDeleteFile {
    def location(): String = "s3://bucket/d.parquet"
    def content(): String = throw new RuntimeException("content boom")
    def specId(): Int = 0
    def equalityFieldIds(): java.util.List[Integer] = null
    def keyMetadata(): java.nio.ByteBuffer = null
  }

  class ThrowingSpecIdDeleteFile {
    def location(): String = "s3://bucket/d.parquet"
    def content(): String = "POSITION_DELETES"
    def specId(): Int = throw new RuntimeException("spec boom")
    def equalityFieldIds(): java.util.List[Integer] = null
    def keyMetadata(): java.nio.ByteBuffer = null
  }

  class ThrowingEqualityIdsDeleteFile {
    def location(): String = "s3://bucket/d.parquet"
    def content(): String = "EQUALITY_DELETES"
    def specId(): Int = 0
    def equalityFieldIds(): java.util.List[Integer] = throw new RuntimeException("ids boom")
    def keyMetadata(): java.nio.ByteBuffer = null
  }

  class NoContentAccessorDeleteFile {
    def location(): String = "s3://bucket/d.parquet"
    def specId(): Int = 0
    def equalityFieldIds(): java.util.List[Integer] = null
    def keyMetadata(): java.nio.ByteBuffer = null
  }

  class NoEqualityIdsAccessorDeleteFile {
    def location(): String = "s3://bucket/d.parquet"
    def content(): String = "EQUALITY_DELETES"
    def specId(): Int = 0
    def keyMetadata(): java.nio.ByteBuffer = null
  }

  class NoPathAccessorDeleteFile {
    def content(): String = "POSITION_DELETES"
    def specId(): Int = 0
    def equalityFieldIds(): java.util.List[Integer] = null
    def keyMetadata(): java.nio.ByteBuffer = null
  }
}
