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

package org.apache.comet.iceberg

import java.lang.reflect.Modifier
import java.util.Collections

import org.scalatest.funsuite.AnyFunSuite

import org.apache.iceberg.BaseMetastoreTableOperations
import org.apache.iceberg.BaseTable
import org.apache.iceberg.DataFiles
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.Schema
import org.apache.iceberg.TableMetadata
import org.apache.iceberg.io.FileIO
import org.apache.iceberg.types.Types

class IcebergReflectionSuite extends AnyFunSuite {

  /** Mimics HiveTableOperations/GlueTableOperations which inherit current(). */
  class StubTableOperations extends BaseMetastoreTableOperations {
    override protected def tableName(): String = "test"
    override def refresh(): TableMetadata = null
    override def io(): FileIO = null
  }

  test("getTableMetadata succeeds when operations class inherits current()") {
    val ops = new StubTableOperations()
    val schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()))
    val expectedMetadata = TableMetadata.newTableMetadata(
      schema,
      PartitionSpec.unpartitioned(),
      "file:///tmp/test-table",
      Collections.emptyMap[String, String]())
    val metadataField = classOf[BaseMetastoreTableOperations]
      .getDeclaredField("currentMetadata")
    metadataField.setAccessible(true)
    metadataField.set(ops, expectedMetadata)
    // current() checks shouldRefresh (default true) and calls refresh() instead of
    // returning currentMetadata. Set to false so current() returns our stubbed metadata.
    val refreshField = classOf[BaseMetastoreTableOperations]
      .getDeclaredField("shouldRefresh")
    refreshField.setAccessible(true)
    refreshField.set(ops, false)

    val table = new BaseTable(ops, "test-table")
    val metadata = IcebergReflection.getTableMetadata(table)
    assert(metadata.isDefined)
    assert(metadata.get.isInstanceOf[TableMetadata])
  }

  test("findMethod resolves a method once and returns the cached instance") {
    val first = IcebergReflection.findMethod(classOf[Schema], "columns")
    val second = IcebergReflection.findMethod(classOf[Schema], "columns")
    assert(first.isDefined)
    assert(first.get.getName == "columns")
    // Class.getMethod hands back a fresh copy per call; the cache must not.
    assert(first.get eq second.get)
  }

  test("an absent method is a cached miss, and getMethod still throws for it") {
    assert(IcebergReflection.findMethod(classOf[Schema], "noSuchAccessor").isEmpty)
    assert(IcebergReflection.findMethod(classOf[Schema], "noSuchAccessor").isEmpty)
    assertThrows[NoSuchMethodException] {
      IcebergReflection.getMethod(classOf[Schema], "noSuchAccessor")
    }
  }

  test("findMethod distinguishes overloads by parameter type") {
    val byId = IcebergReflection.findMethod(classOf[Schema], "findField", classOf[Int])
    val byName = IcebergReflection.findMethod(classOf[Schema], "findField", classOf[String])
    assert(byId.isDefined && byName.isDefined)
    assert(byId.get ne byName.get)

    val schema = new Schema(Types.NestedField.required(7, "id", Types.IntegerType.get()))
    val fieldById = byId.get.invoke(schema, Integer.valueOf(7)).asInstanceOf[Types.NestedField]
    val fieldByName = byName.get.invoke(schema, "id").asInstanceOf[Types.NestedField]
    assert(fieldById.name() == "id")
    assert(fieldByName.fieldId() == 7)
  }

  test("findMethodInHierarchy finds an inherited method and caches it") {
    val first = IcebergReflection.findMethodInHierarchy(classOf[StubTableOperations], "current")
    val second = IcebergReflection.findMethodInHierarchy(classOf[StubTableOperations], "current")
    assert(first.isDefined)
    // current() is declared on BaseMetastoreTableOperations, not on the stub itself.
    assert(first.get.getDeclaringClass == classOf[BaseMetastoreTableOperations])
    assert(first.get eq second.get)
    assert(IcebergReflection.findMethodInHierarchy(classOf[StubTableOperations], "nope").isEmpty)
  }

  test("extractFileLocation reads location() when the class has one") {
    val file = new LocationFile("s3://bucket/data/f.parquet")
    assert(
      IcebergReflection.extractFileLocation(classOf[LocationFile], file) ==
        Some("s3://bucket/data/f.parquet"))
  }

  test("extractFileLocation falls back to path() on Iceberg versions without location()") {
    val file = new PathOnlyFile("s3://bucket/data/f.parquet")
    // Called twice: the second call reads the cached "location() is absent" answer.
    assert(
      IcebergReflection.extractFileLocation(classOf[PathOnlyFile], file) ==
        Some("s3://bucket/data/f.parquet"))
    assert(
      IcebergReflection.extractFileLocation(classOf[PathOnlyFile], file) ==
        Some("s3://bucket/data/f.parquet"))
  }

  test("extractFileLocation returns None when the class exposes neither accessor") {
    assert(IcebergReflection.extractFileLocation(classOf[Object], new Object).isEmpty)
  }

  test("extractFileLocation propagates a genuine invoke failure instead of returning None") {
    val file = new ThrowingLocationFile
    val ex = intercept[java.lang.reflect.InvocationTargetException] {
      IcebergReflection.extractFileLocation(classOf[ThrowingLocationFile], file)
    }
    assert(ex.getCause.getMessage == "boom")
  }

  test("getFileFormat reads format() when declared") {
    val file = new FormatFile("PARQUET")
    assert(IcebergReflection.getFileFormat(classOf[FormatFile], file) == Some("PARQUET"))
  }

  test("getFileFormat returns None when format() is not declared") {
    assert(IcebergReflection.getFileFormat(classOf[Object], new Object).isEmpty)
  }

  test("getFileFormat propagates a genuine invoke failure instead of returning None") {
    val file = new ThrowingFormatFile
    val ex = intercept[java.lang.reflect.InvocationTargetException] {
      IcebergReflection.getFileFormat(classOf[ThrowingFormatFile], file)
    }
    assert(ex.getCause.getMessage == "boom")
  }

  test("getEqualityFieldIds reads declared equality field ids") {
    val ids = java.util.List.of(Integer.valueOf(3), Integer.valueOf(5))
    val file = new EqualityIdsFile(ids)
    assert(IcebergReflection.getEqualityFieldIds(classOf[EqualityIdsFile], file) == ids)
  }

  test("getEqualityFieldIds treats a null return (position delete) as empty, not a failure") {
    val file = new NullEqualityIdsFile
    assert(IcebergReflection.getEqualityFieldIds(classOf[NullEqualityIdsFile], file).isEmpty)
  }

  test("getEqualityFieldIds returns empty when equalityFieldIds() is not declared") {
    assert(IcebergReflection.getEqualityFieldIds(classOf[Object], new Object).isEmpty)
  }

  test("getEqualityFieldIds propagates a genuine invoke failure instead of returning empty") {
    val file = new ThrowingEqualityIdsFile
    val ex = intercept[java.lang.reflect.InvocationTargetException] {
      IcebergReflection.getEqualityFieldIds(classOf[ThrowingEqualityIdsFile], file)
    }
    assert(ex.getCause.getMessage == "boom")
  }

  test("a resolved method has access checks suppressed") {
    // Iceberg's concrete file impls are package-private (a built DataFile is a GenericDataFile,
    // and its accessors are declared on the equally package-private BaseFile), so an accessor
    // resolved on one is not invocable from Comet's package until setAccessible has run. The
    // modifier assertions keep the test from going vacuous if Iceberg ever makes them public.
    val file = DataFiles
      .builder(PartitionSpec.unpartitioned())
      .withPath("/tmp/data/f.parquet")
      .withFileSizeInBytes(10)
      .withRecordCount(1)
      .withFormat("PARQUET")
      .build()
    assert(!Modifier.isPublic(file.getClass.getModifiers))

    val method = IcebergReflection.findMethod(file.getClass, "path")
    assert(method.isDefined)
    assert(!Modifier.isPublic(method.get.getDeclaringClass.getModifiers))
    // Without makeAccessible this invoke throws IllegalAccessException.
    assert(method.get.invoke(file).toString == "/tmp/data/f.parquet")
  }

  /** Mimics a table whose operations installed the stock plaintext manager. */
  class PlaintextEncryptionTable {
    def encryption(): AnyRef =
      org.apache.iceberg.encryption.PlaintextEncryptionManager.instance()
  }

  /** Mimics a table whose (possibly custom) operations installed a real encryption manager. */
  class CustomEncryptionTable {
    def encryption(): AnyRef = new Object
  }

  class NoEncryptionMethodTable

  test("getEncryptionManager resolves the manager the table actually installed") {
    val plaintext = IcebergReflection.getEncryptionManager(new PlaintextEncryptionTable)
    assert(
      plaintext.exists(
        _.getClass.getName == "org.apache.iceberg.encryption.PlaintextEncryptionManager"))

    val custom = IcebergReflection.getEncryptionManager(new CustomEncryptionTable)
    assert(custom.isDefined)
    assert(
      custom.get.getClass.getName != "org.apache.iceberg.encryption.PlaintextEncryptionManager")
  }

  test("getEncryptionManager returns None when encryption() cannot be resolved") {
    // The write gate treats None as fail-closed, so a table type without the accessor (or a
    // future rename) declines the native write rather than assuming plaintext.
    assert(IcebergReflection.getEncryptionManager(new NoEncryptionMethodTable).isEmpty)
  }

  test("executor-side reflection surface resolves against the linked Iceberg") {
    // The eligibility gate declines a native write when any class, method, or constructor used
    // by the executor-side commit-message assembly fails to resolve (it would otherwise be a
    // task failure after data files were already written). Asserting the probe is green here
    // means an Iceberg version bump that moves part of that surface fails this test loudly
    // instead of silently falling every native write back to the JVM writer.
    assert(
      IcebergReflection.executorReflectionUnresolved.isEmpty,
      IcebergReflection.executorReflectionUnresolved)
  }

  /** Mimics a newer Iceberg ContentFile, which exposes location(). */
  class LocationFile(loc: String) {
    def location(): String = loc
  }

  /** Mimics Iceberg before 1.7, where ContentFile only exposed path(): CharSequence. */
  class PathOnlyFile(p: String) {
    def path(): CharSequence = p
  }

  /** location() is declared (not a version difference) but the call itself fails. */
  class ThrowingLocationFile {
    def location(): String = throw new RuntimeException("boom")
  }

  class FormatFile(fmt: String) {
    def format(): String = fmt
  }

  /** format() is declared but the call itself fails. */
  class ThrowingFormatFile {
    def format(): String = throw new RuntimeException("boom")
  }

  class EqualityIdsFile(ids: java.util.List[Integer]) {
    def equalityFieldIds(): java.util.List[Integer] = ids
  }

  /** Mimics a position-delete file: the accessor is declared and returns null, not a failure. */
  class NullEqualityIdsFile {
    def equalityFieldIds(): java.util.List[Integer] = null
  }

  /** equalityFieldIds() is declared but the call itself fails. */
  class ThrowingEqualityIdsFile {
    def equalityFieldIds(): java.util.List[Integer] = throw new RuntimeException("boom")
  }
}
