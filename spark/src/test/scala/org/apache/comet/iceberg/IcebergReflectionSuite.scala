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

import java.util.Collections

import org.scalatest.funsuite.AnyFunSuite

import org.apache.iceberg.BaseMetastoreTableOperations
import org.apache.iceberg.BaseTable
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
      org.apache.iceberg.PartitionSpec.unpartitioned(),
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

  test("findMethod caches the absence of a method") {
    val first = IcebergReflection.findMethod(classOf[Schema], "noSuchAccessor")
    val second = IcebergReflection.findMethod(classOf[Schema], "noSuchAccessor")
    assert(first.isEmpty)
    assert(second.isEmpty)
  }

  test("getMethod throws NoSuchMethodException for an absent method") {
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
    // Repeated so the cached "location() is absent" answer is exercised, not just recorded.
    (1 to 3).foreach { _ =>
      assert(
        IcebergReflection.extractFileLocation(classOf[PathOnlyFile], file) ==
          Some("s3://bucket/data/f.parquet"))
    }
  }

  test("extractFileLocation returns None when the class exposes neither accessor") {
    assert(IcebergReflection.extractFileLocation(classOf[Object], new Object).isEmpty)
  }

  test("findAccessibleMethod resolves and caches a method with access checks suppressed") {
    // Iceberg's bound terms are instances of package-private classes, so the accessors Comet
    // resolves on them have to be marked accessible before they can be invoked.
    val first = IcebergReflection.findAccessibleMethod(classOf[HiddenTransform], "transform")
    val second = IcebergReflection.findAccessibleMethod(classOf[HiddenTransform], "transform")
    assert(first.isDefined)
    assert(first.get eq second.get)
    assert(first.get.invoke(new HiddenTransform).toString == "identity")
    assert(IcebergReflection.findAccessibleMethod(classOf[HiddenTransform], "nope").isEmpty)
  }

  /** Mimics a newer Iceberg ContentFile, which exposes location(). */
  class LocationFile(loc: String) {
    def location(): String = loc
  }

  /** Mimics Iceberg before 1.7, where ContentFile only exposed path(): CharSequence. */
  class PathOnlyFile(p: String) {
    def path(): CharSequence = p
  }

  /** Mimics an Iceberg bound term carrying a transform(). */
  class HiddenTransform {
    def transform(): String = "identity"
  }
}
