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

import org.scalatest.funsuite.AnyFunSuite

class ReflectionCacheSuite extends AnyFunSuite {

  private def icebergAvailable: Boolean = {
    try {
      Class.forName("org.apache.iceberg.ContentScanTask")
      true
    } catch {
      case _: ClassNotFoundException => false
    }
  }

  test("ReflectionCache.create() loads all Iceberg classes") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    val cache = ReflectionCache.create()

    assert(cache.contentScanTaskClass != null)
    assert(cache.fileScanTaskClass != null)
    assert(cache.contentFileClass != null)
    assert(cache.deleteFileClass != null)
    assert(cache.schemaParserClass != null)
    assert(cache.schemaClass != null)
    assert(cache.partitionSpecParserClass != null)
    assert(cache.partitionSpecClass != null)
    assert(cache.structLikeClass != null)

    assert(cache.contentScanTaskClass.getName == IcebergReflection.ClassNames.CONTENT_SCAN_TASK)
    assert(cache.fileScanTaskClass.getName == IcebergReflection.ClassNames.FILE_SCAN_TASK)
    assert(cache.contentFileClass.getName == IcebergReflection.ClassNames.CONTENT_FILE)
    assert(cache.deleteFileClass.getName == IcebergReflection.ClassNames.DELETE_FILE)
    assert(cache.schemaClass.getName == IcebergReflection.ClassNames.SCHEMA)
    assert(cache.partitionSpecClass.getName == IcebergReflection.ClassNames.PARTITION_SPEC)
    assert(cache.structLikeClass.getName == IcebergReflection.ClassNames.STRUCT_LIKE)
  }

  test("ReflectionCache.create() resolves all methods") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    val cache = ReflectionCache.create()

    assert(cache.fileMethod != null)
    assert(cache.startMethod != null)
    assert(cache.lengthMethod != null)
    assert(cache.partitionMethod != null)
    assert(cache.residualMethod != null)
    assert(cache.taskSchemaMethod != null)
    assert(cache.deletesMethod != null)
    assert(cache.specMethod != null)
    assert(cache.schemaToJsonMethod != null)
    assert(cache.specToJsonMethod != null)
    assert(cache.deleteContentMethod != null)
    assert(cache.deleteSpecIdMethod != null)
    assert(cache.deleteEqualityIdsMethod != null)

    assert(cache.fileMethod.getName == "file")
    assert(cache.startMethod.getName == "start")
    assert(cache.lengthMethod.getName == "length")
    assert(cache.partitionMethod.getName == "partition")
    assert(cache.residualMethod.getName == "residual")
    assert(cache.taskSchemaMethod.getName == "schema")
    assert(cache.deletesMethod.getName == "deletes")
    assert(cache.specMethod.getName == "spec")
    assert(cache.schemaToJsonMethod.getName == "toJson")
    assert(cache.specToJsonMethod.getName == "toJson")
    assert(cache.deleteContentMethod.getName == "content")
    assert(cache.deleteSpecIdMethod.getName == "specId")
    assert(cache.deleteEqualityIdsMethod.getName == "equalityFieldIds")
  }

  test("ReflectionCache is reusable across multiple calls") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    val cache = ReflectionCache.create()

    for (_ <- 1 to 10) {
      assert(cache.contentScanTaskClass != null)
      assert(cache.fileMethod != null)
      assert(cache.schemaToJsonMethod != null)
    }
  }

  test("ReflectionCache schemaToJsonMethod is accessible") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    val cache = ReflectionCache.create()

    assert(cache.schemaToJsonMethod.isAccessible)
  }

  test("Multiple ReflectionCache instances are independent") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    val cache1 = ReflectionCache.create()
    val cache2 = ReflectionCache.create()

    assert(cache1.contentScanTaskClass != null)
    assert(cache2.contentScanTaskClass != null)
    assert(cache1.contentScanTaskClass eq cache2.contentScanTaskClass)
    assert(cache1.fileMethod.getName == cache2.fileMethod.getName)
  }
}
