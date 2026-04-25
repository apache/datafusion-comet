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
}
