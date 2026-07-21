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

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.types.StructType

import org.apache.comet.serde.operator.partition2Proto
import org.apache.comet.shims.ShimBatchReader

class CometFilePartitionSerdeSuite extends AnyFunSuite {

  test("file modification time is serialized to the native side") {
    val file = ShimBatchReader
      .newPartitionedFile(InternalRow.empty, "file:///tmp/comet/part-0.parquet")
      .copy(start = 0, length = 100, fileSize = 100, modificationTime = 1700000000123L)

    val proto = partition2Proto(FilePartition(0, Array(file)), StructType(Seq.empty))

    assert(proto.getPartitionedFileCount == 1)
    assert(proto.getPartitionedFile(0).getModificationTime == 1700000000123L)
  }
}
