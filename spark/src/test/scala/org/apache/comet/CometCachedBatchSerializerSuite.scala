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
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.comet.{CometCacheColumnStats, CometCachedBatch, CometCachedBatchSerializer}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import org.apache.comet.CometConf

class CometCachedBatchSerializerSuite extends CometTestBase {

  test("stats row has 5 fields per column in cachedAttributes order") {
    val a = AttributeReference("a", IntegerType, nullable = true)()
    val b = AttributeReference("b", StringType, nullable = true)()
    val acc = new CometCacheColumnStats(Seq(a, b))
    // column 0: values 5, null, 3 ; column 1: "y", "a", null
    acc.update(0, IntegerType, isNull = false, 5)
    acc.update(0, IntegerType, isNull = true, null)
    acc.update(0, IntegerType, isNull = false, 3)
    acc.update(1, StringType, isNull = false, UTF8String.fromString("y"))
    acc.update(1, StringType, isNull = false, UTF8String.fromString("a"))
    acc.update(1, StringType, isNull = true, null)
    acc.setRowCount(3)
    val stats = acc.toInternalRow

    assert(stats.numFields == 10) // 5 fields * 2 columns
    // column 0: [lower=3, upper=5, nullCount=1, count=3, sizeInBytes=0]
    assert(stats.getInt(0) == 3)
    assert(stats.getInt(1) == 5)
    assert(stats.getInt(2) == 1)
    assert(stats.getInt(3) == 3)
    // column 1: [lower="a", upper="y", nullCount=1, count=3, sizeInBytes=0]
    assert(stats.getUTF8String(5) == UTF8String.fromString("a"))
    assert(stats.getUTF8String(6) == UTF8String.fromString("y"))
    assert(stats.getInt(7) == 1)
    assert(stats.getInt(8) == 3)
    // sizeInBytes stat slots (positions 4 and 9) are 0L; they are not used by buildFilter
    assert(stats.getLong(4) == 0L)
    assert(stats.getLong(9) == 0L)
    // CometCachedBatch.sizeInBytes reflects the IPC byte length
    val cb = CometCachedBatch(numRows = 3, bytes = Array[Byte](1, 2, 3, 4, 5), stats = stats)
    assert(cb.sizeInBytes == 5L)
    assert(cb.numRows == 3)
  }

  test("supportsColumnarOutput: true for flat supported schema, delegated for nested") {
    val ser = new CometCachedBatchSerializer
    val flat = StructType(Seq(StructField("a", IntegerType), StructField("b", StringType)))
    val nested = StructType(Seq(StructField("a", ArrayType(IntegerType))))
    assert(ser.supportsColumnarOutput(flat))
    // nested delegates to DefaultCachedBatchSerializer, which does not support columnar output
    assert(!ser.supportsColumnarOutput(nested))
  }

  test("build path produces one CometCachedBatch per Arrow batch with stats") {
    withSQLConf(CometConf.COMET_BATCH_SIZE.key -> "100") {
      val ser = new CometCachedBatchSerializer
      // coalesce(1) makes the batch chunking deterministic: 250 rows / 100 = 3 batches
      val df = spark.range(250).coalesce(1).selectExpr("id", "cast(id as string) as s")
      val attrs = df.queryExecution.analyzed.output
      val rdd = df.queryExecution.toRdd
      val cached = ser
        .convertInternalRowToCachedBatch(
          rdd,
          attrs,
          org.apache.spark.storage.StorageLevel.MEMORY_ONLY,
          spark.sessionState.conf)
        .collect()
      assert(cached.length == 3)
      assert(cached.forall(_.isInstanceOf[CometCachedBatch]))
      assert(cached.map(_.numRows).sum == 250)
      cached.foreach { b =>
        assert(b.sizeInBytes > 0)
        assert(b.asInstanceOf[CometCachedBatch].stats.numFields == attrs.length * 5)
      }
      // column 0 is the bigint id; verify real (non-null) stats were computed
      val statRows = cached.map(_.asInstanceOf[CometCachedBatch].stats)
      // lowerBound of col 0 lives at field 0 (LongType); min across batches must be 0
      assert(statRows.map(_.getLong(0)).min == 0L)
      // nullCount of col 0 lives at field 2; range() has no nulls
      assert(statRows.forall(_.getInt(2) == 0))
    }
  }

  test("round-trip: build then decode all columns matches input") {
    withSQLConf(CometConf.COMET_BATCH_SIZE.key -> "64") {
      val ser = new CometCachedBatchSerializer
      val df = spark.range(200).coalesce(1).selectExpr("id", "cast(id * 2 as string) as s")
      val attrs = df.queryExecution.analyzed.output
      val cached = ser.convertInternalRowToCachedBatch(
        df.queryExecution.toRdd,
        attrs,
        org.apache.spark.storage.StorageLevel.MEMORY_ONLY,
        spark.sessionState.conf)
      val decodedRows = ser
        .convertCachedBatchToInternalRow(cached, attrs, attrs, spark.sessionState.conf)
        .map(r => (r.getLong(0), r.getUTF8String(1).toString))
        .collect()
        .toSet
      val expected = (0 until 200).map(i => (i.toLong, (i * 2).toString)).toSet
      assert(decodedRows == expected)
    }
  }

  test("read path prunes to selected columns") {
    withSQLConf(CometConf.COMET_BATCH_SIZE.key -> "64") {
      val ser = new CometCachedBatchSerializer
      val df = spark.range(200).coalesce(1).selectExpr("id", "cast(id * 2 as string) as s")
      val attrs = df.queryExecution.analyzed.output
      val cached = ser.convertInternalRowToCachedBatch(
        df.queryExecution.toRdd,
        attrs,
        org.apache.spark.storage.StorageLevel.MEMORY_ONLY,
        spark.sessionState.conf)
      // select only the string column (index 1)
      val onlyS = Seq(attrs(1))
      val pruned = ser
        .convertCachedBatchToInternalRow(cached, attrs, onlyS, spark.sessionState.conf)
        .map(_.getUTF8String(0).toString)
        .collect()
        .toSet
      assert(pruned == (0 until 200).map(i => (i * 2).toString).toSet)
    }
  }
}
