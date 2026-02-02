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

package org.apache.comet.exec

import java.sql.Date
import java.sql.Timestamp

import scala.util.Random

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.sql.{CometTestBase, Row}
import org.apache.spark.sql.comet.CometNativeColumnarToRowExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.types._

import org.apache.comet.{CometConf, NativeColumnarToRowConverter}
import org.apache.comet.testing.{DataGenOptions, FuzzDataGenerator, SchemaGenOptions}

/**
 * Test suite for native columnar to row conversion.
 *
 * These tests verify that CometNativeColumnarToRowExec produces correct results for all supported
 * data types.
 */
class CometNativeColumnarToRowSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      withSQLConf(
        CometConf.COMET_NATIVE_COLUMNAR_TO_ROW_ENABLED.key -> "true",
        CometConf.COMET_NATIVE_SCAN_IMPL.key -> CometConf.SCAN_AUTO) {
        testFun
      }
    }
  }

  /**
   * Helper to verify that CometNativeColumnarToRowExec is present in the plan.
   */
  private def assertNativeC2RPresent(df: org.apache.spark.sql.DataFrame): Unit = {
    val plan = stripAQEPlan(df.queryExecution.executedPlan)
    val nativeC2R = plan.collect { case c: CometNativeColumnarToRowExec => c }
    assert(
      nativeC2R.nonEmpty,
      s"Expected CometNativeColumnarToRowExec in plan but found none.\nPlan: $plan")
  }

  test("primitive types: boolean, byte, short, int, long") {
    val data = (0 until 100).map { i =>
      (i % 2 == 0, i.toByte, i.toShort, i, i.toLong)
    }
    withParquetTable(data, "primitives") {
      // Force row output by using a UDF or collect
      val df = sql("SELECT * FROM primitives")
      assertNativeC2RPresent(df)
      checkSparkAnswer(df)
    }
  }

  test("primitive types: float, double") {
    val data = (0 until 100).map { i =>
      (i.toFloat / 10.0f, i.toDouble / 10.0)
    }
    withParquetTable(data, "floats") {
      val df = sql("SELECT * FROM floats")
      assertNativeC2RPresent(df)
      checkSparkAnswer(df)
    }
  }

  test("primitive types with nulls") {
    val data = (0 until 100).map { i =>
      val isNull = i % 5 == 0
      (
        if (isNull) null else (i % 2 == 0),
        if (isNull) null else i.toByte,
        if (isNull) null else i.toShort,
        if (isNull) null else i,
        if (isNull) null else i.toLong,
        if (isNull) null else i.toFloat,
        if (isNull) null else i.toDouble)
    }
    val df = spark
      .createDataFrame(
        spark.sparkContext.parallelize(data.map(Row.fromTuple(_))),
        StructType(
          Seq(
            StructField("bool", BooleanType, nullable = true),
            StructField("byte", ByteType, nullable = true),
            StructField("short", ShortType, nullable = true),
            StructField("int", IntegerType, nullable = true),
            StructField("long", LongType, nullable = true),
            StructField("float", FloatType, nullable = true),
            StructField("double", DoubleType, nullable = true))))
    withParquetDataFrame(df) { parquetDf =>
      assertNativeC2RPresent(parquetDf)
      checkSparkAnswer(parquetDf)
    }
  }

  test("string type") {
    val data = (0 until 100).map { i =>
      (i, s"string_value_$i", if (i % 10 == 0) null else s"nullable_$i")
    }
    val df = spark
      .createDataFrame(
        spark.sparkContext.parallelize(data.map(Row.fromTuple(_))),
        StructType(
          Seq(
            StructField("id", IntegerType),
            StructField("str", StringType),
            StructField("nullable_str", StringType, nullable = true))))
    withParquetDataFrame(df) { parquetDf =>
      assertNativeC2RPresent(parquetDf)
      checkSparkAnswer(parquetDf)
    }
  }

  test("string type with various lengths") {
    val random = new Random(42)
    val data = (0 until 100).map { i =>
      val len = random.nextInt(1000)
      (i, random.alphanumeric.take(len).mkString)
    }
    withParquetTable(data, "strings") {
      val df = sql("SELECT * FROM strings")
      assertNativeC2RPresent(df)
      checkSparkAnswer(df)
    }
  }

  test("binary type") {
    val data = (0 until 100).map { i =>
      (i, s"binary_$i".getBytes)
    }
    val df = spark
      .createDataFrame(
        spark.sparkContext.parallelize(data.map { case (id, bytes) => Row(id, bytes) }),
        StructType(
          Seq(StructField("id", IntegerType), StructField("bin", BinaryType, nullable = true))))
    withParquetDataFrame(df) { parquetDf =>
      assertNativeC2RPresent(parquetDf)
      checkSparkAnswer(parquetDf)
    }
  }

  test("date type") {
    val baseDate = Date.valueOf("2024-01-01")
    val data = (0 until 100).map { i =>
      (i, new Date(baseDate.getTime + i * 24 * 60 * 60 * 1000L))
    }
    withParquetTable(data, "dates") {
      val df = sql("SELECT * FROM dates")
      assertNativeC2RPresent(df)
      checkSparkAnswer(df)
    }
  }

  test("timestamp type") {
    val baseTs = Timestamp.valueOf("2024-01-01 00:00:00")
    val data = (0 until 100).map { i =>
      (i, new Timestamp(baseTs.getTime + i * 1000L))
    }
    withParquetTable(data, "timestamps") {
      val df = sql("SELECT * FROM timestamps")
      assertNativeC2RPresent(df)
      checkSparkAnswer(df)
    }
  }

  test("decimal type - inline precision (precision <= 18)") {
    val data = (0 until 100).map { i =>
      (i, BigDecimal(i * 100 + i) / 100)
    }
    val df = spark
      .createDataFrame(
        spark.sparkContext.parallelize(data.map { case (id, dec) =>
          Row(id, dec.bigDecimal)
        }),
        StructType(
          Seq(
            StructField("id", IntegerType),
            StructField("dec", DecimalType(10, 2), nullable = true))))
    withParquetDataFrame(df) { parquetDf =>
      assertNativeC2RPresent(parquetDf)
      checkSparkAnswer(parquetDf)
    }
  }

  test("decimal type - variable length precision (precision > 18)") {
    val data = (0 until 100).map { i =>
      (i, BigDecimal(s"12345678901234567890.$i"))
    }
    val df = spark
      .createDataFrame(
        spark.sparkContext.parallelize(data.map { case (id, dec) =>
          Row(id, dec.bigDecimal)
        }),
        StructType(
          Seq(
            StructField("id", IntegerType),
            StructField("dec", DecimalType(30, 5), nullable = true))))
    withParquetDataFrame(df) { parquetDf =>
      assertNativeC2RPresent(parquetDf)
      checkSparkAnswer(parquetDf)
    }
  }

  test("struct type") {
    val data = (0 until 100).map { i =>
      (i, (i * 2, s"nested_$i"))
    }
    withParquetTable(data, "structs") {
      val df = sql("SELECT * FROM structs")
      assertNativeC2RPresent(df)
      checkSparkAnswer(df)
    }
  }

  test("array type") {
    val data = (0 until 100).map { i =>
      (i, (0 to i % 10).toArray)
    }
    withParquetTable(data, "arrays") {
      val df = sql("SELECT * FROM arrays")
      assertNativeC2RPresent(df)
      checkSparkAnswer(df)
    }
  }

  test("array of strings") {
    val data = (0 until 100).map { i =>
      (i, (0 to i % 5).map(j => s"elem_${i}_$j").toArray)
    }
    withParquetTable(data, "string_arrays") {
      val df = sql("SELECT * FROM string_arrays")
      assertNativeC2RPresent(df)
      checkSparkAnswer(df)
    }
  }

  test("map type") {
    val data = (0 until 100).map { i =>
      (i, (0 to i % 5).map(j => (s"key_$j", j * 10)).toMap)
    }
    withParquetTable(data, "maps") {
      val df = sql("SELECT * FROM maps")
      assertNativeC2RPresent(df)
      checkSparkAnswer(df)
    }
  }

  test("nested struct in array") {
    val data = (0 until 100).map { i =>
      (i, (0 to i % 5).map(j => (j, s"nested_$j")).toArray)
    }
    withParquetTable(data, "nested_structs") {
      val df = sql("SELECT * FROM nested_structs")
      assertNativeC2RPresent(df)
      checkSparkAnswer(df)
    }
  }

  test("deeply nested: array of arrays") {
    val data = (0 until 100).map { i =>
      (i, (0 to i % 3).map(j => (0 to j).toArray).toArray)
    }
    withParquetTable(data, "nested_arrays") {
      val df = sql("SELECT * FROM nested_arrays")
      assertNativeC2RPresent(df)
      checkSparkAnswer(df)
    }
  }

  test("deeply nested: map with array values") {
    val data = (0 until 100).map { i =>
      (i, (0 to i % 3).map(j => (s"key_$j", (0 to j).toArray)).toMap)
    }
    withParquetTable(data, "map_array_values") {
      val df = sql("SELECT * FROM map_array_values")
      assertNativeC2RPresent(df)
      checkSparkAnswer(df)
    }
  }

  test("deeply nested: struct containing array of maps") {
    val data = (0 until 100).map { i =>
      (
        i,
        ((0 to i % 3).map(j => (0 to j % 2).map(k => (s"k$k", k * 10)).toMap).toArray, s"str_$i"))
    }
    withParquetTable(data, "struct_array_maps") {
      val df = sql("SELECT * FROM struct_array_maps")
      assertNativeC2RPresent(df)
      checkSparkAnswer(df)
    }
  }

  test("all null values") {
    val df = spark
      .createDataFrame(
        spark.sparkContext.parallelize((0 until 100).map(_ => Row(null, null, null))),
        StructType(
          Seq(
            StructField("int_col", IntegerType, nullable = true),
            StructField("str_col", StringType, nullable = true),
            StructField("double_col", DoubleType, nullable = true))))
    withParquetDataFrame(df) { parquetDf =>
      assertNativeC2RPresent(parquetDf)
      checkSparkAnswer(parquetDf)
    }
  }

  test("empty batch") {
    val df = spark
      .createDataFrame(
        spark.sparkContext.parallelize(Seq.empty[Row]),
        StructType(Seq(StructField("int_col", IntegerType), StructField("str_col", StringType))))
    withParquetDataFrame(df) { parquetDf =>
      assertNativeC2RPresent(parquetDf)
      checkSparkAnswer(parquetDf)
    }
  }

  test("mixed types - comprehensive") {
    val baseDate = Date.valueOf("2024-01-01")
    val baseTs = Timestamp.valueOf("2024-01-01 00:00:00")

    val data = (0 until 100).map { i =>
      val isNull = i % 7 == 0
      Row(
        i,
        if (isNull) null else (i % 2 == 0),
        if (isNull) null else i.toByte,
        if (isNull) null else i.toShort,
        if (isNull) null else i.toLong,
        if (isNull) null else i.toFloat,
        if (isNull) null else i.toDouble,
        if (isNull) null else s"string_$i",
        if (isNull) null else new Date(baseDate.getTime + i * 24 * 60 * 60 * 1000L),
        if (isNull) null else new Timestamp(baseTs.getTime + i * 1000L),
        if (isNull) null else BigDecimal(i * 100 + i, 2).bigDecimal)
    }

    val schema = StructType(
      Seq(
        StructField("id", IntegerType),
        StructField("bool_col", BooleanType, nullable = true),
        StructField("byte_col", ByteType, nullable = true),
        StructField("short_col", ShortType, nullable = true),
        StructField("long_col", LongType, nullable = true),
        StructField("float_col", FloatType, nullable = true),
        StructField("double_col", DoubleType, nullable = true),
        StructField("string_col", StringType, nullable = true),
        StructField("date_col", DateType, nullable = true),
        StructField("timestamp_col", TimestampType, nullable = true),
        StructField("decimal_col", DecimalType(10, 2), nullable = true)))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    withParquetDataFrame(df) { parquetDf =>
      assertNativeC2RPresent(parquetDf)
      checkSparkAnswer(parquetDf)
    }
  }

  test("large batch") {
    val data = (0 until 10000).map { i =>
      (i, s"string_value_$i", i.toDouble)
    }
    withParquetTable(data, "large_batch") {
      val df = sql("SELECT * FROM large_batch")
      assertNativeC2RPresent(df)
      checkSparkAnswer(df)
    }
  }

  test("disabled by default") {
    withSQLConf(CometConf.COMET_NATIVE_COLUMNAR_TO_ROW_ENABLED.key -> "false") {
      val data = (0 until 10).map(i => (i, s"value_$i"))
      withParquetTable(data, "test_disabled") {
        val df = sql("SELECT * FROM test_disabled")
        val plan = stripAQEPlan(df.queryExecution.executedPlan)
        val nativeC2R = plan.collect { case c: CometNativeColumnarToRowExec => c }
        assert(
          nativeC2R.isEmpty,
          s"Expected no CometNativeColumnarToRowExec when disabled.\nPlan: $plan")
        checkSparkAnswer(df)
      }
    }
  }

  test("fuzz test with nested types") {
    val random = new Random(42)
    val schemaGenOptions =
      SchemaGenOptions(generateArray = true, generateStruct = true, generateMap = true)
    val dataGenOptions =
      DataGenOptions(generateNegativeZero = false, generateNaN = false, generateInfinity = false)

    // Use generateSchema which creates various nested types including arrays, structs, and maps.
    // Not all generated types may be supported by native C2R, so we just verify correctness.
    val schema = FuzzDataGenerator.generateSchema(schemaGenOptions)
    val df = FuzzDataGenerator.generateDataFrame(random, spark, schema, 100, dataGenOptions)

    withParquetDataFrame(df) { parquetDf =>
      checkSparkAnswer(parquetDf)
    }
  }

  test("fuzz test with generateNestedSchema") {
    val random = new Random(42)

    // Use only primitive types supported by native C2R (excludes TimestampNTZType)
    val supportedPrimitiveTypes: Seq[DataType] = Seq(
      DataTypes.BooleanType,
      DataTypes.ByteType,
      DataTypes.ShortType,
      DataTypes.IntegerType,
      DataTypes.LongType,
      DataTypes.FloatType,
      DataTypes.DoubleType,
      DataTypes.createDecimalType(10, 2),
      DataTypes.DateType,
      DataTypes.TimestampType,
      DataTypes.StringType,
      DataTypes.BinaryType)

    val schemaGenOptions = SchemaGenOptions(
      generateArray = true,
      generateStruct = true,
      generateMap = true,
      primitiveTypes = supportedPrimitiveTypes)
    val dataGenOptions =
      DataGenOptions(generateNegativeZero = false, generateNaN = false, generateInfinity = false)

    // Test with multiple random deeply nested schemas
    for (iteration <- 1 to 3) {
      val schema = FuzzDataGenerator.generateNestedSchema(
        random,
        numCols = 5,
        minDepth = 1,
        maxDepth = 3,
        options = schemaGenOptions)

      val df = FuzzDataGenerator.generateDataFrame(random, spark, schema, 100, dataGenOptions)

      withParquetDataFrame(df) { parquetDf =>
        assertNativeC2RPresent(parquetDf)
        checkSparkAnswer(parquetDf)
      }
    }
  }

  // Regression test for https://github.com/apache/datafusion-comet/issues/3308
  // Native columnar-to-row returns UnsafeRow pointing into a Rust-owned buffer that is
  // cleared/reused on each convert() call. This test directly exercises the converter:
  // it converts multiple batches and holds row references from earlier batches, then
  // verifies they still contain correct data. Without a fix (e.g., copying rows),
  // rows from earlier batches will contain corrupted data from buffer reuse.
  test("rows from earlier batches are not corrupted by subsequent convert() calls") {
    import org.apache.arrow.memory.RootAllocator
    import org.apache.arrow.vector.{IntVector, VarCharVector, VectorSchemaRoot}
    import org.apache.comet.vector.NativeUtil

    val allocator = new RootAllocator()
    val schema = new StructType().add("id", IntegerType).add("str", StringType)

    // Create multiple small Arrow batches
    val numBatches = 10
    val rowsPerBatch = 5
    val batches = (0 until numBatches).map { batchIdx =>
      val intVector = new IntVector("id", allocator)
      val varcharVector = new VarCharVector("str", allocator)
      intVector.allocateNew(rowsPerBatch)
      varcharVector.allocateNew(rowsPerBatch)

      for (i <- 0 until rowsPerBatch) {
        val globalIdx = batchIdx * rowsPerBatch + i
        intVector.setSafe(i, globalIdx)
        varcharVector.setSafe(i, s"value_$globalIdx".getBytes)
      }
      intVector.setValueCount(rowsPerBatch)
      varcharVector.setValueCount(rowsPerBatch)

      val fields = java.util.Arrays.asList(intVector.getField, varcharVector.getField)
      val vectors = java.util.Arrays.asList(
        intVector.asInstanceOf[org.apache.arrow.vector.FieldVector],
        varcharVector.asInstanceOf[org.apache.arrow.vector.FieldVector])
      val root = new VectorSchemaRoot(fields, vectors, rowsPerBatch)
      NativeUtil.rootAsBatch(root)
    }

    val converter = new NativeColumnarToRowConverter(schema, rowsPerBatch)
    try {
      // Mimic the broadcast path: flatMap across batches, collecting all
      // row references eagerly. The native buffer is reused on each
      // convert() call, so rows from earlier batches point to stale memory.
      // Note: NativeRowIterator reuses a single UnsafeRow object AND the
      // native buffer is reused across convert() calls, so we must read
      // row data lazily through the held references after all batches are
      // converted to detect corruption.
      val allRows = batches.iterator.flatMap { batch =>
        converter.convert(batch)
      }.toArray

      assert(
        allRows.length == numBatches * rowsPerBatch,
        s"Expected ${numBatches * rowsPerBatch} rows, got ${allRows.length}")

      // All entries in allRows are the same UnsafeRow object (reused by the
      // iterator). After all batches are converted, it points to the last
      // row's native memory. Verify that reading through held references
      // produces all expected distinct values. Since the UnsafeRow is reused,
      // all entries will return the same value (the last row), proving the bug.
      val distinctIds = allRows.map(_.getInt(0)).toSet
      val totalRows = numBatches * rowsPerBatch
      assert(
        distinctIds.size == totalRows,
        s"UnsafeRow reuse bug: expected $totalRows distinct row IDs but got " +
          s"${distinctIds.size} (values: ${distinctIds.toSeq.sorted.mkString(", ")}). " +
          "This means rows were not copied and all references point to the same " +
          "reused UnsafeRow object.")
    } finally {
      converter.close()
      // Close all batches to free Arrow memory before closing the allocator
      batches.foreach(_.close())
      allocator.close()
    }
  }

  /**
   * Helper to create a parquet table from a DataFrame and run a function with it.
   */
  private def withParquetDataFrame(df: org.apache.spark.sql.DataFrame)(
      f: org.apache.spark.sql.DataFrame => Unit): Unit = {
    withTempPath { path =>
      df.write.parquet(path.getAbsolutePath)
      spark.read.parquet(path.getAbsolutePath).createOrReplaceTempView("test_table")
      f(sql("SELECT * FROM test_table"))
    }
  }
}
