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

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.arrow.vector.{IntVector, VarBinaryVector, VarCharVector, VectorSchemaRoot}
import org.apache.arrow.vector.complex.StructVector
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.spark.SparkConf
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.comet.{CometNativeColumnarToRowExec, CometNativeScanExec}
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.execution.ColumnarToRowExec
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.types.variant.VariantBuilder
import org.apache.spark.unsafe.types.{UTF8String, VariantVal}

import org.apache.comet.{CometArrowAllocator, CometConf, NativeColumnarToRowConverter}
import org.apache.comet.vector.CometVector

class CometVariantColumnarToRowSuite extends CometTestBase {
  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.adaptive.enabled", "false")
    .set("spark.sql.sources.useV1SourceList", "parquet")
    .set("spark.sql.variant.allowReadingShredded", "true")
    .set("spark.sql.variant.pushVariantIntoScan", "false")
    .set(CometConf.COMET_NATIVE_COLUMNAR_TO_ROW_ENABLED.key, "true")

  test("Variant UnsafeRows preserve bytes, nulls and siblings across batches") {
    val schema = new StructType()
      .add("id", IntegerType)
      .add("v", VariantType)
      .add("tail", StringType)
    // Match the native scan's canonical Arrow storage; generic JVM Variant writes stay disabled.
    val variantField = new Field(
      "v",
      new FieldType(
        true,
        ArrowType.Struct.INSTANCE,
        null,
        Map(
          ArrowType.ExtensionType.EXTENSION_METADATA_KEY_NAME -> "arrow.parquet.variant").asJava),
      Seq(
        Field.notNullable("value", ArrowType.Binary.INSTANCE),
        Field.notNullable("metadata", ArrowType.Binary.INSTANCE)).asJava)
    val arrowSchema = new Schema(
      Seq(
        Utils.toArrowField("id", IntegerType, nullable = true, "UTC"),
        variantField,
        Utils.toArrowField("tail", StringType, nullable = true, "UTC")).asJava)
    val values =
      Seq("{\"a\":1,\"nested\":[true,null]}", "[1,2]", "42", "\"text\"", "null", null, "{}", "[]")
        .map { json =>
          if (json == null) null
          else {
            val parsed = VariantBuilder.parseJson(json, false)
            new VariantVal(parsed.getValue, parsed.getMetadata)
          }
        }
    val allocator = CometArrowAllocator.newChildAllocator("variant-c2r", 0, Long.MaxValue)
    val converter = new NativeColumnarToRowConverter(schema, 3)
    val rows = ArrayBuffer.empty[UnsafeRow]
    try {
      // Include empty batches before and after data, and retain every row across buffer reuse.
      for (batchValues <- Seq(Seq.empty[(VariantVal, Int)]) ++
          values.zipWithIndex.grouped(3).toSeq ++ Seq(Seq.empty[(VariantVal, Int)])) {
        val root = VectorSchemaRoot.create(arrowSchema, allocator)
        try {
          root.allocateNew()
          val variant = root.getVector("v").asInstanceOf[StructVector]
          batchValues.zipWithIndex.foreach { case ((value, id), row) =>
            root.getVector("id").asInstanceOf[IntVector].setSafe(row, id)
            root
              .getVector("tail")
              .asInstanceOf[VarCharVector]
              .setSafe(row, UTF8String.fromString(s"tail-$id").getBytes)
            if (value == null) variant.setNull(row)
            else {
              variant.setIndexDefined(row)
              variant.getChild("value").asInstanceOf[VarBinaryVector].setSafe(row, value.getValue)
              variant
                .getChild("metadata")
                .asInstanceOf[VarBinaryVector]
                .setSafe(row, value.getMetadata)
            }
          }
          root.setRowCount(batchValues.size)
          val columns = root.getFieldVectors.asScala.map { vector =>
            CometVector.getVector(vector, null): ColumnVector
          }.toArray
          rows ++= converter
            .convert(new ColumnarBatch(columns, batchValues.size))
            .map(_.asInstanceOf[UnsafeRow])
        } finally {
          root.close()
        }
      }
    } finally {
      converter.close()
      allocator.close()
    }
    assert(rows.size == values.size)
    val projection = UnsafeProjection.create(schema)
    rows.zip(values.zipWithIndex).foreach { case (row, (value, id)) =>
      assert(row.getInt(0) == id)
      assert(row.getUTF8String(2).toString == s"tail-$id")
      if (value == null) {
        assert(row.isNullAt(1))
        assert(row.getVariant(1) == null)
      } else {
        assert(!row.isNullAt(1))
        assert(row.getVariant(1).getValue.sameElements(value.getValue))
        assert(row.getVariant(1).getMetadata.sameElements(value.getMetadata))
      }
      val expected = projection(InternalRow(id, value, UTF8String.fromString(s"tail-$id")))
      assert(row.getBytes.sameElements(expected.getBytes))
    }
  }

  test("native Variant scans use native columnar to row") {
    for (shredding <- Seq("false", "true")) {
      withSQLConf(
        "spark.sql.variant.writeShredding.enabled" -> shredding,
        "spark.sql.variant.forceShreddingSchemaForTest" -> "a INT") {
        withTempPath { path =>
          withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
            sql("""SELECT id, parse_json(json) AS v, id + 10 AS tail FROM VALUES
              (1, '{"a":1}'), (2, '[1,"text",false]'), (3, '42'),
              (4, 'null'), (5, NULL), (6, '{}'), (7, '[]') AS input(id, json)""")
              .coalesce(1)
              .write
              .parquet(path.getCanonicalPath)
          }
          val df = spark.read.parquet(path.getCanonicalPath)
          val plan = df.queryExecution.executedPlan
          assert(
            collect(plan) { case c: CometNativeColumnarToRowExec => c }.nonEmpty,
            plan.toString)
          assert(collect(plan) { case s: CometNativeScanExec => s }.nonEmpty, plan.toString)
          checkSparkAnswer(df)
          withSQLConf(CometConf.COMET_NATIVE_COLUMNAR_TO_ROW_ENABLED.key -> "false") {
            val fallback = spark.read.parquet(path.getCanonicalPath)
            val fallbackPlan = fallback.queryExecution.executedPlan
            assert(collect(fallbackPlan) { case c: CometNativeColumnarToRowExec => c }.isEmpty)
            assert(collect(fallbackPlan) { case c: ColumnarToRowExec => c }.nonEmpty)
            checkSparkAnswer(fallback)
          }
        }
      }
    }
  }

  test("nested Variant keeps native columnar to row fallback") {
    for (nested <- Seq(
        new StructType().add("v", VariantType),
        ArrayType(VariantType),
        MapType(StringType, VariantType))) {
      assert(!CometNativeColumnarToRowExec.supportsSchema(new StructType().add("nested", nested)))
    }
    val ordinary = new StructType().add("value", BinaryType).add("metadata", BinaryType)
    assert(CometNativeColumnarToRowExec.supportsSchema(new StructType().add("s", ordinary)))
  }
}
