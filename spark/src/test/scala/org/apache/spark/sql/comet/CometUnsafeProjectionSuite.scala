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

package org.apache.spark.sql.comet

import java.sql.Timestamp
import java.util.UUID

import org.apache.spark.sql.{CometTestBase, Row}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, InterpretedUnsafeProjection, UnsafeProjection}
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

class CometUnsafeProjectionSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  /** A schema with every writer GenerateUnsafeProjection nests: structs, arrays and maps. */
  private def schema(nestedName: String = "a"): StructType = StructType(
    Seq(
      StructField("i", IntegerType),
      StructField("s", StringType),
      StructField("d", DecimalType(38, 10)),
      StructField(
        "st",
        StructType(
          Seq(StructField(nestedName, LongType), StructField("b", ArrayType(StringType))))),
      StructField(
        "m",
        MapType(StringType, ArrayType(StructType(Seq(StructField("x", DoubleType)))))),
      StructField("aa", ArrayType(ArrayType(IntegerType))),
      StructField("bin", BinaryType),
      StructField("ts", TimestampType)))

  private def attributes(nestedName: String = "a"): Seq[AttributeReference] =
    schema(nestedName).fields.toSeq.map(f => AttributeReference(f.name, f.dataType, f.nullable)())

  /** Attributes whose layout no earlier call has cached, since a nested name is part of it. */
  private def newLayout(): Seq[AttributeReference] =
    attributes(s"a_${UUID.randomUUID().toString.replace("-", "")}")

  private lazy val rows: Seq[InternalRow] = {
    val toCatalyst = CatalystTypeConverters.createToCatalystConverter(schema())
    Seq(
      Row(
        1,
        "a",
        BigDecimal("1.5"),
        Row(2L, Seq("x", null)),
        Map("k" -> Seq(Row(1.0), null)),
        Seq(Seq(1, null), null),
        Array[Byte](1, 2),
        new Timestamp(0)),
      Row(null, null, null, null, null, null, null, null),
      Row(
        3,
        "字" * 100,
        BigDecimal("-1234567890123456789012345678.1234567890"),
        Row(null, Seq.empty),
        Map.empty,
        Seq.empty,
        Array.emptyByteArray,
        null)).map(toCatalyst(_).asInstanceOf[InternalRow])
  }

  private def withCodegenOnly(confs: (String, String)*)(f: => Unit): Unit =
    withSQLConf((SQLConf.CODEGEN_FACTORY_MODE.key -> "CODEGEN_ONLY") +: confs: _*)(f)

  test("rows match UnsafeProjection.create") {
    // A small threshold splits the field writes into many methods.
    Seq("1024", "64").foreach { threshold =>
      withCodegenOnly(SQLConf.CODEGEN_METHOD_SPLIT_THRESHOLD.key -> threshold) {
        val attrs = newLayout()
        val expected = UnsafeProjection.create(attrs, attrs)
        val actual = CometUnsafeProjection.create(attrs)
        rows.foreach(row => assert(actual(row) == expected(row)))
      }
    }
  }

  test("NO_CODEGEN creates an interpreted projection") {
    withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> "NO_CODEGEN") {
      val attrs = attributes()
      val projection = CometUnsafeProjection.create(attrs)
      assert(projection.isInstanceOf[InterpretedUnsafeProjection])
      val expected = UnsafeProjection.create(attrs, attrs)
      rows.foreach(row => assert(projection(row) == expected(row)))
    }
  }

  test("generates the class once per layout and a new projection per call") {
    withCodegenOnly() {
      val attrs = newLayout()
      val before = CometUnsafeProjection.generatedClassCount
      val first = CometUnsafeProjection.create(attrs)
      val second = CometUnsafeProjection.create(attrs)
      assert(CometUnsafeProjection.generatedClassCount == before + 1)
      assert(first ne second)
      // Each projection writes into its own row, so the first row survives the second call.
      val firstRow = first(rows.head)
      val secondRow = second(rows(1))
      val expected = UnsafeProjection.create(attrs, attrs)
      assert(firstRow == expected(rows.head))
      assert(secondRow == expected(rows(1)))
    }
  }

  test("a different method split threshold generates its own class") {
    val attrs = newLayout()
    val before = CometUnsafeProjection.generatedClassCount
    Seq("1024", "64", "1024").foreach { threshold =>
      withCodegenOnly(SQLConf.CODEGEN_METHOD_SPLIT_THRESHOLD.key -> threshold) {
        CometUnsafeProjection.create(attrs)
      }
    }
    assert(CometUnsafeProjection.generatedClassCount == before + 2)
  }

  test("keeps the most recently used classes") {
    withCodegenOnly() {
      val layouts = Seq.fill(CometUnsafeProjection.MaxCachedClasses + 1) {
        val name = s"f_${UUID.randomUUID().toString.replace("-", "")}"
        Seq(AttributeReference("c", StructType(Seq(StructField(name, IntegerType))))())
      }
      layouts.foreach(CometUnsafeProjection.create)
      val before = CometUnsafeProjection.generatedClassCount
      CometUnsafeProjection.create(layouts.last)
      assert(CometUnsafeProjection.generatedClassCount == before)
      CometUnsafeProjection.create(layouts.head)
      assert(CometUnsafeProjection.generatedClassCount == before + 1)
    }
  }

  test("columnar to row outside whole-stage codegen reuses the generated class") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      spark
        .range(200)
        .selectExpr(
          "id",
          "named_struct('a', id, 'b', array(cast(id as string), null)) as st",
          "map(cast(id as string), array(id)) as m")
        .repartition(4)
        .write
        .parquet(path)
      // More fields than this keep a plan out of whole-stage codegen, as a wide schema would.
      withSQLConf(SQLConf.WHOLESTAGE_MAX_NUM_FIELDS.key -> "2") {
        val df = spark.read.parquet(path)
        checkSparkAnswerAndOperator(df)
        val plan = df.queryExecution.executedPlan
        assert(collect(plan) { case c: CometColumnarToRowExec => c }.nonEmpty, plan)
        assert(collect(plan) { case w: WholeStageCodegenExec => w }.isEmpty, plan)
        // The first run generated the class, so another run over every partition generates none.
        val before = CometUnsafeProjection.generatedClassCount
        assert(df.collect().length == 200)
        assert(CometUnsafeProjection.generatedClassCount == before)
      }
    }
  }
}
