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

import org.apache.hadoop.fs.Path
import org.apache.parquet.example.data.simple.SimpleGroup
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.MessageTypeParser
import org.apache.spark.SparkConf
import org.apache.spark.sql.{CometTestBase, DataFrame, Row}
import org.apache.spark.sql.comet.CometNativeColumnarToRowExec
import org.apache.spark.sql.comet.CometNativeScanExec
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.execution.{ColumnarToRowExec, CommandResultExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

import org.apache.comet.serde.operator.CometNativeScan

class CometVariantProjectionSuite extends CometTestBase {
  override protected def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.USE_V1_SOURCE_LIST.key, "parquet")
    .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
    .set("spark.sql.variant.allowReadingShredded", "true")
    .set("spark.sql.variant.pushVariantIntoScan", "false")

  private def withVariantFile(query: String)(check: String => Unit): Unit = {
    assume(Utils.variantType.isDefined, "VariantType requires Spark 4.0+")
    withTempPath { dir =>
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        sql(query).coalesce(1).write.parquet(dir.getCanonicalPath)
      }
      check(dir.getCanonicalPath)
    }
  }

  private def checkVariantAnswer(df: DataFrame, expected: Seq[Row]): SparkPlan = {
    // Shredding can produce different valid byte encodings of the same Variant value.
    // Compare Spark's rendered values while retaining SQL nulls and ordinary sibling types.
    def prepare(rows: Seq[Row]): Seq[Row] = rows
      .map { row =>
        Row.fromSeq(row.toSeq.zip(df.schema.fields).map {
          case (value, field) if value != null && Utils.variantType.contains(field.dataType) =>
            value.toString
          case (value, _) => value
        })
      }
      .sortBy(_.toString)
    assert(prepare(df.collect().toSeq) == prepare(expected))
    df.queryExecution.executedPlan
  }

  private def sparkRows(df: => DataFrame): Seq[Row] = {
    var rows = Seq.empty[Row]
    withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      rows = df.collect().toSeq
    }
    rows
  }

  private def checkNative(df: => DataFrame, expected: Option[Seq[Row]] = None): Unit = {
    val plan = checkVariantAnswer(df, expected.getOrElse(sparkRows(df)))
    checkCometOperators(plan, classOf[ColumnarToRowExec])
    assert(collect(plan) { case scan: CometNativeScanExec => scan }.nonEmpty, plan.toString)
    assert(collect(plan) { case c: CometNativeColumnarToRowExec => c }.isEmpty, plan.toString)
  }

  private def checkScanFallbackPlan(df: DataFrame, reason: String): Unit = {
    val plan = df.queryExecution.executedPlan
    assert(new ExtendedExplainInfo().getFallbackReasons(plan).exists(_.contains(reason)))
    assert(collect(plan) { case scan: CometNativeScanExec => scan }.isEmpty, plan.toString)
  }

  private def checkScanFallback(df: => DataFrame, reason: String): Unit = {
    val (_, plan) = checkSparkAnswerAndFallbackReason(df, reason)
    assert(collect(plan) { case scan: CometNativeScanExec => scan }.isEmpty, plan.toString)
  }

  test("direct Variant projection preserves values and siblings") {
    withVariantFile("""
      SELECT id, parse_json(json) AS v, id + 10 AS tail FROM VALUES
        (1, '{"a":1,"nested":{"b":[true,null,2.5]}}'),
        (2, '[1,"text",false,{"x":2}]'),
        (3, '42'), (4, '"text"'), (5, 'null'), (6, NULL),
        (7, '{}'), (8, '[]') AS input(id, json)
      """) { path =>
      checkNative(spark.read.parquet(path).select("v"))
      checkNative(spark.read.parquet(path).select("id", "v", "tail"))
    }
    withVariantFile("SELECT 1 AS id, CAST(NULL AS VARIANT) AS v") { path =>
      checkNative(spark.read.parquet(path))
    }
  }

  test("Variant objects with empty keys match Spark") {
    for (shredding <- Seq("false", "true")) {
      withSQLConf("spark.sql.variant.writeShredding.enabled" -> shredding) {
        withVariantFile("""
          SELECT id, parse_json(json) AS v FROM VALUES
            (1, '{"":1}'), (2, '{"z":1,"":2,"a":{"":3}}'),
            (3, '[{"z":4,"":5},{"":6}]'), (4, NULL) AS input(id, json)
          """) { path =>
          checkNative(spark.read.parquet(path))
        }
      }
    }
  }

  test("missing Variant default preserves later default indexes and present nulls") {
    assume(Utils.variantType.isDefined, "VariantType requires Spark 4.0+")
    val schema = StructType(
      Seq(
        StructField("id", IntegerType),
        StructField("before", IntegerType).withExistenceDefaultValue("11"),
        StructField("v", Utils.variantType.get)
          .withExistenceDefaultValue("parse_json('{\"default\":42}')"),
        StructField("tail", IntegerType).withExistenceDefaultValue("99")))
    for ((query, expectedValue, expectedTail) <- Seq(
        ("SELECT 1 AS id", "parse_json('{\"default\":42}')", 99),
        ("SELECT 1 AS id, CAST(NULL AS VARIANT) AS v, 7 AS tail", "CAST(NULL AS VARIANT)", 7),
        (
          "SELECT 1 AS id, parse_json('{\"present\":true}') AS v, 7 AS tail",
          "parse_json('{\"present\":true}')",
          7))) {
      withVariantFile(query) { path =>
        // Spark's vectorized reader rejects Variant defaults, and its row reader misapplies
        // later defaults when preceding columns are absent. Use Spark's literal results.
        // TODO: Replace these explicit expected rows with a Spark Parquet read once every
        // supported Spark profile handles Variant defaults and subsequent default indexes.
        val expected = sparkRows(
          sql(s"SELECT 1 AS id, 11 AS before, $expectedValue AS v, $expectedTail AS tail"))
        checkNative(spark.read.schema(schema).parquet(path), Some(expected))
      }
    }
    withSQLConf(CometConf.getExprEnabledConfigKey("CreateNamedStruct") -> "false") {
      assert(CometNativeScan.serializeExistenceDefaultValues(schema, Seq.empty).isEmpty)
      withVariantFile("SELECT 1 AS id") { path =>
        checkScanFallbackPlan(
          spark.read.schema(schema).parquet(path),
          "one or more column default values are not supported")
      }
    }
  }

  test("Variant projection uses shared Unicode field matching") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      for ((physical, logical) <- Seq("MÜNCHEN" -> "münchen", "K" -> "k", "ſ" -> "s")) {
        withVariantFile(s"""SELECT parse_json('{"a":1}') AS `$physical`, 7 AS `Ü`""") { path =>
          val schema = StructType(
            Seq(StructField(logical, Utils.variantType.get), StructField("ü", IntegerType)))
          checkNative(spark.read.schema(schema).parquet(path))
        }
      }
    }
  }

  test("unread Variant roots and nested fields are pruned from native scans") {
    withVariantFile("""
      SELECT 1 AS id, parse_json('{"a":1}') AS v,
        named_struct('n', 7, 'v', parse_json('[1,2]')) AS s
      """) { path =>
      checkNative(spark.read.parquet(path).select("id"))
      checkNative(spark.read.parquet(path).select("s.n"))
      checkScanFallback(spark.read.parquet(path).select("s"), "VariantType")
    }
    for (nested <- Seq("array(parse_json('1'))", "map('key', parse_json('1'))")) {
      withVariantFile(s"SELECT $nested AS nested") { path =>
        checkScanFallback(spark.read.parquet(path), "VariantType")
      }
    }
  }

  test("Variant scans preserve strict reader and timestamp inference fallbacks") {
    withSQLConf("spark.sql.variant.writeShredding.enabled" -> "false") {
      withVariantFile("SELECT parse_json('{\"a\":1}') AS v") { path =>
        withSQLConf("spark.sql.variant.allowReadingShredded" -> "false") {
          checkScanFallback(spark.read.parquet(path), "allowReadingShredded=true")
        }
        for (setting <- Seq(
            "spark.sql.legacy.parquet.nanosAsLong" -> "true",
            "spark.sql.parquet.inferTimestampNTZ.enabled" -> "false")) {
          withSQLConf(setting) {
            checkScanFallback(spark.read.parquet(path), "default Parquet timestamp inference")
          }
        }
        withSQLConf("spark.sql.variant.pushVariantIntoScan" -> "true") {
          checkScanFallback(
            spark.read.parquet(path).selectExpr("variant_get(v, '$.a', 'int')"),
            "VariantType")
        }
      }
    }
  }

  test("Variant consumers fall back above a native scan") {
    withVariantFile("SELECT 1 AS id, parse_json('{\"a\":1}') AS v") { path =>
      withSQLConf("spark.sql.variant.pushVariantIntoScan" -> "false") {
        val (_, plan) = checkSparkAnswerAndFallbackReason(
          spark.read.parquet(path).selectExpr("variant_get(v, '$.a', 'int')"),
          "Native operators do not support schemas containing type VariantType")
        assert(collect(plan) { case p: ProjectExec => p }.nonEmpty)
        assert(collect(plan) { case s: CometNativeScanExec => s }.nonEmpty)
      }
      val expected = sparkRows(spark.read.parquet(path))
      val plan = checkVariantAnswer(spark.read.parquet(path).repartition(2), expected)
      assert(collect(plan) { case s: ShuffleExchangeExec => s }.nonEmpty)
      assert(collect(plan) { case s: CometNativeScanExec => s }.nonEmpty)

      withTempView("variant_source") {
        spark.read.parquet(path).createOrReplaceTempView("variant_source")
        withTempPath { output =>
          withTable("variant_copy") {
            sql(
              s"CREATE TABLE variant_copy (id INT, v VARIANT) USING parquet " +
                s"LOCATION '${output.getCanonicalPath}'")
            withSQLConf(
              CometConf.COMET_NATIVE_PARQUET_WRITE_ENABLED.key -> "true",
              CometConf.getOperatorAllowIncompatConfigKey(
                classOf[DataWritingCommandExec]) -> "true") {
              val command = sql("INSERT INTO variant_copy SELECT * FROM variant_source")
              val plan = command.queryExecution.executedPlan
                .asInstanceOf[CommandResultExec]
                .commandPhysicalPlan
              assert(
                collect(plan) { case write: DataWritingCommandExec => write }.nonEmpty,
                plan.toString)
              assert(
                new ExtendedExplainInfo()
                  .getFallbackReasons(plan)
                  .exists(_.contains(
                    "Native operators do not support schemas containing type VariantType")))
              checkNative(spark.read.parquet(output.getCanonicalPath))
            }
          }
        }
      }
    }
  }

  test("encrypted Variant scans fall back to Spark") {
    withSQLConf(
      "parquet.crypto.factory.class" ->
        "org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory",
      "parquet.encryption.kms.client.class" ->
        "org.apache.parquet.crypto.keytools.mocks.InMemoryKMS",
      "parquet.encryption.key.list" -> "variantKey: MDEyMzQ1Njc4OTAxMjM0NQ==",
      "parquet.encryption.uniform.key" -> "variantKey") {
      withVariantFile("SELECT parse_json('{\"a\":1}') AS v") { path =>
        checkScanFallback(spark.read.parquet(path), "Variant scans do not support encryption")
      }
    }
  }

  test("strict Variant reader preserves malformed layout errors") {
    assume(Utils.variantType.isDefined, "VariantType requires Spark 4.0+")
    withTempPath { file =>
      val physical = MessageTypeParser.parseMessageType("""message root {
        optional group v {
          required binary value;
          optional binary metadata;
        }
      }""")
      val writer = createParquetWriter(physical, new Path(file.toURI))
      try {
        val row = new SimpleGroup(physical)
        row
          .addGroup("v")
          .append("value", Binary.fromConstantByteArray(Array[Byte](0)))
          .append("metadata", Binary.fromConstantByteArray(Array[Byte](1, 0, 0)))
        writer.write(row)
      } finally {
        writer.close()
      }
      withSQLConf("spark.sql.variant.allowReadingShredded" -> "false") {
        val df = spark.read
          .schema(StructType(Seq(StructField("v", Utils.variantType.get))))
          .parquet(file.getCanonicalPath)
        checkScanFallbackPlan(df, "allowReadingShredded=true")
        val error = intercept[Exception](df.collect())
        assert(
          Iterator
            .iterate[Throwable](error)(_.getCause)
            .takeWhile(_ != null)
            .exists(cause =>
              Option(cause.getMessage).exists(
                _.contains("INVALID_VARIANT_FROM_PARQUET.NULLABLE_OR_NOT_BINARY_FIELD"))))
      }
    }
  }
}
