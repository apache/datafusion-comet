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

package org.apache.comet.parquet

import scala.util.Try

import org.apache.hadoop.fs.Path
import org.apache.parquet.example.data.simple.SimpleGroup
import org.apache.parquet.schema.MessageTypeParser
import org.apache.spark.SparkException
import org.apache.spark.sql.{CometTestBase, DataFrame}
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions

/**
 * Tests for Parquet read-schema/file-schema mismatch cases tracked in
 * https://github.com/apache/datafusion-comet/issues/3720.
 */
class ParquetSchemaMismatchSuite extends CometTestBase {
  import testImplicits._

  private def withMismatchedSchema(scanImpl: String)(body: String => DataFrame)(
      check: DataFrame => Unit): Unit = {
    withSQLConf(
      CometConf.COMET_NATIVE_SCAN_IMPL.key -> scanImpl,
      SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {
      withTempPath { dir =>
        val df = body(dir.getCanonicalPath)
        check(df)
      }
    }
  }

  /** Both scan implementations under test, used as a `foreach` driver. */
  private val scanImpls: Seq[String] =
    Seq(CometConf.SCAN_NATIVE_DATAFUSION, CometConf.SCAN_NATIVE_ICEBERG_COMPAT)

  // Case 1: BINARY read as TIMESTAMP. Spark throws SparkException on all
  // versions. Both Comet scan implementations also throw: native_datafusion
  // raises CometNativeException (column type mismatch); native_iceberg_compat
  // raises SparkException (SchemaColumnConvertNotSupportedException). Both
  // surface to the caller as SparkException.
  scanImpls.foreach { scanImpl =>
    test(s"binary read as timestamp: $scanImpl") {
      withMismatchedSchema(scanImpl) { path =>
        val schemaStr =
          """message root {
            |  optional binary _1;
            |}
          """.stripMargin
        val schema = MessageTypeParser.parseMessageType(schemaStr)
        val writer = createParquetWriter(schema, new Path(path, "part-r-0.parquet"))
        (0 until 10).foreach { i =>
          val record = new SimpleGroup(schema)
          record.add(0, s"value-$i")
          writer.write(record)
        }
        writer.close()
        spark.read.schema("_1 timestamp").parquet(path)
      } { df =>
        // Pattern 3 (throw): both scan implementations throw SparkException at
        // collect time; the error message differs but the exception type is the
        // same. Behavior matches Spark's reference behavior on all versions.
        intercept[SparkException] {
          df.collect()
        }
      }
    }
  }

  // Case 2: INT32 read as INT64 (value-preserving widening). Spark 3.4/3.5
  // throw SparkException; Spark 4.0 allows widening.
  // native_datafusion: succeeds with widened values (Pattern 1).
  // native_iceberg_compat: throws SparkException on Spark 3.x (SchemaColumnConvertNotSupportedException
  // from TypeUtil.checkParquetType); on Spark 4.0 COMET_SCHEMA_EVOLUTION_ENABLED defaults to true
  // so the widening is allowed and succeeds with widened values.
  test(s"int32 read as int64: ${CometConf.SCAN_NATIVE_DATAFUSION}") {
    withMismatchedSchema(CometConf.SCAN_NATIVE_DATAFUSION) { path =>
      Seq(1, 2, 3).toDF("c").write.parquet(path)
      spark.read.schema("c bigint").parquet(path)
    } { df =>
      // Pattern 1 (value-preserving widening).
      checkAnswer(df, Seq(1L, 2L, 3L).map(org.apache.spark.sql.Row(_)))
    }
  }

  test(s"int32 read as int64: ${CometConf.SCAN_NATIVE_ICEBERG_COMPAT}") {
    withMismatchedSchema(CometConf.SCAN_NATIVE_ICEBERG_COMPAT) { path =>
      Seq(1, 2, 3).toDF("c").write.parquet(path)
      spark.read.schema("c bigint").parquet(path)
    } { df =>
      // On Spark 3.x: native_iceberg_compat rejects INT32->INT64 widening via
      // TypeUtil.checkParquetType (SchemaColumnConvertNotSupportedException).
      // On Spark 4.0: COMET_SCHEMA_EVOLUTION_ENABLED defaults to true so widening
      // is allowed and succeeds with correctly widened values.
      if (CometSparkSessionExtensions.isSpark40Plus) {
        checkAnswer(df, Seq(1L, 2L, 3L).map(org.apache.spark.sql.Row(_)))
      } else {
        intercept[SparkException] {
          df.collect()
        }
      }
    }
  }

  // Case 3: INT96 TimestampLTZ read as TimestampNTZ. Spark throws on all
  // versions (SPARK-36182). Comet's native_datafusion scan falls back to Spark
  // for TimestampNTZ columns by default (timestampNTZSafetyCheck) because INT96
  // timestamps lose their LTZ/NTZ distinction after Parquet schema coercion.
  // native_iceberg_compat throws via TypeUtil.convertErrorForTimestampNTZ on
  // Spark 3.x (mirrors Spark's behavior). On Spark 4.0, TypeUtil.checkParquetType
  // has an isSpark40Plus guard that bypasses the INT96 check, so the read succeeds.
  test(s"int96 timestamp_ltz read as timestamp_ntz: ${CometConf.SCAN_NATIVE_DATAFUSION}") {
    withMismatchedSchema(CometConf.SCAN_NATIVE_DATAFUSION) { path =>
      withSQLConf(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> "INT96") {
        Seq(java.sql.Timestamp.valueOf("2020-01-01 00:00:00"))
          .toDF("ts")
          .write
          .parquet(path)
      }
      spark.read.schema("ts timestamp_ntz").parquet(path)
    } { df =>
      // native_datafusion falls back to Spark for TimestampNTZ columns
      // (timestampNTZSafetyCheck); Spark throws SparkException on all versions.
      intercept[SparkException] {
        df.collect()
      }
    }
  }

  test(s"int96 timestamp_ltz read as timestamp_ntz: ${CometConf.SCAN_NATIVE_ICEBERG_COMPAT}") {
    withMismatchedSchema(CometConf.SCAN_NATIVE_ICEBERG_COMPAT) { path =>
      withSQLConf(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> "INT96") {
        Seq(java.sql.Timestamp.valueOf("2020-01-01 00:00:00"))
          .toDF("ts")
          .write
          .parquet(path)
      }
      spark.read.schema("ts timestamp_ntz").parquet(path)
    } { df =>
      // On Spark 3.x: native_iceberg_compat throws SparkException via
      // TypeUtil.convertErrorForTimestampNTZ; matches Spark's behavior.
      // On Spark 4.0: isSpark40Plus guard in TypeUtil.checkParquetType bypasses
      // the INT96 check so the read succeeds silently (row count verified only;
      // the wall-clock value may differ due to LTZ->NTZ reinterpretation).
      if (CometSparkSessionExtensions.isSpark40Plus) {
        assert(df.collect().length == 1)
      } else {
        intercept[SparkException] {
          df.collect()
        }
      }
    }
  }

  // Case 4: Decimal(10,2) read as Decimal(5,0). Reading from a higher-precision
  // decimal as a lower-precision decimal can lose data (123.45 cannot fit in
  // decimal(5,0)). Spark throws on all versions (SPARK-34212).
  // native_datafusion rejects this lossy scale-narrowing in the schema adapter
  // (issue #4089) and throws CometNativeException, surfacing as SparkException.
  test(s"decimal(10,2) read as decimal(5,0): ${CometConf.SCAN_NATIVE_DATAFUSION}") {
    withMismatchedSchema(CometConf.SCAN_NATIVE_DATAFUSION) { path =>
      Seq(BigDecimal("123.45"), BigDecimal("67.89"))
        .toDF("d")
        .selectExpr("cast(d as decimal(10,2)) as d")
        .write
        .parquet(path)
      spark.read.schema("d decimal(5,0)").parquet(path)
    } { df =>
      intercept[SparkException] {
        df.collect()
      }
    }
  }

  test(s"decimal(10,2) read as decimal(5,0): ${CometConf.SCAN_NATIVE_ICEBERG_COMPAT}") {
    withMismatchedSchema(CometConf.SCAN_NATIVE_ICEBERG_COMPAT) { path =>
      Seq(BigDecimal("123.45"), BigDecimal("67.89"))
        .toDF("d")
        .selectExpr("cast(d as decimal(10,2)) as d")
        .write
        .parquet(path)
      spark.read.schema("d decimal(5,0)").parquet(path)
    } { df =>
      // native_iceberg_compat throws SparkException via
      // SchemaColumnConvertNotSupportedException (INT64 cannot convert to decimal(5,0));
      // matches Spark's reference behavior.
      intercept[SparkException] {
        df.collect()
      }
    }
  }

  // Case 5: regression guard for row group skipping. Write INT32 values near
  // INT32 max, read as INT64 with a filter whose constant exceeds INT32 max.
  // If the scan treats the filter as INT32, row-group skipping might overflow
  // and skip rows that should match.
  test(s"int32 read as int64 with row group filter: ${CometConf.SCAN_NATIVE_DATAFUSION}") {
    withMismatchedSchema(CometConf.SCAN_NATIVE_DATAFUSION) { path =>
      Seq(Int.MaxValue - 2, Int.MaxValue - 1, Int.MaxValue).toDF("c").write.parquet(path)
      spark.read
        .schema("c bigint")
        .parquet(path)
        .filter(s"c > ${Int.MaxValue.toLong - 1L}")
    } { df =>
      // Pattern 1: filter must not overflow when widened.
      checkAnswer(df, Seq(Int.MaxValue.toLong).map(org.apache.spark.sql.Row(_)))
    }
  }

  test(s"int32 read as int64 with row group filter: ${CometConf.SCAN_NATIVE_ICEBERG_COMPAT}") {
    withMismatchedSchema(CometConf.SCAN_NATIVE_ICEBERG_COMPAT) { path =>
      Seq(Int.MaxValue - 2, Int.MaxValue - 1, Int.MaxValue).toDF("c").write.parquet(path)
      spark.read
        .schema("c bigint")
        .parquet(path)
        .filter(s"c > ${Int.MaxValue.toLong - 1L}")
    } { df =>
      // On Spark 3.x: native_iceberg_compat rejects INT32->INT64 widening (Case 2)
      // so the filter never runs and a SparkException is thrown.
      // On Spark 4.0: COMET_SCHEMA_EVOLUTION_ENABLED defaults to true so widening
      // is allowed; the filter runs correctly without overflow and returns 1 row.
      if (CometSparkSessionExtensions.isSpark40Plus) {
        checkAnswer(df, Seq(Int.MaxValue.toLong).map(org.apache.spark.sql.Row(_)))
      } else {
        intercept[SparkException] {
          df.collect()
        }
      }
    }
  }

  // Case 6: STRING column read as INT. Spark's vectorized reader throws on all
  // versions because BINARY (string) cannot be converted to INT32 at the
  // physical Parquet level.
  // native_datafusion rejects string/binary read as numeric in the schema adapter
  // (PR #4091) and throws CometNativeException, surfacing as SparkException.
  // native_iceberg_compat: throws SparkException (aligns with Spark).
  test(s"string read as int: ${CometConf.SCAN_NATIVE_DATAFUSION}") {
    withMismatchedSchema(CometConf.SCAN_NATIVE_DATAFUSION) { path =>
      Seq("a", "b", "c").toDF("c").write.parquet(path)
      spark.read.schema("c int").parquet(path)
    } { df =>
      intercept[SparkException] {
        df.collect()
      }
    }
  }

  test(s"string read as int: ${CometConf.SCAN_NATIVE_ICEBERG_COMPAT}") {
    withMismatchedSchema(CometConf.SCAN_NATIVE_ICEBERG_COMPAT) { path =>
      Seq("a", "b", "c").toDF("c").write.parquet(path)
      spark.read.schema("c int").parquet(path)
    } { df =>
      val outcome = Try(df.collect())
      assert(
        outcome.isFailure && outcome.failed.get.isInstanceOf[SparkException],
        s"expected SparkException, got: $outcome")
    }
  }

  // Case 7: TIMESTAMP_NTZ column read as ARRAY<TIMESTAMP_NTZ>. Spark throws on all
  // versions (SPARK-45604) because the requested type is a list/group but the
  // physical Parquet column is a scalar.
  test(s"timestamp_ntz read as array<timestamp_ntz>: ${CometConf.SCAN_NATIVE_DATAFUSION}") {
    withMismatchedSchema(CometConf.SCAN_NATIVE_DATAFUSION) { path =>
      Seq(java.time.LocalDateTime.parse("2020-01-01T00:00:00"))
        .toDF("ts")
        .write
        .parquet(path)
      spark.read.schema("ts array<timestamp_ntz>").parquet(path)
    } { df =>
      val outcome = Try(df.collect())
      assert(
        outcome.isFailure && outcome.failed.get.isInstanceOf[SparkException],
        s"expected SparkException, got: $outcome")
    }
  }

  test(s"timestamp_ntz read as array<timestamp_ntz>: ${CometConf.SCAN_NATIVE_ICEBERG_COMPAT}") {
    withMismatchedSchema(CometConf.SCAN_NATIVE_ICEBERG_COMPAT) { path =>
      Seq(java.time.LocalDateTime.parse("2020-01-01T00:00:00"))
        .toDF("ts")
        .write
        .parquet(path)
      spark.read.schema("ts array<timestamp_ntz>").parquet(path)
    } { df =>
      val outcome = Try(df.collect())
      assert(
        outcome.isFailure && outcome.failed.get.isInstanceOf[SparkException],
        s"expected SparkException, got: $outcome")
    }
  }

  // Control C1: INT8 -> INT32 widening. Allowed by Spark on all versions and
  // expected to succeed in both Comet scan impls.
  scanImpls.foreach { scanImpl =>
    test(s"int8 read as int32 (control): $scanImpl") {
      withMismatchedSchema(scanImpl) { path =>
        Seq(1.toByte, 2.toByte, 3.toByte).toDF("c").write.parquet(path)
        spark.read.schema("c int").parquet(path)
      } { df =>
        checkAnswer(df, Seq(1, 2, 3).map(org.apache.spark.sql.Row(_)))
      }
    }
  }

  // Control C2: FLOAT -> DOUBLE widening. Allowed by Spark on all versions.
  // native_datafusion: succeeds with widened values (Pattern 1).
  // native_iceberg_compat on Spark 3.x: throws SparkException via TypeUtil.checkParquetType
  // (SchemaColumnConvertNotSupportedException); diverges from Spark's reference behavior.
  // native_iceberg_compat on Spark 4.0: COMET_SCHEMA_EVOLUTION_ENABLED defaults to true
  // so the widening is allowed and succeeds with widened values (matches Spark).
  test(s"float read as double (control): ${CometConf.SCAN_NATIVE_DATAFUSION}") {
    withMismatchedSchema(CometConf.SCAN_NATIVE_DATAFUSION) { path =>
      Seq(1.0f, 2.0f, 3.0f).toDF("c").write.parquet(path)
      spark.read.schema("c double").parquet(path)
    } { df =>
      // Float -> Double is exact for these magnitudes.
      checkAnswer(df, Seq(1.0d, 2.0d, 3.0d).map(org.apache.spark.sql.Row(_)))
    }
  }

  test(s"float read as double (control): ${CometConf.SCAN_NATIVE_ICEBERG_COMPAT}") {
    withMismatchedSchema(CometConf.SCAN_NATIVE_ICEBERG_COMPAT) { path =>
      Seq(1.0f, 2.0f, 3.0f).toDF("c").write.parquet(path)
      spark.read.schema("c double").parquet(path)
    } { df =>
      // On Spark 3.x: native_iceberg_compat rejects FLOAT->DOUBLE widening via
      // TypeUtil.checkParquetType (SchemaColumnConvertNotSupportedException);
      // diverges from Spark which allows this widening on all versions.
      // On Spark 4.0: COMET_SCHEMA_EVOLUTION_ENABLED defaults to true so widening
      // is allowed and succeeds with correctly widened values (matches Spark).
      if (CometSparkSessionExtensions.isSpark40Plus) {
        checkAnswer(df, Seq(1.0d, 2.0d, 3.0d).map(org.apache.spark.sql.Row(_)))
      } else {
        intercept[SparkException] {
          df.collect()
        }
      }
    }
  }
}
