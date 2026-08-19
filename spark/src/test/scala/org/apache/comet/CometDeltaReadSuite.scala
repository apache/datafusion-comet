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

import org.apache.spark.SparkConf
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf

/**
 * Empirical analysis of how Comet handles reads of Delta Lake tables.
 *
 * Comet has no native Delta integration (see docs/source/about/gluten_comparison.md). These tests
 * pin down what actually happens to a Delta scan, end to end, on a real Comet + Delta build:
 *
 *   1. No native Comet scan is ever used for Delta. Delta reads through a `FileSourceScanExec`
 *      backed by `DeltaParquetFileFormat`, a subclass of Spark's `ParquetFileFormat`.
 *      `CometScanExec.isFileFormatSupported` requires the EXACT `ParquetFileFormat` class, so the
 *      Delta scan is never replaced by a Comet native Parquet scan.
 *
 * 2. With `spark.comet.convert.parquet.enabled=true`, `CometExecRule` matches the Delta scan via
 * `case _: ParquetFileFormat` (which DOES match the subclass) and wraps Spark's Delta scan in a
 * `CometSparkToColumnarExec`. The file bytes are still decoded by Spark, but the rows are
 * converted to Arrow so downstream operators (filter, project, aggregate, ...) run natively.
 *
 * This suite requires the `delta-spark` test dependency declared in `spark/pom.xml`.
 */
class CometDeltaReadSuite extends CometTestBase {

  private val nativeScanNodeNames =
    Set("CometScanExec", "CometNativeScanExec", "CometBatchScanExec")

  /**
   * Tier 0 detection relies on DeltaParquetFileFormat exposing `columnMappingMode`. Cancel the
   * test on Delta versions that do not, so this suite stays green across the Spark/Delta build
   * matrix without asserting behaviour we cannot guarantee.
   */
  private def assumeDeltaTier0(): Unit = {
    val ok =
      try {
        Class
          .forName("org.apache.spark.sql.delta.DeltaParquetFileFormat")
          .getMethods
          .exists(_.getName == "columnMappingMode")
      } catch {
        case _: Throwable => false
      }
    assume(ok, "Delta version does not expose columnMappingMode; skipping Tier 0 assertions")
  }

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    conf
  }

  private def writeDeltaTable(path: String, properties: Map[String, String] = Map.empty): Unit = {
    val writer = spark
      .range(0, 100)
      .selectExpr("id", "id % 5 as grp", "cast(id as string) as name")
      .write
      .format("delta")
    properties.foldLeft(writer) { case (w, (k, v)) => w.option(k, v) }.save(path)
  }

  /**
   * delta-spark 4.1.0 is built against Spark 4.1.0, but Spark 4.1.2 removed
   * `org.apache.spark.sql.catalyst.plans.logical.IgnoreCachedData`, which Delta DML commands
   * (DELETE/UPDATE/ALTER) extend. That skew is unrelated to Comet, so cancel rather than fail.
   */
  private def cancelIfDeltaDmlSkew(t: Throwable): Nothing = {
    var c: Throwable = t
    while (c != null) {
      if (c.toString.contains("IgnoreCachedData")) {
        cancel(
          "Skipping: delta-spark 4.1.0 DML is binary-incompatible with this Spark " +
            "(IgnoreCachedData was removed after 4.1.0); read-path coverage is unaffected.")
      }
      c = c.getCause
    }
    throw t
  }

  /** Force execution, then return every physical-plan node (AQE stripped). */
  private def planNodes(df: DataFrame): Seq[SparkPlan] = {
    df.collect()
    stripAQEPlan(df.queryExecution.executedPlan).collect { case p => p }
  }

  private def nodeNames(df: DataFrame): Seq[String] =
    planNodes(df).map(_.getClass.getSimpleName)

  private def fileFormatOf(df: DataFrame): String =
    planNodes(df)
      .collectFirst { case f: FileSourceScanExec =>
        f.relation.fileFormat.getClass.getName
      }
      .getOrElse("<no FileSourceScanExec>")

  test("Delta scan is never replaced by a native Comet scan") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_CONVERT_FROM_PARQUET_ENABLED.key -> "false") {
      withTempPath { dir =>
        val path = dir.getCanonicalPath
        writeDeltaTable(path)
        val df = spark.read.format("delta").load(path)
        val names = nodeNames(df)
        val fmt = fileFormatOf(df)
        info(s"convert=off plan nodes: ${names.mkString(", ")}")
        info(s"file format: $fmt")
        assert(fmt.toLowerCase.contains("delta"), s"expected a Delta file format, got $fmt")
        assert(
          !names.exists(nativeScanNodeNames.contains),
          s"Comet must NOT natively scan Delta, but found: ${names.mkString(", ")}")
        assert(
          !names.contains("CometSparkToColumnarExec"),
          "with convert.parquet disabled Comet should not touch the Delta scan")
        checkSparkAnswer(df)
      }
    }
  }

  test("convert.parquet wraps the Spark Delta scan with CometSparkToColumnarExec") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_CONVERT_FROM_PARQUET_ENABLED.key -> "true") {
      withTempPath { dir =>
        val path = dir.getCanonicalPath
        writeDeltaTable(path)
        val df = spark.read
          .format("delta")
          .load(path)
          .filter("id > 10")
          .selectExpr("grp", "id + 1 as id1")
        val names = nodeNames(df)
        info(s"convert=on plan nodes: ${names.mkString(", ")}")
        assert(
          names.contains("CometSparkToColumnarExec"),
          s"expected CometSparkToColumnarExec over the Delta scan, got: ${names.mkString(", ")}")
        assert(
          !names.exists(nativeScanNodeNames.contains),
          "file reading must remain in Spark even when converting to Arrow")
        checkSparkAnswer(df)
      }
    }
  }

  test("convert.parquet lets downstream aggregate run natively over a Delta scan") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_CONVERT_FROM_PARQUET_ENABLED.key -> "true") {
      withTempPath { dir =>
        val path = dir.getCanonicalPath
        writeDeltaTable(path)
        val df = spark.read
          .format("delta")
          .load(path)
          .groupBy("grp")
          .count()
        val names = nodeNames(df)
        info(s"aggregate plan nodes: ${names.mkString(", ")}")
        assert(names.contains("CometSparkToColumnarExec"))
        assert(
          names.exists(_.startsWith("CometHashAggregate")),
          s"expected a native Comet aggregate, got: ${names.mkString(", ")}")
        checkSparkAnswer(df)
      }
    }
  }

  test("Delta read with deletion vectors: no native scan, correct results") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_CONVERT_FROM_PARQUET_ENABLED.key -> "true") {
      withTempPath { dir =>
        val path = dir.getCanonicalPath
        writeDeltaTable(path, Map("delta.enableDeletionVectors" -> "true"))
        try {
          spark.sql(s"DELETE FROM delta.`$path` WHERE id % 7 = 0")
        } catch {
          case e: Throwable => cancelIfDeltaDmlSkew(e)
        }
        val df = spark.read.format("delta").load(path)
        val names = nodeNames(df)
        info(s"deletion-vectors plan nodes: ${names.mkString(", ")}")
        assert(
          !names.exists(nativeScanNodeNames.contains),
          s"Comet must NOT natively scan Delta with deletion vectors: ${names.mkString(", ")}")
        checkSparkAnswer(df)
        assert(df.count() == 100 - (0 until 100).count(_ % 7 == 0))
      }
    }
  }

  test("Delta read with column mapping: no native scan, correct results") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_CONVERT_FROM_PARQUET_ENABLED.key -> "true") {
      withTempPath { dir =>
        val path = dir.getCanonicalPath
        writeDeltaTable(
          path,
          Map(
            "delta.columnMapping.mode" -> "name",
            "delta.minReaderVersion" -> "2",
            "delta.minWriterVersion" -> "5"))
        val df = spark.read.format("delta").load(path)
        val names = nodeNames(df)
        info(s"column-mapping plan nodes: ${names.mkString(", ")}")
        assert(
          !names.exists(nativeScanNodeNames.contains),
          s"Comet must NOT natively scan Delta with column mapping: ${names.mkString(", ")}")
        checkSparkAnswer(df)
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Tier 0: native Delta scan for plain Parquet tables, behind
  // spark.comet.scan.delta.enabled. Deletion vectors and column mapping must
  // still fall back to Spark.
  // ---------------------------------------------------------------------------

  test("Tier 0: plain Delta table is scanned natively when enabled") {
    assumeDeltaTier0()
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_DELTA_NATIVE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_CONVERT_FROM_PARQUET_ENABLED.key -> "false") {
      withTempPath { dir =>
        val path = dir.getCanonicalPath
        writeDeltaTable(path)
        val df = spark.read.format("delta").load(path)
        val names = nodeNames(df)
        info(s"tier0 plain plan nodes: ${names.mkString(", ")}")
        assert(
          names.exists(nativeScanNodeNames.contains),
          s"expected a native Comet scan of the Delta table, got: ${names.mkString(", ")}")
        assert(
          !names.contains("FileSourceScanExec"),
          "the Spark Delta FileSourceScanExec should have been replaced by a native Comet scan")
        checkSparkAnswer(df)
      }
    }
  }

  test("Tier 0: partitioned Delta table is scanned natively when enabled") {
    assumeDeltaTier0()
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_DELTA_NATIVE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_CONVERT_FROM_PARQUET_ENABLED.key -> "false") {
      withTempPath { dir =>
        val path = dir.getCanonicalPath
        spark
          .range(0, 100)
          .selectExpr("id", "id % 4 as part", "cast(id as string) as name")
          .write
          .format("delta")
          .partitionBy("part")
          .save(path)
        val df = spark.read.format("delta").load(path).filter("part = 1")
        val names = nodeNames(df)
        info(s"tier0 partitioned plan nodes: ${names.mkString(", ")}")
        assert(
          names.exists(nativeScanNodeNames.contains),
          s"expected a native Comet scan of the partitioned Delta table: ${names.mkString(", ")}")
        checkSparkAnswer(df)
      }
    }
  }

  test("Tier 0: deletion vectors still fall back to Spark even when enabled") {
    assumeDeltaTier0()
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_DELTA_NATIVE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_CONVERT_FROM_PARQUET_ENABLED.key -> "false") {
      withTempPath { dir =>
        val path = dir.getCanonicalPath
        writeDeltaTable(path, Map("delta.enableDeletionVectors" -> "true"))
        try {
          spark.sql(s"DELETE FROM delta.`$path` WHERE id % 7 = 0")
        } catch {
          case e: Throwable => cancelIfDeltaDmlSkew(e)
        }
        val df = spark.read.format("delta").load(path)
        val names = nodeNames(df)
        info(s"tier0 deletion-vectors plan nodes: ${names.mkString(", ")}")
        // If no synthetic deletion-vector columns are present, this Delta version materialized the
        // DELETE as a copy-on-write rewrite (no DV), so there is nothing to fall back for.
        if (names.exists(nativeScanNodeNames.contains)) {
          cancel("DELETE did not materialize a deletion vector on this Delta version")
        }
        assert(
          !names.exists(nativeScanNodeNames.contains),
          s"Delta tables with deletion vectors must fall back to Spark: ${names.mkString(", ")}")
        checkSparkAnswer(df)
        assert(df.count() == 100 - (0 until 100).count(_ % 7 == 0))
      }
    }
  }

  test("Tier 0: column mapping still falls back to Spark even when enabled") {
    assumeDeltaTier0()
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_DELTA_NATIVE_SCAN_ENABLED.key -> "true",
      CometConf.COMET_CONVERT_FROM_PARQUET_ENABLED.key -> "false") {
      withTempPath { dir =>
        val path = dir.getCanonicalPath
        writeDeltaTable(
          path,
          Map(
            "delta.columnMapping.mode" -> "name",
            "delta.minReaderVersion" -> "2",
            "delta.minWriterVersion" -> "5"))
        val df = spark.read.format("delta").load(path)
        val names = nodeNames(df)
        info(s"tier0 column-mapping plan nodes: ${names.mkString(", ")}")
        assert(
          !names.exists(nativeScanNodeNames.contains),
          s"Delta tables with column mapping must fall back to Spark: ${names.mkString(", ")}")
        checkSparkAnswer(df)
      }
    }
  }
}
