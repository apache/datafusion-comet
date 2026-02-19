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

/**
 * Benchmark test for measuring per-file overhead in Comet Iceberg reads against MinIO (S3).
 * Creates an Iceberg table with many small files to amplify opendal operator construction cost.
 *
 * Run with:
 * {{{
 * ./mvnw test -Dsuites="org.apache.comet.CometIcebergManyFilesBenchmarkSuite"
 * }}}
 */
class CometIcebergManyFilesBenchmarkSuite extends CometS3TestBase {

  override protected val testBucketName = "test-benchmark-bucket"

  private def icebergAvailable: Boolean = {
    try {
      Class.forName("org.apache.iceberg.catalog.Catalog")
      true
    } catch {
      case _: ClassNotFoundException => false
    }
  }

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set("spark.sql.catalog.s3_catalog", "org.apache.iceberg.spark.SparkCatalog")
    conf.set("spark.sql.catalog.s3_catalog.type", "hadoop")
    conf.set("spark.sql.catalog.s3_catalog.warehouse", s"s3a://$testBucketName/warehouse")
    conf.set(CometConf.COMET_ENABLED.key, "true")
    conf.set(CometConf.COMET_EXEC_ENABLED.key, "true")
    conf.set(CometConf.COMET_ICEBERG_NATIVE_ENABLED.key, "true")
    conf
  }

  test("many small files benchmark on S3") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    val numFiles = 10000
    val rowsPerFile = 10
    val numRows = numFiles * rowsPerFile
    val tableName = "s3_catalog.db.many_small"
    val warmupIterations = 2
    val measuredIterations = 5

    spark.sql(s"DROP TABLE IF EXISTS $tableName")
    spark.sql(s"""
      CREATE TABLE $tableName (
        id BIGINT,
        value DOUBLE
      ) USING iceberg
      TBLPROPERTIES (
        'format-version'='2',
        'write.parquet.compression-codec' = 'snappy'
      )
    """)

    spark
      .range(numRows)
      .selectExpr("id", "CAST(id * 1.5 AS DOUBLE) as value")
      .repartition(numFiles)
      .writeTo(tableName)
      .append()

    val actualFiles = spark
      .sql(s"SELECT COUNT(*) FROM $tableName.files")
      .collect()(0)
      .getLong(0)
    assert(
      actualFiles >= numFiles,
      s"Expected at least $numFiles data files but found $actualFiles")

    val query = s"SELECT SUM(value) FROM $tableName"

    // scalastyle:off println
    println(s"Table has $actualFiles data files in S3")

    // Warmup
    for (_ <- 0 until warmupIterations) {
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        spark.sql(query).collect()
      }
      withSQLConf(
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {
        spark.sql(query).collect()
      }
    }

    // Measure Spark
    val sparkTimes = (0 until measuredIterations).map { _ =>
      val start = System.nanoTime()
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        spark.sql(query).collect()
      }
      (System.nanoTime() - start) / 1e6
    }

    // Measure Comet
    val cometTimes = (0 until measuredIterations).map { _ =>
      val start = System.nanoTime()
      withSQLConf(
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true") {
        spark.sql(query).collect()
      }
      (System.nanoTime() - start) / 1e6
    }

    println(s"\n=== Many Files Benchmark ($actualFiles files on S3) ===")
    println(
      f"Spark:  ${sparkTimes.map(t => f"$t%.0f").mkString(", ")} ms " +
        f"(avg ${sparkTimes.sum / sparkTimes.length}%.0f ms)")
    println(
      f"Comet:  ${cometTimes.map(t => f"$t%.0f").mkString(", ")} ms " +
        f"(avg ${cometTimes.sum / cometTimes.length}%.0f ms)")
    println(f"Speedup: ${sparkTimes.sum / cometTimes.sum}%.2fx")
    // scalastyle:on println

    spark.sql(s"DROP TABLE IF EXISTS $tableName")
  }
}
