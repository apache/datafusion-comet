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

import java.sql.Timestamp
import java.time.Instant

import org.apache.spark.SparkConf
import org.apache.spark.sql.{CometTestBase, DataFrame, Row}
import org.apache.spark.sql.comet.{CometInMemoryTableScanExec, CometNativeScanExec}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.columnar.CometInMemoryRelationHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import org.apache.comet.CometConf

class CometInMemoryCachePruningSuite extends CometTestBase {

  override protected def beforeAll(): Unit = {
    CometInMemoryRelationHelper.clearSerializer()
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      CometInMemoryRelationHelper.clearSerializer()
    }
  }

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.plugins", "org.apache.spark.CometPlugin")
    .set(
      "spark.sql.cache.serializer",
      "org.apache.spark.sql.comet.execution.arrow.ArrowCachedBatchSerializer")

  private val schema = StructType(
    Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("d", DoubleType),
      StructField("f", FloatType),
      StructField("n", IntegerType),
      StructField("s", StringType),
      StructField("dec", DecimalType(20, 3)),
      StructField("ts", TimestampType),
      StructField("b", BooleanType)))

  private def fixture(): DataFrame = {
    def repeated(d: Double): Seq[Double] = Seq.fill(4)(d)
    // Every four rows form one batch in all three writers. Keep NaN-only, mixed finite/NaN,
    // signed-zero-only, infinity and all-null batches separate so incorrect bounds lose rows.
    val values = Seq(
      repeated(Double.NegativeInfinity),
      repeated(-100.0),
      repeated(-2.0),
      repeated(-0.0),
      repeated(0.0),
      repeated(0.25),
      repeated(1.0),
      repeated(2.0),
      repeated(100.0),
      repeated(Double.PositiveInfinity),
      repeated(Double.NaN),
      Seq(1.0, Double.NaN, 3.0, 2.0),
      repeated(0.0), // all-null batch
      Seq(-0.0, 0.0, -0.0, 0.0),
      Seq(-3.0, -2.0, -1.0, 0.0),
      Seq(Double.PositiveInfinity, Double.NaN, Double.PositiveInfinity, Double.NaN))
    val strings = Seq(
      "",
      "a",
      "ab",
      "b",
      "\u007f",
      "\u0080",
      "\ue000",
      "\ud800\udc00",
      "é",
      "中",
      "prefix-a",
      "prefix-z",
      null,
      "z",
      "e\u0301",
      "😀")
    val rows = values.zipWithIndex.flatMap { case (batch, group) =>
      batch.zipWithIndex.map { case (d, offset) =>
        val isNull = group == 12 || (group == 11 && offset == 2)
        Row(
          group * 4 + offset,
          if (isNull) null else Double.box(d),
          if (isNull) null else Float.box(d.toFloat),
          if (isNull) null else Int.box(group),
          strings(group),
          if (group == 12) null else new java.math.BigDecimal(s"${group - 8}.125"),
          if (group == 12) null
          else
            Timestamp.from(
              Instant
                .parse("1960-01-01T00:00:00Z")
                .plusSeconds(group * 86400L)
                .plusNanos(offset * 1000L)),
          if (group == 12) null else Boolean.box(group % 2 == 0))
      }
    }
    spark.createDataFrame(spark.sparkContext.parallelize(rows, 1), schema)
  }

  private val predicates = Seq(
    "d = CAST('NaN' AS DOUBLE)",
    "f = CAST('NaN' AS FLOAT)",
    "d > CAST('Infinity' AS DOUBLE)",
    "f < CAST('NaN' AS FLOAT)",
    "d = 0.0D",
    "f = CAST('-0.0' AS FLOAT)",
    "d >= CAST('-0.0' AS DOUBLE) AND d <= 0.0D",
    "f >= CAST(0.0 AS FLOAT) AND f <= CAST('-0.0' AS FLOAT)",
    "d = CAST('-Infinity' AS DOUBLE)",
    "f >= CAST('Infinity' AS FLOAT)",
    "d > -2.0D AND d < 2.0D",
    "d IS NULL",
    "d IS NOT NULL",
    "n IS NULL",
    "s = '中'",
    "s >= '\ue000'",
    "s < '\u0080'",
    "s LIKE 'prefix%'",
    "dec >= -1.125 AND dec < 2.125",
    "ts < TIMESTAMP '1960-01-05 00:00:00'",
    "b <=> true",
    "id IN (1, 9, 49)",
    "d = -100.0D OR s = 'prefix-z'")

  Seq("native Arrow", "Spark columnar", "row").foreach { writer =>
    test(s"cache pruning matches uncached Spark with $writer input") {
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC",
        SQLConf.IN_MEMORY_PARTITION_PRUNING.key -> "true",
        SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> "true",
        SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true",
        SQLConf.PARQUET_VECTORIZED_READER_BATCH_SIZE.key -> "4",
        SQLConf.COLUMN_BATCH_SIZE.key -> "4",
        CometConf.COMET_BATCH_SIZE.key -> "4",
        CometConf.COMET_SHUFFLE_JVM_BATCH_SIZE.key -> "4",
        CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true",
        CometConf.COMET_SPARK_TO_ARROW_ENABLED.key -> "false",
        CometConf.COMET_NATIVE_SCAN_ENABLED.key -> (writer == "native Arrow").toString) {
        spark.catalog.clearCache()
        val source = fixture()
        // Collect every expected value and predicate result before registering any cache. A
        // second query against the cached table, even with Comet disabled, is not an oracle:
        // Spark still decodes the same Comet payload and applies the same cached statistics.
        // Use the original in-memory data: Parquet row-group pruning can itself mishandle
        // signed zero, which would make a file-based oracle hide a cache pruning regression.
        var oracle = Seq.empty[Row]
        withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
          oracle = source
            .selectExpr((Seq("*") ++ predicates.zipWithIndex.map { case (p, i) =>
              s"($p) AS predicate_$i"
            }): _*)
            .collect()
            .toSeq
        }
        val expectedRows = oracle.map(row => Row.fromSeq(row.toSeq.take(schema.length)))

        withTempPath { path =>
          val input = if (writer == "row") {
            source
          } else {
            withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
              source.write.option("parquet.enable.dictionary", "false").parquet(path.toString)
            }
            spark.read.parquet(path.toString)
          }
          input.createOrReplaceTempView("pruning_cache")
          val cached = spark.table("pruning_cache").cache()
          try {
            val relation =
              spark.sharedState.cacheManager.lookupCachedData(cached).get.cachedRepresentation
            val plan = relation.cacheBuilder.cachedPlan
            withClue(s"$writer cache writer:\n$plan\n") {
              writer match {
                case "native Arrow" =>
                  assert(plan.supportsColumnar)
                  assert(plan.collect { case s: CometNativeScanExec => s }.nonEmpty)
                case "Spark columnar" =>
                  assert(plan.supportsColumnar)
                  assert(plan.collect { case s: FileSourceScanExec => s }.nonEmpty)
                  assert(plan.collect { case s: CometNativeScanExec => s }.isEmpty)
                case "row" => assert(!plan.supportsColumnar)
              }
            }
            checkCometAnswer(cached, expectedRows)
            val batches = relation.cacheBuilder.cachedColumnBuffers.collect()
            assert(batches.length == 16, "the fixture must produce many distinct small batches")
            assert(batches.forall(_.numRows == 4))
            assert(
              batches.forall(_.getClass.getName ==
                "org.apache.spark.sql.comet.execution.arrow.CometCachedBatch"))

            predicates.zipWithIndex.foreach { case (predicate, i) =>
              def matches(row: Row): Boolean =
                !row.isNullAt(schema.length + i) && row.getBoolean(schema.length + i)
              val expected = oracle
                .filter(matches)
                .map(_.getInt(0))
                .sorted
              val expectedScannedRows = oracle.grouped(4).count(_.exists(matches)) * 4
              assert(expected.nonEmpty && expected.length < expectedRows.length)
              val query = cached.where(predicate).select("id")
              val actual = query.collect().map(_.getInt(0)).sorted.toSeq
              val scans = query.queryExecution.executedPlan.collect {
                case scan: CometInMemoryTableScanExec => scan
              }
              withClue(s"$writer input, predicate: $predicate\n") {
                assert(actual == expected)
                assert(scans.length == 1)
                assert(scans.head.originalPlan.predicates.nonEmpty)
                val scannedRows = scans.head.metrics("numOutputRows").value
                // Counting eligible fixture batches also rejects a filter that only drops the
                // all-null batch, without applying the predicate's actual bounds.
                assert(
                  scannedRows == expectedScannedRows,
                  s"expected $expectedScannedRows rows from eligible batches, decoded $scannedRows")
              }
            }
          } finally {
            cached.unpersist(blocking = true)
            spark.catalog.clearCache()
            spark.catalog.dropTempView("pruning_cache")
          }
        }
      }
    }
  }
}
