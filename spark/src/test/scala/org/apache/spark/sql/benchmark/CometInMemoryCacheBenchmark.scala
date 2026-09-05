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

package org.apache.spark.sql.benchmark

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.comet.CometInMemoryTableScanExec
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.{CometConf, CometSparkSessionExtensions}

object CometInMemoryCacheBenchmark extends CometBenchmarkBase {
  private val numRows = 5 * 1000 * 1000

  // A struct column holds several values per row, so caching the nested relation at the flat row
  // count would multiply its footprint for no extra insight. A fifth of the rows keeps the two in
  // the same order of magnitude; the arms are only ever compared against their own relation, never
  // across the two.
  private val nestedNumRows = 1000 * 1000

  private val cacheTable = "comet_cache_bench"
  private val sourceTable = "comet_cache_bench_src"
  private val nestedCacheTable = "comet_cache_bench_nested"
  private val nestedSourceTable = "comet_cache_bench_nested_src"

  /**
   * A relation cached once and then read under several projections.
   *
   * `columns` is the select list that builds the cached relation, so the projection widths the
   * case labels quote are counted against it.
   */
  private case class CachedRelation(
      table: String,
      source: String,
      columns: Seq[String],
      rows: Int)

  private val flatRelation =
    CachedRelation(cacheTable, sourceTable, Seq("id", "k", "v", "s1", "s2", "s3"), numRows)

  private val nestedRelation = CachedRelation(
    nestedCacheTable,
    nestedSourceTable,
    Seq("id", "sc", "deep", "wide", "tail", "d"),
    nestedNumRows)

  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName("CometInMemoryCacheBenchmark")
      .set("spark.master", "local[1]")
      .setIfMissing("spark.driver.memory", "3g")
      .setIfMissing("spark.executor.memory", "3g")
      .set("spark.plugins", "org.apache.spark.CometPlugin")
      .set(
        "spark.shuffle.manager",
        "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager")
      .set(
        "spark.sql.cache.serializer",
        "org.apache.spark.sql.comet.execution.arrow.ArrowCachedBatchSerializer")

    val sparkSession = SparkSession
      .builder()
      .config(conf)
      .withExtensions(new CometSparkSessionExtensions)
      .getOrCreate()

    sparkSession.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")
    sparkSession.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
    sparkSession.conf.set(SQLConf.CACHE_VECTORIZED_READER_ENABLED.key, "true")
    sparkSession.conf.set(SQLConf.ANSI_ENABLED.key, "false")
    sparkSession.conf.set(CometConf.COMET_ENABLED.key, "false")
    sparkSession.conf.set(CometConf.COMET_EXEC_ENABLED.key, "false")
    sparkSession
  }

  override def runCometBenchmark(args: Array[String]): Unit = {
    withTempTable(sourceTable, cacheTable, nestedSourceTable, nestedCacheTable) {
      // Every column nullable, in both relations, so that `count(c)` genuinely reads c. Spark's
      // NullPropagation rewrites a count over a non-nullable column to `count(1)`, which then
      // prunes that column out of the scan -- and since the projection-width cases below measure
      // nothing but which columns are read, a case labelled "6 of 6" would quietly be measuring
      // three. verifyPlan now asserts the widths rather than trusting them. Note that a column is
      // otherwise nullable or not for incidental reasons: `id % 1000` is nullable only because
      // Remainder can divide by zero, while `id + 1` is not, so relying on that is what let the
      // mislabelling through in the first place.
      spark
        .range(0, numRows, 1, 16)
        .selectExpr(
          "if(id % 8 = 0, null, id) AS id",
          "if(id % 8 = 1, null, id % 1000) AS k",
          "if(id % 8 = 2, null, id + 1) AS v",
          "if(id % 8 = 3, null, concat('str_a_', cast(id % 100000 as string))) AS s1",
          "if(id % 8 = 4, null, concat('str_b_', cast(id % 7919 as string))) AS s2",
          "if(id % 8 = 5, null, concat('str_c_', cast(id as string))) AS s3")
        .createOrReplaceTempView(sourceTable)

      // Struct columns, not arrays or maps. The baseline arm needs Spark's cache scan to bridge into
      // Comet operators, and CometSparkToColumnarExec declines ArrayType and MapType outright, so
      // for a relation projecting one of those the arm simply does not exist -- the partial
      // aggregate stays on Spark and the two cases stop being a scan-boundary comparison. Structs
      // are what can be measured here, and they are the shape that matters for the format anyway: a
      // struct is where one cached column owns several field nodes and a validity buffer per level.
      // Array and map coverage lives in CometInMemoryCacheSuite instead.
      //
      // The structs themselves are non-nullable and carry nullable fields, rather than the other way
      // round. Comet cannot evaluate `if(c, null, named_struct(...))` at all: the Spark type keeps
      // saying the fields are non-nullable while the batch has nulls in them wherever the parent is
      // null, and native execution rejects that with "Cannot cast nullable struct field to
      // non-nullable field". That is a CometProject limitation hit while the source rows are built,
      // nothing to do with the cache. Counting a nullable field reads the whole column regardless,
      // since the cache scan selects whole top-level columns.
      spark
        .range(0, nestedNumRows, 1, 16)
        .selectExpr(
          "if(id % 8 = 0, null, id) AS id",
          "named_struct(" +
            "'a', if(id % 8 = 1, null, id), " +
            "'b', concat('sa_', cast(id as string))) AS sc",
          "named_struct('n', named_struct(" +
            "'v', if(id % 8 = 2, null, id), " +
            "'w', concat('sw_', cast(id as string)))) AS deep",
          "named_struct(" +
            "'p', if(id % 8 = 3, null, id % 1000), " +
            "'q', if(id % 8 = 3, null, id + 1), " +
            "'r', concat('sr_', cast(id % 7919 as string))) AS wide",
          "if(id % 8 = 4, null, concat('t_', cast(id as string))) AS tail",
          "if(id % 8 = 5, null, cast(id as double) / 3) AS d")
        .createOrReplaceTempView(nestedSourceTable)

      runCacheBenchmark(
        flatRelation,
        "in-memory cache repeated scan",
        s"SELECT sum(id), sum(k), sum(v) FROM $cacheTable",
        scanned = 3)

      runCacheBenchmark(
        flatRelation,
        "in-memory cache selective filter",
        s"""
           |SELECT sum(id), sum(k), sum(v)
           |FROM $cacheTable
           |WHERE id >= 4500000 AND id < 4750000
         """.stripMargin,
        scanned = 3)

      // A CometCachedBatch records where each column's buffers sit in its payload, so a scan
      // copies out and decompresses only what it projected and cost tracks the width of the
      // projection. These three cases span that range over one cached relation: no columns, one
      // column, and all six.
      runCacheBenchmark(
        flatRelation,
        "in-memory cache row count only (0 of 6 columns)",
        s"SELECT count(*) FROM $cacheTable",
        scanned = 0)

      runCacheBenchmark(
        flatRelation,
        "in-memory cache narrow projection (1 of 6 columns)",
        s"SELECT count(k) FROM $cacheTable",
        scanned = 1)

      runCacheBenchmark(
        flatRelation,
        "in-memory cache full projection (6 of 6 columns)",
        s"SELECT count(id), count(k), count(v), count(s1), count(s2), count(s3) FROM $cacheTable",
        scanned = 6)

      // The same three widths over a relation whose columns are structs. A struct column's buffers
      // are a run as long as its subtree rather than the two or three a flat column owns, so the
      // per-column bookkeeping the projected read does is proportionally a smaller share of the work
      // here -- which is what these cases measure against the flat ones above.
      //
      // The aggregates reach into a field rather than counting the struct whole, because a struct
      // built this way is non-nullable and `count(c)` over a non-nullable column is rewritten to
      // `count(1)`. Either way the cache scan selects whole top-level columns, so one field is
      // enough to decode all of that column's buffers.
      runCacheBenchmark(
        nestedRelation,
        "in-memory cache nested row count only (0 of 6 columns)",
        s"SELECT count(*) FROM $nestedCacheTable",
        scanned = 0)

      runCacheBenchmark(
        nestedRelation,
        "in-memory cache nested narrow projection (1 of 6 columns)",
        s"SELECT count(deep.n.v) FROM $nestedCacheTable",
        scanned = 1)

      runCacheBenchmark(
        nestedRelation,
        "in-memory cache nested full projection (6 of 6 columns)",
        s"SELECT count(id), count(sc.a), count(deep.n.v), count(wide.p), count(tail), count(d) " +
          s"FROM $nestedCacheTable",
        scanned = 6)
    }
  }

  private def runCacheBenchmark(
      relation: CachedRelation,
      name: String,
      query: String,
      scanned: Int): Unit = {
    withCachedTable(relation) {
      withSQLConf(cacheConf(nativeCacheEnabled = false): _*) {
        verifyPlan(query, nativeCacheEnabled = false, scanned)
      }
      withSQLConf(cacheConf(nativeCacheEnabled = true): _*) {
        verifyPlan(query, nativeCacheEnabled = true, scanned)
      }

      val benchmark = new Benchmark(name, relation.rows, output = output)

      benchmark.addCase("Spark cache scan + CometSparkColumnarToColumnar") { _ =>
        withSQLConf(cacheConf(nativeCacheEnabled = false): _*) {
          spark.sql(query).noop()
        }
      }

      benchmark.addCase("CometInMemoryTableScan") { _ =>
        withSQLConf(cacheConf(nativeCacheEnabled = true): _*) {
          spark.sql(query).noop()
        }
      }

      benchmark.run()
    }
  }

  private def withCachedTable(relation: CachedRelation)(f: => Unit): Unit = {
    spark.catalog.clearCache()

    // Materialize the cache once using Comet's cache serializer, then read it both ways.
    //
    // What the two cases isolate is the cache-scan boundary, not the execution engine above it.
    // cacheConf turns Comet execution on for both, so the aggregation runs on Comet either way;
    // the only flag that moves is COMET_EXEC_IN_MEMORY_CACHE_ENABLED. Disabled, Spark's
    // InMemoryTableScanExec feeds those same Comet operators through a
    // CometSparkColumnarToColumnar bridge; enabled, CometInMemoryTableScan feeds them directly.
    // So the numbers measure "keep the cached scan native" against "fall back to a Spark cache
    // scan and convert" -- which is the overhead this feature exists to remove.
    //
    // Neither case is a baseline for Spark's own cache format. spark.sql.cache.serializer is a
    // static conf, so a single session cannot also materialize a DefaultCachedBatch to compare
    // against; both cases read the same Comet-written CometCachedBatch.
    withSQLConf(cacheConf(nativeCacheEnabled = true): _*) {
      spark
        .sql(s"SELECT ${relation.columns.mkString(", ")} FROM ${relation.source}")
        .createOrReplaceTempView(relation.table)
      spark.catalog.cacheTable(relation.table)
      spark.table(relation.table).count()
    }

    try f
    finally {
      spark.catalog.uncacheTable(relation.table)
      spark.catalog.clearCache()
    }
  }

  // Pins the shape the case labels claim: enabled reads the cache natively with no conversion,
  // disabled reads it through Spark's cache scan and a CometSparkColumnarToColumnar bridge. The
  // bridge is what makes the disabled case a scan-boundary comparison rather than a Spark-vs-Comet
  // execution one, since a Spark-columnar-to-Arrow transition only exists to feed Comet operators.
  //
  // The projection width is checked too, because a case that reads fewer columns than its label
  // says is not slightly off, it is measuring a different query: an optimizer rule that rewrites
  // the aggregate can prune a column out of the scan entirely.
  private def verifyPlan(query: String, nativeCacheEnabled: Boolean, scanned: Int): Unit = {
    val executed = spark.sql(query).queryExecution.executedPlan
    val plan = executed.toString()

    if (nativeCacheEnabled) {
      assert(plan.contains("CometInMemoryTableScan"), s"Expected native cache scan:\n$plan")
      assert(!plan.contains("CometSparkColumnarToColumnar"), s"Unexpected conversion:\n$plan")
    } else {
      assert(
        !plan.contains("CometInMemoryTableScan"),
        s"Native cache scan should be disabled:\n$plan")
      assert(
        plan.contains("CometSparkColumnarToColumnar"),
        s"Expected the fallback read to bridge into Comet operators:\n$plan")
    }

    val scanOutputs = executed.collect {
      case s: CometInMemoryTableScanExec => s.scanOutput
      case s: InMemoryTableScanExec => s.attributes
    }
    assert(scanOutputs.length == 1, s"Expected exactly one cache scan:\n$plan")
    assert(
      scanOutputs.head.length == scanned,
      s"Expected the scan to read $scanned columns, got " +
        s"${scanOutputs.head.map(_.name).mkString("[", ",", "]")}:\n$plan")
  }

  private def cacheConf(nativeCacheEnabled: Boolean): Seq[(String, String)] = {
    Seq(
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> nativeCacheEnabled.toString,
      "spark.comet.sparkToColumnar.enabled" -> "true",
      "spark.comet.exec.onHeap.enabled" -> "true",
      "spark.sql.inMemoryColumnarStorage.batchSize" -> "10000")
  }
}
