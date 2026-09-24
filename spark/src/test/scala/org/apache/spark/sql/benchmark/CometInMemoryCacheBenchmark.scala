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

import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.comet.CometInMemoryTableScanExec
import org.apache.spark.sql.comet.execution.arrow.ArrowCachedBatchSerializer
import org.apache.spark.sql.execution.columnar.{CometInMemoryRelationHelper, DefaultCachedBatchSerializer, InMemoryRelation, InMemoryTableScanExec}
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.types.{DataType, LongType, StringType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

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

  // Every value spark.comet.exec.inMemoryCache.compression.codec accepts, default first. Arrow's
  // other IPC codec, LZ4_FRAME, is not one of them: it is commons-compress's pure-Java LZ4 rather
  // than the JNI-accelerated lz4-java behind spark.io.compression.codec, and the write path
  // rejects it.
  private val codecs = Seq("zstd", "none")

  @volatile private var statsResult: (Array[Any], Array[Any], Array[Int]) = _

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
    runStatsBenchmark()
    // Run just the JVM statistics loop without constructing or scanning a cached relation.
    if (args.contains("--stats-only")) return

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
        "SELECT count(id), count(sc.a), count(deep.n.v), count(wide.p), count(tail), count(d) " +
          s"FROM $nestedCacheTable",
        scanned = 6)

      runCodecBenchmark(flatRelation)
      runSparkOperatorBenchmark(flatRelation)
    }
  }

  /**
   * Every write codec the config accepts, over the same relation.
   *
   * The codec decides both how long materializing a relation takes and how much of a read is
   * decompression, and it is chosen once per relation, so it is a separate axis from the
   * projection widths above rather than another case alongside them.
   *
   * Only one copy is cached at a time. Two views over the same query would not give two cached
   * relations: the cache manager keys on the plan rather than the name, so the second would find
   * the first's copy and every codec would end up measuring whichever wrote last.
   *
   * Every case reads the cache natively. What moves between them is the codec, not the scan.
   */
  private def runCodecBenchmark(relation: CachedRelation): Unit = {
    val view = s"${relation.table}_codec"

    spark.catalog.clearCache()
    withTempTable(view) {
      spark
        .sql(s"SELECT ${relation.columns.mkString(", ")} FROM ${relation.source}")
        .createOrReplaceTempView(view)

      var cached: String = null
      def cacheUnder(codec: String): Unit = if (cached != codec) {
        spark.catalog.uncacheTable(view)
        cached = null
        withSQLConf(cacheConf(nativeCacheEnabled = true) ++ codecConf(codec): _*) {
          spark.catalog.cacheTable(view)
          spark.table(view).count()
        }
        cached = codec
      }

      // Measured before the cases below, while each codec's copy is the one freshly materialized:
      // the stats behind it belong to whichever copy is cached now.
      val footprints = codecs.map { codec =>
        cacheUnder(codec)
        codec -> cachedBytes(view)
      }

      val materialize =
        new Benchmark("in-memory cache materialize by codec", relation.rows, output = output)
      codecs.foreach { codec =>
        // Timed around the caching alone: dropping the previous copy is setup, and a plain
        // addCase would charge it to whichever codec happens to be running.
        materialize.addTimerCase(codec) { timer =>
          spark.catalog.uncacheTable(view)
          cached = null
          withSQLConf(cacheConf(nativeCacheEnabled = true) ++ codecConf(codec): _*) {
            timer.startTiming()
            spark.catalog.cacheTable(view)
            spark.table(view).count()
            timer.stopTiming()
          }
          cached = codec
        }
      }
      materialize.run()

      // Footprint is not a time, so it has no column in a Benchmark table, but it is the other
      // half of why one codec is the default.
      footprints.foreach { case (codec, bytes) =>
        materialize.out.println(
          f"Cached footprint ($codec): ${bytes / (1024.0 * 1024.0)}%.1f MiB")
      }

      // The two projection widths a codec can tell apart: a read decompresses only the buffers it
      // selected, so a narrow projection pays a proportionally smaller share of the codec's cost.
      // A row count decodes nothing at all and so cannot distinguish them.
      Seq(
        ("narrow projection (1 of 6 columns)", s"SELECT count(k) FROM $view", 1),
        (
          "full projection (6 of 6 columns)",
          s"SELECT count(id), count(k), count(v), count(s1), count(s2), count(s3) FROM $view",
          6)).foreach { case (label, query, scanned) =>
        // The plan does not depend on the codec, so whichever copy is cached now will do.
        withSQLConf(cacheConf(nativeCacheEnabled = true): _*) {
          verifyPlan(query, nativeCacheEnabled = true, scanned)
        }
        val benchmark =
          new Benchmark(s"in-memory cache $label by codec", relation.rows, output = output)
        codecs.foreach { codec =>
          // Re-caching under this case's codec is setup, so it is outside the timer, and it only
          // happens on the case's first call, which is a warmup iteration.
          benchmark.addTimerCase(codec) { timer =>
            cacheUnder(codec)
            withSQLConf(cacheConf(nativeCacheEnabled = true): _*) {
              timer.startTiming()
              spark.sql(query).noop()
              timer.stopTiming()
            }
          }
        }
        benchmark.run()
      }

      spark.catalog.uncacheTable(view)
    }
  }

  /**
   * Reads that feed Spark operators rather than Comet ones, against Spark's own cache format.
   *
   * Comet is off in every case, so this measures Spark consuming the cached data: the shape where
   * Comet's format has something to lose, and the reason the feature is off by default. Both
   * formats are cached from the same relation, one copy at a time as in runCodecBenchmark, and
   * each case checks which serializer cached the relation it reads.
   */
  private def runSparkOperatorBenchmark(relation: CachedRelation): Unit = {
    val view = s"${relation.table}_spark_operators"
    val formats = Seq(
      "Spark's cache format" -> classOf[DefaultCachedBatchSerializer].getName,
      "Comet's cache format" -> classOf[ArrowCachedBatchSerializer].getName)

    spark.catalog.clearCache()
    withTempTable(view) {
      spark
        .sql(s"SELECT ${relation.columns.mkString(", ")} FROM ${relation.source}")
        .createOrReplaceTempView(view)

      var cachedBy: String = null
      def cacheBy(serializer: String): Unit = if (cachedBy != serializer) {
        spark.catalog.uncacheTable(view)
        cachedBy = null
        withCacheSerializer(serializer) {
          withSQLConf(sparkOperatorConf: _*) {
            spark.catalog.cacheTable(view)
            spark.table(view).count()
          }
        }
        cachedBy = serializer
      }

      Seq(
        ("row count only (0 of 6 columns)", s"SELECT count(*) FROM $view", 0),
        ("narrow projection (1 of 6 columns)", s"SELECT count(k) FROM $view", 1),
        ("3 of 6 columns", s"SELECT sum(id), sum(k), sum(v) FROM $view", 3),
        (
          "full projection (6 of 6 columns)",
          s"SELECT count(id), count(k), count(v), count(s1), count(s2), count(s3) FROM $view",
          6)).foreach { case (label, query, scanned) =>
        val benchmark = new Benchmark(
          s"in-memory cache read by Spark operators, $label",
          relation.rows,
          output = output)
        formats.foreach { case (name, serializer) =>
          var verified = false
          // Re-caching in this case's format is setup, so it is outside the timer, and it only
          // happens on the case's first call, which is a warmup iteration.
          benchmark.addTimerCase(name) { timer =>
            cacheBy(serializer)
            withSQLConf(sparkOperatorConf: _*) {
              if (!verified) {
                verifySparkOperatorRead(query, scanned, serializer)
                verified = true
              }
              timer.startTiming()
              spark.sql(query).noop()
              timer.stopTiming()
            }
          }
        }
        benchmark.run()
      }

      spark.catalog.uncacheTable(view)
    }
  }

  // spark.sql.cache.serializer is static, and InMemoryRelation memoizes the serializer it names
  // for the life of the JVM. It looks the name up in the active session's conf when a relation is
  // cached, though, so setting it there directly and clearing the memoized instance around one
  // materialization is enough to cache a relation in either format from the same session.
  private def withCacheSerializer(serializer: String)(f: => Unit): Unit = {
    val conf = SQLConf.get
    val key = StaticSQLConf.SPARK_CACHE_SERIALIZER.key
    val previous = conf.getConfString(key)
    conf.setConfString(key, serializer)
    CometInMemoryRelationHelper.clearSerializer()
    try f
    finally {
      conf.setConfString(key, previous)
      CometInMemoryRelationHelper.clearSerializer()
    }
  }

  // Pins what a Spark-operator case claims: no Comet operator anywhere, and one cache scan that
  // reads the columns its label counts from a relation the named serializer cached. The last is
  // what catches both formats silently reading one copy.
  private def verifySparkOperatorRead(query: String, scanned: Int, serializer: String): Unit = {
    val executed = spark.sql(query).queryExecution.executedPlan
    val plan = executed.toString()
    assert(executed.find(_.nodeName.startsWith("Comet")).isEmpty, s"Expected no Comet:\n$plan")
    val scans = executed.collect { case s: InMemoryTableScanExec => s }
    assert(scans.length == 1, s"Expected exactly one cache scan:\n$plan")
    assert(
      scans.head.attributes.length == scanned,
      s"Expected the scan to read $scanned columns:\n$plan")
    val actual = scans.head.relation.cacheBuilder.serializer.getClass.getName
    assert(actual == serializer, s"Expected a relation cached by $serializer, not $actual")
  }

  /** What the cached relation behind `view` occupies, summed over its batches as written. */
  private def cachedBytes(view: String): Long = {
    val relation = spark
      .table(view)
      .queryExecution
      .optimizedPlan
      .collectFirst { case r: InMemoryRelation => r }
      .getOrElse(sys.error(s"$view is not cached"))
    // computeStats rather than the builder's size accumulator, which Spark 4.2 replaced. Before
    // the buffers load it falls back to the plan's estimate, so insist they have.
    assert(relation.cacheBuilder.isCachedColumnBuffersLoaded, s"$view is not materialized")
    relation.computeStats().sizeInBytes.toLong
  }

  private def runStatsBenchmark(): Unit = {
    val batchSize = 10000
    val types: Seq[DataType] = Seq.fill(3)(LongType) ++ Seq.fill(3)(StringType)
    val attrs = types.zipWithIndex.map { case (dt, i) => AttributeReference(s"c$i", dt)() }
    val columns = types.map(dt => new OnHeapColumnVector(batchSize, dt))
    val batch = new ColumnarBatch(columns.map(c => c: ColumnVector).toArray, batchSize)
    try {
      var r = 0
      while (r < batchSize) {
        columns(0).putLong(r, r.toLong)
        columns(1).putLong(r, r % 1000)
        columns(2).putLong(r, r + 1)
        columns(3).putByteArray(r, s"str_a_${r % 100000}".getBytes(StandardCharsets.UTF_8))
        columns(4).putByteArray(r, s"str_b_${r % 7919}".getBytes(StandardCharsets.UTF_8))
        columns(5).putByteArray(r, s"str_c_$r".getBytes(StandardCharsets.UTF_8))
        r += 1
      }
      val serializer = new ArrowCachedBatchSerializer
      // Resolved outside the timed loop because that is where the serializer resolves it: once
      // per partition, not once per batch.
      val orderings = serializer.boundsOrderings(attrs)
      val benchmark = new Benchmark("in-memory cache statistics", numRows, output = output)
      // One case measures this collector across commits; Spark's default cache has its own collector.
      benchmark.addCase("Comet statistics collector") { _ =>
        var i = 0
        while (i < numRows / batchSize) {
          statsResult = serializer.gatherColumnStats(batch, attrs, orderings)
          i += 1
        }
      }
      benchmark.run()
    } finally batch.close()
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
    // Neither case is a baseline for Spark's own cache format: both read the same Comet-written
    // CometCachedBatch. Spark's format is only measured by runSparkOperatorBenchmark, with Comet
    // off, since that is the only comparison it answers.
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

  private def codecConf(codec: String): Seq[(String, String)] =
    Seq(CometConf.COMET_EXEC_IN_MEMORY_CACHE_COMPRESSION_CODEC.key -> codec)

  // Comet off, so every operator above the cache scan is Spark's; the batch size matches
  // cacheConf so both formats cache the relation in the same number of batches.
  private val sparkOperatorConf: Seq[(String, String)] = Seq(
    CometConf.COMET_ENABLED.key -> "false",
    CometConf.COMET_EXEC_ENABLED.key -> "false",
    "spark.sql.inMemoryColumnarStorage.batchSize" -> "10000")
}
