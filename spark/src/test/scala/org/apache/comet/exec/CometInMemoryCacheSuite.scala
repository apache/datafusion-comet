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

import java.{util => ju}
import java.nio.charset.StandardCharsets

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

import org.apache.arrow.compression.ZstdCompressionCodec
import org.apache.arrow.memory.{ArrowBuf, BufferAllocator}
import org.apache.arrow.vector.{BitVector, FixedSizeBinaryVector, IntVector, VarBinaryVector, VarCharVector}
import org.apache.arrow.vector.compression.{CompressionCodec, CompressionUtil, NoCompressionCodec}
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.spark.CometDriverPlugin
import org.apache.spark.SparkConf
import org.apache.spark.sql.{CometTestBase, DataFrame, Observation, QueryTest, Row}
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, LessThan, Literal}
import org.apache.spark.sql.columnar.{CachedBatch, SimpleMetricsCachedBatch}
import org.apache.spark.sql.comet.{CometBroadcastHashJoinExec, CometInMemoryTableScanExec, CometSortAggregateExec, CometSortExec, CometSortMergeJoinExec}
import org.apache.spark.sql.comet.execution.arrow.{ArrowCachedBatchSerializer, CometCachedBatchHelper}
import org.apache.spark.sql.comet.execution.shuffle.CometCelebornShuffleManager
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.execution.{ColumnarToRowExec, CometSparkPlanInfoHelper, FilterExec, FormattedMode, RowToColumnarExec, SortExec, SparkPlanInfo}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AQEShuffleReadExec, QueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.columnar.{CometInMemoryRelationHelper, InMemoryRelation, InMemoryTableScanExec}
import org.apache.spark.sql.execution.exchange.{Exchange, ReusedExchangeExec, ShuffleExchangeLike}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.functions.{count, lit, max, min, sum}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.storage.StorageLevel

import org.apache.comet.{CometArrowAllocator, CometConf, CometKryoRegistrator, ExtendedExplainInfo}
import org.apache.comet.CometSparkSessionExtensions.{isSpark35Plus, isSpark40Plus}
import org.apache.comet.rules.CometCacheColumnarRule
import org.apache.comet.vector.{CometPlainVector, CometVector}

class CometInMemoryCacheSuite extends CometTestBase {

  import testImplicits._

  // `InMemoryRelation` resolves `spark.sql.cache.serializer` once per JVM and memoizes the
  // instance in a static field. Test suites share a forked JVM, so whichever suite caches a
  // table first pins the serializer for everything that follows: without this reset the
  // serializer configured below is ignored and every cached batch here is a `DefaultCachedBatch`.
  // Clear it on the way out as well so this suite does not pin Comet's serializer for the rest
  // of the JVM.
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

  override protected def sparkConf: SparkConf = {
    val conf = new SparkConf()
    conf.set("spark.driver.memory", "1G")
    conf.set("spark.executor.memory", "1G")
    conf.set("spark.executor.memoryOverhead", "2G")
    conf.set("spark.plugins", "org.apache.spark.CometPlugin")
    conf.set(
      "spark.shuffle.manager",
      "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager")
    conf.set("spark.comet.enabled", "true")
    conf.set("spark.comet.exec.enabled", "true")
    conf.set("spark.comet.exec.onHeap.enabled", "true")
    conf.set("spark.comet.metrics.enabled", "true")
    conf.set(
      "spark.sql.cache.serializer",
      "org.apache.spark.sql.comet.execution.arrow.ArrowCachedBatchSerializer")
    conf
  }

  /**
   * `withSQLConf` that also turns on the Spark-to-Arrow conversions, which this suite's conf
   * leaves off, unlike CometTestBase's.
   */
  private def withConversions(pairs: (String, String)*)(f: => Unit): Unit =
    withSQLConf(pairs ++ sparkToArrowConversionConfs(enabled = true): _*)(f)

  private def cachedBatchTypes(table: String): Array[String] = {
    val cached = spark.sharedState.cacheManager.lookupCachedData(spark.table(table)).get
    cached.cachedRepresentation.cacheBuilder.cachedColumnBuffers
      .map(_.getClass.getName)
      .distinct()
      .collect()
  }

  // Disabling Comet does not bypass an existing cache: both readers would still consume the
  // same serialized values. Materialize every Spark reference before registering the cache.
  private def uncachedSparkAnswer(query: String): Array[Row] = {
    var expected = Array.empty[Row]
    withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
      val df = spark.sql(query)
      assert(
        df.queryExecution.withCachedData.collect { case r: InMemoryRelation => r }.isEmpty,
        "the reference answer must not read a cached relation")
      expected = df.collect()
    }
    expected
  }

  // The tests below are ported from Spark 4.1.2's AdaptiveQueryExecSuite; see each source link.
  private def withAQECache(f: => Unit): Unit = {
    withConversions(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.CAN_CHANGE_CACHED_PLAN_OUTPUT_PARTITIONING.key -> "true",
      SQLConf.SHUFFLE_PARTITIONS.key -> "3",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm",
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true") {
      try {
        f
      } finally {
        spark.catalog.clearCache()
      }
    }
  }

  // https://github.com/apache/spark/blob/v4.1.2/sql/core/src/test/scala/org/apache/spark/sql/execution/adaptive/AdaptiveQueryExecSuite.scala#L3114-L3154
  test("AQE SPARK-42101: cold and warm Comet cache materialization") {
    assume(isSpark35Plus, "Table-cache query stages require Spark 3.5+")
    withAQECache {
      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
        val left = spark.range(0, 10, 1, 2).selectExpr("cast(id as string) c1")
        val right = spark.range(0, 10, 1, 2).selectExpr("cast(id as string) c2")
        val cached = left.join(right, $"c1" === $"c2").cache()
        val builder = spark.sharedState.cacheManager
          .lookupCachedData(cached)
          .get
          .cachedRepresentation
          .cacheBuilder

        Seq(true, false).foreach { firstAccess =>
          val df = cached.groupBy("c1").agg(max($"c2"))
          val adaptive = df.queryExecution.executedPlan.asInstanceOf[AdaptiveSparkPlanExec]
          assert(!adaptive.isFinalPlan)
          assert(builder.isCachedColumnBuffersLoaded != firstAccess)
          assert(
            collect(adaptive) { case s: ShuffleExchangeLike => s }.size ==
              (if (firstAccess) 1 else 0))
          assert(collect(adaptive) { case s: CometInMemoryTableScanExec => s }.size == 1)

          checkAnswer(df, (0L until 10L).map(i => Row(i.toString, i.toString)))
          assert(adaptive.isFinalPlan)
          assert(builder.isCachedColumnBuffersLoaded)
          assert(collect(adaptive) { case s: ShuffleExchangeLike => s }.isEmpty)
          assert(collect(adaptive) { case s @ (_: CometSortExec | _: SortExec) => s }.isEmpty)
          assert(collect(adaptive) { case s: CometInMemoryTableScanExec => s }.size == 1)
        }
      }
    }
  }

  // https://github.com/apache/spark/blob/v4.1.2/sql/core/src/test/scala/org/apache/spark/sql/execution/adaptive/AdaptiveQueryExecSuite.scala#L3156-L3176
  test("AQE SPARK-42101: preserve shuffle partitions beside a table cache stage") {
    assume(isSpark35Plus, "Table-cache query stages require Spark 3.5+")
    withAQECache {
      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM.key -> "1") {
        val cached = Seq(1, 2).toDF("c1").repartition(3, $"c1").cache()
        val df = cached.join(Seq(1, 2).toDF("c2"), $"c1" === $"c2")
        checkAnswer(df, Seq(Row(1, 1), Row(2, 2)))
        val plan = df.queryExecution.executedPlan
        assert(plan.asInstanceOf[AdaptiveSparkPlanExec].isFinalPlan)
        assert(collect(plan) { case s: QueryStageExec if isTableCacheStage(s) => s }.size == 1)
        assert(collect(plan) { case s: CometInMemoryTableScanExec => s }.size == 1)
        assert(collect(plan) { case s: ShuffleQueryStageExec => s }.size == 1)
        assert(collect(plan) { case s: AQEShuffleReadExec => s }.isEmpty)
      }
    }
  }

  // https://github.com/apache/spark/blob/v4.1.2/sql/core/src/test/scala/org/apache/spark/sql/execution/adaptive/AdaptiveQueryExecSuite.scala#L3178-L3191
  test("AQE SPARK-42101: coalesce the shuffle partitions of a union with a table cache stage") {
    assume(isSpark35Plus, "Table-cache query stages require Spark 3.5+")
    withAQECache {
      withSQLConf(SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM.key -> "1") {
        val cached = Seq(1).toDF("c").cache()
        val df = Seq(2).toDF("c").repartition($"c").union(cached)
        checkAnswer(df, Seq(Row(1), Row(2)))
        val plan = df.queryExecution.executedPlan
        assert(plan.asInstanceOf[AdaptiveSparkPlanExec].isFinalPlan)
        assert(collect(plan) { case u: org.apache.spark.sql.comet.CometUnionExec => u }.size == 1)
        assert(collect(plan) { case r @ AQEShuffleReadExec(_: ShuffleQueryStageExec, _) =>
          r
        }.size == 1)
        assert(collect(plan) { case s: QueryStageExec if isTableCacheStage(s) => s }.size == 1)
        assert(collect(plan) { case s: CometInMemoryTableScanExec => s }.size == 1)
      }
    }
  }

  // https://github.com/apache/spark/blob/v4.1.2/sql/core/src/test/scala/org/apache/spark/sql/execution/adaptive/AdaptiveQueryExecSuite.scala#L2780-L2832
  test("AQE SPARK-37742: use valid Comet cache statistics for join selection") {
    withAQECache {
      // Spark's own threshold. The large cache has to stay above it once materialized: its 60k
      // 20-byte keys are about 1.4 MB decoded but compress to a small fraction of that, so a cache
      // that reported its compressed size would be broadcast by the third join.
      withSQLConf(
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "1048584",
        SQLConf.ADAPTIVE_OPTIMIZER_EXCLUDED_RULES.key ->
          "org.apache.spark.sql.execution.adaptive.AQEPropagateEmptyRelation") {
        withTempView("cache_large", "cache_other", "cache_small") {
          val key = "00112233445566778899"
          Seq.fill(60000)(key).toDF("key").createOrReplaceTempView("cache_large")
          Seq
            .fill(60000)("11223344556677889900")
            .toDF("key")
            .createOrReplaceTempView("cache_other")
          Seq(key).toDF("key").createOrReplaceTempView("cache_small")
          val cached = spark.sql("SELECT key AS newKey FROM cache_large").cache()
          val relation =
            spark.sharedState.cacheManager.lookupCachedData(cached).get.cachedRepresentation
          assert(!relation.cacheBuilder.isCachedColumnBuffersLoaded)
          val df = spark.sql("""
            SELECT t3.newKey FROM
              (SELECT t1.newKey FROM (SELECT key AS newKey FROM cache_large) t1
               JOIN cache_small t2 ON t1.newKey = t2.key) t3
            JOIN cache_other t4 ON t3.newKey = t4.key
            UNION
            SELECT t1.newKey FROM (SELECT key AS newKey FROM cache_large) t1
            JOIN cache_other t2 ON t1.newKey = t2.key
          """)
          checkAnswer(df, Seq.empty[Row])
          val plan = df.queryExecution.executedPlan
          assert(plan.asInstanceOf[AdaptiveSparkPlanExec].isFinalPlan)
          assert(collect(plan) { case s: CometInMemoryTableScanExec => s }.nonEmpty)
          assert(collect(plan) {
            case j @ (_: CometBroadcastHashJoinExec | _: BroadcastHashJoinExec) => j
          }.size == 1)
          assert(collect(plan) { case j @ (_: CometSortMergeJoinExec | _: SortMergeJoinExec) =>
            j
          }.size == 2)
          val batches = relation.cacheBuilder.cachedColumnBuffers.collect()
          assert(batches.forall(
            _.getClass.getName == "org.apache.spark.sql.comet.execution.arrow.CometCachedBatch"))
          assert(batches.map(_.numRows.toLong).sum == 60000L)
          val stats = relation.computeStats()
          assert(stats.rowCount.contains(BigInt(60000)))
          assert(stats.sizeInBytes == batches.map(_.sizeInBytes).sum)
          assert(stats.sizeInBytes > 1048584L)
        }
      }
    }
  }

  // https://github.com/apache/datafusion-comet/issues/6202
  test("AQE keeps the operators above a table cache stage native once the stage materializes") {
    assume(isSpark35Plus, "Table-cache query stages require Spark 3.5+")
    // `t` holds 1000 ids for each k, and the ids for a given k sum to 1000 * k + 4995000.
    val queries = Seq(
      "SELECT k, count(*) FROM t GROUP BY k" -> (0L until 10L).map(k => Row(k, 1000L)),
      "SELECT name, sum(id) FROM t JOIN d ON k = k2 GROUP BY name" ->
        (0L until 10L).map(k => Row(s"n$k", 1000L * k + 4995000L)),
      "SELECT sum(id) FROM t WHERE k = 3" -> Seq(Row(4998000L)))
    withTempView("t", "d") {
      spark
        .range(0, 10000, 1, 4)
        .selectExpr("id", "id % 10 AS k")
        .createOrReplaceTempView("t")
      spark
        .range(10)
        .selectExpr("id AS k2", "concat('n', id) AS name")
        .createOrReplaceTempView("d")
      for {
        nativeCache <- Seq(true, false)
        (query, expected) <- queries
      } {
        withAQECache {
          withSQLConf(CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> nativeCache.toString) {
            spark.catalog.cacheTable("t")
            spark.catalog.cacheTable("d")
            val scanClass: Class[_] =
              if (nativeCache) classOf[CometInMemoryTableScanExec]
              else classOf[InMemoryTableScanExec]
            // The first run materializes the cache through the stage and the second reads it warm.
            // checkToRDD = false keeps checkAnswer from warming the cache with a run of its own.
            Seq("cold", "warm").foreach { cache =>
              val df = sql(query)
              QueryTest.checkAnswer(df, expected, checkToRDD = false)
              val plan = df.queryExecution.executedPlan
              withClue(s"nativeCache=$nativeCache, $cache cache: $query\n$plan\n") {
                val cacheScans = collect(plan) {
                  case s: QueryStageExec if isTableCacheStage(s) => s.plan
                }
                assert(cacheScans.nonEmpty)
                assert(cacheScans.forall(scanClass.isInstance))
                checkCometOperatorsInFinalPlan(plan, classOf[InMemoryTableScanExec])
              }
            }
          }
        }
      }
    }
  }

  test("CometInMemoryTableScan over CometCachedBatch") {
    withConversions(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm",
      SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> "true",
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true") {

      spark.catalog.clearCache()

      spark
        .range(1000)
        .selectExpr("id as key", "id % 8 as value")
        .createOrReplaceTempView("abc")

      spark.catalog.cacheTable("abc")
      spark.table("abc").count()

      assert(
        cachedBatchTypes("abc").sameElements(
          Array("org.apache.spark.sql.comet.execution.arrow.CometCachedBatch")))

      val df = spark.sql("SELECT key, count(*) FROM abc GROUP BY key")
      checkAnswer(df, (0L until 1000L).map(i => Row(i, 1L)))

      val plan = df.queryExecution.executedPlan.toString()
      assert(plan.contains("CometInMemoryTableScan"))
      assert(!plan.contains("CometSparkColumnarToColumnar"))

      spark.catalog.clearCache()
    }
  }

  test("CometInMemoryTableScan is described by the Spark scan it replaces") {
    // With every constructor field printed, the node dumped its CachedRDDBuilder, the whole cached
    // plan both physical and logical, into the middle of every plan that read the cache, raw
    // newlines and all, in the tree string and in EXPLAIN FORMATTED alike.
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true") {
      spark.catalog.clearCache()
      spark
        .range(1000)
        .selectExpr("id AS key", "id % 8 AS value")
        .createOrReplaceTempView("explain_cache")
      spark.catalog.cacheTable("explain_cache")
      try {
        val df = spark.sql("SELECT key FROM explain_cache WHERE value = 3")
        df.collect()
        val plan = df.queryExecution.executedPlan
        val scans = collect(plan) { case s: CometInMemoryTableScanExec => s }
        assert(scans.size == 1)
        val line = scans.head.simpleString(SQLConf.get.maxToStringFields)
        assert(
          line.startsWith("CometInMemoryTableScan Scan In-memory table explain_cache ["),
          line)
        assert(line.contains("= 3)"), s"the pruning predicates should be shown: $line")
        // EXPLAIN FORMATTED also details the InMemoryRelation drawn below the scan. Before Spark
        // 4.0 that detail prints the relation's CachedRDDBuilder below Spark's own scan as well
        // (SPARK-51861), so there only the scan's own detail is checked.
        Seq(
          plan.treeString,
          if (isSpark40Plus) df.queryExecution.explainString(FormattedMode)
          else scans.head.verboseStringWithOperatorId())
          .foreach { text =>
            assert(!text.contains("CachedRDDBuilder"), text)
            assert(!text.contains(classOf[ArrowCachedBatchSerializer].getName), text)
          }
      } finally {
        spark.catalog.clearCache()
      }
    }
  }

  test("EXPLAIN draws the cached plan below CometInMemoryTableScan") {
    // Spark's own cache scan draws its InMemoryRelation, and below that the plan that built the
    // relation. See https://github.com/apache/datafusion-comet/issues/6572.
    def nodeLines(tree: String): Seq[String] =
      tree.linesIterator.map(_.replaceAll("^[ :+-]*", "")).toSeq

    Seq("false", "true").foreach { aqe =>
      withClue(s"AQE $aqe: ") {
        withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe,
          CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true") {
          withTempView("explain_cached_plan") {
            spark
              .range(1000)
              .selectExpr("id AS key", "id % 8 AS value")
              .createOrReplaceTempView("explain_cached_plan")
            // The cached plan is planned here. Keeping its Range on Spark gives it a fallback
            // reason that the reporting for a query reading the cache must leave out.
            withSQLConf(
              CometConf.COMET_EXEC_RANGE_ENABLED.key -> "false",
              CometConf.COMET_CONVERT_FROM_RANGE_ENABLED.key -> "false") {
              spark.catalog.cacheTable("explain_cached_plan")
            }
            val df = spark.sql("SELECT value, count(*) FROM explain_cached_plan GROUP BY value")
            df.collect()
            val plan = df.queryExecution.executedPlan
            val scans = collect(plan) { case s: CometInMemoryTableScanExec => s }
            assert(scans.size == 1, plan)
            val relation = scans.head.originalPlan.relation

            // The relation's line, then the cached plan's lines, follow the scan's line.
            val lines = nodeLines(plan.treeString)
            val below = nodeLines(relation.treeString)
            assert(below.size > 1, relation)
            val scanAt = lines.indexWhere(_.startsWith("CometInMemoryTableScan "))
            assert(lines.startsWith(below, scanAt + 1), plan)

            // EXPLAIN FORMATTED numbers the relation and draws it below the scan too.
            val formatted = df.queryExecution.explainString(FormattedMode)
            val formattedLines = nodeLines(formatted)
            val formattedScanAt =
              formattedLines.indexWhere(_.startsWith("CometInMemoryTableScan ("))
            assert(
              formattedLines(formattedScanAt + 1).startsWith("InMemoryRelation ("),
              formatted)

            // Comet's own reporting leaves the cached plan out.
            val info = new ExtendedExplainInfo()
            val cachedReasons = info.getFallbackReasons(relation.cachedPlan)
            assert(cachedReasons.nonEmpty, relation.cachedPlan)
            val reasons = info.getFallbackReasons(plan)
            assert(cachedReasons.intersect(reasons).isEmpty, reasons)
            val verbose = info.generateVerboseInfo(plan)
            assert(!verbose.contains("InMemoryRelation"), verbose)
          }
        }
      }
    }
  }

  test("the SQL tab and event log draw the cached plan below CometInMemoryTableScan") {
    // Spark builds both from SparkPlanInfo, which gives its own cache scan the cached plan as a
    // child. See https://github.com/apache/datafusion-comet/issues/6463.
    def scanInfos(info: SparkPlanInfo): Seq[SparkPlanInfo] =
      (if (info.nodeName == "CometInMemoryTableScan") Seq(info) else Nil) ++
        info.children.flatMap(scanInfos)

    Seq("false", "true").foreach { aqe =>
      withClue(s"AQE $aqe: ") {
        withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe,
          CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true") {
          withTempView("plan_info_cache") {
            // The shuffle gives the cached plan an adaptive plan of its own when AQE is on.
            spark
              .range(1000)
              .selectExpr("id % 10 AS k")
              .groupBy("k")
              .count()
              .createOrReplaceTempView("plan_info_cache")
            spark.catalog.cacheTable("plan_info_cache")
            val df = spark.sql("SELECT * FROM plan_info_cache WHERE k > 1")
            df.collect()
            val plan = df.queryExecution.executedPlan
            val scans = collect(plan) { case s: CometInMemoryTableScanExec => s }
            assert(scans.size == 1, plan)
            val sparkScan = scans.head.originalPlan
            val cachedPlanInfo =
              CometSparkPlanInfoHelper.fromSparkPlan(sparkScan.relation.cachedPlan)
            // Spark's own scan of the cache sits below, and the cached plan below that.
            val infos = scanInfos(CometSparkPlanInfoHelper.fromSparkPlan(plan))
            assert(
              infos.map(_.children.map(info => (info.nodeName, info.children))) ==
                Seq(Seq((sparkScan.nodeName, Seq(cachedPlanInfo)))),
              plan)
            // Other walkers of subqueries stop at Spark's scan, as in Spark's own plans, so they
            // do not find the cached plan's shuffle in this query, which has none of its own.
            assert(collectWithSubqueries(plan) { case e: ShuffleExchangeLike => e }.isEmpty, plan)
          }
        }
      }
    }
  }

  test("Comet in-memory cache disabled keeps SparkToColumnar fallback path") {
    withConversions(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm",
      SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> "true",
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true") {

      spark.catalog.clearCache()

      spark
        .range(1000)
        .selectExpr("id as key", "id % 8 as value")
        .createOrReplaceTempView("comet_cache_disabled")

      spark.catalog.cacheTable("comet_cache_disabled")
      spark.table("comet_cache_disabled").count()

      assert(
        cachedBatchTypes("comet_cache_disabled").sameElements(
          Array("org.apache.spark.sql.comet.execution.arrow.CometCachedBatch")))
    }

    withConversions(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm",
      SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> "true",
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "false") {

      val df = spark.sql("SELECT key, count(*) FROM comet_cache_disabled GROUP BY key")
      checkAnswer(df, (0L until 1000L).map(i => Row(i, 1L)))

      val plan = df.queryExecution.executedPlan.toString()
      assert(!plan.contains("CometInMemoryTableScan"))
      assert(plan.contains("CometSparkColumnarToColumnar"))

      spark.catalog.clearCache()
    }
  }

  test("Spark row consumers of Comet cache preserve values across batches") {
    for {
      adaptive <- Seq(false, true)
      mode <- Seq("CODEGEN_ONLY", "NO_CODEGEN")
      vectorized <- Seq(false, true)
    } {
      // Comet on with native execution off, so Spark operators consume the cache scan and the
      // generated ones among them read its vectors through the fused transition.
      withSQLConf(
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "false",
        CometConf.COMET_SHUFFLE_ENABLED.key -> "false",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
        SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> vectorized.toString,
        SQLConf.COLUMN_BATCH_SIZE.key -> "7",
        SQLConf.CODEGEN_FACTORY_MODE.key -> mode,
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> (mode == "CODEGEN_ONLY").toString,
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
        SQLConf.SHUFFLE_PARTITIONS.key -> "2") {
        val scalars = Seq(
          "boolean",
          "tinyint",
          "smallint",
          "int",
          "bigint",
          "float",
          "double",
          "decimal(10,2)",
          "decimal(38,2)",
          "date",
          "timestamp",
          "timestamp_ntz").zipWithIndex.map { case (dt, i) =>
          val value = dt match {
            case "date" | "timestamp" | "timestamp_ntz" =>
              s"cast(date_add(DATE '2000-01-01', cast(id AS INT)) AS $dt)"
            case _ => s"cast(id AS $dt)"
          }
          s"if(id % 3 = 0, null, $value) AS c$i"
        }
        val source = spark
          .range(0, 41, 1, 2)
          .selectExpr((Seq("id AS key") ++ scalars ++ Seq(
            "if(id % 3 = 0, null, repeat(concat('字', id), cast(id + 1 AS INT))) AS s",
            "if(id % 3 = 0, null, cast(concat('binary', id) AS BINARY)) AS b",
            "if(id % 3 = 0, null, array(cast(id AS STRING), null)) AS a",
            "if(id % 3 = 0, null, named_struct('x', id, 'a', array(cast(id AS STRING)))) AS st",
            "if(id % 3 = 0, null, map('k', array(cast(id AS STRING), null))) AS m",
            "null AS n")): _*)

        // Each query, and whether a generated Spark operator consumes the cache scan directly.
        // The other consumers (the query root, exchanges and limits) read the row iterator.
        def queries(df: DataFrame): Seq[(DataFrame, Boolean)] = Seq(
          // The generated filter reads every column, so this covers the whole type matrix.
          df.filter($"key" >= 0) -> true,
          df.select("*") -> false,
          df.selectExpr("s AS renamed", "key", "b", "a", "st", "m") -> true,
          df.orderBy($"s".desc, $"key") -> false,
          df.join(spark.range(41).toDF("join_key"), $"key" === $"join_key")
            .select(df("*")) -> false,
          df.selectExpr("count(*)") -> true,
          df.limit(1) -> false)

        val expected = queries(source).map(_._1.collect().toSeq)
        source.cache()
        try {
          assert(source.count() == 41)
          val relation =
            spark.sharedState.cacheManager.lookupCachedData(source).get.cachedRepresentation
          val buffers = relation.cacheBuilder.cachedColumnBuffers.collect()
          assert(buffers.length > 2)
          assert(buffers.forall(_.getClass.getSimpleName == "CometCachedBatch"))
          queries(source).zip(expected).foreach { case ((df, generatedConsumer), answer) =>
            val plan = df.queryExecution.executedPlan
            checkAnswer(df, answer)
            // Inspected after execution, when an adaptive plan is final.
            val scans = collect(plan) { case scan: InMemoryTableScanExec => scan }
            assert(scans.nonEmpty && scans.forall(_.supportsColumnar == vectorized), plan)
            val transitions = collect(plan) {
              case c: ColumnarToRowExec if collect(c.child) { case s: InMemoryTableScanExec =>
                    s
                  }.nonEmpty =>
                c
            }
            val fused = generatedConsumer && vectorized && mode == "CODEGEN_ONLY"
            assert(transitions.size == (if (fused) 1 else 0), plan)
          }
        } finally source.unpersist(blocking = true)
      }
    }
  }

  test("Spark generated cache consumers respect runtime enable and codegen settings") {
    val planOnly = Seq(
      CometConf.COMET_EXPLAIN_PLAN_ONLY_ENABLED.key -> "true",
      // Plan-only mode applies only while native execution is enabled.
      CometConf.COMET_EXEC_ENABLED.key -> "true")
    for {
      adaptive <- Seq(false, true)
      disabledSettings <- Seq(
        Seq(CometConf.COMET_ENABLED.key -> "false"),
        Seq(CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "false"),
        Seq(SQLConf.CODEGEN_FACTORY_MODE.key -> "NO_CODEGEN"),
        Seq(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false"),
        planOnly)
    } {
      withSQLConf(
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "false",
        CometConf.COMET_SHUFFLE_ENABLED.key -> "false",
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
        SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> "true",
        SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
        SQLConf.CODEGEN_FACTORY_MODE.key -> "CODEGEN_ONLY",
        SQLConf.COLUMN_BATCH_SIZE.key -> "7",
        SQLConf.SHUFFLE_PARTITIONS.key -> "2") {
        val source = spark
          .range(0, 41, 1, 2)
          .selectExpr("id AS key", "if(id % 3 = 0, null, concat('字', id)) AS s")
        def query = source
          .filter("key >= 7")
          .selectExpr("sum(key)", "sum(length(s))", "count(*)")
        val expected = query.collect().toSeq
        source.cache()
        try {
          val builder = spark.sharedState.cacheManager
            .lookupCachedData(source)
            .get
            .cachedRepresentation
            .cacheBuilder
          // Materialize with fusion enabled, then disable and re-enable it on the same cache.
          Seq(true, false, true).zipWithIndex.foreach { case (enabled, index) =>
            val settings = if (enabled) Seq.empty else disabledSettings
            withSQLConf(settings: _*) {
              val cold = index == 0
              val df = query
              val plan = df.queryExecution.executedPlan
              // Planning must not materialize the cache or replace AQE's cache-stage metadata.
              assert(builder.isCachedColumnBuffersLoaded != cold, plan.toString)
              // checkToRDD = false keeps checkAnswer from loading the cache with a query of its
              // own, so the cold run's table-cache stage materializes and AQE re-plans above it.
              QueryTest.checkAnswer(df, expected, checkToRDD = false)
              assert(builder.isCachedColumnBuffersLoaded)
              val transitions = collect(plan) {
                case c: ColumnarToRowExec if collect(c.child) { case s: InMemoryTableScanExec =>
                      s
                    }.nonEmpty =>
                  c
              }
              assert(transitions.size == (if (enabled) 1 else 0), plan.toString)
              assert(collect(plan) { case s: CometInMemoryTableScanExec => s }.isEmpty)
              if (adaptive && isSpark35Plus) {
                assert(collect(plan) {
                  case s: QueryStageExec
                      if s.getClass.getSimpleName == "TableCacheQueryStageExec" =>
                    s
                }.size == 1)
              }
              val scan = collect(plan) { case s: InMemoryTableScanExec => s }.head
              assert(scan.supportsColumnar)
              // A cache scan can also be the root of a columnar request or already have a
              // transition. Applying the rule again must preserve those input/output contracts.
              Seq(scan, ColumnarToRowExec(scan), RowToColumnarExec(scan)).foreach { boundary =>
                assert(CometCacheColumnarRule()(boundary).fastEquals(boundary))
              }
              // The plan-only preview shows the plan Comet would execute, so it still fuses a
              // generated consumer that the executed plan leaves alone in plan-only mode.
              val consumer = FilterExec(Literal.TrueLiteral, scan)
              val fusedConsumer = FilterExec(Literal.TrueLiteral, ColumnarToRowExec(scan))
              assert(CometCacheColumnarRule()(consumer).fastEquals(fusedConsumer) == enabled)
              assert(
                CometCacheColumnarRule(preview = true)(consumer).fastEquals(fusedConsumer) ==
                  (enabled || disabledSettings == planOnly))
            }
          }
        } finally source.unpersist(blocking = true)
      }
    }
  }

  // Column expression and the reason the serializer has to decline it.
  private val unsupportedForArrowCache = Seq(
    // Interval types have no Arrow vector in Utils.getFieldVector. Without the schema check in
    // the serializer, caching this relation fails outright with "Unsupported Arrow Vector for
    // serialize: class org.apache.arrow.vector.DurationVector".
    "make_dt_interval(0, 0, 0, id) AS payload",
    // Java Arrow keys a struct vector's children by name, so the two `a` children collapse into
    // one and the batch fails its arity check coming back across the C data interface. Without
    // the check, this is cached in Comet's format and the read dies with
    // "ArrowArray struct has 2 children (expected 1)".
    // See https://github.com/apache/datafusion-comet/issues/5605.
    "named_struct('a', id, 'a', id + 1) AS payload")

  test("Comet cache serializer delegates unsupported types to Spark's cache format") {
    withConversions(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm",
      SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> "true",
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true") {

      for (column <- unsupportedForArrowCache) {
        spark.catalog.clearCache()

        spark
          .sql(s"SELECT id AS key, $column FROM range(1000)")
          .createOrReplaceTempView("default_cached_batch")

        val columnarQuery = """
          SELECT key, payload
          FROM default_cached_batch
          WHERE key >= 10 AND key < 20
        """
        val rowQuery = """
          SELECT payload
          FROM default_cached_batch
          WHERE key >= 10 AND key < 20
        """
        val expectedColumnar = uncachedSparkAnswer(columnarQuery)
        val expectedRows = uncachedSparkAnswer(rowQuery)

        spark.catalog.cacheTable("default_cached_batch")
        spark.table("default_cached_batch").count()

        assert(
          cachedBatchTypes("default_cached_batch").sameElements(
            Array("org.apache.spark.sql.execution.columnar.DefaultCachedBatch")),
          s"$column was cached in Comet's format")

        // Columnar read path, delegated to Spark's serializer.
        val columnarDf = spark.sql(columnarQuery)
        checkAnswer(columnarDf, expectedColumnar.toSeq)
        assert(
          !columnarDf.queryExecution.executedPlan.toString().contains("CometInMemoryTableScan"))

        // Row read path: disabling the vectorized cache reader makes Spark use
        // convertCachedBatchToInternalRow.
        withSQLConf(SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> "false") {
          val rowDf = spark.sql(rowQuery)
          checkAnswer(rowDf, expectedRows.toSeq)
          assert(!rowDf.queryExecution.executedPlan.toString().contains("CometInMemoryTableScan"))
        }

        spark.catalog.clearCache()
      }
    }
  }

  test("Comet explains Spark's scan of a relation cached in Comet's format") {
    // spark.sql.cache.serializer is static, so a relation cached in Comet's format stays in it
    // after a session turns Comet or its native execution off, and from then on Spark's
    // InMemoryTableScanExec reads it, which nothing else in the plan would record. A relation
    // that Comet's serializer delegated to Spark's format gets no such reason.
    withNativeCache {
      spark
        .sql("SELECT id, id % 7 AS k FROM range(100)")
        .createOrReplaceTempView("comet_format_cache")
      spark
        .sql(s"SELECT id, ${unsupportedForArrowCache.head} FROM range(100)")
        .createOrReplaceTempView("spark_format_cache")
      spark.catalog.cacheTable("comet_format_cache")
      spark.catalog.cacheTable("spark_format_cache")
      assert(
        cachedBatchTypes("comet_format_cache").sameElements(
          Array("org.apache.spark.sql.comet.execution.arrow.CometCachedBatch")))
      assert(
        cachedBatchTypes("spark_format_cache").sameElements(
          Array("org.apache.spark.sql.execution.columnar.DefaultCachedBatch")))

      def reasons(query: String): Seq[String] = {
        val df = spark.sql(query)
        df.collect()
        new ExtendedExplainInfo().getFallbackReasons(df.queryExecution.executedPlan)
      }

      for {
        (key, cause) <- Seq(
          CometConf.COMET_ENABLED.key -> "Comet is disabled",
          CometConf.COMET_EXEC_ENABLED.key -> s"${CometConf.COMET_EXEC_ENABLED.key} is false")
        aqe <- Seq("false", "true")
      } {
        withSQLConf(key -> "false", SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe) {
          val explained = reasons("SELECT k, count(*) FROM comet_format_cache GROUP BY k")
          assert(
            explained.exists(
              _.startsWith(s"$cause, so Spark reads this relation from Comet's cache format")),
            s"$key=false, AQE $aqe: $explained")
          assert(
            !reasons("SELECT count(id) FROM spark_format_cache").exists(
              _.contains("Comet's cache format")),
            s"$key=false, AQE $aqe")
        }
      }
    }
  }

  test("Comet in-memory cache handles multi-partition cache") {
    withConversions(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm",
      SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> "true",
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true") {

      spark.catalog.clearCache()

      val multiPartition =
        spark.range(0, 1000, 1, 5).toDF("id").cache()
      multiPartition.createOrReplaceTempView("multi_partition_cache")
      multiPartition.count()

      assert(
        cachedBatchTypes("multi_partition_cache").sameElements(
          Array("org.apache.spark.sql.comet.execution.arrow.CometCachedBatch")))

      val grouped = spark.sql("""
        SELECT id % 100, count(*)
        FROM multi_partition_cache
        GROUP BY id % 100
      """)
      checkAnswer(grouped, (0L until 100L).map(i => Row(i, 10L)))

      val groupedPlan = grouped.queryExecution.executedPlan.toString()
      assert(groupedPlan.contains("CometInMemoryTableScan"))

      multiPartition.unpersist()
      spark.catalog.clearCache()
    }
  }

  test("Comet in-memory cache handles empty cache") {
    withConversions(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm",
      SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> "true",
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true") {

      spark.catalog.clearCache()

      val empty = spark.range(0).toDF("id").cache()
      empty.createOrReplaceTempView("empty_cache")
      empty.count()

      val emptyDf = spark.sql("SELECT * FROM empty_cache")
      checkCometAnswer(emptyDf, Seq.empty[Row])

      val emptyPlan = emptyDf.queryExecution.executedPlan.toString()
      assert(emptyPlan.contains("CometInMemoryTableScan"))
      assert(!emptyPlan.contains("CometSparkColumnarToColumnar"))

      empty.unpersist()
      spark.catalog.clearCache()
    }
  }

  test("Comet in-memory cache supports projection-only read") {
    withConversions(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm",
      SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> "true",
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true") {

      spark.catalog.clearCache()

      spark
        .range(1000)
        .selectExpr("id as key", "id % 8 as value", "id + 1 as key_plus_1")
        .createOrReplaceTempView("project_cache")

      spark.catalog.cacheTable("project_cache")
      spark.table("project_cache").count()

      assert(
        cachedBatchTypes("project_cache").sameElements(
          Array("org.apache.spark.sql.comet.execution.arrow.CometCachedBatch")))

      val df = spark.sql("SELECT key FROM project_cache")
      checkAnswer(df, (0L until 1000L).map(Row(_)))

      val plan = df.queryExecution.executedPlan.toString()
      assert(plan.contains("CometInMemoryTableScan"))
      // Rows come out of a Comet converter rather than Spark's ColumnarToRow. Either variant
      // satisfies that; which one is used depends on the default of
      // spark.comet.exec.columnarToRow.native.enabled.
      assert(
        plan.contains("CometColumnarToRow") || plan.contains("CometNativeColumnarToRow"),
        s"expected a Comet columnar-to-row above the cache scan, got:\n$plan")

      spark.catalog.clearCache()
    }
  }

  test("sort aggregate over a sorted cache keeps its grouping-key order") {
    // The cached relation reports its sort order, so Spark plans both sort aggregates and the
    // ORDER BY without a SortExec, and the native aggregates read the cache through a scan that
    // reports no order. NULL and an empty array hash alike, so DataFusion's grouping can emit
    // the empty array after [1] unless the aggregate output is sorted natively.
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_SHUFFLE_ENABLED.key -> "true",
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true") {
      try {
        Seq[(Option[Seq[Int]], String)]((None, "n"), (Some(Seq.empty), "e"), (Some(Seq(1)), "o"))
          .toDF("k", "v")
          .coalesce(1)
          .sortWithinPartitions("k")
          .cache()
          .createOrReplaceTempView("sorted_cache")

        val df = sql("SELECT k, first(v) FROM sorted_cache GROUP BY k ORDER BY k")
        checkAnswer(df, Seq(Row(null, "n"), Row(Seq.empty[Int], "e"), Row(Seq(1), "o")))
        val plan = df.queryExecution.executedPlan
        assert(collect(plan) { case s: CometInMemoryTableScanExec => s }.size == 1, plan)
        assert(collect(plan) { case a: CometSortAggregateExec => a }.size == 2, plan)
        assert(collect(plan) { case s @ (_: CometSortExec | _: SortExec) => s }.isEmpty, plan)
      } finally {
        spark.catalog.clearCache()
      }
    }
  }

  test("Comet in-memory cache supports shuffle after cache read") {
    withConversions(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm",
      SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> "true",
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true") {

      spark.catalog.clearCache()

      spark
        .range(1000)
        .selectExpr("id as key", "id % 100 as group")
        .createOrReplaceTempView("shuffle_cache")

      spark.catalog.cacheTable("shuffle_cache")
      spark.table("shuffle_cache").count()

      assert(
        cachedBatchTypes("shuffle_cache").sameElements(
          Array("org.apache.spark.sql.comet.execution.arrow.CometCachedBatch")))

      val df = spark.sql("SELECT group, count(*) FROM shuffle_cache GROUP BY group")
      checkAnswer(df, (0L until 100L).map(i => Row(i, 10L)))

      val plan = df.queryExecution.executedPlan.toString()
      assert(plan.contains("CometInMemoryTableScan"))
      assert(plan.contains("CometHashAggregate"))

      spark.catalog.clearCache()
    }
  }

  test("Comet in-memory cache statistics preserve typed bounds and null counts") {
    withSQLConf(
      CometConf.COMET_ENABLED.key -> "false",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val types = Seq(
        "boolean",
        "tinyint",
        "smallint",
        "int",
        "bigint",
        "float",
        "double",
        "decimal(10,2)",
        "decimal(38,2)",
        "string",
        "date",
        "timestamp",
        "timestamp_ntz",
        "binary")
      val expressions = types.zipWithIndex.map { case (dt, i) =>
        val value = dt match {
          case "date" => "date_add(DATE '2000-01-01', cast(v AS INT))"
          case "timestamp" | "timestamp_ntz" =>
            s"cast(date_add(DATE '2000-01-01', cast(v AS INT)) AS $dt)"
          case "string" | "binary" => s"cast(concat('字', cast(v AS STRING)) AS $dt)"
          case _ => s"cast(v AS $dt)"
        }
        s"$value AS c$i"
      }
      // Leading nulls, updates in both directions, duplicate values, all-null and single-value
      // columns exercise initialization as well as the primitive and reference bounds loops.
      Seq("(NULL), (2), (-3), (0), (1), (2), (NULL)", "(NULL), (NULL)", "(NULL), (1)").foreach {
        values =>
          val df = spark
            .sql(s"SELECT ${expressions.mkString(", ")} FROM VALUES $values AS t(v)")
            .coalesce(1)
          // Compute the reference through Spark before caching, using its internal value types.
          df.createOrReplaceTempView("typed_stats_input")
          val expected = spark
            .sql(
              s"SELECT ${types.indices.flatMap(i => Seq(s"min(c$i)", s"max(c$i)")).mkString(", ")} " +
                "FROM typed_stats_input")
            .queryExecution
            .toRdd
            .map(_.copy())
            .collect()
            .head
          val expectedNulls = df.filter("c0 IS NULL").count().toInt
          val expectedRows = df.count().toInt
          def checkStats(view: String): Unit = {
            val relation = spark.sharedState.cacheManager.lookupCachedData(spark.table(view)).get
            val batches = relation.cachedRepresentation.cacheBuilder.cachedColumnBuffers.collect()
            assert(batches.length == 1)
            val stats = batches.head.asInstanceOf[SimpleMetricsCachedBatch].stats
            df.schema.fields.zipWithIndex.foreach { case (field, i) =>
              if (field.dataType == BinaryType) {
                assert(stats.isNullAt(i * 5) && stats.isNullAt(i * 5 + 1))
              } else {
                assert(stats.get(i * 5, field.dataType) == expected.get(i * 2, field.dataType))
                assert(
                  stats.get(i * 5 + 1, field.dataType) ==
                    expected.get(i * 2 + 1, field.dataType))
              }
              assert(stats.getInt(i * 5 + 2) == expectedNulls)
              assert(stats.getInt(i * 5 + 3) == expectedRows)
            }
          }

          df.cache()
          try {
            df.count()
            checkStats("typed_stats_input")
          } finally {
            df.unpersist(blocking = true)
            spark.catalog.dropTempView("typed_stats_input")
          }

          withSparkColumnarCache("typed_stats_columnar")(path => df.write.parquet(path)) { _ =>
            val relation = spark.sharedState.cacheManager
              .lookupCachedData(spark.table("typed_stats_columnar"))
              .get
              .cachedRepresentation
            assert(relation.cacheBuilder.cachedPlan.supportsColumnar)
            checkStats("typed_stats_columnar")
          }
      }
    }
  }

  test("Comet in-memory cache statistics preserve numeric extremes and floating-point ordering") {
    withSQLConf(
      CometConf.COMET_ENABLED.key -> "false",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = spark
        .sql("""
        SELECT
          CAST(if(id = 0, -128, 127) AS TINYINT) AS b,
          CAST(if(id = 0, -32768, 32767) AS SMALLINT) AS s,
          CAST(if(id = 0, -2147483648, 2147483647) AS INT) AS i,
          if(id = 0, -9223372036854775808L, 9223372036854775807L) AS l,
          CAST(v AS FLOAT) AS f,
          CAST(v AS DOUBLE) AS d,
          CAST(if(id = 0, '0.0', '-0.0') AS FLOAT) AS fz,
          CAST(if(id = 0, '0.0', '-0.0') AS DOUBLE) AS dz
        FROM VALUES (0, '0.0'), (1, '-0.0'), (2, '-Infinity'), (3, 'Infinity'), (4, 'NaN')
        AS t(id, v)
      """)
        .coalesce(1)

      def checkStats(view: String): Unit = {
        val relation = spark.sharedState.cacheManager.lookupCachedData(spark.table(view)).get
        val batches = relation.cachedRepresentation.cacheBuilder.cachedColumnBuffers.collect()
        assert(batches.length == 1)
        val stats = batches.head.asInstanceOf[SimpleMetricsCachedBatch].stats
        assert(stats.getByte(0) == Byte.MinValue && stats.getByte(1) == Byte.MaxValue)
        assert(stats.getShort(5) == Short.MinValue && stats.getShort(6) == Short.MaxValue)
        assert(stats.getInt(10) == Int.MinValue && stats.getInt(11) == Int.MaxValue)
        assert(stats.getLong(15) == Long.MinValue && stats.getLong(16) == Long.MaxValue)
        assert(stats.getFloat(20) == Float.NegativeInfinity && stats.getFloat(21).isNaN)
        assert(stats.getDouble(25) == Double.NegativeInfinity && stats.getDouble(26).isNaN)
        // Spark aggregates consider signed zeros equal; the cache stores Java's total ordering.
        assert(
          java.lang.Float.floatToRawIntBits(stats.getFloat(30)) ==
            java.lang.Float.floatToRawIntBits(-0.0f))
        assert(java.lang.Float.floatToRawIntBits(stats.getFloat(31)) == 0)
        assert(
          java.lang.Double.doubleToRawLongBits(stats.getDouble(35)) ==
            java.lang.Double.doubleToRawLongBits(-0.0d))
        assert(java.lang.Double.doubleToRawLongBits(stats.getDouble(36)) == 0L)
        (0 until 8).foreach { c =>
          assert(stats.getInt(c * 5 + 2) == 0)
          assert(stats.getInt(c * 5 + 3) == 5)
        }
      }

      df.createOrReplaceTempView("extreme_stats_input")
      df.cache()
      try {
        df.count()
        checkStats("extreme_stats_input")
      } finally {
        df.unpersist(blocking = true)
        spark.catalog.dropTempView("extreme_stats_input")
      }
      withSparkColumnarCache("extreme_stats_columnar")(path => df.write.parquet(path)) { _ =>
        checkStats("extreme_stats_columnar")
      }
    }
  }

  test("Comet in-memory cache supports stats-based batch pruning") {
    withConversions(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm",
      SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> "true",
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true",
      "spark.sql.inMemoryColumnarStorage.batchSize" -> "100") {

      spark.catalog.clearCache()

      spark
        .range(0, 1000, 1, 10)
        .selectExpr("id as key", "id % 7 as value")
        .createOrReplaceTempView("prune_cache")

      spark.catalog.cacheTable("prune_cache")
      spark.table("prune_cache").count()

      assert(
        cachedBatchTypes("prune_cache").sameElements(
          Array("org.apache.spark.sql.comet.execution.arrow.CometCachedBatch")))

      val cached = spark.sharedState.cacheManager.lookupCachedData(spark.table("prune_cache")).get
      val relation = cached.cachedRepresentation
      val cachedBuffers = relation.cacheBuilder.cachedColumnBuffers

      // Spark's cache pruning reads statistics through SimpleMetricsCachedBatch.
      // CometCachedBatch must expose the same five statistics per column:
      // lower bound, upper bound, null count, row count, and size in bytes.
      val firstBatch = cachedBuffers.take(1).head
      assert(firstBatch.isInstanceOf[SimpleMetricsCachedBatch])
      assert(
        firstBatch.asInstanceOf[SimpleMetricsCachedBatch].stats.numFields ==
          relation.output.length * 5)

      val keyAttr = relation.output.find(_.name == "key").get

      // Call the serializer filter directly so the test fails if buildFilter is
      // accidentally changed back to a no-op.
      def prunedCount(predicate: Expression): Long = {
        val filter = relation.cacheBuilder.serializer.buildFilter(Seq(predicate), relation.output)
        cachedBuffers.mapPartitionsWithIndex(filter).count()
      }

      val totalBatches = cachedBuffers.count()
      assert(totalBatches > 1)

      val targetPredicate =
        And(GreaterThanOrEqual(keyAttr, Literal(900L)), LessThan(keyAttr, Literal(905L)))
      assert(prunedCount(targetPredicate) == 1)

      val outsidePredicate = LessThan(keyAttr, Literal(0L))
      assert(prunedCount(outsidePredicate) == 0)

      val allPredicate =
        And(GreaterThanOrEqual(keyAttr, Literal(0L)), LessThan(keyAttr, Literal(1000L)))
      assert(prunedCount(allPredicate) == totalBatches)

      val df = spark.sql("""
        SELECT key, value
        FROM prune_cache
        WHERE key >= 900 AND key < 905
      """)
      checkAnswer(df, (900L until 905L).map(i => Row(i, i % 7)))

      val plan = df.queryExecution.executedPlan.toString()
      assert(plan.contains("CometInMemoryTableScan"))
      assert(!plan.contains("CometSparkColumnarToColumnar"))

      spark.catalog.clearCache()
    }
  }

  test("Comet in-memory cache honors inMemoryColumnarStorage.partitionPruning=false") {
    // CometInMemoryTableScanExec applies the serializer's stats filter before decoding, the same
    // way Spark's InMemoryTableScanExec.filteredCachedBatches does. Spark gates that on
    // spark.sql.inMemoryColumnarStorage.partitionPruning, so Comet must too.
    //
    // Pruning is transparent in the results, so it is observed through the scan's numOutputRows:
    // that counts the rows in the batches actually decoded, so pruning fewer batches means fewer
    // rows. With pruning off, every cached row must be decoded.
    def scanRowsFor(pruning: Boolean): (Long, Long) = {
      var result: (Long, Long) = (0L, 0L)
      withConversions(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        CometConf.COMET_SHUFFLE_MODE.key -> "jvm",
        SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> "true",
        CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true",
        "spark.sql.inMemoryColumnarStorage.batchSize" -> "100",
        SQLConf.IN_MEMORY_PARTITION_PRUNING.key -> pruning.toString) {

        spark.catalog.clearCache()
        spark
          .range(0, 1000, 1, 10)
          .selectExpr("id as key", "id % 7 as value")
          .createOrReplaceTempView("prune_conf_cache")
        spark.catalog.cacheTable("prune_conf_cache")
        val totalRows = spark.table("prune_conf_cache").count()

        val df =
          spark.sql("SELECT key, value FROM prune_conf_cache WHERE key >= 900 AND key < 905")
        // Run this exact plan once so its metrics describe the checked result.
        QueryTest.checkAnswer(df, (900L until 905L).map(i => Row(i, i % 7)), checkToRDD = false)

        val scans = df.queryExecution.executedPlan.collect {
          case s: org.apache.spark.sql.comet.CometInMemoryTableScanExec => s
        }
        assert(scans.length == 1, s"expected one CometInMemoryTableScan, got ${scans.length}")
        result = (scans.head.metrics("numOutputRows").value, totalRows)
        spark.catalog.clearCache()
      }
      result
    }

    val (prunedRows, total) = scanRowsFor(pruning = true)
    val (unprunedRows, total2) = scanRowsFor(pruning = false)
    assert(total == total2)
    // With pruning on, only the batch holding keys 900-904 is decoded.
    assert(prunedRows < total, s"expected pruning to decode fewer than $total rows")
    // With pruning off, every cached batch is decoded.
    assert(
      unprunedRows == total,
      s"expected all $total rows to be decoded with pruning disabled, got $unprunedRows")
  }

  test("Comet in-memory cache supports DISK_ONLY storage level") {
    // CometCachedBatch holds a ChunkedByteBuffer, which is Externalizable, so BlockManager can
    // spill it to the DiskStore like any other cached block. Pins that: nothing is held in memory,
    // the bytes really do land on disk, every partition is cached, and the cache still reads back
    // through the native scan.
    withConversions(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm",
      SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> "true",
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true") {

      spark.catalog.clearCache()
      spark
        .range(0, 1000, 1, 4)
        .selectExpr("id as key", "id % 7 as value")
        .createOrReplaceTempView("disk_cache")

      spark.catalog.cacheTable("disk_cache", StorageLevel.DISK_ONLY)
      val total = spark.table("disk_cache").count()
      assert(total == 1000)

      assert(
        cachedBatchTypes("disk_cache").sameElements(
          Array("org.apache.spark.sql.comet.execution.arrow.CometCachedBatch")),
        "DISK_ONLY must still use Comet's cached batch format")

      val cached =
        spark.sharedState.cacheManager.lookupCachedData(spark.table("disk_cache")).get
      val rddId = cached.cachedRepresentation.cacheBuilder.cachedColumnBuffers.id
      val info = spark.sparkContext.getRDDStorageInfo
        .find(_.id == rddId)
        .getOrElse(fail(s"no storage info for cached RDD $rddId"))

      assert(info.memSize == 0, s"expected nothing in memory, got ${info.memSize} bytes")
      assert(info.diskSize > 0, "expected the cached bytes to be on disk")
      assert(
        info.numCachedPartitions == info.numPartitions,
        s"expected all ${info.numPartitions} partitions cached, got ${info.numCachedPartitions}")

      val df = spark.sql("SELECT key, value FROM disk_cache WHERE key >= 900 AND key < 905")
      checkAnswer(df, (900L until 905L).map(i => Row(i, i % 7)))
      val plan = df.queryExecution.executedPlan.toString()
      assert(plan.contains("CometInMemoryTableScan"))

      spark.catalog.clearCache()
    }
  }

  test("Comet in-memory cache stores timestamps with a UTC schema label") {
    // Unlike Spark's Arrow cache, whose RecordBatch is deliberately schema-less, CometCachedBatch
    // stores a full IPC stream including the schema. Labelling TimestampType with the writing
    // session's timezone would persist a mutable session value into cached data and would make the
    // row write path disagree with the columnar one, which already encodes with NATIVE_TIMEZONE.
    // So both paths must write "UTC". This is a label only -- Spark stores timestamps as micros
    // since the Unix epoch regardless of session timezone -- so values must be unaffected.
    Seq("America/Los_Angeles", "Asia/Kolkata").foreach { sessionTz =>
      withConversions(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        CometConf.COMET_SHUFFLE_MODE.key -> "jvm",
        SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> "true",
        CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true",
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> sessionTz) {

        spark.catalog.clearCache()

        // A local Seq gives a row-based plan, so this exercises
        // convertInternalRowToCachedBatch rather than the columnar path.
        val rows = Seq(
          (1, java.sql.Timestamp.valueOf("2024-01-31 12:34:56.789")),
          (2, java.sql.Timestamp.valueOf("1970-01-01 00:00:00")),
          (3, null))
        rows.toDF("id", "ts").createOrReplaceTempView("ts_cache")
        val valuesQuery = "SELECT id, ts FROM ts_cache ORDER BY id"
        val stringsQuery = "SELECT id, CAST(ts AS STRING) AS s FROM ts_cache ORDER BY id"
        val expectedValues = uncachedSparkAnswer(valuesQuery)
        val expectedStrings = uncachedSparkAnswer(stringsQuery)

        spark.catalog.cacheTable("ts_cache")
        assert(spark.table("ts_cache").count() == 3)

        assert(
          cachedBatchTypes("ts_cache").sameElements(
            Array("org.apache.spark.sql.comet.execution.arrow.CometCachedBatch")),
          s"expected Comet cache format for sessionTz=$sessionTz")

        // Decode the cached bytes through the serializer and read the Arrow field metadata back
        // out. The timezone has to be extracted inside the closure: ColumnarBatch is not
        // serializable.
        val relation =
          spark.sharedState.cacheManager
            .lookupCachedData(spark.table("ts_cache"))
            .get
            .cachedRepresentation
        val tsIndex = relation.output.indexWhere(_.name == "ts")
        val labels = relation.cacheBuilder.serializer
          .convertCachedBatchToColumnarBatch(
            relation.cacheBuilder.cachedColumnBuffers,
            relation.output,
            relation.output,
            spark.sessionState.conf)
          .mapPartitions { batches =>
            batches.take(1).map { batch =>
              batch.column(tsIndex) match {
                case v: CometVector =>
                  v.getValueVector.getField.getType match {
                    case t: ArrowType.Timestamp => String.valueOf(t.getTimezone)
                    case other => s"unexpected arrow type $other"
                  }
                case other => s"unexpected vector ${other.getClass.getName}"
              }
            }
          }
          .collect()
          .distinct

        assert(
          labels.sameElements(Array("UTC")),
          s"expected the cached timestamp schema to be labelled UTC for sessionTz=$sessionTz, " +
            s"got ${labels.mkString("[", ",", "]")}")

        // The label change must not move any values.
        checkAnswer(spark.sql(valuesQuery), expectedValues.toSeq)
        checkAnswer(spark.sql(stringsQuery), expectedStrings.toSeq)

        spark.catalog.clearCache()
      }
    }
  }

  test("Comet plugin respects user-provided cache serializer") {
    val serializerKey = StaticSQLConf.SPARK_CACHE_SERIALIZER.key
    val cometSerializer =
      "org.apache.spark.sql.comet.execution.arrow.ArrowCachedBatchSerializer"
    val userSerializer = "com.example.CustomCachedBatchSerializer"

    val defaultConf = new SparkConf()
      .set(CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key, "true")
      .set("spark.shuffle.manager", shuffleManager)
    val defaultExtraConfs = new ju.HashMap[String, String]()

    // With no user serializer configured, the plugin should install Comet's
    // serializer and also return it through extraConfs for executors.
    CometDriverPlugin.maybeSetCacheSerializer(defaultConf, defaultExtraConfs)

    assert(defaultConf.get(serializerKey) == cometSerializer)
    assert(defaultExtraConfs.get(serializerKey) == cometSerializer)

    val userConf = new SparkConf()
      .set(CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key, "true")
      .set("spark.shuffle.manager", shuffleManager)
      .set(serializerKey, userSerializer)
    val userExtraConfs = new ju.HashMap[String, String]()

    // If the user already configured a cache serializer, keep it and do not
    // send a replacement serializer through extraConfs.
    CometDriverPlugin.maybeSetCacheSerializer(userConf, userExtraConfs)

    assert(userConf.get(serializerKey) == userSerializer)
    assert(!userExtraConfs.containsKey(serializerKey))
  }

  /** Whether the Comet plugin installs its cache serializer for an application's `settings`. */
  private def installsCacheSerializer(settings: (String, String)*): Boolean = {
    val serializerKey = StaticSQLConf.SPARK_CACHE_SERIALIZER.key
    val conf = new SparkConf().setAll(settings)
    val extraConfs = new ju.HashMap[String, String]()
    CometDriverPlugin.maybeSetCacheSerializer(conf, extraConfs)
    assert(conf.contains(serializerKey) == extraConfs.containsKey(serializerKey))
    extraConfs.containsKey(serializerKey)
  }

  test("Comet plugin installs its cache serializer only if Comet can scan the cache natively") {
    val cometOn = CometConf.COMET_ENABLED.key -> "true"
    val execOn = CometConf.COMET_EXEC_ENABLED.key -> "true"
    val cacheOn = CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true"
    // Without Comet's shuffle manager Comet disables itself, which the next test covers.
    val cometShuffle = "spark.shuffle.manager" -> shuffleManager

    def installed(settings: (String, String)*): Boolean =
      installsCacheSerializer(cometShuffle +: settings: _*)

    assert(installed(cometOn, execOn, cacheOn))
    // An application that starts with Comet or its native execution off can never plan
    // CometInMemoryTableScan, and spark.sql.cache.serializer is static, so its caches keep
    // Spark's format.
    assert(!installed(CometConf.COMET_ENABLED.key -> "false", execOn, cacheOn))
    assert(!installed(cometOn, CometConf.COMET_EXEC_ENABLED.key -> "false", cacheOn))
    // Unset keys take their defaults rather than values of the plugin's own.
    assert(
      installed(cometOn, execOn) ==
        CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.defaultValue.get)
    assert(
      installed(cacheOn) ==
        (CometConf.COMET_ENABLED.defaultValue.get &&
          CometConf.COMET_EXEC_ENABLED.defaultValue.get))
  }

  test("Comet plugin keeps Spark's cache format where Comet disables itself or Kryo rejects it") {
    val cometShuffle = "spark.shuffle.manager" -> shuffleManager

    def installed(settings: (String, String)*): Boolean =
      installsCacheSerializer(
        Seq(
          CometConf.COMET_ENABLED.key -> "true",
          CometConf.COMET_EXEC_ENABLED.key -> "true",
          CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true") ++ settings: _*)

    assert(installed(cometShuffle))
    // Comet shuffle is enabled by default, and Comet disables itself while it is unless the
    // application runs one of Comet's shuffle managers.
    assert(!installed())
    assert(!installed("spark.shuffle.manager" -> "sort"))
    assert(installed("spark.shuffle.manager" -> classOf[CometCelebornShuffleManager].getName))
    // Without Comet shuffle, under the key or its deprecated name, the shuffle manager does not
    // matter.
    assert(installed(CometConf.COMET_SHUFFLE_ENABLED.key -> "false"))
    assert(installed("spark.comet.exec.shuffle.enabled" -> "false"))

    // Kryo with registration required rejects Comet's cached batch unless something registered
    // it: CometKryoRegistrator, on its own or beside a registrator of the application's, or the
    // application's own registrations.
    val kryo = "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer"
    val registrationRequired = "spark.kryo.registrationRequired" -> "true"
    val registrator = "spark.kryo.registrator"
    val sparkOnly = classOf[SparkCachedBatchKryoRegistrator].getName
    assert(!installed(cometShuffle, kryo, registrationRequired))
    assert(!installed(cometShuffle, kryo, registrationRequired, registrator -> sparkOnly))
    assert(
      installed(
        cometShuffle,
        kryo,
        registrationRequired,
        registrator -> s"$sparkOnly, ${CometKryoRegistrator.CLASS_NAME}"))
    assert(
      installed(
        cometShuffle,
        kryo,
        registrationRequired,
        "spark.kryo.classesToRegister" -> ArrowCachedBatchSerializer.cachedBatchClass.getName))
    // A registrator that cannot be loaded leaves only spark.kryo.registrator to go by.
    assert(!installed(cometShuffle, kryo, registrationRequired, registrator -> "com.example.R"))
    assert(
      installed(
        cometShuffle,
        kryo,
        registrationRequired,
        registrator -> s"com.example.R, ${CometKryoRegistrator.CLASS_NAME}"))
    // Without registrationRequired, Kryo writes the class name of anything unregistered instead.
    assert(installed(cometShuffle, kryo))
  }

  test("Comet plugin finds the Kryo registrations Comet needs however they were made") {
    def unregistered(settings: (String, String)*): Seq[Class[_]] =
      CometDriverPlugin.unregisteredKryoClasses(new SparkConf().setAll(settings))

    val kryo = "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer"
    val registrationRequired = "spark.kryo.registrationRequired" -> "true"
    assert(unregistered().isEmpty)
    assert(unregistered(kryo).isEmpty)
    assert(
      unregistered(kryo, registrationRequired).contains(
        ArrowCachedBatchSerializer.cachedBatchClass))
    assert(
      unregistered(
        kryo,
        registrationRequired,
        "spark.kryo.registrator" -> CometKryoRegistrator.CLASS_NAME).isEmpty)
    assert(
      unregistered(
        kryo,
        registrationRequired,
        "spark.kryo.classesToRegister" -> CometKryoRegistrator.classes
          .map(_.getName)
          .mkString(",")).isEmpty)
  }

  test("Comet in-memory cache supports empty projection scan") {
    withConversions(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm",
      SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> "true",
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true") {

      spark.catalog.clearCache()

      spark
        .range(1000)
        .selectExpr("id as key", "id % 8 as value")
        .createOrReplaceTempView("count_cache")

      spark.catalog.cacheTable("count_cache")
      spark.table("count_cache").count()

      val df = spark.sql("SELECT count(*) FROM count_cache")
      checkAnswer(df, Seq(Row(1000L)))

      val plan = df.queryExecution.executedPlan.toString()
      assert(plan.contains("CometInMemoryTableScan"))

      spark.catalog.clearCache()
    }
  }

  private def withNativeCache(f: => Unit): Unit = {
    withConversions(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm",
      SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> "true",
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true") {
      spark.catalog.clearCache()
      try f
      finally spark.catalog.clearCache()
    }
  }

  test("Comet in-memory cache round-trips all supported types") {
    withNativeCache {
      val query =
        """
          SELECT
            id AS l,
            CAST(id AS INT) AS i,
            CAST(id AS SMALLINT) AS sh,
            CAST(id AS TINYINT) AS ti,
            CAST(id % 2 AS BOOLEAN) AS bo,
            CAST(id AS FLOAT) AS fl,
            CAST(id AS DOUBLE) AS db,
            CAST(id AS DECIMAL(20,4)) AS de,
            CAST(id AS STRING) AS st,
            CAST(CAST(id AS STRING) AS BINARY) AS bi,
            DATE_ADD(DATE'2020-01-01', CAST(id AS INT)) AS da,
            TIMESTAMP'2020-01-01 00:00:00' + make_dt_interval(0, 0, 0, id) AS ts,
            CAST(TIMESTAMP'2020-01-01 00:00:00' + make_dt_interval(0, 0, 0, id) AS TIMESTAMP_NTZ)
              AS tsntz,
            struct(id AS a, CAST(id AS STRING) AS b) AS sc,
            array(id, id + 1) AS ar,
            map('k', id) AS mp
          FROM range(100)
        """

      // Expected values come from the uncached query so a wrong-but-consistent cached answer
      // cannot make this pass.
      val expected = uncachedSparkAnswer(s"$query ORDER BY l")

      spark.sql(query).createOrReplaceTempView("all_types_cache")
      spark.catalog.cacheTable("all_types_cache")
      spark.table("all_types_cache").count()

      assert(
        cachedBatchTypes("all_types_cache").sameElements(
          Array("org.apache.spark.sql.comet.execution.arrow.CometCachedBatch")))

      val df = spark.sql("SELECT * FROM all_types_cache").orderBy("l")
      assert(df.collect() === expected)
      assert(df.queryExecution.executedPlan.toString().contains("CometInMemoryTableScan"))
    }
  }

  test("Comet in-memory cache prunes on collated string columns") {
    assume(isSpark40Plus, "collated string types require Spark 4.0+")
    withNativeCache {
      // Bounds for a collated column are recorded with that collation's own comparison, which is
      // the same ordering the partition filter Spark generates over the column uses. Tracking
      // bounds only for the bare `StringType` object would leave a collated column's bounds null,
      // and a comparison against null bounds prunes every batch, so the query below would return
      // no rows at all rather than merely losing the pruning.
      spark
        .sql("SELECT id, CAST(id AS STRING) COLLATE UTF8_LCASE AS s FROM range(100)")
        .createOrReplaceTempView("collated_cache")
      spark.catalog.cacheTable("collated_cache")
      spark.table("collated_cache").count()

      assert(
        cachedBatchTypes("collated_cache").sameElements(
          Array("org.apache.spark.sql.comet.execution.arrow.CometCachedBatch")))

      val expected =
        spark.sql("SELECT id FROM range(100) WHERE CAST(id AS STRING) >= '5'").collect().length
      assert(expected > 0)
      assert(
        spark.sql("SELECT id FROM collated_cache WHERE s >= '5'").collect().length == expected)

      // UTF8_LCASE compares case-insensitively, so bounds recorded under it have to as well: a
      // batch whose values all sort above 'A' under byte order still contains matches for a
      // predicate that is looking for lower-case letters.
      spark
        .sql(
          "SELECT id, CAST(concat('X', cast(id as string)) AS STRING) COLLATE UTF8_LCASE AS s " +
            "FROM range(100)")
        .createOrReplaceTempView("collated_case_cache")
      spark.catalog.cacheTable("collated_case_cache")
      spark.table("collated_case_cache").count()
      assert(
        spark.sql("SELECT id FROM collated_case_cache WHERE s = 'x1'").collect().length == 1,
        "a case-insensitive match must survive pruning")

      // IsNotNull is pushed down through the null count.
      assert(
        spark.sql("SELECT id FROM collated_cache WHERE s IS NOT NULL").collect().length == 100)
    }
  }

  test("Comet in-memory cache does not prune a collated column on a prefix match") {
    assume(isSpark40Plus, "collated string types require Spark 4.0+")
    // Spark prunes StartsWith by cutting each batch's bounds to the prefix's length in characters,
    // which assumes a matching value begins with as many characters as the prefix has. A
    // collation can break that. Under UTF8_LCASE, U+0130 (a capital I with a dot above) is one
    // character that lowercases to two, 'i' and U+0307, so values beginning with it match that
    // two-character prefix while no bound cut to two characters compares equal to it, and every
    // batch holding a match is pruned.
    val query = "SELECT id, concat('\u0130', cast(id as string)) COLLATE UTF8_LCASE AS s " +
      "FROM range(5)"
    val predicate = "startswith(s, 'i\u0307')"
    withNativeCache {
      // Collected before the relation is cached, so the cache cannot stand in for it.
      val expected = uncachedSparkAnswer(s"SELECT id FROM ($query) WHERE $predicate ORDER BY id")
      assert(expected.length == 5)

      spark.sql(query).createOrReplaceTempView("collated_prefix_cache")
      spark.catalog.cacheTable("collated_prefix_cache")
      spark.table("collated_prefix_cache").count()
      assert(
        cachedBatchTypes("collated_prefix_cache").sameElements(
          Array("org.apache.spark.sql.comet.execution.arrow.CometCachedBatch")))

      assert(
        spark
          .sql(s"SELECT id FROM collated_prefix_cache WHERE $predicate ORDER BY id")
          .collect() === expected,
        "a prefix match under a collation must survive pruning")
    }
  }

  test("Comet in-memory cache prunes only on columns that have bounds") {
    withNativeCache {
      // Binary has no bounds recorded, so its lower and upper stay null. Spark would still build
      // a partition filter for it, and comparing against null bounds prunes every batch, so
      // without the buildFilter guard this query returns no rows.
      spark
        .sql("SELECT id, CAST(CAST(id AS STRING) AS BINARY) AS b FROM range(100)")
        .createOrReplaceTempView("binary_cache")
      spark.catalog.cacheTable("binary_cache")
      spark.table("binary_cache").count()

      assert(
        cachedBatchTypes("binary_cache").sameElements(
          Array("org.apache.spark.sql.comet.execution.arrow.CometCachedBatch")))

      assert(
        spark
          .sql("SELECT id FROM binary_cache WHERE b >= CAST('5' AS BINARY)")
          .collect()
          .length ==
          spark
            .sql("SELECT id FROM range(100) WHERE CAST(CAST(id AS STRING) AS BINARY) >= " +
              "CAST('5' AS BINARY)")
            .collect()
            .length)

      // Null-count based pruning stays available for columns without bounds.
      assert(spark.sql("SELECT id FROM binary_cache WHERE b IS NOT NULL").collect().length == 100)
    }
  }

  test("Comet in-memory cache is readable when Comet is disabled") {
    // spark.sql.cache.serializer is static, so the cached format cannot depend on a runtime
    // config. Disabling Comet must still leave the cached relation readable, including for
    // string columns, which Spark's DefaultCachedBatch columnar decoder cannot handle.
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> "true",
      CometConf.COMET_ENABLED.key -> "false",
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true") {
      spark.catalog.clearCache()
      spark
        .sql("SELECT id, CAST(id AS STRING) AS s FROM range(100)")
        .createOrReplaceTempView("comet_off_cache")
      spark.catalog.cacheTable("comet_off_cache")
      spark.table("comet_off_cache").count()

      val rows = spark.sql("SELECT s FROM comet_off_cache WHERE id >= 90").collect()
      assert(rows.length == 10)
      assert(rows.map(_.getString(0)).toSet == (90 until 100).map(_.toString).toSet)

      spark.catalog.clearCache()
    }
  }

  test("Comet in-memory cache supports the row read path over CometCachedBatch") {
    withNativeCache {
      spark
        .sql("SELECT id AS key, CAST(id AS STRING) AS s FROM range(100)")
        .createOrReplaceTempView("row_path_cache")
      spark.catalog.cacheTable("row_path_cache")
      spark.table("row_path_cache").count()

      assert(
        cachedBatchTypes("row_path_cache").sameElements(
          Array("org.apache.spark.sql.comet.execution.arrow.CometCachedBatch")))

      // Turning off the vectorized cache reader routes the scan through
      // convertCachedBatchToInternalRow rather than convertCachedBatchToColumnarBatch.
      withSQLConf(SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> "false") {
        val rows = spark.sql("SELECT s FROM row_path_cache WHERE key >= 90").collect()
        assert(rows.length == 10)
        assert(rows.map(_.getString(0)).toSet == (90 until 100).map(_.toString).toSet)
      }
    }
  }

  test("Comet in-memory cache projects a reordered full-width selection") {
    withNativeCache {
      spark
        .sql("SELECT id AS key, CAST(id * 10 AS STRING) AS value FROM range(10)")
        .createOrReplaceTempView("reorder_cache")
      spark.catalog.cacheTable("reorder_cache")
      spark.table("reorder_cache").count()

      val relation =
        spark.sharedState.cacheManager
          .lookupCachedData(spark.table("reorder_cache"))
          .get
          .cachedRepresentation
      val serializer = relation.cacheBuilder.serializer

      // A full-width but reordered projection has the same length as the cache schema, so an
      // identity check based on length alone would return the columns in the wrong order.
      val reordered = Seq(relation.output(1), relation.output(0))
      val rows = serializer
        .convertCachedBatchToInternalRow(
          relation.cacheBuilder.cachedColumnBuffers,
          relation.output,
          reordered,
          spark.sessionState.conf)
        .map(row => (row.getString(0).toString, row.getLong(1)))
        .collect()
        .sortBy(_._2)

      assert(rows.length == 10)
      assert(rows === (0 until 10).map(i => ((i * 10).toString, i.toLong)).toArray)
    }
  }

  test("Comet in-memory cache pruning handles NaN floating-point values") {
    withConversions(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm",
      SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> "true",
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true",
      "spark.sql.inMemoryColumnarStorage.batchSize" -> "2") {

      spark.catalog.clearCache()

      // A single row-input partition gives two deterministic two-row cached batches.
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        spark
          .sql("""
          SELECT *
          FROM VALUES
            (0, CAST('NaN' AS DOUBLE), CAST('NaN' AS FLOAT)),
            (1, 1.0D, CAST(1.0 AS FLOAT)),
            (2, CAST('-0.0' AS DOUBLE), CAST('-0.0' AS FLOAT)),
            (3, 0.0D, CAST(0.0 AS FLOAT))
          AS t(id, d, f)
        """)
          .coalesce(1)
          .createOrReplaceTempView("nan_prune_cache")

        spark.catalog.cacheTable("nan_prune_cache")
        spark.table("nan_prune_cache").count()
      }

      val relation = spark.sharedState.cacheManager
        .lookupCachedData(spark.table("nan_prune_cache"))
        .get
        .cachedRepresentation
      val batches = relation.cacheBuilder.cachedColumnBuffers
      assert(batches.count() == 2, "the NaN/finite and signed-zero rows need separate batches")

      Seq(
        ("d", Literal(Double.NaN), Literal(0.0d), Literal(1.0d)),
        ("f", Literal(Float.NaN), Literal(0.0f), Literal(1.0f))).foreach {
        case (column, nan, zero, one) =>
          val attr = relation.output.find(_.name == column).get
          val nanSql = s"CAST('NaN' AS ${nan.dataType.sql})"
          // These comparisons become statistics filters. isnan alone would leave all batches
          // eligible and could not catch an incorrectly recorded NaN upper bound.
          val comparisons = Seq(
            (s"$column = $nanSql", EqualTo(attr, nan), Seq(Row(0)), 1L),
            (s"$column > 1", GreaterThan(attr, one), Seq(Row(0)), 1L),
            (s"$column < $nanSql", LessThan(attr, nan), Seq(Row(1), Row(2), Row(3)), 2L),
            (s"$column = 0", EqualTo(attr, zero), Seq(Row(2), Row(3)), 1L),
            (s"$column > $nanSql", GreaterThan(attr, nan), Seq.empty[Row], 0L),
            (s"$column < 0", LessThan(attr, zero), Seq.empty[Row], 0L))
          comparisons.foreach { case (predicate, expression, expected, expectedBatches) =>
            withClue(s"predicate: $predicate: ") {
              val filter = relation.cacheBuilder.serializer
                .buildFilter(Seq(expression), relation.output)
              assert(batches.mapPartitionsWithIndex(filter).count() == expectedBatches)

              val df = spark.sql(s"SELECT id FROM nan_prune_cache WHERE $predicate")
              checkAnswer(df, expected)
              val plan = df.queryExecution.executedPlan.toString()
              assert(plan.contains("CometInMemoryTableScan"))
              assert(!plan.contains("CometSparkColumnarToColumnar"))
            }
          }
      }

      spark.catalog.clearCache()
    }
  }

  /**
   * Cache `view` over a Parquet file written by `write`, with the cached plan forced to be
   * Spark's own vectorized Parquet reader: its columns are On/OffHeapColumnVector rather than
   * CometVector. Spark's InMemoryRelation strips the ColumnarToRow above that scan because
   * supportsColumnarInput is true for the schema, so the serializer receives non-Arrow columnar
   * batches. Asserts the relation really was stored in Comet's format before handing control to
   * `f`, along with Spark's result collected before caching.
   */
  private def withSparkColumnarCache(view: String, extraConfs: (String, String)*)(
      write: String => Unit)(f: Seq[Row] => Unit): Unit = {
    withTempPath { path =>
      write(path.toString)

      withNativeCache {
        withSQLConf(
          Seq(
            CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "false",
            SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true") ++
            sparkToArrowConversionConfs(enabled = false) ++ extraConfs: _*) {

          spark.read.parquet(path.toString).createOrReplaceTempView(view)
          val expected = uncachedSparkAnswer(s"SELECT * FROM $view")
          spark.catalog.cacheTable(view)
          spark.table(view).count()

          assert(
            cachedBatchTypes(view).sameElements(
              Array("org.apache.spark.sql.comet.execution.arrow.CometCachedBatch")))

          f(expected.toSeq)
        }
      }
    }
  }

  test("cache a Spark columnar plan whose vectors are not Arrow-backed") {
    withSparkColumnarCache(
      "spark_columnar_cache",
      SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Denver") { path =>
      spark
        .range(1000)
        .selectExpr(
          "id as key",
          "id % 8 as value",
          "cast(id as string) as s",
          "cast(id as double) as d",
          "cast(null as int) as n",
          "cast(id as decimal(20,3)) as dec",
          "date_add(date'2020-01-01', cast(id as int)) as dt",
          "timestamp_micros(id * 1000000) as ts")
        .write
        .parquet(path)
    } { expected =>
      assert(spark.table("spark_columnar_cache").count() == 1000)

      checkAnswer(
        spark.sql("SELECT * FROM spark_columnar_cache WHERE key >= 10 AND key < 20 ORDER BY key"),
        expected.filter(row => row.getLong(0) >= 10 && row.getLong(0) < 20).sortBy(_.getLong(0)))
      checkAnswer(
        spark.sql(
          "SELECT sum(key), sum(d), sum(dec), count(s), count(n), max(dt), max(ts) " +
            "FROM spark_columnar_cache"),
        Seq(
          Row(
            499500L,
            499500.0d,
            BigDecimal(499500),
            1000L,
            0L,
            java.sql.Date.valueOf(java.time.LocalDate.of(2020, 1, 1).plusDays(999)),
            java.sql.Timestamp.from(java.time.Instant.ofEpochSecond(999)))))
    }
  }

  test("cache a non-Arrow-backed Spark columnar plan with complex types") {
    withSparkColumnarCache(
      "spark_columnar_complex",
      SQLConf.PARQUET_VECTORIZED_READER_NESTED_COLUMN_ENABLED.key -> "true") { path =>
      spark
        .range(200)
        .selectExpr(
          "id as key",
          "if(id % 5 = 0, null, array(id, id + 1)) as a",
          "named_struct('x', id, 'y', cast(id as string)) as st",
          "if(id % 7 = 0, null, map(cast(id as string), id)) as m",
          // via string: ANSI mode (on by default in Spark 4.x) rejects a direct bigint -> binary
          // cast.
          "cast(cast(id as string) as binary) as b")
        .write
        .parquet(path)
    } { expected =>
      assert(spark.table("spark_columnar_complex").count() == 200)

      checkAnswer(spark.sql("SELECT key, a, st, m, b FROM spark_columnar_complex"), expected)
    }
  }

  // Enough rows that every column's buffers are big enough for Arrow to actually compress them.
  // Arrow stores a buffer verbatim when compressing it would not make it smaller, and a boolean
  // column of a few hundred rows is a few dozen bytes, which takes that fallback -- leaving the
  // corruption the projection tests rely on with nothing to corrupt.
  private val projectionCacheRows = 8000

  private val flatProjectionColumns = Seq(
    "id",
    "id % 100 AS k",
    "cast(id as double) / 3 AS d",
    "concat('a_', cast(id as string)) AS s1",
    "concat('b_', cast(id % 17 as string)) AS s2",
    "cast(id % 2 = 0 as boolean) AS flag")

  // A flat column always owns one field node and two or three buffers; a nested one owns a run
  // whose length is a property of its whole subtree. That arithmetic is what turns a column index
  // into a window of the payload, so a run computed short or long by a single buffer misaligns
  // every column after it -- which a full projection cannot see, because selecting everything
  // covers the whole sequence however it is partitioned. These are the shapes that exercise it: a
  // struct, an array, a map (which Arrow stores as a list of two-child structs, so one column
  // spans four field nodes), a struct wrapping an array, and flat columns on both sides of them.
  private val nestedProjectionColumns = Seq(
    "id",
    "named_struct('a', id, 'b', concat('sa_', cast(id as string))) AS sc",
    "array(concat('e0_', cast(id as string)), concat('e1_', cast(id as string))) AS ar",
    "map(concat('k_', cast(id % 97 as string)), id) AS mp",
    "named_struct('nums', array(id, id + 1, id + 2)) AS deep",
    "concat('t_', cast(id as string)) AS tail")

  // A payload chunk size small enough that every buffer window in a projection-test payload
  // crosses at least one chunk boundary, and odd so that no boundary lines up with Arrow's 8-byte
  // buffer alignment. The default of 1 MiB puts a test-sized payload in a single chunk, which
  // leaves the cross-chunk copy in the read path unexercised.
  private val tinyChunkSize = 61

  // The payload chunk sizes the projection tests run under: the default, and one that splits
  // every payload into many chunks.
  private val chunkSizes = Seq(("single-chunk", None), ("multi-chunk", Some(tinyChunkSize)))

  /**
   * Cache a six-column flat relation and hand the collected batches to `f` along with the
   * relation, so a test can doctor the payload before decoding it again through the serializer.
   */
  private def withProjectionCache(
      f: (org.apache.spark.sql.execution.columnar.InMemoryRelation, Array[CachedBatch]) => Unit)
      : Unit = withProjectionCache(None)(f)

  private def withProjectionCache(chunkSize: Option[Int])(
      f: (org.apache.spark.sql.execution.columnar.InMemoryRelation, Array[CachedBatch]) => Unit)
      : Unit = withCachedProjection("projection_cache", flatProjectionColumns, chunkSize)(f)

  /** The same, over a relation of the same width whose middle four columns are nested. */
  private def withNestedProjectionCache(
      f: (org.apache.spark.sql.execution.columnar.InMemoryRelation, Array[CachedBatch]) => Unit)
      : Unit = withNestedProjectionCache(None)(f)

  private def withNestedProjectionCache(chunkSize: Option[Int])(
      f: (org.apache.spark.sql.execution.columnar.InMemoryRelation, Array[CachedBatch]) => Unit)
      : Unit =
    withCachedProjection("nested_projection_cache", nestedProjectionColumns, chunkSize)(f)

  private type ProjectionFixture = Option[Int] => (
      (
          org.apache.spark.sql.execution.columnar.InMemoryRelation,
          Array[CachedBatch]) => Unit) => Unit

  private def withCachedProjection(view: String, columns: Seq[String], chunkSize: Option[Int])(
      f: (org.apache.spark.sql.execution.columnar.InMemoryRelation, Array[CachedBatch]) => Unit)
      : Unit = {
    withConversions(
      Seq(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true",
        SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> "true") ++
        chunkSize.map(CometConf.COMET_EXEC_IN_MEMORY_CACHE_CHUNK_SIZE.key -> _.toString): _*) {

      spark.catalog.clearCache()
      spark
        .range(0, projectionCacheRows.toLong, 1, 2)
        .selectExpr(columns: _*)
        .createOrReplaceTempView(view)
      spark.catalog.cacheTable(view)
      assert(spark.table(view).count() == projectionCacheRows)
      assert(
        cachedBatchTypes(view).sameElements(
          Array("org.apache.spark.sql.comet.execution.arrow.CometCachedBatch")))

      val relation = spark.sharedState.cacheManager
        .lookupCachedData(spark.table(view))
        .get
        .cachedRepresentation

      try {
        f(relation, relation.cacheBuilder.cachedColumnBuffers.collect())
      } finally {
        spark.catalog.clearCache()
      }
    }
  }

  /**
   * Decode `batches` through the cache serializer, selecting `selected`, and total the rows.
   *
   * `cacheAttributes` defaults to the relation's own, and is overridable so a test can hand the
   * reader a schema the writer did not use.
   */
  private def decodedRowCount(
      relation: org.apache.spark.sql.execution.columnar.InMemoryRelation,
      batches: Array[CachedBatch],
      selected: Seq[Attribute],
      cacheAttributes: Option[Seq[Attribute]] = None): Long = {
    relation.cacheBuilder.serializer
      .convertCachedBatchToColumnarBatch(
        spark.sparkContext.parallelize(batches.toSeq, 1),
        cacheAttributes.getOrElse(relation.output),
        selected,
        spark.sessionState.conf)
      // ColumnarBatch is not serializable, so reduce to a count inside the closure.
      .mapPartitions(batches => Iterator.single(batches.map(_.numRows().toLong).sum))
      .collect()
      .sum
  }

  /**
   * Run `f`, require it to fail, and require the failure to be the decode error itself.
   *
   * The read path allocates an off-heap body, hands it to a record batch that takes its own
   * references, and drops its own. A cleanup path that then releases the body a second time
   * drives its reference count negative, and the reference-count error replaces the decode
   * failure that caused it -- leaving a plain `intercept[Exception]` green while the user sees a
   * error that says nothing about their corrupt cache.
   */
  private def interceptDecodeFailure(f: => Unit): Throwable = {
    val thrown = intercept[Exception](f)
    assert(
      !causeChain(thrown).exists { t =>
        t.getClass.getName.contains("IllegalReferenceCount") ||
        Option(t.getMessage).exists(m => m.contains("RefCnt") || m.contains("refCnt"))
      },
      s"the decode failure must surface as itself, not as a reference-count error: $thrown")
    thrown
  }

  test("Comet in-memory cache round-trips under every compression codec") {
    // Every codec the config accepts, not just the default. `none` takes a different path on read
    // -- the payload records no codec, so nothing is decompressed -- and shipped broken for a
    // while because the only tests that ran were on the default codec.
    Seq("none", "zstd").foreach { codec =>
      withConversions(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> "true",
        CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true",
        CometConf.COMET_EXEC_IN_MEMORY_CACHE_COMPRESSION_CODEC.key -> codec) {

        spark.catalog.clearCache()
        val view = s"codec_cache_$codec"
        spark
          .range(0, 4000, 1, 2)
          .selectExpr(
            "id",
            "cast(id as double) / 3 AS d",
            "concat('s_', cast(id as string)) AS s",
            "cast(id % 2 = 0 as boolean) AS flag")
          .createOrReplaceTempView(view)
        // Collected before the view is cached. checkSparkAnswer alone cannot catch a writer that
        // stores the wrong values: with and without Comet, both sides read the one cached payload.
        val full = s"SELECT * FROM $view ORDER BY id"
        val projected = s"SELECT s FROM $view WHERE id >= 3990 ORDER BY s"
        val expectedFull = uncachedSparkAnswer(full)
        val expectedProjected = uncachedSparkAnswer(projected)
        spark.catalog.cacheTable(view)

        assert(
          cachedBatchTypes(view).sameElements(
            Array("org.apache.spark.sql.comet.execution.arrow.CometCachedBatch")),
          s"codec $codec should still store CometCachedBatch")

        // A full read, a projected read (the buffer-selection path), and a row count that decodes
        // nothing -- the three shapes the read path distinguishes.
        assert(spark.sql(full).collect() === expectedFull, s"codec $codec read the wrong values")
        assert(spark.sql(projected).collect() === expectedProjected)
        withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
          checkAnswer(spark.sql(full), expectedFull.toSeq)
          checkAnswer(spark.sql(projected), expectedProjected.toSeq)
        }
        assert(spark.sql(s"SELECT count(*) FROM $view").collect()(0).getLong(0) == 4000)
        // Pruning reads the statistics rather than the payload, so exercise it too.
        assert(spark.sql(s"SELECT id FROM $view WHERE id >= 3990").collect().length == 10)

        spark.catalog.clearCache()
      }
    }
  }

  test("Comet in-memory cache rejects a payload that records an unknown compression codec") {
    // `CodecType.fromCompressionType` answers NO_COMPRESSION for any byte outside its enum, so a
    // reader that took its word for it would read a compressed body as plain bytes and hand back
    // garbage values instead of failing. No other test reaches this branch: the writer can only
    // record a byte one of Arrow's three CodecTypes owns, so the payload has to be assembled with
    // a byte none of them does.
    val unknownCodec = 99.toByte
    val rows = 64
    val ints = new IntVector("i", CometArrowAllocator)
    try {
      ints.allocateNew(rows)
      (0 until rows).foreach(i => ints.set(i, i))
      ints.setValueCount(rows)
      val batch = new ColumnarBatch(Array[ColumnVector](new CometPlainVector(ints)), rows)
      val cached = CometCachedBatchHelper.cachedBatchWithBodyCompression(batch, unknownCodec)

      val attrs = Seq(AttributeReference("i", IntegerType)())
      val thrown = interceptDecodeFailure {
        new ArrowCachedBatchSerializer()
          .convertCachedBatchToColumnarBatch(
            spark.sparkContext.parallelize(Seq(cached), 1),
            attrs,
            attrs,
            spark.sessionState.conf)
          .mapPartitions(it => Iterator.single(it.map(_.numRows().toLong).sum))
          .collect()
      }
      assert(
        causeChain(thrown).exists(t =>
          Option(t.getMessage)
            .exists(_.contains(s"unknown Arrow compression codec: $unknownCodec"))),
        s"an unrecognized codec must be reported as itself: $thrown")
    } finally {
      ints.close()
    }
  }

  test("Comet in-memory cache reads a projection of only null-typed columns") {
    // A NullVector owns a field node but no buffers, so a scan that selects nothing else asks the
    // read path to copy out no buffers at all, and under a codec to decompress none. Paired with
    // another column, it is a NullVector inside an ordinary payload instead.
    val query = "SELECT id, NULL AS n FROM range(0, 1000, 1, 2)"
    // Collected before anything is cached, so the cache cannot stand in for the reference.
    val expectedNulls = uncachedSparkAnswer(s"SELECT n FROM ($query)")
    val expectedPairs = uncachedSparkAnswer(s"SELECT id, n FROM ($query) ORDER BY id")
    assert(expectedNulls.length == 1000)

    Seq("none", "zstd").foreach { codec =>
      withConversions(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> "true",
        CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true",
        CometConf.COMET_EXEC_IN_MEMORY_CACHE_COMPRESSION_CODEC.key -> codec) {

        spark.catalog.clearCache()
        val view = s"null_cache_$codec"
        spark.sql(query).createOrReplaceTempView(view)
        spark.catalog.cacheTable(view)
        spark.table(view).count()
        assert(
          cachedBatchTypes(view).sameElements(
            Array("org.apache.spark.sql.comet.execution.arrow.CometCachedBatch")),
          s"codec $codec should still store CometCachedBatch")

        assert(spark.sql(s"SELECT n FROM $view").collect() === expectedNulls)
        assert(spark.sql(s"SELECT id, n FROM $view ORDER BY id").collect() === expectedPairs)

        spark.catalog.clearCache()
      }
    }
  }

  test("Comet in-memory cache round-trips a batch with no rows") {
    // Every buffer of a batch with no rows is empty. zstd writes each of those as a bare
    // uncompressed-length prefix of zero, and without a codec each is a zero-length window, so
    // this is the batch where every window the read path copies out is empty or nearly so.
    val cacheSchema = StructType(Seq(StructField("i", IntegerType), StructField("s", StringType)))
    Seq[CompressionCodec](NoCompressionCodec.INSTANCE, new ZstdCompressionCodec(1)).foreach {
      codec =>
        val ints = new IntVector("i", CometArrowAllocator)
        val strings = new VarCharVector("s", CometArrowAllocator)
        val cached =
          try {
            ints.allocateNew()
            ints.setValueCount(0)
            strings.allocateNew()
            strings.setValueCount(0)
            val batch = new ColumnarBatch(
              Array[ColumnVector](new CometPlainVector(ints), new CometPlainVector(strings)),
              0)
            CometCachedBatchHelper.cachedBatch(
              CometCachedBatchHelper.serialize(batch, codec, CometArrowAllocator),
              0)
          } finally {
            ints.close()
            strings.close()
          }

        val allocator =
          CometArrowAllocator.newChildAllocator(
            s"zero-rows-${codec.getCodecType}",
            0,
            Long.MaxValue)
        try {
          Seq(Array(0, 1), Array(1), Array(0)).foreach { selected =>
            val root = CometCachedBatchHelper.load(cached, cacheSchema, selected, allocator)
            try {
              assert(root.getRowCount == 0)
              assert(
                root.getSchema.getFields.asScala.map(_.getName) ==
                  selected.map(cacheSchema(_).name).toSeq,
                s"${codec.getCodecType} read the wrong columns for ${selected.mkString(",")}")
            } finally {
              root.close()
            }
          }
          assert(allocator.getAllocatedMemory == 0)
        } finally {
          allocator.close()
        }
    }
  }

  test("Comet in-memory cache stores no schema message per cached batch") {
    // The reader rebuilds the schema from the cached relation's attributes, so storing one in
    // every batch would repeat the same bytes for as many batches as the relation was cached in.
    withProjectionCache { (relation, batches) =>
      assert(batches.nonEmpty)
      val cacheSchema = Utils.fromAttributes(relation.output)
      batches.foreach { batch =>
        assert(
          !CometCachedBatchHelper.hasSchemaMessage(batch),
          "a cached batch must begin with its record batch, not a schema message")
        val sizes = CometCachedBatchHelper.columnSizes(batch, cacheSchema)
        assert(
          sizes.length == relation.output.length,
          "every cached column must own a run of buffers in the payload")
        assert(sizes.forall(_ > 0), "every cached column must carry data")
      }
    }
  }

  // Every column of both relations takes a turn as the sole projection. Timings would be a weak
  // assertion here, so each turn scrambles the compressed bytes of the columns the read must not
  // touch, leaving every other byte of the payload identical. Reading still has to succeed, which
  // it only can if those columns' buffers were never copied out of the payload and handed to the
  // decompressor. Each turn then corrupts the selected column too, so the assertion cannot pass
  // just because the bad bytes decode silently to nothing.
  //
  // The nested relation is what exercises the span arithmetic. A flat column always owns one field
  // node and two or three buffers, whereas a nested one owns a run as long as its whole subtree,
  // so a run computed short or long by a buffer shifts every column after it -- and which column
  // is selected decides whether that misalignment reaches into a corrupted neighbour.
  for {
    (shape, withCache) <- Seq[(String, ProjectionFixture)](
      ("flat", chunkSize => f => withProjectionCache(chunkSize)(f)),
      ("nested", chunkSize => f => withNestedProjectionCache(chunkSize)(f)))
    (chunking, chunkSize) <- chunkSizes
  } {
    test(
      s"Comet in-memory cache decodes only the projected columns of a $shape relation " +
        s"($chunking payload)") {
      withCache(chunkSize) { (relation, batches) =>
        if (chunkSize.isDefined) {
          assert(
            batches.forall(b => CometCachedBatchHelper.chunkCount(b) > 1),
            "every payload should span several chunks, or this runs nothing across a boundary")
        }
        val cacheSchema = Utils.fromAttributes(relation.output)
        val pristine = CometCachedBatchHelper.snapshotPayloads(batches)

        relation.output.indices.foreach { i =>
          assert(
            batches.forall(b => CometCachedBatchHelper.columnIsCompressed(b, cacheSchema, i)),
            s"column ${relation.output(i).name} is not stored compressed, so corrupting it " +
              "would prove nothing")
        }

        relation.output.indices.foreach { selectedIdx =>
          CometCachedBatchHelper.restorePayloads(batches, pristine)
          val selected = Seq(relation.output(selectedIdx))
          val name = relation.output(selectedIdx).name

          relation.output.indices.filter(_ != selectedIdx).foreach { i =>
            batches.foreach(b => CometCachedBatchHelper.corruptColumn(b, cacheSchema, i))
          }
          assert(
            decodedRowCount(relation, batches, selected) == projectionCacheRows,
            s"reading $name must not decompress the other ${relation.output.length - 1} columns")

          batches.foreach(b => CometCachedBatchHelper.corruptColumn(b, cacheSchema, selectedIdx))
          interceptDecodeFailure {
            decodedRowCount(relation, batches, selected)
          }
        }
      }
    }
  }

  test("Comet in-memory cache rejects a payload that disagrees with the cached schema") {
    // Nothing in the payload says which schema wrote it, and `batch.buffers(j)` is an unchecked
    // flatbuffer accessor, so a reader working from a wider schema than the writer used would
    // otherwise copy windows from wherever the arithmetic landed: wrong values, or an
    // out-of-range read reported from inside the copy rather than as the layout problem it is.
    withProjectionCache { (relation, batches) =>
      val extra = AttributeReference("extra", LongType)()
      val thrown = intercept[Exception] {
        decodedRowCount(
          relation,
          batches,
          Seq(relation.output.head),
          cacheAttributes = Some(relation.output :+ extra))
      }
      assert(
        causeChain(thrown).exists(t =>
          Option(t.getMessage).exists(_.contains("does not match the cached schema"))),
        s"a layout mismatch must be reported as itself: $thrown")
    }
  }

  test("Comet in-memory cache converts a batch whose vectors do not match the cached layout") {
    // BinaryType is an Arrow Binary to the reader -- validity, offsets, data -- but a CometVector
    // may wrap a FixedSizeBinaryVector for the same Spark type, which has no offsets buffer. An
    // accelerated mapInArrow returning pa.binary(n) and an Iceberg fixed[N] read both produce one.
    // Since the payload stores no schema, writing that and reading a Binary shifts every buffer
    // from that column on, so the write path has to notice and convert instead. isArrowBacked
    // cannot: it accepts both vectors, as the first assertion of each case records.
    val cacheSchema = StructType(Seq(StructField("b", BinaryType)))
    val rows = 4

    val fixed = new FixedSizeBinaryVector("b", CometArrowAllocator, 3)
    try {
      fixed.allocateNew(rows)
      (0 until rows).foreach(i => fixed.set(i, Array[Byte](i.toByte, 1, 2)))
      fixed.setValueCount(rows)
      val batch = new ColumnarBatch(Array[ColumnVector](new CometPlainVector(fixed)), rows)
      assert(Utils.isArrowBacked(batch))
      assert(
        !CometCachedBatchHelper.writesDirectly(batch, cacheSchema),
        "a fixed-size binary vector does not have the layout the reader rebuilds for BinaryType")
    } finally {
      fixed.close()
    }

    val varBinary = new VarBinaryVector("b", CometArrowAllocator)
    try {
      varBinary.allocateNew(rows)
      (0 until rows).foreach(i => varBinary.set(i, Array[Byte](i.toByte, 1, 2)))
      varBinary.setValueCount(rows)
      val batch = new ColumnarBatch(Array[ColumnVector](new CometPlainVector(varBinary)), rows)
      assert(Utils.isArrowBacked(batch))
      assert(
        CometCachedBatchHelper.writesDirectly(batch, cacheSchema),
        "the vector Comet's own scans produce for BinaryType must still take the direct path")
    } finally {
      varBinary.close()
    }
  }

  // Ordering by the JSON form rather than by the columns themselves, since a projection that
  // excludes `id` has no orderable key of its own and ORDER BY over a map is not allowed.
  private def orderedByJson(cols: Seq[String], from: String): String =
    s"SELECT to_json(struct(${cols.mkString(", ")})) AS j FROM $from ORDER BY j"

  test("Comet in-memory cache reads correct nested values under a narrow projection") {
    // The corruption test above proves the projected read leaves the other columns' bytes alone,
    // but it asserts on row counts, and a row count comes from the record batch header rather than
    // from any buffer. A window taken from the wrong place within the selected column's own subtree
    // -- a child's buffers swapped, say -- still decodes to the right number of rows and the wrong
    // values. Comparing values against the uncached query is what rules that out.
    withNativeCache {
      val query =
        s"SELECT ${nestedProjectionColumns.mkString(", ")} FROM range($projectionCacheRows)"
      val names = Seq("id", "sc", "ar", "mp", "deep", "tail")

      // Each nested column on its own, then paired with `id`, then two projections that ask for
      // columns out of cache-schema order, then the whole relation. Spark selects cached columns in
      // whatever order the query wants them, and a full projection cannot stand in for that: with
      // every column selected in order, the projected schema and the node/buffer windows are both
      // the whole sequence, so nothing distinguishes them from windows taken in a different order.
      val projections =
        names.filter(_ != "id").map(Seq(_)) ++
          names.filter(_ != "id").map(n => Seq("id", n)) ++
          Seq(Seq("tail", "mp", "id"), Seq("deep", "sc")) ++
          Seq(names)

      // Collected before the relation is cached. Once it is, Spark answers this same query from the
      // cache too, so a reference taken afterwards would compare the cache with itself.
      val expected =
        projections.map(cols => uncachedSparkAnswer(orderedByJson(cols, s"($query)")))
      expected.foreach(rows => assert(rows.length == projectionCacheRows))

      spark.sql(query).createOrReplaceTempView("nested_value_cache")
      spark.catalog.cacheTable("nested_value_cache")
      spark.table("nested_value_cache").count()

      assert(
        cachedBatchTypes("nested_value_cache").sameElements(
          Array("org.apache.spark.sql.comet.execution.arrow.CometCachedBatch")))

      projections.zip(expected).foreach { case (cols, rows) =>
        val list = cols.mkString(", ")
        val df = spark.sql(orderedByJson(cols, "nested_value_cache"))
        assert(
          df.queryExecution.executedPlan.toString().contains("CometInMemoryTableScan"),
          s"projection ($list) should read the cache natively")
        assert(df.collect() === rows, s"projection ($list) read the wrong values")
      }
    }
  }

  test("Comet in-memory cache reads correct values from a payload split across many chunks") {
    // A payload is stored in heap chunks so that one cached batch is not capped at the 2 GiB a
    // single JVM array holds. That limit is not reachable in a unit test, but everything it relies
    // on is: with chunks this small, every buffer window the read path copies out starts in one
    // chunk and ends in another, as does the metadata message ahead of the body. Values are
    // compared against the uncached query rather than a row count, since a window stitched
    // together from the wrong chunk offsets still decodes to the right number of rows.
    val source = s"SELECT ${nestedProjectionColumns.mkString(", ")} " +
      s"FROM range(0, $projectionCacheRows, 1, 2)"
    val projections =
      Seq(Seq("id", "sc", "ar", "mp", "deep", "tail"), Seq("tail", "mp", "id"), Seq("deep", "sc"))
    // Collected before the fixture caches the relation, for the reason given in the test above.
    val expected = projections.map(cols => uncachedSparkAnswer(orderedByJson(cols, s"($source)")))
    expected.foreach(rows => assert(rows.length == projectionCacheRows))

    withNestedProjectionCache(Some(tinyChunkSize)) { (relation, batches) =>
      assert(relation.output.map(_.name) == projections.head)
      batches.foreach { batch =>
        assert(
          CometCachedBatchHelper.chunkCount(batch) > 1,
          "a payload larger than the chunk size must be stored in more than one chunk")
      }

      projections.zip(expected).foreach { case (cols, rows) =>
        val list = cols.mkString(", ")
        val df = spark.sql(orderedByJson(cols, "nested_projection_cache"))
        assert(
          df.queryExecution.executedPlan.toString().contains("CometInMemoryTableScan"),
          s"projection ($list) should read the cache natively")
        assert(df.collect() === rows, s"projection ($list) read the wrong values")
      }
    }
  }

  test("Comet in-memory cache decodes no columns for a row-count-only read") {
    // SELECT count(*) selects no columns. Every column's bytes are corrupted, so the read can
    // only succeed by touching none of them and answering from the row count the cached batch
    // already carries beside the payload.
    withProjectionCache { (relation, batches) =>
      val cacheSchema = Utils.fromAttributes(relation.output)
      relation.output.indices.foreach { i =>
        batches.foreach(b => CometCachedBatchHelper.corruptColumn(b, cacheSchema, i))
      }

      assert(decodedRowCount(relation, batches, Seq.empty) == projectionCacheRows)
    }
  }

  test("Comet in-memory cache records per-column decoded sizes in its statistics") {
    // Each column's size field must be its decoded size (see ArrowCachedBatchSerializer.statsRow),
    // so each field is compared with its column decoded back out of the payload, not with what the
    // column occupies compressed. Run over the nested relation as well: a nested column's size is
    // the sum of its whole subtree, so this is also where a size attributed to the wrong column
    // surfaces. And over the dictionary relation, whose columns reach the writer dictionary
    // encoded: a size measured before they are decoded would be the size of their indices.
    def checkSizes(
        relation: org.apache.spark.sql.execution.columnar.InMemoryRelation,
        batches: Array[CachedBatch]): Unit = {
      val cacheSchema = Utils.fromAttributes(relation.output)
      val allocator = CometArrowAllocator.newChildAllocator("decoded-sizes", 0, Long.MaxValue)
      try {
        batches.foreach { batch =>
          val sizes = CometCachedBatchHelper.decodedColumnSizes(batch, cacheSchema, allocator)
          val stats = batch.asInstanceOf[SimpleMetricsCachedBatch].stats
          sizes.zipWithIndex.foreach { case (size, i) =>
            assert(
              stats.getLong(i * 5 + 4) == size,
              s"column ${relation.output(i).name} should report its decoded size in the " +
                "statistics row")
          }
          assert(batch.sizeInBytes == sizes.sum)
        }
      } finally {
        allocator.close()
      }
    }

    withProjectionCache(checkSizes _)
    withNestedProjectionCache(checkSizes _)
    withDictionaryCache(relation =>
      checkSizes(relation, relation.cacheBuilder.cachedColumnBuffers.collect()))
  }

  test("Comet in-memory cache reports the same relation size under every codec") {
    // The planner compares this size with broadcast thresholds and with the other side of a
    // shuffled hash join, so it must not depend on the codec (see
    // ArrowCachedBatchSerializer.statsRow).
    val measured = Seq.newBuilder[(String, Long, Long)]
    Seq("zstd", "none").foreach { codec =>
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true",
        CometConf.COMET_EXEC_IN_MEMORY_CACHE_COMPRESSION_CODEC.key -> codec) {
        spark.catalog.clearCache()
        spark
          .range(0, 20000, 1, 2)
          .selectExpr("id", "id % 7 AS k", "concat('v_', cast(id % 100 AS string)) AS s")
          .createOrReplaceTempView("codec_size_cache")
        spark.catalog.cacheTable("codec_size_cache")
        assert(spark.table("codec_size_cache").count() == 20000)
        val relation = spark.sharedState.cacheManager
          .lookupCachedData(spark.table("codec_size_cache"))
          .get
          .cachedRepresentation
        val batches = relation.cacheBuilder.cachedColumnBuffers.collect()
        val payload = batches.map(CometCachedBatchHelper.payloadSize).sum
        measured += ((codec, relation.computeStats().sizeInBytes.toLong, payload))
        spark.catalog.clearCache()
      }
    }

    val sizes = measured.result()
    val (_, zstdSize, zstdPayload) = sizes(0)
    val (_, plainSize, _) = sizes(1)
    assert(zstdPayload * 2 < zstdSize, s"zstd should compress this relation: $sizes")
    assert(zstdSize == plainSize, s"the relation size should not depend on the codec: $sizes")
  }

  test("Comet in-memory cache scans no columns for a row-count-only query") {
    // SELECT count(*) selects no columns, and the scan must keep it that way. Widening it -- to
    // the whole cache schema, or to a single placeholder column -- makes the emitted batches
    // disagree with the scan's declared output, which is wrong for any consumer that reads by
    // ordinal instead of by row count. See the join regression below.
    withConversions(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true",
      SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> "true") {

      spark.catalog.clearCache()
      spark
        .range(0, 500, 1, 2)
        .selectExpr(
          "id",
          "id % 100 AS k",
          "concat('a_', cast(id as string)) AS s1",
          "cast(id % 2 = 0 as boolean) AS flag")
        .createOrReplaceTempView("count_only_cache")
      spark.catalog.cacheTable("count_only_cache")
      assert(spark.table("count_only_cache").count() == 500)

      val df = spark.sql("SELECT count(*) FROM count_only_cache")
      val scan = df.queryExecution.executedPlan.collectFirst {
        case s: CometInMemoryTableScanExec => s
      }

      assert(scan.isDefined, "expected a native cache scan")
      assert(scan.get.output.isEmpty, "a count-only scan declares no output")
      assert(
        scan.get.scanOutput.isEmpty,
        s"expected no scanned columns, got ${scan.get.scanOutput.map(_.name).mkString(",")}")

      checkAnswer(df, Seq(Row(500L)))
      spark.catalog.clearCache()
    }
  }

  test("Comet in-memory cache joins correctly over an empty-output cache scan") {
    // An empty-output cache scan can feed a join, not only a count-style aggregate. A join reads
    // its inputs by ordinal, so any column the scan emits beyond its declared output shifts the
    // right side's positions and silently produces wrong results rather than failing.
    withConversions(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true") {

      spark.catalog.clearCache()
      val left = spark.range(10L, 13L).cache()
      left.collect()
      left.createOrReplaceTempView("cached_left")

      // 3 left rows joined to 2 right rows, summing only the right side: 3 * (0 + 1) == 3.
      // Leaking the left id column into the scan output made this read 10 + 11 + 12 twice.
      checkAnswer(
        spark.sql("""
          |SELECT /*+ BROADCAST(r) */ sum(r.id)
          |FROM cached_left l JOIN range(2) r ON true
        """.stripMargin),
        Seq(Row(3L)))

      checkAnswer(
        spark.sql("""
          |SELECT /*+ BROADCAST(r) */ r.id
          |FROM cached_left l JOIN range(2) r ON true
        """.stripMargin),
        Seq.fill(3)(Seq(Row(0L), Row(1L))).flatten)

      spark.catalog.clearCache()
    }
  }

  test(
    "Comet in-memory cache re-encodes a decoded batch whose columns have separate dictionaries") {
    // Spark's columnar Union hands decoded cached batches straight back to this serializer, so
    // caching a union of a cached relation re-encodes batches that came out of the cache. The
    // cache no longer stores dictionary-encoded columns -- the writer decodes them first -- but
    // batches reaching serializeBatches from a shuffle or broadcast still carry independent
    // dictionary providers whose IDs collide, and re-encoding one with only the first column's
    // provider cannot resolve the later columns' IDs.
    withConversions(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm") {

      spark.catalog.clearCache()
      val first = spark
        .range(0, 200, 1, 2)
        .selectExpr(
          "concat('a_', cast(id % 3 as string)) AS s1",
          "concat('b_', cast(id % 4 as string)) AS s2")
        .repartition(2)
        .cache()
      assert(first.count() == 200)

      withSQLConf(
        CometConf.COMET_EXEC_ENABLED.key -> "false",
        CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "false") {
        val second = first.union(first).cache()
        assert(second.count() == 400)
        second.unpersist()
      }

      first.unpersist()
      spark.catalog.clearCache()
    }
  }

  test("Comet in-memory cache does not build the cached RDD while planning") {
    // CachedRDDBuilder.cachedColumnBuffers builds its RDD by executing the cached plan, so
    // touching it during planning runs jobs before the outer query is even submitted. With an
    // adaptively-cached relation that also finalizes the cached plan. EXPLAIN must launch nothing.
    withConversions(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      // Spark caches through a session with some configs forced off, and on 3.4 that list still
      // includes AQE itself, so the cached plan comes back non-adaptive and there is nothing to
      // finalize. This conf is what decides that list; 3.5 defaults it on, and 4.0 stopped
      // disabling AQE either way. Setting it keeps the relation adaptive on every version.
      SQLConf.CAN_CHANGE_CACHED_PLAN_OUTPUT_PARTITIONING.key -> "true",
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true") {

      spark.catalog.clearCache()
      val cached = spark.range(100).repartition(2).cache()
      cached.createOrReplaceTempView("cached_adaptive")

      val builder = spark
        .sql("SELECT * FROM cached_adaptive")
        .queryExecution
        .optimizedPlan
        .collectFirst { case r: InMemoryRelation => r.cacheBuilder }
        .get
      // The cached plan is adaptive and has not run, so AQE has not finalized it. Building the
      // cached RDD executes that plan, which finalizes it; isCachedColumnBuffersLoaded is not the
      // signal to use here, since it additionally requires the blocks to be populated.
      assert(
        builder.cachedPlan.toString.contains("isFinalPlan=false"),
        "cached plan was already finalized before the test ran")

      spark.sql("SELECT * FROM cached_adaptive").explain()

      assert(
        builder.cachedPlan.toString.contains("isFinalPlan=false"),
        "planning must not build the cached RDD: doing so executes the cached plan")

      // It must still be built when the query actually runs.
      assert(spark.sql("SELECT * FROM cached_adaptive").count() == 100)

      cached.unpersist()
      spark.catalog.clearCache()
    }
  }

  test("Comet in-memory cache releases its vectors when a column fails to decode") {
    // Reading a batch allocates twice before anything can go wrong: the root that receives the
    // projected columns, and the off-heap body the selected buffers are copied into. A column
    // that fails to decompress throws between the two, and neither is reachable from anywhere
    // else -- the holder is published to the task-completion listener only once its constructor
    // returns -- so a failure that does not release them leaks off-heap for the life of the
    // executor.
    //
    // Two corruption points, because they fail at different depths. Taking out a column's first
    // buffer fails before anything of that column has been decompressed. Taking out only the last
    // buffer of a string column -- whose offsets and data are separately compressed -- decompresses
    // one buffer into a fresh allocation and then throws on the next, leaving that allocation
    // reachable from nothing the failure path can see. The second is the one that catches a leak
    // in `VectorLoader`; the first is the one that catches a cleanup path releasing the shared
    // body twice. Both run over a multi-chunk payload too, where the copy into the body walks
    // several chunks before the failure.
    chunkSizes.foreach { case (_, chunkSize) =>
      withProjectionCache(chunkSize) { (relation, batches) =>
        val cacheSchema = Utils.fromAttributes(relation.output)
        val pristine = CometCachedBatchHelper.snapshotPayloads(batches)
        val stringIdx = 3
        assert(relation.output(stringIdx).dataType.typeName == "string")

        val cases = Seq(
          (
            "a column's first buffer",
            // Corrupt the second selected column, so the first is copied out successfully first.
            (b: CachedBatch) => CometCachedBatchHelper.corruptColumn(b, cacheSchema, 1),
            Seq(relation.output(0), relation.output(1))),
          (
            "a string column's trailing buffer",
            (b: CachedBatch) =>
              CometCachedBatchHelper.corruptTrailingBuffer(b, cacheSchema, stringIdx),
            Seq(relation.output(stringIdx))))

        cases.foreach { case (where, corrupt, selected) =>
          CometCachedBatchHelper.restorePayloads(batches, pristine)
          batches.foreach(corrupt)

          val before = CometArrowAllocator.getAllocatedMemory
          interceptDecodeFailure {
            decodedRowCount(relation, batches, selected)
          }
          assert(
            CometArrowAllocator.getAllocatedMemory == before,
            s"everything allocated before a failure in $where must be released")
        }
      }
    }
  }

  /**
   * A zstd codec that compresses the first `succeedFor` buffers and then throws.
   *
   * Delegating to the real codec until it fails is the point: the buffers already compressed are
   * genuine off-heap allocations reachable only from inside the writer, which is what a real
   * failure -- zstd unable to allocate its workspace part way through a batch -- leaves behind.
   */
  private class FailAfterCompressionCodec(succeedFor: Int) extends CompressionCodec {
    private val delegate = new ZstdCompressionCodec(1)
    var compressed: Int = 0

    override def compress(allocator: BufferAllocator, buffer: ArrowBuf): ArrowBuf = {
      if (compressed == succeedFor) {
        throw new RuntimeException(FailAfterCompressionCodec.Message)
      }
      compressed += 1
      delegate.compress(allocator, buffer)
    }

    override def decompress(allocator: BufferAllocator, buffer: ArrowBuf): ArrowBuf =
      delegate.decompress(allocator, buffer)

    override def getCodecType: CompressionUtil.CodecType = delegate.getCodecType
  }

  private object FailAfterCompressionCodec {
    val Message: String = "injected compression failure"
  }

  test("Comet in-memory cache releases its buffers when a column fails to compress") {
    // Writing a batch allocates a buffer per compressed buffer before any payload exists, and
    // nothing outside the writer can reach them while it is still assembling the record batch they
    // belong to. This is why the codec is not handed to `VectorUnloader`: it accumulates them in a
    // list local to `getRecordBatch`, which is off the stack by the time a caller sees the failure,
    // so a single failed materialization would leak a batch's worth of off-heap for the life of the
    // executor.
    val rows = 256
    val ints = new IntVector("i", CometArrowAllocator)
    val strings = new VarCharVector("s", CometArrowAllocator)
    try {
      ints.allocateNew(rows)
      (0 until rows).foreach(i => ints.set(i, i))
      ints.setValueCount(rows)

      strings.allocateNew(rows)
      (0 until rows).foreach(i => strings.setSafe(i, s"value_$i".getBytes("UTF-8")))
      strings.setValueCount(rows)

      val batch = new ColumnarBatch(
        Array[ColumnVector](new CometPlainVector(ints), new CometPlainVector(strings)),
        rows)

      // An int vector is validity and data, a varchar validity, offsets and data: five buffers in
      // all. Succeeding for two puts the failure at the string column's first buffer, with the int
      // column's two already compressed into allocations only the writer can reach. Failing at the
      // very first buffer would pass with no cleanup at all.
      val codec = new FailAfterCompressionCodec(succeedFor = 2)
      val before = CometArrowAllocator.getAllocatedMemory
      val thrown = intercept[Exception] {
        CometCachedBatchHelper.serialize(batch, codec, CometArrowAllocator)
      }

      assert(
        causeChain(thrown).exists(t =>
          Option(t.getMessage).contains(FailAfterCompressionCodec.Message)),
        s"a compression failure must surface as itself: $thrown")
      assert(codec.compressed == 2, "the failure must come after some buffers were compressed")
      assert(
        CometArrowAllocator.getAllocatedMemory == before,
        "everything allocated before a write failure must be released")
    } finally {
      // Never reaches the writer's own clear(), which only runs once the payload is built.
      ints.close()
      strings.close()
    }
  }

  test("Comet in-memory cache keeps no compressed bytes alive behind a buffer stored raw") {
    // Arrow stores a buffer verbatim when compressing it would not make it smaller -- a bitmap of
    // random bits, say -- and decompressing hands it back as a slice of whatever holds it. Copied
    // into the same allocation as the compressed buffers of the batch, the smallest such buffer
    // would keep every one of those compressed bytes alive for as long as the decoded vectors.
    val rows = 4096
    val random = new scala.util.Random(42)
    // 0 for null, otherwise false or true: random enough that neither bitmap compresses.
    val flags = Array.fill(rows)(random.nextInt(3))
    // 256 hex digits of random longs per value, which compress to about half.
    val strings = Array.fill(rows)((0 until 16).map(_ => f"${random.nextLong()}%016x").mkString)

    val bits = new BitVector("b", CometArrowAllocator)
    val chars = new VarCharVector("s", CometArrowAllocator)
    val cached =
      try {
        bits.allocateNew(rows)
        chars.allocateNew(rows.toLong * 256, rows)
        (0 until rows).foreach { i =>
          if (flags(i) == 0) bits.setNull(i) else bits.set(i, flags(i) - 1)
          chars.setSafe(i, strings(i).getBytes(StandardCharsets.UTF_8))
        }
        bits.setValueCount(rows)
        chars.setValueCount(rows)
        val batch = new ColumnarBatch(
          Array[ColumnVector](new CometPlainVector(bits), new CometPlainVector(chars)),
          rows)
        CometCachedBatchHelper.cachedBatch(
          CometCachedBatchHelper.serialize(
            batch,
            new ZstdCompressionCodec(1),
            CometArrowAllocator),
          rows)
      } finally {
        bits.close()
        chars.close()
      }

    val cacheSchema = StructType(Seq(StructField("b", BooleanType), StructField("s", StringType)))
    assert(
      CometCachedBatchHelper.columnHasRawBuffer(cached, cacheSchema, 0),
      "the test needs a column Arrow stores raw")
    assert(CometCachedBatchHelper.columnIsCompressed(cached, cacheSchema, 1))
    val compressedBytes = CometCachedBatchHelper.columnSizes(cached, cacheSchema)(1)
    assert(compressedBytes > 256 * 1024, s"the compressed column is only $compressedBytes bytes")

    val allocator = CometArrowAllocator.newChildAllocator("raw-buffer-test", 0, Long.MaxValue)
    try {
      val root = CometCachedBatchHelper.load(cached, cacheSchema, Array(0, 1), allocator)
      try {
        val b = root.getVector(0).asInstanceOf[BitVector]
        val s = root.getVector(1).asInstanceOf[VarCharVector]
        (0 until rows).foreach { i =>
          if (flags(i) == 0) assert(b.isNull(i)) else assert(b.get(i) == flags(i) - 1)
          assert(new String(s.get(i), StandardCharsets.UTF_8) == strings(i))
        }
        b.getBuffers(false).foreach { buffer =>
          val backing = buffer.getReferenceManager.getSize
          assert(
            backing < 64 * 1024,
            s"a ${buffer.capacity()}-byte bitmap is keeping a $backing-byte allocation alive")
        }
      } finally {
        root.close()
      }
      assert(allocator.getAllocatedMemory == 0)
    } finally {
      allocator.close()
    }
  }

  /**
   * Cache two low-cardinality string columns and hand the test the cached relation.
   *
   * The shuffle is what makes this worth its own fixture: its reader hands the cache writer
   * dictionary-encoded columns, which the writer has to decode before storing them.
   */
  private def withDictionaryCache(f: InMemoryRelation => Unit): Unit = {
    withConversions(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm") {

      spark.catalog.clearCache()
      spark
        .range(0, 2000, 1, 2)
        .selectExpr(
          "concat('a_', cast(id % 3 as string)) AS s1",
          "concat('b_', cast(id % 4 as string)) AS s2")
        .repartition(2)
        .createOrReplaceTempView("dictionary_cache")
      spark.catalog.cacheTable("dictionary_cache")
      assert(spark.table("dictionary_cache").count() == 2000)
      assert(
        cachedBatchTypes("dictionary_cache").sameElements(
          Array("org.apache.spark.sql.comet.execution.arrow.CometCachedBatch")))

      val relation = spark.sharedState.cacheManager
        .lookupCachedData(spark.table("dictionary_cache"))
        .get
        .cachedRepresentation

      try {
        f(relation)
      } finally {
        spark.catalog.clearCache()
      }
    }
  }

  test("Comet in-memory cache decodes dictionary-encoded columns before storing them") {
    // The payload carries no schema, so it has nowhere to record that a column is dictionary
    // encoded, nor the dictionary itself. The reader rebuilds a plain Utf8 field for a string
    // column either way, so a writer that stored the index vector as-is would hand the loader
    // integer indices to read as strings. Reading the values back correctly is what proves the
    // writer decoded them first; a row count alone would not.
    withDictionaryCache { relation =>
      assert(relation.output.length == 2)

      val df = spark.sql("SELECT s1, s2 FROM dictionary_cache")
      checkAnswer(df, (0 until 2000).map(i => Row(s"a_${i % 3}", s"b_${i % 4}")))

      val distinct =
        spark.sql("SELECT DISTINCT s1, s2 FROM dictionary_cache ORDER BY s1, s2").collect()
      assert(distinct.length == 12, "3 distinct s1 values by 4 distinct s2 values")
      assert(distinct.head.getString(0) == "a_0" && distinct.head.getString(1) == "b_0")
    }
  }

  test("Comet in-memory cache broadcasts a batch read back from the cache") {
    // A broadcast of a cache scan re-serializes each decoded batch through serializeBatches,
    // which is a different writer from the one that produced the cached payload.
    withDictionaryCache { relation =>
      assert(relation.output.length == 2)

      val df = spark.sql(
        "SELECT /*+ BROADCAST(c) */ c.s1, c.s2 FROM range(1) r JOIN dictionary_cache c ON true")
      checkAnswer(df, (0 until 2000).map(i => Row(s"a_${i % 3}", s"b_${i % 4}")))
      assert(df.count() == 2000)
    }
  }

  test("Comet in-memory cache scans of one cache canonicalize equal, so exchanges are reused") {
    // The wrapped Spark scan is a plan-typed field rather than a child, so canonicalization walks
    // past it and leaves in place the expression IDs of whichever occurrence of the relation
    // produced it. sameResult is what exchange and broadcast reuse are keyed on, so two
    // equivalent scans that compare unequal make a query shuffle and aggregate one cache twice.
    withConversions(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm") {

      spark.catalog.clearCache()
      spark
        .range(0, 400, 1, 2)
        .selectExpr("id", "id % 10 AS k")
        .createOrReplaceTempView("reuse_cache")
      spark.catalog.cacheTable("reuse_cache")
      assert(spark.table("reuse_cache").count() == 400)

      val df = spark.sql(
        "SELECT k, count(*) AS c FROM reuse_cache GROUP BY k " +
          "UNION ALL SELECT k, count(*) AS c FROM reuse_cache GROUP BY k")
      checkAnswer(df, Seq.fill(2)((0L until 10L).map(k => Row(k, 40L))).flatten)

      val plan = df.queryExecution.executedPlan
      val exchanges = plan.collect { case e: Exchange => e }
      val reused = plan.collect { case r: ReusedExchangeExec => r }
      assert(
        exchanges.length == 1 && reused.length == 1,
        s"expected one exchange and one reuse of it, got ${exchanges.length} exchanges and " +
          s"${reused.length} reuses:\n$plan")

      // Canonicalization must not simply drop the wrapped scan: scans that differ only in the
      // predicates pushed into them have to stay distinct.
      def scanOf(query: String): CometInMemoryTableScanExec =
        spark
          .sql(query)
          .queryExecution
          .executedPlan
          .collectFirst { case s: CometInMemoryTableScanExec => s }
          .get

      val under100 = scanOf("SELECT k FROM reuse_cache WHERE id < 100")
      val under200 = scanOf("SELECT k FROM reuse_cache WHERE id < 200")
      assert(
        under100.originalPlan.predicates.nonEmpty,
        "expected the filter to be pushed into the cache scan")
      assert(
        under100.canonicalized != under200.canonicalized,
        "cache scans with different pruning predicates must not compare equal")

      spark.catalog.clearCache()
    }
  }

  test("Comet in-memory cache keeps the observed metrics recorded in a cached plan") {
    // Spark collects the metrics of an observe() inside a cached plan only through an
    // InMemoryTableScanExec over it, so the scan of such a relation has to stay Spark's. Replaced,
    // the metrics come back empty, and on Spark 3.4 Observation.get never returns. Nested the way
    // SPARK-35695's test nests it, with a shuffle in the inner cached plan so that AQE plans it,
    // under one more cache that records no metrics of its own. The metrics read through that top
    // scan are only found by following it into the caches it reads.
    withAQECache {
      val df = spark
        .range(0, 100, 1, 2)
        .repartition(4)
        .observe("inner_event", count(lit(1)).as("rows"), max($"id").as("max_id"))
        .persist()
        .observe("outer_event", min($"id").as("min_id"))
        .persist()
        .filter($"id" > 10)
        .persist()
      df.collect()
      assert(
        df.queryExecution.observedMetrics ==
          Map("inner_event" -> Row(100L, 99L), "outer_event" -> Row(0L)))
      val plan = df.queryExecution.executedPlan
      assert(collect(plan) { case s: CometInMemoryTableScanExec => s }.isEmpty)
      assert(
        new ExtendedExplainInfo()
          .generateExtendedInfo(plan)
          .contains("records Dataset.observe metrics"))
      // Still stored in Comet's format: only the scan changes.
      assert(
        spark.sharedState.cacheManager
          .lookupCachedData(df)
          .get
          .cachedRepresentation
          .cacheBuilder
          .cachedColumnBuffers
          .map(_.getClass.getName)
          .distinct()
          .collect()
          .sameElements(Array("org.apache.spark.sql.comet.execution.arrow.CometCachedBatch")))

      val observation = Observation("cached_observation")
      val observed = spark.range(10).observe(observation, sum($"id").as("total")).persist()
      observed.collect()
      // Bounded, so that a regression fails here rather than hanging the suite on Spark 3.4.
      assert(Await.result(Future(observation.get), 1.minute) == Map("total" -> 45L))

      val plain = spark.range(0, 100, 1, 2).persist()
      plain.collect()
      assert(collect(plain.queryExecution.executedPlan) { case s: CometInMemoryTableScanExec =>
        s
      }.nonEmpty)
    }
  }
}
