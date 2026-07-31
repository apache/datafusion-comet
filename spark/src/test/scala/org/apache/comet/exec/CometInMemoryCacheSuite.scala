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

import org.apache.spark.CometDriverPlugin
import org.apache.spark.SparkConf
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.catalyst.expressions.{And, Expression, GreaterThanOrEqual, LessThan, Literal}
import org.apache.spark.sql.columnar.SimpleMetricsCachedBatch
import org.apache.spark.sql.execution.columnar.CometInMemoryRelationHelper
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.storage.StorageLevel

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.isSpark40Plus

class CometInMemoryCacheSuite extends CometTestBase {

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

  private def cachedBatchTypes(table: String): Array[String] = {
    val cached = spark.sharedState.cacheManager.lookupCachedData(spark.table(table)).get
    cached.cachedRepresentation.cacheBuilder.cachedColumnBuffers
      .map(_.getClass.getName)
      .distinct()
      .collect()
  }

  test("CometInMemoryTableScan over CometCachedBatch") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm",
      SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> "true",
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true",
      "spark.comet.sparkToColumnar.enabled" -> "true") {

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
      checkSparkAnswer(df)

      val plan = df.queryExecution.executedPlan.toString()
      assert(plan.contains("CometInMemoryTableScan"))
      assert(!plan.contains("CometSparkColumnarToColumnar"))

      spark.catalog.clearCache()
    }
  }

  test("Comet in-memory cache disabled keeps SparkToColumnar fallback path") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm",
      SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> "true",
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true",
      "spark.comet.sparkToColumnar.enabled" -> "true") {

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

    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm",
      SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> "true",
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "false",
      "spark.comet.sparkToColumnar.enabled" -> "true") {

      val df = spark.sql("SELECT key, count(*) FROM comet_cache_disabled GROUP BY key")
      checkSparkAnswer(df)

      val plan = df.queryExecution.executedPlan.toString()
      assert(!plan.contains("CometInMemoryTableScan"))
      assert(plan.contains("CometSparkColumnarToColumnar"))

      spark.catalog.clearCache()
    }
  }

  test("Comet cache serializer delegates unsupported types to Spark's cache format") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm",
      SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> "true",
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true",
      "spark.comet.sparkToColumnar.enabled" -> "true") {

      spark.catalog.clearCache()

      // Interval types have no Arrow vector in Utils.getFieldVector. Without the schema check in
      // the serializer, caching this relation fails outright with "Unsupported Arrow Vector for
      // serialize: class org.apache.arrow.vector.DurationVector".
      spark
        .sql("""
          SELECT id AS key, make_dt_interval(0, 0, 0, id) AS dt
          FROM range(1000)
        """)
        .createOrReplaceTempView("default_cached_batch")

      spark.catalog.cacheTable("default_cached_batch")
      spark.table("default_cached_batch").count()

      assert(
        cachedBatchTypes("default_cached_batch").sameElements(
          Array("org.apache.spark.sql.execution.columnar.DefaultCachedBatch")))

      // Columnar read path, delegated to Spark's serializer.
      val columnarDf = spark.sql("""
        SELECT key, dt
        FROM default_cached_batch
        WHERE key >= 10 AND key < 20
      """)
      assert(columnarDf.collect().length == 10)
      checkSparkAnswer(columnarDf)

      val columnarPlan = columnarDf.queryExecution.executedPlan.toString()
      assert(!columnarPlan.contains("CometInMemoryTableScan"))

      // Row read path: disabling the vectorized cache reader makes Spark use
      // convertCachedBatchToInternalRow.
      withSQLConf(SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> "false") {
        val rowDf = spark.sql("""
          SELECT dt
          FROM default_cached_batch
          WHERE key >= 10 AND key < 20
        """)
        assert(rowDf.collect().length == 10)
        checkSparkAnswer(rowDf)

        val rowPlan = rowDf.queryExecution.executedPlan.toString()
        assert(!rowPlan.contains("CometInMemoryTableScan"))
      }

      spark.catalog.clearCache()
    }
  }

  test("Comet in-memory cache handles multi-partition cache") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm",
      SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> "true",
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true",
      "spark.comet.sparkToColumnar.enabled" -> "true") {

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
      checkSparkAnswer(grouped)

      val groupedPlan = grouped.queryExecution.executedPlan.toString()
      assert(groupedPlan.contains("CometInMemoryTableScan"))

      multiPartition.unpersist()
      spark.catalog.clearCache()
    }
  }

  test("Comet in-memory cache handles empty cache") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm",
      SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> "true",
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true",
      "spark.comet.sparkToColumnar.enabled" -> "true") {

      spark.catalog.clearCache()

      val empty = spark.range(0).toDF("id").cache()
      empty.createOrReplaceTempView("empty_cache")
      empty.count()

      val emptyDf = spark.sql("SELECT * FROM empty_cache")
      checkSparkAnswer(emptyDf)

      val emptyPlan = emptyDf.queryExecution.executedPlan.toString()
      assert(emptyPlan.contains("CometInMemoryTableScan"))
      assert(!emptyPlan.contains("CometSparkColumnarToColumnar"))

      empty.unpersist()
      spark.catalog.clearCache()
    }
  }

  test("Comet in-memory cache supports projection-only read") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm",
      SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> "true",
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true",
      "spark.comet.sparkToColumnar.enabled" -> "true") {

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
      checkSparkAnswer(df)

      val plan = df.queryExecution.executedPlan.toString()
      assert(plan.contains("CometInMemoryTableScan"))
      assert(plan.contains("CometNativeColumnarToRow"))

      spark.catalog.clearCache()
    }
  }

  test("Comet in-memory cache supports shuffle after cache read") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm",
      SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> "true",
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true",
      "spark.comet.sparkToColumnar.enabled" -> "true") {

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
      checkSparkAnswer(df)

      val plan = df.queryExecution.executedPlan.toString()
      assert(plan.contains("CometInMemoryTableScan"))
      assert(plan.contains("CometHashAggregate"))

      spark.catalog.clearCache()
    }
  }

  test("Comet in-memory cache supports stats-based batch pruning") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm",
      SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> "true",
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true",
      "spark.comet.sparkToColumnar.enabled" -> "true",
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
      checkSparkAnswer(df)

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
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        CometConf.COMET_SHUFFLE_MODE.key -> "jvm",
        SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> "true",
        CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true",
        "spark.comet.sparkToColumnar.enabled" -> "true",
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
        checkSparkAnswer(df)

        val scans = df.queryExecution.executedPlan.collect {
          case s: org.apache.spark.sql.comet.CometInMemoryTableScanExec => s
        }
        assert(scans.length == 1, s"expected one CometInMemoryTableScan, got ${scans.length}")
        // scalastyle:off println
        println(
          "DIAG rows=" + df.collect().length + " metrics=" + scans.head.metrics
            .map { case (k, v) => k + "=" + v.value }
            .mkString(","))
        println("DIAG plan=" + df.queryExecution.executedPlan.getClass.getName)
        // scalastyle:on println
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
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm",
      SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> "true",
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true",
      "spark.comet.sparkToColumnar.enabled" -> "true") {

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
      checkSparkAnswer(df)
      val plan = df.queryExecution.executedPlan.toString()
      assert(plan.contains("CometInMemoryTableScan"))

      spark.catalog.clearCache()
    }
  }

  test("Comet plugin respects user-provided cache serializer") {
    val serializerKey = StaticSQLConf.SPARK_CACHE_SERIALIZER.key
    val cometSerializer =
      "org.apache.spark.sql.comet.execution.arrow.ArrowCachedBatchSerializer"
    val userSerializer = "com.example.CustomCachedBatchSerializer"

    val defaultConf = new SparkConf()
      .set(CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key, "true")
    val defaultExtraConfs = new ju.HashMap[String, String]()

    // With no user serializer configured, the plugin should install Comet's
    // serializer and also return it through extraConfs for executors.
    CometDriverPlugin.maybeSetCacheSerializer(defaultConf, defaultExtraConfs)

    assert(defaultConf.get(serializerKey) == cometSerializer)
    assert(defaultExtraConfs.get(serializerKey) == cometSerializer)

    val userConf = new SparkConf()
      .set(CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key, "true")
      .set(serializerKey, userSerializer)
    val userExtraConfs = new ju.HashMap[String, String]()

    // If the user already configured a cache serializer, keep it and do not
    // send a replacement serializer through extraConfs.
    CometDriverPlugin.maybeSetCacheSerializer(userConf, userExtraConfs)

    assert(userConf.get(serializerKey) == userSerializer)
    assert(!userExtraConfs.containsKey(serializerKey))
  }

  test("Comet in-memory cache supports empty projection scan") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm",
      SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> "true",
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true",
      "spark.comet.sparkToColumnar.enabled" -> "true") {

      spark.catalog.clearCache()

      spark
        .range(1000)
        .selectExpr("id as key", "id % 8 as value")
        .createOrReplaceTempView("count_cache")

      spark.catalog.cacheTable("count_cache")
      spark.table("count_cache").count()

      val df = spark.sql("SELECT count(*) FROM count_cache")
      checkSparkAnswer(df)

      val plan = df.queryExecution.executedPlan.toString()
      assert(plan.contains("CometInMemoryTableScan"))

      spark.catalog.clearCache()
    }
  }

  private def withNativeCache(f: => Unit): Unit = {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm",
      SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> "true",
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true",
      "spark.comet.sparkToColumnar.enabled" -> "true") {
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
      val expected = spark.sql(query).orderBy("l").collect()

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

  test("Comet in-memory cache prunes only on columns that have bounds") {
    assume(isSpark40Plus, "collated string types require Spark 4.0+")
    withNativeCache {
      // A collated StringType does not match `case StringType` in the serializer's bounds
      // tracking, so its lower and upper bounds stay null. Spark still builds a partition filter
      // for it because a collated string literal is an AtomicType, and comparing against null
      // bounds prunes every batch. Without the buildFilter guard this query returns no rows.
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

      // Null-count based pruning stays available for columns without bounds.
      assert(
        spark.sql("SELECT id FROM collated_cache WHERE s IS NOT NULL").collect().length == 100)
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
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm",
      SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> "true",
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true",
      "spark.comet.sparkToColumnar.enabled" -> "true",
      "spark.sql.inMemoryColumnarStorage.batchSize" -> "2") {

      spark.catalog.clearCache()

      spark
        .sql("""
        SELECT *
        FROM VALUES
          (0, CAST('NaN' AS DOUBLE), CAST('NaN' AS FLOAT)),
          (1, 1.0D, CAST(1.0 AS FLOAT)),
          (2, -0.0D, CAST(-0.0 AS FLOAT)),
          (3, 0.0D, CAST(0.0 AS FLOAT))
        AS t(id, d, f)
      """)
        .createOrReplaceTempView("nan_prune_cache")

      spark.catalog.cacheTable("nan_prune_cache")
      spark.table("nan_prune_cache").count()

      val doubleDf = spark.sql("""
        SELECT id
        FROM nan_prune_cache
        WHERE isnan(d)
      """)
      checkSparkAnswer(doubleDf)

      val floatDf = spark.sql("""
        SELECT id
        FROM nan_prune_cache
        WHERE isnan(f)
      """)
      checkSparkAnswer(floatDf)

      val zeroDf = spark.sql("""
        SELECT id
        FROM nan_prune_cache
        WHERE d = 0.0D OR f = CAST(0.0 AS FLOAT)
      """)
      checkSparkAnswer(zeroDf)

      val plan = doubleDf.queryExecution.executedPlan.toString()
      assert(plan.contains("CometInMemoryTableScan"))
      assert(!plan.contains("CometSparkColumnarToColumnar"))

      spark.catalog.clearCache()
    }
  }

  /**
   * Cache `view` over a Parquet file written by `write`, with the cached plan forced to be
   * Spark's own vectorized Parquet reader: its columns are On/OffHeapColumnVector rather than
   * CometVector. Spark's InMemoryRelation strips the ColumnarToRow above that scan because
   * supportsColumnarInput is true for the schema, so the serializer receives non-Arrow columnar
   * batches. Asserts the relation really was stored in Comet's format before handing control to
   * `f`.
   */
  private def withSparkColumnarCache(view: String, extraConfs: (String, String)*)(
      write: String => Unit)(f: => Unit): Unit = {
    withTempPath { path =>
      write(path.toString)

      withNativeCache {
        withSQLConf(
          Seq(
            CometConf.COMET_NATIVE_SCAN_ENABLED.key -> "false",
            CometConf.COMET_SPARK_TO_ARROW_ENABLED.key -> "false",
            SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true") ++ extraConfs: _*) {

          spark.read.parquet(path.toString).createOrReplaceTempView(view)
          spark.catalog.cacheTable(view)
          spark.table(view).count()

          assert(
            cachedBatchTypes(view).sameElements(
              Array("org.apache.spark.sql.comet.execution.arrow.CometCachedBatch")))

          f
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
    } {
      assert(spark.table("spark_columnar_cache").count() == 1000)

      checkSparkAnswer(
        spark.sql("SELECT * FROM spark_columnar_cache WHERE key >= 10 AND key < 20 ORDER BY key"))
      checkSparkAnswer(
        spark.sql("SELECT sum(key), sum(d), sum(dec), count(s), count(n), max(dt), max(ts) " +
          "FROM spark_columnar_cache"))
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
          "cast(id as binary) as b")
        .write
        .parquet(path)
    } {
      assert(spark.table("spark_columnar_complex").count() == 200)

      checkSparkAnswer(spark.sql("SELECT key, a, st, m, b FROM spark_columnar_complex"))
    }
  }
}
