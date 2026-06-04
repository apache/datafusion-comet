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
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}

import org.apache.comet.CometConf

class CometInMemoryCacheSuite extends CometTestBase {
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

  test("Comet cache serializer can read DefaultCachedBatch fallback data") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm",
      SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> "true",
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "false",
      "spark.comet.sparkToColumnar.enabled" -> "true") {

      spark.catalog.clearCache()

      spark
        .range(1000)
        .selectExpr("id as key", "id % 8 as value", "id + 1 as key_plus_1")
        .createOrReplaceTempView("default_cached_batch")

      spark.catalog.cacheTable("default_cached_batch")
      spark.table("default_cached_batch").count()

      assert(
        cachedBatchTypes("default_cached_batch").sameElements(
          Array("org.apache.spark.sql.execution.columnar.DefaultCachedBatch")))

      // Columnar read path: reads DefaultCachedBatch through
      // convertCachedBatchToColumnarBatch instead of throwing.
      val columnarDf = spark.sql("""
        SELECT key, value
        FROM default_cached_batch
        WHERE key >= 10 AND key < 20
      """)
      checkSparkAnswer(columnarDf)

      val columnarPlan = columnarDf.queryExecution.executedPlan.toString()
      assert(!columnarPlan.contains("CometInMemoryTableScan"))

      // Row read path: disabling the vectorized cache reader makes Spark use
      // convertCachedBatchToInternalRow. This verifies DefaultCachedBatch is
      // readable there as well.
      withSQLConf(SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> "false") {
        val rowDf = spark.sql("""
          SELECT key_plus_1
          FROM default_cached_batch
          WHERE key >= 10 AND key < 20
        """)
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
}
