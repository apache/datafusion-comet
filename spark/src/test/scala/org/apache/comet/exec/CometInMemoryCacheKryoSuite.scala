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

import org.apache.spark.SparkConf
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.execution.columnar.CometInMemoryRelationHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.storage.StorageLevel

import org.apache.comet.{CometConf, CometKryoRegistrator}

/**
 * Covers Comet's cached batch format under `spark.kryo.registrationRequired=true`.
 *
 * Kryo then rejects any class it has not been told about, and Spark serializes a `CachedBatch`
 * whenever a cached block leaves the heap: the disk half of the default `MEMORY_AND_DISK`, the
 * `_SER` levels, replication, and cross-executor fetches. So this is not a `DISK_ONLY`-only
 * concern -- a plain `df.cache()` that spills is enough to reach it. Spark registers its own
 * `ArrowCachedBatch` in `KryoSerializer.loadableSparkClasses`; Comet cannot add to that list, so
 * [[CometKryoRegistrator]] has to be set explicitly, and this suite is what proves it is
 * sufficient.
 *
 * This needs its own suite because `spark.serializer` and `spark.kryo.registrator` are read when
 * `SparkEnv` builds the serializer, so they cannot be changed per test.
 */
class CometInMemoryCacheKryoSuite extends CometTestBase {

  import testImplicits._

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
    val conf = super.sparkConf
    conf.set("spark.plugins", "org.apache.spark.CometPlugin")
    conf.set(
      "spark.sql.cache.serializer",
      "org.apache.spark.sql.comet.execution.arrow.ArrowCachedBatchSerializer")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrationRequired", "true")
    conf.set("spark.kryo.registrator", CometKryoRegistrator.CLASS_NAME)
    conf
  }

  private def cachedBatchTypes(table: String): Array[String] = {
    val cached = spark.sharedState.cacheManager.lookupCachedData(spark.table(table)).get
    cached.cachedRepresentation.cacheBuilder.cachedColumnBuffers
      .map(_.getClass.getName)
      .distinct()
      .collect()
  }

  // Every type whose bounds gatherColumnStats records, so the statistics row carries one of each
  // internal representation Kryo has to write: boxed primitives, UTF8String, and Decimal at both
  // sides of the long/BigDecimal split.
  private val statsColumns = Seq(
    "id AS c_long",
    "cast(id % 2 = 0 as boolean) AS c_bool",
    "cast(id % 100 as byte) AS c_byte",
    "cast(id % 100 as short) AS c_short",
    "cast(id as int) AS c_int",
    "cast(id as float) AS c_float",
    "cast(id as double) AS c_double",
    "cast(id as decimal(9,2)) AS c_dec_short",
    "cast(id as decimal(30,4)) AS c_dec_long",
    "cast(id as string) AS c_string",
    "cast(date '2020-01-01' + cast(id as int) as date) AS c_date",
    "timestamp '2020-01-01 00:00:00' + make_interval(0, 0, 0, 0, 0, 0, id) AS c_ts")

  // DISK_ONLY and MEMORY_AND_DISK_SER both serialize the block on put, so each one reaches Kryo
  // deterministically in local mode. The default MEMORY_AND_DISK reaches it only once a partition
  // spills, which is the case that makes this more than a DISK_ONLY concern but is not something a
  // test can force cheaply.
  Seq(StorageLevel.DISK_ONLY, StorageLevel.MEMORY_AND_DISK_SER)
    .foreach { level =>
      test(s"Comet in-memory cache round-trips through Kryo at $level") {
        withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
          CometConf.COMET_SHUFFLE_MODE.key -> "jvm",
          CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true",
          "spark.comet.sparkToColumnar.enabled" -> "true") {

          spark.catalog.clearCache()
          try {
            spark
              .range(0, 200, 1, 4)
              .selectExpr(statsColumns: _*)
              .createOrReplaceTempView("kryo_cache")

            spark.catalog.cacheTable("kryo_cache", level)
            assert(spark.table("kryo_cache").count() == 200)

            assert(
              cachedBatchTypes("kryo_cache").sameElements(
                Array("org.apache.spark.sql.comet.execution.arrow.CometCachedBatch")),
              "the payload Kryo serialized must be Comet's cached batch format")

            // Read the payload back rather than only the row count, so a Kryo round trip that
            // silently mangles the Arrow bytes fails too. The predicate also exercises the
            // statistics row, which is what carries UTF8String and Decimal through Kryo.
            checkSparkAnswer(
              spark.sql("SELECT c_long, c_string, c_dec_long, c_ts FROM kryo_cache " +
                "WHERE c_dec_short >= 100 AND c_string > '1'"))
          } finally {
            spark.catalog.clearCache()
          }
        }
      }
    }

  test("Comet broadcast exchange survives Kryo with registration required") {
    // Not about the cache: CometBroadcastExchangeExec broadcasts an Array[ChunkedByteBuffer], and
    // Spark registers ChunkedByteBuffer but not an array of them, so this fails on main today
    // under registrationRequired=true. The registrator this suite installs covers it because the
    // cache write path hands back the same type. Kept here rather than split out because that
    // registration is the thing under test.
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "false",
      CometConf.COMET_SHUFFLE_MODE.key -> "jvm",
      CometConf.COMET_EXEC_BROADCAST_EXCHANGE_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10MB") {
      withParquetTable((0 until 100).map(i => (i, i.toString)), "kryo_bcast_a") {
        withParquetTable((0 until 10).map(i => (i, i.toString)), "kryo_bcast_b") {
          val df = spark.sql(
            "SELECT /*+ BROADCAST(b) */ a._1, b._2 " +
              "FROM kryo_bcast_a a JOIN kryo_bcast_b b ON a._1 = b._1")
          assert(
            df.queryExecution.executedPlan.toString().contains("CometBroadcastExchange"),
            "the broadcast has to run through Comet for this to test anything")
          checkSparkAnswer(df)
        }
      }
    }
  }

  test("Comet in-memory cache falls back to Spark's format under Kryo for unsupported types") {
    // A relation Comet cannot store is delegated to DefaultCachedBatch, which Spark registers
    // itself. Pins that the fallback path is not collateral damage of the registration work.
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.key -> "true") {

      spark.catalog.clearCache()
      try {
        spark
          .range(0, 100, 1, 2)
          .selectExpr("id", "make_interval(0, 0, 0, 0, 0, 0, id) AS iv")
          .createOrReplaceTempView("kryo_cache_fallback")

        spark.catalog.cacheTable("kryo_cache_fallback", StorageLevel.DISK_ONLY)
        assert(spark.table("kryo_cache_fallback").count() == 100)
        assert(
          cachedBatchTypes("kryo_cache_fallback").sameElements(
            Array("org.apache.spark.sql.execution.columnar.DefaultCachedBatch")))

        checkSparkAnswer(spark.sql("SELECT id FROM kryo_cache_fallback WHERE id > 90"))
      } finally {
        spark.catalog.clearCache()
      }
    }
  }
}
