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

package org.apache.comet.local

import java.util.concurrent.{Callable, Executors, TimeUnit}

import scala.jdk.CollectionConverters._

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Outcome}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkException
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.exchange.Exchange

import org.apache.comet.{CometArrowImportAllocator, CometConf, CometSparkSessionExtensions, NativeBase}
import org.apache.comet.local.shims.LocalModeSupport
import org.apache.comet.vector.NativeUtil

class CometLocalExecutionSuite
    extends AnyFunSuite
    with BeforeAndAfterAll
    with BeforeAndAfterEach {
  private var spark: SparkSession = _
  private val enabled = CometConf.COMET_EXEC_LOCAL_ENABLED.key

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    if (LocalModeSupport.supported) {
      spark = SparkSession
        .builder()
        .master("local[1]")
        .appName("CometLocalExecutionSuite")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", classOf[CometSparkSessionExtensions].getName)
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.comet.enabled", "true")
        .config("spark.comet.exec.enabled", "true")
        .config("spark.comet.exec.onHeap.enabled", "true")
        .config("spark.comet.shuffle.enabled", "false")
        .config("spark.comet.batchSize", "17")
        .config(enabled, "true")
        .getOrCreate()
      spark.sparkContext.setLogLevel("WARN")
      assert(NativeBase.isLoaded)
    }
  }

  override protected def withFixture(test: NoArgTest): Outcome = {
    if (!LocalModeSupport.supported)
      cancel("Local bridge is currently enabled only for Spark 4.1")
    super.withFixture(test)
  }

  override protected def afterEach(): Unit = {
    try {
      if (spark != null) {
        assert(new NativeLocal().activeQueries() == 0)
        assert(CometArrowImportAllocator.getAllocatedMemory == 0)
        assert(LocalResultHandoff.active == 0)
      }
    } finally super.afterEach()
  }

  override protected def afterAll(): Unit = {
    try {
      if (spark != null) spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    } finally super.afterAll()
  }

  private def withConf[T](key: String, value: String)(f: => T): T = {
    val previous = spark.conf.getOption(key)
    spark.conf.set(key, value)
    try f
    finally
      previous match {
        case Some(v) => spark.conf.set(key, v)
        case None => spark.conf.unset(key)
      }
  }

  private def localNodes(df: DataFrame): Seq[CometLocalExec] =
    df.queryExecution.executedPlan.collect { case p: CometLocalExec => p }

  test("opt-in is required and range projection executes natively on local[1]") {
    assert(!CometConf.COMET_EXEC_LOCAL_ENABLED.defaultValue.get)
    withConf(enabled, "false") {
      assert(localNodes(spark.range(100).toDF()).isEmpty)
    }
    val query = spark.range(0, 4099, 1, 7).selectExpr("id AS x", "id AS y")
    assert(localNodes(query).size == 1)
    assert(query.queryExecution.executedPlan.collect { case p: Exchange => p }.isEmpty)
    assert(localNodes(query).head.outputOrdering.nonEmpty)
    assert(query.queryExecution.executedPlan.execute().getNumPartitions == 1)
    assert(
      query.collect().sortBy(_.getLong(0)).toSeq ==
        (0L until 4099L).map(i => Row(i, i)))
  }

  test("native range preserves ordering after Spark removes a redundant sort") {
    val query = spark.range(0, 300, 1, 7).orderBy("id").toDF()
    assert(localNodes(query).size == 1)
    assert(query.collect().map(_.getLong(0)).toSeq == (0L until 300L))
    assert(spark.range(0, 300, 1, 7).take(3).toSeq == Seq(0L, 1L, 2L))
    assert(spark.range(30, -10, -3, 7).collect().toSeq == (30L until -10L by -3L))
  }

  test("repeated actions build fresh executions without retaining query handles") {
    val query = spark.range(0, 300, 3, 7).selectExpr("id AS value")
    val expected = (0L until 300L by 3L).map(Row(_))
    for (_ <- 0 until 3) {
      assert(query.collect().sortBy(_.getLong(0)).toSeq == expected)
      assert(new NativeLocal().activeQueries() == 0)
    }
  }

  test("take and partition iterator consumption close the native execution early") {
    val query = spark.range(0, Long.MaxValue, 1, 7).toDF()
    assert(localNodes(query).size == 1)
    assert(query.take(3).length == 3)
    assert(
      query.queryExecution.executedPlan
        .execute()
        .mapPartitions(rows => rows.take(2).map(_.copy()))
        .collect()
        .length == 2)
  }

  test("collect and take hand local results to the driver within a Spark job") {
    withParquetData { path =>
      val query = spark.read.parquet(path).filter("id % 3 = 0").select("id", "text")
      val plan = query.queryExecution.executedPlan
      assert(plan.isInstanceOf[CometLocalResultExec], plan.toString)
      assert(localNodes(query).size == 1)
      val expected = withConf("spark.comet.enabled", "false")(query.collect().toSeq)
      assert(query.collect().toSeq.sortBy(_.getLong(0)) == expected.sortBy(_.getLong(0)))
      val taken = query.take(4)
      assert(taken.length == 4 && taken.forall(r => expected.contains(r)))
      assert(query.head(0).isEmpty)
      val ordered = spark.read.parquet(path).orderBy(org.apache.spark.sql.functions.desc("id"))
      assert(ordered.queryExecution.executedPlan.isInstanceOf[CometLocalResultExec])
      assert(ordered.collect().map(_.getLong(0)).toSeq == (199L to 0L by -1L))
      assert(ordered.take(3).map(_.getLong(0)).toSeq == Seq(199L, 198L, 197L))
    }
  }

  test("job group cancellation interrupts a local collect") {
    val query = spark.range(0, Long.MaxValue, 1, 7).toDF()
    assert(query.queryExecution.executedPlan.isInstanceOf[CometLocalResultExec])
    val group = "comet-local-collect-cancel"
    val pool = Executors.newSingleThreadExecutor()
    try {
      val future = pool.submit(new Callable[Int] {
        override def call(): Int = {
          spark.sparkContext.setJobGroup(group, "cancel local collect", interruptOnCancel = true)
          try query.collect().length
          finally spark.sparkContext.clearJobGroup()
        }
      })
      val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20)
      while (new NativeLocal().activeQueries() == 0 && System.nanoTime() < deadline) {
        Thread.sleep(10)
      }
      assert(new NativeLocal().activeQueries() == 1)
      spark.sparkContext.cancelJobGroup(group)
      intercept[java.util.concurrent.ExecutionException] { future.get(20, TimeUnit.SECONDS) }
      while (new NativeLocal().activeQueries() != 0 && System.nanoTime() < deadline) {
        Thread.sleep(10)
      }
    } finally pool.shutdownNow()
  }

  test("local result size limit stops native production and fails the action") {
    val query = spark.range(0, Long.MaxValue, 1, 7).selectExpr("id", "id AS other")
    val plan = query.queryExecution.executedPlan
    assert(plan.isInstanceOf[CometLocalResultExec], plan.toString)
    // Each two-long UnsafeRow is 24 bytes, so 1,000 bytes fit 41 rows and the 42nd exceeds it.
    val failure = intercept[SparkException] {
      LocalResultHandoff.collect(plan.execute(), -1, 1000)
    }
    assert(failure.getMessage.contains("spark.driver.maxResultSize"), failure.getMessage)
    assert(failure.getMessage.contains("at least 1008 bytes"), failure.getMessage)
    assert(new NativeLocal().activeQueries() == 0)
    // A take within the limit still succeeds against the same bound.
    assert(LocalResultHandoff.collect(plan.execute(), 41, 1000).length == 41)
    // Non-positive limits mean unlimited, as in Spark.
    assert(LocalResultHandoff.collect(plan.execute(), 100, 0).length == 100)
  }

  test("empty ranges, descending ranges and iterator results") {
    assert(spark.range(5, 5, 1, 7).collect().isEmpty)
    val query = spark.range(30, -10, -3, 7).toDF()
    assert(localNodes(query).size == 1)
    assert(
      query.toLocalIterator().asScala.toSeq.map(_.getLong(0)).sorted ==
        (30L until -10L by -3L).sorted)
  }

  test("unsupported expression and query fall back as a whole") {
    for (query <- Seq(
        spark.range(10).selectExpr("id + 1 AS x"),
        spark.range(10).filter("id > 3").toDF(),
        spark.range(10).selectExpr("spark_partition_id()"),
        spark.range(10).selectExpr("sum(id)"))) {
      assert(localNodes(query).isEmpty)
      query.collect()
    }
  }

  test("AQE and execution disablement leave the query on the existing path") {
    withConf("spark.sql.adaptive.enabled", "true") {
      assert(localNodes(spark.range(10).toDF()).isEmpty)
    }
    withConf("spark.comet.exec.enabled", "false") {
      assert(localNodes(spark.range(10).toDF()).isEmpty)
    }
  }

  test("non-local environments and subqueries are not admitted") {
    assert(
      LocalModeSupport
        .environmentRejection(isLocal = false, adaptiveEnabled = false)
        .exists(_.contains("in-process")))
    val query = spark.sql("SELECT (SELECT id FROM range(1)) AS value")
    assert(query.queryExecution.executedPlan.collectWithSubqueries { case local: CometLocalExec =>
      local
    }.isEmpty)
    assert(query.collect().toSeq == Seq(Row(0L)))
  }

  test("admission reads the actual context rather than a session master override") {
    withConf("spark.master", "spark://unreachable.invalid:7077") {
      val query = spark.range(10).toDF()
      assert(spark.sparkContext.isLocal)
      assert(localNodes(query).size == 1)
      assert(query.collect().length == 10)
    }
  }

  test("admission freezes batch size in the prepared plan") {
    val query = spark.range(100).toDF()
    val node = localNodes(query).head
    assert(node.spec.batchSize == 17)
    withConf("spark.comet.batchSize", "3") {
      assert(node.spec.batchSize == 17)
      assert(query.collect().length == 100)
    }
  }

  test("native invalid inputs and output pointers fail without leaking handles") {
    val native = new NativeLocal
    intercept[Exception] { native.createRange(0, 10, 0, 1, 17, 1) }
    val id = native.createRange(0, 10, 1, 1, 17, 1)
    intercept[Exception] { native.nextBatch(id, Array(0L), Array(0L)) }
    native.close(id)
    native.close(id)
    assert(native.activeQueries() == 0)
  }

  test("imported Arrow batches outlive native close") {
    val native = new NativeLocal
    val util = new NativeUtil
    val id = native.createRange(10, 20, 1, 1, 17, 1)
    try {
      val batch = util
        .getNextBatch(
          1,
          (arrays, schemas) => {
            var rows = -2L
            while (rows == -2L) rows = native.nextBatch(id, arrays, schemas)
            rows
          })
        .get
      try {
        native.close(id)
        assert(batch.numRows() == 10)
        assert(batch.column(0).getLong(0) == 10)
        assert(batch.column(0).getLong(9) == 19)
      } finally batch.close()
    } finally {
      native.close(id)
      util.close()
    }
  }

  test("Spark job cancellation releases a live local execution") {
    val executor = Executors.newSingleThreadExecutor()
    val query = spark.range(0, Long.MaxValue, 1, 7).toDF()
    assert(localNodes(query).size == 1)
    val future = executor.submit(new Callable[Unit] {
      override def call(): Unit = {
        spark.sparkContext.setJobGroup("comet-local-cancel", "local cancellation", true)
        query.queryExecution.executedPlan.execute().foreachPartition { rows =>
          while (rows.hasNext) rows.next()
        }
      }
    })
    try {
      val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20)
      while (new NativeLocal().activeQueries() == 0 && !future.isDone &&
        System.nanoTime() < deadline) Thread.sleep(10)
      assert(new NativeLocal().activeQueries() > 0, "native execution did not start")
      spark.sparkContext.cancelJobGroup("comet-local-cancel")
      intercept[java.util.concurrent.ExecutionException] { future.get(20, TimeUnit.SECONDS) }
      while (new NativeLocal().activeQueries() != 0 && System.nanoTime() < deadline)
        Thread.sleep(10)
      assert(new NativeLocal().activeQueries() == 0)
    } finally {
      spark.sparkContext.cancelJobGroup("comet-local-cancel")
      executor.shutdownNow()
    }
  }

  private def withParquetData(f: String => Unit): Unit = {
    val directory = java.nio.file.Files.createTempDirectory("comet-local-parquet").toFile
    val path = new java.io.File(directory, "data").getAbsolutePath
    try {
      withConf("spark.comet.enabled", "false") {
        spark
          .range(0, 200, 1, 5)
          .selectExpr(
            "id",
            "CAST(id % 7 AS INT) AS k",
            "CASE WHEN id % 5 = 0 THEN NULL ELSE CAST(id AS STRING) END AS text",
            "CAST(id / 10 AS DECIMAL(12, 2)) AS amount",
            "date_add(DATE '2020-01-01', CAST(id AS INT)) AS day",
            "TIMESTAMP '2020-01-01 00:00:00' + id * INTERVAL 1 SECOND AS ts")
          .write
          .parquet(path)
      }
      withConf("spark.sql.files.maxPartitionBytes", "4096") {
        withConf("spark.sql.files.openCostInBytes", "4096") { f(path) }
      }
    } finally org.apache.commons.io.FileUtils.deleteDirectory(directory)
  }

  private def compareParquet(path: String)(query: DataFrame => DataFrame): DataFrame = {
    val expected = withConf("spark.comet.enabled", "false") {
      query(spark.read.parquet(path)).collect().toSeq.groupBy(identity).map {
        case (row, copies) => row -> copies.size
      }
    }
    val actual = query(spark.read.parquet(path))
    assert(localNodes(actual).size == 1, actual.queryExecution.executedPlan.toString)
    assert(actual.queryExecution.executedPlan.collect { case p: Exchange => p }.isEmpty)
    assert(actual.collect().toSeq.groupBy(identity).map { case (row, copies) =>
      row -> copies.size
    } == expected)
    actual
  }

  test("local Parquet reads every file with shared filter and projection") {
    withParquetData { path =>
      val query = compareParquet(path) { df =>
        df.filter("id >= 12 AND id < 153 AND text IS NOT NULL")
          .selectExpr(
            "id + 3 AS value",
            "amount * CAST(2 AS DECIMAL(2, 0)) AS amount",
            "text",
            "day",
            "ts")
      }
      assert(localNodes(query).head.spec.asInstanceOf[LocalParquetSpec].filePartitions.length > 1)
      assert(localNodes(query).head.outputOrdering.isEmpty)
      assert(query.queryExecution.executedPlan.execute().getNumPartitions == 1)
      assert(query.take(2).length == 2)
      query.collect()
    }
  }

  test("local Parquet pushdown on and off preserve null and decimal predicates") {
    withParquetData { path =>
      for (pushdown <- Seq("true", "false"); rowFilter <- Seq("true", "false")) {
        withConf("spark.sql.parquet.filterPushdown", pushdown) {
          withConf(CometConf.COMET_PARQUET_ROW_FILTER_PUSHDOWN_ENABLED.key, rowFilter) {
            compareParquet(path)(
              _.filter("(text IS NULL OR k >= 4) AND amount > 3.20")
                .selectExpr("id", "amount", "CAST(ts AS STRING) AS time"))
          }
        }
      }
    }
  }

  test("local Parquet empty results and missing nullable columns") {
    withParquetData { path =>
      compareParquet(path)(_.filter("id < 0").selectExpr("id", "text"))
      val schema =
        spark.read.parquet(path).schema.add("missing", org.apache.spark.sql.types.LongType)
      val query = spark.read.schema(schema).parquet(path).selectExpr("id", "missing")
      assert(localNodes(query).nonEmpty)
      assert(query.collect().forall(_.isNullAt(1)))
      val emptyDir = java.nio.file.Files.createTempDirectory("comet-local-empty").toFile
      try {
        val empty = spark.read.schema(schema).parquet(emptyDir.getAbsolutePath)
        assert(localNodes(empty).nonEmpty)
        assert(empty.collect().isEmpty)
      } finally org.apache.commons.io.FileUtils.deleteDirectory(emptyDir)
    }
  }

  test("local Parquet rejects callback expressions and whole unsupported queries") {
    withParquetData { path =>
      val plusOne = org.apache.spark.sql.functions.udf((value: Long) => value + 1)
      val input = spark.read.parquet(path)
      for (query <- Seq(
          input.selectExpr("input_file_name()"),
          input.selectExpr("spark_partition_id()"),
          input.select(plusOne(input("id"))),
          input.selectExpr("sum(id)"),
          input.selectExpr("_metadata.file_path"),
          input.selectExpr("array(id)"))) {
        assert(localNodes(query).isEmpty, query.queryExecution.executedPlan.toString)
        query.collect()
      }
    }
  }

  test("local Parquet freezes timezone and ANSI settings per query") {
    withParquetData { path =>
      val utc = spark.newSession()
      val pacific = spark.newSession()
      utc.conf.set("spark.sql.session.timeZone", "UTC")
      pacific.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
      val first = utc.read.parquet(path).selectExpr("CAST(ts AS STRING) AS time")
      val second = pacific.read.parquet(path).selectExpr("CAST(ts AS STRING) AS time")
      assert(localNodes(first).nonEmpty)
      assert(localNodes(second).nonEmpty)
      utc.conf.set("spark.sql.session.timeZone", "Asia/Tokyo")
      pacific.conf.set("spark.sql.session.timeZone", "Asia/Tokyo")
      assert(first.collect().map(_.getString(0)).min == "2020-01-01 00:00:00")
      assert(second.collect().map(_.getString(0)).min == "2019-12-31 16:00:00")
      for (ansi <- Seq("false", "true")) {
        withConf("spark.sql.ansi.enabled", ansi) {
          val query = spark.read.parquet(path).selectExpr("CAST(id + 100 AS TINYINT)")
          assert(localNodes(query).nonEmpty)
          if (ansi == "true") {
            val failure = intercept[Exception] { query.collect() }
            assert(
              Iterator
                .iterate[Throwable](failure)(_.getCause)
                .takeWhile(_ != null)
                .exists(e => Option(e.getMessage).exists(_.contains("[CAST_OVERFLOW]"))))
          } else compareParquet(path)(_.selectExpr("CAST(id + 100 AS TINYINT)"))
        }
      }
    }
  }

  test("local Parquet read errors close the native query") {
    withParquetData { path =>
      val query = spark.read.parquet(path).selectExpr("id + 1")
      assert(localNodes(query).nonEmpty)
      org.apache.commons.io.FileUtils.deleteDirectory(new java.io.File(path))
      intercept[Exception] { query.collect() }
      assert(new NativeLocal().activeQueries() == 0)
    }
  }

  test("local Parquet preserves static partition pruning and partition column values") {
    withParquetData { path =>
      val directory = java.nio.file.Files.createTempDirectory("comet-local-partitioned").toFile
      val target = new java.io.File(directory, "data").getAbsolutePath
      try {
        withConf("spark.comet.enabled", "false") {
          spark.read.parquet(path).write.partitionBy("k").parquet(target)
        }
        compareParquet(target)(_.filter("k = 3 AND id > 20").selectExpr("id", "k", "amount"))
      } finally org.apache.commons.io.FileUtils.deleteDirectory(directory)
    }
  }

  test("simultaneous native Parquet graphs retain configuration and independent lifetime") {
    withParquetData { path =>
      val utc = spark.newSession()
      val pacific = spark.newSession()
      utc.conf.set("spark.sql.session.timeZone", "UTC")
      pacific.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
      val specs = Seq(utc, pacific).map { session =>
        localNodes(session.read.parquet(path).selectExpr("CAST(ts AS STRING)")).head.spec
          .asInstanceOf[LocalParquetSpec]
      }
      val native = new NativeLocal
      val ids = scala.collection.mutable.ArrayBuffer.empty[Long]
      val util = new NativeUtil
      try {
        specs.foreach { spec =>
          ids += native.createParquet(
            spec.plan,
            spec.filePartitions,
            spec.batchSize,
            spec.columns,
            spec.rowFilterPushdown,
            spec.aggregate,
            spec.memoryLimit,
            spec.spillEnabled)
        }
        assert(native.activeQueries() == 2)
        native.close(ids.head)
        val values = scala.collection.mutable.ArrayBuffer.empty[String]
        var finished = false
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20)
        while (!finished) {
          val batch = util.getNextBatch(
            1,
            (arrays, schemas) => {
              var rows = -2L
              while (rows == -2L) {
                assert(System.nanoTime() < deadline, "local scan failed to make progress")
                rows = native.nextBatch(ids(1), arrays, schemas)
              }
              rows
            })
          batch match {
            case Some(data) =>
              try {
                (0 until data.numRows()).foreach { row =>
                  values += data.column(0).getUTF8String(row).toString
                }
              } finally data.close()
            case None => finished = true
          }
        }
        assert(values.length == 200)
        assert(values.min == "2019-12-31 16:00:00")
      } finally {
        ids.foreach(native.close)
        util.close()
      }
    }
  }

  test("local Parquet reads split row groups exactly once") {
    val directory = java.nio.file.Files.createTempDirectory("comet-local-splits").toFile
    val path = new java.io.File(directory, "data").getAbsolutePath
    try {
      withConf("spark.comet.enabled", "false") {
        spark
          .range(0, 10000, 1, 1)
          .selectExpr("id", "CAST(id AS STRING) AS text")
          .write
          .option("parquet.block.size", "4096")
          .parquet(path)
      }
      withConf("spark.sql.files.maxPartitionBytes", "4096") {
        val query = compareParquet(path)(_.filter("id % 13 = 0").selectExpr("id", "text"))
        assert(
          localNodes(query).head.spec.asInstanceOf[LocalParquetSpec].filePartitions.length > 1)
      }
    } finally org.apache.commons.io.FileUtils.deleteDirectory(directory)
  }

  test("local grouped aggregation replaces Spark shuffle with shared native exchange") {
    withParquetData { path =>
      withConf("spark.sql.shuffle.partitions", "7") {
        val query = compareParquet(path)(
          _.groupBy("k")
            .agg(
              org.apache.spark.sql.functions.count("text").as("n"),
              org.apache.spark.sql.functions.min("amount").as("lo"),
              org.apache.spark.sql.functions.max("id").as("hi")))
        val spec = localNodes(query).head.spec.asInstanceOf[LocalParquetSpec]
        val aggregate =
          org.apache.comet.serde.LocalOuterClass.LocalAggregate.parseFrom(spec.aggregate)
        assert(aggregate.getPartitions == 7)
        assert(spec.filePartitions.length > 1)
        assert(query.collect().length == 7)
        assert(query.take(1).length == 1)
      }
    }
  }

  test("local global aggregation and empty input retain Spark null and count semantics") {
    withParquetData { path =>
      compareParquet(path)(_.selectExpr("count(*) AS n"))
      compareParquet(path)(_.selectExpr("count(text) + 1 AS n", "min(day)", "max(ts)"))
      compareParquet(path)(_.filter("id < 0").selectExpr("count(*)", "min(amount)", "max(id)"))
      compareParquet(path)(_.filter("id < 0").groupBy("k").count())
    }
  }

  test("local aggregation handles null keys skew duplicate keys and aggregate filters") {
    withParquetData { path =>
      withConf("spark.sql.shuffle.partitions", "7") {
        compareParquet(path) { df =>
          df.selectExpr("CASE WHEN id % 3 = 0 THEN NULL ELSE 1 END AS key", "id", "text")
            .groupBy("key")
            .agg(
              org.apache.spark.sql.functions.expr("count(text) FILTER (WHERE id > 50)"),
              org.apache.spark.sql.functions.min("id"))
        }
      }
    }
  }

  test("local aggregate admission rejects distinct sums float keys and nested exchanges") {
    withParquetData { path =>
      val input = spark.read.parquet(path)
      for (query <- Seq(
          input.selectExpr("count(DISTINCT k)"),
          input.selectExpr("sum(id)"),
          input.selectExpr("CAST(k AS DOUBLE) AS key").groupBy("key").count(),
          input.repartition(3).groupBy("k").count())) {
        assert(localNodes(query).isEmpty, query.queryExecution.executedPlan.toString)
        query.collect()
      }
    }
  }

  test("local aggregate reservation failure closes the whole query without fallback") {
    withParquetData { path =>
      withConf(CometConf.COMET_EXEC_LOCAL_MEMORY_LIMIT.key, "1b") {
        withConf(CometConf.COMET_EXEC_LOCAL_SPILL_ENABLED.key, "false") {
          val query = spark.read.parquet(path).groupBy("id").count()
          assert(localNodes(query).nonEmpty)
          val spec = localNodes(query).head.spec.asInstanceOf[LocalParquetSpec]
          assert(spec.memoryLimit == 1L && !spec.spillEnabled)
          val failure = intercept[Exception] { query.collect() }
          assert(
            Iterator
              .iterate[Throwable](failure)(_.getCause)
              .takeWhile(_ != null)
              .exists(e =>
                Option(e.getMessage).exists(m =>
                  m.contains("memory") || m.contains("Memory") || m.contains(
                    "Resources exhausted"))))
          assert(new NativeLocal().activeQueries() == 0)
        }
      }
      compareParquet(path)(_.groupBy("k").count())
    }
  }

  test("local admission respects native operator disablement") {
    withParquetData { path =>
      withConf(CometConf.COMET_EXEC_AGGREGATE_ENABLED.key, "false") {
        assert(localNodes(spark.read.parquet(path).groupBy("k").count()).isEmpty)
      }
      withConf(CometConf.COMET_EXEC_FILTER_ENABLED.key, "false") {
        assert(localNodes(spark.read.parquet(path).filter("id > 10")).isEmpty)
      }
      withConf(CometConf.COMET_EXEC_PROJECT_ENABLED.key, "false") {
        assert(localNodes(spark.read.parquet(path).selectExpr("id + 1")).isEmpty)
      }
    }
  }

  private def joinQuery(
      input: DataFrame,
      kind: String,
      buildRight: Boolean,
      emptyLeft: Boolean = false,
      emptyRight: Boolean = false): DataFrame = {
    val left = input
      .filter(if (emptyLeft) "id < 0" else "id < 80")
      .selectExpr("id AS l", "CASE WHEN id % 3 = 0 THEN NULL ELSE k END AS lk", "amount")
      .alias("a")
    val right = input
      .filter(if (emptyRight) "id < 0" else "id >= 60 AND id < 150")
      .selectExpr("id AS r", "CASE WHEN id % 5 = 0 THEN NULL ELSE k END AS rk", "text")
      .alias("b")
    val l = if (buildRight) left else left.hint("SHUFFLE_HASH")
    val r = if (buildRight) right.hint("SHUFFLE_HASH") else right
    l.join(r, l("lk") === r("rk"), kind)
  }

  test(
    "local partitioned joins preserve duplicate null and unmatched rows for both build sides") {
    withParquetData { path =>
      withConf("spark.sql.autoBroadcastJoinThreshold", "-1") {
        withConf("spark.sql.shuffle.partitions", "7") {
          for (kind <- Seq("inner", "left_outer", "right_outer", "full_outer");
            buildRight <- Seq(false, true)) {
            val query = compareParquet(path)(joinQuery(_, kind, buildRight))
            val spec = localNodes(query).head.spec.asInstanceOf[LocalJoinSpec]
            val proto = org.apache.comet.serde.LocalOuterClass.LocalJoin.parseFrom(spec.plan)
            assert(proto.getPartitions == 7)
            assert(proto.getBuildRight == buildRight)
            assert(proto.getLeftFilesCount > 1 && proto.getRightFilesCount > 1)
            assert(query.take(1).length == 1)
          }
        }
      }
    }
  }

  test("local semi and anti joins preserve left output and empty input semantics") {
    withParquetData { path =>
      withConf("spark.sql.autoBroadcastJoinThreshold", "-1") {
        withConf("spark.sql.shuffle.partitions", "3") {
          for (kind <- Seq(
              "left_semi",
              "left_anti",
              "inner",
              "left_outer",
              "right_outer",
              "full_outer");
            empty <- Seq((false, true), (true, false), (true, true))) {
            compareParquet(path)(joinQuery(_, kind, true, empty._1, empty._2))
          }
          for (kind <- Seq("left_semi", "left_anti")) {
            compareParquet(path)(joinQuery(_, kind, true))
          }
        }
      }
    }
  }

  test("local join supports composite keys result projection and repeated execution") {
    withParquetData { path =>
      withConf("spark.sql.shuffle.partitions", "7") {
        val query = compareParquet(path) { input =>
          val left = input.filter("id < 100").alias("a")
          val right = input.filter("id >= 50").alias("b").hint("SHUFFLE_HASH")
          left
            .join(right, left("k") === right("k") && left("amount") === right("amount"))
            .selectExpr("a.id + b.id AS total", "a.amount", "b.text")
        }
        def rows = query.collect().toSeq.groupBy(identity).map { case (row, copies) =>
          row -> copies.size
        }
        val first = rows
        assert(first.nonEmpty)
        assert(rows == first)
      }
    }
  }

  test("local join rejects broadcast residual conditions float keys and nested joins") {
    withParquetData { path =>
      val input = spark.read.parquet(path)
      val left = input.filter("id < 80").alias("a")
      val right = input.filter("id >= 60").alias("b")
      val hash = right.hint("SHUFFLE_HASH")
      val floats = input.selectExpr("CAST(k AS DOUBLE) AS key", "id").alias("f")
      val queries = Seq(
        left.join(right.hint("BROADCAST"), org.apache.spark.sql.functions.expr("a.k = b.k")),
        left.join(hash, org.apache.spark.sql.functions.expr("a.k = b.k AND a.id < b.id")),
        floats.join(floats.alias("g").hint("SHUFFLE_HASH"), Seq("key")),
        joinQuery(input, "inner", true)
          .join(right.hint("SHUFFLE_HASH"), org.apache.spark.sql.functions.expr("l = b.id")))
      queries.foreach { query =>
        assert(localNodes(query).isEmpty, query.queryExecution.executedPlan.toString)
        query.collect()
      }
      withConf(CometConf.COMET_EXEC_HASH_JOIN_ENABLED.key, "false") {
        assert(localNodes(joinQuery(input, "inner", true)).isEmpty)
      }
    }
  }

  test("local join reservation failure releases both inputs and permits a subsequent query") {
    withParquetData { path =>
      withConf("spark.sql.shuffle.partitions", "7") {
        withConf(CometConf.COMET_EXEC_LOCAL_MEMORY_LIMIT.key, "1b") {
          withConf(CometConf.COMET_EXEC_LOCAL_SPILL_ENABLED.key, "false") {
            val query = joinQuery(spark.read.parquet(path), "inner", true)
            assert(localNodes(query).nonEmpty)
            intercept[Exception] { query.collect() }
            assert(new NativeLocal().activeQueries() == 0)
          }
        }
        compareParquet(path)(joinQuery(_, "inner", true))
      }
    }
  }

  private def compareOrdered(path: String)(query: DataFrame => DataFrame): DataFrame = {
    val expected = withConf("spark.comet.enabled", "false") {
      val baseline = query(spark.read.parquet(path))
      (baseline.collect().toSeq, baseline.queryExecution.executedPlan.outputOrdering.nonEmpty)
    }
    val actual = query(spark.read.parquet(path))
    assert(localNodes(actual).size == 1, actual.queryExecution.executedPlan.toString)
    assert(actual.queryExecution.executedPlan.collect { case p: Exchange => p }.isEmpty)
    assert(localNodes(actual).head.outputOrdering.nonEmpty == expected._2)
    assert(actual.collect().toSeq == expected._1)
    actual
  }

  test("local global sort preserves direction null placement and multiple keys") {
    withParquetData { path =>
      withConf("spark.sql.shuffle.partitions", "7") {
        for (direction <- Seq("ASC", "DESC"); nulls <- Seq("FIRST", "LAST")) {
          compareOrdered(path) { input =>
            input
              .selectExpr("id", "text", "amount")
              .orderBy(
                ((direction, nulls) match {
                  case ("ASC", "FIRST") =>
                    org.apache.spark.sql.functions.col("text").asc_nulls_first
                  case ("ASC", _) => org.apache.spark.sql.functions.col("text").asc_nulls_last
                  case (_, "FIRST") => org.apache.spark.sql.functions.col("text").desc_nulls_first
                  case _ => org.apache.spark.sql.functions.col("text").desc_nulls_last
                }),
                org.apache.spark.sql.functions.col("id").desc)
          }
        }
        compareOrdered(path)(_.orderBy("amount", "day", "ts", "id"))
      }
    }
  }

  test("local Top-K handles offset projection and repeated actions") {
    withParquetData { path =>
      val query = compareOrdered(path) { input =>
        input
          .orderBy(org.apache.spark.sql.functions.col("id").desc)
          .offset(9)
          .limit(13)
          .selectExpr("id + 1 AS value", "text")
      }
      assert(query.collect().map(_.getLong(0)).toSeq == (191L to 179L by -1L))
      assert(query.take(3).length == 3)
    }
  }

  test("local global sort and limit apply after aggregate and partitioned join") {
    withParquetData { path =>
      withConf("spark.sql.shuffle.partitions", "7") {
        compareOrdered(path)(_.groupBy("k").count().orderBy("count", "k").limit(3))
        compareOrdered(path)(input =>
          joinQuery(input, "inner", true)
            .orderBy("l", "r")
            .limit(23))
      }
    }
  }

  test("local full sort limit offset handles empty input and offset beyond the end") {
    withParquetData { path =>
      withConf("spark.sql.execution.topKSortFallbackThreshold", "0") {
        compareOrdered(path)(_.orderBy("id").offset(195).limit(20))
        compareOrdered(path)(_.orderBy("id").offset(250).limit(20))
        compareOrdered(path)(_.orderBy("id").offset(195))
        compareOrdered(path)(_.filter("id < 0").orderBy("id").limit(10))
      }
    }
  }

  test("local unordered limit is global and closes native production") {
    withParquetData { path =>
      val query = spark.read.parquet(path).select("id").offset(11).limit(19)
      assert(localNodes(query).size == 1)
      assert(!query.queryExecution.executedPlan.toString.contains("CollectLimit"))
      val rows = query.collect().map(_.getLong(0))
      assert(rows.length == 19 && rows.distinct.length == 19)
      assert(rows.forall(id => id >= 0 && id < 200))
      assert(new NativeLocal().activeQueries() == 0)
    }
  }

  test("local sort rejects per-partition sorting float keys and disabled operators") {
    withParquetData { path =>
      val input = spark.read.parquet(path)
      assert(localNodes(input.sortWithinPartitions("id")).isEmpty)
      assert(
        localNodes(input.orderBy(org.apache.spark.sql.functions.col("k").cast("double"))).isEmpty)
      withConf(CometConf.COMET_EXEC_SORT_ENABLED.key, "false") {
        assert(localNodes(input.orderBy("id")).isEmpty)
      }
      withConf(CometConf.COMET_EXEC_TAKE_ORDERED_AND_PROJECT_ENABLED.key, "false") {
        assert(localNodes(input.orderBy("id").limit(5)).isEmpty)
      }
    }
  }

  test("local sort reservation failure cleans up and permits another query") {
    withParquetData { path =>
      withConf(CometConf.COMET_EXEC_LOCAL_MEMORY_LIMIT.key, "1b") {
        withConf(CometConf.COMET_EXEC_LOCAL_SPILL_ENABLED.key, "false") {
          val query = spark.read.parquet(path).orderBy("text", "id")
          assert(localNodes(query).size == 1)
          intercept[Exception] { query.collect() }
          assert(new NativeLocal().activeQueries() == 0)
        }
      }
      compareOrdered(path)(_.orderBy("id"))
    }
  }

  private def startNative(native: NativeLocal, spec: LocalQuerySpec): Long = spec match {
    case scan: LocalParquetSpec =>
      native.createParquet(
        scan.plan,
        scan.filePartitions,
        scan.batchSize,
        scan.columns,
        scan.rowFilterPushdown,
        scan.aggregate,
        scan.memoryLimit,
        scan.spillEnabled,
        scan.terminal)
    case join: LocalJoinSpec =>
      native.createJoin(
        join.plan,
        join.batchSize,
        join.columns,
        join.rowFilterPushdown,
        join.memoryLimit,
        join.spillEnabled,
        join.terminal)
    case range: LocalRangeSpec =>
      native.createRange(
        range.start,
        range.end,
        range.step,
        range.partitions,
        range.batchSize,
        range.columns)
  }

  private def readNative(
      native: NativeLocal,
      util: NativeUtil,
      id: Long,
      columns: Int): Option[org.apache.spark.sql.vectorized.ColumnarBatch] = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20)
    util.getNextBatch(
      columns,
      (arrays, schemas) => {
        var rows = -2L
        while (rows == -2L) {
          assert(System.nanoTime() < deadline, "native query did not make progress")
          rows = native.nextBatch(id, arrays, schemas)
        }
        rows
      })
  }

  test("concurrent native join sort and aggregate isolate cancellation and reservation failure") {
    withParquetData { path =>
      withConf("spark.sql.shuffle.partitions", "7") {
        val input = spark.read.parquet(path)
        val healthy = joinQuery(input, "inner", true)
          .orderBy("l", "r")
          .limit(23)
          .select("l", "r")
        val expected = withConf("spark.comet.enabled", "false") {
          joinQuery(spark.read.parquet(path), "inner", true)
            .orderBy("l", "r")
            .limit(23)
            .select("l", "r")
            .collect()
            .toSeq
        }
        val healthySpec = localNodes(healthy).head.spec
        val cancelSpec = localNodes(input.groupBy("id").count().orderBy("id")).head.spec
        val failSpec = localNodes(input.orderBy("id")).head.spec
          .asInstanceOf[LocalParquetSpec]
          .copy(memoryLimit = 1L, spillEnabled = false)
        val native = new NativeLocal
        for (_ <- 0 until 8) {
          val ids = scala.collection.mutable.ArrayBuffer.empty[Long]
          val util = new NativeUtil
          val failedUtil = new NativeUtil
          try {
            ids += startNative(native, healthySpec)
            ids += startNative(native, cancelSpec)
            ids += startNative(native, failSpec)
            assert(native.activeQueries() == 3)
            native.close(ids(1))
            intercept[Exception] { readNative(native, failedUtil, ids(2), failSpec.columns) }
            assert(native.activeQueries() == 1)
            val actual = scala.collection.mutable.ArrayBuffer.empty[Row]
            var finished = false
            while (!finished) {
              readNative(native, util, ids.head, healthySpec.columns) match {
                case Some(batch) =>
                  try {
                    (0 until batch.numRows()).foreach { row =>
                      actual += Row(batch.column(0).getLong(row), batch.column(1).getLong(row))
                    }
                  } finally batch.close()
                case None => finished = true
              }
            }
            assert(actual.toSeq == expected)
            assert(native.activeQueries() == 0)
          } finally {
            ids.foreach(native.close)
            util.close()
            failedUtil.close()
          }
          assert(CometArrowImportAllocator.getAllocatedMemory == 0)
        }
      }
    }
  }

  test("repeated native close racing with result reads does not invalidate imported batches") {
    val native = new NativeLocal
    val closer = Executors.newSingleThreadExecutor()
    try {
      for (_ <- 0 until 24) {
        val id = native.createRange(0, Long.MaxValue, 1, 7, 17, 1)
        val util = new NativeUtil
        val started = new java.util.concurrent.CountDownLatch(1)
        val closed = closer.submit(new Callable[Unit] {
          override def call(): Unit = {
            assert(started.await(5, TimeUnit.SECONDS))
            native.close(id)
            native.close(id)
          }
        })
        try {
          val first = readNative(native, util, id, 1).get
          try {
            started.countDown()
            // Either a batch wins the race or close/cancel wins. Both must preserve
            // ownership of the already-imported first batch.
            try readNative(native, util, id, 1).foreach(_.close())
            catch {
              case failure: Exception =>
                assert(
                  Iterator
                    .iterate[Throwable](failure)(_.getCause)
                    .takeWhile(_ != null)
                    .exists(e =>
                      Option(e.getMessage).exists(m =>
                        m.contains("closed") || m.contains("cancelled"))))
            }
            closed.get(5, TimeUnit.SECONDS)
            assert(first.column(0).getLong(0) == 0L)
          } finally first.close()
          intercept[Exception] { readNative(native, util, id, 1) }
        } finally {
          started.countDown()
          native.close(id)
          util.close()
        }
        assert(native.activeQueries() == 0)
      }
    } finally closer.shutdownNow()
  }

  test("local explain is compact and output metrics count delivered rows and batches") {
    withParquetData { path =>
      val query = spark.read.parquet(path).select("id").orderBy("id").limit(23)
      val node = localNodes(query).head
      val description = node.simpleString(20)
      assert(description.contains("query=parquet") && description.contains("terminal=true"))
      assert(description.contains("memoryLimit=") && description.contains("resultPartitions=1"))
      assert(!description.contains("LocalParquetSpec") && !description.contains(path))
      assert(description.length < 512)
      assert(new NativeLocal().activeQueries() == 0)
      node.resetMetrics()
      assert(query.collect().length == 23)
      assert(node.metrics("numOutputRows").value == 23L)
      assert(node.metrics("numOutputBatches").value > 0L)
    }
  }

}
