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
}
