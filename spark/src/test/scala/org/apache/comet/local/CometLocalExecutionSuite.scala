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
import scala.reflect.ClassTag

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.{SparkEnv, SparkException, TaskContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, row_number}

import org.apache.comet.{CometArrowImportAllocator, CometConf, CometExecIterator, CometSparkSessionExtensions, NativeBase}
import org.apache.comet.vector.NativeUtil

class CometLocalExecutionSuite
    extends AnyFunSuite
    with BeforeAndAfterAll
    with BeforeAndAfterEach {
  private var spark: SparkSession = _
  private val enabled = CometConf.COMET_EXEC_LOCAL_ENABLED.key

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("CometLocalExecutionSuite")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.extensions", classOf[CometSparkSessionExtensions].getName)
      // AQE stays at Spark's default: it does not wrap the exchange-free plans admitted here.
      .config("spark.memory.offHeap.enabled", "true")
      .config("spark.memory.offHeap.size", "256m")
      .config("spark.comet.enabled", "true")
      .config("spark.comet.exec.enabled", "true")
      .config("spark.comet.shuffle.enabled", "false")
      .config(CometConf.COMET_EXEC_RANGE_ENABLED.key, "true")
      .config("spark.comet.batchSize", "17")
      .config(enabled, "true")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    assert(NativeBase.isLoaded)
  }

  override protected def afterEach(): Unit = {
    try {
      assert(new NativeLocal().activeQueries() == 0)
      assert(CometArrowImportAllocator.getAllocatedMemory == 0)
      assert(LocalResultHandoff.active == 0)
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

  /** Spark's own answer, with Comet disabled. */
  private def expected(query: => DataFrame): Seq[Row] =
    withConf("spark.comet.enabled", "false")(query.collect().toSeq)

  /** Checks that `query` runs locally and returns Spark's rows in Spark's order. */
  private def checkLocal(query: => DataFrame): Unit = {
    val actual = query
    assert(localNodes(actual).size == 1, actual.queryExecution.executedPlan.toString)
    assert(actual.collect().toSeq == expected(query))
  }

  /** Runs `f` in a Spark task, as the local query iterator does. */
  private def inTask[T: ClassTag](f: TaskContext => T): T =
    spark.sparkContext
      .runJob(
        spark.sparkContext.parallelize(Seq(0), 1),
        { (_: Iterator[Int]) =>
          f(TaskContext.get())
        })
      .head

  test("opt-in is required and admitted ranges match Spark in order") {
    assert(!CometConf.COMET_EXEC_LOCAL_ENABLED.defaultValue.get)
    withConf(enabled, "false") {
      assert(localNodes(spark.range(0, 100, 1, 7).toDF()).isEmpty)
    }
    checkLocal(spark.range(0, 4099, 1, 7).selectExpr("id AS x", "id * 2 AS y"))
    checkLocal(spark.range(30, -10, -3, 7).toDF())
    checkLocal(spark.range(0, 100, 7, 3).filter("id % 3 = 1").selectExpr("id + 1 AS v"))
    checkLocal(spark.range(0, 1000, 1, 1).toDF())
  }

  test("ranges near the long boundaries match Spark, and overflowing ones stay on Spark") {
    checkLocal(spark.range(Long.MinValue, Long.MaxValue, Long.MaxValue, 7).toDF())
    checkLocal(spark.range(Long.MaxValue, Long.MinValue, Long.MinValue, 3).toDF())
    // Spark's generated code overflows for these, so Comet keeps them on Spark.
    val stepped = () => spark.range(Long.MinValue, Long.MaxValue, 1L << 62, 1).toDF()
    assert(localNodes(stepped()).isEmpty, stepped().queryExecution.executedPlan.toString)
    assert(stepped().collect().toSeq == expected(stepped()))
    val wide = () => spark.range(Long.MinValue, Long.MaxValue, 1, 1).toDF()
    assert(localNodes(wide()).isEmpty)
    assert(wide().take(3).toSeq == withConf("spark.comet.enabled", "false")(wide().take(3).toSeq))
  }

  test("empty ranges are not admitted and return no rows") {
    val query = spark.range(5, 5, 1, 7).toDF()
    assert(localNodes(query).isEmpty)
    assert(query.collect().isEmpty)
  }

  test("partition-dependent, nondeterministic and subquery expressions stay on Comet") {
    for (query <- Seq(
        () => spark.range(0, 50, 1, 7).selectExpr("id", "spark_partition_id() AS p"),
        () => spark.range(0, 50, 1, 7).selectExpr("id", "monotonically_increasing_id() AS m"),
        () => spark.range(0, 50, 1, 7).selectExpr("id", "rand(3) AS r"))) {
      assert(localNodes(query()).isEmpty, query().queryExecution.executedPlan.toString)
      assert(query().collect().toSeq == expected(query()))
    }
    val subquery = spark.sql("SELECT (SELECT max(id) FROM range(5)) + id AS v FROM range(3)")
    assert(subquery.queryExecution.executedPlan.collectWithSubqueries { case p: CometLocalExec =>
      p
    }.isEmpty)
    assert(subquery.collect().toSeq == Seq(Row(4L), Row(5L), Row(6L)))
  }

  test("admission does not depend on AQE for exchange-free plans") {
    for (adaptive <- Seq("true", "false")) {
      withConf("spark.sql.adaptive.enabled", adaptive) {
        checkLocal(spark.range(0, 500, 1, 5).selectExpr("id * 3 AS v"))
      }
    }
  }

  test("ordinary Comet planning applies when Comet execution is off") {
    withConf("spark.comet.exec.enabled", "false") {
      assert(localNodes(spark.range(0, 10, 1, 2).toDF()).isEmpty)
    }
    withConf(CometConf.COMET_EXEC_RANGE_ENABLED.key, "false") {
      assert(localNodes(spark.range(0, 10, 1, 2).toDF()).isEmpty)
    }
  }

  test("limits read partitions in order and take, head and show close early") {
    val query = spark.range(0, 1000, 1, 7).toDF()
    // Unordered limits return the first rows of the first partitions, as Spark does.
    for (_ <- 0 until 3) {
      assert(
        query.limit(10).collect().toSeq == expected(spark.range(0, 1000, 1, 7).toDF().limit(10)))
      assert(query.take(5).map(_.getLong(0)).toSeq == (0L until 5L))
    }
    assert(query.head(0).isEmpty)
    val unbounded = spark.range(0, Long.MaxValue, 1, 7).toDF()
    assert(localNodes(unbounded).size == 1)
    assert(unbounded.take(3).map(_.getLong(0)).toSeq == Seq(0L, 1L, 2L))
    unbounded.show(2)
  }

  test("a query over a cached local result keeps its required distribution") {
    withConf("spark.sql.adaptive.enabled", "false") {
      val cached = spark.range(0, 1000, 1, 7).selectExpr("id", "id % 5 AS k")
      assert(localNodes(cached).size == 1)
      cached.cache()
      try {
        val counts = cached.groupBy("k").count()
        // The cache holds the local result, a single partition with no clustering.
        val cachedPlans = counts.queryExecution.withCachedData.collect {
          case relation: InMemoryRelation => relation.cacheBuilder.cachedPlan
        }
        assert(cachedPlans.flatMap(_.collect { case p: CometLocalExec => p }).size == 1)
        val plan = counts.queryExecution.executedPlan
        assert(plan.collect { case e: ShuffleExchangeLike => e }.nonEmpty, plan.toString)
        assert(
          counts.collect().map(r => r.getLong(0) -> r.getLong(1)).toMap ==
            (0L until 5L).map(_ -> 200L).toMap)
      } finally cached.unpersist(blocking = true)
    }
  }

  test("only an ordering that holds across concatenated partitions is reported") {
    // A range's slices are ranges of its ordering, so the result keeps it.
    assert(localNodes(spark.range(0, 100, 1, 7).toDF()).head.outputOrdering.nonEmpty)
    // Sorting within partitions orders each slice, not their concatenation.
    val sorted = spark.range(0, 20, 1, 2).toDF().sortWithinPartitions(desc("id"))
    assert(localNodes(sorted).size == 1)
    assert(localNodes(sorted).head.outputOrdering.isEmpty)
    assert(
      localNodes(
        spark
          .range(0, 20, 1, 1)
          .toDF()
          .sortWithinPartitions(desc("id"))).head.outputOrdering.nonEmpty)
  }

  test("a window over a cached partition-sorted result still sorts") {
    withConf("spark.sql.adaptive.enabled", "false") {
      val cached = spark.range(0, 20, 1, 2).toDF().sortWithinPartitions(desc("id"))
      assert(localNodes(cached).size == 1)
      cached.cache()
      try {
        cached.count()
        val ranks = withConf("spark.comet.enabled", "false") {
          cached
            .select(col("id"), row_number().over(Window.orderBy(desc("id"))))
            .collect()
            .map(r => r.getLong(0) -> r.getInt(1))
            .toMap
        }
        assert(ranks == (0L until 20L).map(id => id -> (20 - id.toInt)).toMap)
      } finally cached.unpersist(blocking = true)
    }
  }

  test("a limit does not fail on an error in a partition it does not reach") {
    withConf("spark.sql.ansi.enabled", "true") {
      def query(): DataFrame =
        spark
          .range(0, 200000, 1, 2)
          .filter("id >= 99999")
          .selectExpr("100L DIV (100000L - id) AS v")
          .limit(1)
      assert(localNodes(query()).size == 1, query().queryExecution.executedPlan.toString)
      // Spark reads only the first partition; the second divides by zero at id 100000.
      assert(query().collect().toSeq == Seq(Row(100L)))
      assert(query().collect().toSeq == expected(query()))
      // A full collect reaches the failing partition and fails as Spark does.
      def all(): DataFrame =
        spark
          .range(0, 200000, 1, 2)
          .filter("id >= 99999")
          .selectExpr("100L DIV (100000L - id) AS v")
      val failure = intercept[Exception](all().collect())
      assert(failure.getMessage.contains("DIVIDE_BY_ZERO"), failure.getMessage)
    }
  }

  test("many partitions with sorts fit the memory pool") {
    // Each sort registers its consumers when its partition starts and reserves memory for its
    // merge up front. Running only as many partitions at once as Spark runs tasks keeps their
    // fair shares of the 256 MiB pool large enough, as with ordinary execution.
    def query(): DataFrame =
      spark
        .range(0, 32000, 1, 32)
        .selectExpr("id", "sha2(cast(id as string), 256) AS k")
        .sortWithinPartitions("k")
    assert(localNodes(query()).size == 1, query().queryExecution.executedPlan.toString)
    assert(query().collect().toSeq == expected(query()))
  }

  test("repeated actions build fresh executions and iterator results") {
    val query = spark.range(0, 300, 3, 7).selectExpr("id AS value")
    val rows = (0L until 300L by 3L).map(Row(_))
    for (_ <- 0 until 3) {
      assert(query.collect().toSeq == rows)
      assert(new NativeLocal().activeQueries() == 0)
    }
    assert(query.toLocalIterator().asScala.toSeq == rows)
  }

  test("an iterator fetches one partition at a time through ordinary execution") {
    val query = spark.range(0, 400, 1, 8).selectExpr("id", "id * 2 AS v")
    assert(query.queryExecution.executedPlan.isInstanceOf[CometLocalResultExec])
    val group = "comet-local-iterator"
    spark.sparkContext.setJobGroup(group, "local iterator")
    val rows =
      try query.toLocalIterator().asScala.toList
      finally spark.sparkContext.clearJobGroup()
    assert(rows == expected(spark.range(0, 400, 1, 8).selectExpr("id", "id * 2 AS v")))
    // One job per partition, as Spark's executeToIterator runs, rather than one for the query.
    val tracker = spark.sparkContext.statusTracker
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20)
    while (tracker.getJobIdsForGroup(group).length < 8 && System.nanoTime() < deadline) {
      Thread.sleep(10)
    }
    assert(tracker.getJobIdsForGroup(group).length == 8)
  }

  test("a tail scans partitions from the last one through ordinary execution") {
    withConf("spark.sql.ansi.enabled", "true") {
      // As PySpark's tail does. The first partition divides by zero at id 0, but Spark reads only
      // the last partition, which has enough rows.
      def query(): DataFrame = spark.range(0, 20, 1, 2).selectExpr("100L DIV id AS v")
      val plan = query().queryExecution.executedPlan
      assert(plan.isInstanceOf[CometLocalResultExec], plan.toString)
      assert(plan.executeTail(1).map(_.getLong(0)).toSeq == Seq(5L))
      val sparkTail = withConf("spark.comet.enabled", "false") {
        query().queryExecution.executedPlan.executeTail(3).map(_.getLong(0)).toSeq
      }
      assert(plan.executeTail(3).map(_.getLong(0)).toSeq == sparkTail)
      assert(plan.executeTail(0).isEmpty)
    }
  }

  test("collect and take hand local results to the driver within a Spark job") {
    val query = spark.range(0, 200, 1, 5).selectExpr("id", "id % 3 AS k")
    assert(query.queryExecution.executedPlan.isInstanceOf[CometLocalResultExec])
    assert(
      query.collect().toSeq == expected(
        spark.range(0, 200, 1, 5).selectExpr("id", "id % 3 AS k")))
    assert(query.take(4).map(_.getLong(0)).toSeq == (0L until 4L))
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

  test("native creation and output errors fail without leaking handles") {
    val spec = localNodes(spark.range(10, 20, 1, 1).toDF()).head.spec
    val (failures, live) = inTask(CometLocalExecutionSuite.creationAndOutputErrors(spec, _))
    assert(failures == Seq(true, true, true))
    assert(live == 0)
  }

  test("imported Arrow batches outlive native close") {
    val spec = localNodes(spark.range(10, 20, 1, 1).toDF()).head.spec
    assert(inTask(CometLocalExecutionSuite.readAfterClose(spec, _)) == (10L until 20L))
  }

  test("explain shows the native block and metrics count delivered rows and batches") {
    val query = spark.range(0, 100, 1, 4).selectExpr("id + 1 AS v")
    val local = localNodes(query).head
    assert(local.toString.contains("partitions=4"))
    assert(query.queryExecution.executedPlan.treeString.contains("CometRange"))
    query.collect()
    assert(local.metrics("numOutputRows").value == 100)
    assert(local.metrics("numOutputBatches").value >= 4)
  }

  test("the native graph takes its batch size from the session when the query runs") {
    for (batchSize <- Seq(10, 17)) {
      withConf("spark.comet.batchSize", batchSize.toString) {
        val query = spark.range(0, 100, 1, 1).toDF()
        assert(query.collect().length == 100)
        val batches = localNodes(query).head.metrics("numOutputBatches").value
        assert(batches == (100 + batchSize - 1) / batchSize)
      }
    }
  }
}

/** Bodies of tests that run in a Spark task, kept out of the suite so it is not serialized. */
object CometLocalExecutionSuite {

  /** Creates a native query for `spec` in the calling task, as the iterator does. */
  private def createQuery(
      native: NativeLocal,
      spec: LocalQuerySpec,
      context: TaskContext,
      partitions: Int,
      columns: Int): Long = {
    val memory = CometExecIterator.getMemoryConfig(SparkEnv.get.conf)
    native.createQuery(
      spec.plan,
      CometExecIterator.serializeCometSQLConfs(),
      partitions,
      columns,
      CometExecIterator.taskMemory(context, 0L).manager,
      SparkEnv.get.blockManager.getLocalDiskDirs,
      17,
      memory.offHeapMode,
      memory.memoryPoolType,
      memory.memoryLimit,
      context.taskAttemptId(),
      1L)
  }

  private def fails(f: => Any): Boolean =
    try {
      f
      false
    } catch { case _: Exception => true }

  /** Whether invalid creation and output pointers fail, and the live queries left after. */
  def creationAndOutputErrors(
      spec: LocalQuerySpec,
      context: TaskContext): (Seq[Boolean], Long) = {
    val native = new NativeLocal
    val noPartitions = fails(createQuery(native, spec, context, partitions = 0, columns = 1))
    val wrongWidth = fails(createQuery(native, spec, context, partitions = 1, columns = 2))
    val id = createQuery(native, spec, context, partitions = 1, columns = 1)
    val nullOutput = fails(native.nextBatch(id, Array(0L), Array(0L)))
    native.close(id)
    native.close(id)
    (Seq(noPartitions, wrongWidth, nullOutput), native.activeQueries())
  }

  /** The values of the first batch, read after its native query is closed. */
  def readAfterClose(spec: LocalQuerySpec, context: TaskContext): Seq[Long] = {
    val native = new NativeLocal
    val util = new NativeUtil
    val id = createQuery(native, spec, context, partitions = 1, columns = 1)
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
        (0 until batch.numRows()).map(batch.column(0).getLong)
      } finally batch.close()
    } finally {
      native.close(id)
      util.close()
    }
  }
}
