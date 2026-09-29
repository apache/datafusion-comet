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

package org.apache.spark

import java.io.ByteArrayInputStream
import java.lang.ref.WeakReference
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

import scala.reflect.ClassTag

import org.apache.arrow.memory.RootAllocator
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.memory.{TaskMemoryManager, TestMemoryManager}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.catalyst.expressions.PrettyAttribute
import org.apache.spark.sql.comet.{CometExec, CometExecUtils, CometMetricNode, CometNativeScanExec, CometProjectExec, CometSortExec}
import org.apache.spark.sql.comet.execution.arrow.CometArrowStream
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{LongType, StructField, StructType}

import org.apache.comet.{CometConf, CometExecIterator, CometShuffleBlockIterator, Native}
import org.apache.comet.serde.Config.ConfigMap
import org.apache.comet.serde.OperatorOuterClass

/**
 * Regression tests for the native plan lifecycle: every `createPlan` must be balanced by exactly
 * one release of the native execution context and of its task-shared memory pool reference, even
 * when a step of the lifecycle fails partway through. See issue #5212 (positions 2, 3 and 8).
 */
class CometExecIteratorLifecycleSuite extends CometTestBase {

  private def withTaskContext[T](taskAttemptId: Long)(f: => T): T = {
    val memoryManager = new TestMemoryManager(new SparkConf())
    val taskMemoryManager = new TaskMemoryManager(memoryManager, taskAttemptId)
    val taskContext = new TaskContextImpl(
      stageId = 0,
      stageAttemptNumber = 0,
      partitionId = 0,
      numPartitions = 1,
      taskAttemptId = taskAttemptId,
      attemptNumber = 0,
      taskMemoryManager = taskMemoryManager,
      localProperties = new Properties,
      metricsSystem = null,
      taskMetrics = TaskMetrics.empty,
      cpus = 1,
      resources = Map.empty)
    TaskContext.setTaskContext(taskContext)
    try {
      f
    } finally {
      try {
        // Runs the task completion listeners, as the end of a real task does.
        taskContext.markTaskCompleted(None)
      } finally {
        taskMemoryManager.cleanUpAllAllocatedMemory()
        TaskContext.unset()
      }
    }
  }

  /** Retries GC until every weak reference clears or the deadline passes; returns survivors. */
  private def survivorsAfterGc(refs: Seq[WeakReference[_]]): Int = {
    val deadline = System.nanoTime() + 30L * 1000 * 1000 * 1000
    while (refs.exists(_.get() != null) && System.nanoTime() < deadline) {
      System.gc()
      Thread.sleep(50)
    }
    refs.count(_.get() != null)
  }

  test("createPlan failure releases the task-shared memory pool reference") {
    val nativeLib = new Native()
    val emptyPlan = OperatorOuterClass.Operator.newBuilder().build().toByteArray
    // An unknown DataFusion config makes createPlan fail while building the session context,
    // which happens after the task-shared memory pool has been registered for the task.
    val badConfigs = ConfigMap
      .newBuilder()
      .putEntries("spark.comet.datafusion.no_such_namespace.option", "1")
      .build()
      .toByteArray

    val managerRefs = (0 until 10).map { i =>
      // Unique synthetic task attempt ids keep each iteration's pool entry independent.
      val taskAttemptId = 4200000L + i
      withTaskContext(taskAttemptId) {
        val manager = new CometTaskMemoryManager(i, taskAttemptId)
        val thrown = intercept[Throwable] {
          nativeLib.createPlan(
            i,
            Array.empty[Object],
            emptyPlan,
            badConfigs,
            1,
            CometMetricNode(Map.empty),
            0L,
            manager,
            Array(System.getProperty("java.io.tmpdir")),
            8192,
            true,
            "fair_unified",
            64L << 20,
            taskAttemptId,
            1L,
            null,
            null,
            null)
        }
        // Guard against a vacuous pass: the failure must be the injected config error thrown
        // inside createPlan, not e.g. an UnsatisfiedLinkError from a missing native library.
        assert(
          thrown.getMessage != null && thrown.getMessage.contains("no_such_namespace"),
          s"expected the injected DataFusion config failure, got: $thrown")
        new WeakReference(manager)
      }
    }

    // A stranded TASK_SHARED_MEMORY_POOLS entry holds a JNI global ref to the
    // CometTaskMemoryManager, so the manager staying reachable means the pool leaked.
    val survivors = survivorsAfterGc(managerRefs)
    assert(
      survivors == 0,
      s"$survivors of ${managerRefs.size} CometTaskMemoryManagers stayed reachable: " +
        "createPlan failure leaked their task-shared memory pool references")
  }

  test("close() is idempotent and still releases the plan when teardown throws") {
    withTaskContext(4300000L) {
      val boom = new java.io.IOException("injected shuffle block close failure")
      val throwingBlockIter =
        new CometShuffleBlockIterator(new ByteArrayInputStream(Array.emptyByteArray)) {
          override def close(): Unit = throw boom
        }
      @volatile var laterInputClosed = false
      val trackingBlockIter =
        new CometShuffleBlockIterator(new ByteArrayInputStream(Array.emptyByteArray)) {
          override def close(): Unit = {
            laterInputClosed = true
            super.close()
          }
        }
      val limitOp =
        CometExecUtils.getLimitNativePlan(Seq(PrettyAttribute("test", LongType)), 100).get
      val iter = new CometExecIterator(
        id = 1L,
        inputObjects = Array.empty[Object],
        numOutputCols = 1,
        protobufQueryPlan = limitOp.toByteArray,
        nativeMetrics = CometMetricNode(Map.empty),
        numParts = 1,
        partitionIndex = 0,
        shuffleBlockIterators = Map(0 -> throwingBlockIter, 1 -> trackingBlockIter))

      val thrown = intercept[java.io.IOException](iter.close())
      assert(thrown eq boom)
      // One input's close failure must not skip the remaining resources: this close() is the only
      // chance to release them, since the task-completion retry is a no-op once `closed` is set.
      assert(laterInputClosed, "a later shuffle input was not closed after an earlier one threw")
      // The first close() must have marked the iterator closed and released the plan despite the
      // teardown failure: a second close() re-running releasePlan would free the native
      // execution context twice, and skipping the release would strand it.
      iter.close()
    }
  }

  test("releasePlan frees the native context even when the final metrics update fails") {
    // Disable the periodic metrics updates inside executePlan, so the only metrics update -- and
    // therefore the only place the injected failure can fire -- is the one in releasePlan.
    withSQLConf(CometConf.COMET_METRICS_UPDATE_INTERVAL.key -> "0") {
      withTaskContext(4400000L) {
        val failMetrics = new AtomicBoolean(false)
        class ThrowingMetricNode extends CometMetricNode(Map.empty, Nil) {
          override def set_all_from_bytes(bytes: Array[Byte]): Unit = {
            if (failMetrics.get()) {
              throw new IllegalStateException("injected metrics update failure")
            }
          }
        }
        val schema = StructType(Seq(StructField("test", LongType, nullable = false)))
        val stream = CometArrowStream.fromColumnarBatchIter(
          Iterator.empty,
          schema,
          CometArrowStream.NATIVE_TIMEZONE,
          "lifecycle-test")
        val limitOp =
          CometExecUtils.getLimitNativePlan(Seq(PrettyAttribute("test", LongType)), 100).get
        val iter = CometExec.getCometIterator(
          Array(stream.asInstanceOf[Object]),
          1,
          limitOp,
          new ThrowingMetricNode,
          1,
          0,
          None,
          Seq.empty)

        failMetrics.set(true)
        // Exhausting the iterator closes it, and the close propagates the metrics failure thrown
        // by the native releasePlan call.
        val thrown = intercept[Throwable](iter.hasNext)
        // Guard against a vacuous pass: the failure must be the injected one, thrown from the
        // releasePlan metrics update (the only metrics update left with the interval disabled).
        assert(
          thrown.getMessage != null && thrown.getMessage.contains(
            "injected metrics update failure"),
          s"expected the injected metrics update failure, got: $thrown")
        // The metrics failure must not have left the iterator open or the native context alive: a
        // second close() must be a no-op instead of calling releasePlan again.
        iter.close()
      }
    }
  }

  test("a plan fed only by native scans returns its memory when its consumer stops early") {
    // See issue #2453. The sort keeps its sorted runs in memory, and the merge that reads them
    // spawns a Tokio task for each run.
    assertMemoryReturnedWhenTheConsumerStopsEarly(expectSpill = false)
  }

  test("a plan fed only by native scans returns its memory when it spilled and stops early") {
    // With a tiny pool the sort spills, and the merge that reads the spill files back holds its
    // memory in the plan's own stream.
    withSQLConf(
      CometConf.COMET_OFFHEAP_MEMORY_POOL_FRACTION.key -> "0.002",
      CometConf.COMET_RESPECT_DATAFUSION_CONFIGS.key -> "true",
      "spark.comet.datafusion.execution.sort_spill_reservation_bytes" -> "65536") {
      assertMemoryReturnedWhenTheConsumerStopsEarly(expectSpill = true)
    }
  }

  /**
   * Sorts one file natively, reads a single row of the result and stops, as a JVM limit does, and
   * checks that the task holds no memory once the native plan has been closed.
   *
   * Spark frees whatever a task still holds when the task ends, and can hand it to another task
   * at once. Memory the native plan returns after that is memory it was still using while Spark
   * counted it as free, and Spark logs "release called on N bytes but task only has 0 bytes" when
   * it arrives.
   */
  private def assertMemoryReturnedWhenTheConsumerStopsEarly(expectSpill: Boolean): Unit = {
    withTempPath { path =>
      // One file keeps every row in one task, so the sort still holds most of them when the
      // consumer stops.
      spark
        .range(0, 100000, 1, 1)
        .selectExpr("id", "CAST(id AS STRING) AS s")
        .write
        .parquet(path.getAbsolutePath)
      withParquetTable(path.getAbsolutePath, "tbl") {
        withSQLConf(CometConf.COMET_BATCH_SIZE.key -> "1024") {
          // Stalls on one row of the sort's second batch, so the plan is still producing that
          // batch when the consumer stops after the first. The native projection calls it on the
          // thread running the plan.
          val stallInSecondBatch = udf { (id: Long) =>
            if (id == 98000L) Thread.sleep(500)
            id
          }
          val sorted = sql("SELECT * FROM tbl SORT BY id DESC")
            .select(stallInSecondBatch(col("id")).as("id"), col("s"))
          val plan = sorted.queryExecution.executedPlan
          // With no JVM input, the plan runs on a Tokio task rather than the Spark task thread.
          val sorts = plan.collect { case sort: CometSortExec => sort }
          assert(sorts.nonEmpty, s"Expected a native sort:\n$plan")
          assert(
            plan.find(_.isInstanceOf[CometProjectExec]).isDefined,
            s"Expected the UDF in a native projection:\n$plan")
          assert(
            plan.find(_.isInstanceOf[CometNativeScanExec]).isDefined,
            s"Expected a native scan:\n$plan")

          val heldWhileReading = spark.sparkContext.longAccumulator
          val heldOnceClosed = spark.sparkContext.longAccumulator
          val rowsRead = new RunAfterParentTaskCompletion(
            sorted.queryExecution.toRdd,
            context =>
              heldOnceClosed.add(context.taskMemoryManager().getMemoryConsumptionForThisTask))
            .mapPartitions { rows =>
              val read = if (rows.hasNext) {
                rows.next()
                1L
              } else {
                0L
              }
              heldWhileReading.add(
                TaskContext.get().taskMemoryManager().getMemoryConsumptionForThisTask)
              Iterator.single(read)
            }
            .collect()
            .sum

          assert(rowsRead == 1)
          val spilled = sorts.map(_.metrics("spilled_bytes").value).sum
          assert((spilled > 0) == expectSpill, s"The sort spilled $spilled bytes")
          // Guards against a vacuous pass: the sort must hold memory when the consumer stops.
          assert(heldWhileReading.value > 0, "The native sort held no memory while it was read")
          assert(
            heldOnceClosed.value == 0,
            s"The task still held ${heldOnceClosed.value} bytes after its native plan was closed")
        }
      }
    }
  }

  test("getMemoryUsage counts live plans and reports native allocation") {
    val nativeLib = new Native()
    // Other suites' plans can still be live, so the plan count is compared as a delta.
    val plansBefore = nativeLib.getMemoryUsage()(3)
    withTaskContext(4500000L) {
      val limitOp =
        CometExecUtils.getLimitNativePlan(Seq(PrettyAttribute("test", LongType)), 100).get
      val iter = new CometExecIterator(
        id = 4500001L,
        inputObjects = Array.empty[Object],
        numOutputCols = 1,
        protobufQueryPlan = limitOp.toByteArray,
        nativeMetrics = CometMetricNode(Map.empty),
        numParts = 1,
        partitionIndex = 0)
      try {
        val usage = nativeLib.getMemoryUsage()
        assert(usage(3) == plansBefore + 1, "a created plan must be counted until it is released")
        assert(usage(2) >= 1, "a live plan must have a memory pool")
        assert(usage(1) >= 0)
        // The native library always installs the accounting allocator, so a zero allocation
        // means it or its wiring was lost.
        assert(usage(0) > 0, s"native allocation was reported as ${usage(0)}")
      } finally {
        iter.close()
      }
    }
    assert(nativeLib.getMemoryUsage()(3) == plansBefore, "a released plan must not be counted")
  }

  test("a native plan that closes while another plan in its task holds memory does not warn") {
    withTempPath { path =>
      spark
        .range(0, 100000, 1, 1)
        .selectExpr("id", "CAST(id AS STRING) AS s")
        .write
        .parquet(path.getAbsolutePath)
      withParquetTable(path.getAbsolutePath, "tbl") {
        val first = sql("SELECT id FROM tbl WHERE id < 10")
        val second = sql("SELECT * FROM tbl SORT BY id DESC")
        for (df <- Seq(first, second)) {
          val plan = df.queryExecution.executedPlan
          assert(
            plan.find(_.isInstanceOf[CometNativeScanExec]).isDefined,
            s"Expected a native scan:\n$plan")
        }
        val sorted = second.queryExecution.executedPlan
        assert(sorted.find(_.isInstanceOf[CometSortExec]).isDefined, s"Expected a sort:\n$sorted")

        var heldBySort = Array.empty[Long]
        val warnings = nonZeroMemoryUsageWarnings {
          // Zipping runs both plans in one task. The first is created first, so the task's native
          // memory pool is created along with it.
          heldBySort = first.queryExecution.toRdd
            .zipPartitions(second.queryExecution.toRdd) { (firstRows, secondRows) =>
              // The sort takes in all of its input to produce its first row, and holds on to it.
              assert(secondRows.hasNext)
              val held = TaskContext.get().taskMemoryManager().getMemoryConsumptionForThisTask
              // Reading the first plan to its end closes it while the sort holds its memory.
              firstRows.foreach(_ => ())
              Iterator.single(held)
            }
            .collect()
        }
        // Guards against a vacuous pass: the sort must hold memory when the first plan closes.
        assert(heldBySort.length == 1 && heldBySort.head > 0, heldBySort.mkString(", "))
        assert(warnings.isEmpty, warnings.mkString("\n"))
      }
    }
  }

  test("the last native plan in a task to close warns about the memory the task still holds") {
    withTaskContext(4600000L) {
      val first = planWithoutInput(4600001L)
      val second = planWithoutInput(4600002L)
      // Stands in for native memory that outlives the plans that reserved it: every native plan
      // in the task acquires memory through the task's manager.
      val manager = CometExecIterator.taskMemory(TaskContext.get(), 4600003L).manager
      assert(manager.acquireMemory(1234L) == 1234L)
      try {
        val whileAnotherPlanIsOpen = nonZeroMemoryUsageWarnings(first.close())
        assert(whileAnotherPlanIsOpen.isEmpty, whileAnotherPlanIsOpen.mkString("\n"))
        val warnings = nonZeroMemoryUsageWarnings(second.close())
        assert(
          warnings.size == 1 && warnings.head.contains(": 1234 bytes, held by task 4600000"),
          warnings.mkString("\n"))
      } finally {
        manager.releaseMemory(1234L)
      }
    }
  }

  test("a task's memory manager is released when the task ends, even with a plan left open") {
    val managerRef = withTaskContext(4700000L) {
      // Closed by the end of the task.
      planWithoutInput(4700001L)
      new WeakReference(CometExecIterator.taskMemory(TaskContext.get(), 4700002L).manager)
    }
    assert(survivorsAfterGc(Seq(managerRef)) == 0, "the task's memory manager outlived the task")
  }

  /** A native plan with no input, in the current task. */
  private def planWithoutInput(id: Long): CometExecIterator = {
    val limitOp =
      CometExecUtils.getLimitNativePlan(Seq(PrettyAttribute("test", LongType)), 100).get
    new CometExecIterator(
      id = id,
      inputObjects = Array.empty[Object],
      numOutputCols = 1,
      protobufQueryPlan = limitOp.toByteArray,
      nativeMetrics = CometMetricNode(Map.empty),
      numParts = 1,
      partitionIndex = 0)
  }

  /** The warnings about a native plan closing with memory still in use that `f` logs. */
  private def nonZeroMemoryUsageWarnings(f: => Unit): Seq[String] = {
    import org.apache.logging.log4j.Level
    // Listen on the package logger. For a logger with no config of its own, withLogAppender
    // creates one that outlives the test and does not pass events up, which would hide
    // CometExecIterator's warnings from later appenders on org.apache.comet.
    val appender = new LogAppender("non-zero memory usage warnings")
    withLogAppender(appender, Seq("org.apache.comet"), Some(Level.WARN))(f)
    appender.loggingEvents
      .map(_.getMessage.getFormattedMessage)
      .filter(_.contains("closed with non-zero memory usage"))
      .toSeq
  }

  test("the memory usage log reports while plans run and once after the last one finishes") {
    import CometExecIterator.JvmArrowMemory
    val mib = 1024L * 1024
    val busy = Array(300 * mib, 100 * mib, 2L, 3L)
    val jvmArrow = JvmArrowMemory(allocated = 40 * mib, imported = 10 * mib)
    assert(
      CometExecIterator
        .memoryUsageMessage(busy, jvmArrow, plansAtLastLog = 0)
        .contains(
          "Comet native memory usage: allocated 300.0 MiB, reserved 100.0 MiB " +
            "(3 native plans, 2 memory pools); JVM Arrow allocated 40.0 MiB, 10.0 MiB of it " +
            "imported from native"))

    // The line after the last plan finishes shows the allocation the plans left behind.
    val idle = Array(20 * mib, 0L, 0L, 0L)
    val noArrow = JvmArrowMemory(0L, 0L)
    assert(
      CometExecIterator
        .memoryUsageMessage(idle, noArrow, plansAtLastLog = 3)
        .exists(_.contains("allocated 20.0 MiB, reserved 0.0 MiB (0 native plans")))
    assert(CometExecIterator.memoryUsageMessage(idle, noArrow, plansAtLastLog = 0).isEmpty)
  }

  test("the memory usage log reads JVM Arrow memory from the allocators, imports apart") {
    import CometExecIterator.JvmArrowMemory
    val root = new RootAllocator(Long.MaxValue)
    try {
      val imports = root.newChildAllocator("imports", 0, Long.MaxValue)
      val others = root.newChildAllocator("others", 0, Long.MaxValue)
      val owned = others.buffer(1024 * 1024)
      val imported = imports.buffer(256 * 1024)
      try {
        val memory = JvmArrowMemory.of(root, imports)
        assert(memory == JvmArrowMemory(allocated = 1280 * 1024, imported = 256 * 1024))
        assert(memory.allocatedByJvm == 1024 * 1024)
      } finally {
        imported.close()
        owned.close()
        imports.close()
        others.close()
      }
    } finally {
      root.close()
    }
    // The two figures are read one after the other, so an import can land in between.
    assert(JvmArrowMemory(allocated = 10L, imported = 20L).allocatedByJvm == 0L)
  }

  test("the memory usage log warns when the native footprint exceeds the container") {
    import CometExecIterator.{nativeMemoryLimitWarning, JvmArrowMemory}
    val mib = 1024L * 1024
    // A 4 GiB off-heap pool with a 1 GiB overhead, and a pool running at 0.8 of the off-heap size.
    val limit = 5120 * mib
    val noArrow = JvmArrowMemory(0L, 0L)
    // Spark's off-heap pool holds only Comet's reservation unless told otherwise.
    def warning(
        allocated: Long,
        reserved: Long = 3000 * mib,
        jvmArrow: JvmArrowMemory = noArrow,
        sparkOffHeapUsed: Option[Long] = None): Option[String] =
      nativeMemoryLimitWarning(
        Array(allocated, reserved, 1L, 1L),
        jvmArrow,
        sparkOffHeapUsed.getOrElse(reserved),
        limit)

    // 1500 MiB untracked is more than the overhead, but fits in what the pool left free.
    assert(warning(4500 * mib).isEmpty)
    // 2500 MiB untracked does not fit: 2500 + 3000 = 5500 MiB.
    val native = warning(5500 * mib)
    assert(native.exists(_.contains("(2500.0 MiB, native and JVM Arrow) plus Spark's off-heap")))
    assert(
      native.exists(_.contains("memory in use (3000.0 MiB, including Comet's reservations)")))
    assert(native.exists(_.contains("is 5500.0 MiB, more than the 5120.0 MiB")))
    // Spark's own off-heap use counts against the same limit.
    assert(warning(4500 * mib, sparkOffHeapUsed = Some(4000 * mib)).isDefined)
    // So does Arrow memory the JVM allocated itself: 1500 + 700 + 3000 = 5200 MiB.
    val jvm = warning(4500 * mib, jvmArrow = JvmArrowMemory(900 * mib, imported = 200 * mib))
    assert(jvm.exists(_.contains("(2200.0 MiB, native and JVM Arrow)")))
    assert(jvm.exists(_.contains("is 5200.0 MiB, more than the 5120.0 MiB")))
    // Imported buffers were allocated by native code, so the allocation already counts them.
    assert(
      warning(4500 * mib, jvmArrow = JvmArrowMemory(900 * mib, imported = 900 * mib)).isEmpty)
    // A batch the JVM allocated and a native operator holds on to is reserved as well, so it
    // counts once: 1000 + 2500 - 3500 leaves nothing untracked, and 3500 MiB in all fits.
    assert(
      warning(
        1000 * mib,
        reserved = 3500 * mib,
        jvmArrow = JvmArrowMemory(2500 * mib, 0L)).isEmpty)
    // Reservations can exceed the allocation, since operators reserve before they allocate.
    assert(warning(100 * mib).isEmpty)
  }

  test("the memory pool limit reads a bare off-heap size as bytes, as Spark does") {
    import CometExecIterator.getMemoryConfig
    val fourGiB = 4L * 1024 * 1024 * 1024
    val offHeap = new SparkConf(false)
      .set("spark.master", "local[4]")
      .set("spark.memory.offHeap.enabled", "true")
    assert(
      getMemoryConfig(offHeap.clone.set("spark.memory.offHeap.size", "4294967296")).memoryLimit
        == fourGiB)
    assert(
      getMemoryConfig(offHeap.clone.set("spark.memory.offHeap.size", "4g")).memoryLimit
        == fourGiB)
  }

  test("the native memory limit is the off-heap size plus the memory overhead") {
    import CometExecIterator.nativeMemoryLimit
    val mib = 1024L * 1024
    val offHeap = new SparkConf(false)
      .set("spark.master", "yarn")
      .set("spark.executor.memory", "16g")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "4g")
    assert(nativeMemoryLimit(offHeap) == Some((4096 + 1638) * mib))
    assert(nativeMemoryLimit(offHeap.clone.set("spark.memory.offHeap.enabled", "false")).isEmpty)
    assert(nativeMemoryLimit(offHeap.clone.set("spark.master", "local[*]")).isEmpty)
  }

  test("the executor memory overhead is sized as Spark sizes the container") {
    import CometExecIterator.executorMemoryOverhead
    val mib = 1024L * 1024
    def conf(settings: (String, String)*): SparkConf =
      new SparkConf(false).set("spark.master", "yarn").setAll(settings)

    assert(
      executorMemoryOverhead(conf("spark.executor.memoryOverhead" -> "3g")) == Some(3072 * mib))
    // A bare number is in MiB, as Spark reads it.
    assert(
      executorMemoryOverhead(conf("spark.executor.memoryOverhead" -> "500")) == Some(500 * mib))
    assert(executorMemoryOverhead(conf("spark.executor.memory" -> "16g")) == Some(1638 * mib))
    // The 1g default executor would get 102 MiB from the factor, so the minimum applies.
    assert(executorMemoryOverhead(conf()) == Some(384 * mib))
    assert(
      executorMemoryOverhead(
        conf(
          "spark.executor.memory" -> "10g",
          "spark.executor.memoryOverheadFactor" -> "0.25")) == Some(2560 * mib))
    assert(executorMemoryOverhead(conf("spark.executor.memoryOverhead" -> "lots")).isEmpty)
    assert(executorMemoryOverhead(new SparkConf(false).set("spark.master", "local[4]")).isEmpty)
  }

  test("the memory usage log interval disables the log on a value it cannot use") {
    import CometExecIterator.memoryUsageLogInterval
    assert(memoryUsageLogInterval(None) == 10000L)
    assert(memoryUsageLogInterval(Some("1s")) == 1000L)
    assert(memoryUsageLogInterval(Some("250ms")) == 250L)
    // A bare number is in milliseconds, the unit the setting is declared with.
    assert(memoryUsageLogInterval(Some("500")) == 500L)
    assert(memoryUsageLogInterval(Some("0")) == 0L)
    // Values that would otherwise throw from the plan that starts the log, failing its task.
    assert(memoryUsageLogInterval(Some("-5s")) == 0L)
    assert(memoryUsageLogInterval(Some("false")) == 0L)
    assert(memoryUsageLogInterval(Some("10 seconds please")) == 0L)
  }
}

/**
 * Adds `onTaskEnd` as a task completion listener before it computes `prev`. Spark runs completion
 * listeners in the reverse order they were added, so `onTaskEnd` runs after every listener that
 * computing `prev` adds, such as the one that closes a native plan.
 */
private class RunAfterParentTaskCompletion[T: ClassTag](
    prev: RDD[T],
    onTaskEnd: TaskContext => Unit)
    extends RDD[T](prev) {

  override protected def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    context.addTaskCompletionListener[Unit](onTaskEnd)
    firstParent[T].iterator(split, context)
  }
}
