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

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.memory.{TaskMemoryManager, TestMemoryManager}
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.catalyst.expressions.PrettyAttribute
import org.apache.spark.sql.comet.{CometExec, CometExecUtils, CometMetricNode}
import org.apache.spark.sql.comet.execution.arrow.CometArrowStream
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
      taskMemoryManager.cleanUpAllAllocatedMemory()
      TaskContext.unset()
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

  test("the memory usage log reports while plans run and once after the last one finishes") {
    val mib = 1024L * 1024
    val busy = Array(300 * mib, 100 * mib, 2L, 3L)
    assert(
      CometExecIterator
        .memoryUsageMessage(busy, plansAtLastLog = 0)
        .contains("Comet native memory usage: allocated 300.0 MiB, reserved 100.0 MiB " +
          "(3 native plans, 2 memory pools)"))

    // The line after the last plan finishes shows the allocation the plans left behind.
    val idle = Array(20 * mib, 0L, 0L, 0L)
    assert(
      CometExecIterator
        .memoryUsageMessage(idle, plansAtLastLog = 3)
        .exists(_.contains("allocated 20.0 MiB, reserved 0.0 MiB (0 native plans")))
    assert(CometExecIterator.memoryUsageMessage(idle, plansAtLastLog = 0).isEmpty)
  }

  test("the memory usage log warns when the native footprint exceeds the container") {
    import CometExecIterator.nativeMemoryLimitWarning
    val mib = 1024L * 1024
    // A 4 GiB off-heap pool with a 1 GiB overhead, and a pool running at 0.8 of the off-heap size.
    val limit = 5120 * mib
    val reserved = 3000 * mib
    // 1500 MiB untracked is more than the overhead, but fits in what the pool left free, since
    // Spark's off-heap pool holds only the reservation.
    assert(nativeMemoryLimitWarning(Array(4500 * mib, reserved, 1L, 1L), reserved, limit).isEmpty)
    // 2500 MiB untracked does not fit: 2500 + 3000 = 5500 MiB.
    val warning = nativeMemoryLimitWarning(Array(5500 * mib, reserved, 1L, 1L), reserved, limit)
    assert(warning.exists(_.contains("(2500.0 MiB) plus Spark's off-heap memory in use (3000.0")))
    assert(warning.exists(_.contains("is 5500.0 MiB, more than the 5120.0 MiB")))
    // Spark's own off-heap use counts against the same limit.
    assert(
      nativeMemoryLimitWarning(Array(4500 * mib, reserved, 1L, 1L), 4000 * mib, limit).isDefined)
    // Reservations can exceed the allocation, since operators reserve before they allocate.
    assert(nativeMemoryLimitWarning(Array(100 * mib, reserved, 1L, 1L), reserved, limit).isEmpty)
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
