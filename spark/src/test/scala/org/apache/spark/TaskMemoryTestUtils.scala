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

import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import scala.collection.mutable.ArrayBuffer

import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.{LogEvent, LoggerContext}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager, UnifiedMemoryManager}

import org.apache.comet.MissingTaskEntryRetry

/**
 * Helpers for suites that drive Spark's task memory manager from Comet's memory consumers, among
 * them a reproduction of a request that waits in Spark's execution pool and loses the task's
 * entry there (SPARK-59444).
 */
trait TaskMemoryTestUtils extends SparkFunSuite {

  protected val TimeoutSeconds = 10L
  protected val MaxAcquireAttempts: Int = MissingTaskEntryRetry.MAX_ATTEMPTS

  /** A 100 byte off-heap execution pool. */
  protected def offHeapMemoryManager(): UnifiedMemoryManager = {
    val conf = new SparkConf()
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "100")
      .set("spark.memory.storageFraction", "0")
    new UnifiedMemoryManager(conf, 1000L, 500L, 1)
  }

  /** An off-heap consumer that never spills. */
  protected class OffHeapConsumer(taskMemoryManager: TaskMemoryManager)
      extends MemoryConsumer(taskMemoryManager, 0L, MemoryMode.OFF_HEAP) {
    override def spill(size: Long, trigger: MemoryConsumer): Long = 0L
  }

  private val MaxFailingCalls = 10

  /**
   * A task memory manager over a 100 byte off-heap pool whose first `failures` acquires throw the
   * result of `newError`, recording each one in `errors`.
   */
  protected class FailingTaskMemoryManager(newError: () => Throwable, failures: Int)
      extends TaskMemoryManager(offHeapMemoryManager(), 0L) {
    val calls = new AtomicInteger()
    val errors = new ArrayBuffer[Throwable]()

    override def acquireExecutionMemory(required: Long, consumer: MemoryConsumer): Long = {
      val call = calls.incrementAndGet()
      // Past this bound a caller is retrying without a cap; fail the test instead of hanging it.
      if (call > MaxFailingCalls) {
        throw new IllegalStateException(
          s"acquireExecutionMemory called more than $MaxFailingCalls times")
      }
      if (call <= failures) {
        val error = newError()
        errors += error
        throw error
      }
      super.acquireExecutionMemory(required, consumer)
    }
  }

  protected def noSuchElement(
      message: String,
      fromExecutionMemoryPool: Boolean): NoSuchElementException = {
    val error = new NoSuchElementException(message)
    if (fromExecutionMemoryPool) {
      val poolFrame = new StackTraceElement(
        "org.apache.spark.memory.ExecutionMemoryPool",
        "acquireMemory",
        "ExecutionMemoryPool.scala",
        115)
      error.setStackTrace(poolFrame +: error.getStackTrace)
    }
    error
  }

  /** Whether `thread` is parked in Spark's execution pool waiting for memory. */
  protected def waitingInSpark(thread: Thread): Boolean =
    thread.getState == Thread.State.WAITING &&
      thread.getStackTrace.exists(_.getClassName == "org.apache.spark.memory.ExecutionMemoryPool")

  /** Waits until `thread` is parked in Spark's execution pool, failing after the timeout. */
  protected def awaitWaitingInSpark(thread: Thread): Unit = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TimeoutSeconds)
    while (!waitingInSpark(thread) && System.nanoTime() < deadline) Thread.sleep(10)
    assert(waitingInSpark(thread), s"the request is ${thread.getState}")
  }

  /**
   * A request for memory made while the task's balance empties; see
   * [[checkRequestWhileBalanceEmpties]]. `hold` and `release` take and give back the task's bytes
   * through a consumer other than the one `request` asks through, and `request` returns what it
   * was granted. `check` runs on the test thread once the request has returned.
   */
  protected case class WaitingRequest(
      hold: Long => Long,
      release: Long => Unit,
      request: () => Long,
      check: Long => Unit)

  /**
   * The task holds 10 bytes of a 100 byte pool through `hold` and another task holds 90. The
   * request is made on another thread and waits in Spark below the task's minimum share of 25
   * while `release` gives back the task's last 10 bytes, which removes the task's entry from
   * Spark's pool. Once the other task frees its memory the request must not fail, and `check`
   * sees what it was granted. The first attempt lost the task's entry, so `loggers` must have
   * logged a single retry at INFO.
   */
  protected def checkRequestWhileBalanceEmpties(loggers: Seq[String])(
      setup: TaskMemoryManager => WaitingRequest): Unit = {
    val memoryManager = offHeapMemoryManager()
    val otherTask = new OffHeapConsumer(new TaskMemoryManager(memoryManager, 1L))

    withTaskContext(new TaskMemoryManager(memoryManager, 0L)) { taskMemoryManager =>
      val waiting = setup(taskMemoryManager)
      // The other task goes first: acquiring its 90 bytes after the task's 10 would cap it at 50.
      assert(otherTask.acquireMemory(90L) == 90L)
      assert(waiting.hold(10L) == 10L)

      val granted = new AtomicReference[java.lang.Long]()
      val failure = new AtomicReference[Throwable]()
      val thread = new Thread(() =>
        try granted.set(waiting.request())
        catch { case t: Throwable => failure.set(t) })
      thread.setDaemon(true)

      val events = logEvents(Level.INFO, loggers) {
        try {
          thread.start()
          awaitWaitingInSpark(thread)
          waiting.release(10L)
        } finally {
          // Free the other task's memory so that the request does not outlive a failed test.
          otherTask.freeMemory(otherTask.getUsed)
          thread.join(TimeUnit.SECONDS.toMillis(TimeoutSeconds))
        }
      }

      assert(!thread.isAlive, s"the request is ${thread.getState}")
      assert(failure.get == null, s"the request failed: ${failure.get}")
      waiting.check(granted.get)
      assert(events.map(_.getLevel) == Seq(Level.INFO), messages(events))
    }
  }

  /** The events that `loggers` log at `level` or above while `f` runs. */
  protected def logEvents(level: Level, loggers: Seq[String])(f: => Unit): Seq[LogEvent] = {
    val appender = new LogAppender(loggers.mkString(", "))
    appender.setThreshold(level)
    val context = LogManager.getContext(false).asInstanceOf[LoggerContext]
    val unconfigured = loggers.filterNot(context.getConfiguration.getLoggers.containsKey)
    try withLogAppender(appender, loggers, Some(level))(f)
    finally {
      // For a logger with no config of its own, withLogAppender adds one and never removes it.
      // It copies the root config's additivity, which is off, so left in place it would keep the
      // logger's events out of the test log for the rest of the run.
      unconfigured.foreach(context.getConfiguration.removeLogger)
      context.updateLoggers()
    }
    appender.loggingEvents.toSeq
  }

  protected def messages(events: Seq[LogEvent]): String =
    events.map(_.getMessage.getFormattedMessage).mkString("\n")

  /** Runs `f` as task 0 with `taskMemoryManager`, then frees whatever the task still holds. */
  protected def withTaskContext(taskMemoryManager: TaskMemoryManager)(
      f: TaskMemoryManager => Unit): Unit = {
    val taskContext = new TaskContextImpl(
      stageId = 0,
      stageAttemptNumber = 0,
      partitionId = 0,
      numPartitions = 1,
      taskAttemptId = 0L,
      attemptNumber = 0,
      taskMemoryManager = taskMemoryManager,
      localProperties = new Properties,
      metricsSystem = null,
      taskMetrics = TaskMetrics.empty,
      cpus = 1,
      resources = Map.empty)

    TaskContext.setTaskContext(taskContext)
    try {
      f(taskMemoryManager)
    } finally {
      try {
        taskMemoryManager.cleanUpAllAllocatedMemory()
      } finally {
        TaskContext.unset()
      }
    }
  }
}
