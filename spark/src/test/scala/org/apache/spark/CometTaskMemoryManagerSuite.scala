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

import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.{LogEvent, LoggerContext}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.memory.{MemoryConsumer, TaskMemoryManager, TestMemoryManager}

class CometTaskMemoryManagerSuite extends SparkFunSuite {

  test("native memory usage is visible to Spark's memory consumer") {
    withTaskMemoryManager { taskMemoryManager =>
      val manager = new CometTaskMemoryManager(1L, 0L)
      val consumer = nativeMemoryConsumer(manager)

      assert(manager.getUsed == 0L)
      assert(consumer.getUsed == 0L)

      assert(manager.acquireMemory(128L) == 128L)
      assert(manager.getUsed == 128L)
      assert(consumer.getUsed == 128L)
      assert(taskMemoryManager.getMemoryConsumptionForThisTask == 128L)

      manager.releaseMemory(128L)
      assert(manager.getUsed == 0L)
      assert(consumer.getUsed == 0L)
      assert(taskMemoryManager.getMemoryConsumptionForThisTask == 0L)
    }
  }

  test("partial and zero grants account only for acquired memory") {
    withTaskMemoryManager { taskMemoryManager =>
      val manager = new CometTaskMemoryManager(1L, 0L)
      val consumer = nativeMemoryConsumer(manager)

      def checkUsage(expected: Long): Unit = {
        assert(manager.getUsed == expected)
        assert(consumer.getUsed == expected)
        assert(taskMemoryManager.getMemoryConsumptionForThisTask == expected)
      }

      assert(manager.acquireMemory(768L) == 768L)
      checkUsage(768L)
      assert(manager.acquireMemory(512L) == 256L)
      checkUsage(1024L)
      assert(manager.acquireMemory(128L) == 0L)
      checkUsage(1024L)

      manager.releaseMemory(256L)
      checkUsage(768L)
      assert(manager.acquireMemory(128L) == 128L)
      checkUsage(896L)
      manager.releaseMemory(896L)
      checkUsage(0L)
    }
  }

  test("managers in the same task retain separate memory accounting") {
    withTaskMemoryManager { taskMemoryManager =>
      val first = new CometTaskMemoryManager(1L, 0L)
      val second = new CometTaskMemoryManager(2L, 0L)
      val firstConsumer = nativeMemoryConsumer(first)
      val secondConsumer = nativeMemoryConsumer(second)

      def checkUsage(firstBytes: Long, secondBytes: Long): Unit = {
        assert(first.getUsed == firstBytes)
        assert(firstConsumer.getUsed == firstBytes)
        assert(second.getUsed == secondBytes)
        assert(secondConsumer.getUsed == secondBytes)
        assert(taskMemoryManager.getMemoryConsumptionForThisTask == firstBytes + secondBytes)
      }

      checkUsage(0L, 0L)
      assert(first.acquireMemory(128L) == 128L)
      checkUsage(128L, 0L)
      assert(second.acquireMemory(256L) == 256L)
      checkUsage(128L, 256L)
      first.releaseMemory(64L)
      checkUsage(64L, 256L)
      second.releaseMemory(256L)
      checkUsage(64L, 0L)
      first.releaseMemory(64L)
      checkUsage(0L, 0L)
    }
  }

  test("partial and zero grants log nothing at INFO or above") {
    // Spark refuses native reservations routinely under memory pressure, each time with one of
    // these.
    withTaskMemoryManager { _ =>
      val manager = new CometTaskMemoryManager(1L, 0L)
      val events = logEvents(Level.INFO) {
        assert(manager.acquireMemory(768L) == 768L)
        assert(manager.acquireMemory(512L) == 256L)
        assert(manager.acquireMemory(128L) == 0L)
      }
      assert(events.isEmpty, events.map(_.getMessage.getFormattedMessage).mkString("\n"))
      manager.releaseMemory(1024L)
    }
  }

  test("a partial grant is logged at DEBUG, without the task's memory usage dump") {
    withTaskMemoryManager { _ =>
      val manager = new CometTaskMemoryManager(1L, 0L)
      val messages = logEvents(Level.DEBUG) {
        assert(manager.acquireMemory(768L) == 768L)
        assert(manager.acquireMemory(512L) == 256L)
      }.map(_.getMessage.getFormattedMessage)
      val log = messages.mkString("\n")
      assert(messages.exists(_.contains("requested 512 bytes but only received 256 bytes")), log)
      // TaskMemoryManager.showMemoryUsage takes the task's monitor; see acquireMemory.
      assert(!messages.exists(_.contains("Memory used in task")), log)
      manager.releaseMemory(1024L)
    }
  }

  /** The events that Comet's and Spark's task memory managers log at `level` or above. */
  private def logEvents(level: Level)(f: => Unit): Seq[LogEvent] = {
    val appender = new LogAppender("task memory manager")
    appender.setThreshold(level)
    val loggers = Seq(classOf[CometTaskMemoryManager].getName, classOf[TaskMemoryManager].getName)
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

  private def withTaskMemoryManager(f: TaskMemoryManager => Unit): Unit = {
    val memoryManager = new TestMemoryManager(new SparkConf())
    memoryManager.limit(1024)
    val taskMemoryManager = new TaskMemoryManager(memoryManager, 0L)
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

  private def nativeMemoryConsumer(manager: CometTaskMemoryManager): MemoryConsumer = {
    val field = classOf[CometTaskMemoryManager].getDeclaredField("nativeMemoryConsumer")
    field.setAccessible(true)
    field.get(manager).asInstanceOf[MemoryConsumer]
  }
}
