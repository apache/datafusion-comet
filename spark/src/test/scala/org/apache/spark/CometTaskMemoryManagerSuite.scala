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

import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.LogEvent
import org.apache.spark.memory.{MemoryConsumer, TaskMemoryManager, TestMemoryManager}

class CometTaskMemoryManagerSuite extends TaskMemoryTestUtils {

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

  test("an acquire waiting in Spark survives a release that empties the task's balance") {
    checkRequestWhileBalanceEmpties(loggers) { taskMemoryManager =>
      val manager = new CometTaskMemoryManager(1L, 0L)
      // Another native thread releases the task's last 10 bytes while the acquire waits.
      WaitingRequest(
        manager.acquireMemory(_),
        manager.releaseMemory(_),
        () => manager.acquireMemory(20L),
        granted => {
          assert(granted == 20L)
          assert(manager.getUsed == 20L)
          assert(taskMemoryManager.getMemoryConsumptionForThisTask == 20L)
          manager.releaseMemory(20L)
          assert(taskMemoryManager.getMemoryConsumptionForThisTask == 0L)
        })
    }
  }

  test("a NoSuchElementException other than Spark's missing task entry is rethrown") {
    val wrongTask = noSuchElement("key not found: 7", fromExecutionMemoryPool = true)
    val notFromPool = noSuchElement("key not found: 0", fromExecutionMemoryPool = false)
    for (error <- Seq(wrongTask, notFromPool)) {
      val taskMemoryManager = new FailingTaskMemoryManager(() => error, failures = Int.MaxValue)
      withTaskContext(taskMemoryManager) { _ =>
        val manager = new CometTaskMemoryManager(1L, 0L)
        val thrown = intercept[NoSuchElementException](manager.acquireMemory(10L))
        assert(thrown eq error)
        assert(taskMemoryManager.calls.get == 1)
        assert(manager.getUsed == 0L)
      }
    }
  }

  test("an acquire that keeps losing its task entry gives up with a zero grant") {
    // A new exception on every call, so the test can tell which one is logged.
    val taskMemoryManager = new FailingTaskMemoryManager(
      () => noSuchElement("key not found: 0", fromExecutionMemoryPool = true),
      failures = Int.MaxValue)
    withTaskContext(taskMemoryManager) { _ =>
      val manager = new CometTaskMemoryManager(1L, 0L)
      val events = logEvents(Level.INFO) {
        // A refusal rather than an exception, so native spills as for any refused reservation.
        assert(manager.acquireMemory(10L) == 0L)
      }
      assert(events.map(_.getLevel) == Seq(Level.INFO, Level.INFO, Level.WARN), messages(events))
      assert(events.last.getThrown eq taskMemoryManager.errors.last)
      assert(taskMemoryManager.calls.get == MaxAcquireAttempts)
      assert(manager.getUsed == 0L)
      assert(nativeMemoryConsumer(manager).getUsed == 0L)
      assert(taskMemoryManager.getMemoryConsumptionForThisTask == 0L)
    }
  }

  private val loggers =
    Seq(classOf[CometTaskMemoryManager].getName, classOf[TaskMemoryManager].getName)

  /** The events that Comet's and Spark's task memory managers log at `level` or above. */
  private def logEvents(level: Level)(f: => Unit): Seq[LogEvent] = logEvents(level, loggers)(f)

  private def withTaskMemoryManager(f: TaskMemoryManager => Unit): Unit = {
    val memoryManager = new TestMemoryManager(new SparkConf())
    memoryManager.limit(1024)
    withTaskContext(new TaskMemoryManager(memoryManager, 0L))(f)
  }

  private def nativeMemoryConsumer(manager: CometTaskMemoryManager): MemoryConsumer = {
    val field = classOf[CometTaskMemoryManager].getDeclaredField("nativeMemoryConsumer")
    field.setAccessible(true)
    field.get(manager).asInstanceOf[MemoryConsumer]
  }
}
