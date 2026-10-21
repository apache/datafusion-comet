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

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}

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

  test("a release that Spark rejects leaves the usage unchanged") {
    val memoryManager = new TestMemoryManager(new SparkConf())
    memoryManager.limit(1024)
    val rejectRelease = new AtomicBoolean(false)
    val taskMemoryManager = new TaskMemoryManager(memoryManager, 0L) {
      override def releaseExecutionMemory(size: Long, consumer: MemoryConsumer): Unit = {
        if (rejectRelease.get) {
          throw new IllegalStateException("release rejected")
        }
        super.releaseExecutionMemory(size, consumer)
      }
    }

    withTaskContext(taskMemoryManager) { _ =>
      val manager = new CometTaskMemoryManager(1L, 0L)
      val consumer = nativeMemoryConsumer(manager)
      assert(manager.acquireMemory(128L) == 128L)

      rejectRelease.set(true)
      intercept[IllegalStateException](manager.releaseMemory(64L))
      rejectRelease.set(false)
      // Native code gets the error and still counts every byte as held, and so must the usage.
      assert(manager.getUsed == 128L)
      assert(consumer.getUsed == 128L)
      assert(taskMemoryManager.getMemoryConsumptionForThisTask == 128L)

      manager.releaseMemory(128L)
      assert(manager.getUsed == 0L)
      assert(taskMemoryManager.getMemoryConsumptionForThisTask == 0L)
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

  test("a short grant is handed back while another acquire of the task waits in Spark") {
    val memoryManager = offHeapMemoryManager()
    val otherTask = new OffHeapConsumer(new TaskMemoryManager(memoryManager, 1L))
    val thirdTask = new OffHeapConsumer(new TaskMemoryManager(memoryManager, 2L))
    assert(otherTask.acquireMemory(82L) == 82L)
    assert(thirdTask.acquireMemory(1L) == 1L)

    val shortGrant = new CountDownLatch(1)
    val secondParked = new CountDownLatch(1)
    val taskMemoryManager = new TaskMemoryManager(memoryManager, 0L) {
      override def acquireExecutionMemory(required: Long, consumer: MemoryConsumer): Long = {
        val got = super.acquireExecutionMemory(required, consumer)
        // Out of Spark's monitor, the short grant waits until the second acquire has parked.
        if (got < required && shortGrant.getCount > 0) {
          shortGrant.countDown()
          secondParked.await(2 * TimeoutSeconds, TimeUnit.SECONDS)
        }
        got
      }
    }

    withTaskContext(taskMemoryManager) { _ =>
      val manager = new CometTaskMemoryManager(1L, 0L)
      // Keeps the task's balance above zero, so handing back the short grant never drops its
      // entry from Spark's pool under the waiting acquire.
      assert(manager.acquireMemory(1L) == 1L)

      // Three active tasks and 16 bytes free: a 30 byte request is short granted 16 bytes.
      val first = new NativeThread(
        "the first acquire",
        () => {
          val granted = manager.acquireMemory(30L)
          manager.releaseMemory(granted)
          granted
        })
      val second = new NativeThread("the second acquire", () => manager.acquireMemory(10L))

      try {
        // At DEBUG the short grant is logged, which must not wait on the task's monitor that the
        // parked second acquire holds.
        val messages = logEvents(Level.DEBUG) {
          first.start()
          assert(shortGrant.await(TimeoutSeconds, TimeUnit.SECONDS), s"no short grant: $first")
          // Two active tasks: a 25 byte minimum share and 1 byte free, so 10 bytes waits inside
          // Spark holding the task's monitor.
          thirdTask.freeMemory(1L)
          second.start()
          withClue(second)(awaitWaitingInSpark(second.thread))
          secondParked.countDown()

          assert(second.result(first) == 10L)
          assert(first.result(second) == 16L)
        }.map(_.getMessage.getFormattedMessage)
        assert(
          messages.exists(_.contains("requested 30 bytes but only received 16 bytes")),
          messages.mkString("\n"))
      } finally {
        secondParked.countDown()
        // Free the other task's memory so that neither thread outlives a failed test.
        otherTask.freeMemory(otherTask.getUsed)
        Seq(first, second).foreach(_.join())
      }
      manager.releaseMemory(10L)
      manager.releaseMemory(1L)
      assert(taskMemoryManager.getMemoryConsumptionForThisTask == 0L)
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

  for ((state, end) <- Seq[(String, TaskContextImpl => Unit)](
      ("completed", _.markTaskCompleted(None)),
      ("was killed", _.markInterrupted("killed")))) {
    test(s"an acquire that loses its task entry after the task $state is not made again") {
      val memoryManager = offHeapMemoryManager()
      val otherTask = new OffHeapConsumer(new TaskMemoryManager(memoryManager, 1L))

      withTaskContext(new TaskMemoryManager(memoryManager, 0L)) { taskMemoryManager =>
        val manager = new CometTaskMemoryManager(1L, 0L)
        assert(otherTask.acquireMemory(90L) == 90L)
        assert(manager.acquireMemory(10L) == 10L)

        val granted = new AtomicReference[java.lang.Long]()
        val failure = new AtomicReference[Throwable]()
        val acquire = new Thread(() =>
          try granted.set(manager.acquireMemory(20L))
          catch { case t: Throwable => failure.set(t) })
        acquire.setDaemon(true)
        // Spark's executor frees what the task still holds once the task has ended, under the
        // task memory manager's monitor, which a parked acquire holds.
        val cleanUp = new Thread(() => taskMemoryManager.cleanUpAllAllocatedMemory())
        cleanUp.setDaemon(true)

        try {
          acquire.start()
          awaitWaitingInSpark(acquire)
          // The task ends and releasing its plan gives back the task's last bytes while a native
          // thread still waits in Spark, as releasePlan does when it drops a plan's stream.
          end(TaskContext.get().asInstanceOf[TaskContextImpl])
          manager.releaseMemory(10L)
          acquire.join(TimeUnit.SECONDS.toMillis(TimeoutSeconds))
          // Parked again, the acquire would wait for the other task while holding the monitor.
          assert(!acquire.isAlive, s"the acquire is ${acquire.getState} after the task $state")
          cleanUp.start()
          cleanUp.join(TimeUnit.SECONDS.toMillis(TimeoutSeconds))
          assert(!cleanUp.isAlive, s"the clean up is ${cleanUp.getState}")
        } finally {
          // Free the other task's memory so that neither thread outlives a failed test.
          otherTask.freeMemory(otherTask.getUsed)
          acquire.join(TimeUnit.SECONDS.toMillis(TimeoutSeconds))
          cleanUp.join(TimeUnit.SECONDS.toMillis(TimeoutSeconds))
        }

        assert(failure.get == null, s"the acquire failed: ${failure.get}")
        assert(granted.get == 0L)
        assert(manager.getUsed == 0L)
        assert(taskMemoryManager.getMemoryConsumptionForThisTask == 0L)
      }
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

  /** Runs `body` on a daemon thread, as native code would, keeping its result or failure. */
  private class NativeThread(name: String, body: () => Long) {
    private val value = new AtomicLong(-1L)
    private val failure = new AtomicReference[Throwable]()
    val thread: Thread = new Thread(() =>
      try value.set(body())
      catch { case t: Throwable => failure.set(t) })
    thread.setDaemon(true)

    def start(): Unit = thread.start()

    def join(): Unit = thread.join(TimeUnit.SECONDS.toMillis(TimeoutSeconds))

    def result(clue: => Any = ""): Long = {
      join()
      assert(!thread.isAlive && failure.get == null, s"$this. $clue")
      value.get
    }

    override def toString: String =
      Option(failure.get).fold(s"$name is ${thread.getState}")(t => s"$name failed: $t")
  }
}
