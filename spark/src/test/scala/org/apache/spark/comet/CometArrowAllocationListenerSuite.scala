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

package org.apache.spark.comet

import java.io.{InterruptedIOException, IOException}
import java.util.Properties
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

import org.scalatest.funsuite.AnyFunSuite

import org.apache.arrow.memory.BufferAllocator
import org.apache.spark.{SparkConf, TaskContext, TaskContextImpl}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.memory.{MemoryConsumer, MemoryManager, MemoryMode, SparkOutOfMemoryError, TaskMemoryManager, TestMemoryManager}

import org.apache.comet.CometArrowAllocator

/**
 * Tests that JVM Arrow allocations are reported to Spark, that they are reported against the task
 * that made them rather than whichever task happens to be on the releasing thread, and, just as
 * importantly, that the paths where they cannot be reported fail quietly rather than throwing.
 * Arrow allocation on these paths cannot fail today and this listener must not change that.
 */
class CometArrowAllocationListenerSuite extends AnyFunSuite {

  private val blockSize = CometArrowAllocationListener.BLOCK_SIZE
  private val poolBytes = 64L * 1024 * 1024

  /** Task attempt ids are keys in a process-wide map, so no two tests may share one. */
  private val nextTaskAttemptId = new AtomicLong(1000L)

  // ---------------------------------------------------------------------------------------------
  // Reservation arithmetic. Driven through the listener directly, since Arrow's rounding policy
  // would otherwise decide the sizes under test.
  // ---------------------------------------------------------------------------------------------

  test("allocations are charged to the current task in whole blocks") {
    withTask() { task =>
      val listener = new CometArrowAllocationListener(task.taskMemoryManager)

      // Far smaller than a block, so the reservation should round up to exactly one block.
      listener.onAllocation(128L)
      assert(listener.reservedBytes == blockSize)

      // Still inside the first block, so Spark is not asked again.
      listener.onAllocation(1024L)
      assert(listener.reservedBytes == blockSize)

      listener.taskCompleted()
    }
  }

  test("a request larger than a block rounds up to a block multiple") {
    withTask() { task =>
      val listener = new CometArrowAllocationListener(task.taskMemoryManager)
      listener.onAllocation(blockSize * 3 + 7L)
      // Rounded up rather than sized to the exact deficit, so growth leaves headroom and the next
      // small allocation does not go straight back into Spark.
      assert(listener.reservedBytes == blockSize * 4)
      listener.taskCompleted()
    }
  }

  test("releasing returns whole blocks to Spark") {
    withTask() { task =>
      val listener = new CometArrowAllocationListener(task.taskMemoryManager)
      listener.onAllocation(blockSize * 2)
      assert(listener.reservedBytes == blockSize * 2)

      listener.onRelease(blockSize * 2)
      assert(listener.reservedBytes == 0L)
      listener.taskCompleted()
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Which allocator is handed out, and what it charges.
  // ---------------------------------------------------------------------------------------------

  test("a real Arrow allocation from the task allocator is charged to that task") {
    withTask() { task =>
      val allocator = CometTaskArrowAllocator.forCurrentTask()
      assert(allocator ne CometArrowAllocator)
      val buf = allocator.buffer(blockSize)
      try {
        assert(reservedFor(task) == blockSize)
        assert(task.taskMemoryManager.getMemoryConsumptionForThisTask == blockSize)
      } finally {
        buf.close()
      }
      assert(reservedFor(task) == 0L)
    }
  }

  test("a child of the task allocator is charged to the same task") {
    withTask() { task =>
      // The Python runner and the native Arrow source cut their own children. Arrow passes the
      // parent's listener down, so they are accounted without knowing anything about it.
      val child =
        CometTaskArrowAllocator.forCurrentTask().newChildAllocator("probe", 0L, Long.MaxValue)
      try {
        val buf = child.buffer(blockSize)
        try {
          assert(reservedFor(task) == blockSize)
        } finally {
          buf.close()
        }
      } finally {
        child.close()
      }
      assert(reservedFor(task) == 0L)
    }
  }

  test("the process-wide root is not accounted, which is what the FFI paths rely on") {
    withTask() { task =>
      // Establish the task allocator first, so this asserts "not charged" rather than "no task".
      CometTaskArrowAllocator.forCurrentTask()
      // Both directions across the C Data Interface use the listener-less root: imported buffers
      // wrap memory the native side owns, and buffers allocated for export are reserved again by
      // whichever native operator retains the batch, through a pool that charges the same task.
      val buf = CometArrowAllocator.buffer(blockSize)
      try {
        assert(reservedFor(task) == 0L)
      } finally {
        buf.close()
      }
    }
  }

  test("no active task uses the unaccounted root allocator") {
    TaskContext.unset()
    // Broadcast coalescing and the cached batch serializer can allocate off a task thread.
    assert(CometTaskArrowAllocator.forCurrentTask() eq CometArrowAllocator)
  }

  test("on-heap mode uses the unaccounted root allocator") {
    withTask(offHeap = false) { task =>
      // Comet's on-heap mode exists so the Spark SQL suite can run without off-heap memory.
      // Charging an off-heap consumer there would be wrong.
      assert(CometTaskArrowAllocator.forCurrentTask() eq CometArrowAllocator)
      assert(CometTaskArrowAllocator.listenerForTask(task.taskAttemptId).isEmpty)
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Ownership: a release is attributed to the task that allocated, not to the releasing thread.
  // ---------------------------------------------------------------------------------------------

  test("a release on a thread with no task context is charged to the allocating task") {
    withTask() { task =>
      val buf = CometTaskArrowAllocator.forCurrentTask().buffer(blockSize)
      assert(reservedFor(task) == blockSize)

      // This is what happens when a shuffle-read batch is handed on to a native operator: native
      // pins it and drops it later from a Tokio worker with no task context installed. Reading
      // TaskContext in onRelease would ignore this release and leave the task charged for memory
      // it had already freed.
      onDetachedThread(buf.close())

      assert(reservedFor(task) == 0L)
      assert(task.taskMemoryManager.getMemoryConsumptionForThisTask == 0L)
    }
  }

  test("a release under a different task does not touch that task's accounting") {
    withTask() { taskA =>
      val bufA = CometTaskArrowAllocator.forCurrentTask().buffer(blockSize)
      withTask() { taskB =>
        val bufB = CometTaskArrowAllocator.forCurrentTask().buffer(blockSize)
        assert(reservedFor(taskB) == blockSize)

        // A's buffer, released while B's context is on the thread. B must not pay for it.
        bufA.close()

        assert(reservedFor(taskB) == blockSize)
        assert(reservedFor(taskA) == 0L)
        bufB.close()
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Task completion, and buffers that outlive their task.
  // ---------------------------------------------------------------------------------------------

  test("task completion releases the whole reservation and closes the allocator") {
    val task = newTask()
    val allocatorName = withInstalledTask(task) {
      val allocator = CometTaskArrowAllocator.forCurrentTask()
      val buf = allocator.buffer(blockSize)
      assert(task.taskMemoryManager.getMemoryConsumptionForThisTask == blockSize)
      buf.close()

      task.context.markTaskCompleted(None)

      assert(CometTaskArrowAllocator.listenerForTask(task.taskAttemptId).isEmpty)
      assert(task.taskMemoryManager.getMemoryConsumptionForThisTask == 0L)
      allocator.getName
    }
    assert(!rootChildNames().contains(allocatorName))
  }

  test("a buffer outliving its task parks the allocator until it is released") {
    val task = newTask()
    withInstalledTask(task) {
      val allocator = CometTaskArrowAllocator.forCurrentTask()
      val buf = allocator.buffer(blockSize)

      task.context.markTaskCompleted(None)

      // The reservation goes back to Spark even though the buffer is still alive: the task is
      // over, and leaving it charged would be reported as a Spark memory leak.
      assert(task.taskMemoryManager.getMemoryConsumptionForThisTask == 0L)
      assert(CometTaskArrowAllocator.listenerForTask(task.taskAttemptId).isEmpty)
      // Arrow treats closing an allocator that still owns bytes as a leak, so it has to stay open.
      assert(rootChildNames().contains(allocator.getName))

      // A late release is ignored rather than charged to whoever is running by then...
      buf.close()
      assert(task.taskMemoryManager.getMemoryConsumptionForThisTask == 0L)

      // ...and the drained allocator is reaped by the next task, so the root does not accumulate
      // one child per task attempt.
      withTask() { _ => CometTaskArrowAllocator.forCurrentTask() }
      assert(!rootChildNames().contains(allocator.getName))
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Failure containment: Spark's acquisition is fallible, Arrow's callbacks are not allowed to be.
  // ---------------------------------------------------------------------------------------------

  for ((label, failure) <- Seq(
      "an I/O failure" -> new IOException("spill failed"),
      "an interrupt" -> new InterruptedIOException("task killed"))) {
    test(s"$label while spilling does not fail or leak the Arrow allocation") {
      // Exactly one block of budget, already taken by a consumer that refuses to spill, so the
      // listener's acquisition has to go through Spark's spill path and comes back throwing.
      withTask(pool = blockSize) { task =>
        val hostile = new FailingSpillConsumer(task.taskMemoryManager, failure)
        assert(hostile.take(blockSize) == blockSize)

        val allocator = CometTaskArrowAllocator.forCurrentTask()
        // `BaseAllocator.buffer` marks the allocation successful before calling `onAllocation`, so
        // throwing from the listener would lose this buffer: Arrow neither returns nor frees it.
        val buf = allocator.buffer(blockSize)
        try {
          assert(allocator.getAllocatedMemory == blockSize)
          // Nothing was reserved, which is what says the acquisition really did go down the spill
          // path and throw rather than quietly succeeding and making this test vacuous.
          assert(reservedFor(task) == 0L)
        } finally {
          buf.close()
        }
        // Zero here is the leak check: a buffer Arrow created but never handed back would still
        // be counted.
        assert(allocator.getAllocatedMemory == 0L)
      }
    }
  }

  test("an interrupt while spilling is re-armed rather than thrown or swallowed") {
    // Spark's execution pool parks in `lock.wait()` when a task is below its fair share, so a task
    // kill raises a plain InterruptedException out of `acquireExecutionMemory`. `NonFatal` excludes
    // it, so before this it escaped `onAllocation` and Arrow lost the buffer it had just created.
    // TestMemoryManager never parks, so the interrupt is injected through a failing spill instead.
    withTask(pool = blockSize) { task =>
      val hostile =
        new FailingSpillConsumer(task.taskMemoryManager, new InterruptedException("task killed"))
      assert(hostile.take(blockSize) == blockSize)

      val allocator = CometTaskArrowAllocator.forCurrentTask()
      val buf = allocator.buffer(blockSize)
      try {
        assert(allocator.getAllocatedMemory == blockSize)
        assert(reservedFor(task) == 0L)
      } finally {
        buf.close()
      }
      assert(allocator.getAllocatedMemory == 0L)
      // Cleared here as well as asserted, so the flag does not leak into the next test.
      assert(Thread.interrupted(), "the interrupt was swallowed instead of being re-armed")
    }
  }

  test("a partial grant lost to a failing spill is adopted rather than stranded") {
    // One block already taken, one still in the pool, and a two-block request: Spark hands over the
    // block it has and only then asks the other consumer to spill, which throws. It never reports
    // the block it already took, so nothing would release it before the task ended.
    withTask(pool = blockSize * 2) { task =>
      val hostile =
        new FailingSpillConsumer(task.taskMemoryManager, new IOException("spill failed"))
      assert(hostile.take(blockSize) == blockSize)

      val allocator = CometTaskArrowAllocator.forCurrentTask()
      val buf = allocator.buffer(blockSize * 2)
      try {
        assert(reservedFor(task) == blockSize)
        assert(task.taskMemoryManager.getMemoryConsumptionForThisTask == blockSize * 2)
      } finally {
        buf.close()
      }
      assert(reservedFor(task) == 0L)
      // Only the other consumer's block is left. Without adopting the orphan this would still be
      // two blocks, with one of them charged to the task and owned by nobody.
      assert(task.taskMemoryManager.getMemoryConsumptionForThisTask == blockSize)
    }
  }

  test("a concurrent acquisition is not adopted as this listener's lost grant") {
    // The orphan is measured as a change in the task's total consumption, which Spark reports per
    // task rather than per consumer. If another consumer could acquire between the snapshot taken
    // before the acquisition and the acquisition itself, its bytes would be adopted here and handed
    // back when this listener next shrank, leaving two consumers holding the same bytes between
    // them. Both snapshots and the call therefore run as one transaction under the
    // TaskMemoryManager monitor. This forces the interleaving that transaction exists to exclude.
    val spare = 1024L
    val task = newTask(pool = blockSize * 2 + spare)
    withInstalledTask(task) {
      val hostile =
        new FailingSpillConsumer(task.taskMemoryManager, new IOException("spill failed"))
      assert(hostile.take(blockSize) == blockSize)

      val interloper = new PlainConsumer(task.taskMemoryManager)
      // Once the acquisition has taken what was left, the pool is empty and Spark answers the
      // interloper by asking the hostile consumer to spill, which throws. That is a legitimate
      // outcome for the interloper and not what is under test here; what it ends up holding is.
      val interloperThread = daemonThread("interloper") {
        try interloper.take(spare)
        catch { case _: SparkOutOfMemoryError => }
      }

      // Fires once, in place of the snapshot taken before the acquisition: exactly the window the
      // transaction has to close. The interloper either runs to completion here, which is the bug,
      // or blocks on the monitor the acquisition is holding, which is the fix.
      task.snapshotHook.set(() => {
        interloperThread.start()
        awaitBlockedOrFinished(interloperThread)
      })

      val allocator = CometTaskArrowAllocator.forCurrentTask()
      val buf = allocator.buffer(blockSize * 2)
      try {
        interloperThread.join(30000L)
        assert(
          !interloperThread.isAlive,
          "the interloper never finished; the transaction deadlocked")
        // Nothing is claimed twice: what the listener adopted has to fit alongside what the other
        // two consumers hold. Without the transaction the listener adopts the interloper's bytes on
        // top of its own grant, and this sum comes out over what the task actually holds.
        assert(
          listenerFor(task).reservedBytes + hostile.getUsed + interloper.getUsed ==
            task.taskMemoryManager.getMemoryConsumptionForThisTask,
          "the listener adopted bytes belonging to another consumer")
      } finally {
        buf.close()
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Lock order. Spark calls getUsed and spill while holding the TaskMemoryManager monitor, and the
  // listener holds its own monitor while waiting for that one.
  // ---------------------------------------------------------------------------------------------

  test("the usage snapshot does not take the reservation monitor") {
    withTask() { task =>
      val buf = CometTaskArrowAllocator.forCurrentTask().buffer(blockSize)
      try {
        val listener = CometTaskArrowAllocator.listenerForTask(task.taskAttemptId).get
        val used = new AtomicLong(-1L)
        val spilled = new AtomicLong(-1L)
        listener.synchronized {
          // A native reservation arriving through CometTaskMemoryManager on a Tokio thread holds
          // Spark's monitor here. If either call waited on this one, it would deadlock against an
          // Arrow allocation on the same task that already holds this monitor and wants Spark's.
          val probe = new Thread(() => {
            used.set(listener.getUsed)
            spilled.set(listener.spill(blockSize, listener))
          })
          probe.setDaemon(true)
          probe.setName("lock-order-probe")
          probe.start()
          probe.join(30000L)
          assert(!probe.isAlive, "getUsed or spill blocked on the reservation monitor")
        }
        assert(used.get == blockSize)
        assert(spilled.get == 0L)
      } finally {
        buf.close()
      }
    }
  }

  test("concurrent Arrow and native reservations make progress") {
    // Two blocks of budget shared by both consumers, so most requests are short and Spark walks
    // its consumer list, calling getUsed on the Arrow listener while holding its own monitor.
    withTask(pool = blockSize * 2) { task =>
      val allocator = CometTaskArrowAllocator.forCurrentTask()
      val native = new NativeLikeConsumer(task.taskMemoryManager)
      val failure = new AtomicReference[Throwable]()

      val arrowThread = loopingThread("arrow-allocations", failure) {
        val buf = allocator.buffer(blockSize)
        buf.close()
      }
      val nativeThread = loopingThread("native-reservations", failure) {
        native.release(native.reserve(blockSize))
      }

      Seq(arrowThread, nativeThread).foreach(_.start())
      Seq(arrowThread, nativeThread).foreach { t =>
        t.join(60000L)
        assert(!t.isAlive, s"${t.getName} did not finish; concurrent reservations deadlocked")
      }

      Option(failure.get).foreach(e => fail("a worker failed", e))
      assert(allocator.getAllocatedMemory == 0L)
      native.release(native.used())
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Fixtures.
  // ---------------------------------------------------------------------------------------------

  /** Holds memory and refuses to give it back, so `trySpillAndAcquire` throws on its behalf. */
  private class FailingSpillConsumer(tmm: TaskMemoryManager, failure: Exception)
      extends MemoryConsumer(tmm, 0L, MemoryMode.OFF_HEAP) {
    def take(bytes: Long): Long = acquireMemory(bytes)
    override def spill(size: Long, trigger: MemoryConsumer): Long = throw failure
  }

  /** Any other consumer in the task: takes memory, and cannot give it back. */
  private class PlainConsumer(tmm: TaskMemoryManager)
      extends MemoryConsumer(tmm, 0L, MemoryMode.OFF_HEAP) {
    def take(bytes: Long): Long = acquireMemory(bytes)
    override def spill(size: Long, trigger: MemoryConsumer): Long = 0L
  }

  /**
   * Runs one action immediately after the usage snapshot the listener takes before asking Spark
   * for memory, so a test can drive what happens in the window between that snapshot and the
   * acquisition. The real value is read first, which is what makes the window the one under test:
   * running the action before the read would fold whatever it does into the snapshot itself.
   */
  private class HookedTaskMemoryManager(
      memoryManager: MemoryManager,
      taskAttemptId: Long,
      hook: AtomicReference[Runnable])
      extends TaskMemoryManager(memoryManager, taskAttemptId) {
    override def getMemoryConsumptionForThisTask(): Long = {
      val held = super.getMemoryConsumptionForThisTask()
      val pending = hook.getAndSet(null)
      if (pending != null) pending.run()
      held
    }
  }

  /** Stands in for `CometTaskMemoryManager`: reserves from Spark directly and never spills. */
  private class NativeLikeConsumer(tmm: TaskMemoryManager)
      extends MemoryConsumer(tmm, 0L, MemoryMode.OFF_HEAP) {
    private val reserved = new AtomicLong(0L)
    override def spill(size: Long, trigger: MemoryConsumer): Long = 0L
    override def getUsed: Long = reserved.get()
    def used(): Long = reserved.get()
    def reserve(bytes: Long): Long = {
      val granted = tmm.acquireExecutionMemory(bytes, this)
      reserved.addAndGet(granted)
      granted
    }
    def release(bytes: Long): Unit = {
      if (bytes > 0L) {
        reserved.addAndGet(-bytes)
        tmm.releaseExecutionMemory(bytes, this)
      }
    }
  }

  private case class TaskFixture(
      taskAttemptId: Long,
      context: TaskContextImpl,
      taskMemoryManager: TaskMemoryManager,
      snapshotHook: AtomicReference[Runnable])

  private def reservedFor(task: TaskFixture): Long =
    CometTaskArrowAllocator.reservedBytesForTask(task.taskAttemptId)

  private def listenerFor(task: TaskFixture): CometArrowAllocationListener =
    CometTaskArrowAllocator.listenerForTask(task.taskAttemptId).get

  private def newTask(offHeap: Boolean = true, pool: Long = poolBytes): TaskFixture = {
    val conf = new SparkConf(false)
    if (offHeap) {
      conf
        .set("spark.memory.offHeap.enabled", "true")
        .set("spark.memory.offHeap.size", pool.toString)
    }
    val memoryManager = new TestMemoryManager(conf)
    memoryManager.limit(pool)
    val taskAttemptId = nextTaskAttemptId.getAndIncrement()
    val snapshotHook = new AtomicReference[Runnable]()
    val taskMemoryManager =
      new HookedTaskMemoryManager(memoryManager, taskAttemptId, snapshotHook)
    val context = new TaskContextImpl(
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
    TaskFixture(taskAttemptId, context, taskMemoryManager, snapshotHook)
  }

  /** Installs the task on this thread, restoring whatever was there before. */
  private def withInstalledTask[T](task: TaskFixture)(f: => T): T = {
    val previous = TaskContext.get()
    TaskContext.setTaskContext(task.context)
    try {
      f
    } finally {
      try {
        // Fires the completion listener that drops the reservation; harmless if already run.
        task.context.markTaskCompleted(None)
        task.taskMemoryManager.cleanUpAllAllocatedMemory()
      } finally {
        if (previous == null) TaskContext.unset() else TaskContext.setTaskContext(previous)
      }
    }
  }

  private def withTask(offHeap: Boolean = true, pool: Long = poolBytes)(
      f: TaskFixture => Unit): Unit = {
    val task = newTask(offHeap, pool)
    withInstalledTask(task)(f(task))
  }

  /** Runs the body on a fresh thread, which by construction carries no task context. */
  private def onDetachedThread(body: => Unit): Unit = {
    val failure = new AtomicReference[Throwable]()
    val thread = new Thread(() => {
      try body
      catch { case t: Throwable => failure.set(t) }
    })
    thread.setDaemon(true)
    thread.setName("detached-release")
    thread.start()
    thread.join(30000L)
    assert(!thread.isAlive, "the detached release did not finish")
    Option(failure.get).foreach(t => throw t)
  }

  /** An unstarted daemon thread, so a test can choose the moment it runs. */
  private def daemonThread(name: String)(body: => Unit): Thread = {
    val thread = new Thread(() => body)
    thread.setDaemon(true)
    thread.setName(name)
    thread
  }

  /**
   * Waits until the thread is either blocked on a monitor or finished, whichever happens first,
   * so that a test can tell the two orderings apart without depending on timing.
   */
  private def awaitBlockedOrFinished(thread: Thread): Unit = {
    val deadline = System.currentTimeMillis() + 30000L
    var state = thread.getState
    while (state != Thread.State.BLOCKED && state != Thread.State.TERMINATED &&
      System.currentTimeMillis() < deadline) {
      Thread.sleep(1L)
      state = thread.getState
    }
  }

  private def loopingThread(name: String, failure: AtomicReference[Throwable])(
      body: => Unit): Thread = {
    val thread = new Thread(() => {
      try {
        var i = 0
        while (i < 500) {
          body
          i += 1
        }
      } catch {
        case t: Throwable => failure.compareAndSet(null, t)
      }
    })
    thread.setDaemon(true)
    thread.setName(name)
    thread
  }

  private def rootChildNames(): Set[String] = {
    val names = Set.newBuilder[String]
    val children = CometArrowAllocator.getChildAllocators.iterator()
    while (children.hasNext) {
      names += children.next().asInstanceOf[BufferAllocator].getName
    }
    names.result()
  }
}
