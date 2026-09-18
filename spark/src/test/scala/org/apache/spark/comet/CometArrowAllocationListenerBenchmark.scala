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

import java.util.Properties
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import org.apache.arrow.memory.{ArrowBuf, BufferAllocator, RootAllocator}
import org.apache.spark.{SparkConf, TaskContext, TaskContextImpl}
import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.memory.{MemoryConsumer, MemoryManager, MemoryMode, TaskMemoryManager, TestMemoryManager}

/**
 * Measures what reporting JVM Arrow allocations to Spark costs, since it is on by default.
 *
 * Each case runs the same allocate/release loop twice: once against a plain `RootAllocator`,
 * which is what Comet did before [[CometArrowAllocationListener]] existed and what
 * `spark.comet.arrowAllocator.accounting.enabled=false` restores, and once against a task
 * allocator carrying the listener. Reservation call counts are printed under each table, because
 * the interesting variable is not the per-buffer bookkeeping but how often a buffer size crosses
 * a block boundary and has to go into `TaskMemoryManager` at all.
 *
 * To run this benchmark:
 * {{{
 * SPARK_GENERATE_BENCHMARK_FILES=1 make benchmark-org.apache.spark.comet.CometArrowAllocationListenerBenchmark
 * }}}
 */
object CometArrowAllocationListenerBenchmark extends BenchmarkBase {

  private val blockSize = 1024L * 1024L
  private val poolBytes = 1024L * 1024L * 1024L

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("JVM Arrow allocations reported to Spark") {
      // Many sub-block buffers, the shape a codegen output vector's validity and offset buffers
      // take. None of them comes close to a block, so the listener should never reach Spark after
      // the first one.
      allocateAndRelease("small buffers", bufferSize = 128L, buffersPerIteration = 512)
      // A wide batch: many medium buffers alive at once, one block boundary crossed per few
      // buffers on the way up and the same on the way down.
      allocateAndRelease(
        "wide batch buffers",
        bufferSize = 64L * 1024L,
        buffersPerIteration = 128)
      // Worst case for the block batching: every allocation crosses a boundary, so every one of
      // them takes the executor-wide lock in `acquireExecutionMemory`.
      allocateAndRelease("block-sized buffers", bufferSize = blockSize, buffersPerIteration = 8)
      // Same, with a second thread reserving from the same constrained pool the way Comet's native
      // side does through CometTaskMemoryManager.
      allocateUnderNativePressure()
      // Per call site rather than per buffer, but it is the cost the root allocator `val` did not
      // have: a TaskContext lookup and a concurrent map read.
      allocatorLookup()
    }
  }

  private def allocateAndRelease(
      name: String,
      bufferSize: Long,
      buffersPerIteration: Int): Unit = {
    val benchmark =
      new Benchmark(
        s"$name (${buffersPerIteration}x$bufferSize)",
        buffersPerIteration,
        output = output)

    // Both allocators are built once, outside the timed body, so what is measured is the
    // steady-state cost of allocating and releasing rather than the cost of standing a task up.
    val root = new RootAllocator(Long.MaxValue)
    try {
      withTaskAllocator() { (accounted, memory) =>
        benchmark.addCase("not accounted") { _ =>
          churn(root, bufferSize, buffersPerIteration)
        }
        benchmark.addCase("accounted") { _ =>
          churn(accounted, bufferSize, buffersPerIteration)
        }
        benchmark.run()

        // One more round with the counters zeroed, to report how often a single iteration reaches
        // the memory manager. That, rather than the per-buffer bookkeeping, is the cost that
        // scales with buffer size.
        memory.reset()
        churn(accounted, bufferSize, buffersPerIteration)
        writeLine(s"  accounted: ${memory.summary(buffersPerIteration)} per iteration")
      }
    } finally {
      root.close()
    }
  }

  private def allocateUnderNativePressure(): Unit = {
    val buffersPerIteration = 8
    val benchmark = new Benchmark(
      s"block-sized buffers under native pressure (${buffersPerIteration}x$blockSize)",
      buffersPerIteration,
      output = output)

    val root = new RootAllocator(Long.MaxValue)
    try {
      // Two blocks of pool for both arms, with the pressure thread running throughout. In the
      // unaccounted arm only that thread touches the pool, which is the point: the delta is what
      // the Arrow side adds once it competes for the same budget.
      withTaskAllocator(poolBytes = blockSize * 2) { (accounted, memory) =>
        withNativePressure(memory) {
          benchmark.addCase("not accounted") { _ =>
            churn(root, blockSize, buffersPerIteration)
          }
          benchmark.addCase("accounted") { _ =>
            churn(accounted, blockSize, buffersPerIteration)
          }
          benchmark.run()

          memory.reset()
          churn(accounted, blockSize, buffersPerIteration)
          writeLine(s"  accounted: ${memory.summary(buffersPerIteration)} per iteration")
        }
      }
    } finally {
      root.close()
    }
  }

  private def allocatorLookup(): Unit = {
    val lookupsPerIteration = 100000
    val benchmark =
      new Benchmark("allocator lookup", lookupsPerIteration.toLong, output = output)

    // The "before" shape: call sites read a package-object `val`, which the JIT folds away
    // entirely. The interesting number is therefore the absolute cost of the second case.
    benchmark.addCase("process-wide val") { _ =>
      var i = 0
      var sink = 0
      while (i < lookupsPerIteration) {
        sink += System.identityHashCode(org.apache.comet.CometArrowAllocator)
        i += 1
      }
      assert(sink != Int.MinValue)
    }
    benchmark.addCase("forCurrentTask()") { _ =>
      withTaskAllocator() { (_, _) =>
        var i = 0
        var sink = 0
        while (i < lookupsPerIteration) {
          sink += System.identityHashCode(CometTaskArrowAllocator.forCurrentTask())
          i += 1
        }
        assert(sink != Int.MinValue)
      }
    }

    benchmark.run()
  }

  /**
   * Allocates the whole set, then releases it, so the peak is what the reservation has to cover.
   */
  private def churn(allocator: BufferAllocator, bufferSize: Long, count: Int): Unit = {
    val buffers = new Array[ArrowBuf](count)
    var i = 0
    while (i < count) {
      buffers(i) = allocator.buffer(bufferSize)
      i += 1
    }
    i = 0
    while (i < count) {
      buffers(i).close()
      i += 1
    }
  }

  /**
   * Runs the body against a task allocator, torn down afterwards, so that repeated iterations do
   * not accumulate reservations or allocators.
   */
  private def withTaskAllocator[T](poolBytes: Long = poolBytes)(
      f: (BufferAllocator, CountingTaskMemoryManager) => T): T = {
    val memory = newTaskMemoryManager(poolBytes)
    val context = newTaskContext(memory)
    val previous = TaskContext.get()
    TaskContext.setTaskContext(context)
    try {
      f(CometTaskArrowAllocator.forCurrentTask(), memory)
    } finally {
      try {
        context.markTaskCompleted(None)
        memory.cleanUpAllAllocatedMemory()
      } finally {
        if (previous == null) TaskContext.unset() else TaskContext.setTaskContext(previous)
      }
    }
  }

  /** Hammers the same pool from another thread, the way native reservations do. */
  private def withNativePressure[T](memory: TaskMemoryManager)(f: => T): T = {
    val stop = new AtomicBoolean(false)
    val consumer = new NativeLikeConsumer(memory)
    val thread = new Thread(() => {
      while (!stop.get()) {
        val granted = consumer.reserve(blockSize)
        consumer.release(granted)
      }
    })
    thread.setDaemon(true)
    thread.setName("native-reservations")
    thread.start()
    try f
    finally {
      stop.set(true)
      thread.join()
    }
  }

  private val nextTaskAttemptId = new AtomicLong(1L)

  private def newTaskMemoryManager(poolBytes: Long): CountingTaskMemoryManager = {
    val conf = new SparkConf(false)
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", poolBytes.toString)
    val memoryManager = new TestMemoryManager(conf)
    memoryManager.limit(poolBytes)
    new CountingTaskMemoryManager(memoryManager, nextTaskAttemptId.getAndIncrement())
  }

  private def newTaskContext(memory: CountingTaskMemoryManager): TaskContextImpl = {
    new TaskContextImpl(
      stageId = 0,
      stageAttemptNumber = 0,
      partitionId = 0,
      numPartitions = 1,
      taskAttemptId = memory.getTaskAttemptId,
      attemptNumber = 0,
      taskMemoryManager = memory,
      localProperties = new Properties,
      metricsSystem = null,
      taskMetrics = TaskMetrics.empty,
      cpus = 1,
      resources = Map.empty)
  }

  private def writeLine(line: String): Unit = {
    // scalastyle:off println
    println(line)
    // scalastyle:on println
    output.foreach(_.write(s"$line\n".getBytes("UTF-8")))
  }

  private class CountingTaskMemoryManager(memoryManager: MemoryManager, taskAttemptId: Long)
      extends TaskMemoryManager(memoryManager, taskAttemptId) {
    private val acquires = new AtomicLong(0L)
    private val releases = new AtomicLong(0L)

    def getTaskAttemptId: Long = taskAttemptId

    // Only the Arrow listener's own calls are counted, so the pressure thread's traffic does not
    // land in the reported figure.
    override def acquireExecutionMemory(required: Long, consumer: MemoryConsumer): Long = {
      if (consumer.isInstanceOf[CometArrowAllocationListener]) acquires.incrementAndGet()
      super.acquireExecutionMemory(required, consumer)
    }

    override def releaseExecutionMemory(size: Long, consumer: MemoryConsumer): Unit = {
      if (consumer.isInstanceOf[CometArrowAllocationListener]) releases.incrementAndGet()
      super.releaseExecutionMemory(size, consumer)
    }

    def reset(): Unit = {
      acquires.set(0L)
      releases.set(0L)
    }

    def summary(buffers: Int): String =
      s"${acquires.get()} acquire and ${releases.get()} release calls for $buffers buffers"
  }

  /** Stands in for `CometTaskMemoryManager`: reserves from Spark directly and never spills. */
  private class NativeLikeConsumer(memory: TaskMemoryManager)
      extends MemoryConsumer(memory, 0L, MemoryMode.OFF_HEAP) {
    private val reserved = new AtomicLong(0L)
    override def spill(size: Long, trigger: MemoryConsumer): Long = 0L
    override def getUsed: Long = reserved.get()
    def reserve(bytes: Long): Long = {
      val granted = memory.acquireExecutionMemory(bytes, this)
      reserved.addAndGet(granted)
      granted
    }
    def release(bytes: Long): Unit = {
      if (bytes > 0L) {
        reserved.addAndGet(-bytes)
        memory.releaseExecutionMemory(bytes, this)
      }
    }
  }
}
