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

package org.apache.spark;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.memory.TaskMemoryManager;

/**
 * A adapter class that is used by Comet native to acquire & release memory through Spark's unified
 * memory manager. This assumes Spark's off-heap memory mode is enabled.
 */
public class CometTaskMemoryManager {

  private static final Logger logger = LoggerFactory.getLogger(CometTaskMemoryManager.class);

  /** The id uniquely identifies the native plan this memory manager is associated to */
  private final long id;

  private final long taskAttemptId;

  public final TaskMemoryManager internal;
  private final NativeMemoryConsumer nativeMemoryConsumer;

  /** Bytes Comet's memory pools hold from Spark through this manager, see {@link #getUsed}. */
  private final AtomicLong used = new AtomicLong();

  /**
   * Bytes held as a memory pool's anchor, see {@link #acquireAnchor}. Counted apart from {@link
   * #used}, which is what {@code CometExecIterator.close} checks for reservations that were never
   * released.
   */
  private final AtomicLong anchor = new AtomicLong();

  public CometTaskMemoryManager(long id, long taskAttemptId) {
    this.id = id;
    this.taskAttemptId = taskAttemptId;
    this.internal = TaskContext$.MODULE$.get().taskMemoryManager();
    this.nativeMemoryConsumer = new NativeMemoryConsumer();
  }

  /**
   * Bytes of the executor's off-heap memory pool in use, for execution and storage. In off-heap
   * mode this includes every reservation Comet's memory pools have acquired from Spark. Spark's
   * memory manager is private to Spark, which is why this lives here.
   */
  public static long sparkOffHeapUsed() {
    org.apache.spark.memory.MemoryManager memoryManager = SparkEnv.get().memoryManager();
    return memoryManager.offHeapExecutionMemoryUsed() + memoryManager.offHeapStorageMemoryUsed();
  }

  // Called by Comet native through JNI.
  // Returns the actual amount of memory (in bytes) granted.
  public long acquireMemory(long size) {
    if (logger.isTraceEnabled()) {
      logger.trace("Task {} requested {} bytes", taskAttemptId, size);
    }
    long acquired = internal.acquireExecutionMemory(size, nativeMemoryConsumer);
    long newUsed = used.addAndGet(acquired);
    if (acquired < size) {
      // This thread holds the short grant until native code hands it back, and another acquire
      // of this task can be waiting inside Spark for those bytes while it holds the
      // TaskMemoryManager monitor. So nothing here may take that monitor, which rules out
      // TaskMemoryManager.showMemoryUsage. getMemoryConsumptionForThisTask takes only the memory
      // manager's monitor, which a waiting acquire gives up.
      logger.warn(
          "Task {} requested {} bytes but only received {} bytes. Current allocation is {} and "
              + "the total memory consumption is {} bytes.",
          taskAttemptId,
          size,
          acquired,
          newUsed,
          internal.getMemoryConsumptionForThisTask());
    }
    return acquired;
  }

  // Called by Comet native through JNI
  public void releaseMemory(long size) {
    if (logger.isTraceEnabled()) {
      logger.trace("Task {} released {} bytes", taskAttemptId, size);
    }
    long newUsed = used.addAndGet(-size);
    if (newUsed < 0) {
      logger.error(
          "Task {} used memory is negative ({}) after releasing {} bytes",
          taskAttemptId,
          newUsed,
          size);
    }
    internal.releaseExecutionMemory(size, nativeMemoryConsumer);
  }

  // Called by Comet native through JNI.
  // Takes the fair memory pool's anchor, which the pool holds for its whole life so that the task
  // stays in Spark's active set (see the memory management guide). The bytes come out of the
  // task's balance like any other acquire but are not counted in `used`. The pool is shared by
  // every native plan of the task and is charged to the first plan's manager, so a plan closing
  // while another plan still holds the pool would otherwise report the anchor as a leak. Spark
  // declines the anchor with a zero grant when the task is at its share and the pool retries it
  // later, so a short grant is not logged here.
  public long acquireAnchor(long size) {
    long acquired = internal.acquireExecutionMemory(size, nativeMemoryConsumer);
    anchor.addAndGet(acquired);
    return acquired;
  }

  // Called by Comet native through JNI
  public void releaseAnchor(long size) {
    anchor.addAndGet(-size);
    internal.releaseExecutionMemory(size, nativeMemoryConsumer);
  }

  /**
   * Bytes Comet's memory pools hold from Spark through this manager, without any anchor. A non-zero
   * value once the plan is released is a reservation that was never freed.
   */
  public long getUsed() {
    return used.get();
  }

  /**
   * A dummy memory consumer that does nothing when spilling. At the moment, Comet native doesn't
   * share the same API as Spark and cannot trigger spill when acquire memory. Therefore, when
   * acquiring memory from native or JVM, spilling can only be triggered from JVM operators.
   */
  private class NativeMemoryConsumer extends MemoryConsumer {
    protected NativeMemoryConsumer() {
      super(CometTaskMemoryManager.this.internal, 0, MemoryMode.OFF_HEAP);
    }

    @Override
    public long spill(long size, MemoryConsumer trigger) throws IOException {
      // No spilling
      return 0;
    }

    @Override
    public long getUsed() {
      // Native allocations call TaskMemoryManager directly, bypassing MemoryConsumer.used. The
      // anchor is included so that Spark's view of this consumer matches the task's balance.
      return CometTaskMemoryManager.this.used.get() + anchor.get();
    }

    @Override
    public String toString() {
      return String.format("NativeMemoryConsumer(id=%d)", id);
    }
  }
}
