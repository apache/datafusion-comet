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

import org.apache.comet.Native;
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
  private final AtomicLong used = new AtomicLong();

  /**
   * The native ExecutionContext handle. Set after native plan creation. Used to route spill()
   * requests to the native memory pool.
   */
  private volatile long nativePlanHandle = 0;

  public CometTaskMemoryManager(long id, long taskAttemptId) {
    this.id = id;
    this.taskAttemptId = taskAttemptId;
    this.internal = TaskContext$.MODULE$.get().taskMemoryManager();
    this.nativeMemoryConsumer = new NativeMemoryConsumer();
  }

  /**
   * Set the native plan handle after plan creation. This enables the spill() callback to route
   * requests to the native memory pool.
   */
  public void setNativePlanHandle(long handle) {
    this.nativePlanHandle = handle;
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
      logger.warn(
          "Task {} requested {} bytes but only received {} bytes. Current allocation is {} and "
              + "the total memory consumption is {} bytes.",
          taskAttemptId,
          size,
          acquired,
          newUsed,
          internal.getMemoryConsumptionForThisTask());
      // If memory manager is not able to acquire the requested size, log memory usage
      internal.showMemoryUsage();
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

  public long getUsed() {
    return used.get();
  }

  /**
   * A memory consumer that routes spill requests to the native memory pool. When Spark's memory
   * manager needs to reclaim memory (e.g., for another task), it calls spill() which signals the
   * native pool to apply backpressure. DataFusion operators (Sort, Aggregate, Shuffle) react by
   * spilling their internal state to disk.
   */
  private class NativeMemoryConsumer extends MemoryConsumer {
    protected NativeMemoryConsumer() {
      super(CometTaskMemoryManager.this.internal, 0, MemoryMode.OFF_HEAP);
    }

    @Override
    public long spill(long size, MemoryConsumer trigger) throws IOException {
      long handle = nativePlanHandle;
      if (handle == 0) {
        // Native plan not yet created or already destroyed
        return 0;
      }
      logger.info(
          "Task {} received spill request for {} bytes, forwarding to native",
          taskAttemptId,
          size);
      try {
        long freed = new Native().requestSpill(handle, size);
        logger.info("Task {} native spill freed {} bytes", taskAttemptId, freed);
        return freed;
      } catch (Exception e) {
        logger.warn("Task {} native spill failed: {}", taskAttemptId, e.getMessage());
        return 0;
      }
    }

    @Override
    public String toString() {
      return String.format(
          "NativeMemoryConsumer(id=%d, taskAttemptId=%d)", id, taskAttemptId);
    }
  }
}
