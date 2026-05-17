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

package org.apache.comet.udf;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.arrow.memory.AllocationListener;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.spark.TaskContext;
import org.apache.spark.comet.CometTaskContextShim;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.util.TaskCompletionListener;

/**
 * Provides a per-Spark-task Arrow {@link BufferAllocator} whose allocations are accounted to the
 * task's {@link TaskMemoryManager} via a {@link MemoryConsumer}.
 *
 * <p>One child allocator is created on the first {@link #acquire(TaskContext)} call within a Spark
 * task and cached for the lifetime of that task. A task-completion listener closes the child
 * allocator and removes the cache entry, so allocations and frees stay balanced with the task
 * lifecycle. Subsequent UDF dispatches within the same task reuse the cached allocator without
 * additional accounting overhead.
 *
 * <p>The allocator is a child of the project-wide {@code CometArrowAllocator}, with a custom {@link
 * AllocationListener} attached. The listener forwards pre-allocation to {@code
 * TaskMemoryManager.acquireExecutionMemory} and on-release to {@code releaseExecutionMemory}. A
 * partial acquisition is released and the allocation aborted via {@link OutOfMemoryError}; this is
 * the standard Arrow + Spark integration pattern.
 */
public final class CometUdfAllocator {

  private static final Logger LOG = LoggerFactory.getLogger(CometUdfAllocator.class);

  /** Keyed by {@code TaskContext.taskAttemptId()}. */
  private static final ConcurrentHashMap<Long, BufferAllocator> CACHE = new ConcurrentHashMap<>();

  private CometUdfAllocator() {}

  /**
   * Returns the per-task child allocator, creating it on first call. Safe to call from multiple
   * worker threads within the same Spark task.
   */
  public static BufferAllocator acquire(TaskContext taskContext) {
    long key = taskContext.taskAttemptId();
    return CACHE.computeIfAbsent(key, k -> create(taskContext));
  }

  /** Visible for testing. */
  static int cacheSize() {
    return CACHE.size();
  }

  private static BufferAllocator create(TaskContext taskContext) {
    TaskMemoryManager tmm = CometTaskContextShim.taskMemoryManager(taskContext);
    TaskMemoryConsumer consumer = new TaskMemoryConsumer(tmm);
    AllocationListener listener = new TaskAllocationListener(tmm, consumer);
    BufferAllocator child =
        org.apache.comet.package$.MODULE$.CometArrowAllocator()
            .newChildAllocator(
                "comet-udf-task-" + taskContext.taskAttemptId(), listener, 0L, Long.MAX_VALUE);
    long key = taskContext.taskAttemptId();
    try {
      taskContext.addTaskCompletionListener(
          (TaskCompletionListener) ctx -> closeAndRemove(key, child));
    } catch (RuntimeException e) {
      try {
        child.close();
      } catch (RuntimeException ignored) {
        // do not mask the original failure
      }
      throw e;
    }
    return child;
  }

  private static void closeAndRemove(long key, BufferAllocator child) {
    CACHE.remove(key);
    try {
      child.close();
    } catch (RuntimeException e) {
      // Arrow throws IllegalStateException if any buffers leaked. Log and swallow so we do not
      // mask other task-completion errors.
      LOG.warn(
          "Comet UDF child allocator for taskAttemptId={} closed with leaked allocations", key, e);
    }
  }

  /** Delegates Arrow allocation events to a Spark {@link MemoryConsumer}. */
  private static final class TaskAllocationListener implements AllocationListener {
    private final TaskMemoryManager tmm;
    private final TaskMemoryConsumer consumer;

    TaskAllocationListener(TaskMemoryManager tmm, TaskMemoryConsumer consumer) {
      this.tmm = tmm;
      this.consumer = consumer;
    }

    @Override
    public void onPreAllocation(long size) {
      long acquired = tmm.acquireExecutionMemory(size, consumer);
      if (acquired < size) {
        if (acquired > 0) {
          tmm.releaseExecutionMemory(acquired, consumer);
        }
        throw new OutOfMemoryError(
            "Comet UDF: failed to acquire " + size + " bytes from Spark TaskMemoryManager");
      }
    }

    @Override
    public void onRelease(long size) {
      tmm.releaseExecutionMemory(size, consumer);
    }
  }

  /** Non-spillable off-heap consumer for Arrow buffers exported via FFI. */
  private static final class TaskMemoryConsumer extends MemoryConsumer {
    TaskMemoryConsumer(TaskMemoryManager tmm) {
      // pageSize = 0: this consumer never uses allocatePage(); only acquire/releaseExecutionMemory.
      // Matches the existing NativeMemoryConsumer pattern in CometTaskMemoryManager.
      super(tmm, 0L, MemoryMode.OFF_HEAP);
    }

    @Override
    public long spill(long size, MemoryConsumer trigger) throws IOException {
      // UDF result buffers are pinned for the duration of the call and exported via Arrow FFI;
      // they cannot be spilled.
      return 0L;
    }
  }
}
