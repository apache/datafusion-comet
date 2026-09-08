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

package org.apache.spark.shuffle.comet;

import java.io.IOException;
import java.util.BitSet;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.memory.SparkOutOfMemoryError;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.unsafe.memory.UnsafeMemoryAllocator;

import org.apache.comet.CometConf$;
import org.apache.comet.CometSparkSessionExtensions$;

/**
 * A simple memory allocator used by `CometShuffleExternalSorter` to allocate memory blocks which
 * store serialized rows. We don't rely on Spark memory allocator because we need to allocate
 * off-heap memory no matter memory mode is on-heap or off-heap. This allocator is configured with
 * fixed size of memory, and it will throw `SparkOutOfMemoryError` if the memory is not enough.
 *
 * <p>Some methods are copied from `org.apache.spark.unsafe.memory.TaskMemoryManager` with
 * modifications. Most modifications are to remove the dependency on the configured memory mode.
 *
 * <p>This allocator is only used by Comet Columnar Shuffle when running in on-heap mode. It is used
 * when users run in on-heap mode as well as in the Spark tests which require on-heap memory
 * configuration.
 *
 * <p>Thus, this allocator is used to allocate separate off-heap memory allocation for Comet
 * Columnar Shuffle and execution apart from Spark's on-heap memory configuration.
 */
public final class CometBoundedShuffleMemoryAllocator extends CometShuffleMemoryAllocatorTrait {
  private static final Logger logger =
      LoggerFactory.getLogger(CometBoundedShuffleMemoryAllocator.class);

  private final UnsafeMemoryAllocator allocator = new UnsafeMemoryAllocator();

  private final long pageSize;
  private final long totalMemory;
  private long allocatedMemory = 0L;

  /** How often a thread blocked in {@link #allocateBlocking(long)} logs that it is waiting. */
  private static final long WAIT_LOG_INTERVAL_MS = 30_000L;

  /** How often a blocked thread checks for cooperative task cancellation. */
  private static final long TASK_KILL_POLL_INTERVAL_MS = 1_000L;

  /** The number of bits used to address the page table. */
  private static final int PAGE_NUMBER_BITS = 13;

  /** The number of entries in the page table. */
  private static final int PAGE_TABLE_SIZE = 1 << PAGE_NUMBER_BITS;

  private final MemoryBlock[] pageTable = new MemoryBlock[PAGE_TABLE_SIZE];
  private final BitSet allocatedPages = new BitSet(PAGE_TABLE_SIZE);

  /** The thread that allocated each page, used to decide whether a blocked wait can succeed. */
  private final Thread[] pageOwners = new Thread[PAGE_TABLE_SIZE];

  /** Pool memory currently retained by each thread. */
  private final HashMap<Thread, Long> retainedMemory = new HashMap<>();

  /** Threads currently blocked in {@link #allocateBlocking(long)} and their request sizes. */
  private final HashMap<Thread, Long> waitingThreads = new HashMap<>();

  private static final int OFFSET_BITS = 51;
  private static final long MASK_LONG_LOWER_51_BITS = 0x7FFFFFFFFFFFFL;

  CometBoundedShuffleMemoryAllocator(
      SparkConf conf, TaskMemoryManager taskMemoryManager, long pageSize) {
    super(taskMemoryManager, pageSize, MemoryMode.OFF_HEAP);
    this.pageSize = pageSize;
    this.totalMemory =
        CometSparkSessionExtensions$.MODULE$.getCometShuffleMemorySize(conf, SQLConf.get());
  }

  /**
   * Returns the current allocation total in bytes.
   *
   * <p>Allocations bypass Spark's memory manager and use this allocator's own counter. Since the
   * allocator is shared across tasks, this reports the shared total rather than per-task usage.
   */
  @Override
  public synchronized long getUsed() {
    return allocatedMemory;
  }

  private synchronized long _acquireMemory(long size) {
    if (allocatedMemory >= totalMemory) {
      throw new SparkOutOfMemoryError(
          "UNABLE_TO_ACQUIRE_MEMORY",
          java.util.Map.of(
              "requestedBytes", String.valueOf(size),
              "receivedBytes", String.valueOf(totalMemory - allocatedMemory)));
    }
    long allocationSize = Math.min(size, totalMemory - allocatedMemory);
    allocatedMemory += allocationSize;
    return allocationSize;
  }

  public long spill(long l, MemoryConsumer memoryConsumer) throws IOException {
    return 0;
  }

  public synchronized LongArray allocateArray(long size) {
    long required = size * 8L;
    MemoryBlock page = allocateMemoryBlock(required);
    return new LongArray(page);
  }

  public synchronized void freeArray(LongArray array) {
    if (array == null) {
      return;
    }
    free(array.memoryBlock());
  }

  public synchronized MemoryBlock allocate(long required) {
    long size = Math.max(pageSize, required);
    return allocateMemoryBlock(size);
  }

  /**
   * Like {@link #allocate(long)}, but waits for other tasks of this shared pool to free memory,
   * mirroring how Spark's unified memory manager blocks a task until memory becomes available.
   * Callers must first spill buffered data they can cheaply release; memory this thread still
   * retains (e.g. the sorter's pointer array or sibling writers' pages) is included in the liveness
   * checks below. The wait fails fast when it can never succeed: when the request does not fit next
   * to the requester's retained memory, or when all allocated memory is retained by blocked threads
   * and none of their requests fits in the free pool. Because the holders it depends on may in turn
   * be blocked on resources outside this pool that only a task waiting here can release, the wait
   * is also bounded by `spark.comet.shuffle.jvm.memoryWaitTimeout`, after which the managed
   * allocation error is thrown and Spark's task retry can recover. Task cancellation or Java
   * interruption aborts the wait.
   */
  @Override
  public synchronized MemoryBlock allocateBlocking(long required) {
    long memoryWaitTimeoutMs =
        (long) CometConf$.MODULE$.COMET_SHUFFLE_JVM_MEMORY_WAIT_TIMEOUT().get();
    long size = Math.max(pageSize, required);
    Thread self = Thread.currentThread();
    TaskContext taskContext = TaskContext.get();
    long waitStart = 0;
    long lastLog = 0;
    try {
      while (true) {
        if (taskContext != null) {
          taskContext.killTaskIfInterrupted();
        }
        try {
          return allocateMemoryBlock(size);
        } catch (SparkOutOfMemoryError e) {
          if (waitingThreads.put(self, size) == null) {
            // Wake existing waiters so they re-evaluate the deadlock check against the enlarged
            // waiting set.
            notifyAll();
          }
          // This thread cannot free what it retains while it waits, so a request that does not
          // fit next to its own retained memory can never be satisfied.
          if (size > totalMemory - retainedMemory.getOrDefault(self, 0L)) {
            throw e;
          }
          // The allocation just failed, so the request does not fit in the unallocated pool.
          // Waiting can only succeed while some thread can still free memory: either a thread
          // outside the waiting set retains pool memory, or another waiter's request fits in the
          // free pool, in which case that waiter can proceed and eventually free what it retains.
          if (allocatedMemory <= retainedByWaitingThreads() && !anyWaiterCanProceed()) {
            throw e;
          }
          // The holders this wait depends on may themselves be blocked on resources outside
          // this pool (Spark execution memory, locks, I/O) that only a task waiting here can
          // release - a cycle this allocator cannot observe. Bound the wait so such cycles
          // unwind with the managed allocation error instead of hanging the executor; Spark's
          // task retry can then recover.
          long now = System.currentTimeMillis();
          if (waitStart == 0) {
            waitStart = now;
            lastLog = now;
            logger.warn(
                "Waiting for other tasks to free up {} bytes of Comet shuffle pool memory", size);
          } else if (now - waitStart >= memoryWaitTimeoutMs) {
            logger.warn(
                "Giving up after waiting {} ms for {} bytes of Comet shuffle pool memory "
                    + "(see {})",
                now - waitStart,
                size,
                CometConf$.MODULE$.COMET_SHUFFLE_JVM_MEMORY_WAIT_TIMEOUT().key());
            throw e;
          } else if (now - lastLog >= WAIT_LOG_INTERVAL_MS) {
            lastLog = now;
            logger.warn(
                "Still waiting ({} ms so far) for {} bytes of Comet shuffle pool memory; "
                    + "{} bytes free, {} thread(s) waiting",
                now - waitStart,
                size,
                totalMemory - allocatedMemory,
                waitingThreads.size());
          }
          try {
            wait(
                Math.max(
                    1L,
                    Math.min(TASK_KILL_POLL_INTERVAL_MS, memoryWaitTimeoutMs - (now - waitStart))));
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            // Not an allocation failure: stay non-fatal so that an intentional task kill is
            // classified as TaskKilled rather than ExceptionFailure (Spark's killed-task handler
            // only matches `InterruptedException | NonFatal(_)`).
            throw new RuntimeException(
                "Interrupted while waiting for Comet shuffle pool memory", ie);
          }
        }
      }
    } finally {
      if (waitingThreads.remove(self) != null) {
        notifyAll();
      }
    }
  }

  private long retainedByWaitingThreads() {
    long retained = 0;
    for (Thread thread : waitingThreads.keySet()) {
      retained += retainedMemory.getOrDefault(thread, 0L);
    }
    return retained;
  }

  private boolean anyWaiterCanProceed() {
    long free = totalMemory - allocatedMemory;
    for (long requested : waitingThreads.values()) {
      if (requested <= free) {
        return true;
      }
    }
    return false;
  }

  private synchronized MemoryBlock allocateMemoryBlock(long required) {
    if (required > TaskMemoryManager.MAXIMUM_PAGE_SIZE_BYTES) {
      throw new TooLargePageException(required);
    }

    long got = _acquireMemory(required);

    if (got < required) {
      allocatedMemory -= got;

      throw new SparkOutOfMemoryError(
          "UNABLE_TO_ACQUIRE_MEMORY",
          java.util.Map.of(
              "requestedBytes", String.valueOf(required),
              "receivedBytes", String.valueOf(totalMemory - allocatedMemory)));
    }

    int pageNumber = allocatedPages.nextClearBit(0);
    if (pageNumber >= PAGE_TABLE_SIZE) {
      allocatedMemory -= got;

      throw new IllegalStateException(
          "Have already allocated a maximum of " + PAGE_TABLE_SIZE + " pages");
    }

    MemoryBlock block = allocator.allocate(got);

    block.pageNumber = pageNumber;
    pageTable[pageNumber] = block;
    allocatedPages.set(pageNumber);
    pageOwners[pageNumber] = Thread.currentThread();
    retainedMemory.merge(Thread.currentThread(), got, Long::sum);

    return block;
  }

  public synchronized long free(MemoryBlock block) {
    if (block.pageNumber == MemoryBlock.FREED_IN_ALLOCATOR_PAGE_NUMBER
        || block.pageNumber == MemoryBlock.FREED_IN_TMM_PAGE_NUMBER) {
      // Already freed block
      return 0;
    }
    long blockSize = block.size();
    allocatedMemory -= blockSize;

    Thread owner = pageOwners[block.pageNumber];
    pageOwners[block.pageNumber] = null;
    if (owner != null) {
      retainedMemory.computeIfPresent(owner, (t, v) -> v - blockSize <= 0 ? null : v - blockSize);
    }

    pageTable[block.pageNumber] = null;
    allocatedPages.clear(block.pageNumber);
    block.pageNumber = MemoryBlock.FREED_IN_TMM_PAGE_NUMBER;

    allocator.free(block);
    // Wake up tasks waiting in `allocateBlocking`.
    notifyAll();
    return blockSize;
  }

  /**
   * Returns the offset in the page for the given page plus base offset address. Note that this
   * method assumes that the page number is valid.
   */
  public long getOffsetInPage(long pagePlusOffsetAddress) {
    long offsetInPage = decodeOffset(pagePlusOffsetAddress);
    int pageNumber = TaskMemoryManager.decodePageNumber(pagePlusOffsetAddress);
    assert (pageNumber >= 0 && pageNumber < PAGE_TABLE_SIZE);
    MemoryBlock page = pageTable[pageNumber];
    assert (page != null);
    return page.getBaseOffset() + offsetInPage;
  }

  public long decodeOffset(long pagePlusOffsetAddress) {
    return pagePlusOffsetAddress & MASK_LONG_LOWER_51_BITS;
  }

  public long encodePageNumberAndOffset(int pageNumber, long offsetInPage) {
    assert (pageNumber >= 0);
    return ((long) pageNumber) << OFFSET_BITS | offsetInPage & MASK_LONG_LOWER_51_BITS;
  }

  public long encodePageNumberAndOffset(MemoryBlock page, long offsetInPage) {
    return encodePageNumberAndOffset(page.pageNumber, offsetInPage - page.getBaseOffset());
  }
}
