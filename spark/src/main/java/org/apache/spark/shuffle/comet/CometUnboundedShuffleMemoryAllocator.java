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
import java.util.concurrent.atomic.AtomicLong;

import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.memory.SparkOutOfMemoryError;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.unsafe.memory.UnsafeMemoryAllocator;

/**
 * The memory allocator used by `CometShuffleExternalSorter` to allocate the memory blocks that hold
 * serialized rows when Spark runs in on-heap mode.
 *
 * <p>Spark's own allocator cannot be used here. `TaskMemoryManager.allocatePage` hands out pages
 * from `tungstenMemoryAllocator`, which is the on-heap allocator in this mode, and the row
 * addresses derived from these pages are passed to `writeSortedFileNative` for Rust to dereference.
 * The pages therefore have to be `Unsafe`-allocated regardless of Spark's memory mode.
 *
 * <p>Nothing bounds these allocations. On-heap mode exists so that the Spark SQL tests can run
 * against Comet without changing Spark's memory configuration; it is not a production
 * configuration, and Comet performs no memory accounting in it. See the memory management page in
 * the contributor guide. The off-heap path (`CometUnifiedShuffleMemoryAllocator`) is the one that
 * accounts, through Spark's unified memory manager.
 *
 * <p>The page table below is adapted from `org.apache.spark.unsafe.memory.TaskMemoryManager`, with
 * the dependency on the configured memory mode removed.
 *
 * <p>A page number indexes this instance's own table, so an address encoded by one allocator means
 * nothing to another. Everything that addresses a page therefore has to come from the same
 * instance, which is why each caller creates one allocator and shares it for the life of the task
 * rather than creating one per writer. The off-heap allocator has no such constraint, because it
 * stores its pages in the `TaskMemoryManager` instead.
 */
public final class CometUnboundedShuffleMemoryAllocator extends CometShuffleMemoryAllocatorTrait {
  private final UnsafeMemoryAllocator allocator = new UnsafeMemoryAllocator();

  private final long pageSize;

  /** The number of bits used to address the page table. */
  private static final int PAGE_NUMBER_BITS = 13;

  /** The number of entries in the page table. */
  private static final int PAGE_TABLE_SIZE = 1 << PAGE_NUMBER_BITS;

  private final MemoryBlock[] pageTable = new MemoryBlock[PAGE_TABLE_SIZE];
  private final BitSet allocatedPages = new BitSet(PAGE_TABLE_SIZE);

  /** Bytes currently held in the page table, reported by {@link #getUsed()}. */
  private final AtomicLong allocatedMemory = new AtomicLong();

  private static final int OFFSET_BITS = 51;
  private static final long MASK_LONG_LOWER_51_BITS = 0x7FFFFFFFFFFFFL;

  CometUnboundedShuffleMemoryAllocator(TaskMemoryManager taskMemoryManager, long pageSize) {
    super(taskMemoryManager, pageSize, MemoryMode.OFF_HEAP);
    this.pageSize = pageSize;
  }

  /**
   * Returns the current allocation total in bytes. Allocations bypass Spark's memory manager, and
   * this allocator keeps no budget, so it reports the bytes currently held in its page table.
   *
   * <p>Read from an {@link AtomicLong} rather than under this allocator's monitor:
   * `TaskMemoryManager` calls this while holding its own monitor, so taking a second lock here
   * would invert the lock order against any future caller that allocates while holding it.
   */
  @Override
  public long getUsed() {
    return allocatedMemory.get();
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

  private synchronized MemoryBlock allocateMemoryBlock(long required) {
    if (required > TaskMemoryManager.MAXIMUM_PAGE_SIZE_BYTES) {
      throw new TooLargePageException(required);
    }

    int pageNumber = allocatedPages.nextClearBit(0);
    if (pageNumber >= PAGE_TABLE_SIZE) {
      // The page table is the only limit this allocator has. Report it the way a memory manager
      // reports a refused acquisition, so that the callers which already handle that by spilling
      // and retrying (`SpillWriter.acquireNewPageIfNecessary`,
      // `CometShuffleExternalSorter.growPointerArrayIfNecessary`) can free pages and make
      // progress, rather than failing the task with an exception nothing catches.
      throw new SparkOutOfMemoryError(
          "UNABLE_TO_ACQUIRE_MEMORY",
          java.util.Map.of(
              "requestedBytes", String.valueOf(required),
              "receivedBytes", String.valueOf(0)));
    }

    MemoryBlock block = allocator.allocate(required);

    block.pageNumber = pageNumber;
    pageTable[pageNumber] = block;
    allocatedPages.set(pageNumber);
    allocatedMemory.addAndGet(block.size());

    return block;
  }

  public synchronized long free(MemoryBlock block) {
    if (block.pageNumber == MemoryBlock.FREED_IN_ALLOCATOR_PAGE_NUMBER
        || block.pageNumber == MemoryBlock.FREED_IN_TMM_PAGE_NUMBER) {
      // Already freed block
      return 0;
    }
    long blockSize = block.size();

    pageTable[block.pageNumber] = null;
    allocatedPages.clear(block.pageNumber);
    block.pageNumber = MemoryBlock.FREED_IN_TMM_PAGE_NUMBER;
    allocatedMemory.addAndGet(-blockSize);

    allocator.free(block);
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
