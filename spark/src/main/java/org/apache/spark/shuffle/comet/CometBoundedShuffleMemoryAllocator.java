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

import org.apache.spark.SparkConf;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.memory.SparkOutOfMemoryError;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.unsafe.memory.UnsafeMemoryAllocator;

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
  private final UnsafeMemoryAllocator allocator = new UnsafeMemoryAllocator();

  private final long pageSize;
  private final long totalMemory;
  private long allocatedMemory = 0L;

  /** The number of bits used to address the page table. */
  private static final int PAGE_NUMBER_BITS = 13;

  /** The number of entries in the page table. */
  private static final int PAGE_TABLE_SIZE = 1 << PAGE_NUMBER_BITS;

  private final MemoryBlock[] pageTable = new MemoryBlock[PAGE_TABLE_SIZE];
  private final BitSet allocatedPages = new BitSet(PAGE_TABLE_SIZE);

  private static final int OFFSET_BITS = 51;
  private static final long MASK_LONG_LOWER_51_BITS = 0x7FFFFFFFFFFFFL;

  CometBoundedShuffleMemoryAllocator(
      SparkConf conf, TaskMemoryManager taskMemoryManager, long pageSize) {
    super(taskMemoryManager, pageSize, MemoryMode.OFF_HEAP);
    this.pageSize = pageSize;
    this.totalMemory =
        CometSparkSessionExtensions$.MODULE$.getCometShuffleMemorySize(conf, SQLConf.get());
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

    pageTable[block.pageNumber] = null;
    allocatedPages.clear(block.pageNumber);
    block.pageNumber = MemoryBlock.FREED_IN_TMM_PAGE_NUMBER;

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
