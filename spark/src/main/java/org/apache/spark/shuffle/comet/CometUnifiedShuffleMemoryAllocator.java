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

import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.unsafe.memory.MemoryBlock;

/**
 * A simple memory allocator used by `CometShuffleExternalSorter` to allocate memory blocks which
 * store serialized rows. This class is simply an implementation of `MemoryConsumer` that delegates
 * memory allocation to the `TaskMemoryManager`. This requires that the `TaskMemoryManager` is
 * configured with `MemoryMode.OFF_HEAP`, i.e. it is using off-heap memory.
 *
 * <p>If the user does not enable off-heap memory then we want to use
 * CometBoundedShuffleMemoryAllocator. The tests also need to default to using this because off-heap
 * is not enabled when running the Spark SQL tests.
 */
public final class CometUnifiedShuffleMemoryAllocator extends CometShuffleMemoryAllocatorTrait {

  CometUnifiedShuffleMemoryAllocator(TaskMemoryManager taskMemoryManager, long pageSize) {
    super(taskMemoryManager, pageSize, MemoryMode.OFF_HEAP);
    if (taskMemoryManager.getTungstenMemoryMode() != MemoryMode.OFF_HEAP) {
      throw new IllegalArgumentException(
          "CometUnifiedShuffleMemoryAllocator should be used with off-heap "
              + "memory mode, but got "
              + taskMemoryManager.getTungstenMemoryMode());
    }
  }

  public long spill(long l, MemoryConsumer memoryConsumer) throws IOException {
    // JVM shuffle writer does not support spilling for other memory consumers
    return 0;
  }

  public synchronized MemoryBlock allocate(long required) {
    return this.allocatePage(required);
  }

  public synchronized void free(MemoryBlock block) {
    if (block.pageNumber == MemoryBlock.FREED_IN_ALLOCATOR_PAGE_NUMBER
        || block.pageNumber == MemoryBlock.FREED_IN_TMM_PAGE_NUMBER) {
      // Already freed block
      return;
    }
    this.freePage(block);
  }

  /**
   * Returns the offset in the page for the given page plus base offset address. Note that this
   * method assumes that the page number is valid.
   */
  public long getOffsetInPage(long pagePlusOffsetAddress) {
    return taskMemoryManager.getOffsetInPage(pagePlusOffsetAddress);
  }

  public long encodePageNumberAndOffset(int pageNumber, long offsetInPage) {
    return TaskMemoryManager.encodePageNumberAndOffset(pageNumber, offsetInPage);
  }

  public long encodePageNumberAndOffset(MemoryBlock page, long offsetInPage) {
    return encodePageNumberAndOffset(page.pageNumber, offsetInPage - page.getBaseOffset());
  }
}
