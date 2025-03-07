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

import org.apache.spark.SparkConf;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.util.Utils;

import org.apache.comet.CometConf$;

/**
 * A simple memory allocator used by `CometShuffleExternalSorter` to allocate memory blocks which
 * store serialized rows. This class is simply an implementation of `MemoryConsumer` that delegates
 * memory allocation to the `TaskMemoryManager`. This requires that the `TaskMemoryManager` is
 * configured with `MemoryMode.OFF_HEAP`, i.e. it is using off-heap memory.
 */
public final class CometShuffleMemoryAllocator extends CometShuffleMemoryAllocatorTrait {
  private static CometShuffleMemoryAllocatorTrait INSTANCE;

  /**
   * Returns the singleton instance of `CometShuffleMemoryAllocator`. This method should be used
   * instead of the constructor to ensure that only one instance of `CometShuffleMemoryAllocator` is
   * created. For Spark tests, this returns `CometTestShuffleMemoryAllocator` which is a test-only
   * allocator that should not be used in production.
   */
  public static CometShuffleMemoryAllocatorTrait getInstance(
      SparkConf conf, TaskMemoryManager taskMemoryManager, long pageSize) {
    boolean isSparkTesting = Utils.isTesting();
    boolean useUnifiedMemAllocator =
        (boolean)
            CometConf$.MODULE$.COMET_COLUMNAR_SHUFFLE_UNIFIED_MEMORY_ALLOCATOR_IN_TEST().get();

    if (isSparkTesting && !useUnifiedMemAllocator) {
      synchronized (CometShuffleMemoryAllocator.class) {
        if (INSTANCE == null) {
          // CometTestShuffleMemoryAllocator handles pages by itself so it can be a singleton.
          INSTANCE = new CometTestShuffleMemoryAllocator(conf, taskMemoryManager, pageSize);
        }
      }
      return INSTANCE;
    } else {
      if (taskMemoryManager.getTungstenMemoryMode() != MemoryMode.OFF_HEAP) {
        throw new IllegalArgumentException(
            "CometShuffleMemoryAllocator should be used with off-heap "
                + "memory mode, but got "
                + taskMemoryManager.getTungstenMemoryMode());
      }

      // CometShuffleMemoryAllocator stores pages in TaskMemoryManager which is not singleton,
      // but one instance per task. So we need to create a new instance for each task.
      return new CometShuffleMemoryAllocator(taskMemoryManager, pageSize);
    }
  }

  CometShuffleMemoryAllocator(TaskMemoryManager taskMemoryManager, long pageSize) {
    super(taskMemoryManager, pageSize, MemoryMode.OFF_HEAP);
  }

  public long spill(long l, MemoryConsumer memoryConsumer) throws IOException {
    // JVM shuffle writer does not support spilling for other memory consumers
    return 0;
  }

  public synchronized MemoryBlock allocate(long required) {
    return this.allocatePage(required);
  }

  public synchronized void free(MemoryBlock block) {
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
