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

import org.apache.spark.SparkConf;
import org.apache.spark.memory.TaskMemoryManager;

import org.apache.comet.CometSparkSessionExtensions;

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
public final class CometShuffleMemoryAllocator {
  private static CometShuffleMemoryAllocatorTrait INSTANCE;

  /**
   * Returns the singleton instance of `CometShuffleMemoryAllocator`. This method should be used
   * instead of the constructor to ensure that only one instance of `CometShuffleMemoryAllocator` is
   * created. For Spark tests, this returns `CometBoundedShuffleMemoryAllocator`.
   */
  public static CometShuffleMemoryAllocatorTrait getInstance(
      SparkConf conf, TaskMemoryManager taskMemoryManager, long pageSize) {

    if (CometSparkSessionExtensions.cometUnifiedMemoryManagerEnabled(conf)) {
      // CometShuffleMemoryAllocator stores pages in TaskMemoryManager which is not singleton,
      // but one instance per task. So we need to create a new instance for each task.
      return new CometUnifiedShuffleMemoryAllocator(taskMemoryManager, pageSize);
    }

    synchronized (CometShuffleMemoryAllocator.class) {
      if (INSTANCE == null) {
        // CometBoundedShuffleMemoryAllocator handles pages by itself so it can be a singleton.
        INSTANCE = new CometBoundedShuffleMemoryAllocator(conf, taskMemoryManager, pageSize);
      }
    }
    return INSTANCE;
  }
}
