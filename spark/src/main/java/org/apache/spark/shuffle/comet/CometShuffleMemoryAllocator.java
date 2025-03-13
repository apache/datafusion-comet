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
 * An interface to instantiate either CometBoundedShuffleMemoryAllocator (on-heap mode) or
 * CometUnifiedShuffleMemoryAllocator (off-heap mode).
 */
public final class CometShuffleMemoryAllocator {
  private static CometShuffleMemoryAllocatorTrait INSTANCE;

  /**
   * Returns the singleton instance of `CometShuffleMemoryAllocator`. This method should be used
   * instead of the constructor to ensure that only one instance of `CometShuffleMemoryAllocator` is
   * created. For on-heap mode (Spark tests), this returns `CometBoundedShuffleMemoryAllocator`.
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
