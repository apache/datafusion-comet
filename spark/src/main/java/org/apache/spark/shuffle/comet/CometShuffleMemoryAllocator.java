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

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.memory.TaskMemoryManager;

/**
 * An interface to instantiate either CometUnboundedShuffleMemoryAllocator (on-heap mode) or
 * CometUnifiedShuffleMemoryAllocator (off-heap mode).
 */
public final class CometShuffleMemoryAllocator {

  /**
   * Returns the shuffle memory allocator for the current task. Allocators store pages in the
   * `TaskMemoryManager`, or in their own page table, so a new instance is created per task. For
   * on-heap mode (Spark tests), this returns `CometUnboundedShuffleMemoryAllocator`.
   *
   * <p>Call this once per task and share the result. `CometUnboundedShuffleMemoryAllocator` numbers
   * pages within its own table, so a record address produced by one instance cannot be resolved by
   * another, and two instances in one task would decode each other's addresses to the wrong memory.
   */
  public static CometShuffleMemoryAllocatorTrait getInstance(
      TaskMemoryManager taskMemoryManager, long pageSize) {

    if (taskMemoryManager.getTungstenMemoryMode() == MemoryMode.OFF_HEAP) {
      return new CometUnifiedShuffleMemoryAllocator(taskMemoryManager, pageSize);
    }

    return new CometUnboundedShuffleMemoryAllocator(taskMemoryManager, pageSize);
  }
}
