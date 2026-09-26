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

import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.unsafe.memory.MemoryBlock;

/** The base class for Comet JVM shuffle memory allocators. */
public abstract class CometShuffleMemoryAllocatorTrait extends MemoryConsumer {
  protected CometShuffleMemoryAllocatorTrait(
      TaskMemoryManager taskMemoryManager, long pageSize, MemoryMode mode) {
    super(taskMemoryManager, pageSize, mode);
  }

  public abstract MemoryBlock allocate(long required);

  public abstract long free(MemoryBlock block);

  public abstract long getOffsetInPage(long pagePlusOffsetAddress);

  public abstract long encodePageNumberAndOffset(MemoryBlock page, long offsetInPage);
}
