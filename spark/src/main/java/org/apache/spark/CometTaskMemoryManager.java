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

package org.apache.spark;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.memory.TaskMemoryManager;

/**
 * A adapter class that is used by Comet native to acquire & release memory through Spark's unified
 * memory manager. This assumes Spark's off-heap memory mode is enabled.
 */
public class CometTaskMemoryManager {

  private static final Logger logger = LoggerFactory.getLogger(CometTaskMemoryManager.class);

  /** The id uniquely identifies the native plan this memory manager is associated to */
  private final long id;

  private final TaskMemoryManager internal;
  private final NativeMemoryConsumer nativeMemoryConsumer;
  private final AtomicLong used = new AtomicLong();

  public CometTaskMemoryManager(long id) {
    this.id = id;
    this.internal = TaskContext$.MODULE$.get().taskMemoryManager();
    this.nativeMemoryConsumer = new NativeMemoryConsumer();
  }

  // Called by Comet native through JNI.
  // Returns the actual amount of memory (in bytes) granted.
  public long acquireMemory(long size) {
    long acquired = internal.acquireExecutionMemory(size, nativeMemoryConsumer);
    used.addAndGet(acquired);
    return acquired;
  }

  // Called by Comet native through JNI
  public void releaseMemory(long size) {
    long newUsed = used.addAndGet(-size);
    if (newUsed < 0) {
      logger.error("Used memory is negative: " + newUsed + " after releasing memory chunk of: " + size);
    }
    internal.releaseExecutionMemory(size, nativeMemoryConsumer);
  }

  public long getUsed() {
    return used.get();
  }

  /**
   * A dummy memory consumer that does nothing when spilling. At the moment, Comet native doesn't
   * share the same API as Spark and cannot trigger spill when acquire memory. Therefore, when
   * acquiring memory from native or JVM, spilling can only be triggered from JVM operators.
   */
  private class NativeMemoryConsumer extends MemoryConsumer {
    protected NativeMemoryConsumer() {
      super(CometTaskMemoryManager.this.internal, 0, MemoryMode.OFF_HEAP);
    }

    @Override
    public long spill(long size, MemoryConsumer trigger) throws IOException {
      // No spilling
      return 0;
    }

    @Override
    public String toString() {
      return String.format("NativeMemoryConsumer(id=%)", id);
    }
  }
}
