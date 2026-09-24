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

import java.lang.ref.WeakReference;

import org.apache.spark.memory.MemoryManager;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.BroadcastBlockId;

/**
 * Charges prepared broadcasts to Spark off-heap storage across task boundaries. One cap covers
 * loading and active builds in a SparkEnv generation; retired owners never charge a replacement
 * environment. Lives in org.apache.spark to access Spark's storage memory API.
 */
public final class CometBroadcastMemoryManager {
  private static long nextGeneration;
  private static CometBroadcastMemoryManager current;

  private final WeakReference<Object> environment;
  private MemoryManager memoryManager;
  private final BlockId blockId;
  private final long generation;
  private final long limit;
  private long used;
  private boolean retired;

  /** Capture an executor without retaining its SparkEnv after shutdown. */
  private CometBroadcastMemoryManager(
      Object environment, MemoryManager memoryManager, long generation, long limit) {
    this.environment = new WeakReference<>(environment);
    this.memoryManager = memoryManager;
    this.generation = generation;
    this.limit = limit;
    this.blockId = new BroadcastBlockId(generation, "comet-prepared-native");
  }

  /**
   * Return an owner only when CometPlugin and Spark off-heap memory support reuse. Constructing the
   * owner does not reserve storage; a null owner makes the caller open an ordinary stream.
   */
  public static CometBroadcastMemoryManager getOrCreate(long maxBytes) {
    SparkEnv env = SparkEnv.get();
    if (env == null) {
      return null;
    }
    boolean hasCometPlugin = false;
    for (String plugin : env.conf().get("spark.plugins", "").split(",")) {
      if (plugin.trim().equals("org.apache.spark.CometPlugin")) {
        hasCometPlugin = true;
        break;
      }
    }
    if (!hasCometPlugin) {
      return null;
    }
    return getOrCreate(
        env,
        env.memoryManager(),
        env.conf().getBoolean("spark.memory.offHeap.enabled", false),
        maxBytes);
  }

  /**
   * Package-scope variant for lifecycle tests. One SparkEnv shares one cap: tasks with a different
   * cap receive no owner rather than sharing an ambiguous budget. A new SparkEnv retires the old
   * owner's Spark charge and starts a separate generation.
   */
  static synchronized CometBroadcastMemoryManager getOrCreate(
      Object environment, MemoryManager memoryManager, boolean offHeapEnabled, long maxBytes) {
    if (current != null && current.environment.get() != environment) {
      current.retire();
      current = null;
    }
    if (!offHeapEnabled) {
      return null;
    }
    if (current != null) {
      return !current.retired && current.limit == maxBytes ? current : null;
    }
    ++nextGeneration;
    current = new CometBroadcastMemoryManager(environment, memoryManager, nextGeneration, maxBytes);
    return current;
  }

  /**
   * Grant the whole native allocation or none, under one cap for all builds in this generation.
   * Spark's usual off-heap storage admission may evict other cached Spark blocks.
   */
  public synchronized long acquireMemory(long size) {
    if (retired || size <= 0 || size > limit - used) {
      return 0;
    }
    if (!memoryManager.acquireStorageMemory(blockId, size, MemoryMode.OFF_HEAP)) {
      return 0;
    }
    used += size;
    return size;
  }

  /**
   * Release a native lease. Retirement already returned the Spark storage charge, so late releases
   * update only this owner's local count and cannot touch a replacement executor.
   */
  public synchronized void releaseMemory(long size) {
    if (size > used) {
      throw new IllegalArgumentException("Broadcast memory release exceeds its outstanding grant");
    }
    if (!retired && size != 0) {
      memoryManager.releaseStorageMemory(size, MemoryMode.OFF_HEAP);
    }
    used -= size;
  }

  /** Includes outstanding native leases after retirement has returned their Spark charge. */
  public synchronized long getUsedMemory() {
    return used;
  }

  public long getGeneration() {
    return generation;
  }

  public long getLimit() {
    return limit;
  }

  /**
   * Return Spark's storage charge once, while keeping the outstanding count for native leases that
   * release after executor shutdown or a SparkEnv replacement.
   */
  private synchronized void retire() {
    if (!retired) {
      retired = true;
      MemoryManager previousManager = memoryManager;
      memoryManager = null;
      if (used != 0) {
        previousManager.releaseStorageMemory(used, MemoryMode.OFF_HEAP);
      }
    }
  }

  /** Retire this executor's owner without reopening it for the same SparkEnv. */
  public static synchronized void shutdown() {
    if (current != null) {
      current.retire();
    }
  }
}
