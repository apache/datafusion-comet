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

package org.apache.comet.local

import org.apache.spark.CometTaskMemoryManager

import org.apache.comet.NativeBase

/**
 * Numeric handles, never native pointers. Every successful create needs close, including errors.
 */
private[local] class NativeLocal extends NativeBase {

  /**
   * Creates and starts a local query over `partitions` partitions of a serialized Comet plan
   * without JVM inputs, with the memory pool and session that `Native.createPlan` creates for the
   * calling Spark task. Returns its handle.
   */
  // scalastyle:off argcount
  @native def createQuery(
      plan: Array[Byte],
      configs: Array[Byte],
      partitions: Int,
      columns: Int,
      taskMemoryManager: CometTaskMemoryManager,
      localDirs: Array[String],
      batchSize: Int,
      offHeapMode: Boolean,
      memoryPoolType: String,
      memoryLimit: Long,
      taskAttemptId: Long,
      taskCpus: Long): Long
  // scalastyle:on argcount

  /** Exports the next batch; returns its row count, -1 at EOF, or -2 while none is ready. */
  @native def nextBatch(id: Long, arrays: Array[Long], schemas: Array[Long]): Long

  @native def close(id: Long): Unit

  @native def activeQueries(): Long
}
