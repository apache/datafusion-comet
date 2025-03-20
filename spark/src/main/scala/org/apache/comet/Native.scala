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

package org.apache.comet

import java.nio.ByteBuffer

import org.apache.spark.CometTaskMemoryManager
import org.apache.spark.sql.comet.CometMetricNode

class Native extends NativeBase {

  // scalastyle:off
  /**
   * Create a native query plan from execution SparkPlan serialized in bytes.
   * @param id
   *   The id of the query plan.
   * @param configMap
   *   The Java Map object for the configs of native engine.
   * @param iterators
   *   the input iterators to the native query plan. It should be the same number as the number of
   *   scan nodes in the SparkPlan.
   * @param plan
   *   the bytes of serialized SparkPlan.
   * @param metrics
   *   the native metrics of SparkPlan.
   * @param metricsUpdateInterval
   *   the interval in milliseconds to update metrics, if interval is negative, metrics will be
   *   updated upon task completion.
   * @param taskMemoryManager
   *   the task-level memory manager that is responsible for tracking memory usage across JVM and
   *   native side.
   * @return
   *   the address to native query plan.
   */
  // scalastyle:off
  @native def createPlan(
      id: Long,
      iterators: Array[CometBatchIterator],
      plan: Array[Byte],
      partitionCount: Int,
      metrics: CometMetricNode,
      metricsUpdateInterval: Long,
      taskMemoryManager: CometTaskMemoryManager,
      batchSize: Int,
      offHeapMode: Boolean,
      memoryPoolType: String,
      memoryLimit: Long,
      memoryLimitPerTask: Long,
      taskAttemptId: Long,
      debug: Boolean,
      explain: Boolean,
      workerThreads: Int,
      blockingThreads: Int): Long
  // scalastyle:on

  /**
   * Execute a native query plan based on given input Arrow arrays.
   *
   * @param stage
   *   the stage ID, for informational purposes
   * @param partition
   *   the partition ID, for informational purposes
   * @param plan
   *   the address to native query plan.
   * @param arrayAddrs
   *   the addresses of Arrow Array structures
   * @param schemaAddrs
   *   the addresses of Arrow Schema structures
   * @return
   *   the number of rows, if -1, it means end of the output.
   */
  @native def executePlan(
      stage: Int,
      partition: Int,
      plan: Long,
      arrayAddrs: Array[Long],
      schemaAddrs: Array[Long]): Long

  /**
   * Release and drop the native query plan object and context object.
   *
   * @param plan
   *   the address to native query plan.
   */
  @native def releasePlan(plan: Long): Unit

  /**
   * Used by Comet shuffle external sorter to write sorted records to disk.
   *
   * @param addresses
   *   the array of addresses of Spark unsafe rows.
   * @param rowSizes
   *   the row sizes of Spark unsafe rows.
   * @param datatypes
   *   the datatypes of fields in Spark unsafe rows.
   * @param file
   *   the file path to write to.
   * @param preferDictionaryRatio
   *   the ratio of total values to distinct values in a string column that makes the writer to
   *   prefer dictionary encoding. If it is larger than the specified ratio, dictionary encoding
   *   will be used when writing columns of string type.
   * @param batchSize
   *   the batch size on the native side to buffer outputs during the row to columnar conversion
   *   before writing them out to disk.
   * @param checksumEnabled
   *   whether to compute checksum of written file.
   * @param checksumAlgo
   *   the checksum algorithm to use. 0 for CRC32, 1 for Adler32.
   * @param currentChecksum
   *   the current checksum of the file. As the checksum is computed incrementally, this is used
   *   to resume the computation of checksum for previous written data.
   * @param compressionCodec
   *   the compression codec
   * @param compressionLevel
   *   the compression level
   * @return
   *   [the number of bytes written to disk, the checksum]
   */
  // scalastyle:off
  @native def writeSortedFileNative(
      addresses: Array[Long],
      rowSizes: Array[Int],
      datatypes: Array[Array[Byte]],
      file: String,
      preferDictionaryRatio: Double,
      batchSize: Int,
      checksumEnabled: Boolean,
      checksumAlgo: Int,
      currentChecksum: Long,
      compressionCodec: String,
      compressionLevel: Int,
      enableFastEncoding: Boolean): Array[Long]
  // scalastyle:on

  /**
   * Sorts partition ids of Spark unsafe rows in place. Used by Comet shuffle external sorter.
   *
   * @param addr
   *   the address of the array of compacted partition ids.
   * @param size
   *   the size of the array.
   */
  @native def sortRowPartitionsNative(addr: Long, size: Long): Unit

  /**
   * Decompress and decode a native shuffle block.
   * @param shuffleBlock
   *   the encoded anc compressed shuffle block.
   * @param length
   *   the limit of the byte buffer.
   * @param addr
   *   the address of the array of compressed and encoded bytes.
   * @param size
   *   the size of the array.
   */
  @native def decodeShuffleBlock(
      shuffleBlock: ByteBuffer,
      length: Int,
      arrayAddrs: Array[Long],
      schemaAddrs: Array[Long]): Long

}
