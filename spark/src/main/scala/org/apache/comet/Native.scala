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

import org.apache.comet.parquet.CometFileKeyUnwrapper

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
      configMapProto: Array[Byte],
      partitionCount: Int,
      metrics: CometMetricNode,
      metricsUpdateInterval: Long,
      taskMemoryManager: CometTaskMemoryManager,
      localDirs: Array[String],
      batchSize: Int,
      offHeapMode: Boolean,
      memoryPoolType: String,
      memoryLimit: Long,
      memoryLimitPerTask: Long,
      taskAttemptId: Long,
      taskCPUs: Long,
      keyUnwrapper: CometFileKeyUnwrapper): Long
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
      tracingEnabled: Boolean): Array[Long]
  // scalastyle:on

  /**
   * Sorts partition ids of Spark unsafe rows in place. Used by Comet shuffle external sorter.
   *
   * @param addr
   *   the address of the array of compacted partition ids.
   * @param size
   *   the size of the array.
   */
  @native def sortRowPartitionsNative(addr: Long, size: Long, tracingEnabled: Boolean): Unit

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
      schemaAddrs: Array[Long],
      tracingEnabled: Boolean): Long

  /**
   * Log the beginning of an event.
   * @param name
   *   The name of the event.
   */
  @native def traceBegin(name: String): Unit

  /**
   * Log the end of an event.
   * @param name
   *   The name of the event.
   */
  @native def traceEnd(name: String): Unit

  /**
   * Log the amount of memory currently in use.
   *
   * @param name
   *   Type of memory e.g. jvm, native, off-heap pool
   * @param memoryUsageBytes
   *   Number of bytes in use
   */
  @native def logMemoryUsage(name: String, memoryUsageBytes: Long): Unit

  // Native Columnar to Row conversion methods

  /**
   * Initialize a native columnar to row converter.
   *
   * @param schema
   *   Array of serialized data types (as byte arrays) for each column in the schema.
   * @param batchSize
   *   The maximum number of rows that will be converted in a single batch. Used to pre-allocate
   *   the output buffer.
   * @return
   *   A handle to the native converter context. This handle must be passed to subsequent convert
   *   and close calls.
   */
  @native def columnarToRowInit(schema: Array[Array[Byte]], batchSize: Int): Long

  /**
   * Convert Arrow columnar data to Spark UnsafeRow format.
   *
   * @param c2rHandle
   *   The handle returned by columnarToRowInit.
   * @param arrayAddrs
   *   The addresses of Arrow Array structures for each column.
   * @param schemaAddrs
   *   The addresses of Arrow Schema structures for each column.
   * @param numRows
   *   The number of rows to convert.
   * @return
   *   A NativeColumnarToRowInfo containing the memory address of the row buffer and metadata
   *   (offsets and lengths) for each row.
   */
  @native def columnarToRowConvert(
      c2rHandle: Long,
      arrayAddrs: Array[Long],
      schemaAddrs: Array[Long],
      numRows: Int): NativeColumnarToRowInfo

  /**
   * Close and release the native columnar to row converter.
   *
   * @param c2rHandle
   *   The handle returned by columnarToRowInit.
   */
  @native def columnarToRowClose(c2rHandle: Long): Unit

  // Native Iceberg Compaction methods

  /**
   * Execute native Iceberg compaction.
   *
   * This function:
   *   1. Parses the compaction configuration from JSON 2. Creates a native scan plan for the
   *      input files 3. Writes compacted output using IcebergParquetWriterExec 4. Returns
   *      metadata for new files (does NOT commit)
   *
   * @param compactionConfigJson
   *   JSON string containing CompactionTaskConfig with:
   *   - table_config: IcebergTableConfig (table identifier, metadata location, etc.)
   *   - file_scan_tasks: Array of FileScanTaskConfig (files to compact)
   *   - target_file_size_bytes: Target size for output files
   *   - compression: Compression codec (snappy, zstd, etc.)
   *   - data_dir: Output data directory
   * @return
   *   JSON string containing NativeCompactionResult with:
   *   - success: Boolean indicating if compaction succeeded
   *   - error_message: Error message if failed
   *   - result: IcebergCompactionResult with files_to_delete and files_to_add
   */
  @native def executeIcebergCompaction(compactionConfigJson: String): String

  /**
   * Get the version of the native Iceberg compaction library.
   *
   * @return
   *   Version string of the native library
   */
  @native def getIcebergCompactionVersion(): String

}
