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

package org.apache.spark.shuffle.sort;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.shuffle.comet.CometShuffleMemoryAllocatorTrait;
import org.apache.spark.sql.comet.execution.shuffle.SpillInfo;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.BlockManager;

import org.apache.comet.CometConf$;

/**
 * An external sorter interface specialized for sort-based shuffle.
 *
 * <p>Incoming records are appended to data pages. When all records have been inserted (or when the
 * current thread's shuffle memory limit is reached), the in-memory records are sorted according to
 * their partition ids using native sorter. The sorted records are then written to a single output
 * file (or multiple files, if we've spilled).
 *
 * <p>Unlike {@link org.apache.spark.util.collection.ExternalSorter}, this sorter does not merge its
 * spill files. Instead, this merging is performed in {@link
 * org.apache.spark.sql.comet.execution.shuffle.CometUnsafeShuffleWriter}, which uses a specialized
 * merge procedure that avoids extra serialization/deserialization.
 */
public interface CometShuffleExternalSorter {

  int MAXIMUM_PAGE_SIZE_BYTES = PackedRecordPointer.MAXIMUM_PAGE_SIZE_BYTES;

  /** Returns the checksums for each partition. */
  long[] getChecksums();

  /** Sort and spill the current records in response to memory pressure. */
  void spill() throws IOException;

  /** Return the peak memory used so far, in bytes. */
  long getPeakMemoryUsedBytes();

  /** Force all memory and spill files to be deleted; called by shuffle error-handling code. */
  void cleanupResources();

  /**
   * Writes a record to the shuffle sorter. This copies the record data into this external sorter's
   * managed memory, which may trigger spilling if the copy would exceed the memory limit. It
   * inserts a pointer for the record and record's partition id into the in-memory sorter.
   */
  void insertRecord(Object recordBase, long recordOffset, int length, int partitionId)
      throws IOException;

  /**
   * Close the sorter, causing any buffered data to be sorted and written out to disk.
   *
   * @return metadata for the spill files written by this sorter. If no records were ever inserted
   *     into this sorter, then this will return an empty array.
   */
  SpillInfo[] closeAndGetSpills() throws IOException;

  /**
   * Factory method to create the appropriate sorter implementation based on configuration.
   *
   * @return either a sync or async implementation based on the COMET_COLUMNAR_SHUFFLE_ASYNC_ENABLED
   *     configuration
   */
  static CometShuffleExternalSorter create(
      CometShuffleMemoryAllocatorTrait allocator,
      BlockManager blockManager,
      TaskContext taskContext,
      int initialSize,
      int numPartitions,
      SparkConf conf,
      ShuffleWriteMetricsReporter writeMetrics,
      StructType schema) {

    boolean isAsync = (boolean) CometConf$.MODULE$.COMET_COLUMNAR_SHUFFLE_ASYNC_ENABLED().get();

    if (isAsync) {
      return new CometShuffleExternalSorterAsync(
          allocator,
          blockManager,
          taskContext,
          initialSize,
          numPartitions,
          conf,
          writeMetrics,
          schema);
    } else {
      return new CometShuffleExternalSorterSync(
          allocator,
          blockManager,
          taskContext,
          initialSize,
          numPartitions,
          conf,
          writeMetrics,
          schema);
    }
  }
}
