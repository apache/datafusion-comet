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

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;

import scala.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.memory.SparkOutOfMemoryError;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.shuffle.comet.CometShuffleChecksumSupport;
import org.apache.spark.shuffle.comet.CometShuffleMemoryAllocatorTrait;
import org.apache.spark.shuffle.comet.TooLargePageException;
import org.apache.spark.sql.comet.execution.shuffle.SpillInfo;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.TempShuffleBlockId;
import org.apache.spark.unsafe.UnsafeAlignedOffset;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.util.Utils;

import org.apache.comet.CometConf$;

/**
 * Synchronous implementation of the external sorter for sort-based shuffle.
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
public final class CometShuffleExternalSorterSync
    implements CometShuffleExternalSorter, CometShuffleChecksumSupport {

  private static final Logger logger =
      LoggerFactory.getLogger(CometShuffleExternalSorterSync.class);

  private final int numPartitions;
  private final BlockManager blockManager;
  private final TaskContext taskContext;
  private final ShuffleWriteMetricsReporter writeMetrics;

  private final StructType schema;

  /** Force this sorter to spill when there are this many elements in memory. */
  private final int numElementsForSpillThreshold;

  // When this external sorter allocates memory of `sorterArray`, we need to keep its
  // assigned initial size. After spilling, we will reset the array to its initial size.
  // See `sorterArray` comment for more details.
  private int initialSize;

  private SpillSorter activeSpillSorter;

  private final LinkedList<SpillInfo> spills = new LinkedList<>();

  /** Peak memory used by this sorter so far, in bytes. */
  private long peakMemoryUsedBytes;

  // Checksum calculator for each partition. Empty when shuffle checksum disabled.
  private final long[] partitionChecksums;

  private final String checksumAlgorithm;
  private final String compressionCodec;
  private final int compressionLevel;

  // The memory allocator for this sorter. It is used to allocate/free memory pages for this sorter.
  // Because we need to allocate off-heap memory regardless of configured Spark memory mode
  // (on-heap/off-heap), we need a separate memory allocator.
  private final CometShuffleMemoryAllocatorTrait allocator;

  private boolean spilling = false;

  private final int uaoSize = UnsafeAlignedOffset.getUaoSize();
  private final double preferDictionaryRatio;
  private final boolean tracingEnabled;

  public CometShuffleExternalSorterSync(
      CometShuffleMemoryAllocatorTrait allocator,
      BlockManager blockManager,
      TaskContext taskContext,
      int initialSize,
      int numPartitions,
      SparkConf conf,
      ShuffleWriteMetricsReporter writeMetrics,
      StructType schema) {
    this.allocator = allocator;
    this.blockManager = blockManager;
    this.taskContext = taskContext;
    this.numPartitions = numPartitions;
    this.schema = schema;
    this.numElementsForSpillThreshold =
        (int) CometConf$.MODULE$.COMET_COLUMNAR_SHUFFLE_SPILL_THRESHOLD().get();
    this.writeMetrics = writeMetrics;

    this.peakMemoryUsedBytes = getMemoryUsage();
    this.partitionChecksums = createPartitionChecksums(numPartitions, conf);
    this.checksumAlgorithm = getChecksumAlgorithm(conf);
    this.compressionCodec = CometConf$.MODULE$.COMET_EXEC_SHUFFLE_COMPRESSION_CODEC().get();
    this.compressionLevel =
        (int) CometConf$.MODULE$.COMET_EXEC_SHUFFLE_COMPRESSION_ZSTD_LEVEL().get();

    this.initialSize = initialSize;
    this.tracingEnabled = (boolean) CometConf$.MODULE$.COMET_TRACING_ENABLED().get();

    this.preferDictionaryRatio =
        (double) CometConf$.MODULE$.COMET_SHUFFLE_PREFER_DICTIONARY_RATIO().get();

    this.activeSpillSorter = createSpillSorter();
  }

  /** Creates a new SpillSorter with all required dependencies. */
  private SpillSorter createSpillSorter() {
    return new SpillSorter(
        allocator,
        initialSize,
        schema,
        uaoSize,
        preferDictionaryRatio,
        compressionCodec,
        compressionLevel,
        checksumAlgorithm,
        partitionChecksums,
        writeMetrics,
        taskContext,
        spills,
        this::spill);
  }

  @Override
  public long[] getChecksums() {
    return partitionChecksums;
  }

  /** Sort and spill the current records in response to memory pressure. */
  @Override
  public void spill() throws IOException {
    if (spilling || activeSpillSorter == null || activeSpillSorter.numRecords() == 0) {
      return;
    }

    spilling = true;

    logger.info(
        "Thread {} spilling sort data of {} to disk ({} {} so far)",
        Thread.currentThread().getId(),
        Utils.bytesToString(getMemoryUsage()),
        spills.size(),
        spills.size() > 1 ? " times" : " time");

    final Tuple2<TempShuffleBlockId, File> spilledFileInfo =
        blockManager.diskBlockManager().createTempShuffleBlock();
    final File file = spilledFileInfo._2();
    final TempShuffleBlockId blockId = spilledFileInfo._1();
    final SpillInfo spillInfo = new SpillInfo(numPartitions, file, blockId);

    activeSpillSorter.setSpillInfo(spillInfo);

    activeSpillSorter.writeSortedFileNative(false, tracingEnabled);
    final long spillSize = activeSpillSorter.freeMemory();
    activeSpillSorter.reset();

    // Reset the in-memory sorter's pointer array only after freeing up the memory pages holding
    // the records. Otherwise, if the task is over allocated memory, then without freeing the
    // memory pages, we might not be able to get memory for the pointer array.
    taskContext.taskMetrics().incMemoryBytesSpilled(spillSize);

    spilling = false;
  }

  private long getMemoryUsage() {
    if (activeSpillSorter != null) {
      return activeSpillSorter.getMemoryUsage();
    }
    return 0;
  }

  private void updatePeakMemoryUsed() {
    long mem = getMemoryUsage();
    if (mem > peakMemoryUsedBytes) {
      peakMemoryUsedBytes = mem;
    }
  }

  /** Return the peak memory used so far, in bytes. */
  @Override
  public long getPeakMemoryUsedBytes() {
    updatePeakMemoryUsed();
    return peakMemoryUsedBytes;
  }

  private long freeMemory() {
    updatePeakMemoryUsed();
    long memoryFreed = activeSpillSorter.freeMemory();
    activeSpillSorter.freeArray();
    return memoryFreed;
  }

  /** Force all memory and spill files to be deleted; called by shuffle error-handling code. */
  @Override
  public void cleanupResources() {
    freeMemory();

    for (SpillInfo spill : spills) {
      if (spill.file.exists() && !spill.file.delete()) {
        logger.error("Unable to delete spill file {}", spill.file.getPath());
      }
    }
  }

  /**
   * Checks whether there is enough space to insert an additional record in to the sort pointer
   * array and grows the array if additional space is required. If the required space cannot be
   * obtained, then the in-memory data will be spilled to disk.
   */
  private void growPointerArrayIfNecessary() throws IOException {
    assert (activeSpillSorter != null);
    if (!activeSpillSorter.hasSpaceForAnotherRecord()) {
      long used = activeSpillSorter.getMemoryUsage();
      LongArray array;
      try {
        // could trigger spilling
        array = allocator.allocateArray(used / 8 * 2);
      } catch (TooLargePageException e) {
        // The pointer array is too big to fix in a single page, spill.
        spill();
        return;
      } catch (SparkOutOfMemoryError e) {
        // Cannot allocate enough memory, spill and reset pointer array.
        try {
          spill();
        } catch (SparkOutOfMemoryError e2) {
          // Cannot allocate memory even after spilling, throw the error.
          if (!activeSpillSorter.hasSpaceForAnotherRecord()) {
            logger.error("Unable to grow the pointer array");
            throw e2;
          }
        }
        return;
      }
      // check if spilling is triggered or not
      if (activeSpillSorter.hasSpaceForAnotherRecord()) {
        allocator.freeArray(array);
      } else {
        activeSpillSorter.expandPointerArray(array);
      }
    }
  }

  /**
   * Writes a record to the shuffle sorter. This copies the record data into this external sorter's
   * managed memory, which may trigger spilling if the copy would exceed the memory limit. It
   * inserts a pointer for the record and record's partition id into the in-memory sorter.
   */
  @Override
  public void insertRecord(Object recordBase, long recordOffset, int length, int partitionId)
      throws IOException {

    assert (activeSpillSorter != null);
    int threshold = numElementsForSpillThreshold;
    if (activeSpillSorter.numRecords() >= threshold) {
      logger.info(
          "Spilling data because number of spilledRecords crossed the threshold " + threshold);
      spill();
    }

    growPointerArrayIfNecessary();

    // Need 4 or 8 bytes to store the record length.
    final int required = length + uaoSize;
    // Acquire enough memory to store the record.
    // If we cannot acquire enough memory, we will spill current writers.
    if (!activeSpillSorter.acquireNewPageIfNecessary(required)) {
      // Spilling is happened, initiate new memory page for new writer.
      activeSpillSorter.initialCurrentPage(required);
    }

    activeSpillSorter.insertRecord(recordBase, recordOffset, length, partitionId);
  }

  /**
   * Close the sorter, causing any buffered data to be sorted and written out to disk.
   *
   * @return metadata for the spill files written by this sorter. If no records were ever inserted
   *     into this sorter, then this will return an empty array.
   */
  @Override
  public SpillInfo[] closeAndGetSpills() throws IOException {
    if (activeSpillSorter != null) {
      // Do not count the final file towards the spill count.
      final Tuple2<TempShuffleBlockId, File> spilledFileInfo =
          blockManager.diskBlockManager().createTempShuffleBlock();
      final File file = spilledFileInfo._2();
      final TempShuffleBlockId blockId = spilledFileInfo._1();
      final SpillInfo spillInfo = new SpillInfo(numPartitions, file, blockId);

      activeSpillSorter.setSpillInfo(spillInfo);
      activeSpillSorter.writeSortedFileNative(true, tracingEnabled);

      freeMemory();
    }

    return spills.toArray(new SpillInfo[spills.size()]);
  }
}
