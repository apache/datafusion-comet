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
import java.util.LinkedList;
import javax.annotation.Nullable;

import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.shuffle.comet.CometShuffleMemoryAllocatorTrait;
import org.apache.spark.sql.comet.execution.shuffle.SpillInfo;
import org.apache.spark.sql.comet.execution.shuffle.SpillWriter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.UnsafeAlignedOffset;
import org.apache.spark.unsafe.array.LongArray;

import org.apache.comet.Native;

/**
 * A spill sorter that buffers records in memory, sorts them by partition ID, and writes them to
 * disk. This class is used by CometShuffleExternalSorter to manage individual spill operations.
 *
 * <p>Each SpillSorter instance manages its own memory pages and pointer array. When spilling is
 * triggered, the records are sorted by partition ID using native code and written to a spill file.
 */
public class SpillSorter extends SpillWriter {

  /** Callback interface for triggering spill operations in the parent sorter. */
  @FunctionalInterface
  public interface SpillCallback {
    void onSpillRequired() throws IOException;
  }

  // Configuration fields (immutable after construction)
  private final int initialSize;
  private final int uaoSize;
  private final double preferDictionaryRatio;
  private final String compressionCodec;
  private final int compressionLevel;
  private final String checksumAlgorithm;

  // Shared state (mutable, passed by reference from parent)
  private final long[] partitionChecksums;
  private final ShuffleWriteMetricsReporter writeMetrics;
  private final TaskContext taskContext;
  private final LinkedList<SpillInfo> spills;
  private final SpillCallback spillCallback;

  // Internal state
  private boolean freed = false;
  private SpillInfo spillInfo;
  @Nullable private ShuffleInMemorySorter inMemSorter;
  private LongArray sorterArray;

  /**
   * Creates a new SpillSorter with explicit dependencies.
   *
   * @param allocator Memory allocator for pages and arrays
   * @param initialSize Initial size for the pointer array
   * @param schema Schema of the records being sorted
   * @param uaoSize Size of UnsafeAlignedOffset (4 or 8 bytes)
   * @param preferDictionaryRatio Dictionary encoding preference ratio
   * @param compressionCodec Compression codec for spill files
   * @param compressionLevel Compression level
   * @param checksumAlgorithm Checksum algorithm (e.g., "crc32", "adler32")
   * @param partitionChecksums Array to store partition checksums (shared with parent)
   * @param writeMetrics Metrics reporter for shuffle writes
   * @param taskContext Task context for metrics updates
   * @param spills List to accumulate spill info (shared with parent)
   * @param spillCallback Callback to trigger spill in parent sorter
   */
  public SpillSorter(
      CometShuffleMemoryAllocatorTrait allocator,
      int initialSize,
      StructType schema,
      int uaoSize,
      double preferDictionaryRatio,
      String compressionCodec,
      int compressionLevel,
      String checksumAlgorithm,
      long[] partitionChecksums,
      ShuffleWriteMetricsReporter writeMetrics,
      TaskContext taskContext,
      LinkedList<SpillInfo> spills,
      SpillCallback spillCallback) {

    this.initialSize = initialSize;
    this.uaoSize = uaoSize;
    this.preferDictionaryRatio = preferDictionaryRatio;
    this.compressionCodec = compressionCodec;
    this.compressionLevel = compressionLevel;
    this.checksumAlgorithm = checksumAlgorithm;
    this.partitionChecksums = partitionChecksums;
    this.writeMetrics = writeMetrics;
    this.taskContext = taskContext;
    this.spills = spills;
    this.spillCallback = spillCallback;

    this.spillInfo = null;
    this.allocator = allocator;

    // Allocate array for in-memory sorter.
    // As we cannot access the address of the internal array in the sorter, so we need to
    // allocate the array manually and expand the pointer array in the sorter.
    // We don't want in-memory sorter to allocate memory but the initial size cannot be zero.
    try {
      this.inMemSorter = new ShuffleInMemorySorter(allocator, 1, true);
    } catch (java.lang.IllegalAccessError e) {
      throw new java.lang.RuntimeException(
          "Error loading in-memory sorter check class path -- see "
              + "https://github.com/apache/arrow-datafusion-comet?tab=readme-ov-file#enable-comet-shuffle",
          e);
    }
    sorterArray = allocator.allocateArray(initialSize);
    this.inMemSorter.expandPointerArray(sorterArray);

    this.allocatedPages = new LinkedList<>();

    this.nativeLib = new Native();
    this.dataTypes = serializeSchema(schema);
  }

  /** Frees allocated memory pages of this writer */
  @Override
  public long freeMemory() {
    // We need to synchronize here because we may get the memory usage by calling
    // this method in the task thread.
    synchronized (this) {
      return super.freeMemory();
    }
  }

  @Override
  public long getMemoryUsage() {
    // We need to synchronize here because we may free the memory pages in another thread,
    // i.e. when spilling, but this method may be called in the task thread.
    synchronized (this) {
      long totalPageSize = super.getMemoryUsage();

      if (freed) {
        return totalPageSize;
      } else {
        return ((inMemSorter == null) ? 0 : inMemSorter.getMemoryUsage()) + totalPageSize;
      }
    }
  }

  @Override
  protected void spill(int required) throws IOException {
    spillCallback.onSpillRequired();
  }

  /** Free the pointer array held by this sorter. */
  public void freeArray() {
    synchronized (this) {
      inMemSorter.free();
      freed = true;
    }
  }

  /**
   * Reset the in-memory sorter's pointer array only after freeing up the memory pages holding the
   * records.
   */
  public void reset() {
    synchronized (this) {
      // We allocate pointer array outside the sorter.
      // So we can get array address which can be used by native code.
      inMemSorter.reset();
      sorterArray = allocator.allocateArray(initialSize);
      inMemSorter.expandPointerArray(sorterArray);
      freed = false;
    }
  }

  void setSpillInfo(SpillInfo spillInfo) {
    this.spillInfo = spillInfo;
  }

  public int numRecords() {
    return this.inMemSorter.numRecords();
  }

  public void writeSortedFileNative(boolean isLastFile, boolean tracingEnabled) throws IOException {
    // This call performs the actual sort.
    long arrayAddr = this.sorterArray.getBaseOffset();
    int pos = inMemSorter.numRecords();
    nativeLib.sortRowPartitionsNative(arrayAddr, pos, tracingEnabled);
    ShuffleInMemorySorter.ShuffleSorterIterator sortedRecords =
        new ShuffleInMemorySorter.ShuffleSorterIterator(pos, this.sorterArray, 0);

    // If there are no sorted records, so we don't need to create an empty spill file.
    if (!sortedRecords.hasNext()) {
      return;
    }

    final ShuffleWriteMetricsReporter writeMetricsToUse;

    if (isLastFile) {
      // We're writing the final non-spill file, so we _do_ want to count this as shuffle bytes.
      writeMetricsToUse = writeMetrics;
    } else {
      // We're spilling, so bytes written should be counted towards spill rather than write.
      // Create a dummy WriteMetrics object to absorb these metrics, since we don't want to count
      // them towards shuffle bytes written.
      writeMetricsToUse = new ShuffleWriteMetrics();
    }

    int currentPartition = -1;

    final RowPartition rowPartition = new RowPartition(initialSize);

    while (sortedRecords.hasNext()) {
      sortedRecords.loadNext();
      final int partition = sortedRecords.packedRecordPointer.getPartitionId();
      assert (partition >= currentPartition);
      if (partition != currentPartition) {
        // Switch to the new partition
        if (currentPartition != -1) {

          if (partitionChecksums.length > 0) {
            // If checksum is enabled, we need to update the checksum for the current partition.
            setChecksum(partitionChecksums[currentPartition]);
            setChecksumAlgo(checksumAlgorithm);
          }

          long written =
              doSpilling(
                  dataTypes,
                  spillInfo.file,
                  rowPartition,
                  writeMetricsToUse,
                  preferDictionaryRatio,
                  compressionCodec,
                  compressionLevel,
                  tracingEnabled);
          spillInfo.partitionLengths[currentPartition] = written;

          // Store the checksum for the current partition.
          partitionChecksums[currentPartition] = getChecksum();
        }
        currentPartition = partition;
      }

      final long recordPointer = sortedRecords.packedRecordPointer.getRecordPointer();
      final long recordOffsetInPage = allocator.getOffsetInPage(recordPointer);
      // Note that we need to skip over record key (partition id)
      // Note that we already use off-heap memory for serialized rows, so recordPage is always
      // null.
      int recordSizeInBytes = UnsafeAlignedOffset.getSize(null, recordOffsetInPage) - 4;
      long recordReadPosition = recordOffsetInPage + uaoSize + 4; // skip over record length too
      rowPartition.addRow(recordReadPosition, recordSizeInBytes);
    }

    if (currentPartition != -1) {
      if (partitionChecksums.length > 0) {
        // If checksum is enabled, we need to update the checksum for the last partition.
        setChecksum(partitionChecksums[currentPartition]);
        setChecksumAlgo(checksumAlgorithm);
      }

      long written =
          doSpilling(
              dataTypes,
              spillInfo.file,
              rowPartition,
              writeMetricsToUse,
              preferDictionaryRatio,
              compressionCodec,
              compressionLevel,
              tracingEnabled);
      spillInfo.partitionLengths[currentPartition] = written;

      // Store the checksum for the last partition.
      if (partitionChecksums.length > 0) {
        partitionChecksums[currentPartition] = getChecksum();
      }

      synchronized (spills) {
        spills.add(spillInfo);
      }
    }

    if (!isLastFile) { // i.e. this is a spill file
      // The current semantics of `shuffleRecordsWritten` seem to be that it's updated when
      // records
      // are written to disk, not when they enter the shuffle sorting code. DiskBlockObjectWriter
      // relies on its `recordWritten()` method being called in order to trigger periodic updates
      // to
      // `shuffleBytesWritten`. If we were to remove the `recordWritten()` call and increment that
      // counter at a higher-level, then the in-progress metrics for records written and bytes
      // written would get out of sync.
      //
      // When writing the last file, we pass `writeMetrics` directly to the DiskBlockObjectWriter;
      // in all other cases, we pass in a dummy write metrics to capture metrics, then copy those
      // metrics to the true write metrics here. The reason for performing this copying is so that
      // we can avoid reporting spilled bytes as shuffle write bytes.
      //
      // Note that we intentionally ignore the value of `writeMetricsToUse.shuffleWriteTime()`.
      // Consistent with ExternalSorter, we do not count this IO towards shuffle write time.
      // SPARK-3577 tracks the spill time separately.

      // This is guaranteed to be a ShuffleWriteMetrics based on the if check in the beginning
      // of this method.
      synchronized (writeMetrics) {
        writeMetrics.incRecordsWritten(((ShuffleWriteMetrics) writeMetricsToUse).recordsWritten());
        taskContext
            .taskMetrics()
            .incDiskBytesSpilled(((ShuffleWriteMetrics) writeMetricsToUse).bytesWritten());
      }
    }
  }

  public boolean hasSpaceForAnotherRecord() {
    return inMemSorter.hasSpaceForAnotherRecord();
  }

  public void expandPointerArray(LongArray newArray) {
    inMemSorter.expandPointerArray(newArray);
    this.sorterArray = newArray;
  }

  public void insertRecord(Object recordBase, long recordOffset, int length, int partitionId) {
    final Object base = currentPage.getBaseObject();
    final long recordAddress = allocator.encodePageNumberAndOffset(currentPage, pageCursor);
    UnsafeAlignedOffset.putSize(base, pageCursor, length);
    pageCursor += uaoSize;
    Platform.copyMemory(recordBase, recordOffset, base, pageCursor, length);
    pageCursor += length;
    inMemSorter.insertRecord(recordAddress, partitionId);
  }
}
