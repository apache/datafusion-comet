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
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.shuffle.comet.CometShuffleMemoryAllocatorTrait;
import org.apache.spark.sql.comet.execution.shuffle.SpillInfo;
import org.apache.spark.sql.comet.execution.shuffle.SpillWriter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.UnsafeAlignedOffset;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;

import org.apache.comet.CometConf;
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
  private final boolean offsetsAsLengths;
  // The checksum algorithm's id for native code, or -1 when shuffle checksums are disabled.
  private final int checksumAlgoId;

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
   * @param offsetsAsLengths Whether to store offsets as lengths where the codec takes it
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
      boolean offsetsAsLengths,
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
    this.offsetsAsLengths = offsetsAsLengths;
    this.checksumAlgoId =
        partitionChecksums.length > 0 ? checksumAlgorithmId(checksumAlgorithm) : -1;
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
    boolean adopted = false;
    try {
      sorterArray = allocator.allocateArray(initialSize);
      this.inMemSorter.expandPointerArray(sorterArray);
      adopted = true;

      this.allocatedPages = new LinkedList<>();

      this.nativeLib = new Native();
      this.dataTypes = serializeSchema(schema);
    } catch (Throwable t) {
      // This writer is never handed to Spark when its constructor fails, so nothing else could
      // reclaim what was allocated so far; free it here to keep the shared pool leak-free.
      // Before adoption the sorter still owns only its initial one-entry array; after adoption
      // it owns `sorterArray`, and `free()` releases whichever it holds.
      if (!adopted && sorterArray != null) {
        allocator.freeArray(sorterArray);
        sorterArray = null;
      }
      this.inMemSorter.free();
      throw t;
    }
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

  /**
   * Memory held by the in-memory sorter's pointer array alone, excluding the data pages. This is
   * what the pointer array growth in {@link CometShuffleExternalSorter} must be sized from: sizing
   * it from {@link #getMemoryUsage()} would grow the array in proportion to the data pages instead
   * of doubling it.
   */
  public long getPointerArrayMemoryUsage() {
    synchronized (this) {
      if (freed || inMemSorter == null) {
        return 0;
      }
      return inMemSorter.getMemoryUsage();
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

  /**
   * Sorts the buffered records by partition and writes them to the spill file set by {@link
   * #setSpillInfo}, every partition in one native call, recording each partition's length and
   * checksum.
   */
  public void writeSortedPartitions(boolean isLastFile, boolean tracingEnabled) throws IOException {
    final int numRecords = inMemSorter.numRecords();
    // If there are no records, we don't need to create an empty spill file.
    if (numRecords == 0) {
      return;
    }

    // This call performs the actual sort. The sorter is a radix sort one, so it keeps the second
    // half of the pointer array free, and the native sort uses it as scratch space.
    long arrayAddr = this.sorterArray.getBaseOffset();
    nativeLib.sortRowPartitionsNative(
        arrayAddr, numRecords, this.sorterArray.size(), tracingEnabled);

    // Native code resolves the sorted record pointers through the page table, so the rows are
    // read where they lie, and continues each partition's checksum over the bytes it writes.
    long start = System.nanoTime();
    long encodeNanos =
        nativeLib.writeSortedPartitionsNative(
            arrayAddr,
            numRecords,
            pageAddresses(),
            uaoSize,
            dataTypes,
            spillInfo.file.getAbsolutePath(),
            preferDictionaryRatio,
            CometConf.jvmShuffleBatchSize(),
            checksumAlgoId,
            partitionChecksums,
            spillInfo.partitionLengths,
            compressionCodec,
            compressionLevel,
            offsetsAsLengths,
            tracingEnabled);
    addEncodeNanos(encodeNanos);

    long written = 0;
    for (long length : spillInfo.partitionLengths) {
      written += length;
    }

    // Other threads may be updating the metrics at the same time, so we need to synchronize it.
    // As in Spark's ShuffleExternalSorter, a spill's records count as written, but its bytes count
    // as spilled rather than as shuffle bytes, and its time does not count as shuffle write time
    // (SPARK-3577 tracks spill time separately).
    synchronized (writeMetrics) {
      writeMetrics.incRecordsWritten(numRecords);
      if (isLastFile) {
        writeMetrics.incWriteTime(System.nanoTime() - start);
        writeMetrics.incBytesWritten(written);
      } else {
        taskContext.taskMetrics().incDiskBytesSpilled(written);
      }
    }

    synchronized (spills) {
      spills.add(spillInfo);
    }
  }

  /**
   * The base address of each page holding records, indexed by page number, for native code to
   * resolve the record pointers through. Pages this sorter does not hold are 0.
   */
  private long[] pageAddresses() {
    synchronized (this) {
      int maxPageNumber = -1;
      for (MemoryBlock page : allocatedPages) {
        maxPageNumber = Math.max(maxPageNumber, page.pageNumber);
      }
      long[] addresses = new long[maxPageNumber + 1];
      for (MemoryBlock page : allocatedPages) {
        addresses[page.pageNumber] = page.getBaseOffset();
      }
      return addresses;
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
