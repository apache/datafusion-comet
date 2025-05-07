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
import java.util.concurrent.*;
import javax.annotation.Nullable;

import scala.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.memory.SparkOutOfMemoryError;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.shuffle.comet.CometShuffleChecksumSupport;
import org.apache.spark.shuffle.comet.CometShuffleMemoryAllocatorTrait;
import org.apache.spark.shuffle.comet.TooLargePageException;
import org.apache.spark.sql.comet.execution.shuffle.CometUnsafeShuffleWriter;
import org.apache.spark.sql.comet.execution.shuffle.ShuffleThreadPool;
import org.apache.spark.sql.comet.execution.shuffle.SpillInfo;
import org.apache.spark.sql.comet.execution.shuffle.SpillWriter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.TempShuffleBlockId;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.UnsafeAlignedOffset;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.util.Utils;

import org.apache.comet.CometConf$;
import org.apache.comet.Native;

/**
 * An external sorter that is specialized for sort-based shuffle.
 *
 * <p>Incoming records are appended to data pages. When all records have been inserted (or when the
 * current thread's shuffle memory limit is reached), the in-memory records are sorted according to
 * their partition ids using native sorter. The sorted records are then written to a single output
 * file (or multiple files, if we've spilled).
 *
 * <p>Unlike {@link org.apache.spark.util.collection.ExternalSorter}, this sorter does not merge its
 * spill files. Instead, this merging is performed in {@link CometUnsafeShuffleWriter}, which uses a
 * specialized merge procedure that avoids extra serialization/deserialization.
 *
 * <p>This sorter provides async spilling write mode. When spilling, it will submit a task to thread
 * pool to write shuffle spilling file. After submitting the task, it will continue to buffer, sort
 * incoming records and submit another spilling task once spilling threshold reached again or memory
 * is not enough to buffer incoming records. Each spilling task will write a shuffle spilling file
 * separately. After all records have been sorted and spilled, all spill files will be merged by
 * {@link CometUnsafeShuffleWriter}.
 */
public final class CometShuffleExternalSorter implements CometShuffleChecksumSupport {

  private static final Logger logger = LoggerFactory.getLogger(CometShuffleExternalSorter.class);

  public static int MAXIMUM_PAGE_SIZE_BYTES = PackedRecordPointer.MAXIMUM_PAGE_SIZE_BYTES;
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

  /** All sorters with memory pages used by the sorters. */
  private final ConcurrentLinkedQueue<SpillSorter> spillingSorters = new ConcurrentLinkedQueue<>();

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

  /** Whether to write shuffle spilling file in async mode */
  private final boolean isAsync;

  /** Thread pool shared for async spilling write */
  private final ExecutorService threadPool;

  private final int threadNum;

  private ConcurrentLinkedQueue<Future<Void>> asyncSpillTasks = new ConcurrentLinkedQueue<>();

  private boolean spilling = false;

  private final int uaoSize = UnsafeAlignedOffset.getUaoSize();
  private final double preferDictionaryRatio;

  public CometShuffleExternalSorter(
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

    this.isAsync = (boolean) CometConf$.MODULE$.COMET_COLUMNAR_SHUFFLE_ASYNC_ENABLED().get();

    if (isAsync) {
      this.threadNum = (int) CometConf$.MODULE$.COMET_COLUMNAR_SHUFFLE_ASYNC_THREAD_NUM().get();
      assert (this.threadNum > 0);
      this.threadPool = ShuffleThreadPool.getThreadPool();
    } else {
      this.threadNum = 0;
      this.threadPool = null;
    }

    this.activeSpillSorter = new SpillSorter();

    this.preferDictionaryRatio =
        (double) CometConf$.MODULE$.COMET_SHUFFLE_PREFER_DICTIONARY_RATIO().get();
  }

  public long[] getChecksums() {
    return partitionChecksums;
  }

  /** Sort and spill the current records in response to memory pressure. */
  public void spill() throws IOException {
    if (spilling || activeSpillSorter == null || activeSpillSorter.numRecords() == 0) {
      return;
    }

    // In async mode, if new in-memory sorter cannot allocate required array, it triggers spill
    // here. This method will initiate new sorter following normal spill logic and casue stack
    // overflow eventually. So we need to avoid triggering spilling again while spilling. But
    // we cannot make this as "synchronized" because it will block the caller thread.
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

    if (isAsync) {
      SpillSorter spillingSorter = activeSpillSorter;
      Callable<Void> task =
          () -> {
            spillingSorter.writeSortedFileNative(false);
            final long spillSize = spillingSorter.freeMemory();
            spillingSorter.freeArray();
            spillingSorters.remove(spillingSorter);

            // Reset the in-memory sorter's pointer array only after freeing up the memory pages
            // holding the records. Otherwise, if the task is over allocated memory, then without
            // freeing the memory pages, we might not be able to get memory for the pointer array.
            synchronized (CometShuffleExternalSorter.this) {
              taskContext.taskMetrics().incMemoryBytesSpilled(spillSize);
            }

            return null;
          };

      spillingSorters.add(spillingSorter);
      asyncSpillTasks.add(threadPool.submit(task));

      while (asyncSpillTasks.size() == threadNum) {
        for (Future<Void> spillingTask : asyncSpillTasks) {
          if (spillingTask.isDone()) {
            asyncSpillTasks.remove(spillingTask);
            break;
          }
        }
      }

      activeSpillSorter = new SpillSorter();
    } else {
      activeSpillSorter.writeSortedFileNative(false);
      final long spillSize = activeSpillSorter.freeMemory();
      activeSpillSorter.reset();

      // Reset the in-memory sorter's pointer array only after freeing up the memory pages holding
      // the
      // records. Otherwise, if the task is over allocated memory, then without freeing the memory
      // pages, we might not be able to get memory for the pointer array.
      synchronized (CometShuffleExternalSorter.this) {
        taskContext.taskMetrics().incMemoryBytesSpilled(spillSize);
      }
    }

    spilling = false;
  }

  private long getMemoryUsage() {
    long totalPageSize = 0;
    for (SpillSorter sorter : spillingSorters) {
      totalPageSize += sorter.getMemoryUsage();
    }
    if (activeSpillSorter != null) {
      totalPageSize += activeSpillSorter.getMemoryUsage();
    }
    return totalPageSize;
  }

  private void updatePeakMemoryUsed() {
    long mem = getMemoryUsage();
    if (mem > peakMemoryUsedBytes) {
      peakMemoryUsedBytes = mem;
    }
  }

  /** Return the peak memory used so far, in bytes. */
  public long getPeakMemoryUsedBytes() {
    updatePeakMemoryUsed();
    return peakMemoryUsedBytes;
  }

  private long freeMemory() {
    updatePeakMemoryUsed();
    long memoryFreed = 0;
    if (isAsync) {
      for (SpillSorter sorter : spillingSorters) {
        memoryFreed += sorter.freeMemory();
        sorter.freeArray();
      }
    }
    memoryFreed += activeSpillSorter.freeMemory();
    activeSpillSorter.freeArray();

    return memoryFreed;
  }

  /** Force all memory and spill files to be deleted; called by shuffle error-handling code. */
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
  public SpillInfo[] closeAndGetSpills() throws IOException {
    if (activeSpillSorter != null) {
      // Do not count the final file towards the spill count.
      final Tuple2<TempShuffleBlockId, File> spilledFileInfo =
          blockManager.diskBlockManager().createTempShuffleBlock();
      final File file = spilledFileInfo._2();
      final TempShuffleBlockId blockId = spilledFileInfo._1();
      final SpillInfo spillInfo = new SpillInfo(numPartitions, file, blockId);

      // Waits for all async tasks to finish.
      if (isAsync) {
        for (Future<Void> task : asyncSpillTasks) {
          try {
            task.get();
          } catch (Exception e) {
            throw new IOException(e);
          }
        }

        asyncSpillTasks.clear();
      }

      activeSpillSorter.setSpillInfo(spillInfo);
      activeSpillSorter.writeSortedFileNative(true);

      freeMemory();
    }

    return spills.toArray(new SpillInfo[spills.size()]);
  }

  class SpillSorter extends SpillWriter {
    private boolean freed = false;

    private SpillInfo spillInfo;

    // These variables are reset after spilling:
    @Nullable private ShuffleInMemorySorter inMemSorter;

    // This external sorter can call native code to sort partition ids and record pointers of rows.
    // In order to do that, we need pass the address of the internal array in the sorter to native.
    // But we cannot access it as it is private member in the Spark sorter. Instead, we allocate
    // the array and assign the pointer array in the sorter.
    private LongArray sorterArray;

    SpillSorter() {
      this.spillInfo = null;

      this.allocator = CometShuffleExternalSorter.this.allocator;

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
      CometShuffleExternalSorter.this.spill();
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
      // We allocate pointer array outside the sorter.
      // So we can get array address which can be used by native code.
      inMemSorter.reset();
      sorterArray = allocator.allocateArray(initialSize);
      inMemSorter.expandPointerArray(sorterArray);
    }

    void setSpillInfo(SpillInfo spillInfo) {
      this.spillInfo = spillInfo;
    }

    public int numRecords() {
      return this.inMemSorter.numRecords();
    }

    public void writeSortedFileNative(boolean isLastFile) throws IOException {
      // This call performs the actual sort.
      long arrayAddr = this.sorterArray.getBaseOffset();
      int pos = inMemSorter.numRecords();
      nativeLib.sortRowPartitionsNative(arrayAddr, pos);
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
                    compressionLevel);
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
        long written =
            doSpilling(
                dataTypes,
                spillInfo.file,
                rowPartition,
                writeMetricsToUse,
                preferDictionaryRatio,
                compressionCodec,
                compressionLevel);
        spillInfo.partitionLengths[currentPartition] = written;

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
          writeMetrics.incRecordsWritten(
              ((ShuffleWriteMetrics) writeMetricsToUse).recordsWritten());
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
}
