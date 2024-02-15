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

package org.apache.spark.sql.comet.execution.shuffle;

import java.io.*;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.internal.config.package$;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.shuffle.comet.CometShuffleMemoryAllocator;
import org.apache.spark.shuffle.sort.RowPartition;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.FileSegment;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.UnsafeAlignedOffset;

import com.google.common.annotations.VisibleForTesting;

import org.apache.comet.CometConf$;
import org.apache.comet.Native;

/**
 * This class has similar role of Spark `DiskBlockObjectWriter` class which is used to write shuffle
 * data to disk. For Comet, this is specialized to shuffle unsafe rows into disk in Arrow IPC format
 * using native code. Spark `DiskBlockObjectWriter` is not a `MemoryConsumer` as it can simply
 * stream the data to disk. However, for Comet, we need to buffer rows in memory page and then write
 * them to disk as batches in Arrow IPC format. So, we need to extend `MemoryConsumer` to be able to
 * spill the buffered rows to disk when memory pressure is high.
 *
 * <p>Similar to `CometShuffleExternalSorter`, this class also provides asynchronous spill
 * mechanism. But different from `CometShuffleExternalSorter`, as a writer class for Spark
 * hash-based shuffle, it writes all the rows for a partition into a single file, instead of each
 * file for each spill.
 */
public final class CometDiskBlockWriter {
  private static final Logger logger = LoggerFactory.getLogger(CometDiskBlockWriter.class);
  private static final ClassTag<Object> OBJECT_CLASS_TAG = ClassTag$.MODULE$.Object();

  /** List of all `NativeDiskBlockArrowIPCWriter`s of same shuffle task. */
  private static final LinkedList<CometDiskBlockWriter> currentWriters = new LinkedList<>();

  /** Queue of pending asynchronous spill tasks. */
  private ConcurrentLinkedQueue<Future<Void>> asyncSpillTasks = new ConcurrentLinkedQueue<>();

  /** List of `ArrowIPCWriter`s which are spilling. */
  private final LinkedList<ArrowIPCWriter> spillingWriters = new LinkedList<>();

  private final TaskContext taskContext;

  @VisibleForTesting static final int DEFAULT_INITIAL_SER_BUFFER_SIZE = 1024 * 1024;

  // Copied from Spark `org.apache.spark.shuffle.sort.PackedRecordPointer.MAXIMUM_PAGE_SIZE_BYTES`
  static final int MAXIMUM_PAGE_SIZE_BYTES = 1 << 27;

  /** The Comet allocator used to allocate pages. */
  private final CometShuffleMemoryAllocator allocator;

  /** The serializer used to write rows to memory page. */
  private final SerializerInstance serializer;

  /** The native library used to write rows to disk. */
  private final Native nativeLib;

  private final int uaoSize = UnsafeAlignedOffset.getUaoSize();
  private final StructType schema;
  private final ShuffleWriteMetricsReporter writeMetrics;
  private final File file;
  private long totalWritten = 0L;
  private boolean initialized = false;
  private final int initialBufferSize;
  private final boolean isAsync;
  private final int asyncThreadNum;
  private final ExecutorService threadPool;
  private final int numElementsForSpillThreshold;

  private final double preferDictionaryRatio;

  /** The current active writer. All incoming rows will be inserted into it. */
  private ArrowIPCWriter activeWriter;

  /** A flag indicating whether we are in the process of spilling. */
  private boolean spilling = false;

  /** The buffer used to store serialized row. */
  private ExposedByteArrayOutputStream serBuffer;

  private SerializationStream serOutputStream;

  private long outputRecords = 0;

  private long insertRecords = 0;

  CometDiskBlockWriter(
      File file,
      TaskMemoryManager taskMemoryManager,
      TaskContext taskContext,
      SerializerInstance serializer,
      StructType schema,
      ShuffleWriteMetricsReporter writeMetrics,
      SparkConf conf,
      boolean isAsync,
      int asyncThreadNum,
      ExecutorService threadPool) {
    this.allocator =
        CometShuffleMemoryAllocator.getInstance(
            conf,
            taskMemoryManager,
            Math.min(MAXIMUM_PAGE_SIZE_BYTES, taskMemoryManager.pageSizeBytes()));
    this.nativeLib = new Native();

    this.taskContext = taskContext;
    this.serializer = serializer;
    this.schema = schema;
    this.writeMetrics = writeMetrics;
    this.file = file;
    this.isAsync = isAsync;
    this.asyncThreadNum = asyncThreadNum;
    this.threadPool = threadPool;

    this.initialBufferSize =
        (int) (long) conf.get(package$.MODULE$.SHUFFLE_SORT_INIT_BUFFER_SIZE());

    this.numElementsForSpillThreshold =
        (int) CometConf$.MODULE$.COMET_EXEC_SHUFFLE_SPILL_THRESHOLD().get();

    this.preferDictionaryRatio =
        (double) CometConf$.MODULE$.COMET_SHUFFLE_PREFER_DICTIONARY_RATIO().get();

    this.activeWriter = new ArrowIPCWriter();

    synchronized (currentWriters) {
      currentWriters.add(this);
    }
  }

  public void setChecksumAlgo(String checksumAlgo) {
    this.activeWriter.setChecksumAlgo(checksumAlgo);
  }

  public void setChecksum(long checksum) {
    this.activeWriter.setChecksum(checksum);
  }

  public long getChecksum() {
    return this.activeWriter.getChecksum();
  }

  private void doSpill(boolean forceSync) throws IOException {
    // We only allow spilling request from `NativeDiskBlockArrowIPCWriter`.
    if (spilling || activeWriter.numRecords() == 0) {
      return;
    }

    // Set this into spilling state first, so it cannot recursively trigger another spill on itself.
    spilling = true;

    if (isAsync && !forceSync) {
      // Although we can continue to submit spill tasks to thread pool, buffering more rows in
      // memory page will increase memory usage. So, we need to wait for at least one spilling
      // task to finish.
      while (asyncSpillTasks.size() == asyncThreadNum) {
        for (Future<Void> task : asyncSpillTasks) {
          if (task.isDone()) {
            asyncSpillTasks.remove(task);
            break;
          }
        }
      }

      final ArrowIPCWriter spillingWriter = activeWriter;
      activeWriter = new ArrowIPCWriter();

      spillingWriters.add(spillingWriter);

      asyncSpillTasks.add(
          threadPool.submit(
              new Runnable() {
                @Override
                public void run() {
                  try {
                    long written = spillingWriter.doSpilling(false);
                    totalWritten += written;
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  } finally {
                    spillingWriter.freeMemory();
                    spillingWriters.remove(spillingWriter);
                  }
                }
              },
              null));

    } else {
      // Spill in a synchronous way.
      // This spill could be triggered by other thread (i.e., other `CometDiskBlockWriter`),
      // so we need to synchronize it.
      synchronized (CometDiskBlockWriter.this) {
        totalWritten += activeWriter.doSpilling(false);
        activeWriter.freeMemory();
      }
    }

    spilling = false;
  }

  public long getOutputRecords() {
    return outputRecords;
  }

  /** Serializes input row and inserts into current allocated page. */
  public void insertRow(UnsafeRow row, int partitionId) throws IOException {
    insertRecords++;

    if (!initialized) {
      serBuffer = new ExposedByteArrayOutputStream(DEFAULT_INITIAL_SER_BUFFER_SIZE);
      serOutputStream = serializer.serializeStream(serBuffer);

      initialized = true;
    }

    serBuffer.reset();
    serOutputStream.writeKey(partitionId, OBJECT_CLASS_TAG);
    serOutputStream.writeValue(row, OBJECT_CLASS_TAG);
    serOutputStream.flush();

    final int serializedRecordSize = serBuffer.size();
    assert (serializedRecordSize > 0);

    // While proceeding with possible spilling and inserting the record, we need to synchronize
    // it, because other threads may be spilling this writer at the same time.
    synchronized (CometDiskBlockWriter.this) {
      if (activeWriter.numRecords() >= numElementsForSpillThreshold) {
        logger.info(
            "Spilling data because number of spilledRecords crossed the threshold "
                + numElementsForSpillThreshold);
        // Spill the current writer
        doSpill(false);
        if (activeWriter.numRecords() != 0) {
          throw new RuntimeException(
              "activeWriter.numRecords()(" + activeWriter.numRecords() + ") != 0");
        }
      }

      // Need 4 or 8 bytes to store the record length.
      final int required = serializedRecordSize + uaoSize;
      // Acquire enough memory to store the record.
      // If we cannot acquire enough memory, we will spill current writers.
      if (!activeWriter.acquireNewPageIfNecessary(required)) {
        // Spilling is happened, initiate new memory page for new writer.
        activeWriter.initialCurrentPage(required);
      }
      activeWriter.insertRecord(
          serBuffer.getBuf(), Platform.BYTE_ARRAY_OFFSET, serializedRecordSize);
    }
  }

  FileSegment close() throws IOException {
    if (isAsync) {
      for (Future<Void> task : asyncSpillTasks) {
        try {
          task.get();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }

    totalWritten += activeWriter.doSpilling(true);

    if (outputRecords != insertRecords) {
      throw new RuntimeException(
          "outputRecords("
              + outputRecords
              + ") != insertRecords("
              + insertRecords
              + "). Please file a bug report.");
    }

    serBuffer = null;
    serOutputStream = null;

    activeWriter.freeMemory();

    synchronized (currentWriters) {
      currentWriters.remove(this);
    }

    return new FileSegment(file, 0, totalWritten);
  }

  File getFile() {
    return file;
  }

  /** Returns the memory usage of active writer. */
  long getActiveMemoryUsage() {
    return activeWriter.getMemoryUsage();
  }

  void freeMemory() {
    for (ArrowIPCWriter writer : spillingWriters) {
      writer.freeMemory();
    }
    activeWriter.freeMemory();
  }

  class ArrowIPCWriter extends SpillWriter {
    /**
     * The list of addresses and sizes of rows buffered in memory page which wait for
     * spilling/shuffle.
     */
    private final RowPartition rowPartition;

    ArrowIPCWriter() {
      rowPartition = new RowPartition(initialBufferSize);

      this.allocatedPages = new LinkedList<>();
      this.allocator = CometDiskBlockWriter.this.allocator;

      this.nativeLib = CometDiskBlockWriter.this.nativeLib;
      this.dataTypes = serializeSchema(schema);
    }

    /** Inserts a record into current allocated page. */
    void insertRecord(Object recordBase, long recordOffset, int length) {
      // This `ArrowIPCWriter` could be spilled by other threads, so we need to synchronize it.
      final Object base = currentPage.getBaseObject();

      // Add row addresses
      final long recordAddress = allocator.encodePageNumberAndOffset(currentPage, pageCursor);
      rowPartition.addRow(allocator.getOffsetInPage(recordAddress) + uaoSize + 4, length - 4);

      // Write the record (row) size
      UnsafeAlignedOffset.putSize(base, pageCursor, length);
      pageCursor += uaoSize;
      // Copy the record (row) data from serialized buffer to page
      Platform.copyMemory(recordBase, recordOffset, base, pageCursor, length);
      pageCursor += length;
    }

    int numRecords() {
      return rowPartition.getNumRows();
    }

    /** Spills the current in-memory records of this `ArrowIPCWriter` to disk. */
    long doSpilling(boolean isLast) throws IOException {
      final ShuffleWriteMetricsReporter writeMetricsToUse;

      if (isLast) {
        // We're writing the final non-spill file, so we _do_ want to count this as shuffle bytes.
        writeMetricsToUse = writeMetrics;
      } else {
        // We're spilling, so bytes written should be counted towards spill rather than write.
        // Create a dummy WriteMetrics object to absorb these metrics, since we don't want to count
        // them towards shuffle bytes written.
        writeMetricsToUse = new ShuffleWriteMetrics();
      }

      final long written;

      // All threads are writing to the same file, so we need to synchronize it.
      synchronized (file) {
        outputRecords += rowPartition.getNumRows();
        written =
            doSpilling(dataTypes, file, rowPartition, writeMetricsToUse, preferDictionaryRatio);
      }

      // Update metrics
      // Other threads may be updating the metrics at the same time, so we need to synchronize it.
      synchronized (writeMetrics) {
        if (!isLast) {
          writeMetrics.incRecordsWritten(
              ((ShuffleWriteMetrics) writeMetricsToUse).recordsWritten());
          taskContext
              .taskMetrics()
              .incDiskBytesSpilled(((ShuffleWriteMetrics) writeMetricsToUse).bytesWritten());
        }
      }

      return written;
    }

    /**
     * Spills the current in-memory records of all `NativeDiskBlockArrowIPCWriter`s until required
     * memory is acquired.
     */
    @Override
    protected void spill(int required) throws IOException {
      // Cannot allocate enough memory, spill and try again
      synchronized (currentWriters) {
        // Spill from the largest writer first to maximize the amount of memory we can
        // acquire
        Collections.sort(
            currentWriters,
            new Comparator<CometDiskBlockWriter>() {
              @Override
              public int compare(CometDiskBlockWriter lhs, CometDiskBlockWriter rhs) {
                long lhsMemoryUsage = lhs.getActiveMemoryUsage();
                long rhsMemoryUsage = rhs.getActiveMemoryUsage();
                return Long.compare(rhsMemoryUsage, lhsMemoryUsage);
              }
            });

        for (CometDiskBlockWriter writer : currentWriters) {
          // Force to spill the writer in a synchronous way, otherwise, we may not be able to
          // acquire enough memory.
          writer.doSpill(true);

          if (allocator.getAvailableMemory() >= required) {
            break;
          }
        }
      }
    }
  }
}
