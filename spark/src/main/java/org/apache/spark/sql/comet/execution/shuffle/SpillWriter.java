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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Locale;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.memory.SparkOutOfMemoryError;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.shuffle.comet.CometShuffleMemoryAllocatorTrait;
import org.apache.spark.shuffle.sort.RowPartition;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.memory.MemoryBlock;

import org.apache.comet.CometConf;
import org.apache.comet.Native;
import org.apache.comet.serde.QueryPlanSerde$;

/**
 * The interface for writing records into disk. This interface takes input rows and stores them in
 * allocated memory pages. When certain condition is met, the writer will spill the content of
 * memory to disk.
 */
public abstract class SpillWriter {
  private static final Logger logger = LoggerFactory.getLogger(SpillWriter.class);

  /**
   * Memory pages that hold the records being sorted. The pages in this list are freed when
   * spilling, although in principle we could recycle these pages across spills (on the other hand,
   * this might not be necessary if we maintained a pool of re-usable pages in the TaskMemoryManager
   * itself).
   */
  protected LinkedList<MemoryBlock> allocatedPages;

  @Nullable protected MemoryBlock currentPage = null;
  protected long pageCursor = -1;

  // The memory allocator for this sorter. It is used to allocate/free memory pages for this sorter.
  // Because we need to allocate off-heap memory regardless of configured Spark memory mode
  // (on-heap/off-heap), we need a separate memory allocator.
  protected CometShuffleMemoryAllocatorTrait allocator;

  protected Native nativeLib;

  protected byte[][] dataTypes;

  // 0: CRC32, 1: Adler32. Spark uses Adler32 by default.
  protected int checksumAlgo = 1;
  protected long checksum = -1;

  /** Serialize row schema to byte array. */
  protected byte[][] serializeSchema(StructType schema) {
    byte[][] dataTypes = new byte[schema.length()][];
    for (int i = 0; i < schema.length(); i++) {
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      try {
        QueryPlanSerde$.MODULE$
            .serializeDataType(schema.apply(i).dataType())
            .get()
            .writeTo(outputStream);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      dataTypes[i] = outputStream.toByteArray();
    }

    return dataTypes;
  }

  protected void setChecksumAlgo(String checksumAlgo) {
    String algo = checksumAlgo.toLowerCase(Locale.ROOT);

    if (algo.equals("crc32")) {
      this.checksumAlgo = 0;
    } else if (algo.equals("adler32")) {
      this.checksumAlgo = 1;
    } else {
      throw new UnsupportedOperationException(
          "Unsupported shuffle checksum algorithm: " + checksumAlgo);
    }
  }

  protected void setChecksum(long checksum) {
    this.checksum = checksum;
  }

  protected long getChecksum() {
    return checksum;
  }

  /**
   * Spills the current in-memory records to disk.
   *
   * @param required the amount of required memory.
   */
  protected abstract void spill(int required) throws IOException;

  /**
   * Allocates more memory in order to insert an additional record. This will request additional
   * memory from the memory manager and spill if the requested memory can not be obtained.
   *
   * @param required the required space in the data page, in bytes, including space for storing the
   *     record size. This must be less than or equal to the page size (records that exceed the page
   *     size are handled via a different code path which uses special overflow pages).
   * @return true if the memory is allocated successfully, false otherwise. If false is returned, it
   *     means spilling is happening and the caller should not continue insert into this writer.
   */
  public boolean acquireNewPageIfNecessary(int required) {
    if (currentPage == null
        || pageCursor + required > currentPage.getBaseOffset() + currentPage.size()) {
      // TODO: try to find space in previous pages
      try {
        currentPage = allocator.allocate(required);
      } catch (SparkOutOfMemoryError error) {
        try {
          // Cannot allocate enough memory, spill
          spill(required);
          return false;
        } catch (IOException e) {
          throw new RuntimeException("Unable to spill() in order to acquire " + required, e);
        }
      }
      assert (currentPage != null);
      pageCursor = currentPage.getBaseOffset();
      allocatedPages.add(currentPage);
    }
    return true;
  }

  /** Allocates initial memory page */
  public void initialCurrentPage(int required) {
    assert (currentPage == null);
    try {
      currentPage = allocator.allocate(required);
    } catch (SparkOutOfMemoryError e) {
      logger.error("Unable to acquire {} bytes of memory", required);
      throw e;
    }
    assert (currentPage != null);
    pageCursor = currentPage.getBaseOffset();
    allocatedPages.add(currentPage);
  }

  /** The core logic of spilling buffered rows into disk. */
  protected long doSpilling(
      byte[][] dataTypes,
      File file,
      RowPartition rowPartition,
      ShuffleWriteMetricsReporter writeMetricsToUse,
      double preferDictionaryRatio,
      String compressionCodec,
      int compressionLevel) {
    long[] addresses = rowPartition.getRowAddresses();
    int[] sizes = rowPartition.getRowSizes();

    boolean checksumEnabled = checksum != -1;
    long currentChecksum = checksumEnabled ? checksum : 0L;

    long start = System.nanoTime();
    int batchSize = (int) CometConf.COMET_COLUMNAR_SHUFFLE_BATCH_SIZE().get();
    boolean enableFastEncoding = (boolean) CometConf.COMET_SHUFFLE_ENABLE_FAST_ENCODING().get();
    long[] results =
        nativeLib.writeSortedFileNative(
            addresses,
            sizes,
            dataTypes,
            file.getAbsolutePath(),
            preferDictionaryRatio,
            batchSize,
            checksumEnabled,
            checksumAlgo,
            currentChecksum,
            compressionCodec,
            compressionLevel,
            enableFastEncoding);

    long written = results[0];
    checksum = results[1];

    rowPartition.reset();

    // Update metrics
    // Other threads may be updating the metrics at the same time, so we need to synchronize it.
    synchronized (writeMetricsToUse) {
      writeMetricsToUse.incWriteTime(System.nanoTime() - start);
      writeMetricsToUse.incRecordsWritten(addresses.length);
      writeMetricsToUse.incBytesWritten(written);
    }

    return written;
  }

  /** Frees allocated memory pages of this writer */
  public long freeMemory() {
    long freed = 0L;
    for (MemoryBlock block : allocatedPages) {
      freed += block.size();
      allocator.free(block);
    }
    allocatedPages.clear();
    currentPage = null;
    pageCursor = 0;

    return freed;
  }

  /** Returns the amount of memory used by this writer, in bytes. */
  public long getMemoryUsage() {
    // Assume this method won't be called on a spilling writer, so we don't need to synchronize it.
    long used = 0;

    for (MemoryBlock block : allocatedPages) {
      used += block.size();
    }

    return used;
  }
}
