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

package org.apache.comet.parquet;

import java.io.IOException;

import org.apache.arrow.c.CometSchemaImporter;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReader;
import org.apache.spark.sql.types.DataType;

import org.apache.comet.vector.CometLazyVector;
import org.apache.comet.vector.CometVector;

public class LazyColumnReader extends ColumnReader {

  // Remember the largest skipped index for sanity checking.
  private int lastSkippedRowId = Integer.MAX_VALUE;

  // Track whether the underlying page is drained.
  private boolean isPageDrained = true;

  // Leftover number of rows that did not skip in the previous batch.
  private int numRowsToSkipFromPrevBatch;

  // The lazy vector being updated.
  private final CometLazyVector vector;

  public LazyColumnReader(
      DataType sparkReadType,
      ColumnDescriptor descriptor,
      CometSchemaImporter importer,
      int batchSize,
      boolean useDecimal128,
      boolean useLegacyDateTimestamp) {
    super(sparkReadType, descriptor, importer, batchSize, useDecimal128, useLegacyDateTimestamp);
    this.batchSize = 0; // the batch size is set later in `readBatch`
    this.vector = new CometLazyVector(sparkReadType, this, useDecimal128);
  }

  @Override
  public void setPageReader(PageReader pageReader) throws IOException {
    super.setPageReader(pageReader);
    lastSkippedRowId = Integer.MAX_VALUE;
    isPageDrained = true;
    numRowsToSkipFromPrevBatch = 0;
    currentNumValues = batchSize;
  }

  /**
   * Lazily read a batch of 'total' rows for this column. The includes: 1) Skip any unused rows from
   * the previous batch 2) Reset the native columnar batch 3) Reset tracking variables
   *
   * @param total the number of rows in the batch. MUST be <= the number of rows available in this
   *     column chunk.
   */
  @Override
  public void readBatch(int total) {
    // Before starting a new batch, take care of the remaining rows to skip from the previous batch.
    tryPageSkip(batchSize);
    numRowsToSkipFromPrevBatch += batchSize - currentNumValues;

    // Now first reset the current columnar batch so that it can be used to fill in a new batch
    // of values. Then, keep reading more data pages (via 'readBatch') until the current batch is
    // full, or we have read 'total' number of values.
    Native.resetBatch(nativeHandle);

    batchSize = total;
    currentNumValues = 0;
    lastSkippedRowId = -1;
  }

  @Override
  public CometVector currentBatch() {
    return vector;
  }

  /** Read all rows up to the `batchSize`. Expects no rows are skipped so far. */
  public void readAllBatch() {
    // All rows should be read without any skips so far
    assert (lastSkippedRowId == -1);

    readBatch(batchSize - 1, 0);
  }

  /**
   * Read at least up to `rowId`. It may read beyond `rowId` if enough rows available in the page.
   * It may skip reading rows before `rowId`. In case `rowId` is already read, return immediately.
   *
   * @param rowId the row index in the batch to read.
   * @return true if `rowId` is newly materialized, or false if `rowId` is already materialized.
   */
  public boolean materializeUpToIfNecessary(int rowId) {
    // Not allowed reading rowId if it may have skipped previously.
    assert (rowId > lastSkippedRowId);

    // If `rowId` is already materialized, return immediately.
    if (rowId < currentNumValues) return false;

    int numRowsWholePageSkipped = tryPageSkip(rowId);
    readBatch(rowId, numRowsWholePageSkipped);
    return true;
  }

  /**
   * Read up to `rowId` (inclusive). If the whole pages are skipped previously in `tryPageSkip()`,
   * pad the number of whole page skipped rows with nulls to the underlying vector before reading.
   *
   * @param rowId the row index in the batch to read.
   * @param numNullRowsToPad the number of nulls to pad before reading.
   */
  private void readBatch(int rowId, int numNullRowsToPad) {
    if (numRowsToSkipFromPrevBatch > 0) {
      // Reaches here only when starting a new batch and the page is previously drained
      readPage();
      isPageDrained = false;
      Native.skipBatch(nativeHandle, numRowsToSkipFromPrevBatch, true);
      numRowsToSkipFromPrevBatch = 0;
    }
    while (rowId >= currentNumValues) {
      int numRowsToRead = batchSize - currentNumValues;
      if (isPageDrained) {
        readPage();
      }
      int[] array = Native.readBatch(nativeHandle, numRowsToRead, numNullRowsToPad);
      int read = array[0];
      isPageDrained = read < numRowsToRead;
      currentNumValues += read;
      currentNumNulls += array[1];
      // No need to update numNullRowsToPad. numNullRowsToPad > 0 means there were whole page skips.
      // That guarantees that the Native.readBatch can read up to rowId in the current page.
    }
  }

  /**
   * Try to skip until `rowId` (exclusive). If possible, it skips whole underlying pages without
   * decompressing. In that case, it returns early at the page end, so that the next iteration can
   * lazily decide to `readPage()` or `tryPageSkip()` again.
   *
   * @param rowId the row index in the batch that it tries to skip up until (exclusive).
   * @return the number of rows that the whole page skips were applied.
   */
  private int tryPageSkip(int rowId) {
    int total = rowId - currentNumValues;
    int wholePageSkipped = 0;
    if (total > 0) {
      // First try to skip from the non-drained underlying page.
      int skipped = isPageDrained ? 0 : Native.skipBatch(nativeHandle, total);
      total -= skipped;
      isPageDrained = total > 0;
      if (isPageDrained) {
        ColumnPageReader columnPageReader = (ColumnPageReader) pageReader;
        // It is always `columnPageReader.getPageValueCount() > numRowsToSkipFromPriorBatch`
        int pageValueCount = columnPageReader.getPageValueCount() - numRowsToSkipFromPrevBatch;
        while (pageValueCount <= total) {
          // skip the entire page if the next page is small enough
          columnPageReader.skipPage();
          numRowsToSkipFromPrevBatch = 0;
          total -= pageValueCount;
          wholePageSkipped += pageValueCount;
          pageValueCount = columnPageReader.getPageValueCount();
        }
      }

      currentNumValues += skipped + wholePageSkipped;
      currentNumNulls += skipped;
      lastSkippedRowId = currentNumValues - 1;
    }
    return wholePageSkipped;
  }
}
