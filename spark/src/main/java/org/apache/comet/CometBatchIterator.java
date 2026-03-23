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

package org.apache.comet;

import scala.collection.Iterator;

import org.apache.spark.sql.vectorized.ColumnarBatch;

import org.apache.comet.vector.CometSelectionVector;
import org.apache.comet.vector.CometVector;
import org.apache.comet.vector.NativeUtil;

/**
 * Iterator for fetching batches from JVM to native code. Usually called via JNI from native
 * ScanExec.
 *
 * <p>Ownership of Arrow buffer memory is transferred to native code via the Arrow C Data Interface
 * release callback mechanism. When {@link #next} is called, each vector is exported via {@code
 * Data.exportVector()}, which calls {@code ArrowBuf.getReferenceManager().retain()} on every
 * underlying buffer. The JVM-side vector is then closed immediately, releasing the JVM reference.
 * The buffers remain alive until native code calls the Arrow release callback, at which point the
 * retained reference is released and the memory is freed.
 *
 * <p>If the task is cancelled before a batch is exported, {@link #close} releases the JVM
 * references directly so the allocator can be closed cleanly.
 */
public class CometBatchIterator implements AutoCloseable {
  private final Iterator<ColumnarBatch> input;
  private final NativeUtil nativeUtil;
  private ColumnarBatch currentBatch = null;

  CometBatchIterator(Iterator<ColumnarBatch> input, NativeUtil nativeUtil) {
    this.input = input;
    this.nativeUtil = nativeUtil;
  }

  /**
   * Fetch the next input batch.
   *
   * @return Number of rows in next batch or -1 if no batches left.
   */
  public int hasNext() {
    if (currentBatch == null) {
      if (input.hasNext()) {
        currentBatch = input.next();
      }
    }
    if (currentBatch == null) {
      return -1;
    } else {
      return currentBatch.numRows();
    }
  }

  /**
   * Export the current batch to native code via the Arrow C Data Interface and release JVM
   * references. After this call the buffer memory is owned by native code and will be freed through
   * the Arrow release callback.
   *
   * @param arrayAddrs The addresses of the ArrowArray structures.
   * @param schemaAddrs The addresses of the ArrowSchema structures.
   * @return the number of rows of the current batch. -1 if there is no more batch.
   */
  public int next(long[] arrayAddrs, long[] schemaAddrs) {
    if (currentBatch == null) {
      return -1;
    }

    // Export the batch via the Arrow C Data Interface. ArrayExporter.export() calls
    // arrowBuf.getReferenceManager().retain() for every buffer, so the ExportedArrayPrivateData
    // keeps the buffers alive until native calls the release callback.
    int numRows = nativeUtil.exportBatch(arrayAddrs, schemaAddrs, currentBatch);

    // Release JVM-side vector references. The buffers remain alive via the retained references
    // held by the Arrow exporter and will be freed when native calls the release callback.
    closeBatchVectors(currentBatch);
    currentBatch = null;

    return numRows;
  }

  /**
   * Check if the current batch has selection vectors for all columns.
   *
   * @return true if all columns are CometSelectionVector instances, false otherwise
   */
  public boolean hasSelectionVectors() {
    if (currentBatch == null) {
      return false;
    }

    // Check if all columns are CometSelectionVector instances
    for (int i = 0; i < currentBatch.numCols(); i++) {
      if (!(currentBatch.column(i) instanceof CometSelectionVector)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Export selection indices for all columns when they are selection vectors.
   *
   * @param arrayAddrs The addresses of the ArrowArray structures for indices
   * @param schemaAddrs The addresses of the ArrowSchema structures for indices
   * @return Number of selection indices arrays exported
   */
  public int exportSelectionIndices(long[] arrayAddrs, long[] schemaAddrs) {
    if (currentBatch == null) {
      return 0;
    }

    int exportCount = 0;
    for (int i = 0; i < currentBatch.numCols(); i++) {
      if (currentBatch.column(i) instanceof CometSelectionVector) {
        CometSelectionVector selectionVector = (CometSelectionVector) currentBatch.column(i);

        // Export the indices vector
        nativeUtil.exportSingleVector(
            selectionVector.getIndices(), arrayAddrs[exportCount], schemaAddrs[exportCount]);
        exportCount++;
      }
    }
    return exportCount;
  }

  /**
   * Release JVM references for any batch that was fetched but not yet exported (e.g., task
   * cancellation). This allows the Arrow allocator to be closed cleanly.
   */
  @Override
  public void close() {
    closeBatchVectors(currentBatch);
    currentBatch = null;
  }

  private void closeBatchVectors(ColumnarBatch batch) {
    if (batch != null) {
      for (int i = 0; i < batch.numCols(); i++) {
        if (batch.column(i) instanceof CometVector) {
          ((CometVector) batch.column(i)).close();
        }
      }
    }
  }
}
