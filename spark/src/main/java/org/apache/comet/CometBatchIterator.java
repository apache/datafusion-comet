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

import org.apache.comet.vector.NativeUtil;

/**
 * Iterator for fetching batches from JVM to native code. Usually called via JNI from native
 * ScanExec.
 *
 * <p>Batches are owned by the JVM. Native code can safely access the batch after calling `next` but
 * the native code must not retain references to the batch because the next call to `hasNext` will
 * signal to the JVM that the batch can be closed.
 */
public class CometBatchIterator {
  private final Iterator<ColumnarBatch> input;
  private final NativeUtil nativeUtil;
  private ColumnarBatch previousBatch = null;
  private ColumnarBatch currentBatch = null;

  CometBatchIterator(Iterator<ColumnarBatch> input, NativeUtil nativeUtil) {
    this.input = input;
    this.nativeUtil = nativeUtil;
  }

  /**
   * Fetch the next input batch and allow the previous batch to be closed (this may not happen
   * immediately).
   *
   * @return Number of rows in next batch or -1 if no batches left.
   */
  public int hasNext() {

    // release reference to previous batch
    previousBatch = null;

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
   * Get the next batch of Arrow arrays.
   *
   * @param arrayAddrs The addresses of the ArrowArray structures.
   * @param schemaAddrs The addresses of the ArrowSchema structures.
   * @return the number of rows of the current batch. -1 if there is no more batch.
   */
  public int next(long[] arrayAddrs, long[] schemaAddrs) {
    if (currentBatch == null) {
      return -1;
    }

    // export the batch using the Arrow C Data Interface
    int numRows = nativeUtil.exportBatch(arrayAddrs, schemaAddrs, currentBatch);

    // keep a reference to the exported batch so that it doesn't get garbage collected
    // while the native code is still processing it
    previousBatch = currentBatch;

    currentBatch = null;

    return numRows;
  }
}
