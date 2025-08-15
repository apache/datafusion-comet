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
 * An iterator that can be used to get batches of Arrow arrays from a Spark iterator of
 * ColumnarBatch. It will consume input iterator and return Arrow arrays by addresses. This is
 * called by native code to retrieve Arrow arrays from Spark through JNI.
 */
public class CometBatchIterator {
  final Iterator<ColumnarBatch> input;
  final NativeUtil nativeUtil;
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
   * Get the next batches of Arrow arrays.
   *
   * Each call to this method can potentially release arrays that were returned from the previous call.
   *
   * @param arrayAddrs The addresses of the ArrowArray structures.
   * @param schemaAddrs The addresses of the ArrowSchema structures.
   * @return the number of rows of the current batch. -1 if there is no more batch.
   */
  public int next(long[] arrayAddrs, long[] schemaAddrs) {
    if (currentBatch == null) {
      return -1;
    }
    int numRows = nativeUtil.exportBatch(arrayAddrs, schemaAddrs, currentBatch);

    // TODO releasing the reference here seems sketchy
    currentBatch = null;

    return numRows;
  }
}
