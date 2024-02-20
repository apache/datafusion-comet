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

  CometBatchIterator(Iterator<ColumnarBatch> input, NativeUtil nativeUtil) {
    this.input = input;
    this.nativeUtil = nativeUtil;
  }

  /**
   * Get the next batches of Arrow arrays. It will consume input iterator and return Arrow arrays by
   * addresses. If the input iterator is done, it will return a one negative element array
   * indicating the end of the iterator.
   */
  public long[] next() {
    boolean hasBatch = input.hasNext();

    if (!hasBatch) {
      return new long[] {-1};
    }

    return nativeUtil.exportBatch(input.next());
  }
}
