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

package org.apache.comet.vector;

import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.unsafe.Platform;

/** A Comet column vector for list type. */
public class CometListVector extends CometDecodedVector {
  final ListVector listVector;
  final ValueVector dataVector;
  final ColumnVector dataColumnVector;
  final DictionaryProvider dictionaryProvider;
  final long offsetBufferAddress;

  public CometListVector(
      ValueVector vector, boolean useDecimal128, DictionaryProvider dictionaryProvider) {
    super(vector, vector.getField(), useDecimal128);

    this.listVector = ((ListVector) vector);
    this.dataVector = listVector.getDataVector();
    this.dictionaryProvider = dictionaryProvider;
    this.dataColumnVector = getVector(dataVector, useDecimal128, dictionaryProvider);
    this.offsetBufferAddress = listVector.getOffsetBuffer().memoryAddress();
  }

  /** Returns the cached offset buffer memory address for direct access. */
  public long getOffsetBufferAddress() {
    return offsetBufferAddress;
  }

  /** Returns the wrapped data column vector for the array elements. */
  public ColumnVector getDataColumnVector() {
    return dataColumnVector;
  }

  @Override
  public ColumnarArray getArray(int i) {
    if (isNullAt(i)) return null;
    int start = Platform.getInt(null, offsetBufferAddress + (long) i * ListVector.OFFSET_WIDTH);
    int end = Platform.getInt(null, offsetBufferAddress + (long) (i + 1) * ListVector.OFFSET_WIDTH);

    return new ColumnarArray(dataColumnVector, start, end - start);
  }

  @Override
  public CometVector slice(int offset, int length) {
    TransferPair tp = this.valueVector.getTransferPair(this.valueVector.getAllocator());
    tp.splitAndTransfer(offset, length);

    return new CometListVector(tp.getTo(), useDecimal128, dictionaryProvider);
  }
}
