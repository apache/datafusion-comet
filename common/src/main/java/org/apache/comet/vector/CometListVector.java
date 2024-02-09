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
import org.apache.arrow.vector.util.TransferPair;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;

/** A Comet column vector for list type. */
public class CometListVector extends CometDecodedVector {
  final ListVector listVector;
  final ValueVector dataVector;
  final ColumnVector dataColumnVector;

  public CometListVector(ValueVector vector, boolean useDecimal128) {
    super(vector, vector.getField(), useDecimal128);

    this.listVector = ((ListVector) vector);
    this.dataVector = listVector.getDataVector();
    this.dataColumnVector = getVector(dataVector, useDecimal128);
  }

  @Override
  public ColumnarArray getArray(int i) {
    int start = listVector.getOffsetBuffer().getInt(i * ListVector.OFFSET_WIDTH);
    int end = listVector.getOffsetBuffer().getInt((i + 1) * ListVector.OFFSET_WIDTH);

    return new ColumnarArray(dataColumnVector, start, end - start);
  }

  @Override
  public CometVector slice(int offset, int length) {
    TransferPair tp = this.valueVector.getTransferPair(this.valueVector.getAllocator());
    tp.splitAndTransfer(offset, length);

    return new CometListVector(tp.getTo(), useDecimal128);
  }
}
