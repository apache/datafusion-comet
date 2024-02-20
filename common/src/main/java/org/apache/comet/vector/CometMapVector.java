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
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarMap;

/** A Comet column vector for map type. */
public class CometMapVector extends CometDecodedVector {
  final MapVector mapVector;
  final ValueVector dataVector;
  final CometStructVector dataColumnVector;

  final ColumnVector keys;
  final ColumnVector values;

  public CometMapVector(ValueVector vector, boolean useDecimal128) {
    super(vector, vector.getField(), useDecimal128);

    this.mapVector = ((MapVector) vector);
    this.dataVector = mapVector.getDataVector();

    if (dataVector instanceof StructVector) {
      this.dataColumnVector = new CometStructVector(dataVector, useDecimal128);

      if (dataColumnVector.children.size() != 2) {
        throw new RuntimeException(
            "MapVector's dataVector should have 2 children, but got: "
                + dataColumnVector.children.size());
      }

      this.keys = dataColumnVector.getChild(0);
      this.values = dataColumnVector.getChild(1);
    } else {
      throw new RuntimeException(
          "MapVector's dataVector should be StructVector, but got: "
              + dataVector.getClass().getSimpleName());
    }
  }

  @Override
  public ColumnarMap getMap(int i) {
    int start = mapVector.getOffsetBuffer().getInt(i * MapVector.OFFSET_WIDTH);
    int end = mapVector.getOffsetBuffer().getInt((i + 1) * MapVector.OFFSET_WIDTH);

    return new ColumnarMap(keys, values, start, end - start);
  }

  @Override
  public CometVector slice(int offset, int length) {
    TransferPair tp = this.valueVector.getTransferPair(this.valueVector.getAllocator());
    tp.splitAndTransfer(offset, length);

    return new CometMapVector(tp.getTo(), useDecimal128);
  }
}
