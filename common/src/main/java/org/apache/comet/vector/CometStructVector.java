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

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.spark.sql.vectorized.ColumnVector;

/** A Comet column vector for struct type. */
public class CometStructVector extends CometDecodedVector {
  final List<ColumnVector> children;
  final DictionaryProvider dictionaryProvider;

  public CometStructVector(
      ValueVector vector, boolean useDecimal128, DictionaryProvider dictionaryProvider) {
    super(vector, vector.getField(), useDecimal128);

    StructVector structVector = ((StructVector) vector);

    int size = structVector.size();
    List<ColumnVector> children = new ArrayList<>();

    for (int i = 0; i < size; ++i) {
      ValueVector value = structVector.getVectorById(i);
      children.add(getVector(value, useDecimal128, dictionaryProvider));
    }
    this.children = children;
    this.dictionaryProvider = dictionaryProvider;
  }

  @Override
  public ColumnVector getChild(int i) {
    return children.get(i);
  }

  @Override
  public CometVector slice(int offset, int length) {
    TransferPair tp = this.valueVector.getTransferPair(this.valueVector.getAllocator());
    tp.splitAndTransfer(offset, length);

    return new CometStructVector(tp.getTo(), useDecimal128, dictionaryProvider);
  }
}
