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

import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.spark.sql.comet.util.Utils;
import org.apache.spark.unsafe.Platform;

/** A Comet vector whose elements are already decoded (i.e., materialized). */
public abstract class CometDecodedVector extends CometVector {
  /**
   * The vector that stores all the values. For dictionary-backed vector, this is the vector of
   * indices.
   */
  protected final ValueVector valueVector;

  private boolean hasNull;
  private int numNulls;
  private int numValues;
  private int validityByteCacheIndex = -1;
  private byte validityByteCache;

  protected CometDecodedVector(ValueVector vector, Field valueField, boolean useDecimal128) {
    super(Utils.fromArrowField(valueField), useDecimal128);
    this.valueVector = vector;
    this.numNulls = valueVector.getNullCount();
    this.numValues = valueVector.getValueCount();
    this.hasNull = numNulls != 0;
  }

  @Override
  public ValueVector getValueVector() {
    return valueVector;
  }

  @Override
  public void setNumNulls(int numNulls) {
    // We don't need to update null count in 'valueVector' since 'ValueVector.getNullCount' will
    // re-compute the null count from validity buffer.
    this.numNulls = numNulls;
    this.hasNull = numNulls != 0;
    this.validityByteCacheIndex = -1;
  }

  @Override
  public void setNumValues(int numValues) {
    this.numValues = numValues;
    if (valueVector instanceof BaseVariableWidthVector) {
      BaseVariableWidthVector bv = (BaseVariableWidthVector) valueVector;
      // In case `lastSet` is smaller than `numValues`, `setValueCount` will set all the offsets
      // within `[lastSet + 1, numValues)` to be empty, which is incorrect in our case.
      //
      // For instance, this can happen if one first call `setNumValues` with input 100, and then
      // again `setNumValues` with 200. The first call will set `lastSet` to 99, while the second
      // call will set all strings between indices `[100, 200)` to be empty.
      bv.setLastSet(numValues);
    }
    valueVector.setValueCount(numValues);
  }

  public int numValues() {
    return numValues;
  }

  @Override
  public boolean hasNull() {
    return hasNull;
  }

  @Override
  public int numNulls() {
    return numNulls;
  }

  @Override
  public boolean isNullAt(int rowId) {
    if (!hasNull) return false;

    int byteIndex = rowId >> 3;
    if (byteIndex != validityByteCacheIndex) {
      long validityBufferAddress = valueVector.getValidityBuffer().memoryAddress();
      validityByteCache = Platform.getByte(null, validityBufferAddress + byteIndex);
      validityByteCacheIndex = byteIndex;
    }
    return ((validityByteCache >> (rowId & 7)) & 1) == 0;
  }
}
