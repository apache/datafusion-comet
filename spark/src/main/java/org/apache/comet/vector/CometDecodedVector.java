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

import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.spark.sql.comet.util.Utils;
import org.apache.spark.unsafe.Platform;

/**
 * A Comet vector backed by a single Arrow {@link ValueVector} whose values are already decoded (vs.
 * {@link CometDictionaryVector}, which keeps indices and dictionary values in separate vectors and
 * decodes on access).
 *
 * <p>Caches the most recently read validity byte to amortize null checks during the common
 * sequential row-access pattern.
 */
public abstract class CometDecodedVector extends CometVector {
  /**
   * The vector that stores all the values. For dictionary-backed vector, this is the vector of
   * indices.
   */
  protected final ValueVector valueVector;

  private final boolean hasNull;
  private final int numNulls;
  private final int numValues;
  private int validityByteCacheIndex = -1;
  private byte validityByteCache;
  private final long validityBufferAddress;
  protected final boolean isUuid;

  protected CometDecodedVector(ValueVector vector, Field valueField) {
    this(vector, valueField, false);
  }

  protected CometDecodedVector(ValueVector vector, Field valueField, boolean isUuid) {
    super(Utils.fromArrowField(valueField));
    this.valueVector = vector;
    this.numNulls = valueVector.getNullCount();
    this.numValues = valueVector.getValueCount();
    this.hasNull = numNulls != 0;
    // NullVector has no validity buffer (all values are logically null), so use a sentinel.
    // Subclasses that wrap NullVector (e.g. CometPlainVector) short-circuit isNullAt before
    // it reaches the base implementation, so the sentinel is never dereferenced.
    this.validityBufferAddress =
        (vector instanceof NullVector) ? -1L : valueVector.getValidityBuffer().memoryAddress();
    this.isUuid = isUuid;
  }

  @Override
  public ValueVector getValueVector() {
    return valueVector;
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
      validityByteCache = Platform.getByte(null, validityBufferAddress + byteIndex);
      validityByteCacheIndex = byteIndex;
    }
    return ((validityByteCache >> (rowId & 7)) & 1) == 0;
  }
}
