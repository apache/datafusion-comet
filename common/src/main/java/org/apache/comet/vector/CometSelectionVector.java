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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A zero-copy selection vector that extends CometVector. This implementation stores the original
 * data vector and selection indices as separate CometVectors, providing zero copy access to the the
 * underlying data.
 *
 * <p>If the original vector has values [v0, v1, v2, v3, v4, v5, v6, v7] and the selection indices
 * are [0, 1, 3, 4, 5, 7], then this selection vector will logically represent [v0, v1, v3, v4, v5,
 * v7] without actually copying the data.
 *
 * <p>Most of the implementations of CometVector methods are implemented for completeness. We don't
 * use this class except to transfer the original data and the selection indices to the native code.
 */
public class CometSelectionVector extends CometVector {
  /** The original vector containing all values */
  private final CometVector values;

  /**
   * The valid indices in the values vector. This array is converted into an Arrow vector so we can
   * transfer the data to native in one JNI call. This is used to represent the rowid mapping used
   * by Iceberg
   */
  private final int[] selectionIndices;

  /**
   * The indices vector containing selection indices. This is currently allocated by the JVM side
   * unlike the values vector which is allocated on the native side
   */
  private final CometVector indices;

  /**
   * Number of selected elements. The indices array may have a length greater than this but only
   * numValues elements in the array are valid
   */
  private final int numValues;

  /**
   * Creates a new selection vector from the given vector and indices.
   *
   * @param values The original vector to select from
   * @param indices The indices to select from the original vector
   * @param numValues The number of valid values in the indices array
   * @throws IllegalArgumentException if any index is out of bounds
   */
  public CometSelectionVector(CometVector values, int[] indices, int numValues) {
    // Use the values vector's datatype, useDecimal128, and dictionary provider
    super(values.dataType(), values.useDecimal128);

    this.values = values;
    this.selectionIndices = indices;
    this.numValues = numValues;

    // Validate indices
    int originalLength = values.numValues();
    for (int idx : indices) {
      if (idx < 0 || idx >= originalLength) {
        throw new IllegalArgumentException(
            String.format(
                "Index %d is out of bounds for vector of length %d", idx, originalLength));
      }
    }

    // Create indices vector
    BufferAllocator allocator = values.getValueVector().getAllocator();
    IntVector indicesVector = new IntVector("selection_indices", allocator);
    indicesVector.allocateNew(numValues);
    for (int i = 0; i < numValues; i++) {
      indicesVector.set(i, indices[i]);
    }
    indicesVector.setValueCount(numValues);

    this.indices =
        CometVector.getVector(indicesVector, values.useDecimal128, values.getDictionaryProvider());
  }

  /**
   * Returns the index in the values vector for the given selection vector index.
   *
   * @param selectionIndex Index in the selection vector
   * @return The corresponding index in the original vector
   * @throws IndexOutOfBoundsException if selectionIndex is out of bounds
   */
  private int getValuesIndex(int selectionIndex) {
    if (selectionIndex < 0 || selectionIndex >= numValues) {
      throw new IndexOutOfBoundsException(
          String.format(
              "Selection index %d is out of bounds for selection vector of length %d",
              selectionIndex, numValues));
    }
    return indices.getInt(selectionIndex);
  }

  /**
   * Returns a reference to the values vector.
   *
   * @return The CometVector containing the values
   */
  public CometVector getValues() {
    return values;
  }

  /**
   * Returns the indices vector.
   *
   * @return The CometVector containing the indices
   */
  public CometVector getIndices() {
    return indices;
  }

  /**
   * Returns the selected indices.
   *
   * @return Array of selected indices
   */
  private int[] getSelectedIndices() {
    return selectionIndices;
  }

  @Override
  public int numValues() {
    return numValues;
  }

  @Override
  public void setNumValues(int numValues) {
    // For selection vectors, we don't allow changing the number of values
    // as it would break the mapping between selection indices and values
    throw new UnsupportedOperationException("CometSelectionVector doesn't support setNumValues");
  }

  @Override
  public void setNumNulls(int numNulls) {
    // For selection vectors, null count should be delegated to the underlying values vector
    // The selection doesn't change the null semantics
    values.setNumNulls(numNulls);
  }

  @Override
  public boolean hasNull() {
    return values.hasNull();
  }

  @Override
  public int numNulls() {
    return values.numNulls();
  }

  // ColumnVector method implementations - delegate to original vector with index mapping
  @Override
  public boolean isNullAt(int rowId) {
    return values.isNullAt(getValuesIndex(rowId));
  }

  @Override
  public boolean getBoolean(int rowId) {
    return values.getBoolean(getValuesIndex(rowId));
  }

  @Override
  public byte getByte(int rowId) {
    return values.getByte(getValuesIndex(rowId));
  }

  @Override
  public short getShort(int rowId) {
    return values.getShort(getValuesIndex(rowId));
  }

  @Override
  public int getInt(int rowId) {
    return values.getInt(getValuesIndex(rowId));
  }

  @Override
  public long getLong(int rowId) {
    return values.getLong(getValuesIndex(rowId));
  }

  @Override
  public long getLongDecimal(int rowId) {
    return values.getLongDecimal(getValuesIndex(rowId));
  }

  @Override
  public float getFloat(int rowId) {
    return values.getFloat(getValuesIndex(rowId));
  }

  @Override
  public double getDouble(int rowId) {
    return values.getDouble(getValuesIndex(rowId));
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    return values.getUTF8String(getValuesIndex(rowId));
  }

  @Override
  public byte[] getBinary(int rowId) {
    return values.getBinary(getValuesIndex(rowId));
  }

  @Override
  public ColumnarArray getArray(int rowId) {
    return values.getArray(getValuesIndex(rowId));
  }

  @Override
  public ColumnarMap getMap(int rowId) {
    return values.getMap(getValuesIndex(rowId));
  }

  @Override
  public ColumnVector getChild(int ordinal) {
    // Return the child from the original vector
    return values.getChild(ordinal);
  }

  @Override
  public DictionaryProvider getDictionaryProvider() {
    return values.getDictionaryProvider();
  }

  @Override
  public CometVector slice(int offset, int length) {
    if (offset < 0 || length < 0 || offset + length > numValues) {
      throw new IllegalArgumentException("Invalid slice parameters");
    }
    // Get the current indices and slice them
    int[] currentIndices = getSelectedIndices();
    int[] slicedIndices = new int[length];
    // This is not a very efficient version of slicing, but that is
    // not important because we are not likely to use it.
    System.arraycopy(currentIndices, offset, slicedIndices, 0, length);
    return new CometSelectionVector(values, slicedIndices, length);
  }

  @Override
  public org.apache.arrow.vector.ValueVector getValueVector() {
    return values.getValueVector();
  }

  @Override
  public void close() {
    // Close both the values and indices vectors
    values.close();
    indices.close();
  }
}
