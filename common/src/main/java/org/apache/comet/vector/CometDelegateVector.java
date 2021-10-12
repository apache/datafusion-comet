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

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

/** A special Comet vector that just delegate all calls */
public class CometDelegateVector extends CometVector {
  protected CometVector delegate;

  public CometDelegateVector(DataType dataType) {
    this(dataType, null, false);
  }

  public CometDelegateVector(DataType dataType, boolean useDecimal128) {
    this(dataType, null, useDecimal128);
  }

  public CometDelegateVector(DataType dataType, CometVector delegate, boolean useDecimal128) {
    super(dataType, useDecimal128);
    if (delegate instanceof CometDelegateVector) {
      throw new IllegalArgumentException("cannot have nested delegation");
    }
    this.delegate = delegate;
  }

  public void setDelegate(CometVector delegate) {
    this.delegate = delegate;
  }

  @Override
  public void setNumNulls(int numNulls) {
    delegate.setNumNulls(numNulls);
  }

  @Override
  public void setNumValues(int numValues) {
    delegate.setNumValues(numValues);
  }

  @Override
  public int numValues() {
    return delegate.numValues();
  }

  @Override
  public boolean hasNull() {
    return delegate.hasNull();
  }

  @Override
  public int numNulls() {
    return delegate.numNulls();
  }

  @Override
  public boolean isNullAt(int rowId) {
    return delegate.isNullAt(rowId);
  }

  @Override
  public boolean getBoolean(int rowId) {
    return delegate.getBoolean(rowId);
  }

  @Override
  public byte getByte(int rowId) {
    return delegate.getByte(rowId);
  }

  @Override
  public short getShort(int rowId) {
    return delegate.getShort(rowId);
  }

  @Override
  public int getInt(int rowId) {
    return delegate.getInt(rowId);
  }

  @Override
  public long getLong(int rowId) {
    return delegate.getLong(rowId);
  }

  @Override
  public float getFloat(int rowId) {
    return delegate.getFloat(rowId);
  }

  @Override
  public double getDouble(int rowId) {
    return delegate.getDouble(rowId);
  }

  @Override
  public Decimal getDecimal(int i, int precision, int scale) {
    return delegate.getDecimal(i, precision, scale);
  }

  @Override
  byte[] getBinaryDecimal(int i) {
    return delegate.getBinaryDecimal(i);
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    return delegate.getUTF8String(rowId);
  }

  @Override
  public byte[] getBinary(int rowId) {
    return delegate.getBinary(rowId);
  }

  @Override
  public ColumnarArray getArray(int i) {
    return delegate.getArray(i);
  }

  @Override
  public ColumnarMap getMap(int i) {
    return delegate.getMap(i);
  }

  @Override
  public ColumnVector getChild(int i) {
    return delegate.getChild(i);
  }

  @Override
  ValueVector getValueVector() {
    return delegate.getValueVector();
  }

  @Override
  public CometVector slice(int offset, int length) {
    return delegate.slice(offset, length);
  }

  @Override
  DictionaryProvider getDictionaryProvider() {
    return delegate.getDictionaryProvider();
  }
}
