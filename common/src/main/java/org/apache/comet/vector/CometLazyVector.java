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
import org.apache.spark.sql.types.DataType;

import org.apache.comet.parquet.LazyColumnReader;

public class CometLazyVector extends CometDelegateVector {
  private final LazyColumnReader columnReader;

  public CometLazyVector(DataType type, LazyColumnReader columnReader, boolean useDecimal128) {
    super(type, useDecimal128);
    this.columnReader = columnReader;
  }

  public CometDecodedVector getDecodedVector() {
    return (CometDecodedVector) delegate;
  }

  @Override
  public ValueVector getValueVector() {
    columnReader.readAllBatch();
    setDelegate(columnReader.loadVector());
    return super.getValueVector();
  }

  @Override
  public void setNumNulls(int numNulls) {
    throw new UnsupportedOperationException("CometLazyVector doesn't support 'setNumNulls'");
  }

  @Override
  public void setNumValues(int numValues) {
    throw new UnsupportedOperationException("CometLazyVector doesn't support 'setNumValues'");
  }

  @Override
  public void close() {
    // Do nothing. 'vector' is closed by 'columnReader' which owns it.
  }

  @Override
  public boolean hasNull() {
    columnReader.readAllBatch();
    setDelegate(columnReader.loadVector());
    return super.hasNull();
  }

  @Override
  public int numNulls() {
    columnReader.readAllBatch();
    setDelegate(columnReader.loadVector());
    return super.numNulls();
  }

  @Override
  public boolean isNullAt(int rowId) {
    if (columnReader.materializeUpToIfNecessary(rowId)) {
      setDelegate(columnReader.loadVector());
    }
    return super.isNullAt(rowId);
  }
}
