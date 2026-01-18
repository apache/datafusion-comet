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

import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.vectorized.ColumnVector;

/**
 * A mutable implementation of MapData backed by ColumnVectors. Unlike Spark's ColumnarMap which has
 * final fields, this class allows updating the offset and length to enable object reuse across
 * rows, reducing GC pressure.
 */
public class CometColumnarMap extends MapData {
  private final CometColumnarArray keys;
  private final CometColumnarArray values;
  private int length;

  public CometColumnarMap(ColumnVector keysData, ColumnVector valuesData) {
    this.keys = new CometColumnarArray(keysData);
    this.values = new CometColumnarArray(valuesData);
    this.length = 0;
  }

  public CometColumnarMap(ColumnVector keysData, ColumnVector valuesData, int offset, int length) {
    this.keys = new CometColumnarArray(keysData, offset, length);
    this.values = new CometColumnarArray(valuesData, offset, length);
    this.length = length;
  }

  /** Updates this map to point to a new slice of the underlying data. */
  public void update(int offset, int length) {
    this.keys.update(offset, length);
    this.values.update(offset, length);
    this.length = length;
  }

  @Override
  public int numElements() {
    return length;
  }

  @Override
  public ArrayData keyArray() {
    return keys;
  }

  @Override
  public ArrayData valueArray() {
    return values;
  }

  @Override
  public MapData copy() {
    return new ArrayBasedMapData(keys.copy(), values.copy());
  }
}
