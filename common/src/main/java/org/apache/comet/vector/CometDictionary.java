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
import org.apache.spark.unsafe.types.UTF8String;

/** A dictionary which maps indices (integers) to values. */
public class CometDictionary implements AutoCloseable {
  private static final int DECIMAL_BYTE_WIDTH = 16;

  private CometPlainVector values;
  private final int numValues;

  public CometDictionary(CometPlainVector values) {
    this.values = values;
    this.numValues = values.numValues();
  }

  public void setDictionaryVector(CometPlainVector values) {
    this.values = values;
    if (values.numValues() != numValues) {
      throw new IllegalArgumentException("Mismatched dictionary size");
    }
  }

  public ValueVector getValueVector() {
    return values.getValueVector();
  }

  public boolean decodeToBoolean(int index) {
    return values.getBoolean(index);
  }

  public byte decodeToByte(int index) {
    return values.getByte(index);
  }

  public short decodeToShort(int index) {
    return values.getShort(index);
  }

  public int decodeToInt(int index) {
    return values.getInt(index);
  }

  public long decodeToLong(int index) {
    return values.getLong(index);
  }

  public float decodeToFloat(int index) {
    return values.getFloat(index);
  }

  public double decodeToDouble(int index) {
    return values.getDouble(index);
  }

  public byte[] decodeToBinary(int index) {
    switch (values.getValueVector().getMinorType()) {
      case VARBINARY:
      case FIXEDSIZEBINARY:
        return values.getBinary(index);
      case DECIMAL:
        byte[] bytes = new byte[DECIMAL_BYTE_WIDTH];
        bytes = values.copyBinaryDecimal(index, bytes);
        return bytes;
      default:
        throw new IllegalArgumentException(
            "Invalid Arrow minor type: " + values.getValueVector().getMinorType());
    }
  }

  public UTF8String decodeToUTF8String(int index) {
    return values.getUTF8String(index);
  }

  @Override
  public void close() {
    values.close();
  }

  private static class ByteArrayWrapper {
    private final byte[] bytes;

    ByteArrayWrapper(byte[] bytes) {
      this.bytes = bytes;
    }
  }
}
