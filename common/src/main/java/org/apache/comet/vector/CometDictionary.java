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

  private final CometPlainVector values;
  private final int numValues;

  /** Decoded dictionary values. Only one of the following is set. */
  private byte[] bytes;

  private short[] shorts;
  private int[] ints;
  private long[] longs;
  private float[] floats;
  private double[] doubles;
  private boolean[] booleans;
  private ByteArrayWrapper[] binaries;
  private UTF8String[] strings;

  public CometDictionary(CometPlainVector values) {
    this.values = values;
    this.numValues = values.numValues();
    initialize();
  }

  public ValueVector getValueVector() {
    return values.getValueVector();
  }

  public boolean decodeToBoolean(int index) {
    return booleans[index];
  }

  public byte decodeToByte(int index) {
    return bytes[index];
  }

  public short decodeToShort(int index) {
    return shorts[index];
  }

  public int decodeToInt(int index) {
    return ints[index];
  }

  public long decodeToLong(int index) {
    return longs[index];
  }

  public float decodeToFloat(int index) {
    return floats[index];
  }

  public double decodeToDouble(int index) {
    return doubles[index];
  }

  public byte[] decodeToBinary(int index) {
    return binaries[index].bytes;
  }

  public UTF8String decodeToUTF8String(int index) {
    return strings[index];
  }

  @Override
  public void close() {
    values.close();
  }

  private void initialize() {
    switch (values.getValueVector().getMinorType()) {
      case BIT:
        booleans = new boolean[numValues];
        for (int i = 0; i < numValues; i++) {
          booleans[i] = values.getBoolean(i);
        }
        break;
      case TINYINT:
        bytes = new byte[numValues];
        for (int i = 0; i < numValues; i++) {
          bytes[i] = values.getByte(i);
        }
        break;
      case SMALLINT:
        shorts = new short[numValues];
        for (int i = 0; i < numValues; i++) {
          shorts[i] = values.getShort(i);
        }
        break;
      case INT:
      case DATEDAY:
        ints = new int[numValues];
        for (int i = 0; i < numValues; i++) {
          ints[i] = values.getInt(i);
        }
        break;
      case BIGINT:
      case TIMESTAMPMICRO:
      case TIMESTAMPMICROTZ:
        longs = new long[numValues];
        for (int i = 0; i < numValues; i++) {
          longs[i] = values.getLong(i);
        }
        break;
      case FLOAT4:
        floats = new float[numValues];
        for (int i = 0; i < numValues; i++) {
          floats[i] = values.getFloat(i);
        }
        break;
      case FLOAT8:
        doubles = new double[numValues];
        for (int i = 0; i < numValues; i++) {
          doubles[i] = values.getDouble(i);
        }
        break;
      case VARBINARY:
      case FIXEDSIZEBINARY:
        binaries = new ByteArrayWrapper[numValues];
        for (int i = 0; i < numValues; i++) {
          binaries[i] = new ByteArrayWrapper(values.getBinary(i));
        }
        break;
      case VARCHAR:
        strings = new UTF8String[numValues];
        for (int i = 0; i < numValues; i++) {
          strings[i] = values.getUTF8String(i);
        }
        break;
      case DECIMAL:
        binaries = new ByteArrayWrapper[numValues];
        for (int i = 0; i < numValues; i++) {
          // Need copying here since we re-use byte array for decimal
          byte[] bytes = values.getBinaryDecimal(i);
          byte[] copy = new byte[DECIMAL_BYTE_WIDTH];
          System.arraycopy(bytes, 0, copy, 0, DECIMAL_BYTE_WIDTH);
          binaries[i] = new ByteArrayWrapper(copy);
        }
        break;
      default:
        throw new IllegalArgumentException(
            "Invalid Arrow minor type: " + values.getValueVector().getMinorType());
    }
  }

  private static class ByteArrayWrapper {
    private final byte[] bytes;

    ByteArrayWrapper(byte[] bytes) {
      this.bytes = bytes;
    }
  }
}
