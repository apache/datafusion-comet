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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.UUID;

import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.parquet.Preconditions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String;

/** A column vector whose elements are plainly decoded. */
public class CometPlainVector extends CometDecodedVector {
  private final long valueBufferAddress;
  private final boolean isBaseFixedWidthVector;

  private byte booleanByteCache;
  private int booleanByteCacheIndex = -1;

  /** Value buffer bytes */
  //private ByteBuffer valueBytes;
  private byte[] buffer = null;

  public CometPlainVector(ValueVector vector, boolean useDecimal128) {
    this(vector, useDecimal128, false);
  }

  public CometPlainVector(ValueVector vector, boolean useDecimal128, boolean isUuid) {
    super(vector, vector.getField(), useDecimal128, isUuid);
    // NullType doesn't have data buffer.
    if (vector instanceof NullVector) {
      this.valueBufferAddress = -1;
    } else {
      this.valueBufferAddress = vector.getDataBuffer().memoryAddress();
    }

    isBaseFixedWidthVector = valueVector instanceof BaseFixedWidthVector;

    prefetch();
  }

  @Override
  public void prefetch() {
    if (buffer != null) {
      return;
    }

    // I am not seeing a performance gain from pre-fetching primitive arrays

    /*if (dataType() == DataTypes.ByteType) {
      loadValueBytes(1);
    } else if (dataType() == DataTypes.ShortType) {
      loadValueBytes(2);
    } else if (dataType() == DataTypes.IntegerType) {
      loadValueBytes(4);
    } else if (dataType() == DataTypes.LongType) {
      loadValueBytes(8);
    } else*/

    // TODO decimals

    // this is a crude approach of pre-fetching the entire array but we could load
    // in chunks on demand if this approach shows any benefit

    // an even better approach would be making one JNI call per row to construct
    // UnsafeRow bytes in native code

    if (dataType() == DataTypes.StringType || dataType() == DataTypes.BinaryType) {
      int rowCount = this.getValueVector().getValueCapacity();
      if (rowCount > 0) {
        long startAddress = valueBufferAddress + Platform.BYTE_ARRAY_OFFSET;
        if (isBaseFixedWidthVector) {
          System.out.println("pre-fetching fixed-width column");
          BaseFixedWidthVector fixedWidthVector = (BaseFixedWidthVector) valueVector;
          int offset = 0;
          int length = rowCount * fixedWidthVector.getTypeWidth();
          buffer = new byte[length];
          System.out.println("Platform.copyMemory() offset=" + offset + ", length=" + length);
          Platform.copyMemory(null, startAddress, buffer, offset, length);
          System.out.println("fetched: " + bytesToHex(buffer));
//          valueBytes = ByteBuffer.wrap(buffer);
//          valueBytes.order(ByteOrder.LITTLE_ENDIAN);

        } else {
          System.out.println("pre-fetching variable-width column");
          BaseVariableWidthVector varWidthVector = (BaseVariableWidthVector) valueVector;
          ArrowBuf offsetBuffer = varWidthVector.getOffsetBuffer();
          System.out.println("offset buffer capacity=" + offsetBuffer.capacity());
          System.out.println("row count=" + rowCount);
          long offsetBufferAddress = offsetBuffer.memoryAddress();
          // TODO offset and length buffers could also be pre-fetched
          int offset = Platform.getInt(null, offsetBufferAddress);
          int length = Platform.getInt(null, offsetBufferAddress + rowCount * 4L);
          buffer = new byte[length];
          System.out.println("Platform.copyMemory() offset=" + offset + ", length=" + length);
          Platform.copyMemory(null, startAddress, buffer, offset, length);
          System.out.println("fetched: " + bytesToHex(buffer));
//          valueBytes = ByteBuffer.wrap(buffer);
//          valueBytes.order(ByteOrder.LITTLE_ENDIAN);
        }
      }
    }
  }

  private static String bytesToHex(byte[] bytes) {
    StringBuilder hexString = new StringBuilder();
    for (byte b : bytes) {
      String hex = Integer.toHexString(0xFF & b); // Mask with 0xFF to handle negative bytes
      if (hex.length() == 1) {
        hexString.append('0'); // Append leading zero if needed
      }
      hexString.append(hex);
    }
    return hexString.toString().toUpperCase();
  }

  //  private void loadValueBytes(int bytesPerValue) {
  //    final int numBytes = numValues() * bytesPerValue;
  //    final byte[] buffer = new byte[numBytes];
  //    valueBytes = ByteBuffer.wrap(buffer);
  //    valueBytes.order(ByteOrder.LITTLE_ENDIAN);
  //    final long valueBufferAddress = getValueVector().getDataBuffer().memoryAddress();
  //    Platform.copyMemory(null, valueBufferAddress, buffer, Platform.BYTE_ARRAY_OFFSET, numBytes);
  //  }

  @Override
  public void setNumNulls(int numNulls) {
    super.setNumNulls(numNulls);
    this.booleanByteCacheIndex = -1;
  }

  @Override
  public boolean getBoolean(int rowId) {
    int byteIndex = rowId >> 3;
    if (byteIndex != booleanByteCacheIndex) {
      booleanByteCache = getByte(byteIndex);
      booleanByteCacheIndex = byteIndex;
    }
    return ((booleanByteCache >> (rowId & 7)) & 1) == 1;
  }

  @Override
  public byte getByte(int rowId) {
//    if (valueBytes != null) {
//      return valueBytes.get(rowId);
//    }
    return Platform.getByte(null, valueBufferAddress + rowId);
  }

  @Override
  public short getShort(int rowId) {
    final long offset = rowId * 2L;
//    if (valueBytes != null) {
//      return valueBytes.getShort((int) offset);
//    }
    return Platform.getShort(null, valueBufferAddress + offset);
  }

  @Override
  public int getInt(int rowId) {
    final long offset = rowId * 4L;
//    if (valueBytes != null) {
//      return valueBytes.getInt((int) offset);
//    }
    return Platform.getInt(null, valueBufferAddress + offset);
  }

  @Override
  public long getLong(int rowId) {
    final long offset = rowId * 8L;
//    if (valueBytes != null) {
//      return valueBytes.getLong((int) offset);
//    }
    return Platform.getLong(null, valueBufferAddress + offset);
  }

  @Override
  public long getLongDecimal(int rowId) {
    return Platform.getLong(null, valueBufferAddress + rowId * 16L);
  }

  @Override
  public float getFloat(int rowId) {
    return Platform.getFloat(null, valueBufferAddress + rowId * 4L);
  }

  @Override
  public double getDouble(int rowId) {
    return Platform.getDouble(null, valueBufferAddress + rowId * 8L);
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    if (isBaseFixedWidthVector) {
      if (isUuid) {
        return UTF8String.fromString(convertToUuid(getBinary(rowId)).toString());
      } else {
        return UTF8String.fromBytes(getBinary(rowId));
      }
    } else {
      return UTF8String.fromBytes(getBinary(rowId));
    }
  }

  @Override
  public byte[] getBinary(int rowId) {
    assert (buffer != null);
    System.out.println("getBinary(" + rowId + ")");
    if (isBaseFixedWidthVector) {
      BaseFixedWidthVector fixedWidthVector = (BaseFixedWidthVector) valueVector;
      int length = fixedWidthVector.getTypeWidth();
      int offset = rowId * length;
//      byte[] result = new byte[length];
      System.out.println("calling valueBytes.get(result, " + offset + "," + length + ")");

//      return result;
      return Arrays.copyOfRange(buffer, offset, length);
    } else {
      BaseVariableWidthVector varWidthVector = (BaseVariableWidthVector) valueVector;
      long offsetBufferAddress = varWidthVector.getOffsetBuffer().memoryAddress();
      // TODO could pre-fetch offset and length arrays
      int offset = Platform.getInt(null, offsetBufferAddress + rowId * 4L);
      int length = Platform.getInt(null, offsetBufferAddress + (rowId + 1L) * 4L) - offset;
//      byte[] result = new byte[length];
      System.out.println("calling valueBytes.get(result, " + offset + "," + length + ")");
      //valueBytes.get(result, offset, length);
      return Arrays.copyOfRange(buffer, offset, length);
    }
  }

  @Override
  public CDataDictionaryProvider getDictionaryProvider() {
    return null;
  }

  @Override
  public boolean isNullAt(int rowId) {
    return this.valueBufferAddress == -1 || super.isNullAt(rowId);
  }

  @Override
  public CometVector slice(int offset, int length) {
    TransferPair tp = this.valueVector.getTransferPair(this.valueVector.getAllocator());
    tp.splitAndTransfer(offset, length);

    return new CometPlainVector(tp.getTo(), useDecimal128);
  }

  private static UUID convertToUuid(byte[] buf) {
    Preconditions.checkArgument(buf.length == 16, "UUID require 16 bytes");
    ByteBuffer bb = ByteBuffer.wrap(buf);
    bb.order(ByteOrder.BIG_ENDIAN);
    long mostSigBits = bb.getLong();
    long leastSigBits = bb.getLong();
    return new UUID(mostSigBits, leastSigBits);
  }
}
