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
import java.util.UUID;

import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.parquet.Preconditions;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String;

/** A column vector whose elements are plainly decoded. */
public class CometPlainVector extends CometDecodedVector {
  private final long valueBufferAddress;
  private final boolean isBaseFixedWidthVector;

  private byte booleanByteCache;
  private int booleanByteCacheIndex = -1;

  public CometPlainVector(ValueVector vector, boolean useDecimal128) {
    super(vector, vector.getField(), useDecimal128);
    // NullType doesn't have data buffer.
    if (vector instanceof NullVector) {
      this.valueBufferAddress = -1;
    } else {
      this.valueBufferAddress = vector.getDataBuffer().memoryAddress();
    }

    isBaseFixedWidthVector = valueVector instanceof BaseFixedWidthVector;
  }

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
    return Platform.getByte(null, valueBufferAddress + rowId);
  }

  @Override
  public short getShort(int rowId) {
    return Platform.getShort(null, valueBufferAddress + rowId * 2L);
  }

  @Override
  public int getInt(int rowId) {
    return Platform.getInt(null, valueBufferAddress + rowId * 4L);
  }

  @Override
  public long getLong(int rowId) {
    return Platform.getLong(null, valueBufferAddress + rowId * 8L);
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
    if (!isBaseFixedWidthVector) {
      BaseVariableWidthVector varWidthVector = (BaseVariableWidthVector) valueVector;
      long offsetBufferAddress = varWidthVector.getOffsetBuffer().memoryAddress();
      int offset = Platform.getInt(null, offsetBufferAddress + rowId * 4L);
      int length = Platform.getInt(null, offsetBufferAddress + (rowId + 1L) * 4L) - offset;
      return UTF8String.fromAddress(null, valueBufferAddress + offset, length);
    } else {
      // Iceberg maps UUID to StringType.
      // The data type here must be UUID because the only FLBA -> String mapping we have is UUID.
      BaseFixedWidthVector fixedWidthVector = (BaseFixedWidthVector) valueVector;
      int length = fixedWidthVector.getTypeWidth();
      int offset = rowId * length;
      byte[] result = new byte[length];
      Platform.copyMemory(
          null, valueBufferAddress + offset, result, Platform.BYTE_ARRAY_OFFSET, length);
      return UTF8String.fromString(convertToUuid(result).toString());
    }
  }

  @Override
  public byte[] getBinary(int rowId) {
    int offset;
    int length;
    if (valueVector instanceof BaseVariableWidthVector) {
      BaseVariableWidthVector varWidthVector = (BaseVariableWidthVector) valueVector;
      long offsetBufferAddress = varWidthVector.getOffsetBuffer().memoryAddress();
      offset = Platform.getInt(null, offsetBufferAddress + rowId * 4L);
      length = Platform.getInt(null, offsetBufferAddress + (rowId + 1L) * 4L) - offset;
    } else if (valueVector instanceof BaseFixedWidthVector) {
      BaseFixedWidthVector fixedWidthVector = (BaseFixedWidthVector) valueVector;
      length = fixedWidthVector.getTypeWidth();
      offset = rowId * length;
    } else {
      throw new RuntimeException("Unsupported binary vector type: " + valueVector.getName());
    }
    byte[] result = new byte[length];
    Platform.copyMemory(
        null, valueBufferAddress + offset, result, Platform.BYTE_ARRAY_OFFSET, length);
    return result;
  }

  @Override
  CDataDictionaryProvider getDictionaryProvider() {
    return null;
  }

  @Override
  public boolean isNullAt(int rowId) {
    if (this.valueBufferAddress == -1) {
      return true;
    } else {
      return super.isNullAt(rowId);
    }
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
