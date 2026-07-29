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

package org.apache.comet;

import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String;

import org.apache.comet.vector.CometPlainVector;
import org.apache.comet.vector.CometVector;

/**
 * Converts columnar batches to {@link UnsafeRow}s without per-value object allocation.
 *
 * <p>The conversion done by {@code rowIterator} plus {@code UnsafeProjection} routes every value
 * through two virtual calls and allocates per object-typed value: a {@code Decimal} per compact
 * decimal value and a {@code byte[]}/{@code BigInteger}/{@code BigDecimal} chain per decimal value
 * with precision above 18. This converter instead resolves each column to a primitive type code
 * once at construction and writes values straight from the Arrow buffers into a reused row buffer:
 * compact decimals as unscaled longs via {@link CometVector#getLongDecimal(int)} and wide decimals
 * as raw big-endian bytes via {@link CometVector#copyBinaryDecimal(int, byte[])}.
 *
 * <p>The produced rows are byte-identical to {@code UnsafeProjection} output, which matters because
 * {@link UnsafeRow} equality and hashing are byte-wise. The returned row and its backing buffer are
 * reused across calls; callers must copy a row to retain it.
 */
public final class DirectColumnarToRowConverter {

  private static final int BOOLEAN = 0;
  private static final int BYTE = 1;
  private static final int SHORT = 2;
  private static final int INT = 3;
  private static final int LONG = 4;
  private static final int FLOAT = 5;
  private static final int DOUBLE = 6;
  private static final int STRING = 7;
  private static final int DECIMAL_COMPACT = 8;
  private static final int DECIMAL_WIDE = 9;

  private final int numFields;
  private final int[] typeCodes;
  private final int[] precisions;
  private final int[] scales;
  private final int nullBitsetWidth;
  private final int fixedSize;

  private final byte[] decimalBytes = new byte[16];
  private final UnsafeRow row;
  private byte[] buffer = new byte[64];
  private int cursor;

  // Per-batch state
  private ColumnVector[] columns;
  private boolean[] hasNulls;

  public DirectColumnarToRowConverter(StructType schema) {
    StructField[] fields = schema.fields();
    numFields = fields.length;
    typeCodes = new int[numFields];
    precisions = new int[numFields];
    scales = new int[numFields];
    for (int i = 0; i < numFields; i++) {
      DataType dt = fields[i].dataType();
      if (dt instanceof BooleanType) {
        typeCodes[i] = BOOLEAN;
      } else if (dt instanceof ByteType) {
        typeCodes[i] = BYTE;
      } else if (dt instanceof ShortType) {
        typeCodes[i] = SHORT;
      } else if (dt instanceof IntegerType || dt instanceof DateType) {
        typeCodes[i] = INT;
      } else if (dt instanceof LongType
          || dt instanceof TimestampType
          || dt instanceof TimestampNTZType) {
        typeCodes[i] = LONG;
      } else if (dt instanceof FloatType) {
        typeCodes[i] = FLOAT;
      } else if (dt instanceof DoubleType) {
        typeCodes[i] = DOUBLE;
      } else if (dt instanceof StringType) {
        typeCodes[i] = STRING;
      } else if (dt instanceof DecimalType) {
        DecimalType d = (DecimalType) dt;
        precisions[i] = d.precision();
        scales[i] = d.scale();
        typeCodes[i] = d.precision() <= Decimal.MAX_LONG_DIGITS() ? DECIMAL_COMPACT : DECIMAL_WIDE;
      } else {
        throw new UnsupportedOperationException(
            "DirectColumnarToRowConverter does not support data type: " + dt);
      }
    }
    nullBitsetWidth = UnsafeRow.calculateBitSetWidthInBytes(numFields);
    fixedSize = nullBitsetWidth + numFields * 8;
    row = new UnsafeRow(numFields);
    if (buffer.length < fixedSize) {
      buffer = new byte[fixedSize];
    }
  }

  /** Prepares the converter for a new batch. */
  public void setBatch(ColumnarBatch batch) {
    if (batch.numCols() != numFields) {
      throw new IllegalArgumentException(
          "Column count mismatch: expected " + numFields + ", got " + batch.numCols());
    }
    if (columns == null) {
      columns = new ColumnVector[numFields];
      hasNulls = new boolean[numFields];
    }
    for (int i = 0; i < numFields; i++) {
      columns[i] = batch.column(i);
      hasNulls[i] = columns[i].hasNull();
    }
  }

  /**
   * Converts one row of the current batch. The returned row is reused across calls and valid until
   * the next call.
   */
  public UnsafeRow convertRow(int rowId) {
    // Zero the null bitset; fixed slots are always fully written below.
    for (int i = 0; i < nullBitsetWidth; i += 8) {
      Platform.putLong(buffer, Platform.BYTE_ARRAY_OFFSET + i, 0L);
    }
    cursor = fixedSize;

    for (int c = 0; c < numFields; c++) {
      ColumnVector col = columns[c];
      long slot = Platform.BYTE_ARRAY_OFFSET + nullBitsetWidth + c * 8L;
      boolean isNull = hasNulls[c] && col.isNullAt(rowId);
      switch (typeCodes[c]) {
        case BOOLEAN:
          if (isNull) {
            setNull(c, slot);
          } else {
            Platform.putLong(buffer, slot, 0L);
            Platform.putBoolean(buffer, slot, col.getBoolean(rowId));
          }
          break;
        case BYTE:
          if (isNull) {
            setNull(c, slot);
          } else {
            Platform.putLong(buffer, slot, 0L);
            Platform.putByte(buffer, slot, col.getByte(rowId));
          }
          break;
        case SHORT:
          if (isNull) {
            setNull(c, slot);
          } else {
            Platform.putLong(buffer, slot, 0L);
            Platform.putShort(buffer, slot, col.getShort(rowId));
          }
          break;
        case INT:
          if (isNull) {
            setNull(c, slot);
          } else {
            Platform.putLong(buffer, slot, 0L);
            Platform.putInt(buffer, slot, col.getInt(rowId));
          }
          break;
        case LONG:
          if (isNull) {
            setNull(c, slot);
          } else {
            Platform.putLong(buffer, slot, col.getLong(rowId));
          }
          break;
        case FLOAT:
          if (isNull) {
            setNull(c, slot);
          } else {
            float f = col.getFloat(rowId);
            if (Float.isNaN(f)) {
              f = Float.NaN;
            }
            Platform.putLong(buffer, slot, 0L);
            Platform.putFloat(buffer, slot, f);
          }
          break;
        case DOUBLE:
          if (isNull) {
            setNull(c, slot);
          } else {
            double d = col.getDouble(rowId);
            if (Double.isNaN(d)) {
              d = Double.NaN;
            }
            Platform.putDouble(buffer, slot, d);
          }
          break;
        case STRING:
          if (isNull) {
            setNull(c, slot);
          } else {
            writeString(slot, col.getUTF8String(rowId));
          }
          break;
        case DECIMAL_COMPACT:
          if (isNull) {
            setNull(c, slot);
          } else {
            Platform.putLong(buffer, slot, ((CometVector) col).getLongDecimal(rowId));
          }
          break;
        case DECIMAL_WIDE:
          writeWideDecimal(c, slot, col, rowId, isNull);
          break;
        default:
          throw new IllegalStateException("Unknown type code: " + typeCodes[c]);
      }
    }

    row.pointTo(buffer, cursor);
    return row;
  }

  private void setNull(int ordinal, long slot) {
    // Matches UnsafeRowWriter.setNullAt: set the bit and zero the fixed slot.
    long wordOffset = Platform.BYTE_ARRAY_OFFSET + (ordinal >> 6) * 8L;
    long word = Platform.getLong(buffer, wordOffset);
    Platform.putLong(buffer, wordOffset, word | (1L << (ordinal & 63)));
    Platform.putLong(buffer, slot, 0L);
  }

  private void writeString(long slot, UTF8String value) {
    int numBytes = value.numBytes();
    int roundedSize = (numBytes + 7) & ~7;
    ensureCapacity(cursor + roundedSize);
    if ((numBytes & 7) != 0) {
      // Zero the last partial word so padding bytes are deterministic (buffer is reused).
      Platform.putLong(buffer, Platform.BYTE_ARRAY_OFFSET + cursor + ((numBytes >> 3) << 3), 0L);
    }
    value.writeToMemory(buffer, Platform.BYTE_ARRAY_OFFSET + cursor);
    Platform.putLong(buffer, slot, ((long) cursor << 32) | numBytes);
    cursor += roundedSize;
  }

  private void writeWideDecimal(
      int ordinal, long slot, ColumnVector col, int rowId, boolean isNull) {
    // Matches UnsafeRowWriter.write(ordinal, Decimal, precision, scale) for precision > 18:
    // 16 bytes are always reserved (and consumed) in the variable-length region, the minimal
    // big-endian two's-complement bytes are written at the cursor, and for null values the null
    // bit is set while the offset is still recorded with size 0.
    ensureCapacity(cursor + 16);
    Platform.putLong(buffer, Platform.BYTE_ARRAY_OFFSET + cursor, 0L);
    Platform.putLong(buffer, Platform.BYTE_ARRAY_OFFSET + cursor + 8, 0L);
    if (isNull) {
      long wordOffset = Platform.BYTE_ARRAY_OFFSET + (ordinal >> 6) * 8L;
      long word = Platform.getLong(buffer, wordOffset);
      Platform.putLong(buffer, wordOffset, word | (1L << (ordinal & 63)));
      Platform.putLong(buffer, slot, (long) cursor << 32);
    } else {
      byte[] be;
      int start;
      if (col instanceof CometPlainVector) {
        be = ((CometVector) col).copyBinaryDecimal(rowId, decimalBytes);
        // Trim to the minimal two's-complement form BigInteger.toByteArray would produce,
        // so the row bytes match what UnsafeRowWriter writes.
        byte sign = (be[0] & 0x80) != 0 ? (byte) 0xFF : (byte) 0x00;
        start = 0;
        while (start < 15 && be[start] == sign && ((be[start + 1] ^ sign) & 0x80) == 0) {
          start++;
        }
      } else {
        // Dictionary-encoded or other vector: fall back to the allocating accessor.
        be =
            col.getDecimal(rowId, precisions[ordinal], scales[ordinal])
                .toJavaBigDecimal()
                .unscaledValue()
                .toByteArray();
        start = 0;
      }
      int numBytes = be.length - start;
      Platform.copyMemory(
          be,
          Platform.BYTE_ARRAY_OFFSET + start,
          buffer,
          Platform.BYTE_ARRAY_OFFSET + cursor,
          numBytes);
      Platform.putLong(buffer, slot, ((long) cursor << 32) | numBytes);
    }
    cursor += 16;
  }

  private void ensureCapacity(int needed) {
    if (needed > buffer.length) {
      int newSize = Math.max(needed, buffer.length * 2);
      byte[] newBuffer = new byte[newSize];
      System.arraycopy(buffer, 0, newBuffer, 0, cursor);
      buffer = newBuffer;
    }
  }
}
