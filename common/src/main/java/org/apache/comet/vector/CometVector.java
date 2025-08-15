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

import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.arrow.vector.FixedWidthVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String;

/** Base class for all Comet column vector implementations. */
public abstract class CometVector extends ColumnVector {
  private static final int DECIMAL_BYTE_WIDTH = 16;
  private final byte[] DECIMAL_BYTES = new byte[DECIMAL_BYTE_WIDTH];
  protected final boolean useDecimal128;

  private static final long decimalValOffset;

  static {
    try {
      java.lang.reflect.Field unsafeField = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
      unsafeField.setAccessible(true);
      final sun.misc.Unsafe unsafe = (sun.misc.Unsafe) unsafeField.get(null);
      decimalValOffset = unsafe.objectFieldOffset(Decimal.class.getDeclaredField("decimalVal"));
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  protected CometVector(DataType type, boolean useDecimal128) {
    super(type);
    System.out.println("new CometVector");
    this.useDecimal128 = useDecimal128;
  }

  /**
   * Sets the number of nulls in this vector to be 'numNulls'. This is used when the vector is
   * reused across batches.
   */
  public abstract void setNumNulls(int numNulls);

  /**
   * Sets the number of values (including both nulls and non-nulls) in this vector to be
   * 'numValues'. This is used when the vector is reused across batches.
   */
  public abstract void setNumValues(int numValues);

  /** Returns the number of values in this vector. */
  public abstract int numValues();

  /** Whether the elements of this vector are of fixed length. */
  public boolean isFixedLength() {
    return getValueVector() instanceof FixedWidthVector;
  }

  @Override
  public Decimal getDecimal(int i, int precision, int scale) {
    if (!useDecimal128 && precision <= Decimal.MAX_INT_DIGITS() && type instanceof IntegerType) {
      return createDecimal(getInt(i), precision, scale);
    } else if (precision <= Decimal.MAX_LONG_DIGITS()) {
      return createDecimal(useDecimal128 ? getLongDecimal(i) : getLong(i), precision, scale);
    } else {
      byte[] bytes = getBinaryDecimal(i);
      BigInteger bigInteger = new BigInteger(bytes);
      BigDecimal javaDecimal = new BigDecimal(bigInteger, scale);
      return createDecimal(javaDecimal, precision, scale);
    }
  }

  /** This method skips the negative scale check, otherwise the same as Decimal.createUnsafe(). */
  private Decimal createDecimal(long unscaled, int precision, int scale) {
    Decimal dec = new Decimal();
    dec.org$apache$spark$sql$types$Decimal$$longVal_$eq(unscaled);
    dec.org$apache$spark$sql$types$Decimal$$_precision_$eq(precision);
    dec.org$apache$spark$sql$types$Decimal$$_scale_$eq(scale);
    return dec;
  }

  /** This method skips a few checks, otherwise the same as Decimal.apply(). */
  private Decimal createDecimal(BigDecimal value, int precision, int scale) {
    Decimal dec = new Decimal();
    Platform.putObjectVolatile(dec, decimalValOffset, new scala.math.BigDecimal(value));
    dec.org$apache$spark$sql$types$Decimal$$_precision_$eq(precision);
    dec.org$apache$spark$sql$types$Decimal$$_scale_$eq(scale);
    return dec;
  }

  /**
   * Reads a 16-byte byte array which are encoded big-endian for decimal128 into internal byte
   * array.
   */
  byte[] getBinaryDecimal(int i) {
    return copyBinaryDecimal(i, DECIMAL_BYTES);
  }

  /** Reads a 16-byte byte array which are encoded big-endian for decimal128. */
  public byte[] copyBinaryDecimal(int i, byte[] dest) {
    long valueBufferAddress = getValueVector().getDataBuffer().memoryAddress();
    Platform.copyMemory(
        null,
        valueBufferAddress + (long) i * DECIMAL_BYTE_WIDTH,
        dest,
        Platform.BYTE_ARRAY_OFFSET,
        DECIMAL_BYTE_WIDTH);
    // Decimal is stored little-endian in Arrow, so we need to reverse the bytes here
    for (int j = 0, k = DECIMAL_BYTE_WIDTH - 1; j < DECIMAL_BYTE_WIDTH / 2; j++, k--) {
      byte tmp = dest[j];
      dest[j] = dest[k];
      dest[k] = tmp;
    }
    return dest;
  }

  @Override
  public boolean getBoolean(int rowId) {
    throw new UnsupportedOperationException("Not yet supported");
  }

  @Override
  public byte getByte(int rowId) {
    throw new UnsupportedOperationException("Not yet supported");
  }

  @Override
  public short getShort(int rowId) {
    throw new UnsupportedOperationException("Not yet supported");
  }

  @Override
  public int getInt(int rowId) {
    throw new UnsupportedOperationException("Not yet supported");
  }

  @Override
  public long getLong(int rowId) {
    throw new UnsupportedOperationException("Not yet supported");
  }

  public long getLongDecimal(int rowId) {
    throw new UnsupportedOperationException("Not yet supported");
  }

  @Override
  public float getFloat(int rowId) {
    throw new UnsupportedOperationException("Not yet supported");
  }

  @Override
  public double getDouble(int rowId) {
    throw new UnsupportedOperationException("Not yet supported");
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    throw new UnsupportedOperationException("Not yet supported");
  }

  @Override
  public byte[] getBinary(int rowId) {
    throw new UnsupportedOperationException("Not yet supported");
  }

  @Override
  public ColumnarArray getArray(int i) {
    throw new UnsupportedOperationException("Not yet supported");
  }

  @Override
  public ColumnarMap getMap(int i) {
    throw new UnsupportedOperationException("Not yet supported");
  }

  @Override
  public ColumnVector getChild(int i) {
    throw new UnsupportedOperationException("Not yet supported");
  }

  @Override
  public void close() {
    String msg = "[" + Thread.currentThread().getName() + "] CometVector.close() " + getValueVector();
    System.out.println(msg);
    new RuntimeException(msg).printStackTrace();
    getValueVector().close();
  }

  public DictionaryProvider getDictionaryProvider() {
    throw new UnsupportedOperationException("Not implemented");
  }

  public abstract ValueVector getValueVector();

  /**
   * Returns a zero-copying new vector that contains the values from [offset, offset + length).
   *
   * @param offset the offset of the new vector
   * @param length the length of the new vector
   * @return the new vector
   */
  public abstract CometVector slice(int offset, int length);

  /**
   * Returns a corresponding `CometVector` implementation based on the given Arrow `ValueVector`.
   *
   * @param vector Arrow `ValueVector`
   * @param useDecimal128 Whether to use Decimal128 for decimal column
   * @return `CometVector` implementation
   */
  public static CometVector getVector(
      ValueVector vector, boolean useDecimal128, DictionaryProvider dictionaryProvider) {
    if (vector instanceof StructVector) {
      return new CometStructVector(vector, useDecimal128, dictionaryProvider);
    } else if (vector instanceof MapVector) {
      return new CometMapVector(vector, useDecimal128, dictionaryProvider);
    } else if (vector instanceof ListVector) {
      return new CometListVector(vector, useDecimal128, dictionaryProvider);
    } else {
      DictionaryEncoding dictionaryEncoding = vector.getField().getDictionary();
      CometPlainVector cometVector = new CometPlainVector(vector, useDecimal128);

      if (dictionaryEncoding == null) {
        return cometVector;
      } else {
        Dictionary dictionary = dictionaryProvider.lookup(dictionaryEncoding.getId());
        CometPlainVector dictionaryVector =
            new CometPlainVector(dictionary.getVector(), useDecimal128);
        CometDictionary cometDictionary = new CometDictionary(dictionaryVector);

        return new CometDictionaryVector(
            cometVector, cometDictionary, dictionaryProvider, useDecimal128);
      }
    }
  }

  protected static CometVector getVector(ValueVector vector, boolean useDecimal128) {
    return getVector(vector, useDecimal128, null);
  }
}
