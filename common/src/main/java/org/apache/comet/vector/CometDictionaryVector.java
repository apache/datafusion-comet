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

import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.util.TransferPair;
import org.apache.parquet.Preconditions;
import org.apache.spark.unsafe.types.UTF8String;

/** A column vector whose elements are dictionary-encoded. */
public class CometDictionaryVector extends CometDecodedVector {
  public final CometPlainVector indices;
  public final CometDictionary values;
  public final DictionaryProvider provider;

  /** Whether this vector is an alias sliced from another vector. */
  private final boolean isAlias;

  public CometDictionaryVector(
      CometPlainVector indices,
      CometDictionary values,
      DictionaryProvider provider,
      boolean useDecimal128) {
    this(indices, values, provider, useDecimal128, false);
  }

  public CometDictionaryVector(
      CometPlainVector indices,
      CometDictionary values,
      DictionaryProvider provider,
      boolean useDecimal128,
      boolean isAlias) {
    super(indices.valueVector, values.getValueVector().getField(), useDecimal128);
    Preconditions.checkArgument(
        indices.valueVector instanceof IntVector, "'indices' should be a IntVector");
    this.values = values;
    this.indices = indices;
    this.provider = provider;
    this.isAlias = isAlias;
  }

  @Override
  DictionaryProvider getDictionaryProvider() {
    return this.provider;
  }

  @Override
  public void close() {
    super.close();
    // Only close the values vector if this is not a sliced vector.
    if (!isAlias) {
      values.close();
    }
  }

  @Override
  public boolean getBoolean(int i) {
    return values.decodeToBoolean(indices.getInt(i));
  }

  @Override
  public byte getByte(int i) {
    return values.decodeToByte(indices.getInt(i));
  }

  @Override
  public short getShort(int i) {
    return values.decodeToShort(indices.getInt(i));
  }

  @Override
  public int getInt(int i) {
    return values.decodeToInt(indices.getInt(i));
  }

  @Override
  public long getLong(int i) {
    return values.decodeToLong(indices.getInt(i));
  }

  @Override
  public float getFloat(int i) {
    return values.decodeToFloat(indices.getInt(i));
  }

  @Override
  public double getDouble(int i) {
    return values.decodeToDouble(indices.getInt(i));
  }

  @Override
  public UTF8String getUTF8String(int i) {
    return values.decodeToUTF8String(indices.getInt(i));
  }

  @Override
  public byte[] getBinary(int i) {
    return values.decodeToBinary(indices.getInt(i));
  }

  @Override
  byte[] getBinaryDecimal(int i) {
    return values.decodeToBinary(indices.getInt(i));
  }

  @Override
  public CometVector slice(int offset, int length) {
    TransferPair tp = indices.valueVector.getTransferPair(indices.valueVector.getAllocator());
    tp.splitAndTransfer(offset, length);
    CometPlainVector sliced = new CometPlainVector(tp.getTo(), useDecimal128);

    // Set the alias flag to true so that the sliced vector will not close the dictionary vector.
    // Otherwise, if the dictionary is closed, the sliced vector will not be able to access the
    // dictionary.
    return new CometDictionaryVector(sliced, values, provider, useDecimal128, true);
  }
}
