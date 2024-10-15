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

public class CometNativeVector extends CometVector {
  private final long address;

  public CometNativeVector(DataType type, boolean useDecimal128, long address) {
    super(type, useDecimal128);
    this.address = address;
  }

  @Override
  public void setNumNulls(int numNulls) {}

  @Override
  public void setNumValues(int numValues) {}

  @Override
  public int numValues() {
    return 0;
  }

  @Override
  public ValueVector getValueVector() {
    return null;
  }

  @Override
  public CometVector slice(int offset, int length) {
    return null;
  }

  @Override
  public boolean hasNull() {
    return false;
  }

  @Override
  public int numNulls() {
    return 0;
  }

  @Override
  public boolean isNullAt(int i) {
    return false;
  }

  @Override
  public void close() {}

  public long getAddress() {
    return address;
  }
}
