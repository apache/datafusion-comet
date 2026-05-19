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

import org.junit.Test;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestCometPlainVector {

  @Test
  public void testGetUTF8StringWithVariableWidthVector() {
    try (RootAllocator allocator = new RootAllocator(Integer.MAX_VALUE)) {
      VarCharVector vector = new VarCharVector("strings", allocator);
      vector.allocateNew();
      vector.setSafe(0, bytes("alpha"));
      vector.setSafe(1, bytes(""));
      vector.setSafe(2, bytes("spark"));
      vector.setValueCount(4); // row 3 is null (validity bit not set)

      try (CometPlainVector cv = new CometPlainVector(vector, false)) {
        assertEquals("alpha", cv.getUTF8String(0).toString());
        assertEquals("", cv.getUTF8String(1).toString());
        assertEquals("spark", cv.getUTF8String(2).toString());
        assertNull(cv.getUTF8String(3));
      }
    }
  }

  @Test
  public void testGetBinaryWithVariableWidthVector() {
    try (RootAllocator allocator = new RootAllocator(Integer.MAX_VALUE)) {
      VarBinaryVector vector = new VarBinaryVector("bytes", allocator);
      vector.allocateNew();
      vector.setSafe(0, new byte[] {1, 2, 3}, 0, 3);
      vector.setSafe(1, new byte[0], 0, 0);
      vector.setSafe(2, new byte[] {4, 5}, 0, 2);
      vector.setValueCount(4); // row 3 is null (validity bit not set)

      try (CometPlainVector cv = new CometPlainVector(vector, false)) {
        assertArrayEquals(new byte[] {1, 2, 3}, cv.getBinary(0));
        assertArrayEquals(new byte[0], cv.getBinary(1));
        assertArrayEquals(new byte[] {4, 5}, cv.getBinary(2));
        assertNull(cv.getBinary(3));
      }
    }
  }

  private static byte[] bytes(String s) {
    return s.getBytes();
  }
}
