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

package org.apache.comet.parquet;

import org.junit.Test;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;

import org.apache.comet.vector.CometPlainVector;
import org.apache.comet.vector.CometVector;

import static org.junit.Assert.*;

public class TestColumnReader {
  @Test
  public void testIsFixedLength() {
    BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);

    ValueVector vv = new IntVector("v1", allocator);
    CometVector vector = new CometPlainVector(vv, false);
    assertTrue(vector.isFixedLength());

    vv = new FixedSizeBinaryVector("v2", allocator, 12);
    vector = new CometPlainVector(vv, false);
    assertTrue(vector.isFixedLength());

    vv = new VarBinaryVector("v3", allocator);
    vector = new CometPlainVector(vv, false);
    assertFalse(vector.isFixedLength());
  }
}
