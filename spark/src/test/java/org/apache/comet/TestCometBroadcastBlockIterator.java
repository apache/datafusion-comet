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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestCometBroadcastBlockIterator {

  @Test
  public void releasesGrownBufferAtEof() throws Exception {
    ByteBuffer small = ByteBuffer.wrap(new byte[] {1, 2, 3});
    ByteBuffer large = ByteBuffer.allocate(256 * 1024);
    large.put(0, (byte) 7);

    CometBroadcastBlockIterator iterator =
        new CometBroadcastBlockIterator(
            Arrays.asList(new ByteBuffer[] {small}, new ByteBuffer[] {large}).iterator());

    assertEquals(3, iterator.hasNext());
    assertEquals(3, iterator.getCurrentBlockLength());
    assertEquals(256 * 1024, iterator.hasNext());
    assertTrue(iterator.getBuffer().capacity() >= 256 * 1024);
    assertEquals(7, iterator.getBuffer().get(0));

    assertEquals(-1, iterator.hasNext());
    assertNull(iterator.getBuffer());
    assertEquals(0, iterator.getCurrentBlockLength());
  }

  @Test
  public void releasesBufferOnExplicitClose() throws Exception {
    CometBroadcastBlockIterator iterator =
        new CometBroadcastBlockIterator(
            Collections.singletonList(new ByteBuffer[] {ByteBuffer.allocate(256 * 1024)})
                .iterator());

    assertEquals(256 * 1024, iterator.hasNext());
    assertTrue(iterator.getBuffer().capacity() >= 256 * 1024);
    iterator.close();

    assertNull(iterator.getBuffer());
    assertEquals(0, iterator.getCurrentBlockLength());
    assertEquals(-1, iterator.hasNext());
  }
}
