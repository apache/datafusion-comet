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

package org.apache.spark.sql.comet.execution.shuffle

import java.io.ByteArrayInputStream
import java.io.InputStream

import org.scalatest.funsuite.AnyFunSuite

class CometBlockStoreShuffleReaderSuite extends AnyFunSuite {
  test("closing a partially consumed raw stream does not fetch remaining blocks") {
    class TrackingStream extends ByteArrayInputStream(Array[Byte](1, 2, 3)) {
      var wasClosed = false
      override def close(): Unit = {
        wasClosed = true
        super.close()
      }
    }

    val first = new TrackingStream
    val second = new TrackingStream
    var fetched = 0
    val blocks = new Iterator[InputStream] {
      private val remaining = List(first, second).iterator
      override def hasNext: Boolean = remaining.hasNext
      override def next(): InputStream = {
        fetched += 1
        remaining.next()
      }
    }

    val input = CometBlockStoreShuffleReader.concatenate(blocks)
    assert(input.read() == 1)
    assert(fetched == 1)

    input.close()

    assert(first.wasClosed)
    assert(!second.wasClosed)
    assert(fetched == 1)
  }

  test("reading the full raw stream returns and closes every block") {
    class TrackingStream(bytes: Array[Byte]) extends ByteArrayInputStream(bytes) {
      var wasClosed = false
      override def close(): Unit = {
        wasClosed = true
        super.close()
      }
    }

    val first = new TrackingStream(Array[Byte](1, 2, 3))
    val second = new TrackingStream(Array[Byte](4, 5, 6))
    var fetched = 0
    val blocks = new Iterator[InputStream] {
      private val remaining = List(first, second).iterator
      override def hasNext: Boolean = remaining.hasNext
      override def next(): InputStream = {
        fetched += 1
        remaining.next()
      }
    }

    val input = CometBlockStoreShuffleReader.concatenate(blocks)
    val bytes = Iterator.continually(input.read()).takeWhile(_ != -1).toList

    assert(bytes == List(1, 2, 3, 4, 5, 6))
    assert(fetched == 2)
    assert(first.wasClosed)
    assert(second.wasClosed)
  }
}
