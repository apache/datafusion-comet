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

package org.apache.spark.sql.comet.execution.arrow

import java.nio.ByteBuffer

import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.util.io.ChunkedByteBuffer

/**
 * Test-only access to the internals of `CometCachedBatch`.
 *
 * A top-level `private` class in Scala is visible to its own package, so this shim needs no
 * reflection; it exists so tests outside `org.apache.spark.sql.comet.execution.arrow` can assert
 * on the cached payload's shape.
 */
object CometCachedBatchHelper {

  /** Number of independently decodable column streams in a cached batch. */
  def numColumnStreams(batch: CachedBatch): Int =
    batch.asInstanceOf[CometCachedBatch].columns.length

  /** Serialized size of each column stream, in column order. */
  def columnStreamSizes(batch: CachedBatch): Seq[Long] =
    batch.asInstanceOf[CometCachedBatch].columns.map(_.size).toSeq

  /**
   * Replace one column's stream with bytes that cannot be decoded, in place.
   *
   * Reading a column this has corrupted fails; reading any other column only succeeds if that
   * column's stream was never touched. That is the difference between decoding what was projected
   * and decoding everything and projecting afterwards, so it is what the projection tests assert
   * on rather than timings.
   */
  def corruptColumnStream(batch: CachedBatch, index: Int): Unit = {
    val columns = batch.asInstanceOf[CometCachedBatch].columns
    columns(index) = new ChunkedByteBuffer(Array(ByteBuffer.wrap(Array[Byte](1, 2, 3, 4))))
  }
}
