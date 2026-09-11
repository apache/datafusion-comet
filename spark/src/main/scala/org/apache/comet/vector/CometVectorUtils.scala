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

package org.apache.comet.vector

import scala.collection.mutable.ArrayBuffer

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.dictionary.DictionaryEncoder

/** Shared logical-vector access for Arrow stream producers. */
object CometVectorUtils {

  /**
   * Borrow `columns` in order and call `body` synchronously with their logical Arrow vectors.
   * Top-level dictionaries are decoded into new buffers in `allocator`; plain vectors remain
   * borrowed. The caller keeps the source columns, dictionaries and allocator alive, and `body`
   * must not close or retain the supplied vectors beyond this call. The return value is `body`'s
   * result and must not depend on those temporary vectors remaining open.
   *
   * All successfully decoded vectors are closed in reverse order, including when a later decode
   * or `body` fails. Source vectors are never closed or mutated. The original error is propagated
   * with cleanup errors suppressed; if only cleanup fails, its first error is propagated after
   * every temporary has been visited. Nested dictionaries are not materialized by this helper.
   */
  def withDecodedVectors[T](columns: Seq[CometVector], allocator: BufferAllocator)(
      body: Seq[FieldVector] => T): T = {
    val owned = ArrayBuffer.empty[FieldVector]
    var failure: Throwable = null
    try {
      val vectors = columns.map {
        case column: CometDictionaryVector =>
          val decoded = DictionaryEncoder
            .decode(column.getValueVector, column.getDictionary, allocator)
            .asInstanceOf[FieldVector]
          owned += decoded
          decoded
        case column => column.getValueVector.asInstanceOf[FieldVector]
      }
      body(vectors)
    } catch {
      case error: Throwable =>
        failure = error
        throw error
    } finally {
      var closeFailure = failure
      owned.reverseIterator.foreach { vector =>
        try vector.close()
        catch {
          case error: Throwable =>
            if (closeFailure == null) closeFailure = error
            else if (closeFailure ne error) closeFailure.addSuppressed(error)
        }
      }
      if (failure == null && closeFailure != null) throw closeFailure
    }
  }
}
