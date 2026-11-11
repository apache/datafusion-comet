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
   * Borrow plain vectors and temporarily decode top-level dictionaries in `allocator`. The
   * callback must not retain or close the vectors. Close all temporaries in reverse order,
   * preserving the original failure with cleanup errors suppressed. Source columns stay borrowed.
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
