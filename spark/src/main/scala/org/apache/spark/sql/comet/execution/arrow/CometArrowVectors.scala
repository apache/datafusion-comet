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

import java.util.{ArrayList => JArrayList}

import scala.collection.mutable.ListBuffer

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.dictionary.DictionaryEncoder
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.vector.{CometDictionaryVector, CometVector}

private[arrow] object CometArrowVectors {

  /**
   * The Arrow field vectors holding a Comet batch's logical values, in column order.
   *
   * A dictionary-encoded column is decoded first: its `getValueVector` exposes the index vector,
   * whose buffer layout does not match the field the column advertises, so an unload of the
   * column as-is would not line up with that field. Decoded vectors are appended to `decoded`;
   * the caller owns them and must close them once it is done with the returned list.
   *
   * The returned vectors are borrowed from `batch`, which retains ownership of them.
   */
  def materialize(
      batch: ColumnarBatch,
      allocator: BufferAllocator,
      decoded: ListBuffer[FieldVector]): JArrayList[FieldVector] = {
    val vectors = new JArrayList[FieldVector](batch.numCols())
    var i = 0
    while (i < batch.numCols()) {
      val column = batch.column(i).asInstanceOf[CometVector]
      vectors.add(column match {
        case d: CometDictionaryVector =>
          val indices = d.getValueVector
          val dictionary = d.provider.lookup(indices.getField.getDictionary.getId)
          val plain =
            DictionaryEncoder.decode(indices, dictionary, allocator).asInstanceOf[FieldVector]
          decoded += plain
          plain
        case other => other.getValueVector.asInstanceOf[FieldVector]
      })
      i += 1
    }
    vectors
  }
}
