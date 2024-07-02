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

import scala.collection.mutable

import org.apache.arrow.c.{ArrowArray, ArrowImporter, ArrowSchema, CDataDictionaryProvider, Data}
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.dictionary.DictionaryProvider
import org.apache.spark.SparkException
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.CometArrowAllocator

class NativeUtil {
  import Utils._

  private val allocator = CometArrowAllocator
  private val dictionaryProvider: CDataDictionaryProvider = new CDataDictionaryProvider
  private val importer = new ArrowImporter(allocator)

  /**
   * Exports a Comet `ColumnarBatch` into a list of memory addresses that can be consumed by the
   * native execution.
   *
   * @param batch
   *   the input Comet columnar batch
   * @return
   *   a list containing number of rows + pairs of memory addresses in the format of (address of
   *   Arrow array, address of Arrow schema)
   */
  def exportBatch(batch: ColumnarBatch): Array[Long] = {
    val exportedVectors = mutable.ArrayBuffer.empty[Long]
    exportedVectors += batch.numRows()

    (0 until batch.numCols()).foreach { index =>
      batch.column(index) match {
        case a: CometVector =>
          val valueVector = a.getValueVector

          val provider = if (valueVector.getField.getDictionary != null) {
            a.getDictionaryProvider
          } else {
            null
          }

          val arrowSchema = ArrowSchema.allocateNew(allocator)
          val arrowArray = ArrowArray.allocateNew(allocator)
          Data.exportVector(
            allocator,
            getFieldVector(valueVector, "export"),
            provider,
            arrowArray,
            arrowSchema)

          exportedVectors += arrowArray.memoryAddress()
          exportedVectors += arrowSchema.memoryAddress()
        case c =>
          throw new SparkException(
            "Comet execution only takes Arrow Arrays, but got " +
              s"${c.getClass}")
      }
    }

    exportedVectors.toArray
  }

  /**
   * Imports a list of Arrow addresses from native execution, and return a list of Comet vectors.
   *
   * @param arrayAddress
   *   a list containing paris of Arrow addresses from the native, in the format of (address of
   *   Arrow array, address of Arrow schema)
   * @return
   *   a list of Comet vectors
   */
  def importVector(arrayAddress: Array[Long]): Seq[CometVector] = {
    val arrayVectors = mutable.ArrayBuffer.empty[CometVector]

    for (i <- arrayAddress.indices by 2) {
      val arrowSchema = ArrowSchema.wrap(arrayAddress(i + 1))
      val arrowArray = ArrowArray.wrap(arrayAddress(i))

      // Native execution should always have 'useDecimal128' set to true since it doesn't support
      // other cases.
      arrayVectors += CometVector.getVector(
        importer.importVector(arrowArray, arrowSchema, dictionaryProvider),
        true,
        dictionaryProvider)

      arrowArray.close()
      arrowSchema.close()
    }
    arrayVectors.toSeq
  }

  /**
   * Takes zero-copy slices of the input batch with given start index and maximum number of rows.
   *
   * @param batch
   *   Input batch
   * @param startIndex
   *   Start index of the slice
   * @param maxNumRows
   *   Maximum number of rows in the slice
   * @return
   *   A new batch with the sliced vectors
   */
  def takeRows(batch: ColumnarBatch, startIndex: Int, maxNumRows: Int): ColumnarBatch = {
    val arrayVectors = mutable.ArrayBuffer.empty[CometVector]

    for (i <- 0 until batch.numCols()) {
      val column = batch.column(i).asInstanceOf[CometVector]
      arrayVectors += column.slice(startIndex, maxNumRows)
    }

    new ColumnarBatch(arrayVectors.toArray, maxNumRows)
  }
}

object NativeUtil {
  def rootAsBatch(arrowRoot: VectorSchemaRoot): ColumnarBatch = {
    rootAsBatch(arrowRoot, null)
  }

  def rootAsBatch(arrowRoot: VectorSchemaRoot, provider: DictionaryProvider): ColumnarBatch = {
    val vectors = (0 until arrowRoot.getFieldVectors.size()).map { i =>
      val vector = arrowRoot.getFieldVectors.get(i)
      // Native shuffle always uses decimal128.
      CometVector.getVector(vector, true, provider)
    }
    new ColumnarBatch(vectors.toArray, arrowRoot.getRowCount)
  }
}
