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
   * Allocates Arrow structs for the given number of columns.
   *
   * @param numCols
   *   the number of columns
   * @return
   *   a pair of Arrow arrays and Arrow schemas
   */
  def allocateArrowStructs(numCols: Int): (Array[ArrowArray], Array[ArrowSchema]) = {
    val arrays = new Array[ArrowArray](numCols)
    val schemas = new Array[ArrowSchema](numCols)

    (0 until numCols).foreach { index =>
      val arrowSchema = ArrowSchema.allocateNew(allocator)
      val arrowArray = ArrowArray.allocateNew(allocator)
      arrays(index) = arrowArray
      schemas(index) = arrowSchema
    }

    (arrays, schemas)
  }

  /**
   * Exports a Comet `ColumnarBatch` into a list of memory addresses that can be consumed by the
   * native execution.
   *
   * @param batch
   *   the input Comet columnar batch
   * @return
   *   an exported batches object containing an array containing number of rows + pairs of memory
   *   addresses in the format of (address of Arrow array, address of Arrow schema)
   */
  def exportBatch(
      arrayAddrs: Array[Long],
      schemaAddrs: Array[Long],
      batch: ColumnarBatch): Int = {
    (0 until batch.numCols()).foreach { index =>
      batch.column(index) match {
        case a: CometVector =>
          val valueVector = a.getValueVector

          val provider = if (valueVector.getField.getDictionary != null) {
            a.getDictionaryProvider
          } else {
            null
          }

          // The array and schema structures are allocated by native side.
          // Don't need to deallocate them here.
          val arrowSchema = ArrowSchema.wrap(schemaAddrs(index))
          val arrowArray = ArrowArray.wrap(arrayAddrs(index))
          Data.exportVector(
            allocator,
            getFieldVector(valueVector, "export"),
            provider,
            arrowArray,
            arrowSchema)
        case c =>
          throw new SparkException(
            "Comet execution only takes Arrow Arrays, but got " +
              s"${c.getClass}")
      }
    }

    batch.numRows()
  }

  /**
   * Gets the next batch from native execution.
   *
   * @param numOutputCols
   *   The number of output columns
   * @param func
   *   The function to call to get the next batch
   * @return
   *   The number of row of the next batch, or None if there are no more batches
   */
  def getNextBatch(
      numOutputCols: Int,
      func: (Array[Long], Array[Long]) => Long): Option[ColumnarBatch] = {
    val (arrays, schemas) = allocateArrowStructs(numOutputCols)

    val arrayAddrs = arrays.map(_.memoryAddress())
    val schemaAddrs = schemas.map(_.memoryAddress())

    val result = func(arrayAddrs, schemaAddrs)

    result match {
      case -1 =>
        // EOF
        None
      case numRows =>
        val cometVectors = importVector(arrays, schemas)
        Some(new ColumnarBatch(cometVectors.toArray, numRows.toInt))
      case flag =>
        throw new IllegalStateException(s"Invalid native flag: $flag")
    }
  }

  /**
   * Imports a list of Arrow addresses from native execution, and return a list of Comet vectors.
   *
   * @param arrays
   *   a list of Arrow array
   * @param schemas
   *   a list of Arrow schema
   * @return
   *   a list of Comet vectors
   */
  def importVector(arrays: Array[ArrowArray], schemas: Array[ArrowSchema]): Seq[CometVector] = {
    val arrayVectors = mutable.ArrayBuffer.empty[CometVector]

    (0 until arrays.length).foreach { i =>
      val arrowSchema = schemas(i)
      val arrowArray = arrays(i)

      // Native execution should always have 'useDecimal128' set to true since it doesn't support
      // other cases.
      arrayVectors += CometVector.getVector(
        importer.importVector(arrowArray, arrowSchema, dictionaryProvider),
        true,
        dictionaryProvider)
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
