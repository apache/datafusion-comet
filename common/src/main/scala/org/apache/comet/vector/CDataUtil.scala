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

import org.apache.arrow.c.{ArrowArray, ArrowImporter, ArrowSchema, CDataDictionaryProvider}
import org.apache.arrow.memory.BufferAllocator
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.CometArrowAllocator

/**
 * Import-only C Data Interface bridge for Comet's shaded Arrow side.
 *
 * The caller (in the spark module) provides an export callback that fills pre-allocated
 * ArrowArray/ArrowSchema structs at given memory addresses. This object allocates those structs
 * using the shaded allocator and imports the resulting vectors as CometVectors.
 *
 * This design eliminates the need for reflection to cross the shading boundary: the spark module
 * calls unshaded Arrow directly, and the common module calls shaded Arrow directly. The two sides
 * communicate through Long memory addresses only.
 */
object CDataUtil {

  /**
   * Imports a columnar batch from the C Data Interface using a child of the global
   * [[CometArrowAllocator]]. This is the preferred entry point from the spark module since it
   * avoids passing a shaded allocator type across the shading boundary.
   */
  def importBatch(
      numCols: Int,
      numRows: Int,
      exportFn: (Int, Long, Long) => Unit): ColumnarBatch = {
    val allocator =
      CometArrowAllocator.newChildAllocator("CDataUtil-import", 0, Long.MaxValue)
    importBatch(numCols, numRows, allocator, exportFn)
  }

  /**
   * Imports a columnar batch from the C Data Interface.
   *
   * Allocates shaded ArrowArray/ArrowSchema structs for each column, invokes the provided export
   * function to fill them (using unshaded Arrow on the caller side), then imports the vectors
   * into CometVectors.
   *
   * @param numCols
   *   number of columns to import
   * @param numRows
   *   row count for the resulting ColumnarBatch
   * @param allocator
   *   shaded BufferAllocator for struct and vector allocation
   * @param exportFn
   *   callback (colIndex, arrayAddr, schemaAddr) => Unit that exports the unshaded vector into
   *   the struct memory at the given addresses
   * @return
   *   a ColumnarBatch with CometVector columns
   */
  def importBatch(
      numCols: Int,
      numRows: Int,
      allocator: BufferAllocator,
      exportFn: (Int, Long, Long) => Unit): ColumnarBatch = {
    val cometVectors = (0 until numCols).map { idx =>
      val arrowArray = ArrowArray.allocateNew(allocator)
      val arrowSchema = ArrowSchema.allocateNew(allocator)
      try {
        exportFn(idx, arrowArray.memoryAddress(), arrowSchema.memoryAddress())
        val importer = new ArrowImporter(allocator)
        val dictionaryProvider = new CDataDictionaryProvider()
        val vector = importer.importVector(arrowArray, arrowSchema, dictionaryProvider)
        CometVector.getVector(vector, true, dictionaryProvider)
      } catch {
        case e: Exception =>
          arrowArray.close()
          arrowSchema.close()
          throw e
      }
    }
    new ColumnarBatch(cometVectors.toArray, numRows)
  }
}
