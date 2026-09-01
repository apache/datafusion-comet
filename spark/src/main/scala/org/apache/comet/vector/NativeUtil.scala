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

import java.util.{ArrayList, HashSet}
import java.util.function.Function

import scala.collection.mutable

import org.apache.arrow.c.{ArrowArray, ArrowImporter, ArrowSchema, CDataDictionaryProvider, Data}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.util.AutoCloseables
import org.apache.arrow.vector.{FieldVector, VectorSchemaRoot}
import org.apache.arrow.vector.complex.{AbstractStructVector, ListVector, MapVector, StructVector}
import org.apache.arrow.vector.dictionary.DictionaryProvider
import org.apache.arrow.vector.types.pojo.{ArrowType, Field}
import org.apache.spark.SparkException
import org.apache.spark.sql.comet.execution.arrow.ConstantColumnVectors
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.execution.vectorized.ConstantColumnVector
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.CometArrowAllocator

/**
 * Provides functionality for importing Arrow vectors from native code and wrapping them as
 * CometVectors.
 *
 * Also provides functionality for exporting Comet columnar batches to native code.
 *
 * Each instance of NativeUtil creates an instance of CDataDictionaryProvider (a
 * DictionaryProvider that is used in C Data Interface for imports).
 *
 * NativeUtil must be closed after use to release resources in the dictionary provider.
 */
class NativeUtil extends AutoCloseable {
  import Utils._

  /** Use the global allocator */
  private val allocator = CometArrowAllocator

  /** ArrowImporter does not hold any state and does not need to be closed */
  private val importer = new ArrowImporter(allocator)

  /** Reuse one factory on the per-batch import hot path. */
  private val importVectorFactory: Function[Field, FieldVector] =
    field => NativeUtil.createVectorForImport(field, allocator)

  /**
   * Dictionary provider to use for the lifetime of this instance of NativeUtil. The dictionary
   * provider is closed when NativeUtil is closed.
   */
  private val dictionaryProvider: CDataDictionaryProvider = new CDataDictionaryProvider

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

    try {
      (0 until numCols).foreach { index =>
        // Publish each allocation before the next one, so partial allocation failures can free it.
        schemas(index) = ArrowSchema.allocateNew(allocator)
        arrays(index) = ArrowArray.allocateNew(allocator)
      }
    } catch {
      case failure: Throwable =>
        releaseArrowStructs(arrays, schemas, failure)
        throw failure
    }

    (arrays, schemas)
  }

  /**
   * Exports a ColumnarBatch to Arrow FFI and returns the memory addresses.
   *
   * This is a convenience method that allocates Arrow structs, exports the batch, and returns
   * just the memory addresses (without exposing the Arrow types).
   *
   * @param batch
   *   the columnar batch to export
   * @return
   *   a tuple of (array addresses, schema addresses, number of rows)
   */
  def exportBatchToAddresses(batch: ColumnarBatch): (Array[Long], Array[Long], Int) = {
    val numCols = batch.numCols()
    val (arrays, schemas) = allocateArrowStructs(numCols)
    val arrayAddrs = arrays.map(_.memoryAddress())
    val schemaAddrs = schemas.map(_.memoryAddress())
    val numRows = exportBatch(arrayAddrs, schemaAddrs, batch)
    (arrayAddrs, schemaAddrs, numRows)
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
    val numRows = mutable.ArrayBuffer.empty[Int]

    (0 until batch.numCols()).foreach { index =>
      batch.column(index) match {
        case a: CometVector =>
          val valueVector = a.getValueVector

          numRows += valueVector.getValueCount

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
        case cv: ConstantColumnVector =>
          // Spark uses ConstantColumnVector for partition columns / per-batch constants (e.g.
          // partition values, synthetic columns). Materialise to a fresh Arrow vector so Comet's
          // native side -- which expects Arrow Arrays only -- can ingest the batch. Without this,
          // queries that pull constants through a Comet operator fail with "Comet execution only
          // takes Arrow Arrays". "UTC" is intentional -- see `ConstantColumnVectors`.
          val materialised = ConstantColumnVectors
            .materialize(cv, cv.dataType(), batch.numRows(), s"_const_$index", allocator, "UTC")

          numRows += materialised.getValueCount

          val arrowSchema = ArrowSchema.wrap(schemaAddrs(index))
          val arrowArray = ArrowArray.wrap(arrayAddrs(index))
          Data.exportVector(allocator, materialised, null, arrowArray, arrowSchema)
        case c =>
          throw new SparkException(
            "Comet execution only takes Arrow Arrays, but got " +
              s"${c.getClass}")
      }
    }

    if (numRows.distinct.length > 1) {
      throw new SparkException(
        s"Number of rows in each column should be the same, but got [${numRows.distinct}]")
    }

    numRows.headOption.getOrElse(batch.numRows())
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

    val result =
      try {
        val arrayAddrs = arrays.map(_.memoryAddress())
        val schemaAddrs = schemas.map(_.memoryAddress())
        func(arrayAddrs, schemaAddrs)
      } catch {
        case failure: Throwable =>
          // Native may have populated some fields before failing. Their C release callbacks own
          // native buffers; closing the Arrow struct wrappers alone would leave those buffers live.
          releaseArrowStructs(arrays, schemas, failure)
          throw failure
      }

    result match {
      case -1 =>
        // EOF has no importer to consume the allocated structs.
        releaseArrowStructs(arrays, schemas)
        None
      case numRows =>
        val cometVectors = importVector(arrays, schemas)
        Some(new ColumnarBatch(cometVectors.toArray, numRows.toInt))
    }
  }

  private def releaseArrowStructs(
      arrays: Array[ArrowArray],
      schemas: Array[ArrowSchema],
      originalFailure: Throwable = null): Unit = {
    var failure = originalFailure
    def release(resource: => Unit): Unit = {
      try resource
      catch {
        case caught: Throwable =>
          if (failure == null) failure = caught
          else if (caught ne failure) failure.addSuppressed(caught)
      }
    }

    arrays.foreach { array =>
      if (array != null) {
        release(array.release())
        release(array.close())
      }
    }
    schemas.foreach { schema =>
      if (schema != null) {
        release(schema.release())
        release(schema.close())
      }
    }
    if (originalFailure == null && failure != null) throw failure
  }

  /**
   * Imports a list of Arrow addresses from native execution, and return a list of Comet vectors.
   *
   * On failure this releases everything it was given, both the arrays and schemas it has not
   * imported yet and the vectors it has already imported, so callers must not add cleanup of
   * their own.
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
    var firstUnconsumed = 0

    try {
      (0 until arrays.length).foreach { i =>
        val arrowSchema = schemas(i)
        val arrowArray = arrays(i)

        // importField's finally consumes the schema. ArrayImporter takes the array normally, while
        // ArrowImporter's catch releases it if the import fails before that transfer.
        firstUnconsumed = i + 1
        val arrowVector =
          importer.importVector(arrowArray, arrowSchema, dictionaryProvider, importVectorFactory)
        val cometVector =
          try CometVector.getVector(arrowVector, dictionaryProvider)
          catch {
            case failure: Throwable =>
              // Nothing took ownership of the column, so release both halves: the vector and the
              // dictionary values the import left in the provider. Read the field before closing,
              // so the walk never depends on metadata read back from a closed vector.
              val field = arrowVector.getField
              AutoCloseables.close(failure, arrowVector: AutoCloseable)
              ArrowImporter.closeDictionaries(field, dictionaryProvider, failure)
              throw failure
          }
        arrayVectors += cometVector
      }
      arrayVectors.toSeq
    } catch {
      case failure: Throwable =>
        AutoCloseables.close(failure, arrayVectors.toSeq: _*)
        releaseArrowStructs(arrays.drop(firstUnconsumed), schemas.drop(firstUnconsumed), failure)
        throw failure
    }
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

  override def close(): Unit = {
    // closing the dictionary provider also closes the dictionary arrays
    dictionaryProvider.close()
  }
}

object NativeUtil {

  /**
   * Create a vector whose physical struct children remain positional when the exported Arrow
   * schema contains duplicate names. Arrow's default struct factory indexes children by name and
   * collapses such fields.
   */
  private[comet] def createVector(field: Field, allocator: BufferAllocator): FieldVector = {
    val runtimeField = fieldForAllocation(field)
    createPinnedVector(runtimeField, field, allocator)
  }

  /**
   * Preserve Arrow's default allocation path unless a duplicate-name struct needs positional
   * runtime children. This is called for every imported column of every native batch.
   */
  private[comet] def createVectorForImport(
      field: Field,
      allocator: BufferAllocator): FieldVector = {
    val runtimeField = fieldForAllocation(field)
    if (runtimeField eq field) {
      field.createVector(allocator).asInstanceOf[FieldVector]
    } else {
      createPinnedVector(runtimeField, field, allocator)
    }
  }

  private def createPinnedVector(
      runtimeField: Field,
      exportField: Field,
      allocator: BufferAllocator): FieldVector = {
    exportField.getType match {
      case _: ArrowType.List | _: ArrowType.LargeList | _: ArrowType.FixedSizeList =>
        val vector = new RenamedListVector(runtimeField, exportField, allocator)
        vector.initializeChildrenFromFields(runtimeField.getChildren)
        vector
      case _: ArrowType.Map =>
        val vector = new RenamedMapVector(runtimeField, exportField, allocator)
        vector.initializeChildrenFromFields(runtimeField.getChildren)
        vector
      case _: ArrowType.Struct =>
        val vector = new RenamedStructVector(runtimeField, exportField, allocator)
        // The Field-based StructVector constructor creates the direct children. Initialize each
        // child's descendants from the runtime schema without adding the direct children twice.
        val runtimeChildren = runtimeField.getChildren
        var ordinal = 0
        while (ordinal < runtimeChildren.size()) {
          vector
            .getChildByOrdinal(ordinal)
            .asInstanceOf[FieldVector]
            .initializeChildrenFromFields(runtimeChildren.get(ordinal).getChildren)
          ordinal += 1
        }
        vector
      case _ => exportField.createVector(allocator).asInstanceOf[FieldVector]
    }
  }

  private def fieldForAllocation(field: Field): Field = {
    val children = field.getChildren
    if (children.isEmpty) return field

    val names = field.getType match {
      case _: ArrowType.Struct if children.size() > 1 => new HashSet[String](children.size())
      case _ => null
    }

    var hasDuplicateNames = false
    var runtimeChildren: ArrayList[Field] = null
    var ordinal = 0
    while (ordinal < children.size()) {
      val child = children.get(ordinal)
      val runtimeChild = fieldForAllocation(child)

      if ((runtimeChild ne child) && runtimeChildren == null) {
        runtimeChildren = new ArrayList[Field](children.size())
        var priorOrdinal = 0
        while (priorOrdinal < ordinal) {
          runtimeChildren.add(children.get(priorOrdinal))
          priorOrdinal += 1
        }
      }
      if (runtimeChildren != null) runtimeChildren.add(runtimeChild)

      if (names != null && !names.add(child.getName)) hasDuplicateNames = true
      ordinal += 1
    }

    val childrenForAllocation = if (runtimeChildren == null) children else runtimeChildren
    if (hasDuplicateNames) {
      val renamedChildren = new ArrayList[Field](children.size())
      ordinal = 0
      while (ordinal < childrenForAllocation.size()) {
        val child = childrenForAllocation.get(ordinal)
        renamedChildren.add(
          new Field(s"__comet_runtime_field_$ordinal", child.getFieldType, child.getChildren))
        ordinal += 1
      }
      new Field(field.getName, field.getFieldType, renamedChildren)
    } else if (runtimeChildren != null) {
      new Field(field.getName, field.getFieldType, runtimeChildren)
    } else {
      field
    }
  }

  /**
   * Pin `getField()` to the imported Field so FFI keeps the original child labels. ListVector's
   * runtime data-vector label is `"$data$"`; struct runtime names may be private and unique.
   */
  private final class RenamedListVector(
      runtimeField: Field,
      exportField: Field,
      allocator: BufferAllocator)
      extends ListVector(runtimeField, allocator, null) {
    override def getField: Field = exportField
  }

  private final class RenamedMapVector(
      runtimeField: Field,
      exportField: Field,
      allocator: BufferAllocator)
      extends MapVector(runtimeField, allocator, null) {
    override def getField: Field = exportField
  }

  private final class RenamedStructVector(
      runtimeField: Field,
      exportField: Field,
      allocator: BufferAllocator)
      extends StructVector(
        runtimeField,
        allocator,
        null,
        AbstractStructVector.ConflictPolicy.CONFLICT_ERROR,
        true) {
    override def getField: Field = {
      // StructVector's constructor calls getField before creating its children. Keep the unique
      // runtime field visible for that call, then publish the original metadata once all children
      // exist. The child count avoids a separate construction-state flag.
      if (size() == exportField.getChildren.size()) exportField else super.getField
    }
  }

  def rootAsBatch(arrowRoot: VectorSchemaRoot): ColumnarBatch = {
    rootAsBatch(arrowRoot, null)
  }

  def rootAsBatch(arrowRoot: VectorSchemaRoot, provider: DictionaryProvider): ColumnarBatch = {
    val vectors = (0 until arrowRoot.getFieldVectors.size()).map { i =>
      val vector = arrowRoot.getFieldVectors.get(i)
      CometVector.getVector(vector, provider)
    }
    new ColumnarBatch(vectors.toArray, arrowRoot.getRowCount)
  }
}
