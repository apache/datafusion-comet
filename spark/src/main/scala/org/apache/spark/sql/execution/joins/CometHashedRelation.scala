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

package org.apache.spark.sql.execution.joins

import java.io.{ObjectInput, ObjectOutput}

import scala.collection.JavaConverters._

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.config.{BUFFER_PAGESIZE, MEMORY_OFFHEAP_ENABLED}
import org.apache.spark.memory.{TaskMemoryManager, UnifiedMemoryManager}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, UnsafeProjection}
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.{DataType, LongType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnarBatchRow}
import org.apache.spark.unsafe.map.BytesToBytesMap
import org.apache.spark.util.io.ChunkedByteBuffer

trait CometHashedRelation {
  def values: Array[ChunkedByteBuffer]
}

class CometUnsafeHashedRelation(
    var values: Array[ChunkedByteBuffer],
    private var mode: HashedRelationBroadcastMode)
    extends UnsafeHashedRelation
    with CometHashedRelation {

  @transient
  private lazy val rowBaseRelation: HashedRelation = {
    val batches = Utils.deserializeBatches(values.toIterator)
    CometHashedRelation.transform(batches, mode)
  }

  override def get(key: Long): Iterator[InternalRow] = {
    rowBaseRelation.get(key)
  }

  override def get(key: InternalRow): Iterator[InternalRow] = {
    rowBaseRelation.get(key)
  }

  override def getValue(key: Long): InternalRow = {
    rowBaseRelation.getValue(key)
  }

  override def getValue(key: InternalRow): InternalRow = {
    rowBaseRelation.getValue(key)
  }

  override def getWithKeyIndex(key: InternalRow): Iterator[ValueRowWithKeyIndex] = {
    rowBaseRelation.getWithKeyIndex(key)
  }

  override def getValueWithKeyIndex(key: InternalRow): ValueRowWithKeyIndex = {
    rowBaseRelation.getValueWithKeyIndex(key)
  }

  override def valuesWithKeyIndex(): Iterator[ValueRowWithKeyIndex] = {
    rowBaseRelation.valuesWithKeyIndex()
  }

  override def maxNumKeysIndex: Int = {
    rowBaseRelation.maxNumKeysIndex
  }

  override def keyIsUnique: Boolean = rowBaseRelation.keyIsUnique

  override def keys(): Iterator[InternalRow] = rowBaseRelation.keys()

  override def asReadOnlyCopy(): CometUnsafeHashedRelation =
    new CometUnsafeHashedRelation(values, mode)

  override def estimatedSize: Long = values.map(_.size).sum

  override def close(): Unit = rowBaseRelation.close()

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeObject(values)
    out.writeObject(mode)
  }

  override def readExternal(in: ObjectInput): Unit = {
    values = in.readObject().asInstanceOf[Array[ChunkedByteBuffer]]
    mode = in.readObject().asInstanceOf[HashedRelationBroadcastMode]
  }
}

class CometLongHashedRelation(
    var values: Array[ChunkedByteBuffer],
    private var mode: HashedRelationBroadcastMode)
    extends LongHashedRelation
    with CometHashedRelation {

  @transient
  private lazy val rowBaseRelation: HashedRelation = {
    val batches = Utils.deserializeBatches(values.toIterator)
    CometHashedRelation.transform(batches, mode)
  }

  override def get(key: Long): Iterator[InternalRow] = {
    rowBaseRelation.get(key)
  }

  override def get(key: InternalRow): Iterator[InternalRow] = {
    rowBaseRelation.get(key)
  }

  override def getValue(key: Long): InternalRow = {
    rowBaseRelation.getValue(key)
  }

  override def getValue(key: InternalRow): InternalRow = {
    rowBaseRelation.getValue(key)
  }

  override def getWithKeyIndex(key: InternalRow): Iterator[ValueRowWithKeyIndex] = {
    rowBaseRelation.getWithKeyIndex(key)
  }

  override def getValueWithKeyIndex(key: InternalRow): ValueRowWithKeyIndex = {
    rowBaseRelation.getValueWithKeyIndex(key)
  }

  override def valuesWithKeyIndex(): Iterator[ValueRowWithKeyIndex] = {
    rowBaseRelation.valuesWithKeyIndex()
  }

  override def maxNumKeysIndex: Int = {
    rowBaseRelation.maxNumKeysIndex
  }

  override def keyIsUnique: Boolean = rowBaseRelation.keyIsUnique

  override def keys(): Iterator[InternalRow] = rowBaseRelation.keys()

  override def asReadOnlyCopy(): CometLongHashedRelation =
    new CometLongHashedRelation(values, mode)

  override def estimatedSize: Long = values.map(_.size).sum

  override def close(): Unit = rowBaseRelation.close()

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeObject(values)
    out.writeObject(mode)
  }

  override def readExternal(in: ObjectInput): Unit = {
    values = in.readObject().asInstanceOf[Array[ChunkedByteBuffer]]
    mode = in.readObject().asInstanceOf[HashedRelationBroadcastMode]
  }
}

object CometHashedRelation {

  def apply(
      values: Array[ChunkedByteBuffer],
      mode: HashedRelationBroadcastMode): CometHashedRelation = {
    if (mode.key.length == 1 && mode.key.head.dataType == LongType) {
      new CometLongHashedRelation(values, mode)
    } else {
      new CometUnsafeHashedRelation(values, mode)
    }
  }

  def transform(
      batches: Iterator[ColumnarBatch],
      mode: HashedRelationBroadcastMode): HashedRelation = {
    var numRows = 0L
    var valueFields = Array.empty[DataType]
    var valueFieldsSet = false
    val rows = batches.toStream
      .flatMap(b => {
        if (!valueFieldsSet) {
          valueFields = (0 until b.numCols()).map(b.column(_).dataType()).toArray
          valueFieldsSet = true
        }
        numRows += b.numRows()
        b.rowIterator().asScala
      })
      .toIterator
    if (numRows <= 0) {
      numRows = 64
    }

    val mm = new TaskMemoryManager(
      new UnifiedMemoryManager(
        new SparkConf().set(MEMORY_OFFHEAP_ENABLED.key, "false"),
        Long.MaxValue,
        Long.MaxValue / 2,
        1),
      0)

    if (!rows.hasNext) {
      EmptyHashedRelation
    } else if (mode.key.length == 1 && mode.key.head.dataType == LongType) {
      createLongHashedRelation(rows, mode.key, valueFields, numRows.toInt, mm, mode.isNullAware)
    } else {
      createUnsafeHashedRelation(rows, mode.key, valueFields, numRows.toInt, mm, mode.isNullAware)
    }
  }

  def createLongHashedRelation(
      input: Iterator[InternalRow],
      key: Seq[Expression],
      valueFields: Array[DataType],
      sizeEstimate: Int,
      taskMemoryManager: TaskMemoryManager,
      isNullAware: Boolean = false,
      ignoresDuplicatedKey: Boolean = false): HashedRelation = {

    val map = new LongToUnsafeRowMap(taskMemoryManager, sizeEstimate, ignoresDuplicatedKey)
    val keyGenerator = UnsafeProjection.create(key)
    val valueGenerator = UnsafeProjection.create(valueFields)

    // Create a mapping of key -> rows
    var numFields = 0
    while (input.hasNext) {
      val row = input.next().asInstanceOf[ColumnarBatchRow]
      numFields = row.numFields()
      val rowKey = keyGenerator(row)
      if (!rowKey.isNullAt(0)) {
        val key = rowKey.getLong(0)
        val unsafeRow = valueGenerator(row)
        map.append(key, unsafeRow)
      } else if (isNullAware) {
        map.free()
        return HashedRelationWithAllNullKeys
      }
    }
    map.optimize()
    new LongHashedRelation(numFields, map)
  }

  def createUnsafeHashedRelation(
      input: Iterator[InternalRow],
      key: Seq[Expression],
      valueFields: Array[DataType],
      sizeEstimate: Int,
      taskMemoryManager: TaskMemoryManager,
      isNullAware: Boolean = false,
      allowsNullKey: Boolean = false,
      ignoresDuplicatedKey: Boolean = false): HashedRelation = {
    require(
      !(isNullAware && allowsNullKey),
      "isNullAware and allowsNullKey cannot be enabled at same time")

    val pageSizeBytes = Option(SparkEnv.get)
      .map(_.memoryManager.pageSizeBytes)
      .getOrElse(new SparkConf().get(BUFFER_PAGESIZE).getOrElse(16L * 1024 * 1024))
    val binaryMap = new BytesToBytesMap(
      taskMemoryManager,
      // Only 70% of the slots can be used before growing, more capacity help to reduce collision
      (sizeEstimate * 1.5 + 1).toInt,
      pageSizeBytes)

    // Create a mapping of buildKeys -> rows
    val keyGenerator = UnsafeProjection.create(key)
    val valueGenerator = UnsafeProjection.create(valueFields)
    var numFields = 0
    while (input.hasNext) {
      val row = input.next().asInstanceOf[ColumnarBatchRow]
      numFields = row.numFields()
      val key = keyGenerator(row)
      val value = valueGenerator(row)
      if (!key.anyNull || allowsNullKey) {
        val loc = binaryMap.lookup(key.getBaseObject, key.getBaseOffset, key.getSizeInBytes)
        if (!(ignoresDuplicatedKey && loc.isDefined)) {
          val success = loc.append(
            key.getBaseObject,
            key.getBaseOffset,
            key.getSizeInBytes,
            value.getBaseObject,
            value.getBaseOffset,
            value.getSizeInBytes)
          if (!success) {
            binaryMap.free()
            throw QueryExecutionErrors.cannotAcquireMemoryToBuildUnsafeHashedRelationError()
          }
        }
      } else if (isNullAware) {
        binaryMap.free()
        return HashedRelationWithAllNullKeys
      }
    }

    new UnsafeHashedRelation(key.size, numFields, binaryMap)
  }

}
