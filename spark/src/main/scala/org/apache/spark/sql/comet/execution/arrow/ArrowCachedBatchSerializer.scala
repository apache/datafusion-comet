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

import scala.collection.JavaConverters._

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, UnsafeProjection}
import org.apache.spark.sql.columnar.{CachedBatch, CachedBatchSerializer}
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.execution.columnar.{DefaultCachedBatch, DefaultCachedBatchSerializer}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.io.ChunkedByteBuffer

import org.apache.comet.CometConf

/**
 * Cached batch format used when Comet writes Spark in-memory cache data.
 *
 * `bytes` contains compressed Arrow stream data produced by `Utils.serializeBatches`. The cache
 * manager still owns storage and eviction; this class only changes the cached payload.
 */
private case class CometCachedBatch(
    override val numRows: Int,
    override val sizeInBytes: Long,
    stats: InternalRow,
    bytes: ChunkedByteBuffer)
    extends CachedBatch

/**
 * Cache serializer that stores Comet-compatible Arrow batches in Spark's in-memory cache.
 *
 * When Comet cache support is disabled, row-based cache writes and default cache reads are
 * delegated to Spark's `DefaultCachedBatchSerializer`.
 */
class ArrowCachedBatchSerializer extends CachedBatchSerializer {

  private val fallback = new DefaultCachedBatchSerializer()

  // Cache writes use Comet format only when both Comet and the in-memory cache scan are enabled.
  private def enabled(conf: SQLConf): Boolean = {
    CometConf.COMET_ENABLED.get(conf) &&
    CometConf.COMET_EXEC_IN_MEMORY_CACHE_ENABLED.get(conf)
  }

  // Row-to-Arrow conversion needs a StructType, while cache APIs pass attributes.
  private def toStructType(schema: Seq[Attribute]): StructType = {
    StructType(schema.map { attr =>
      StructField(attr.name, attr.dataType, attr.nullable, attr.metadata)
    })
  }

  override def supportsColumnarInput(schema: Seq[Attribute]): Boolean = {
    val activeConf = SQLConf.get
    activeConf != null && enabled(activeConf)
  }
  override def supportsColumnarOutput(schema: StructType): Boolean = true

  // Columnar Comet output is stored as compressed Arrow stream bytes.
  override def convertColumnarBatchToCachedBatch(
      input: RDD[ColumnarBatch],
      schema: Seq[Attribute],
      storageLevel: StorageLevel,
      conf: SQLConf): RDD[CachedBatch] = {

    input.mapPartitions { batches =>
      Utils.serializeBatches(batches).map { case (rows, buffer) =>
        CometCachedBatch(
          numRows = rows.toInt,
          sizeInBytes = buffer.size,
          stats = InternalRow.empty,
          bytes = buffer)
      }
    }
  }

  override def convertCachedBatchToColumnarBatch(
      input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[ColumnarBatch] = {

    // Resolve requested columns by exprId, not by name, because aliases may reuse names.
    val selectedIndices =
      if (selectedAttributes.isEmpty) {
        cacheAttributes.indices.toArray
      } else {
        val byExprId = cacheAttributes.zipWithIndex.map { case (attr, idx) =>
          attr.exprId -> idx
        }.toMap

        selectedAttributes.map { attr =>
          byExprId.getOrElse(
            attr.exprId,
            throw new IllegalStateException(
              s"Could not resolve selected attribute ${attr.name} from cache attributes"))
        }.toArray
      }

    val batchTypes = input.map(_.getClass.getName).distinct().collect()

    if (batchTypes.isEmpty) {
      input.sparkContext.emptyRDD[ColumnarBatch]
    } else if (batchTypes.length > 1) {
      throw new IllegalStateException(
        s"Mixed cached batch types are not supported: ${batchTypes.mkString(", ")}")
    } else if (batchTypes.head == classOf[CometCachedBatch].getName) {
      input.mapPartitions { it =>
        it.flatMap {
          case cb: CometCachedBatch =>
            Utils.decodeBatches(cb.bytes, "CometCache").map { batch =>
              if (selectedIndices.length == batch.numCols()) {
                batch
              } else {
                val cols =
                  selectedIndices.map(i => batch.column(i).asInstanceOf[ColumnVector])
                new ColumnarBatch(cols, batch.numRows())
              }
            }

          case other =>
            throw new IllegalStateException(
              s"Expected CometCachedBatch, got ${other.getClass.getName}")
        }
      }
    } else if (batchTypes.head == classOf[DefaultCachedBatch].getName) {
      fallback.convertCachedBatchToColumnarBatch(input, cacheAttributes, selectedAttributes, conf)
    } else {
      throw new IllegalStateException(s"Unsupported cached batch type: ${batchTypes.head}")
    }
  }

  // Row input can still be cached in Comet format by converting rows to Arrow batches first.
  override def convertInternalRowToCachedBatch(
      input: RDD[InternalRow],
      schema: Seq[Attribute],
      storageLevel: StorageLevel,
      conf: SQLConf): RDD[CachedBatch] = {

    if (!enabled(conf)) {
      fallback.convertInternalRowToCachedBatch(input, schema, storageLevel, conf)
    } else {
      val batchSize = conf.columnBatchSize
      val sessionTz = conf.sessionLocalTimeZone

      input.mapPartitions { rows =>
        val iter = CometArrowConverters.rowToArrowBatchIter(
          rows,
          toStructType(schema),
          batchSize,
          sessionTz,
          TaskContext.get())

        Utils.serializeBatches(iter).map { case (rows, buffer) =>
          CometCachedBatch(
            numRows = rows.toInt,
            sizeInBytes = buffer.size,
            stats = InternalRow.empty,
            bytes = buffer)
        }
      }
    }
  }

  override def convertCachedBatchToInternalRow(
      input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[InternalRow] = {

    // Resolve requested columns by exprId, not by name, because aliases may reuse names.
    val selectedIndices =
      if (selectedAttributes.isEmpty) {
        cacheAttributes.indices.toArray
      } else {
        val byExprId = cacheAttributes.zipWithIndex.map { case (attr, idx) =>
          attr.exprId -> idx
        }.toMap

        selectedAttributes.map { attr =>
          byExprId.getOrElse(
            attr.exprId,
            throw new IllegalStateException(
              s"Could not resolve selected attribute ${attr.name} from cache attributes"))
        }.toArray
      }

    val batchTypes = input.map(_.getClass.getName).distinct().collect()

    if (batchTypes.isEmpty) {
      input.sparkContext.emptyRDD[InternalRow]
    } else if (batchTypes.length > 1) {
      throw new IllegalStateException(
        s"Mixed cached batch types are not supported: ${batchTypes.mkString(", ")}")
    } else if (batchTypes.head == classOf[DefaultCachedBatch].getName) {
      fallback.convertCachedBatchToInternalRow(input, cacheAttributes, selectedAttributes, conf)
    } else if (batchTypes.head == classOf[CometCachedBatch].getName) {
      input.mapPartitions { it =>
        it.flatMap {
          case cb: CometCachedBatch =>
            Utils.decodeBatches(cb.bytes, "CometCache").flatMap { batch =>
              val projectedBatch =
                if (selectedIndices.length == batch.numCols()) {
                  batch
                } else {
                  val cols =
                    selectedIndices.map(i => batch.column(i).asInstanceOf[ColumnVector])
                  new ColumnarBatch(cols, batch.numRows())
                }

              // Spark's row collect path expects UnsafeRow, not ColumnarBatchRow wrappers.
              val toUnsafe = UnsafeProjection.create(selectedAttributes, selectedAttributes)
              projectedBatch.rowIterator().asScala.map(row => toUnsafe(row).copy())
            }

          case other =>
            throw new IllegalStateException(
              s"Expected CometCachedBatch, got ${other.getClass.getName}")
        }
      }
    } else {
      throw new IllegalStateException(s"Unsupported cached batch type: ${batchTypes.head}")
    }
  }

  override def buildFilter(
      predicates: Seq[Expression],
      cachedAttributes: Seq[Attribute]): (Int, Iterator[CachedBatch]) => Iterator[CachedBatch] = {
    (partitionIndex: Int, it: Iterator[CachedBatch]) => it
  }
}
