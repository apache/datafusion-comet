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

import scala.jdk.CollectionConverters._

import org.apache.arrow.c.{ArrowArrayStream, Data}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.arrow.vector.types.pojo.{Field, FieldType, Schema}
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.CometArrowAllocator
import org.apache.comet.vector.{CometDictionaryVector, CometVector, NativeUtil}

/**
 * Marker for Comet operators that can produce Arrow data destined for a Comet native executor
 * directly as the C Stream Interface, skipping the intermediate `RDD[ColumnarBatch]` layer.
 */
trait CometNativeArrowSource extends SparkPlan {
  def doExecuteAsArrowStream(): RDD[ArrowArrayStream]
}

object CometArrowStream extends Logging {

  /**
   * Native side asserts `Timestamp(Microsecond, Some("UTC"))` regardless of session timezone;
   * Spark's internal timestamp representation is always UTC microseconds anyway, and a non-UTC
   * timezone here would only show up as schema metadata that breaks Arrow RowConverter
   * validation. See COMET-2720.
   */
  val NATIVE_TIMEZONE: String = "UTC"

  /**
   * Wrap an `RDD[ColumnarBatch]` whose batches are Arrow-backed into an `RDD[ArrowArrayStream]`.
   */
  def wrapColumnarBatchRDD(
      rdd: RDD[ColumnarBatch],
      sparkSchema: StructType,
      timeZoneId: String,
      name: String): RDD[ArrowArrayStream] = {
    // Arrow `Schema` is not Serializable; only Spark's `StructType` is. Build the Arrow schema
    // inside the per-task body so the closure cleaner doesn't try to ship a Schema across.
    rdd.mapPartitionsInternal { batchIter =>
      val expected = Utils.toArrowSchema(sparkSchema, timeZoneId)
      val (arrowSchema, iter) = reconcileStreamSchema(name, expected, batchIter)
      stream(name, allocator => new ColumnarBatchArrowReader(allocator, arrowSchema, iter))
    }
  }

  /**
   * Wrap a single per-partition `Iterator[ColumnarBatch]` (Arrow-backed) and return the exported
   * `ArrowArrayStream`. For callers outside `CometExecRDD` that hand a JNI input slot directly to
   * a `CometExecIterator`.
   */
  def fromColumnarBatchIter(
      iter: Iterator[ColumnarBatch],
      sparkSchema: StructType,
      timeZoneId: String,
      name: String): ArrowArrayStream = {
    val expected = Utils.toArrowSchema(sparkSchema, timeZoneId)
    val (arrowSchema, reconciled) = reconcileStreamSchema(name, expected, iter)
    stream(name, allocator => new ColumnarBatchArrowReader(allocator, arrowSchema, reconciled))
      .next()
  }

  /**
   * Build the `inputObjects` array that `CometExecIterator` / `CometExec.getCometIterator` pass
   * to native `createPlan`, for the common case of a single scan input fed by one per-partition
   * `Iterator[ColumnarBatch]`. The iterator is exported to one `ArrowArrayStream` (Arrow C
   * Stream) and boxed as the lone element, using the native timezone.
   */
  def inputObjects(
      iter: Iterator[ColumnarBatch],
      sparkSchema: StructType,
      name: String): Array[Object] =
    Array[Object](fromColumnarBatchIter(iter, sparkSchema, NATIVE_TIMEZONE, name))

  /**
   * Build the stream's advertised Arrow schema from the actual `CometVector` types in the first
   * batch, not from `expected` (which derives from the consumer's Spark-declared types). Native
   * operators like `ScanExec` already cast their input to the declared scan-input schema in
   * `build_record_batch`, so the truthful schema lets that cast actually fire. Advertising
   * `expected` instead silently mislabels Int32 buffers as Int64 (and similar) and corrupts on
   * import. See PR #4393 width_bucket investigation.
   *
   * If the first batch's column types differ from `expected` in their `DataType` (timezone-only
   * differences on `Timestamp` are ignored), log one warning naming the operator, column, and
   * type drift; the cast happens transparently downstream in native.
   */
  private[arrow] def reconcileStreamSchema(
      name: String,
      expected: Schema,
      iter: Iterator[ColumnarBatch]): (Schema, Iterator[ColumnarBatch]) = {
    val buffered = iter.buffered
    if (!buffered.hasNext) {
      // Empty partition: keep the consumer-declared schema; consumer can still build its plan.
      return (expected, buffered)
    }
    val first = buffered.head
    val expectedFields = expected.getFields
    val actualFields = (0 until first.numCols()).map { i =>
      val col = first.column(i).asInstanceOf[CometVector]
      actualFieldOf(col, expectedFields.get(i))
    }
    val mismatches = actualFields.zip(expectedFields.asScala).zipWithIndex.collect {
      case ((actual, exp), idx) if actual.getType != exp.getType =>
        s"col[$idx] '${exp.getName}': expected ${exp.getType}, child produced ${actual.getType}"
    }
    if (mismatches.nonEmpty) {
      logWarning(
        s"CometArrowStream '$name' input schema mismatch: ${mismatches.mkString("; ")}. " +
          "Native ScanExec will cast at the boundary. This usually means a DataFusion-Spark " +
          "function declares a different return type than Spark catalyst.")
    }
    (new Schema(actualFields.asJava), buffered)
  }

  /**
   * The Arrow field that this column's buffers will look like once unloaded. For a
   * `CometDictionaryVector`, [[ColumnarBatchArrowReader]] decodes it via
   * `DictionaryEncoder.decode` before unloading, so the wire-level field is the dictionary's
   * *value* type, not `Dictionary<index, value>`. For everything else, use the underlying value
   * vector's field.
   *
   * Field name and metadata come from `expected` so that consumers indexing by name keep working.
   * Nullability is the union of the two: a CometVector that happens to hold no nulls in this
   * batch can still be nullable per Spark's contract (the next batch may have one), and a column
   * whose actual buffer carries validity bits must stay nullable even if Spark thought otherwise.
   * Taking only `raw.isNullable` here would advertise non-nullable when the next batch does carry
   * a null and crash native validation.
   */
  private def actualFieldOf(col: CometVector, expected: Field): Field = {
    val raw = col match {
      case d: CometDictionaryVector =>
        val indices = d.getValueVector
        val dict = d.provider.lookup(indices.getField.getDictionary.getId)
        dict.getVector.getField
      case _ => col.getValueVector.getField
    }
    val nullable = expected.isNullable || raw.isNullable
    val fieldType =
      new FieldType(nullable, raw.getType, raw.getDictionary, expected.getMetadata)
    new Field(expected.getName, fieldType, raw.getChildren)
  }

  /**
   * Allocate a child allocator, build a reader, export it as an `ArrowArrayStream`, and register
   * task-completion cleanup. Returns a single-element iterator so this composes with
   * `RDD.mapPartitionsInternal`.
   *
   * Close ordering: when native drops its `ArrowArrayStreamReader`, the C release callback fires
   * synchronously into `ExportedArrayStreamPrivateData.close` -> `reader.close` -> the VSR's
   * buffers are released. The task-completion listener registered here runs strictly after that
   * (Spark fires listeners in reverse registration order, and the listener that drops the native
   * plan is registered later by `CometExecIterator`), so `allocator.close` finds zero outstanding
   * bytes.
   */
  def stream(
      name: String,
      readerFactory: BufferAllocator => ArrowReader): Iterator[ArrowArrayStream] = {
    val context = TaskContext.get()
    val allocator = CometArrowAllocator.newChildAllocator(name, 0, Long.MaxValue)
    var reader: ArrowReader = null
    var arrowStream: ArrowArrayStream = null
    try {
      reader = readerFactory(allocator)
      arrowStream = ArrowArrayStream.allocateNew(allocator)
      Data.exportArrayStream(allocator, reader, arrowStream)
    } catch {
      case t: Throwable =>
        // Roll back partial setup before rethrowing -- nothing has been registered with
        // TaskContext yet, so without this the allocator (and possibly the reader/stream) leaks.
        if (arrowStream != null) {
          try arrowStream.close()
          catch { case _: Throwable => () }
        }
        if (reader != null) {
          try reader.close()
          catch { case _: Throwable => () }
        }
        try allocator.close()
        catch { case _: Throwable => () }
        throw t
    }
    if (context != null) {
      val streamRef = arrowStream
      context.addTaskCompletionListener[Unit] { _ =>
        streamRef.close()
        allocator.close()
      }
    }
    Iterator.single(arrowStream)
  }

  /**
   * Drive an `ArrowReader` from a per-task body and emit `ColumnarBatch`es wrapping the reader's
   * stable VSR. Lifecycle: the supplied factory builds the reader against a fresh child
   * allocator; both close at task completion. This is the non-native consumer path
   * (`doExecuteColumnar`) -- the native consumer path uses [[stream]] to export instead.
   */
  def readerBatchIter(
      name: String,
      readerFactory: BufferAllocator => ArrowReader): Iterator[ColumnarBatch] = {
    val context = TaskContext.get()
    val allocator = CometArrowAllocator.newChildAllocator(name, 0, Long.MaxValue)
    val reader =
      try readerFactory(allocator)
      catch {
        case t: Throwable =>
          try allocator.close()
          catch { case _: Throwable => () }
          throw t
      }
    if (context != null) {
      context.addTaskCompletionListener[Unit] { _ =>
        reader.close()
        allocator.close()
      }
    }
    new Iterator[ColumnarBatch] {
      // Lazily prefetch one batch so `hasNext` can answer without consuming.
      private var loaded: Boolean = false
      private var hasMore: Boolean = false

      private def ensureLoaded(): Unit = {
        if (!loaded) {
          hasMore = reader.loadNextBatch()
          loaded = true
        }
      }

      override def hasNext: Boolean = {
        ensureLoaded()
        hasMore
      }

      override def next(): ColumnarBatch = {
        ensureLoaded()
        if (!hasMore) {
          throw new NoSuchElementException("No more batches")
        }
        loaded = false
        NativeUtil.rootAsBatch(reader.getVectorSchemaRoot)
      }
    }
  }
}
